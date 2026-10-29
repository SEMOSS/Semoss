/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.qs;

import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.StringProvider;
import net.sf.jsqlparser.statement.DescribeStatement;
import net.sf.jsqlparser.statement.ExplainStatement;
import net.sf.jsqlparser.statement.ShowColumnsStatement;
import net.sf.jsqlparser.statement.ShowStatement;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.Values;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.statement.show.ShowIndexStatement;
import net.sf.jsqlparser.statement.show.ShowTablesStatement;
import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.SqlTransactionException;
import prerna.engine.api.SqlTransactionResult;
import prerna.engine.impl.pipeline.PipelineInvocationHandler;
import prerna.query.parsers.SqlScriptSplitter;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.ITask;
import prerna.usertracking.DatabaseWriteAudit;
import prerna.util.EngineSyncUtility;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;

public abstract class AbstractSqlQueryReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AbstractSqlQueryReactor.class);

	private static final int DEFAULT_LIMIT = 50;
	private static final String COMMIT_KEY = "commit";
	private static final String TRANSACTION_KEY = "transaction";
	private static final String TIMEOUT_KEY = "timeout";

	/** The first words of the statements that start, end or mark a transaction */
	private static final Set<String> TRANSACTION_CONTROL = Set.of("BEGIN", "START", "COMMIT", "END", "ROLLBACK",
			"ABORT", "SAVEPOINT", "RELEASE", "XA");
	/** The settings a SET statement changes that commit or end the transaction */
	private static final Set<String> TRANSACTION_SETTINGS = Set.of("TRANSACTION", "AUTOCOMMIT",
			"IMPLICIT_TRANSACTIONS");
	/**
	 * The scopes a SET statement can name before its setting, as in SET SESSION
	 * autocommit
	 */
	private static final Set<String> SETTING_SCOPES = Set.of("SESSION", "GLOBAL", "LOCAL");
	/** The engine method a script in one transaction runs through */
	private static final String TRANSACTION_METHOD = "executeInTransaction";
	/** The engine methods the statements of a script run through one at a time */
	private static final List<String> PER_STATEMENT_METHODS = List.of("execQuery", "insertData", "removeData");

	// the user's level on the database, read once at the start of the request and
	// checked against every statement
	private AccessPermissionEnum userLevel;

	enum QueryRoute {
		READ(true, false), LOCKING_READ(true, true), WRITE(false, true);

		final boolean returnsRows;
		final boolean requiresEdit;

		QueryRoute(boolean returnsRows, boolean requiresEdit) {
			this.returnsRows = returnsRows;
			this.requiresEdit = requiresEdit;
		}
	}

	public AbstractSqlQueryReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.QUERY_KEY.getKey(), ReactorKeysEnum.DATABASE.getKey(),
				ReactorKeysEnum.LIMIT.getKey(), COMMIT_KEY, TRANSACTION_KEY, TIMEOUT_KEY };
		this.keyRequired = new int[] { 1, 1, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata("User must be signed into an account in order to run this operation",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		organizeKeys();
		String sqlQuery = getDecodedQuery();
		String databaseId = this.keyValue.get(this.keysToGet[1]);
		String limitStr = this.keyValue.get(this.keysToGet[2]);
		String commitStr = this.keyValue.get(this.keysToGet[3]);
		boolean oneTransaction = parseFlag(this.keyValue.get(TRANSACTION_KEY));
		Integer timeoutSeconds = parseTimeout(this.keyValue.get(TIMEOUT_KEY));

		if (sqlQuery == null || sqlQuery.trim().isEmpty()) {
			throw new SemossPixelException("SQL query cannot be empty");
		}

		if (databaseId == null || databaseId.trim().isEmpty()) {
			throw new SemossPixelException("Database id is required");
		}

		if (timeoutSeconds != null && !oneTransaction) {
			throw new SemossPixelException("The timeout is only used with transaction=true");
		}

		IEngine engine = Utility.getEngine(databaseId);
		if (!(engine instanceof IRDBMSEngine)) {
			throw new IllegalArgumentException("The database is not a RDBMS engine that accepts SQL");
		}
		IRDBMSEngine rdbmsEngine = (IRDBMSEngine) engine;

		this.userLevel = SecurityEngineUtils.getCurrentUserEngineLevel(user, databaseId);
		if (this.userLevel == null) {
			throw new SemossPixelException("User does not have permission to query this database");
		}

		if (oneTransaction) {
			return executeInOneTransaction(sqlQuery, databaseId, rdbmsEngine, timeoutSeconds, parseLimit(limitStr));
		}

		try {
			List<ParsedSqlStatement> statements = parseQueryStatements(sqlQuery,
					usesSquareBracketQuotation(rdbmsEngine));
			for (ParsedSqlStatement statement : statements) {
				classLogger.info("Detected SQL query route: {}", statement.route);
				requireAccess(this.userLevel, statement.route);
			}

			if (statements.size() == 1) {
				return executeSingleStatement(sqlQuery, databaseId, rdbmsEngine, statements.get(0).route, limitStr,
						commitStr);
			}
			return executeBatch(statements, sqlQuery, databaseId, rdbmsEngine, limitStr, commitStr);
		} catch (Exception e) {
			classLogger.error("Error executing SQL query for database {}", databaseId, e);
			throw new SemossPixelException("Error executing SQL query: " + e.getMessage());
		}
	}

	/**
	 * 
	 * @return Decoded SQL query string ready for execution
	 * @throws IllegalArgumentException if decoding fails or input is invalid
	 */
	protected abstract String getDecodedQuery();

	/**
	 * Parse one SQL statement and determine its execution route from the AST.
	 * Parser failures are not guessed from SQL text because routing a mutation as a
	 * read would weaken both authorization and execution semantics.
	 */
	static QueryRoute detectQueryRoute(String sql, boolean squareBracketQuotation) {
		List<ParsedSqlStatement> statements = parseQueryStatements(sql, squareBracketQuotation);
		if (statements.size() != 1) {
			throw new IllegalArgumentException("SqlQuery contains more than one SQL statement");
		}
		return statements.get(0).route;
	}

	static List<ParsedSqlStatement> parseQueryStatements(String sql, boolean squareBracketQuotation) {
		if (sql == null || sql.trim().isEmpty()) {
			throw new IllegalArgumentException("SQL query cannot be empty");
		}

		try {
			CCJSqlParser parser = new CCJSqlParser(new StringProvider(sql));
			parser.withSquareBracketQuotation(squareBracketQuotation);
			parser.setErrorRecovery(false);
			Statements statements = parser.Statements();
			if (statements.isEmpty()) {
				throw new IllegalArgumentException("SQL query does not contain an executable statement");
			}
			List<ParsedSqlStatement> parsed = new ArrayList<>(statements.size());
			for (Statement statement : statements) {
				parsed.add(new ParsedSqlStatement(statement.toString(), routeStatement(statement)));
			}
			return parsed;
		} catch (Exception e) {
			if (e instanceof IllegalArgumentException) {
				throw (IllegalArgumentException) e;
			}
			throw new IllegalArgumentException("Unable to parse SQL and determine its execution route", e);
		}
	}

	static final class ParsedSqlStatement {
		final String sql;
		final QueryRoute route;

		ParsedSqlStatement(String sql, QueryRoute route) {
			this.sql = sql;
			this.route = route;
		}
	}

	private static QueryRoute routeStatement(Statement statement) {
		if (statement instanceof Select) {
			return routeSelect((Select) statement);
		}
		if (statement instanceof ShowStatement || statement instanceof ShowColumnsStatement
				|| statement instanceof ShowTablesStatement || statement instanceof ShowIndexStatement
				|| statement instanceof DescribeStatement || statement instanceof ExplainStatement) {
			return QueryRoute.READ;
		}
		return QueryRoute.WRITE;
	}

	private static QueryRoute routeSelect(Select select) {
		QueryRoute route = routeSelectBody(select);
		if (select.getWithItemsList() != null) {
			for (WithItem<?> withItem : select.getWithItemsList()) {
				route = stricterRoute(route, routeSelectBody(withItem.getSelect()));
			}
		}
		return route;
	}

	private static QueryRoute routeSelectBody(Select selectBody) {
		if (selectBody == null) {
			throw new IllegalArgumentException("SQL SELECT has no query body");
		}
		if (selectBody instanceof ParenthesedSelect) {
			return routeSelect(((ParenthesedSelect) selectBody).getSelect());
		}
		if (selectBody instanceof PlainSelect) {
			PlainSelect select = (PlainSelect) selectBody;
			if (select.getIntoTables() != null && !select.getIntoTables().isEmpty()) {
				return QueryRoute.WRITE;
			}
			return select.getForMode() != null ? QueryRoute.LOCKING_READ : QueryRoute.READ;
		}
		if (selectBody instanceof SetOperationList) {
			QueryRoute route = QueryRoute.READ;
			for (Select child : ((SetOperationList) selectBody).getSelects()) {
				route = stricterRoute(route, routeSelectBody(child));
			}
			return route;
		}
		if (selectBody instanceof Values) {
			return QueryRoute.READ;
		}
		throw new IllegalArgumentException("Unsupported SQL SELECT body: " + selectBody.getClass().getSimpleName());
	}

	private static QueryRoute stricterRoute(QueryRoute left, QueryRoute right) {
		if (left == QueryRoute.WRITE || right == QueryRoute.WRITE) {
			return QueryRoute.WRITE;
		}
		if (left == QueryRoute.LOCKING_READ || right == QueryRoute.LOCKING_READ) {
			return QueryRoute.LOCKING_READ;
		}
		return QueryRoute.READ;
	}

	private static boolean usesSquareBracketQuotation(IRDBMSEngine engine) {
		RdbmsTypeEnum dbType = engine.getDbType();
		return dbType == RdbmsTypeEnum.SQL_SERVER || dbType == RdbmsTypeEnum.SYNAPSE;
	}

	/**
	 * Refuses a statement the user's level does not cover: a read needs any level,
	 * and a locking read or a write needs editor or owner. The same rule holds with
	 * and without transaction=true.
	 *
	 * @param level the user's level on the database, or null for no access
	 * @param route the statement's route
	 * @throws SemossPixelException when the level does not cover the route
	 */
	static void requireAccess(AccessPermissionEnum level, QueryRoute route) {
		if (level == null) {
			throw new SemossPixelException("User does not have permission to query this database");
		}
		if (route.requiresEdit && !AccessPermissionEnum.isEditor(level.getId())) {
			throw new SemossPixelException("User does not have permission to modify this database");
		}
	}

	/**
	 * 
	 * @param sqlQuery
	 * @param databaseId
	 * @param queryRoute
	 * @param limitStr
	 * @param commitStr
	 * @return
	 */
	private NounMetadata delegateToAppropriateReactor(String sqlQuery, String databaseId, QueryRoute queryRoute,
			String limitStr, String commitStr) {

		if (queryRoute.returnsRows) {
			return executeSelectQuery(sqlQuery, databaseId, limitStr);
		} else {
			return executeModificationQuery(sqlQuery, databaseId, commitStr);
		}
	}

	/**
	 * Runs a request holding one statement. A write records its database write
	 * event and pushes a file-based engine once it has run.
	 */
	private NounMetadata executeSingleStatement(String sqlQuery, String databaseId, IRDBMSEngine engine,
			QueryRoute route, String limitStr, String commitStr) {
		if (route.returnsRows) {
			return executeSelectQuery(sqlQuery, databaseId, limitStr);
		}
		NounMetadata result;
		try {
			result = executeModificationQuery(sqlQuery, databaseId, commitStr);
		} catch (RuntimeException e) {
			recordWrite(engine, sqlQuery, 1, 0, 0, e);
			throw e;
		}
		recordWrite(engine, sqlQuery, 1, 1, 1, null);
		pushFileBasedEngine(engine, result);
		return result;
	}

	/**
	 * Runs the statements in order, each committing on its own, and stops at the
	 * first failure. A request with any write records one database write event and
	 * pushes a file-based engine once, after the last statement that ran.
	 */
	private NounMetadata executeBatch(List<ParsedSqlStatement> statements, String sqlQuery, String databaseId,
			IRDBMSEngine engine, String limitStr, String commitStr) {
		List<Map<String, Object>> results = new ArrayList<>(statements.size());
		int succeeded = 0;
		int committedWrites = 0;
		Exception failure = null;
		for (int index = 0; index < statements.size(); index++) {
			ParsedSqlStatement statement = statements.get(index);
			long startedAt = System.nanoTime();
			try {
				NounMetadata execution = delegateToAppropriateReactor(statement.sql, databaseId, statement.route,
						limitStr, commitStr);
				results.add(toBatchSuccess(index, statement, execution, startedAt));
				succeeded++;
				if (!statement.route.returnsRows) {
					committedWrites++;
				}
			} catch (Exception e) {
				failure = e;
				results.add(toBatchError(index, statement, e, elapsedMillis(startedAt)));
				for (int skippedIndex = index + 1; skippedIndex < statements.size(); skippedIndex++) {
					results.add(toBatchSkipped(skippedIndex, statements.get(skippedIndex)));
				}
				break;
			}
		}
		NounMetadata noun = new NounMetadata(results, PixelDataType.VECTOR, PixelOperationType.VECTOR);
		boolean hasWrite = statements.stream().anyMatch(statement -> !statement.route.returnsRows);
		if (hasWrite) {
			recordWrite(engine, sqlQuery, statements.size(), succeeded, committedWrites, failure);
			if (committedWrites > 0) {
				pushFileBasedEngine(engine, noun);
			}
		}
		return noun;
	}

	private Map<String, Object> toBatchSuccess(int index, ParsedSqlStatement statement, NounMetadata execution,
			long startedAt) throws Exception {
		Map<String, Object> result = batchResult(index, statement.sql, statement.route, "SUCCESS", 0);
		if (!statement.route.returnsRows) {
			result.put("type", "MESSAGE");
			result.put("message", "Statement executed successfully");
			result.put("timeToRun", elapsedMillis(startedAt));
			return result;
		}

		Object value = execution.getValue();
		if (!(value instanceof ITask)) {
			throw new IllegalStateException("Read statement did not return a query task");
		}
		ITask task = (ITask) value;
		try {
			result.put("type", "TABLE");
			result.put("output", task.collect(false).get("data"));
			result.put("timeToRun", elapsedMillis(startedAt));
			return result;
		} finally {
			task.close();
		}
	}

	private static Map<String, Object> toBatchError(int index, ParsedSqlStatement statement, Exception exception,
			long timeToRun) {
		Map<String, Object> result = batchResult(index, statement.sql, statement.route, "ERROR", timeToRun);
		result.put("type", "ERROR");
		result.put("message",
				exception.getMessage() == null ? exception.getClass().getSimpleName() : exception.getMessage());
		return result;
	}

	private static Map<String, Object> toBatchSkipped(int index, ParsedSqlStatement statement) {
		Map<String, Object> result = batchResult(index, statement.sql, statement.route, "SKIPPED", 0);
		result.put("type", "SKIPPED");
		result.put("message", "Not executed because an earlier statement failed");
		return result;
	}

	private static Map<String, Object> batchResult(int index, String sql, QueryRoute route, String status,
			long timeToRun) {
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("statement", index + 1);
		result.put("query", sql);
		result.put("route", route.name());
		result.put("status", status);
		result.put("timeToRun", timeToRun);
		return result;
	}

	private static long elapsedMillis(long startedAt) {
		return (System.nanoTime() - startedAt) / 1_000_000;
	}

	/**
	 * Runs a script in one transaction that commits once, so a failing statement
	 * leaves none of the script's changes in the database. Each statement runs as
	 * submitted and needs the access it needs without transaction=true. Nothing
	 * runs when a statement needs access the user does not have, a statement
	 * controls the transaction itself, or the database's guardrails check its
	 * statements one at a time but not scripts. The script is recorded as its
	 * statements would be: one write event, and its SQL in the audit logs when they
	 * store the database's engine calls. A file-based database is locked while the
	 * script runs and is pushed to cloud storage after a write, so no other change
	 * or cloud sync touches its files in between.
	 *
	 * @param maxRows the most rows to keep from each statement that returns rows
	 */
	private NounMetadata executeInOneTransaction(String sqlQuery, String databaseId, IRDBMSEngine engine,
			Integer timeoutSeconds, int maxRows) {
		List<SqlScriptSplitter.Statement> statements = splitScript(sqlQuery);
		List<QueryRoute> routes = new ArrayList<>(statements.size());
		for (int index = 0; index < statements.size(); index++) {
			SqlScriptSplitter.Statement statement = statements.get(index);
			String conflict = transactionConflict(statement.leadingWords());
			if (conflict != null) {
				throw new SemossPixelException(
						"Statement " + (index + 1) + " cannot run in one transaction because it " + conflict);
			}
			routes.add(routeOrWrite(statement.body(), usesSquareBracketQuotation(engine)));
		}
		for (QueryRoute route : routes) {
			requireAccess(this.userLevel, route);
		}
		requireTransactionGuardrails(engine);

		if (!engine.holdsFileLocks()) {
			return runOneTransaction(sqlQuery, databaseId, engine, statements, routes, timeoutSeconds, maxRows);
		}
		// the engine's monitor, which a file-based insert also holds, so file writes go
		// one at a time; and its sync lock, so no push, pull or copy reads or replaces
		// its files until this change is pushed
		synchronized (engine) {
			ReentrantLock syncLock = EngineSyncUtility.getEngineLock(engine.getEngineId());
			syncLock.lock();
			try {
				return runOneTransaction(sqlQuery, databaseId, engine, statements, routes, timeoutSeconds, maxRows);
			} finally {
				syncLock.unlock();
			}
		}
	}

	/**
	 * Refuses a script in one transaction on a database whose guardrails check its
	 * SQL statement by statement but have no pipeline for scripts, so a script
	 * never runs what its statements could not run one at a time.
	 *
	 * @param engine the database's engine
	 */
	static void requireTransactionGuardrails(IRDBMSEngine engine) {
		if (PipelineInvocationHandler.guardsMethod(engine, TRANSACTION_METHOD)) {
			return;
		}
		for (String method : PER_STATEMENT_METHODS) {
			if (PipelineInvocationHandler.guardsMethod(engine, method)) {
				throw new SemossPixelException("This database's guardrails check SQL statements run one at a time "
						+ "but not scripts run in one transaction. Ask its owner to add an " + TRANSACTION_METHOD
						+ " entry to its pipeline, or run the script without transaction=true");
			}
		}
	}

	/**
	 * Runs the checked statements in one transaction. A script with a write records
	 * the write and pushes a file-based engine whose commit happened, or may have;
	 * a script of reads does neither, as outside a transaction.
	 */
	private NounMetadata runOneTransaction(String sqlQuery, String databaseId, IRDBMSEngine engine,
			List<SqlScriptSplitter.Statement> statements, List<QueryRoute> routes, Integer timeoutSeconds,
			int maxRows) {
		boolean writes = routes.stream().anyMatch(route -> !route.returnsRows);
		List<String> texts = new ArrayList<>(statements.size());
		for (SqlScriptSplitter.Statement statement : statements) {
			texts.add(statement.text());
		}
		SqlTransactionResult result;
		try {
			result = engine.executeInTransaction(texts, timeoutSeconds == null ? 0 : timeoutSeconds, maxRows);
		} catch (SqlTransactionException e) {
			result = e.getResult();
		} catch (SQLFeatureNotSupportedException e) {
			// nothing ran: the database cannot run this script in one transaction, such as
			// one with a statement the database can commit as soon as it runs
			String message = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
			if (writes) {
				recordNotRun(sqlQuery, engine, statements.size(), message, timeoutSeconds);
			}
			throw new SemossPixelException(
					"The SQL script did not run: " + message + ". Run it without transaction=true instead");
		} catch (Exception e) {
			// nothing ran: a guardrail blocked the script, or no connection could be opened
			classLogger.error("The SQL script did not run on database {}", databaseId, e);
			String message = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
			if (writes) {
				recordNotRun(sqlQuery, engine, statements.size(), message, timeoutSeconds);
			}
			throw new SemossPixelException("The SQL script did not run: " + message);
		}

		if (writes) {
			DatabaseWriteAudit.record(this.insight, engine,
					new DatabaseWriteAudit.Write(getPixelName(), DatabaseWriteAudit.Transaction.ONE_TRANSACTION,
							statements.size(), result.getRanCount(), result.getFailedStatement(),
							auditOutcome(result.getOutcome()), sqlQuery, result.getMessage(), timeoutSeconds));
		}
		NounMetadata noun = toTransactionResult(statements, routes, result);
		if (writes && result.getOutcome() != SqlTransactionResult.Outcome.ROLLED_BACK) {
			// a commit that happened, or may have, leaves files to push
			pushFileBasedEngine(engine, noun);
		}
		return noun;
	}

	/**
	 * Records a one-transaction script with a write, none of whose statements ran
	 */
	private void recordNotRun(String sqlQuery, IRDBMSEngine engine, int statementCount, String message,
			Integer timeoutSeconds) {
		DatabaseWriteAudit.record(this.insight, engine,
				new DatabaseWriteAudit.Write(getPixelName(), DatabaseWriteAudit.Transaction.ONE_TRANSACTION,
						statementCount, 0, null, DatabaseWriteAudit.Outcome.ROLLED_BACK, sqlQuery, message,
						timeoutSeconds));
	}

	private static List<SqlScriptSplitter.Statement> splitScript(String sqlQuery) {
		List<SqlScriptSplitter.Statement> statements;
		try {
			statements = SqlScriptSplitter.split(sqlQuery);
		} catch (IllegalArgumentException e) {
			throw new SemossPixelException("Unable to split the SQL into statements: " + e.getMessage());
		}
		if (statements.isEmpty()) {
			throw new SemossPixelException("SQL query does not contain an executable statement");
		}
		return statements;
	}

	/**
	 * Why a statement cannot run in the script's one transaction, read from its
	 * first words. Only statements that start, end or mark a transaction are
	 * refused here. On a database that commits data definition as soon as it runs,
	 * the engine refuses a script of more than one statement with any statement
	 * that does not only read or change rows. A statement the database itself will
	 * not run inside a transaction fails when it runs, and the script rolls back.
	 *
	 * @param words the statement's first words outside quotes and comments, in
	 *              upper case
	 * @return the reason, worded to follow "because it", or null when it can run
	 */
	static String transactionConflict(List<String> words) {
		if (words.isEmpty()) {
			return null;
		}
		String first = words.get(0);
		String second = words.size() > 1 ? words.get(1) : "";
		String third = words.size() > 2 ? words.get(2) : "";
		boolean setsTransaction = "SET".equals(first) && (TRANSACTION_SETTINGS.contains(second)
				|| (SETTING_SCOPES.contains(second) && "AUTOCOMMIT".equals(third)));
		if (TRANSACTION_CONTROL.contains(first) || setsTransaction
				|| ("PREPARE".equals(first) && "TRANSACTION".equals(second))
				|| ("SAVE".equals(first) && ("TRAN".equals(second) || "TRANSACTION".equals(second)))) {
			return "controls the transaction itself";
		}
		return null;
	}

	/**
	 * The route of one statement in one transaction, which decides the access it
	 * needs. A statement JSqlParser cannot read, such as a stored function
	 * definition, is labeled a write, so it needs edit access, instead of being
	 * rejected.
	 */
	static QueryRoute routeOrWrite(String sql, boolean squareBracketQuotation) {
		try {
			List<ParsedSqlStatement> parsed = parseQueryStatements(sql, squareBracketQuotation);
			return parsed.size() == 1 ? parsed.get(0).route : QueryRoute.WRITE;
		} catch (IllegalArgumentException e) {
			return QueryRoute.WRITE;
		}
	}

	private static DatabaseWriteAudit.Outcome auditOutcome(SqlTransactionResult.Outcome outcome) {
		return switch (outcome) {
		case COMMITTED -> DatabaseWriteAudit.Outcome.COMMITTED;
		case ROLLED_BACK -> DatabaseWriteAudit.Outcome.ROLLED_BACK;
		case UNKNOWN -> DatabaseWriteAudit.Outcome.UNKNOWN;
		};
	}

	/**
	 * The result of a one-transaction script: its {@code outcome} (COMMITTED,
	 * ROLLED_BACK or UNKNOWN), a {@code message} saying what the transaction left
	 * in the database, and its {@code statements}, one entry each in the shape the
	 * per-statement path returns. A statement whose rows were kept is a TABLE
	 * entry, rolled back or not, with {@code hasMoreRows} saying whether rows were
	 * left out. The message is also an additional return, as a success or error
	 * message.
	 */
	private static NounMetadata toTransactionResult(List<SqlScriptSplitter.Statement> statements,
			List<QueryRoute> routes, SqlTransactionResult result) {
		List<Map<String, Object>> results = new ArrayList<>(statements.size());
		for (SqlTransactionResult.StatementResult statementResult : result.getStatements()) {
			int index = statementResult.getStatement() - 1;
			Map<String, Object> entry = batchResult(index, statements.get(index).text(), routes.get(index),
					statementResult.getStatus().name(), statementResult.getTimeToRun());
			switch (statementResult.getStatus()) {
			case SUCCESS -> putRowsOrMessage(entry, statementResult, "Statement executed successfully");
			case ROLLED_BACK -> putRowsOrMessage(entry, statementResult,
					"Statement executed, then rolled back because the transaction did not commit");
			case ERROR -> {
				entry.put("type", "ERROR");
				entry.put("message", statementResult.getMessage());
			}
			case SKIPPED -> {
				entry.put("type", "SKIPPED");
				entry.put("message", "Not executed because an earlier statement failed");
			}
			}
			if (statementResult.getUpdateCount() != null) {
				entry.put("updateCount", statementResult.getUpdateCount());
			}
			if (statementResult.getSqlState() != null) {
				entry.put("sqlState", statementResult.getSqlState());
			}
			results.add(entry);
		}

		String message = switch (result.getOutcome()) {
		case COMMITTED -> "All " + statements.size() + " statements ran and were committed in one transaction";
		case ROLLED_BACK -> result.getMessage() + ". The transaction was rolled back, so no changes were kept";
		case UNKNOWN -> result.getMessage() + ". Check the database before running the script again";
		};
		Map<String, Object> output = new LinkedHashMap<>();
		output.put("outcome", result.getOutcome().name());
		output.put("message", message);
		output.put("statements", results);
		NounMetadata noun = new NounMetadata(output, PixelDataType.MAP);
		noun.addAdditionalReturn(result.getOutcome() == SqlTransactionResult.Outcome.COMMITTED
				? NounMetadata.getSuccessNounMessage(message)
				: NounMetadata.getErrorNounMessage(message));
		return noun;
	}

	/**
	 * Fills in a statement that ran: a table of the rows kept from it, or the
	 * message when no rows were kept.
	 */
	private static void putRowsOrMessage(Map<String, Object> entry, SqlTransactionResult.StatementResult result,
			String message) {
		if (result.getRows() == null) {
			entry.put("type", "MESSAGE");
			entry.put("message", message);
			return;
		}
		Map<String, Object> output = new LinkedHashMap<>();
		output.put("headers", result.getHeaders());
		output.put("values", result.getRows());
		entry.put("type", "TABLE");
		entry.put("output", output);
		entry.put("hasMoreRows", result.hasMoreRows());
	}

	private void recordWrite(IRDBMSEngine engine, String sql, int statements, int succeeded, int committedWrites,
			Throwable error) {
		DatabaseWriteAudit.record(this.insight, engine, DatabaseWriteAudit.Write.perStatement(getPixelName(), sql,
				statements, succeeded, committedWrites, error));
	}

	/**
	 * Pushes the engine to cloud storage when it keeps its data in its own folder.
	 * The SQL already committed, so a failed push is reported as a warning on the
	 * result rather than as a failed request.
	 */
	private void pushFileBasedEngine(IRDBMSEngine engine, NounMetadata result) {
		if (!engine.holdsFileLocks()) {
			return;
		}
		try {
			ClusterUtil.pushEngine(engine.getEngineId());
		} catch (Exception e) {
			classLogger.error("Failed to push database {} to cloud storage after its SQL write", engine.getEngineId(),
					e);
			result.addAdditionalReturn(NounMetadata.getWarningNounMessage(
					"The changes were saved, but the database could not be pushed to cloud storage: "
							+ e.getMessage()));
		}
	}

	private static boolean parseFlag(String value) {
		return value != null && Boolean.parseBoolean(value.trim());
	}

	/**
	 * The script's time limit.
	 *
	 * @param timeoutStr the timeout key's value
	 * @return the seconds, or null when no timeout was given
	 * @throws SemossPixelException when the value is not a positive whole number
	 */
	static Integer parseTimeout(String timeoutStr) {
		if (timeoutStr == null || timeoutStr.isBlank()) {
			return null;
		}
		double seconds;
		try {
			// pixel numbers can arrive as decimals, such as 60.0
			seconds = Double.parseDouble(timeoutStr.trim());
		} catch (NumberFormatException e) {
			seconds = -1;
		}
		if (seconds < 1 || seconds > Integer.MAX_VALUE || seconds != Math.rint(seconds)) {
			throw new SemossPixelException("The timeout must be a positive whole number of seconds");
		}
		return (int) seconds;
	}

	/**
	 * For select, create task and return it directly, much like the collect reactor
	 * 
	 * @param sqlQuery
	 * @param databaseId
	 * @param limitStr
	 * @return
	 */
	private NounMetadata executeSelectQuery(String sqlQuery, String databaseId, String limitStr) {
		try {
			HardSelectQueryStruct qs = getQs(sqlQuery, databaseId);
			// set limit if provided
			int limit = parseLimit(limitStr);
			BasicIteratorTask task = new BasicIteratorTask(qs);
			task.setNumCollect(limit);
			task.setCollectLimit(limit);
			this.insight.addQueriedDatabases(databaseId);
			return new NounMetadata(task, PixelDataType.FORMATTED_DATA_SET);
		} catch (Exception e) {
			classLogger.error("Error executing SELECT SQL query for database {}", databaseId, e);
			throw new SemossPixelException("Error executing SELECT query: " + e.getMessage());
		}
	}

	/**
	 * Runs one write through ExecQuery, which checks edit access against the level
	 * this reactor read at the start of the request rather than reading it again.
	 * This reactor records the write event and pushes the engine itself, once for
	 * the whole request.
	 *
	 * @param sqlQuery
	 * @param databaseId
	 * @param commitStr
	 * @return
	 */
	private NounMetadata executeModificationQuery(String sqlQuery, String databaseId, String commitStr) {
		try {
			HardSelectQueryStruct qs = getQs(sqlQuery, databaseId);
			// default to false if not provided
			boolean commit = commitStr != null && !commitStr.trim().isEmpty() && Boolean.parseBoolean(commitStr.trim());

			NounStore execQueryNounStore = new NounStore("ExecQuery");
			execQueryNounStore.makeGenRowStruct(PixelDataType.QUERY_STRUCT.getKey())
					.add(new NounMetadata(qs, PixelDataType.QUERY_STRUCT));
			execQueryNounStore.makeGenRowStruct("commit").add(new NounMetadata(commit, PixelDataType.BOOLEAN));

			ExecQueryReactor execReactor = new ExecQueryReactor();
			execReactor.setInsight(this.insight);
			execReactor.setNounStore(execQueryNounStore);
			execReactor.setRecordedByCaller(true);
			execReactor.setCallerLevel(databaseId, this.userLevel);

			return execReactor.execute();
		} catch (Exception e) {
			classLogger.error("Error executing SQL modification query for database {}", databaseId, e);
			throw new SemossPixelException("Error executing modification query: " + e.getMessage());
		}
	}

	/**
	 * 
	 * @param sqlQuery
	 * @param databaseId
	 * @return
	 */
	private HardSelectQueryStruct getQs(String sqlQuery, String databaseId) {
		IDatabaseEngine engine = Utility.getDatabase(databaseId);
		if (engine == null) {
			throw new SemossPixelException("Database with ID '" + databaseId + "' not found or could not be loaded");
		}

		// Create HardSelectQueryStruct for raw SQL
		HardSelectQueryStruct qs = new HardSelectQueryStruct();
		qs.setEngineId(databaseId);
		qs.setEngine(engine);
		qs.setQuery(sqlQuery);
		qs.setQsType(AbstractQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);
		return qs;
	}

	private int parseLimit(String limitStr) {
		int limit = DEFAULT_LIMIT; // default
		if (limitStr != null && !limitStr.trim().isEmpty()) {
			try {
				limit = Integer.parseInt(limitStr.trim());
			} catch (NumberFormatException e) {
				classLogger.warn("Invalid limit value: {}, using default {}", limitStr, DEFAULT_LIMIT);
			}
		}
		return limit;
	}

	@Override
	public String getReactorDescription() {
		return "Execute one or more SQL statements against a database";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.DATABASE.getKey())) {
			return "The database id";
		} else if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "Limits the number of rows retrieved by a select query, or kept from each statement with "
					+ "transaction=true";
		} else if (key.equals(TRANSACTION_KEY)) {
			return """
					When true, runs every statement in one transaction that commits once, so a failing \
					statement leaves none of the script's changes. Each statement needs the access it needs \
					without transaction=true. Returns the outcome (COMMITTED, ROLLED_BACK or UNKNOWN), a \
					message and each statement's result, with up to limit rows from each statement that \
					returns rows. A database that commits DDL as soon as it runs, such as H2, MySQL or \
					Oracle, refuses a script of more than one statement with any statement that is not a \
					query or an insert, update, delete or merge, such as CREATE TABLE, LOCK TABLES or CALL, \
					before anything runs.""";
		} else if (key.equals(TIMEOUT_KEY)) {
			return "Seconds the whole script may run with transaction=true; when reached, the script is rolled back";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
