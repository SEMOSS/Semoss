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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.statement.values.ValuesStatement;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
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
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;

public abstract class AbstractSqlQueryReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AbstractSqlQueryReactor.class);

	private static final int DEFAULT_LIMIT = 50;

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
				ReactorKeysEnum.LIMIT.getKey(), "commit" };
		this.keyRequired = new int[] { 1, 1, 0, 0 };
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

		if (sqlQuery == null || sqlQuery.trim().isEmpty()) {
			throw new SemossPixelException("SQL query cannot be empty");
		}

		if (databaseId == null || databaseId.trim().isEmpty()) {
			throw new SemossPixelException("Database id is required");
		}

		IEngine engine = Utility.getEngine(databaseId);
		if (!(engine instanceof IRDBMSEngine)) {
			throw new IllegalArgumentException("The database is not a RDBMS engine that accepts SQL");
		}
		IRDBMSEngine rdbmsEngine = (IRDBMSEngine) engine;

		try {
			List<ParsedSqlStatement> statements = parseQueryStatements(sqlQuery,
					usesSquareBracketQuotation(rdbmsEngine));
			for (ParsedSqlStatement statement : statements) {
				classLogger.info("Detected SQL query route: {}", statement.route);
				validateUserPermissions(user, databaseId, statement.route);
			}

			if (statements.size() == 1) {
				return delegateToAppropriateReactor(sqlQuery, databaseId, statements.get(0).route, limitStr, commitStr);
			}
			return executeBatch(statements, databaseId, limitStr, commitStr);
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
			if (statements.getStatements().isEmpty()) {
				throw new IllegalArgumentException("SQL query does not contain an executable statement");
			}
			List<ParsedSqlStatement> parsed = new ArrayList<>(statements.getStatements().size());
			for (Statement statement : statements.getStatements()) {
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
				|| statement instanceof DescribeStatement || statement instanceof ExplainStatement
				|| statement instanceof ValuesStatement) {
			return QueryRoute.READ;
		}
		return QueryRoute.WRITE;
	}

	private static QueryRoute routeSelect(Select select) {
		QueryRoute route = routeSelectBody(select.getSelectBody());
		if (select.getWithItemsList() != null) {
			for (WithItem withItem : select.getWithItemsList()) {
				route = stricterRoute(route, routeSelectBody(withItem));
			}
		}
		return route;
	}

	private static QueryRoute routeSelectBody(SelectBody selectBody) {
		if (selectBody == null) {
			throw new IllegalArgumentException("SQL SELECT has no query body");
		}
		if (selectBody instanceof PlainSelect) {
			PlainSelect select = (PlainSelect) selectBody;
			if (select.getIntoTables() != null && !select.getIntoTables().isEmpty()) {
				return QueryRoute.WRITE;
			}
			return select.isForUpdate() ? QueryRoute.LOCKING_READ : QueryRoute.READ;
		}
		if (selectBody instanceof SetOperationList) {
			QueryRoute route = QueryRoute.READ;
			for (SelectBody child : ((SetOperationList) selectBody).getSelects()) {
				route = stricterRoute(route, routeSelectBody(child));
			}
			return route;
		}
		if (selectBody instanceof WithItem) {
			return routeSelectBody(((WithItem) selectBody).getSelectBody());
		}
		if (selectBody instanceof ValuesStatement) {
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
	 * 
	 * @param user
	 * @param databaseId
	 * @param queryRoute
	 */
	private void validateUserPermissions(User user, String databaseId, QueryRoute queryRoute) {
		if (queryRoute.requiresEdit) {
			if (!SecurityEngineUtils.userCanEditEngine(user, databaseId)) {
				throw new SemossPixelException("User does not have permission to modify this database");
			}
		} else if (!SecurityEngineUtils.userCanViewEngine(user, databaseId)) {
			throw new SemossPixelException("User does not have permission to query this database");
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

	private NounMetadata executeBatch(List<ParsedSqlStatement> statements, String databaseId, String limitStr,
			String commitStr) {
		List<Map<String, Object>> results = new ArrayList<>(statements.size());
		for (int index = 0; index < statements.size(); index++) {
			ParsedSqlStatement statement = statements.get(index);
			long startedAt = System.nanoTime();
			try {
				NounMetadata execution = delegateToAppropriateReactor(statement.sql, databaseId, statement.route,
						limitStr, commitStr);
				results.add(toBatchSuccess(index, statement, execution, startedAt));
			} catch (Exception e) {
				results.add(toBatchError(index, statement, e, elapsedMillis(startedAt)));
				for (int skippedIndex = index + 1; skippedIndex < statements.size(); skippedIndex++) {
					results.add(toBatchSkipped(skippedIndex, statements.get(skippedIndex)));
				}
				break;
			}
		}
		return new NounMetadata(results, PixelDataType.VECTOR, PixelOperationType.VECTOR);
	}

	private Map<String, Object> toBatchSuccess(int index, ParsedSqlStatement statement, NounMetadata execution,
			long startedAt) throws Exception {
		Map<String, Object> result = batchResult(index, statement, "SUCCESS", 0);
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
		Map<String, Object> result = batchResult(index, statement, "ERROR", timeToRun);
		result.put("type", "ERROR");
		result.put("message",
				exception.getMessage() == null ? exception.getClass().getSimpleName() : exception.getMessage());
		return result;
	}

	private static Map<String, Object> toBatchSkipped(int index, ParsedSqlStatement statement) {
		Map<String, Object> result = batchResult(index, statement, "SKIPPED", 0);
		result.put("type", "SKIPPED");
		result.put("message", "Not executed because an earlier statement failed");
		return result;
	}

	private static Map<String, Object> batchResult(int index, ParsedSqlStatement statement, String status,
			long timeToRun) {
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("statement", index + 1);
		result.put("query", statement.sql);
		result.put("route", statement.route.name());
		result.put("status", status);
		result.put("timeToRun", timeToRun);
		return result;
	}

	private static long elapsedMillis(long startedAt) {
		return (System.nanoTime() - startedAt) / 1_000_000;
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
			return "Limits the number of rows retrieved by a select query";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
