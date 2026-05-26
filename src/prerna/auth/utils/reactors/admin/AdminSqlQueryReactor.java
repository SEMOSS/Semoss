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
package prerna.auth.utils.reactors.admin;

import java.util.Locale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.DescribeStatement;
import net.sf.jsqlparser.statement.ExplainStatement;
import net.sf.jsqlparser.statement.ShowColumnsStatement;
import net.sf.jsqlparser.statement.ShowStatement;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

/**
 * Admin version of SqlQueryReactor.
 *
 * This reactor: Only checks whether the user is currently in admin mode. Allows
 * all SQL types (SELECT + INSERT/UPDATE/DELETE + CREATE/ALTER/DROP).
 */
public class AdminSqlQueryReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AdminSqlQueryReactor.class);

	private static final int DEFAULT_LIMIT = 50;
	private static final int MAX_LIMIT = 5_000;

	private enum QueryType {
		SELECT, INSERT, UPDATE, DELETE, OTHER
	}

	public AdminSqlQueryReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.QUERY_KEY.getKey(), ReactorKeysEnum.DATABASE.getKey(),
				ReactorKeysEnum.LIMIT.getKey(), "commit" };
		this.keyRequired = new int[] { 1, 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(this.insight.getUser());
		if (adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}

		organizeKeys();
		String sqlQuery = this.keyValue.get(this.keysToGet[0]);
		String databaseId = this.keyValue.get(this.keysToGet[1]);
		String limitStr = this.keyValue.get(this.keysToGet[2]);
		String commitStr = this.keyValue.get(this.keysToGet[3]);

		if (sqlQuery == null || sqlQuery.trim().isEmpty()) {
			throw new SemossPixelException("SQL query cannot be empty");
		}

		if (databaseId == null || databaseId.trim().isEmpty()) {
			throw new SemossPixelException("Database id is required");
		}

		try {
			QueryType queryType = detectQueryType(sqlQuery);
			classLogger.info("Admin SQL Query type detected: {}", queryType);
			return delegateToAppropriateReactor(sqlQuery, databaseId, queryType, limitStr, commitStr);
		} catch (Exception e) {
			classLogger.error("Unable to process the admin SQL query request.", e);
			throw new SemossPixelException("Error executing admin SQL query: " + e.getMessage());
		}
	}

	/**
	 * Detect SQL statement type using JSQLParser
	 */
	private QueryType detectQueryType(String sql) {
		try {
			Statement statement = CCJSqlParserUtil.parse(sql);

			if (statement instanceof Select || statement instanceof ShowStatement
					|| statement instanceof ShowColumnsStatement || statement instanceof DescribeStatement
					|| statement instanceof ExplainStatement) {
				return QueryType.SELECT;
			} else if (statement instanceof Insert) {
				return QueryType.INSERT;
			} else if (statement instanceof Update) {
				return QueryType.UPDATE;
			} else if (statement instanceof Delete) {
				return QueryType.DELETE;
			} else {
				// For CREATE, ALTER, DROP, etc.
				return QueryType.OTHER;
			}
		} catch (Exception e) {
			classLogger.warn("Could not parse SQL statement, using keyword fallback: {}", e.getMessage());
			return detectQueryTypeFromKeyword(sql);
		}
	}

	/**
	 * Fallback when parser fails for dialect-specific read-only queries.
	 */
	private QueryType detectQueryTypeFromKeyword(String sql) {
		if (sql == null) {
			return QueryType.OTHER;
		}

		String normalizedSql = sql.trim().toLowerCase(Locale.ROOT);
		if (normalizedSql.startsWith("select ") || normalizedSql.equals("select") || normalizedSql.startsWith("with ")
				|| normalizedSql.equals("with") || normalizedSql.startsWith("show ") || normalizedSql.equals("show")
				|| normalizedSql.startsWith("describe ") || normalizedSql.equals("describe")
				|| normalizedSql.startsWith("desc ") || normalizedSql.equals("desc")
				|| normalizedSql.startsWith("explain ") || normalizedSql.equals("explain")) {
			return QueryType.SELECT;
		}

		return QueryType.OTHER;
	}

	/**
	 * 
	 * @param sqlQuery
	 * @param databaseId
	 * @param queryType
	 * @param limitStr
	 * @param commitStr
	 * @return
	 */
	private NounMetadata delegateToAppropriateReactor(String sqlQuery, String databaseId, QueryType queryType,
			String limitStr, String commitStr) {

		if (queryType == QueryType.SELECT) {
			return executeSelectQuery(sqlQuery, databaseId, limitStr);
		} else {
			return executeModificationQuery(sqlQuery, databaseId, commitStr);
		}
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
			classLogger.error("Unable to execute the admin SELECT query.", e);
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

			AdminExecQueryReactor execReactor = new AdminExecQueryReactor();
			execReactor.setInsight(this.insight);
			execReactor.setNounStore(execQueryNounStore);

			return execReactor.execute();
		} catch (Exception e) {
			classLogger.error("Unable to execute the admin data-modification query.", e);
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
		IDatabaseEngine engine = null;
		if (SystemEngineRegistry.isSystemEngine(databaseId)) {
			engine = SystemEngineRegistry.getSystemEngine(databaseId);
		} else {
			engine = Utility.getDatabase(databaseId);
		}
		if (engine == null) {
			throw new SemossPixelException("Database with ID '" + databaseId + "' not found or could not be loaded");
		}

		// Create HardSelectQueryStruct for raw SQL
		HardSelectQueryStruct qs = new HardSelectQueryStruct();
		qs.setEngineId(databaseId);
		qs.setEngine(engine);
		qs.setQuery(sqlQuery);
		qs.setQsType(QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);
		return qs;
	}

	/**
	 * Parse and validate limit parameter
	 * 
	 * @param limitStr
	 * @return
	 */
	private int parseLimit(String limitStr) {
		int limit = DEFAULT_LIMIT; // default

		if (limitStr != null && !limitStr.trim().isEmpty()) {
			try {
				limit = Integer.parseInt(limitStr.trim());

				if (limit <= 0) {
					classLogger.warn("Non-positive limit value: {}, using default {}", limit, DEFAULT_LIMIT);
					limit = DEFAULT_LIMIT;
				} else if (limit > MAX_LIMIT) {
					classLogger.warn("Limit value {} exceeds maximum {}, using maximum", limit, MAX_LIMIT);
					limit = MAX_LIMIT;
				}

			} catch (NumberFormatException e) {
				classLogger.warn("Invalid limit value: {}, using default {}", limitStr, DEFAULT_LIMIT);
			}
		}

		return limit;
	}

	@Override
	public String getReactorDescription() {
		return "Admin-only SQL query execution with pagination support (limit and offset), bypassing normal permission checks";
	}

}
