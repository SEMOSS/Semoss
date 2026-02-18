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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import prerna.algorithm.api.SemossDataType;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.reactor.AbstractReactor;
import prerna.reactor.database.upload.rdbms.RDBMSEngineCreationHelper;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.util.Constants;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

/**
 * Unified SQL Query Reactor that: 1. Parses SQL to detect query type (SELECT vs
 * modification) 2. Validates user permissions based on query type 3. Delegates
 * to appropriate existing reactors
 * 
 * Usage: SqlQuery(database=["myDb"], query=["SELECT * FROM table"],
 * limit=[100], commit=[true])
 */
public class SqlQueryReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SqlQueryReactor.class);

	private static final int DEFAULT_LIMIT = 50;
	private static final int MAX_LIMIT = 5_000;

	private enum QueryType {
		SELECT, INSERT, UPDATE, DELETE, OTHER
	}

	public SqlQueryReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.QUERY_KEY.getKey(), ReactorKeysEnum.DATABASE.getKey(),
				ReactorKeysEnum.LIMIT.getKey(), "commit" };
		this.keyRequired = new int[] { 1, 1, 0, 0, 0 };
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
		String sqlQuery = Utility.decodeURIComponent(this.keyValue.get(this.keysToGet[0]));
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
			// determine query type
			QueryType queryType = detectQueryType(sqlQuery);
			classLogger.info("Detected query type: {}", queryType);
			validateUserPermissions(user, databaseId, queryType);

			// create query structure and delegate
			return delegateToAppropriateReactor(sqlQuery, databaseId, queryType, limitStr, commitStr);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Error executing SQL query: " + e.getMessage());
		}
	}

	/**
	 * Detect SQL statement type using JSQLParser
	 */
	private QueryType detectQueryType(String sql) {
		try {
			Statement statement = CCJSqlParserUtil.parse(sql);

			if (statement instanceof Select) {
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
			classLogger.warn("Could not parse SQL statement, defaulting to OTHER type: " + e.getMessage());
			return QueryType.OTHER;
		}
	}

	/**
	 * 
	 * @param user
	 * @param databaseId
	 * @param queryType
	 */
	private void validateUserPermissions(User user, String databaseId, QueryType queryType) {
		switch (queryType) {
		case SELECT:
			// view permission
			if (!SecurityEngineUtils.userCanViewEngine(user, databaseId)) {
				throw new SemossPixelException("User does not have permission to query this database");
			}
			break;
		default:
			// any other modification queries need edit permission
			if (!SecurityEngineUtils.userCanEditEngine(user, databaseId)) {
				throw new SemossPixelException("User does not have permission to modify this database");
			}
			break;
		}
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
		}

		return executeModificationQuery(sqlQuery, databaseId, queryType, commitStr);
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
			this.insight.addQueriedDatabasesese(databaseId);
			return new NounMetadata(task, PixelDataType.FORMATTED_DATA_SET);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
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
	private NounMetadata executeModificationQuery(String sqlQuery, String databaseId, QueryType queryType,
			String commitStr) {
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

			NounMetadata result = execReactor.execute();
			if (queryType == QueryType.OTHER && isDdlQuery(sqlQuery)) {
				syncOwlAfterSchemaChange(databaseId);
			}
			return result;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Error executing modification query: " + e.getMessage());
		}
	}

	private void syncOwlAfterSchemaChange(String databaseId) {
		IDatabaseEngine engine = Utility.getDatabase(databaseId);
		if (engine == null) {
			classLogger.warn("Skipping OWL sync because engine {} could not be loaded", databaseId);
			return;
		}
		if (!(engine instanceof IRDBMSEngine)) {
			classLogger.warn("Skipping OWL sync for non-RDBMS engine {}", databaseId);
			return;
		}

		try (WriteOWLEngine owlEngine = engine.getOWLEngineFactory().getWriteOWL()) {
			Map<String, Map<String, SemossDataType>> existingMetamodel = UploadUtilities
					.getExistingMetamodel(owlEngine);
			Map<String, Map<String, String>> rawStructure = RDBMSEngineCreationHelper
					.getExistingRDBMSStructure(engine);

			Map<String, Map<String, String>> newStructure = new HashMap<>();
			rawStructure.forEach((tableName, columnMap) -> {
				String cleanTable = RDBMSEngineCreationHelper.cleanTableName(tableName);
				Map<String, String> cleanedColumns = new HashMap<>();
				columnMap.forEach((columnName, columnType) -> {
					String cleanColumn = RDBMSEngineCreationHelper.cleanTableName(columnName);
					cleanedColumns.put(cleanColumn, columnType);
				});
				newStructure.put(cleanTable, cleanedColumns);
			});

			Map<String, Set<String>> propsToReAdd = new HashMap<>();
			existingMetamodel.forEach((tableName, existingProps) -> {
				if (!newStructure.containsKey(tableName)) {
					owlEngine.removeConcept(tableName);
					return;
				}

				Map<String, String> newColumns = newStructure.get(tableName);
				existingProps.forEach((propName, existingType) -> {
					String newType = newColumns.get(propName);
					if (newType == null) {
						owlEngine.removeProp(tableName, propName);
						return;
					}
					SemossDataType newSemossType = SemossDataType.convertStringToDataType(newType);
					if (newSemossType != null && existingType != null && !newSemossType.equals(existingType)) {
						owlEngine.removeProp(tableName, propName);
						propsToReAdd.computeIfAbsent(tableName, key -> new HashSet<>()).add(propName);
					}
				});
			});

			newStructure.forEach((tableName, newColumns) -> {
				if (!existingMetamodel.containsKey(tableName)) {
					owlEngine.addConcept(tableName, null, null);
				}
				newColumns.forEach((propName, propType) -> {
					boolean hasProp = existingMetamodel.containsKey(tableName)
							&& existingMetamodel.get(tableName).containsKey(propName);
					boolean needsReAdd = propsToReAdd.containsKey(tableName)
							&& propsToReAdd.get(tableName).contains(propName);
					if (!hasProp || needsReAdd) {
						owlEngine.addProp(tableName, propName, propType, null, null);
					}
				});
			});

			owlEngine.commit();
			owlEngine.export();
			ClusterUtil.pushOwl(databaseId, owlEngine);
		} catch (Exception e) {
			classLogger.warn("Failed to sync OWL after schema change: {}", e.getMessage());
			classLogger.error(Constants.STACKTRACE, e);
			return;
		}

		try {
			Utility.synchronizeEngineMetadata(databaseId);
		} catch (Exception e) {
			classLogger.warn("Failed to sync master metadata after schema change: {}", e.getMessage());
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

	private boolean isDdlQuery(String sqlQuery) {
		String normalized = sqlQuery.trim().toUpperCase();
		return normalized.startsWith("CREATE ") || normalized.startsWith("ALTER ")
				|| normalized.startsWith("DROP ") || normalized.startsWith("TRUNCATE ")
				|| normalized.startsWith("RENAME ");
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
					classLogger.warn("Non-positive limit value: " + limit + ", using default " + DEFAULT_LIMIT);
					limit = DEFAULT_LIMIT;
				} else if (limit > MAX_LIMIT) {
					classLogger.warn("Limit value " + limit + " exceeds maximum " + MAX_LIMIT + ", using maximum");
					limit = MAX_LIMIT;
				}

			} catch (NumberFormatException e) {
				classLogger.warn("Invalid limit value: " + limitStr + ", using default " + DEFAULT_LIMIT);
			}
		}

		return limit;
	}

	@Override
	public String getReactorDescription() {
		return "Execute a SQL query against a database with pagination support (limit and offset)";
	}

}