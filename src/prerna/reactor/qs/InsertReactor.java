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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.date.SemossDate;
import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.DatabaseWriteAudit;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;

public class InsertReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(InsertReactor.class);

	private static final String REACTOR_NAME = "Insert";

	private NounMetadata qStruct = null;

	public InsertReactor() {
		this.keysToGet = new String[] { "into", "values", "commit", ReactorKeysEnum.CUSTOM_SUCCESS_MESSAGE.getKey() };
	}

	/*
	 * Pixel can contain a <UUID> value which will be replaced with
	 * UUID.randomUUID() Pixel can also contain <USER_ID> value which will be
	 * replaced with the main user id upon inserting into the database
	 */

	@Override
	public NounMetadata execute() {
		if (qStruct == null) {
			qStruct = getQueryStruct();
		}

		AbstractQueryStruct qs = (AbstractQueryStruct) qStruct.getValue();
		IRDBMSEngine database = null;
		ITableDataFrame frame = null;
		AbstractSqlQueryUtil queryUtil = null;
		String userId = "user not defined";

		if (!(qStruct.getValue() instanceof AbstractQueryStruct)) {
			throw new IllegalArgumentException("Input to exec query requires a query struct");
		}

		qs = ((AbstractQueryStruct) qStruct.getValue());
		if (qs.getQsType() == AbstractQueryStruct.QUERY_STRUCT_TYPE.ENGINE) {
			IDatabaseEngine engine = qs.retrieveQueryStructEngine();
			if (!(engine instanceof IRDBMSEngine)) {
				throw new IllegalArgumentException("Insert query only works for rdbms databases");
			}
			database = (IRDBMSEngine) engine;
			queryUtil = database.getQueryUtil();
			// the user id fills any <USER_ID> value in an insert
			User user = this.insight.getUser();
			if (user != null) {
				userId = user.getAccessToken(user.getLogins().get(0)).getId();
			}

			// check that the user can edit the engine
			if (!SecurityEngineUtils.userCanEditEngine(user, database.getEngineId())) {
				throw new IllegalArgumentException("User does not have permission to insert query for this database");
			}
		} else if (qs.getQsType() == AbstractQueryStruct.QUERY_STRUCT_TYPE.FRAME) {
			frame = qs.getFrame();
			if (!(frame instanceof AbstractRdbmsFrame)) {
				throw new IllegalArgumentException("Insert query only works for sql frames");
			}
			queryUtil = ((AbstractRdbmsFrame) frame).getQueryUtil();
		}

		StringBuilder prefixSb = new StringBuilder("INSERT INTO ");

		GenRowStruct colGrs = this.store.getGenRowStruct("into");
		GenRowStruct valGrs = this.store.getGenRowStruct("values");
		GenRowStruct commitGrs = this.store.getGenRowStruct("commit");
		Boolean commit = false;
		if (commitGrs != null && !commitGrs.isEmpty()) {
			commit = Boolean.parseBoolean(commitGrs.get(0) + "");
		}

		List<IQuerySelector> selectors = new Vector<>();
		for (int i = 0; i < colGrs.size(); i++) {
			String s = colGrs.get(i).toString();
			selectors.add(new QueryColumnSelector(s));
		}

		if (frame != null) {
			qs.setSelectors(selectors);
			qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, frame.getMetaData());
			selectors = qs.getSelectors();
		}

		// Insert table name
		QueryColumnSelector t = (QueryColumnSelector) selectors.get(0);
		prefixSb.append(t.getTable()).append(" (");

		// Insert columns
		for (int i = 0; i < selectors.size(); i++) {
			QueryColumnSelector c = (QueryColumnSelector) selectors.get(i);
			if (i > 0) {
				prefixSb.append(", ");
			}
			if (c.getColumn().equals(AbstractQueryStruct.PRIM_KEY_PLACEHOLDER)) {
				prefixSb.append(getPrimKey(database, c.getTable()));
			} else {
				prefixSb.append(c.getColumn());
			}
		}
		prefixSb.append(") VALUES (");

		String initial = prefixSb.toString();
		List<Object[]> valueCombinations = flattenCombinations(valGrs);

		// h2/sqlite is file based
		// so need to do the whole sync across engines
		// only need for cloud
		if (ClusterUtil.IS_CLUSTER && database != null) {
			RdbmsTypeEnum eType = database.getQueryUtil().getDbType();
			if (eType == RdbmsTypeEnum.H2_DB || eType == RdbmsTypeEnum.SQLITE) {
				insertFileEngine(database, queryUtil, initial, valueCombinations, userId);
				NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.ALTER_DATABASE,
						PixelOperationType.FORCE_SAVE_DATA_TRANSFORMATION);
				String customSuccessMessage = getCustomSuccessMessage();
				if (customSuccessMessage != null && !customSuccessMessage.isEmpty()) {
					noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage(customSuccessMessage));
				}
				return noun;
			}
		}

		// determine if we can insert booleans as true/false
		boolean allowBooleanType = queryUtil.allowBooleanDataType();

		// the inserts sent to the database, for its write event
		List<String> sentQueries = new ArrayList<>();
		// we are a frame
		// or a tcp/ip rdbms type
		for (Object[] values : valueCombinations) {
			StringBuilder valuesSb = new StringBuilder();
			// Insert values
			for (int i = 0; i < values.length; i++) {
				// add comma for next value
				if (i != 0) {
					valuesSb.append(", ");
				}

				// append the value
				if (values[i] == null) {
					valuesSb.append("NULL");
				} else if (values[i] instanceof String) {
					if (values[i].equals("<UUID>")) {
						valuesSb.append(
								"'" + AbstractSqlQueryUtil.escapeForSQLStatement(UUID.randomUUID().toString()) + "'");
					} else if (values[i].equals("<USER_ID>")) {
						valuesSb.append("'" + AbstractSqlQueryUtil.escapeForSQLStatement(userId) + "'");
					} else {
						valuesSb.append("'" + AbstractSqlQueryUtil.escapeForSQLStatement(values[i] + "") + "'");
					}
				} else if (values[i] instanceof SemossDate) {
					String dateValue = ((SemossDate) values[i]).getFormattedDate();
					if (dateValue == null || dateValue.isEmpty() || dateValue.equals("null")) {
						valuesSb.append("NULL, ");
					} else {
						valuesSb.append("'" + ((SemossDate) values[i]).getFormattedDate() + "'");
					}
				} else if (values[i] instanceof Boolean) {
					if (allowBooleanType) {
						valuesSb.append(values[i]);
					} else {
						// append 1 or 0 based on true/false
						if (Boolean.parseBoolean(values[i] + "")) {
							valuesSb.append(1);
						} else {
							valuesSb.append(0);
						}
					}
				} else {
					valuesSb.append(values[i]);
				}
			}
			valuesSb.append(")");

			String query = initial + valuesSb.toString();
			classLogger.info("SQL QUERY....{}", query);
			if (qs.getQsType() == AbstractQueryStruct.QUERY_STRUCT_TYPE.ENGINE) {
				if (database == null) {
					throw new NullPointerException("No engine passed in to insert the data");
				}
				int inserted = sentQueries.size();
				sentQueries.add(query);
				try {
					database.insertData(query);
					if (commit) {
						database.commit();
					}
				} catch (Exception e) {
					classLogger.error("Failed to insert new records into the database with query {}", query, e);
					recordInsert(database, sentQueries, valueCombinations.size(), inserted, e);
					throw new SemossPixelException(
							new NounMetadata("An error occurred trying to insert new records in the database",
									PixelDataType.CONST_STRING, PixelOperationType.ERROR));
				}
			} else {
				try {
					if (frame != null) {
						((AbstractRdbmsFrame) frame).getBuilder().runQuery(query);
					}
				} catch (Exception e) {
					classLogger.error("Failed to insert new records into the frame with query {}", query, e);
					throw new SemossPixelException(
							new NounMetadata("An error occurred trying to insert new records in the frame",
									PixelDataType.CONST_STRING, PixelOperationType.ERROR));
				}
			}
		}
		if (database != null && !sentQueries.isEmpty()) {
			recordInsert(database, sentQueries, sentQueries.size(), sentQueries.size(), null);
		}

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.ALTER_DATABASE,
				PixelOperationType.FORCE_SAVE_DATA_TRANSFORMATION);
		String customSuccessMessage = getCustomSuccessMessage();
		if (customSuccessMessage != null && !customSuccessMessage.isEmpty()) {
			noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage(customSuccessMessage));
		}
		return noun;
	}

	/**
	 * Insert into the engine
	 * 
	 * @param database
	 * @param initial
	 * @param valueCombinations
	 * @param userId
	 */
	private void insertFileEngine(IRDBMSEngine database, AbstractSqlQueryUtil queryUtil, String initial,
			List<Object[]> valueCombinations, String userId) {
		synchronized (database) {
			ClusterUtil.pullEngine(database.getEngineId());

			// determine if we can insert booleans as true/false
			boolean allowBooleanType = queryUtil.allowBooleanDataType();

			// the inserts sent to the database, for its write event
			List<String> sentQueries = new ArrayList<>();
			for (Object[] values : valueCombinations) {
				StringBuilder valuesSb = new StringBuilder();
				// insert values
				for (int i = 0; i < values.length; i++) {
					// add comma for next value
					if (i != 0) {
						valuesSb.append(", ");
					}

					// append the value
					if (values[i] == null) {
						valuesSb.append("NULL");
					} else if (values[i] instanceof String) {
						if (values[i].equals("<UUID>")) {
							valuesSb.append("'"
									+ AbstractSqlQueryUtil.escapeForSQLStatement(UUID.randomUUID().toString()) + "'");
						} else if (values[i].equals("<USER_ID>")) {
							valuesSb.append("'" + AbstractSqlQueryUtil.escapeForSQLStatement(userId) + "'");
						} else {
							valuesSb.append("'" + AbstractSqlQueryUtil.escapeForSQLStatement(values[i] + "") + "'");
						}
					} else if (values[i] instanceof SemossDate) {
						String dateValue = ((SemossDate) values[i]).getFormattedDate();
						if (dateValue == null || dateValue.isEmpty() || dateValue.equals("null")) {
							valuesSb.append("NULL, ");
						} else {
							valuesSb.append("'" + ((SemossDate) values[i]).getFormattedDate() + "'");
						}
					} else if (values[i] instanceof Boolean) {
						if (allowBooleanType) {
							valuesSb.append(values[i]);
						} else {
							// append 1 or 0 based on true/false
							if (Boolean.parseBoolean(values[i] + "")) {
								valuesSb.append(1);
							} else {
								valuesSb.append(0);
							}
						}
					} else {
						valuesSb.append(values[i]);
					}
				}
				valuesSb.append(")");
				// generate the query
				String query = initial + valuesSb.toString();

				int inserted = sentQueries.size();
				sentQueries.add(query);
				try {
					database.insertData(query);
				} catch (Exception e) {
					classLogger.error("Failed to insert new records into the database with query {}", query, e);
					recordInsert(database, sentQueries, valueCombinations.size(), inserted, e);
					throw new SemossPixelException(
							new NounMetadata("An error occurred trying to insert new records in the database",
									PixelDataType.CONST_STRING, PixelOperationType.ERROR));
				}
			}
			if (!sentQueries.isEmpty()) {
				recordInsert(database, sentQueries, sentQueries.size(), sentQueries.size(), null);
			}
			// push back to the cluster
			ClusterUtil.pushEngine(database.getEngineId());
		}
	}

	/**
	 * Records the inserts sent to a database as one write event. Each insert
	 * commits on its own, so the inserts before a failure stay in the database.
	 *
	 * @param database    the database inserted into
	 * @param sentQueries the inserts sent, including the one that failed
	 * @param statements  the number of inserts the reactor was asked to run
	 * @param inserted    the number that succeeded
	 * @param error       the failure that stopped the inserts, or null
	 */
	private void recordInsert(IRDBMSEngine database, List<String> sentQueries, int statements, int inserted,
			Exception error) {
		// the SQL is only hashed for an engine that keeps its input, so it is joined
		// only
		// then
		String sql = database.keepInputOutput() ? String.join(";\n", sentQueries) : null;
		DatabaseWriteAudit.record(this.insight, database,
				DatabaseWriteAudit.Write.perStatement(REACTOR_NAME, sql, statements, inserted, inserted, error));
	}

	private NounMetadata getQueryStruct() {
		GenRowStruct allNouns = getNounStore().getGenRowStruct(PixelDataType.QUERY_STRUCT.getKey());
		NounMetadata queryStruct = null;
		if (allNouns != null) {
			return allNouns.getNoun(0);
		}
		return queryStruct;
	}

	private List<Object[]> flattenCombinations(GenRowStruct valGrs) {
		List<Object[]> combinations = new Vector<>();

		Map<Integer, Integer> currIndexMap = new HashMap<>();

		int numInputs = valGrs.size();
		boolean moreCombinations = true;
		while (moreCombinations) {
			Object[] row = new Object[numInputs];
			for (int i = 0; i < numInputs; i++) {

				Object thisValue = null;
				Object result = valGrs.get(i);
				if (result instanceof List) {
					// if we know which index to grab, lets just grab it
					if (currIndexMap.containsKey(Integer.valueOf(i))) {
						Integer indexToGrab = currIndexMap.get(Integer.valueOf(i));
						thisValue = ((List) result).get(indexToGrab);
					} else {
						thisValue = ((List) result).get(0);
						currIndexMap.put(Integer.valueOf(i), Integer.valueOf(0));
					}
				} else {
					thisValue = result;
				}

				// set the value into the current row
				// if this is an array of more than 1 value
				// then it is a list of noun metadatas
				// and we need to get the value
				if (thisValue instanceof NounMetadata) {
					row[i] = ((NounMetadata) thisValue).getValue();
				} else {
					row[i] = thisValue;
				}
			}
			combinations.add(row);

			// now we need to know if we should update curr index map
			// or if we are done
			boolean loopAgain = false;
			UPDATE_LOOP: for (int i = numInputs - 1; i >= 0; i--) {
				// we start at the last list
				// and see if the current index is at the end
				Object result = valGrs.get(i);
				if (result instanceof List) {
					Integer indexToGrab = currIndexMap.get(Integer.valueOf(i));
					int numIndicesToGrab = ((List) result).size();
					if ((indexToGrab + 1) == numIndicesToGrab) {
						// we are have iterated through all of this guy
						// so let us reset him
						// BUT, this doesn't mean we know we need to loop again
						// i am just preparing for the case where a list above requires us to start
						// and loop through all the last pieces
						currIndexMap.put(Integer.valueOf(i), Integer.valueOf(0));
					} else {
						// we have not looped through everything in this list
						// we need to loop again
						// after i increase the index to grab
						currIndexMap.put(Integer.valueOf(i), Integer.valueOf(indexToGrab.intValue() + 1));
						loopAgain = true;
						break UPDATE_LOOP;
					}
				}
			}

			moreCombinations = loopAgain;
		}

		return combinations;
	}

	public String getCustomSuccessMessage() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.CUSTOM_SUCCESS_MESSAGE.getKey());
		if (grs != null && !grs.isEmpty()) {
			return (String) grs.get(0);
		}

		return null;
	}

	/**
	 * Get the primary key for a table
	 * 
	 * @param engine
	 * @param tableName
	 * @return
	 */
	private String getPrimKey(IDatabaseEngine engine, String tableName) {
		String physicalUri = engine.getPhysicalUriFromPixelSelector(tableName);
		return engine.getLegacyPrimKey4Table(physicalUri);
	}

	@Override
	public String getName() {
		return "Insert";
	}

}
