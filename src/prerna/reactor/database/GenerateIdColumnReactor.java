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
package prerna.reactor.database;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

/**
 * Adds an ID column to a table in a database
 */
public class GenerateIdColumnReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GenerateIdColumnReactor.class);
	protected transient Logger logger;

	public GenerateIdColumnReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.TABLE.getKey(),
				ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.DATA_TYPE.getKey(),
				ReactorKeysEnum.NEW_COLUMN.getKey() };
	}

	@Override
	public NounMetadata execute() {
		// Need to
		// 1) Load data into a temp table
		// 2) Delete old table
		// 3) Rename table
		organizeKeys();
		this.logger = getLogger(this.getClass().getName());
		int stepCounter = 1;
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		if (databaseId == null || databaseId.isEmpty()) {
			throw new IllegalArgumentException("Need to add " + this.keysToGet[0]);
		}
		databaseId = testDatabaseId(databaseId, true);
		String tableName = this.keyValue.get(this.keysToGet[1]);
		if (tableName == null || tableName.isEmpty()) {
			throw new IllegalArgumentException("Need to add table name");
		}
		List<String> columns = getListValues(this.keysToGet[2]);
		List<String> types = getListValues(this.keysToGet[3]);
		if (columns.size() != types.size()) {
			throw new IllegalArgumentException("Column and data types size does not match");
		}
		String newColumn = this.keyValue.get(this.keysToGet[4]);
		if (newColumn == null || newColumn.isEmpty()) {
			throw new IllegalArgumentException("Need to add the new id column name");
		}
		IRDBMSEngine database = (IRDBMSEngine) Utility.getDatabase(databaseId);
		database.setAutoCommit(false);
		AbstractSqlQueryUtil queryUtil = database.getQueryUtil();
		Connection conn = null;

		// Step 1 query and grab data to insert into temp table
		IRawSelectWrapper iterator = getData(database, tableName, columns);
		PreparedStatement ps = null;
		// load data
		try {
			// create temp table
			String tempTable = Utility.getRandomString(8);
			// add new id column to temp table
			columns.add(newColumn);
			types.add("VARCHAR(50)");
			String[] columnNames = columns.toArray(new String[0]);
			logger.info(stepCounter + ". Creating temp table...");
			database.insertData(queryUtil.createTable(tempTable, columnNames, types.toArray(new String[0])));
			logger.info(stepCounter + ". Complete...");
			stepCounter++;

			conn = database.getConnection();
			logger.info(stepCounter + ". Loading data in temp table...");
			ps = conn.prepareStatement(queryUtil.createInsertPreparedStatementString(tempTable, columnNames));
			// align headers with types and exclude new id column
			String[] headers = iterator.getHeaders();
			while (iterator.hasNext()) {
				Object[] values = iterator.next().getRawValues();
				// add existing data
				int i = 0;
				for (; i < headers.length; i++) {
					Object value = values[i];
					String type = types.get(i);
					// this can get messy with all the casting :/
					if (Utility.isStringType(type)) {
						ps.setString(i + 1, (String) value);
					} else if (Utility.isDoubleType(type)) {
						ps.setDouble(i + 1, (double) value);
					} else if (Utility.isIntegerType(type)) {
						ps.setInt(i + 1, (int) value);
					} else if (Utility.isTimeStamp(type)) {
						ps.setTimestamp(i + 1, (Timestamp) value);
					} else if (Utility.isBoolean(type)) {
						ps.setBoolean(i + 1, (boolean) value);
					} else {
						ps.setObject(i + 1, value);
					}
				}
				// add id value
				String id = UUID.randomUUID().toString();
				ps.setString(i + 1, id);
				ps.addBatch();
			}
			ps.executeBatch();
			logger.info(stepCounter + ". Complete...");
			stepCounter++;
			// delete existing table
			logger.info(stepCounter + ". Deleting exiting table...");
			database.insertData(queryUtil.dropTable(tableName));
			logger.info(stepCounter + ". Complete...");
			stepCounter++;

			// rename temp table
			logger.info(stepCounter + ". Renaming temp table...");
			database.insertData(queryUtil.alterTableName(tempTable, tableName));
			logger.info(stepCounter + ". Complete...");
			stepCounter++;
			database.commit();
			NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
			noun.addAdditionalReturn(getSuccess("Successfully added id column: " + newColumn + " to " + tableName));
			return noun;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Unable to add id column: " + e.getMessage());
		} finally {
			if (iterator != null) {
				try {
					iterator.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}

			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	private IRawSelectWrapper getData(IRDBMSEngine database, String tableName, List<String> columns) {
		IRawSelectWrapper iterator = null;
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.setDistinct(false);
		for (String column : columns) {
			qs.addSelector(new QueryColumnSelector(tableName + "__" + column));
		}
		try {
			iterator = WrapperManager.getInstance().getRawWrapper(database, qs);
		} catch (Exception e1) {
			// error occurred querying the data
			classLogger.error(Constants.STACKTRACE, e1);
			throw new IllegalArgumentException("Unable to query columns: " + e1.getMessage());
		}
		return iterator;
	}

	private List<String> getListValues(String key) {
		List<String> values = new Vector<String>();
		GenRowStruct grs = this.store.getGenRowStruct(key);
		for (int i = 0; i < grs.size(); i++) {
			values.add(grs.get(i).toString());
		}
		if (values.isEmpty()) {
			throw new IllegalArgumentException("Need to add " + key + " values.");

		}
		return values;
	}

}
