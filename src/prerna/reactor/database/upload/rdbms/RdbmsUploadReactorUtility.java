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
package prerna.reactor.database.upload.rdbms;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class RdbmsUploadReactorUtility {

	private static final Logger classLogger = LogManager.getLogger(RdbmsUploadReactorUtility.class);

	public static final String UNIQUE_ROW_ID = "_UNIQUE_ROW_ID";

	private RdbmsUploadReactorUtility() {

	}

	/**
	 * Fill in the sqlHash with the types
	 */
	public static void createSQLTypes(Map<String, String> sqlHash) {
		sqlHash.put("DECIMAL", "FLOAT");
		sqlHash.put("DOUBLE", "FLOAT");
		sqlHash.put("STRING", "VARCHAR(2000)");
		sqlHash.put("TEXT", "VARCHAR(2000)");
		// TODO: the FE needs to differentiate between "dates with times" vs.
		// "dates"
		sqlHash.put("DATE", "DATE");
		sqlHash.put("SIMPLEDATE", "DATE");
		// currently only add in numbers as doubles
		sqlHash.put("NUMBER", "FLOAT");
		sqlHash.put("INTEGER", "FLOAT");
		sqlHash.put("BOOLEAN", "BOOLEAN");

		// TODO: standardized set of values
		sqlHash.put(SemossDataType.BOOLEAN.toString(), "BOOLEAN");
		sqlHash.put(SemossDataType.INT.toString(), "INT");
		sqlHash.put(SemossDataType.DOUBLE.toString(), "FLOAT");
		sqlHash.put(SemossDataType.STRING.toString(), "VARCHAR(2000)");
		sqlHash.put(SemossDataType.DATE.toString(), "DATE");
		sqlHash.put(SemossDataType.TIMESTAMP.toString(), "TIMESTAMP");
	}

	/**
	 * Add the metadata into the OWL
	 * 
	 * @param owlEngine
	 * @param tableName
	 * @param uniqueRowId
	 * @param headers
	 * @param sqlTypes
	 */
	public static void generateTableMetadata(WriteOWLEngine owlEngine, String tableName, String uniqueRowId,
			String[] headers, String[] sqlTypes, String[] additionalTypes) {
		// add the table
		owlEngine.addConcept(tableName, null, null);
		// add the generated column
		owlEngine.addProp(tableName, uniqueRowId, "LONG");
		// add the props
		for (int i = 0; i < headers.length; i++) {
			// NOTE ::: SQL_TYPES will have the added unique row id at index 0
			owlEngine.addProp(tableName, headers[i], sqlTypes[i + 1], additionalTypes[i]);
		}
	}

	/**
	 * Create a new table given the table name, headers, and types Returns the sql
	 * types that were generated
	 * 
	 * @param tableName
	 * @param headers
	 * @param types
	 * @throws IOException
	 */
	public static String[] createNewTable(IRDBMSEngine engine, String tableName, String uniqueRowId, String[] headers,
			SemossDataType[] types, boolean replace) throws Exception {
		// we need to add the identity column
		int size = types.length;
		String[] sqlTypes = new String[size + 1];
		String[] newHeaders = new String[size + 1];

		IRDBMSEngine rdbmsEng = engine;
		AbstractSqlQueryUtil queryUtil = rdbmsEng.getQueryUtil();
		final String BOOLEAN_DATATYPE_NAME = queryUtil.getBooleanDataTypeName();
		final String TIMESTAMP_DATATYPE_NAME = queryUtil.getDateWithTimeDataType();
		final String NUMERIC_DATATYPE_NAME = queryUtil.getDoubleDataTypeName();

		newHeaders[0] = uniqueRowId;
		sqlTypes[0] = "IDENTITY";
		for (int i = 0; i < size; i++) {
			newHeaders[i + 1] = headers[i];
			SemossDataType sType = types[i];
			if (sType == SemossDataType.STRING || sType == SemossDataType.FACTOR) {
				sqlTypes[i + 1] = "VARCHAR(2000)";
			} else if (sType == SemossDataType.INT) {
				sqlTypes[i + 1] = "INT";
			} else if (sType == SemossDataType.DOUBLE) {
				sqlTypes[i + 1] = NUMERIC_DATATYPE_NAME;
			} else if (sType == SemossDataType.DATE) {
				sqlTypes[i + 1] = "DATE";
			} else if (sType == SemossDataType.TIMESTAMP) {
				sqlTypes[i + 1] = TIMESTAMP_DATATYPE_NAME;
			} else if (sType == SemossDataType.BOOLEAN) {
				sqlTypes[i + 1] = BOOLEAN_DATATYPE_NAME;
			}
		}

		Connection conn = null;
		Statement stmt = null;
		try {
			conn = rdbmsEng.getConnection();
			stmt = conn.createStatement();
			if (replace) {
				try {
					String createTable = queryUtil.createTable(tableName, newHeaders, sqlTypes);
					stmt.execute(createTable);
				} catch (Exception e) {
					String dropTable = queryUtil.dropTable(tableName);
					stmt.execute(dropTable);
					String createTable = queryUtil.createTable(tableName, newHeaders, sqlTypes);
					stmt.execute(createTable);
				}
			} else {
				String createTable = queryUtil.createTableIfNotExists(tableName, newHeaders, sqlTypes);
				stmt.execute(createTable);
			}
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(rdbmsEng, conn, stmt, null);
		}

		return sqlTypes;
	}

	/**
	 * 
	 * @param engine
	 * @param tableName
	 * @param columnName
	 * @throws IOException
	 */
	public static void addIndex(IRDBMSEngine engine, String tableName, String columnName) throws Exception {
		String indexName = columnName.toUpperCase() + "_INDEX";
		String indexSql = "CREATE INDEX " + indexName + " ON " + tableName + "(" + columnName.toUpperCase() + ")";
		engine.insertData(indexSql);
	}

	/**
	 * Delete all the row records from the database Only runs the operation on the
	 * tables that are identified in the OWL file
	 * 
	 * @param engine
	 */
	public static void deleteRowsFromAllTables(IRDBMSEngine engine) {
		AbstractSqlQueryUtil queryUtil = engine.getQueryUtil();
		List<String> tableUris = engine.getPhysicalConcepts();
		for (String tableUri : tableUris) {
			String tableName = Utility.getInstanceName(tableUri);
			String deleteQuery = queryUtil.deleteAllRowsFromTable(tableName);
			try {
				engine.removeData(deleteQuery);
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
	}

}
