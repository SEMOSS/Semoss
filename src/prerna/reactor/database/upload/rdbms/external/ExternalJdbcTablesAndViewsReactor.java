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
package prerna.reactor.database.upload.rdbms.external;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

public class ExternalJdbcTablesAndViewsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ExternalJdbcTablesAndViewsReactor.class);

	private static final String CLASS_NAME = ExternalJdbcTablesAndViewsReactor.class.getName();

	public ExternalJdbcTablesAndViewsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.CONNECTION_DETAILS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);

		Map<String, Object> connectionDetails = getConDetails();
		if (connectionDetails != null) {
			String host = (String) connectionDetails.get(AbstractSqlQueryUtil.HOSTNAME);
			if (host != null) {
				String testUpdatedHost = this.insight.getAbsoluteInsightFolderPath(host);
				if (new File(testUpdatedHost).exists()) {
					host = testUpdatedHost;
					connectionDetails.put(AbstractSqlQueryUtil.HOSTNAME, host);
				}
			}
		}

		String driver = (String) connectionDetails.get(Constants.RDBMS_TYPE);
		if (driver == null) {
			driver = (String) connectionDetails.get(AbstractSqlQueryUtil.DRIVER_NAME);
		}
		RdbmsTypeEnum driverEnum = RdbmsTypeEnum.getEnumFromString(driver);
		AbstractSqlQueryUtil queryUtil = SqlQueryUtilFactory.initialize(driverEnum);

		Connection con = null;
		DatabaseMetaData meta = null;
		Statement tableStmt = null;
		ResultSet tablesRs = null;
		try {
			String connectionUrl = null;
			try {
				connectionUrl = queryUtil.setConnectionDetailsfromMap(connectionDetails);
			} catch (RuntimeException e) {
				throw new SemossPixelException(
						new NounMetadata("Unable to generation connection url with message " + e.getMessage(),
								PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			}

			try {
				con = AbstractSqlQueryUtil.makeConnection(queryUtil, connectionUrl, connectionDetails);
			} catch (SQLException e) {
				classLogger.error("Failed to establish JDBC connection using driver {}", driverEnum, e);
				String driverError = e.getMessage();
				String errorMessage = "Unable to establish connection given the connection details.\nDriver produced error: \" ";
				errorMessage += driverError;
				errorMessage += " \"";
				throw new SemossPixelException(
						new NounMetadata(errorMessage, PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			}

			try {
				meta = con.getMetaData();
			} catch (SQLException e) {
				classLogger.error("Failed to retrieve database metadata from JDBC connection", e);
				throw new SemossPixelException(new NounMetadata("Unable to get the database metadata",
						PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			}

			String catalogFilter = queryUtil.getDatabaseMetadataCatalogFilter();
			if (catalogFilter == null) {
				try {
					catalogFilter = con.getCatalog();
				} catch (SQLException e) {
					// we can ignore this
					classLogger.error(
							"Failed to retrieve catalog from JDBC connection; continuing without catalog filter", e);
				}
			}

			String schemaFilter = queryUtil.getDatabaseMetadataSchemaFilter();
			if (schemaFilter == null) {
				schemaFilter = (String) connectionDetails.get(AbstractSqlQueryUtil.SCHEMA);
			}
			if (schemaFilter == null) {
				schemaFilter = RdbmsConnectionHelper.getSchema(meta, con, connectionUrl, driverEnum);
			}

			try {
				tableStmt = con.createStatement();
				tablesRs = RdbmsConnectionHelper.getTables(con, tableStmt, meta, catalogFilter, schemaFilter,
						driverEnum);
			} catch (SQLException e) {
				classLogger.error("Failed to fetch tables and views from database metadata (catalog={}, schema={})",
						catalogFilter, schemaFilter, e);
				throw new SemossPixelException(new NounMetadata("Unable to get tables and views from database metadata",
						PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			}

			// keep a list of tables and views
			List<String> tableSchemas = new ArrayList<String>();
			List<String> tables = new ArrayList<String>();
			List<String> viewSchemas = new ArrayList<String>();
			List<String> views = new ArrayList<String>();

			String[] tableKeys = RdbmsConnectionHelper.getTableKeys(driverEnum);
			final String TABLE_NAME_STR = tableKeys[0];
			final String TABLE_TYPE_STR = tableKeys[1];
			final String TABLE_SCHEMA_STR = tableKeys[2];
			try {
				while (tablesRs.next()) {
					String table = tablesRs.getString(TABLE_NAME_STR);
					// this will be table or view
					String tableType = tablesRs.getString(TABLE_TYPE_STR);
					if (tableType != null) {
						tableType = tableType.toUpperCase();
					}
					// get schema
					String tableSchema = null;
					if (driverEnum.equals(RdbmsTypeEnum.ORACLE)) {
						tableSchema = meta.getUserName();
					} else {
						tableSchema = tablesRs.getString(TABLE_SCHEMA_STR);
					}
					if (tableSchema != null) {
						tableSchema = tableSchema.toUpperCase();
					}
					if (tableType.toUpperCase().contains("TABLE")) {
						logger.info("Found table = {}", Utility.cleanLogString(table));
						tables.add(table);
						tableSchemas.add(tableSchema);
					} else {
						// there may be views built from sys or information schema
						// we want to ignore these
						logger.info("Found view = {}", Utility.cleanLogString(table));
						views.add(table);
						viewSchemas.add(tableSchema);
					}
				}
			} catch (SQLException e) {
				classLogger.error("Error iterating over tables/views result set from database metadata", e);
			}

			logger.info("Done parsing database metadata");

			Map<String, List<String>> ret = new HashMap<String, List<String>>();
			ret.put("tables", tables);
			ret.put("tableSchemas", tableSchemas);
			ret.put("views", views);
			ret.put("viewSchemas", viewSchemas);
			return new NounMetadata(ret, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} finally {
			closeAutoClosable(tablesRs, logger);
			closeAutoClosable(tableStmt, logger);
			closeAutoClosable(con, logger);
		}
	}

	/**
	 * Close a connection, statement, or result set
	 * 
	 * @param closeable
	 */
	private void closeAutoClosable(AutoCloseable closeable, Logger logger) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (Exception e) {
				logger.error("Failed to close JDBC resource {}", closeable.getClass().getSimpleName(), e);
			}
		}
	}

	private Map<String, Object> getConDetails() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.CONNECTION_DETAILS.getKey());
		if (grs != null && !grs.isEmpty()) {
			List<Object> mapInput = grs.getValuesOfType(PixelDataType.MAP);
			if (mapInput != null && !mapInput.isEmpty()) {
				return (Map<String, Object>) mapInput.get(0);
			}
		}

		List<Object> mapInput = grs.getValuesOfType(PixelDataType.MAP);
		if (mapInput != null && !mapInput.isEmpty()) {
			return (Map<String, Object>) mapInput.get(0);
		}

		return null;
	}

}
