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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
import prerna.reactor.AbstractReactor;
import prerna.reactor.masterdatabase.util.GenerateMetamodelLayout;
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

public class ExternalUpdateJdbcSchemaReactor extends AbstractReactor {

	private static final String CLASS_NAME = ExternalUpdateJdbcSchemaReactor.class.getName();
	private static final Logger classLogger = LogManager.getLogger(ExternalUpdateJdbcSchemaReactor.class);

	public static final String TABLES_KEY = "tables";
	public static final String RELATIONS_KEY = "relationships";

	public ExternalUpdateJdbcSchemaReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.FILTERS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);

		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		User user = this.insight.getUser();
		databaseId = SecurityQueryUtils.testUserEngineIdForAlias(user, databaseId);
		if (!SecurityEngineUtils.userCanEditEngine(user, databaseId)) {
			throw new IllegalArgumentException("User does not have permission to edit this database schema");
		}

		IDatabaseEngine database = Utility.getDatabase(databaseId);
		IRDBMSEngine nativeDatabase = null;
		if (database instanceof IRDBMSEngine) {
			nativeDatabase = (IRDBMSEngine) database;
		} else {
			throw new IllegalArgumentException("Database must be a valid JDBC engine");
		}
		AbstractSqlQueryUtil queryUtil = nativeDatabase.getQueryUtil();

		Connection connection = null;
		DatabaseMetaData meta = null;
		try {
			try {
				connection = nativeDatabase.getConnection();
			} catch (SQLException e) {
				classLogger.error("Failed to get a connection to database {}", Utility.cleanLogString(databaseId), e);
				throw new IllegalArgumentException("Could not connect to database.");
			}

			// tablename
			List<Map<String, Object>> databaseTables = new ArrayList<Map<String, Object>>();
			List<Map<String, String>> databaseJoins = new ArrayList<Map<String, String>>();

			try {
				meta = connection.getMetaData();
			} catch (SQLException e) {
				classLogger.error("Failed to retrieve database metadata from JDBC connection", e);
				throw new SemossPixelException(new NounMetadata("Unable to get the database metadata",
						PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			}

			List<String> tableAndViewFilters = getFilters();
			boolean hasFilters = !tableAndViewFilters.isEmpty();

			RdbmsTypeEnum driverEnum = nativeDatabase.getDbType();

			String catalogFilter = queryUtil.getDatabaseMetadataCatalogFilter();
			if (catalogFilter == null) {
				try {
					catalogFilter = connection.getCatalog();
				} catch (SQLException e) {
					classLogger.error(
							"Failed to retrieve catalog from JDBC connection; continuing without catalog filter", e);
				}
			}

			String schemaFilter = queryUtil.getDatabaseMetadataSchemaFilter();
			if (schemaFilter == null) {
				schemaFilter = nativeDatabase.getSchema();
			}

			CustomTableAndViewIterator tableViewIterator = new CustomTableAndViewIterator(connection, meta,
					catalogFilter, schemaFilter, driverEnum, tableAndViewFilters);

			final String TABLE_KEY = "table";
			final String COLUMNS_KEY = "columns";
			final String TYPES_KEY = "raw_type";
			final String CLEAN_TYPES_KEY = "type";
			final String PRIM_KEY = "isPrimKey";

			final String TO_TABLE_KEY = "toTable";
			final String TO_COL_KEY = "toCol";
			final String FROM_TABLE_KEY = "fromTable";
			final String FROM_COL_KEY = "fromCol";

			String[] columnKeys = RdbmsConnectionHelper.getColumnKeys(driverEnum);
			final String COLUMN_NAME_STR = columnKeys[0];
			final String COLUMN_TYPE_STR = columnKeys[1];

			try {
				while (tableViewIterator.hasNext()) {
					String[] nextRow = tableViewIterator.next();
					String tableOrView = nextRow[0];
					String tableType = nextRow[1];
					String schem = nextRow[2];
					boolean isTable = tableType.toUpperCase().contains("TABLE");

					// this will be table or view
					if (isTable) {
						logger.info("Processing table = {}", Utility.cleanLogString(tableOrView));
					} else {
						// there may be views built from sys or information schema
						// we want to ignore these
						if (schem != null) {
							if (schem.equalsIgnoreCase("INFORMATION_SCHEMA") || schem.equalsIgnoreCase("SYS")) {
								continue;
							}
						}
						logger.info("Processing view = {}", Utility.cleanLogString(tableOrView));
					}
					// grab the table
					// we want to get the following information
					// table name
					// column name
					// column type
					// is primary key
					Map<String, Object> tableDetails = new HashMap<String, Object>();
					tableDetails.put(TABLE_KEY, tableOrView);

					List<String> primaryKeys = new ArrayList<String>();
					ResultSet keys = null;
					try {
						keys = meta.getPrimaryKeys(catalogFilter, schemaFilter, tableOrView);
						while (keys.next()) {
							primaryKeys.add(keys.getString(COLUMN_NAME_STR));
						}
					} catch (SQLException e) {
						classLogger.error("Failed to retrieve primary keys for {}", Utility.cleanLogString(tableOrView),
								e);
					} finally {
						closeRs(keys);
					}

					List<String> columnNames = new ArrayList<String>();
					List<String> columnTypes = new ArrayList<String>();
					List<String> cleanColumnTypes = new ArrayList<String>();
					List<Boolean> isPrimKeys = new ArrayList<Boolean>();

					ResultSet columnsRs = null;
					try {
						logger.info("....Processing columns");

						columnsRs = RdbmsConnectionHelper.getColumns(meta, tableOrView, catalogFilter, schemaFilter,
								driverEnum);

						while (columnsRs.next()) {
							String cName = columnsRs.getString(COLUMN_NAME_STR);
							columnNames.add(cName);
							columnTypes.add(columnsRs.getString(COLUMN_TYPE_STR));
							cleanColumnTypes.add(SemossDataType
									.convertStringToDataType(columnsRs.getString(COLUMN_TYPE_STR)).toString());
							if (primaryKeys.contains(cName)) {
								isPrimKeys.add(true);
							} else {
								isPrimKeys.add(false);
							}
						}
						// done looping through
						// add the data into the table details
						tableDetails.put(COLUMNS_KEY, columnNames);
						tableDetails.put(TYPES_KEY, columnTypes);
						tableDetails.put(CLEAN_TYPES_KEY, cleanColumnTypes);
						tableDetails.put(PRIM_KEY, isPrimKeys);

					} catch (SQLException e) {
						classLogger.error("Failed to retrieve columns for {}", Utility.cleanLogString(tableOrView), e);
					} finally {
						closeRs(columnsRs);
					}
					databaseTables.add(tableDetails);

					// we are now done with the table info
					// let us go to the joins
					// only do this for tables, not for views
					ResultSet relRs = null;
					if (isTable) {
						try {
							logger.info("....Processing table foreign keys");
							relRs = meta.getExportedKeys(catalogFilter, schemaFilter, tableOrView);
							while (relRs.next()) {
								String otherTableName = relRs.getString("FKTABLE_NAME");

								// add filter check
								if (hasFilters && !tableAndViewFilters.contains(otherTableName)) {
									// we will ignore this table and view!
									continue;
								}

								Map<String, String> joinInfo = new HashMap<String, String>();
								joinInfo.put(FROM_TABLE_KEY, tableOrView);
								joinInfo.put(FROM_COL_KEY, relRs.getString("PKCOLUMN_NAME"));
								joinInfo.put(TO_TABLE_KEY, otherTableName);
								joinInfo.put(TO_COL_KEY, relRs.getString("FKCOLUMN_NAME"));
								databaseJoins.add(joinInfo);
							}
						} catch (SQLException e) {
							classLogger.error("Failed to retrieve foreign keys for table {}",
									Utility.cleanLogString(tableOrView), e);
						} finally {
							closeRs(relRs);
						}
					}
				}
			} finally {
				if (tableViewIterator != null) {
					tableViewIterator.close();
				}
			}
			logger.info("Done parsing database metadata");

			HashMap<String, Object> ret = new HashMap<String, Object>();
			ret.put(TABLES_KEY, databaseTables);
			ret.put(RELATIONS_KEY, databaseJoins);
			Map<String, Map<String, Double>> positions = GenerateMetamodelLayout
					.generateMetamodelLayoutForExternal(databaseTables, databaseJoins);
			ret.put(Constants.POSITION_PROP, positions);
			return new NounMetadata(ret, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} finally {
			if (nativeDatabase.isConnectionPooling()) {
				if (connection != null) {
					try {
						connection.close();
					} catch (SQLException e) {
						classLogger.error("Failed to close pooled JDBC connection", e);
					}
				}
			}
		}
	}

	/**
	 * Close the result set
	 * 
	 * @param rs
	 */
	private void closeRs(ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				classLogger.error("Failed to close result set", e);
			}
		}
	}

	///////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////

	/**
	 * Simple method to get string input
	 * 
	 * @param index
	 * @return
	 */
	public String getStringInput(int index) {
		GenRowStruct valueGrs = this.store.getGenRowStruct(this.keysToGet[index]);
		if (valueGrs != null) {
			return valueGrs.get(0).toString();
		}

		if (this.curRow.size() > index) {
			return this.curRow.get(index).toString();
		}

		return null;
	}

	/**
	 * Get a list of table / view column filters
	 * 
	 * @return
	 */
	private List<String> getFilters() {
		List<String> filterValues = new Vector<String>();

		GenRowStruct valueGrs = this.store.getGenRowStruct(this.keysToGet[1]);
		if (valueGrs != null && !valueGrs.isEmpty()) {
			int length = valueGrs.size();
			for (int i = 0; i < length; i++) {
				filterValues.add(valueGrs.get(i).toString());
			}
			return filterValues;
		}

		for (int i = 7; i < this.curRow.size(); i++) {
			filterValues.add(this.curRow.get(i).toString());
		}

		return filterValues;
	}

}
