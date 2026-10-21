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
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
import prerna.masterdatabase.utility.MasterDatabaseUtility;
import prerna.reactor.AbstractReactor;
import prerna.reactor.masterdatabase.util.GenerateMetamodelLayout;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EngineSyncUtility;
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

			// list the tables and views first, so their columns can be read together
			List<String[]> tablesAndViews = new ArrayList<>();
			int schemaTableCount;
			try (CustomTableAndViewIterator tableViewIterator = new CustomTableAndViewIterator(connection, meta,
					catalogFilter, schemaFilter, driverEnum, tableAndViewFilters)) {
				while (tableViewIterator.hasNext()) {
					String[] nextRow = tableViewIterator.next();
					boolean isTable = nextRow[1].toUpperCase().contains("TABLE");
					String schem = nextRow[2];
					// there may be views built from sys or information schema
					// we want to ignore these
					if (!isTable && schem != null
							&& (schem.equalsIgnoreCase("INFORMATION_SCHEMA") || schem.equalsIgnoreCase("SYS"))) {
						continue;
					}
					tablesAndViews.add(nextRow);
				}
				schemaTableCount = tableViewIterator.getListedCount();
			}

			// the columns of every table in one call for the whole schema, or a call per
			// table when only a few of a large schema's tables are asked for; see
			// readColumns
			logger.info("Processing the columns of {} tables and views", tablesAndViews.size());
			Map<String, List<RdbmsConnectionHelper.ColumnMetadata>> columnsByTable = RdbmsConnectionHelper
					.readColumns(meta, tablesAndViews, schemaTableCount, catalogFilter, schemaFilter, driverEnum);

			// the keys are read a table at a time, since JDBC only reads them for one table
			for (String[] nextRow : tablesAndViews) {
				String tableOrView = nextRow[0];
				boolean isTable = nextRow[1].toUpperCase().contains("TABLE");
				if (isTable) {
					logger.info("Processing table = {}", Utility.cleanLogString(tableOrView));
				} else {
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
				try (ResultSet keys = meta.getPrimaryKeys(catalogFilter, schemaFilter, tableOrView)) {
					if (keys != null) {
						while (keys.next()) {
							primaryKeys.add(keys.getString(COLUMN_NAME_STR));
						}
					}
				} catch (SQLException e) {
					classLogger.error("Failed to retrieve primary keys for {}", Utility.cleanLogString(tableOrView), e);
				}

				// a table whose columns could not be read is listed without them
				List<RdbmsConnectionHelper.ColumnMetadata> columns = columnsByTable.get(tableOrView);
				if (columns != null) {
					List<String> columnNames = new ArrayList<String>();
					List<String> columnTypes = new ArrayList<String>();
					List<String> cleanColumnTypes = new ArrayList<String>();
					List<Boolean> isPrimKeys = new ArrayList<Boolean>();
					for (RdbmsConnectionHelper.ColumnMetadata column : columns) {
						columnNames.add(column.name());
						columnTypes.add(column.type());
						cleanColumnTypes.add(SemossDataType.convertStringToDataType(column.type()).toString());
						isPrimKeys.add(primaryKeys.contains(column.name()));
					}
					// add the data into the table details
					tableDetails.put(COLUMNS_KEY, columnNames);
					tableDetails.put(TYPES_KEY, columnTypes);
					tableDetails.put(CLEAN_TYPES_KEY, cleanColumnTypes);
					tableDetails.put(PRIM_KEY, isPrimKeys);
				}
				databaseTables.add(tableDetails);

				// we are now done with the table info
				// let us go to the joins
				// only do this for tables, not for views
				if (isTable) {
					try (ResultSet relRs = meta.getExportedKeys(catalogFilter, schemaFilter, tableOrView)) {
						if (relRs != null) {
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
						}
					} catch (SQLException e) {
						classLogger.error("Failed to retrieve foreign keys for table {}",
								Utility.cleanLogString(tableOrView), e);
					}
				}
			}
			logger.info("Done parsing database metadata");

			HashMap<String, Object> ret = new HashMap<String, Object>();
			ret.put(TABLES_KEY, databaseTables);
			ret.put(RELATIONS_KEY, databaseJoins);
			// tables the user already arranged keep their place; only new tables are placed
			Map<String, Map<String, Double>> positions = GenerateMetamodelLayout
					.generateMetamodelLayoutForExternal(databaseTables, databaseJoins, savedPositions(databaseId));
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
	 * The table positions saved for the database, from the local master database
	 * or, when it has none, the positions cache. A layout is never generated here,
	 * since only the new tables need placing.
	 *
	 * @return the saved positions by table name, or null when none are saved
	 */
	private static Map<String, Object> savedPositions(String databaseId) {
		Map<String, Object> positions = MasterDatabaseUtility.getMetamodelPositions(databaseId);
		if (positions != null && !positions.isEmpty()) {
			return positions;
		}
		return EngineSyncUtility.getMetamodelPositions(databaseId);
	}

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
