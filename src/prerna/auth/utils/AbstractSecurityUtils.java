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
package prerna.auth.utils;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mindrot.jbcrypt.BCrypt;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.PasswordRequirements;
import prerna.auth.User;
import prerna.date.SemossDate;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.SystemDefaultDatabases;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public abstract class AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(AbstractSecurityUtils.class);

	@Deprecated
	static boolean adminSetPublisher = false;
	static boolean adminSetExporter = false;
	static String ADMIN_ADDED_USER = "ADMIN_ADDED_USER";
	static boolean anonymousUsersEnabled = false;
	static boolean anonymousUsersUploadData = false;

	static boolean adminOnlyProjectAdd = false;
	static boolean adminOnlyProjectDelete = false;
	static boolean adminOnlyProjectAddAccess = false;
	static boolean adminOnlyProjectSetPublic = false;
	static boolean adminOnlyProjectSetDiscoverable = false;

	static boolean adminOnlyDatabaseAdd = false;
	static boolean adminOnlyDatabaseDelete = false;
	static boolean adminOnlyDatabaseAddAccess = false;
	static boolean adminOnlyDatabaseSetPublic = false;
	static boolean adminOnlyDatabaseSetDiscoverable = false;

	static boolean adminOnlyModelAdd = false;
	static boolean adminOnlyModelDelete = false;
	static boolean adminOnlyModelAddAccess = false;
	static boolean adminOnlyModelSetPublic = false;
	static boolean adminOnlyModelSetDiscoverable = false;

	static boolean adminOnlyStorageAdd = false;
	static boolean adminOnlyStorageDelete = false;
	static boolean adminOnlyStorageAddAccess = false;
	static boolean adminOnlyStorageSetPublic = false;
	static boolean adminOnlyStorageSetDiscoverable = false;

	static boolean adminOnlyVectorAdd = false;
	static boolean adminOnlyVectorDelete = false;
	static boolean adminOnlyVectorAddAccess = false;
	static boolean adminOnlyVectorSetPublic = false;
	static boolean adminOnlyVectorSetDiscoverable = false;

	static boolean adminOnlyFunctionAdd = false;
	static boolean adminOnlyFunctionDelete = false;
	static boolean adminOnlyFunctionAddAccess = false;
	static boolean adminOnlyFunctionSetPublic = false;
	static boolean adminOnlyFunctionSetDiscoverable = false;

	static boolean adminOnlyGuardrailAdd = false;
	static boolean adminOnlyGuardrailDelete = false;
	static boolean adminOnlyGuardrailAddAccess = false;
	static boolean adminOnlyGuardrailSetPublic = false;
	static boolean adminOnlyGuardrailSetDiscoverable = false;

	static boolean adminOnlyInsightSetPublic = false;
	static boolean adminOnlyInsightAddAccess = false;

	static boolean adminOnlyInsightShare = false;

	static Gson securityGson = new GsonBuilder().disableHtmlEscaping().create();

	/**
	 * Only used for static references
	 */
	AbstractSecurityUtils() {

	}

	public static void loadSecurityDatabase() throws Exception {
		IRDBMSEngine loadedSecurityDb = SystemEngineRegistry.getSecurityDb();
		SecurityOwlCreator owlCreator = new SecurityOwlCreator(loadedSecurityDb);
		if (owlCreator.needsRemake()) {
			owlCreator.remakeOwl();
		}
		initialize();
		// this is to update the bad naming in the security db for type values
		updateUserTypeEnum();

		Object anonymousUsers = Utility.getDIHelperLocalProperty(Constants.ANONYMOUS_USER_ALLOWED);
		if (anonymousUsers == null) {
			anonymousUsersEnabled = false;
		} else {
			anonymousUsersEnabled = (anonymousUsers instanceof Boolean && ((boolean) anonymousUsers))
					|| (Boolean.parseBoolean(anonymousUsers.toString()));
		}

		Object anonymousUsersData = Utility.getDIHelperLocalProperty(Constants.ANONYMOUS_USER_UPLOAD_DATA);
		if (anonymousUsersData == null) {
			anonymousUsersUploadData = false;
		} else {
			anonymousUsersUploadData = (anonymousUsersData instanceof Boolean && ((boolean) anonymousUsersData))
					|| (Boolean.parseBoolean(anonymousUsersData.toString()));
		}

		Object adminSetsPublisher = Utility.getDIHelperLocalProperty(Constants.ADMIN_SET_PUBLISHER);
		if (adminSetsPublisher == null) {
			adminSetPublisher = false;
		} else {
			adminSetPublisher = (adminSetsPublisher instanceof Boolean && ((boolean) adminSetsPublisher))
					|| (Boolean.parseBoolean(adminSetsPublisher.toString()));
		}

		Object adminSetsExporter = Utility.getDIHelperLocalProperty(Constants.ADMIN_SET_EXPORTER);
		if (adminSetsExporter == null) {
			adminSetExporter = false;
		} else {
			adminSetExporter = (adminSetsExporter instanceof Boolean && ((boolean) adminSetsExporter))
					|| (Boolean.parseBoolean(adminSetsExporter.toString()));
		}

		adminOnlyProjectAdd = Utility.getApplicationAdminOnlyProjectAdd();
		adminOnlyProjectDelete = Utility.getApplicationAdminOnlyProjectDelete();
		adminOnlyProjectAddAccess = Utility.getApplicationAdminOnlyProjectAddAccess();
		adminOnlyProjectSetPublic = Utility.getApplicationAdminOnlyProjectSetPublic();
		adminOnlyProjectSetDiscoverable = Utility.getApplicationAdminOnlyProjectSetDiscoverable();

		adminOnlyDatabaseAdd = Utility.getApplicationAdminOnlyDbAdd();
		adminOnlyDatabaseDelete = Utility.getApplicationAdminOnlyDbDelete();
		adminOnlyDatabaseAddAccess = Utility.getApplicationAdminOnlyDbAddAccess();
		adminOnlyDatabaseSetPublic = Utility.getApplicationAdminOnlyDbSetPublic();
		adminOnlyDatabaseSetDiscoverable = Utility.getApplicationAdminOnlyDbSetDiscoverable();

		adminOnlyModelAdd = Utility.getApplicationAdminOnlyModelAdd();
		adminOnlyModelDelete = Utility.getApplicationAdminOnlyModelDelete();
		adminOnlyModelAddAccess = Utility.getApplicationAdminOnlyModelAddAccess();
		adminOnlyModelSetPublic = Utility.getApplicationAdminOnlyModelSetPublic();
		adminOnlyModelSetDiscoverable = Utility.getApplicationAdminOnlyModelSetDiscoverable();

		adminOnlyStorageAdd = Utility.getApplicationAdminOnlyStorageAdd();
		adminOnlyStorageDelete = Utility.getApplicationAdminOnlyStorageDelete();
		adminOnlyStorageAddAccess = Utility.getApplicationAdminOnlyStorageAddAccess();
		adminOnlyStorageSetPublic = Utility.getApplicationAdminOnlyStorageSetPublic();
		adminOnlyStorageSetDiscoverable = Utility.getApplicationAdminOnlyStorageSetDiscoverable();

		adminOnlyVectorAdd = Utility.getApplicationAdminOnlyVectorAdd();
		adminOnlyVectorDelete = Utility.getApplicationAdminOnlyVectorDelete();
		adminOnlyVectorAddAccess = Utility.getApplicationAdminOnlyVectorAddAccess();
		adminOnlyVectorSetPublic = Utility.getApplicationAdminOnlyVectorSetPublic();
		adminOnlyVectorSetDiscoverable = Utility.getApplicationAdminOnlyVectorSetDiscoverable();

		adminOnlyFunctionAdd = Utility.getApplicationAdminOnlyFunctionAdd();
		adminOnlyFunctionDelete = Utility.getApplicationAdminOnlyFunctionDelete();
		adminOnlyFunctionAddAccess = Utility.getApplicationAdminOnlyFunctionAddAccess();
		adminOnlyFunctionSetPublic = Utility.getApplicationAdminOnlyFunctionSetPublic();
		adminOnlyFunctionSetDiscoverable = Utility.getApplicationAdminOnlyFunctionSetDiscoverable();

		adminOnlyGuardrailAdd = Utility.getApplicationAdminOnlyGuardrailAdd();
		adminOnlyGuardrailDelete = Utility.getApplicationAdminOnlyGuardrailDelete();
		adminOnlyGuardrailAddAccess = Utility.getApplicationAdminOnlyGuardrailAddAccess();
		adminOnlyGuardrailSetPublic = Utility.getApplicationAdminOnlyGuardrailSetPublic();
		adminOnlyGuardrailSetDiscoverable = Utility.getApplicationAdminOnlyGuardrailSetDiscoverable();

		adminOnlyInsightSetPublic = Utility.getApplicationAdminOnlyInsightSetPublic();
		adminOnlyInsightAddAccess = Utility.getApplicationAdminOnlyInsightAddAccess();

		adminOnlyInsightShare = Utility.getApplicationAdminOnlyInsightShare();
	}

	public static boolean anonymousUsersEnabled() {
		return anonymousUsersEnabled;
	}

	public static boolean anonymousUserUploadData() {
		return anonymousUsersEnabled() && anonymousUsersUploadData;
	}

	@Deprecated
	public static boolean adminSetPublisher() {
		return adminSetPublisher;
	}

	public static boolean adminSetExporter() {
		return adminSetExporter;
	}

	public static boolean adminOnlyProjectAdd() {
		return adminOnlyProjectAdd;
	}

	public static boolean adminOnlyProjectDelete() {
		return adminOnlyProjectDelete;
	}

	public static boolean adminOnlyProjectAddAccess() {
		return adminOnlyProjectAddAccess;
	}

	public static boolean adminOnlyProjectSetPublic() {
		return adminOnlyProjectSetPublic;
	}

	public static boolean adminOnlyProjectSetDiscoverable() {
		return adminOnlyProjectSetDiscoverable;
	}

	public static boolean adminOnlyDatabaseAdd() {
		return adminOnlyDatabaseAdd;
	}

	public static boolean adminOnlyDatabaseDelete() {
		return adminOnlyDatabaseDelete;
	}

	public static boolean adminOnlyDatabaseAddAccess() {
		return adminOnlyDatabaseAddAccess;
	}

	public static boolean adminOnlyDatabaseSetPublic() {
		return adminOnlyDatabaseSetPublic;
	}

	public static boolean adminOnlyDatabaseSetDiscoverable() {
		return adminOnlyDatabaseSetDiscoverable;
	}

	public static boolean adminOnlyModelAdd() {
		return adminOnlyModelAdd;
	}

	public static boolean adminOnlyModelDelete() {
		return adminOnlyModelDelete;
	}

	public static boolean adminOnlyModelAddAccess() {
		return adminOnlyModelAddAccess;
	}

	public static boolean adminOnlyModelSetPublic() {
		return adminOnlyModelSetPublic;
	}

	public static boolean adminOnlyModelSetDiscoverable() {
		return adminOnlyModelSetDiscoverable;
	}

	public static boolean adminOnlyStorageAdd() {
		return adminOnlyStorageAdd;
	}

	public static boolean adminOnlyStorageDelete() {
		return adminOnlyStorageDelete;
	}

	public static boolean adminOnlyStorageAddAccess() {
		return adminOnlyStorageAddAccess;
	}

	public static boolean adminOnlyStorageSetPublic() {
		return adminOnlyStorageSetPublic;
	}

	public static boolean adminOnlyStorageSetDiscoverable() {
		return adminOnlyStorageSetDiscoverable;
	}

	public static boolean adminOnlyVectorAdd() {
		return adminOnlyVectorAdd;
	}

	public static boolean adminOnlyVectorDelete() {
		return adminOnlyVectorDelete;
	}

	public static boolean adminOnlyVectorAddAccess() {
		return adminOnlyVectorAddAccess;
	}

	public static boolean adminOnlyVectorSetPublic() {
		return adminOnlyVectorSetPublic;
	}

	public static boolean adminOnlyVectorSetDiscoverable() {
		return adminOnlyVectorSetDiscoverable;
	}

	public static boolean adminOnlyFunctionAdd() {
		return adminOnlyFunctionAdd;
	}

	public static boolean adminOnlyFunctionDelete() {
		return adminOnlyFunctionDelete;
	}

	public static boolean adminOnlyFunctionAddAccess() {
		return adminOnlyFunctionAddAccess;
	}

	public static boolean adminOnlyFunctionSetPublic() {
		return adminOnlyFunctionSetPublic;
	}

	public static boolean adminOnlyFunctionSetDiscoverable() {
		return adminOnlyFunctionSetDiscoverable;
	}

	public static boolean adminOnlyGuardrailAdd() {
		return adminOnlyGuardrailAdd;
	}

	public static boolean adminOnlyGuardrailDelete() {
		return adminOnlyGuardrailDelete;
	}

	public static boolean adminOnlyGuardrailAddAccess() {
		return adminOnlyGuardrailAddAccess;
	}

	public static boolean adminOnlyGuardrailSetPublic() {
		return adminOnlyGuardrailSetPublic;
	}

	public static boolean adminOnlyGuardrailSetDiscoverable() {
		return adminOnlyGuardrailSetDiscoverable;
	}

	public static boolean adminOnlyInsightSetPublic() {
		return adminOnlyInsightSetPublic;
	}

	public static boolean adminOnlyInsightAddAccess() {
		return adminOnlyInsightAddAccess;
	}

	public static boolean adminOnlyInsightShare() {
		return adminOnlyInsightShare;
	}

	public static boolean adminOnlyEngineAdd(String engineId) {
		return adminOnlyEngineAdd(SecurityEngineUtils.getEngineType(engineId));
	}

	public static boolean adminOnlyEngineAdd(IEngine.CATALOG_TYPE type) {
		if (IEngine.CATALOG_TYPE.DATABASE == type) {
			return adminOnlyDatabaseAdd;
		} else if (IEngine.CATALOG_TYPE.MODEL == type) {
			return adminOnlyModelAdd;
		} else if (IEngine.CATALOG_TYPE.STORAGE == type) {
			return adminOnlyStorageAdd;
		} else if (IEngine.CATALOG_TYPE.VECTOR == type) {
			return adminOnlyVectorAdd;
		} else if (IEngine.CATALOG_TYPE.FUNCTION == type) {
			return adminOnlyFunctionAdd;
		} else if (IEngine.CATALOG_TYPE.GUARDRAIL == type) {
			return adminOnlyGuardrailAdd;
		}

		throw new IllegalArgumentException("Admin only configuration must be defined for catalog type = " + type);
	}

	public static boolean adminOnlyEngineDelete(String engineId) {
		return adminOnlyEngineDelete(SecurityEngineUtils.getEngineType(engineId));
	}

	public static boolean adminOnlyEngineDelete(IEngine.CATALOG_TYPE type) {
		if (IEngine.CATALOG_TYPE.DATABASE == type) {
			return adminOnlyDatabaseDelete;
		} else if (IEngine.CATALOG_TYPE.MODEL == type) {
			return adminOnlyModelDelete;
		} else if (IEngine.CATALOG_TYPE.STORAGE == type) {
			return adminOnlyStorageDelete;
		} else if (IEngine.CATALOG_TYPE.VECTOR == type) {
			return adminOnlyVectorDelete;
		} else if (IEngine.CATALOG_TYPE.FUNCTION == type) {
			return adminOnlyFunctionDelete;
		} else if (IEngine.CATALOG_TYPE.GUARDRAIL == type) {
			return adminOnlyGuardrailDelete;
		}

		throw new IllegalArgumentException("Admin only configuration must be defined for catalog type = " + type);
	}

	public static boolean adminOnlyEngineAddAccess(String engineId) {
		return adminOnlyEngineAddAccess(SecurityEngineUtils.getEngineType(engineId));
	}

	public static boolean adminOnlyEngineAddAccess(IEngine.CATALOG_TYPE type) {
		if (IEngine.CATALOG_TYPE.DATABASE == type) {
			return adminOnlyDatabaseAddAccess;
		} else if (IEngine.CATALOG_TYPE.MODEL == type) {
			return adminOnlyModelAddAccess;
		} else if (IEngine.CATALOG_TYPE.STORAGE == type) {
			return adminOnlyStorageAddAccess;
		} else if (IEngine.CATALOG_TYPE.VECTOR == type) {
			return adminOnlyVectorAddAccess;
		} else if (IEngine.CATALOG_TYPE.FUNCTION == type) {
			return adminOnlyFunctionAddAccess;
		} else if (IEngine.CATALOG_TYPE.GUARDRAIL == type) {
			return adminOnlyGuardrailAddAccess;
		}

		throw new IllegalArgumentException("Admin only configuration must be defined for catalog type = " + type);
	}

	public static boolean adminOnlyEngineSetPublic(String engineId) {
		return adminOnlyEngineSetPublic(SecurityEngineUtils.getEngineType(engineId));
	}

	public static boolean adminOnlyEngineSetPublic(IEngine.CATALOG_TYPE type) {
		if (IEngine.CATALOG_TYPE.DATABASE == type) {
			return adminOnlyDatabaseSetPublic;
		} else if (IEngine.CATALOG_TYPE.MODEL == type) {
			return adminOnlyModelSetPublic;
		} else if (IEngine.CATALOG_TYPE.STORAGE == type) {
			return adminOnlyStorageSetPublic;
		} else if (IEngine.CATALOG_TYPE.VECTOR == type) {
			return adminOnlyVectorSetPublic;
		} else if (IEngine.CATALOG_TYPE.FUNCTION == type) {
			return adminOnlyFunctionSetPublic;
		} else if (IEngine.CATALOG_TYPE.GUARDRAIL == type) {
			return adminOnlyGuardrailSetPublic;
		}

		throw new IllegalArgumentException("Admin only configuration must be defined for catalog type = " + type);
	}

	public static boolean adminOnlyEngineSetDiscoverable(String engineId) {
		return adminOnlyEngineSetDiscoverable(SecurityEngineUtils.getEngineType(engineId));
	}

	public static boolean adminOnlyEngineSetDiscoverable(IEngine.CATALOG_TYPE type) {
		if (IEngine.CATALOG_TYPE.DATABASE == type) {
			return adminOnlyDatabaseSetDiscoverable;
		} else if (IEngine.CATALOG_TYPE.MODEL == type) {
			return adminOnlyModelSetDiscoverable;
		} else if (IEngine.CATALOG_TYPE.STORAGE == type) {
			return adminOnlyStorageSetDiscoverable;
		} else if (IEngine.CATALOG_TYPE.VECTOR == type) {
			return adminOnlyVectorSetDiscoverable;
		} else if (IEngine.CATALOG_TYPE.FUNCTION == type) {
			return adminOnlyFunctionSetDiscoverable;
		} else if (IEngine.CATALOG_TYPE.GUARDRAIL == type) {
			return adminOnlyGuardrailSetDiscoverable;
		}

		throw new IllegalArgumentException("Admin only configuration must be defined for catalog type = " + type);
	}

	public static void initialize() throws Exception {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String database = securityDb.getDatabase();
		String schema = securityDb.getSchema();
		Connection conn = securityDb.getConnection();
		try {
			String[] colNames = null;
			String[] types = null;
			Object[] defaultValues = null;
			/*
			 * Currently used
			 */

			AbstractSqlQueryUtil queryUtil = securityDb.getQueryUtil();
			boolean allowIfExistsTable = queryUtil.allowsIfExistsTableSyntax();
			boolean allowIfExistsIndexs = queryUtil.allowIfExistsIndexSyntax();
			final String CLOB_DATATYPE_NAME = queryUtil.getClobDataTypeName();
			final String BOOLEAN_DATATYPE_NAME = queryUtil.getBooleanDataTypeName();
			final String TIMESTAMP_DATATYPE_NAME = queryUtil.getDateWithTimeDataType();
			final String INTEGER_DATATYPE_NAME = queryUtil.getIntegerDataTypeName();
			final String DOBLE_DATATYPE_NAME = queryUtil.getDoubleDataTypeName();

			// 2021-08-06
			// on h2 when you renmae a column it doens't update/change anything on the index
			// name
			// also had some invalid indexes on certain tables
			if (allowIfExistsIndexs) {
				String sql = queryUtil.dropIndexIfExists("INSIGHT_ENGINEID_INDEX", "INSIGHT");
				classLogger.info("Running sql {}", sql);
				securityDb.removeData(sql);
				sql = queryUtil.dropIndexIfExists("INSIGHTMETA_ENGINEID_INDEX", "INSIGHT");
				classLogger.info("Running sql {}", sql);
				securityDb.removeData(sql);
				sql = queryUtil.dropIndexIfExists("INSIGHTMETA_ENGINEID_INDEX", "INSIGHTMETA");
				classLogger.info("Running sql {}", sql);
				securityDb.removeData(sql);
				sql = queryUtil.dropIndexIfExists("USERINSIGHTPERMISSION_ENGINEID_INDEX", "USERINSIGHTPERMISSION");
				classLogger.info("Running sql {}", sql);
				securityDb.removeData(sql);

				// these are right name - but were added to wrong table
				// so will do an exists check anyway
				try {
					if (queryUtil.indexExists(securityDb, "INSIGHTMETA_PROJECTID_INDEX", "INSIGHT", database, schema)) {
						sql = queryUtil.dropIndex("INSIGHTMETA_PROJECTID_INDEX", "INSIGHT");
						classLogger.info("Running sql {}", sql);
						securityDb.removeData(sql);
					}
					if (queryUtil.indexExists(securityDb, "INSIGHTMETA_INSIGHTID_INDEX", "INSIGHT", database, schema)) {
						sql = queryUtil.dropIndex("INSIGHTMETA_INSIGHTID_INDEX", "INSIGHT");
						classLogger.info("Running sql {}", sql);
						securityDb.removeData(sql);
					}
				} catch (UnsupportedOperationException ignore) {
					// ignore
				}
			} else {
				// see if index exists
				if (queryUtil.indexExists(securityDb, "INSIGHT_ENGINEID_INDEX", "INSIGHT", database, schema)) {
					String sql = queryUtil.dropIndex("INSIGHT_ENGINEID_INDEX", "INSIGHT");
					classLogger.info("Running sql {}", sql);
					securityDb.removeData(sql);
				}
				if (queryUtil.indexExists(securityDb, "INSIGHTMETA_ENGINEID_INDEX", "INSIGHT", database, schema)) {
					String sql = queryUtil.dropIndex("INSIGHTMETA_ENGINEID_INDEX", "INSIGHT");
					classLogger.info("Running sql {}", sql);
					securityDb.removeData(sql);
				}
				if (queryUtil.indexExists(securityDb, "INSIGHTMETA_ENGINEID_INDEX", "INSIGHTMETA", database, schema)) {
					String sql = queryUtil.dropIndex("INSIGHTMETA_ENGINEID_INDEX", "INSIGHTMETA");
					classLogger.info("Running sql {}", sql);
					securityDb.removeData(sql);
				}
				if (queryUtil.indexExists(securityDb, "USERINSIGHTPERMISSION_ENGINEID_INDEX", "USERINSIGHTPERMISSION",
						database, schema)) {
					String sql = queryUtil.dropIndex("USERINSIGHTPERMISSION_ENGINEID_INDEX", "USERINSIGHTPERMISSION");
					classLogger.info("Running sql {}", sql);
					securityDb.removeData(sql);
				}
				if (queryUtil.indexExists(securityDb, "INSIGHTMETA_PROJECTID_INDEX", "INSIGHT", database, schema)) {
					String sql = queryUtil.dropIndex("INSIGHTMETA_PROJECTID_INDEX", "INSIGHT");
					classLogger.info("Running sql {}", sql);
					securityDb.removeData(sql);
				}
				if (queryUtil.indexExists(securityDb, "INSIGHTMETA_INSIGHTID_INDEX", "INSIGHT", database, schema)) {
					String sql = queryUtil.dropIndex("INSIGHTMETA_INSIGHTID_INDEX", "INSIGHT");
					classLogger.info("Running sql {}", sql);
					securityDb.removeData(sql);
				}
			}

			// ENGINE
			colNames = new String[] { "ENGINEID", "ENGINENAME", "ENGINEDISPLAYNAME", "GLOBAL", "DISCOVERABLE",
					"CREATEDBY", "CREATEDBYTYPE", "DATECREATED", "ENGINETYPE", "ENGINESUBTYPE", "COST", "TOOL_APP", };
			types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", BOOLEAN_DATATYPE_NAME,
					BOOLEAN_DATATYPE_NAME, "VARCHAR(255)", "VARCHAR(255)", TIMESTAMP_DATATYPE_NAME, "VARCHAR(255)",
					"VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)" };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists("ENGINE", colNames, types);
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "ENGINE", database, schema)) {
					// make the table
					String sql = queryUtil.createTable("ENGINE", colNames, types);
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}
			// UPDATE TO CHECK ALL COLUMNS! - ADDED 07/07/2023
			{
				List<String> allCols = queryUtil.getTableColumns(conn, "ENGINE", database, schema);
				for (int i = 0; i < colNames.length; i++) {
					String col = colNames[i];
					if (!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
						classLogger.info("Column '{}' is not present in current list of columns: {}", col, allCols);
						String addColumnSql = queryUtil.alterTableAddColumn("ENGINE", col, types[i]);
						classLogger.info("Running sql {}", addColumnSql);
						securityDb.insertData(addColumnSql);
					}
				}

				// if type columns exist, remove it - ADDED 07/18/2023
				{
					if (allCols.contains("TYPE") || allCols.contains("type")) {
						String dropTypeColumn = queryUtil.alterTableDropColumn("ENGINE", "TYPE");
						classLogger.info("Running sql {}", dropTypeColumn);
						securityDb.insertData(dropTypeColumn);
					}
				}

				securityDb.insertData("UPDATE ENGINE SET ENGINETYPE='" + IEngine.CATALOG_TYPE.DATABASE.toString()
						+ "' WHERE ENGINETYPE IS NULL");
				// backfill display name from canonical name for existing rows
				securityDb.insertData(
						"UPDATE ENGINE SET ENGINEDISPLAYNAME = ENGINENAME WHERE ENGINEDISPLAYNAME IS NULL OR ENGINEDISPLAYNAME = ''");
			}
			if (allowIfExistsIndexs) {
				String sql = queryUtil.createIndexIfNotExists("ENGINE_GLOBAL_INDEX", "ENGINE", "GLOBAL");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);

				sql = queryUtil.createIndexIfNotExists("ENGINE_DISCOVERABLE_INDEX", "ENGINE", "DISCOVERABLE");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);

				sql = queryUtil.createIndexIfNotExists("ENGINE_ENGINENAME_INDEX", "ENGINE", "ENGINENAME");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);

				sql = queryUtil.createIndexIfNotExists("ENGINE_ENGINEID_INDEX", "ENGINE", "ENGINEID");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);

				sql = queryUtil.createIndexIfNotExists("ENGINE_ENGINEDISPLAYNAME_INDEX", "ENGINE", "ENGINEDISPLAYNAME");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if index exists
				if (!queryUtil.indexExists(securityDb, "ENGINE_GLOBAL_INDEX", "ENGINE", database, schema)) {
					String sql = queryUtil.createIndex("ENGINE_GLOBAL_INDEX", "ENGINE", "GLOBAL");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
				if (!queryUtil.indexExists(securityDb, "ENGINE_DISCOVERABLE_INDEX", "ENGINE", database, schema)) {
					String sql = queryUtil.createIndex("ENGINE_DISCOVERABLE_INDEX", "ENGINE", "DISCOVERABLE");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
				if (!queryUtil.indexExists(securityDb, "ENGINE_ENGINENAME_INDEX", "ENGINE", database, schema)) {
					String sql = queryUtil.createIndex("ENGINE_ENGINENAME_INDEX", "ENGINE", "ENGINENAME");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
				if (!queryUtil.indexExists(securityDb, "ENGINE_ENGINEID_INDEX", "ENGINE", database, schema)) {
					String sql = queryUtil.createIndex("ENGINE_ENGINEID_INDEX", "ENGINE", "ENGINEID");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
				if (!queryUtil.indexExists(securityDb, "ENGINE_ENGINEDISPLAYNAME_INDEX", "ENGINE", database, schema)) {
					String sql = queryUtil.createIndex("ENGINE_ENGINEDISPLAYNAME_INDEX", "ENGINE", "ENGINEDISPLAYNAME");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}

			// ENGINEMETA
			// check if column exists
			// TEMPORARY CHECK! - not sure when added but todays date is 12/16
			{
				List<String> allCols = queryUtil.getTableColumns(conn, "ENGINEMETA", database, schema);
				// this should return in all upper case
				// ... but sometimes it is not -_- i.e. postgres always lowercases
				if (!allCols.contains("METAORDER") && !allCols.contains("metaorder")) {
					if (allowIfExistsTable) {
						String sql = queryUtil.dropTableIfExists("ENGINEMETA");
						classLogger.info("Running sql {}", sql);
						securityDb.removeData(sql);
					} else if (queryUtil.tableExists(conn, "ENGINEMETA", database, schema)) {
						String sql = queryUtil.dropTable("ENGINEMETA");
						classLogger.info("Running sql {}", sql);
						securityDb.removeData(sql);
					}
				}
			}
			colNames = new String[] { "ENGINEID", "METAKEY", "METAVALUE", "METAORDER" };
			types = new String[] { "VARCHAR(255)", "VARCHAR(255)", CLOB_DATATYPE_NAME, INTEGER_DATATYPE_NAME };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists("ENGINEMETA", colNames, types);
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "ENGINEMETA", database, schema)) {
					// make the table
					String sql = queryUtil.createTable("ENGINEMETA", colNames, types);
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}
			if (allowIfExistsIndexs) {
				String sql = queryUtil.createIndexIfNotExists("ENGINEMETA_ENGINEID_INDEX", "ENGINEMETA", "ENGINEID");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if index exists
				if (!queryUtil.indexExists(securityDb, "ENGINEMETA_ENGINEID_INDEX", "ENGINEMETA", database, schema)) {
					String sql = queryUtil.createIndex("ENGINEMETA_ENGINEID_INDEX", "ENGINEMETA", "ENGINEID");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}

			// ENGINEPERMISSION
			colNames = new String[] { "USERID", "PERMISSION", "ENGINEID", "VISIBILITY", "FAVORITE",
					"PERMISSIONGRANTEDBY", "PERMISSIONGRANTEDBYTYPE", "DATEADDED", "ENDDATE", "USAGERESTRICTION",
					"MAXTOKENS", "MAXRESPONSETIME", "USAGEFREQUENCY" };
			types = new String[] { "VARCHAR(255)", INTEGER_DATATYPE_NAME, "VARCHAR(255)", BOOLEAN_DATATYPE_NAME,
					BOOLEAN_DATATYPE_NAME, "VARCHAR(255)", "VARCHAR(255)", TIMESTAMP_DATATYPE_NAME,
					TIMESTAMP_DATATYPE_NAME, "VARCHAR(255)", INTEGER_DATATYPE_NAME, DOBLE_DATATYPE_NAME,
					"VARCHAR(255)" };
			defaultValues = new Object[] { null, null, null, true, false, null, null, null, null, null, null, null,
					null };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExistsWithDefaults("ENGINEPERMISSION", colNames, types,
						defaultValues);
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "ENGINEPERMISSION", database, schema)) {
					// make the table
					String sql = queryUtil.createTable("ENGINEPERMISSION", colNames, types);
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}
			// TEMPORARY CHECK! - ADDED 03/17/2021
			{
				List<String> allCols = queryUtil.getTableColumns(conn, "ENGINEPERMISSION", database, schema);
				for (int i = 0; i < colNames.length; i++) {
					String col = colNames[i];
					if (!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
						classLogger.info("Column '{}' is not present in current list of columns: {}", col, allCols);
						String addColumnSql = queryUtil.alterTableAddColumn("ENGINEPERMISSION", col, types[i]);
						classLogger.info("Running sql {}", addColumnSql);
						securityDb.insertData(addColumnSql);
					}
				}
			}
			if (allowIfExistsIndexs) {
				String sql = queryUtil.createIndexIfNotExists("ENGINEPERMISSION_PERMISSION_INDEX", "ENGINEPERMISSION",
						"PERMISSION");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);

				sql = queryUtil.createIndexIfNotExists("ENGINEPERMISSION_VISIBILITY_INDEX", "ENGINEPERMISSION",
						"VISIBILITY");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);

				sql = queryUtil.createIndexIfNotExists("ENGINEPERMISSION_ENGINEID_INDEX", "ENGINEPERMISSION",
						"ENGINEID");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);

				sql = queryUtil.createIndexIfNotExists("ENGINEPERMISSION_FAVORITE_INDEX", "ENGINEPERMISSION",
						"FAVORITE");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);

				sql = queryUtil.createIndexIfNotExists("ENGINEPERMISSION_USERID_INDEX", "ENGINEPERMISSION", "USERID");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if index exists
				if (!queryUtil.indexExists(securityDb, "ENGINEPERMISSION_PERMISSION_INDEX", "ENGINEPERMISSION",
						database, schema)) {
					String sql = queryUtil.createIndex("ENGINEPERMISSION_PERMISSION_INDEX", "ENGINEPERMISSION",
							"PERMISSION");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
				if (!queryUtil.indexExists(securityDb, "ENGINEPERMISSION_VISIBILITY_INDEX", "ENGINEPERMISSION",
						database, schema)) {
					String sql = queryUtil.createIndex("ENGINEPERMISSION_VISIBILITY_INDEX", "ENGINEPERMISSION",
							"VISIBILITY");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
				if (!queryUtil.indexExists(securityDb, "ENGINEPERMISSION_ENGINEID_INDEX", "ENGINEPERMISSION", database,
						schema)) {
					String sql = queryUtil.createIndex("ENGINEPERMISSION_ENGINEID_INDEX", "ENGINEPERMISSION",
							"ENGINEID");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
				if (!queryUtil.indexExists(securityDb, "ENGINEPERMISSION_FAVORITE_INDEX", "ENGINEPERMISSION", database,
						schema)) {
					String sql = queryUtil.createIndex("ENGINEPERMISSION_FAVORITE_INDEX", "ENGINEPERMISSION",
							"FAVORITE");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
				if (!queryUtil.indexExists(securityDb, "ENGINEPERMISSION_USERID_INDEX", "ENGINEPERMISSION", database,
						schema)) {
					String sql = queryUtil.createIndex("ENGINEPERMISSION_USERID_INDEX", "ENGINEPERMISSION", "USERID");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}

			/*
			 *
			 *
			 * ADDING IN INITIAL PROJECT TABLES
			 * 
			 */

			// PROJECT
			// Type and cost are the main questions -
			boolean projectExists = queryUtil.tableExists(conn, "PROJECT", database, schema);
			colNames = new String[] { "PROJECTID", "PROJECTNAME", "PROJECTDISPLAYNAME", "GLOBAL", "DISCOVERABLE",
					"CREATEDBY", "CREATEDBYTYPE", "DATECREATED", "DATELASTEDITED", "TYPE", "COST", "CATALOGNAME",
					"HASPORTAL", "PORTALNAME", "PORTALPUBLISHED", "PORTALPUBLISHEDUSER", "PORTALPUBLISHEDTYPE",
					"REACTORSCOMPILED", "REACTORSCOMPILEDUSER", "REACTORSCOMPILEDTYPE" };
			types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", BOOLEAN_DATATYPE_NAME,
					BOOLEAN_DATATYPE_NAME, "VARCHAR(255)", "VARCHAR(255)", TIMESTAMP_DATATYPE_NAME,
					TIMESTAMP_DATATYPE_NAME, "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", BOOLEAN_DATATYPE_NAME,
					"VARCHAR(255)", TIMESTAMP_DATATYPE_NAME, "VARCHAR(255)", "VARCHAR(255)", TIMESTAMP_DATATYPE_NAME,
					"VARCHAR(255)", "VARCHAR(255)" };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists("PROJECT", colNames, types);
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "PROJECT", database, schema)) {
					// make the table
					String sql = queryUtil.createTable("PROJECT", colNames, types);
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}
			// 2023-01-31
			// HAVE A LOT OF COLUMN CHECKS SO NOW JUST LOOPING THROUGH ALL OF THEM
			{
				List<String> projectCols = queryUtil.getTableColumns(conn, "PROJECT", database, schema);
				for (int i = 0; i < colNames.length; i++) {
					String col = colNames[i];
					if (!projectCols.contains(col) && !projectCols.contains(col.toLowerCase())) {
						classLogger.info("Column '{}' is not present in current list of columns: {}", col, projectCols);
						String addColumnSql = queryUtil.alterTableAddColumn("PROJECT", col, types[i]);
						classLogger.info("Running sql {}", addColumnSql);
						securityDb.insertData(addColumnSql);
					}
				}

				// backfill display name from canonical name for existing rows
				securityDb.insertData(
						"UPDATE PROJECT SET PROJECTDISPLAYNAME = PROJECTNAME WHERE PROJECTDISPLAYNAME IS NULL OR PROJECTDISPLAYNAME = ''");
			}
			if (allowIfExistsIndexs) {
				String sql = queryUtil.createIndexIfNotExists("PROJECT_GLOBAL_INDEX", "PROJECT", "GLOBAL");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);

				sql = queryUtil.createIndexIfNotExists("PROJECT_DISCOVERABLE_INDEX", "PROJECT", "DISCOVERABLE");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);

				sql = queryUtil.createIndexIfNotExists("PROJECT_PROJECTENAME_INDEX", "PROJECT", "PROJECTNAME");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);

				sql = queryUtil.createIndexIfNotExists("PROJECT_PROJECTID_INDEX", "PROJECT", "PROJECTID");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);

				sql = queryUtil.createIndexIfNotExists("PROJECT_PROJECTDISPLAYNAME_INDEX", "PROJECT",
						"PROJECTDISPLAYNAME");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if index exists
				if (!queryUtil.indexExists(securityDb, "PROJECT_GLOBAL_INDEX", "PROJECT", database, schema)) {
					String sql = queryUtil.createIndex("PROJECT_GLOBAL_INDEX", "PROJECT", "GLOBAL");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
				if (!queryUtil.indexExists(securityDb, "PROJECT_DISCOVERABLE_INDEX", "PROJECT", database, schema)) {
					String sql = queryUtil.createIndex("PROJECT_DISCOVERABLE_INDEX", "PROJECT", "DISCOVERABLE");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
				if (!queryUtil.indexExists(securityDb, "PROJECT_PROJECTENAME_INDEX", "PROJECT", database, schema)) {
					String sql = queryUtil.createIndex("PROJECT_PROJECTENAME_INDEX", "PROJECT", "PROJECTNAME");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
				if (!queryUtil.indexExists(securityDb, "PROJECT_PROJECTID_INDEX", "PROJECT", database, schema)) {
					String sql = queryUtil.createIndex("PROJECT_PROJECTID_INDEX", "PROJECT", "PROJECTID");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
				if (!queryUtil.indexExists(securityDb, "PROJECT_PROJECTDISPLAYNAME_INDEX", "PROJECT", database,
						schema)) {
					String sql = queryUtil.createIndex("PROJECT_PROJECTDISPLAYNAME_INDEX", "PROJECT",
							"PROJECTDISPLAYNAME");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}

			List<String> newProjectsAutoAdded = new ArrayList<>();
			if (!projectExists) {
				try (IRawSelectWrapper wrapper2 = WrapperManager.getInstance().getRawWrapper(securityDb,
						"select engineid, enginename, global, discoverable from engine")) {
					while (wrapper2.hasNext()) {
						Object[] values = wrapper2.next().getValues();
						// insert into project table
						securityDb.insertData(queryUtil.insertIntoTable("PROJECT", colNames, types,
								new Object[] { values[1], values[0], values[2], values[3], null, null }));

						// store this so we also move over permissions
						// this is the engine id which is the same as the project id
						newProjectsAutoAdded.add(values[0] + "");
					}
				} catch (Exception e) {
					classLogger.error("Error migrating ENGINE records into PROJECT.", e);
				}
			}

			// PROJECTMETA
			// check if column exists
			colNames = new String[] { "PROJECTID", "METAKEY", "METAVALUE", "METAORDER" };
			types = new String[] { "VARCHAR(255)", "VARCHAR(255)", CLOB_DATATYPE_NAME, INTEGER_DATATYPE_NAME };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists("PROJECTMETA", colNames, types);
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "PROJECTMETA", database, schema)) {
					// make the table
					String sql = queryUtil.createTable("PROJECTMETA", colNames, types);
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}
			if (allowIfExistsIndexs) {
				String sql = queryUtil.createIndexIfNotExists("PROJECTMETA_PROJECTID_INDEX", "PROJECTMETA",
						"PROJECTID");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if index exists
				if (!queryUtil.indexExists(securityDb, "PROJECTMETA_PROJECTID_INDEX", "PROJECTMETA", database,
						schema)) {
					String sql = queryUtil.createIndex("PROJECTMETA_PROJECTID_INDEX", "PROJECTMETA", "PROJECTID");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}

			// PROJECTPERMISSION
			boolean projectPermissionExists = queryUtil.tableExists(conn, "PROJECTPERMISSION", database, schema);
			colNames = new String[] { "USERID", "PERMISSION", "PROJECTID", "VISIBILITY", "FAVORITE",
					"PERMISSIONGRANTEDBY", "PERMISSIONGRANTEDBYTYPE", "DATEADDED", "ENDDATE" };
			types = new String[] { "VARCHAR(255)", INTEGER_DATATYPE_NAME, "VARCHAR(255)", BOOLEAN_DATATYPE_NAME,
					BOOLEAN_DATATYPE_NAME, "VARCHAR(255)", "VARCHAR(255)", TIMESTAMP_DATATYPE_NAME,
					TIMESTAMP_DATATYPE_NAME };
			defaultValues = new Object[] { null, null, null, true, false, null, null, null, null };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExistsWithDefaults("PROJECTPERMISSION", colNames, types,
						defaultValues);
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "PROJECTPERMISSION", database, schema)) {
					// make the table
					String sql = queryUtil.createTable("PROJECTPERMISSION", colNames, types);
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}

			// TEMPORARY CHECK! - ADDED 09/18/2023
			if (projectPermissionExists) {
				List<String> allCols = queryUtil.getTableColumns(conn, "PROJECTPERMISSION", database, schema);
				for (int i = 0; i < colNames.length; i++) {
					String col = colNames[i];
					if (!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
						classLogger.info("Column '{}' is not present in current list of columns: {}", col, allCols);
						String addColumnSql = queryUtil.alterTableAddColumn("PROJECTPERMISSION", col, types[i]);
						classLogger.info("Running sql {}", addColumnSql);
						securityDb.insertData(addColumnSql);
					}
				}
			} else {
				// copy over engine permission to project permission for legacy installations
				try (IRawSelectWrapper wrapper2 = WrapperManager.getInstance().getRawWrapper(securityDb,
						"select userid, permission, engineid, visibility, favorite from enginepermission")) {
					while (wrapper2.hasNext()) {
						Object[] values = wrapper2.next().getValues();
						// if the project exists - we will insert it
						if (newProjectsAutoAdded.contains(values[2])) {
							// insert into project permission table
							securityDb.insertData(
									queryUtil.insertIntoTable("PROJECTPERMISSION", colNames, types, values));
						}
					}
				} catch (Exception e) {
					classLogger.error("Error migrating ENGINEPERMISSION records into PROJECTPERMISSION.", e);
				}
			}

			if (allowIfExistsIndexs) {
				String sql = queryUtil.createIndexIfNotExists("PROJECTPERMISSION_PERMISSION_INDEX", "PROJECTPERMISSION",
						"PERMISSION");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);

				sql = queryUtil.createIndexIfNotExists("PROJECTPERMISSION_VISIBILITY_INDEX", "PROJECTPERMISSION",
						"VISIBILITY");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);

				sql = queryUtil.createIndexIfNotExists("PROJECTPERMISSION_PROJECTID_INDEX", "PROJECTPERMISSION",
						"PROJECTID");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);

				sql = queryUtil.createIndexIfNotExists("PROJECTPERMISSION_FAVORITE_INDEX", "PROJECTPERMISSION",
						"FAVORITE");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);

				sql = queryUtil.createIndexIfNotExists("PROJECTPERMISSION_USERID_INDEX", "PROJECTPERMISSION", "USERID");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if index exists
				if (!queryUtil.indexExists(securityDb, "PROJECTPERMISSION_PERMISSION_INDEX", "PROJECTPERMISSION",
						database, schema)) {
					String sql = queryUtil.createIndex("PROJECTPERMISSION_PERMISSION_INDEX", "PROJECTPERMISSION",
							"PERMISSION");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
				if (!queryUtil.indexExists(securityDb, "PROJECTPERMISSION_VISIBILITY_INDEX", "PROJECTPERMISSION",
						database, schema)) {
					String sql = queryUtil.createIndex("PROJECTPERMISSION_VISIBILITY_INDEX", "PROJECTPERMISSION",
							"VISIBILITY");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
				if (!queryUtil.indexExists(securityDb, "PROJECTPERMISSION_PROJECTID_INDEX", "PROJECTPERMISSION",
						database, schema)) {
					String sql = queryUtil.createIndex("PROJECTPERMISSION_PROJECTID_INDEX", "PROJECTPERMISSION",
							"PROJECTID");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
				if (!queryUtil.indexExists(securityDb, "PROJECTPERMISSION_FAVORITE_INDEX", "PROJECTPERMISSION",
						database, schema)) {
					String sql = queryUtil.createIndex("PROJECTPERMISSION_FAVORITE_INDEX", "PROJECTPERMISSION",
							"FAVORITE");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
				if (!queryUtil.indexExists(securityDb, "PROJECTPERMISSION_USERID_INDEX", "PROJECTPERMISSION", database,
						schema)) {
					String sql = queryUtil.createIndex("PROJECTPERMISSION_USERID_INDEX", "PROJECTPERMISSION", "USERID");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}

			// PROJECTDEPENDENCIES
			colNames = new String[] { "PROJECTID", "ENGINEID", "ENGINETYPE", "USERID", "TYPE", "DATEADDED" };
			types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)",
					TIMESTAMP_DATATYPE_NAME };
			defaultValues = null;
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists("PROJECTDEPENDENCIES", colNames, types);
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "PROJECTDEPENDENCIES", database, schema)) {
					// make the table
					String sql = queryUtil.createTable("PROJECTDEPENDENCIES", colNames, types);
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}
			// handle column changes
			{
				List<String> projectCols = queryUtil.getTableColumns(conn, "PROJECTDEPENDENCIES", database, schema);
				for (int i = 0; i < colNames.length; i++) {
					String col = colNames[i];
					if (!projectCols.contains(col) && !projectCols.contains(col.toLowerCase())) {
						classLogger.info("Column '{}' is not present in current list of columns: {}", col, projectCols);
						String addColumnSql = queryUtil.alterTableAddColumn("PROJECTDEPENDENCIES", col, types[i]);
						classLogger.info("Running sql {}", addColumnSql);
						securityDb.insertData(addColumnSql);
					}
				}
			}
			performDependencyUpdate(securityDb, queryUtil, colNames, types, conn, database, schema, allowIfExistsTable);

			/**
			 * 
			 * END PROJECT TABLES
			 * 
			 */

			// ASSETENGINE
			colNames = new String[] { "USERID", "TYPE", "PROJECTID" };
			types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)" };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists("ASSETENGINE", colNames, types);
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "ASSETENGINE", database, schema)) {
					// make the table
					String sql = queryUtil.createTable("ASSETENGINE", colNames, types);
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}
			// MAKING MODIFICATION FROM ENGINEID TO PROJECTID - 04/22/2021
			{
				List<String> allCols = queryUtil.getTableColumns(conn, "ASSETENGINE", database, schema);
				// this should return in all upper case
				// ... but sometimes it is not -_- i.e. postgres always lowercases
				if ((!allCols.contains("PROJECTID") && !allCols.contains("projectid"))
						&& (allCols.contains("ENGINEID") || allCols.contains("engineid"))) {
					String updateColName = queryUtil.modColumnName("ASSETENGINE", "ENGINEID", "PROJECTID");
					classLogger.info("Running sql {}", updateColName);
					securityDb.insertData(updateColName);
				}
			}
			if (allowIfExistsIndexs) {
				String sql = queryUtil.createIndexIfNotExists("ASSETENGINE_TYPE_INDEX", "ASSETENGINE", "TYPE");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
				sql = queryUtil.createIndexIfNotExists("ASSETENGINE_USERID_INDEX", "ASSETENGINE", "USERID");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if index exists
				if (!queryUtil.indexExists(securityDb, "ASSETENGINE_TYPE_INDEX", "ASSETENGINE", database, schema)) {
					String sql = queryUtil.createIndex("ASSETENGINE_TYPE_INDEX", "ASSETENGINE", "TYPE");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
				if (!queryUtil.indexExists(securityDb, "ASSETENGINE_USERID_INDEX", "ASSETENGINE", database, schema)) {
					String sql = queryUtil.createIndex("ASSETENGINE_USERID_INDEX", "ASSETENGINE", "USERID");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}

			// INSIGHT
			colNames = new String[] { "PROJECTID", "INSIGHTID", "INSIGHTNAME", "GLOBAL", "EXECUTIONCOUNT", "CREATEDON",
					"LASTMODIFIEDON", "LAYOUT", "CACHEABLE", "CACHEMINUTES", "CACHECRON", "CACHEDON", "CACHEENCRYPT",
					"RECIPE", "SCHEMANAME" };
			types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", BOOLEAN_DATATYPE_NAME, "BIGINT",
					TIMESTAMP_DATATYPE_NAME, TIMESTAMP_DATATYPE_NAME, "VARCHAR(255)", BOOLEAN_DATATYPE_NAME,
					INTEGER_DATATYPE_NAME, "VARCHAR(25)", TIMESTAMP_DATATYPE_NAME, BOOLEAN_DATATYPE_NAME,
					CLOB_DATATYPE_NAME, "VARCHAR(255)" };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists("INSIGHT", colNames, types);
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "INSIGHT", database, schema)) {
					// make the table
					String sql = queryUtil.createTable("INSIGHT", colNames, types);
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}
			// MAKING MODIFICATION FROM ENGINEID TO PROJECTID - 04/22/2021
			{
				List<String> insightCols = queryUtil.getTableColumns(conn, "INSIGHT", database, schema);
				// this should return in all upper case
				// ... but sometimes it is not -_- i.e. postgres always lowercases
				if ((!insightCols.contains("PROJECTID") && !insightCols.contains("projectid"))
						&& (insightCols.contains("ENGINEID") || insightCols.contains("engineid"))) {
					String updateColName = queryUtil.modColumnName("INSIGHT", "ENGINEID", "PROJECTID");
					classLogger.info("Running sql {}", updateColName);
					securityDb.insertData(updateColName);
				}
				// 2023-01-31
				// HAVE A LOT OF COLUMN CHECKS SO NOW JUST LOOPING THROUGH ALL OF THEM
				for (int i = 0; i < colNames.length; i++) {
					String col = colNames[i];
					if (!insightCols.contains(col) && !insightCols.contains(col.toLowerCase())) {
						classLogger.info("Column '{}' is not present in current list of columns: {}", col, insightCols);
						String addColumnSql = queryUtil.alterTableAddColumn("INSIGHT", col, types[i]);
						classLogger.info("Running sql {}", addColumnSql);
						securityDb.insertData(addColumnSql);
					}
				}
			}
			if (allowIfExistsIndexs) {
				String sql = queryUtil.createIndexIfNotExists("INSIGHT_LASTMODIFIEDON_INDEX", "INSIGHT",
						"LASTMODIFIEDON");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
				sql = queryUtil.createIndexIfNotExists("INSIGHT_GLOBAL_INDEX", "INSIGHT", "GLOBAL");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
				sql = queryUtil.createIndexIfNotExists("INSIGHT_PROJECTID_INDEX", "INSIGHT", "PROJECTID");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
				sql = queryUtil.createIndexIfNotExists("INSIGHT_INSIGHTID_INDEX", "INSIGHT", "INSIGHTID");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if index exists
				if (!queryUtil.indexExists(securityDb, "INSIGHT_LASTMODIFIEDON_INDEX", "INSIGHT", database, schema)) {
					String sql = queryUtil.createIndex("INSIGHT_LASTMODIFIEDON_INDEX", "INSIGHT", "LASTMODIFIEDON");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
				if (!queryUtil.indexExists(securityDb, "INSIGHT_GLOBAL_INDEX", "INSIGHT", database, schema)) {
					String sql = queryUtil.createIndex("INSIGHT_GLOBAL_INDEX", "INSIGHT", "GLOBAL");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
				if (!queryUtil.indexExists(securityDb, "INSIGHT_PROJECTID_INDEX", "INSIGHT", database, schema)) {
					String sql = queryUtil.createIndex("INSIGHT_PROJECTID_INDEX", "INSIGHT", "PROJECTID");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
				if (!queryUtil.indexExists(securityDb, "INSIGHT_INSIGHTID_INDEX", "INSIGHT", database, schema)) {
					String sql = queryUtil.createIndex("INSIGHT_INSIGHTID_INDEX", "INSIGHT", "INSIGHTID");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}

			// USERINSIGHTPERMISSION
			colNames = new String[] { "USERID", "PROJECTID", "INSIGHTID", "PERMISSION", "FAVORITE",
					"PERMISSIONGRANTEDBY", "PERMISSIONGRANTEDBYTYPE", "DATEADDED", "ENDDATE" };
			types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", INTEGER_DATATYPE_NAME,
					BOOLEAN_DATATYPE_NAME, "VARCHAR(255)", "VARCHAR(255)", TIMESTAMP_DATATYPE_NAME,
					TIMESTAMP_DATATYPE_NAME };
			defaultValues = new Object[] { null, null, null, null, false, null, null, null, null };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists("USERINSIGHTPERMISSION", colNames, types);
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "USERINSIGHTPERMISSION", database, schema)) {
					// make the table
					String sql = queryUtil.createTable("USERINSIGHTPERMISSION", colNames, types);
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}
			// MAKING MODIFICATION FROM ENGINEID TO PROJECTID - 04/22/2021
			{
				List<String> allCols = queryUtil.getTableColumns(conn, "USERINSIGHTPERMISSION", database, schema);
				// this should return in all upper case
				// ... but sometimes it is not -_- i.e. postgres always lowercases
				if ((!allCols.contains("PROJECTID") && !allCols.contains("projectid"))
						&& (allCols.contains("ENGINEID") || allCols.contains("engineid"))) {
					String updateColName = queryUtil.modColumnName("USERINSIGHTPERMISSION", "ENGINEID", "PROJECTID");
					classLogger.info("Running sql {}", updateColName);
					securityDb.insertData(updateColName);
				}
			}
			// TEMPORARY CHECK! - ADDED 09/19/2021
			{
				List<String> allCols = queryUtil.getTableColumns(conn, "USERINSIGHTPERMISSION", database, schema);
				for (int i = 0; i < colNames.length; i++) {
					String col = colNames[i];
					if (!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
						classLogger.info("Column '{}' is not present in current list of columns: {}", col, allCols);
						String addColumnSql = queryUtil.alterTableAddColumn("USERINSIGHTPERMISSION", col, types[i]);
						classLogger.info("Running sql {}", addColumnSql);
						securityDb.insertData(addColumnSql);
					}
				}
			}

			if (allowIfExistsIndexs) {
				String sql = queryUtil.createIndexIfNotExists("USERINSIGHTPERMISSION_PERMISSION_INDEX",
						"USERINSIGHTPERMISSION", "PERMISSION");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
				sql = queryUtil.createIndexIfNotExists("USERINSIGHTPERMISSION_PROJECTID_INDEX", "USERINSIGHTPERMISSION",
						"PROJECTID");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
				sql = queryUtil.createIndexIfNotExists("USERINSIGHTPERMISSION_USERID_INDEX", "USERINSIGHTPERMISSION",
						"USERID");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
				sql = queryUtil.createIndexIfNotExists("USERINSIGHTPERMISSION_FAVORITE_INDEX", "USERINSIGHTPERMISSION",
						"FAVORITE");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if index exists
				if (!queryUtil.indexExists(securityDb, "USERINSIGHTPERMISSION_PERMISSION_INDEX",
						"USERINSIGHTPERMISSION", database, schema)) {
					String sql = queryUtil.createIndex("USERINSIGHTPERMISSION_PERMISSION_INDEX",
							"USERINSIGHTPERMISSION", "PERMISSION");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
				if (!queryUtil.indexExists(securityDb, "USERINSIGHTPERMISSION_PROJECTID_INDEX", "USERINSIGHTPERMISSION",
						database, schema)) {
					String sql = queryUtil.createIndex("USERINSIGHTPERMISSION_PROJECTID_INDEX", "USERINSIGHTPERMISSION",
							"PROJECTID");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
				if (!queryUtil.indexExists(securityDb, "USERINSIGHTPERMISSION_USERID_INDEX", "USERINSIGHTPERMISSION",
						database, schema)) {
					String sql = queryUtil.createIndex("USERINSIGHTPERMISSION_USERID_INDEX", "USERINSIGHTPERMISSION",
							"USERID");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
				if (!queryUtil.indexExists(securityDb, "USERINSIGHTPERMISSION_FAVORITE_INDEX", "USERINSIGHTPERMISSION",
						database, schema)) {
					String sql = queryUtil.createIndex("USERINSIGHTPERMISSION_FAVORITE_INDEX", "USERINSIGHTPERMISSION",
							"FAVORITE");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}

			// INSIGHTMETA
			colNames = new String[] { "PROJECTID", "INSIGHTID", "METAKEY", "METAVALUE", "METAORDER" };
			types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", CLOB_DATATYPE_NAME,
					INTEGER_DATATYPE_NAME };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists("INSIGHTMETA", colNames, types);
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "INSIGHTMETA", database, schema)) {
					// make the table
					String sql = queryUtil.createTable("INSIGHTMETA", colNames, types);
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}
			// MAKING MODIFICATION FROM ENGINEID TO PROJECTID - 04/22/2021
			{
				List<String> allCols = queryUtil.getTableColumns(conn, "INSIGHTMETA", database, schema);
				// this should return in all upper case
				// ... but sometimes it is not -_- i.e. postgres always lowercases
				if ((!allCols.contains("PROJECTID") && !allCols.contains("projectid"))
						&& (allCols.contains("ENGINEID") || allCols.contains("engineid"))) {
					String updateColName = queryUtil.modColumnName("INSIGHTMETA", "ENGINEID", "PROJECTID");
					classLogger.info("Running sql {}", updateColName);
					securityDb.insertData(updateColName);
				}
			}
			// END MODIFICATION
			if (allowIfExistsIndexs) {
				String sql = queryUtil.createIndexIfNotExists("INSIGHTMETA_PROJECTID_INDEX", "INSIGHTMETA",
						"PROJECTID");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
				sql = queryUtil.createIndexIfNotExists("INSIGHTMETA_INSIGHTID_INDEX", "INSIGHTMETA", "INSIGHTID");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if index exists
				if (!queryUtil.indexExists(securityDb, "INSIGHTMETA_PROJECTID_INDEX", "INSIGHTMETA", database,
						schema)) {
					String sql = queryUtil.createIndex("INSIGHTMETA_PROJECTID_INDEX", "INSIGHTMETA", "PROJECTID");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
				if (!queryUtil.indexExists(securityDb, "INSIGHTMETA_INSIGHTID_INDEX", "INSIGHTMETA", database,
						schema)) {
					String sql = queryUtil.createIndex("INSIGHTMETA_INSIGHTID_INDEX", "INSIGHTMETA", "INSIGHTID");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}

			// INSIGHTFRAMES
			colNames = new String[] { "PROJECTID", "INSIGHTID", "TABLENAME", "TABLETYPE", "COLUMNNAME", "COLUMNTYPE",
					"ADDITIONALTYPE" };
			types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)",
					"VARCHAR(255)", "VARCHAR(255)" };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists("INSIGHTFRAMES", colNames, types);
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "INSIGHTFRAMES", database, schema)) {
					// make the table
					String sql = queryUtil.createTable("INSIGHTFRAMES", colNames, types);
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}
			if (allowIfExistsIndexs) {
				String sql = queryUtil.createIndexIfNotExists("INSIGHTFRAMES_PROJECTID_INDEX", "INSIGHTMETA",
						"PROJECTID");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
				sql = queryUtil.createIndexIfNotExists("INSIGHTFRAMES_INSIGHTID_INDEX", "INSIGHTMETA", "INSIGHTID");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if index exists
				if (!queryUtil.indexExists(securityDb, "INSIGHTFRAMES_PROJECTID_INDEX", "INSIGHTFRAMES", database,
						schema)) {
					String sql = queryUtil.createIndex("INSIGHTFRAMES_PROJECTID_INDEX", "INSIGHTFRAMES", "PROJECTID");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
				if (!queryUtil.indexExists(securityDb, "INSIGHTFRAMES_INSIGHTID_INDEX", "INSIGHTFRAMES", database,
						schema)) {
					String sql = queryUtil.createIndex("INSIGHTFRAMES_INSIGHTID_INDEX", "INSIGHTFRAMES", "INSIGHTID");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}

			// Altering table to store additional types for frames.
			// added on 10-26-2022
			List<String> insightFramesCols = queryUtil.getTableColumns(conn, "INSIGHTFRAMES", database, schema);
			if (!insightFramesCols.contains("ADDITIONALTYPE") && !insightFramesCols.contains("additionaltype")) {
				String addColumnSql = queryUtil.alterTableAddColumn("INSIGHTFRAMES", "ADDITIONALTYPE", "VARCHAR(255)");
				classLogger.info("Running sql {}", addColumnSql);
				securityDb.insertData(addColumnSql);
			}

			// SMSS_USER
			colNames = new String[] { "NAME", "EMAIL", "TYPE", "ID", "PASSWORD", "SALT", "USERNAME", "ADMIN",
					"PUBLISHER", "EXPORTER", "DATECREATED", "LASTLOGIN", "LASTPASSWORDRESET", "LOCKED", "PHONE",
					"PHONEEXTENSION", "COUNTRYCODE", "MODELUSAGERESTRICTION", "MODELMAXTOKENS", "MODELMAXRESPONSETIME",
					"MODELUSAGEFREQUENCY" };
			types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)",
					"VARCHAR(255)", "VARCHAR(255)", BOOLEAN_DATATYPE_NAME, BOOLEAN_DATATYPE_NAME, BOOLEAN_DATATYPE_NAME,
					TIMESTAMP_DATATYPE_NAME, TIMESTAMP_DATATYPE_NAME, TIMESTAMP_DATATYPE_NAME, BOOLEAN_DATATYPE_NAME,
					"VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", INTEGER_DATATYPE_NAME,
					DOBLE_DATATYPE_NAME, "VARCHAR(255)" };
			// TEMPORARY CHECK! - 2021-01-17 this table used to be USER
			// but some rdbms types (postgres) does not allow it
			// so i am going ahead and moving over user to smss_user
			if (queryUtil.tableExists(conn, "USER", database, schema)) {
				performSmssUserTemporaryUpdate(securityDb, queryUtil, colNames, types, conn, database, schema,
						allowIfExistsTable);
			} else {
				if (allowIfExistsTable) {
					String sql = queryUtil.createTableIfNotExists("SMSS_USER", colNames, types);
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				} else {
					// see if table exists
					if (!queryUtil.tableExists(conn, "SMSS_USER", database, schema)) {
						// make the table
						String sql = queryUtil.createTable("SMSS_USER", colNames, types);
						classLogger.info("Running sql {}", sql);
						securityDb.insertData(sql);
					}
				}
			}
			// 2023-01-31
			// HAVE A LOT OF COLUMN CHECKS SO NOW JUST LOOPING THROUGH ALL OF THEM
			{
				List<String> smssUserCols = queryUtil.getTableColumns(conn, "SMSS_USER", database, schema);
				for (int i = 0; i < colNames.length; i++) {
					String col = colNames[i];
					if (!smssUserCols.contains(col) && !smssUserCols.contains(col.toLowerCase())) {
						classLogger.info("Column '{}' is not present in current list of columns: {}", col,
								smssUserCols);
						String addColumnSql = queryUtil.alterTableAddColumn("SMSS_USER", col, types[i]);
						classLogger.info("Running sql {}", addColumnSql);
						securityDb.insertData(addColumnSql);
					}
				}
			}
			if (allowIfExistsIndexs) {
				String sql = queryUtil.createIndexIfNotExists("SMSS_USER_ID_INDEX", "SMSS_USER", "ID");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if index exists
				if (!queryUtil.indexExists(securityDb, "SMSS_USER_ID_INDEX", "SMSS_USER", database, schema)) {
					String sql = queryUtil.createIndex("SMSS_USER_ID_INDEX", "SMSS_USER", "ID");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}

			// SMSS_USER_ACCESS_KEYS
			colNames = new String[] { "USERID", "TYPE", "ACCESSKEY", "SECRETKEY", "SECRETSALT", "DATECREATED",
					"LASTUSED", "TOKENNAME", "TOKENDESCRIPTION" };
			types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)",
					TIMESTAMP_DATATYPE_NAME, TIMESTAMP_DATATYPE_NAME, "VARCHAR(255)", "VARCHAR(500)" };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists("SMSS_USER_ACCESS_KEYS", colNames, types);
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "SMSS_USER_ACCESS_KEYS", database, schema)) {
					// make the table
					String sql = queryUtil.createTable("SMSS_USER_ACCESS_KEYS", colNames, types);
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}
			{
				List<String> allCols = queryUtil.getTableColumns(conn, "SMSS_USER_ACCESS_KEYS", database, schema);
				for (int i = 0; i < colNames.length; i++) {
					String col = colNames[i];
					if (!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
						classLogger.info("Column '{}' is not present in current list of columns: {}", col, allCols);
						String addColumnSql = queryUtil.alterTableAddColumn("SMSS_USER_ACCESS_KEYS", col, types[i]);
						classLogger.info("Running sql {}", addColumnSql);
						securityDb.insertData(addColumnSql);
					}
				}
			}

			/*
			 * We need to store when a user comes in if they are part of a group what level
			 * of permission does this give the user for a respective database or project or
			 * insight
			 * 
			 * We do not need to store the user -> group mapping (yet - will think about
			 * future custom groups) The SOT will be the IDP that will give us the updated
			 * groups each time the user logs in
			 */

			// GROUP TABLE
			colNames = new String[] { "ID", "TYPE", "DESCRIPTION", "DATEADDED", "USERID", "USERIDTYPE" };
			types = new String[] { "VARCHAR(255)", "VARCHAR(255)", CLOB_DATATYPE_NAME, TIMESTAMP_DATATYPE_NAME,
					"VARCHAR(255)", "VARCHAR(255)" };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists("SMSS_GROUP", colNames, types);
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "SMSS_GROUP", database, schema)) {
					// make the table
					String sql = queryUtil.createTable("SMSS_GROUP", colNames, types);
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}
			{
				List<String> allCols = queryUtil.getTableColumns(conn, "SMSS_GROUP", database, schema);
				for (int i = 0; i < colNames.length; i++) {
					String col = colNames[i];
					if (!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
						classLogger.info("Column '{}' is not present in current list of columns: {}", col, allCols);
						String addColumnSql = queryUtil.alterTableAddColumn("SMSS_GROUP", col, types[i]);
						classLogger.info("Running sql {}", addColumnSql);
						securityDb.insertData(addColumnSql);
					}
				}
			}

			// CUSTOM GROUP ASSIGNMENT TABLE
			colNames = new String[] { "GROUPID", "USERID", "TYPE", "DATEADDED", "ENDDATE", "PERMISSIONGRANTEDBY",
					"PERMISSIONGRANTEDBYTYPE" };
			types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", TIMESTAMP_DATATYPE_NAME,
					TIMESTAMP_DATATYPE_NAME, "VARCHAR(255)", "VARCHAR(255)" };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists("CUSTOMGROUPASSIGNMENT", colNames, types);
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "CUSTOMGROUPASSIGNMENT", database, schema)) {
					// make the table
					String sql = queryUtil.createTable("CUSTOMGROUPASSIGNMENT", colNames, types);
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}
			{
				List<String> allCols = queryUtil.getTableColumns(conn, "CUSTOMGROUPASSIGNMENT", database, schema);
				for (int i = 0; i < colNames.length; i++) {
					String col = colNames[i];
					if (!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
						classLogger.info("Column '{}' is not present in current list of columns: {}", col, allCols);
						String addColumnSql = queryUtil.alterTableAddColumn("CUSTOMGROUPASSIGNMENT", col, types[i]);
						classLogger.info("Running sql {}", addColumnSql);
						securityDb.insertData(addColumnSql);
					}
				}
			}

			// GROUP ENGINE PERMISSION
			// TODO::: look into how we want to allow user hiding of dbs that are assigned
			// at group lvl
			colNames = new String[] { "ID", "TYPE", "ENGINEID", "PERMISSION", "DATEADDED", "ENDDATE",
					"PERMISSIONGRANTEDBY", "PERMISSIONGRANTEDBYTYPE" };
			types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", INTEGER_DATATYPE_NAME,
					TIMESTAMP_DATATYPE_NAME, TIMESTAMP_DATATYPE_NAME, "VARCHAR(255)", "VARCHAR(255)" };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists("GROUPENGINEPERMISSION", colNames, types);
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "GROUPENGINEPERMISSION", database, schema)) {
					// make the table
					String sql = queryUtil.createTable("GROUPENGINEPERMISSION", colNames, types);
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}

			// TEMPORARY CHECK! - ADDED 10/04/2023
			{
				List<String> allCols = queryUtil.getTableColumns(conn, "GROUPENGINEPERMISSION", database, schema);
				for (int i = 0; i < colNames.length; i++) {
					String col = colNames[i];
					if (!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
						classLogger.info("Column '{}' is not present in current list of columns: {}", col, allCols);
						String addColumnSql = queryUtil.alterTableAddColumn("GROUPENGINEPERMISSION", col, types[i]);
						classLogger.info("Running sql {}", addColumnSql);
						securityDb.insertData(addColumnSql);
					}
				}
			}

			// GROUP PROJECT PERMISSION
			// TODO::: look into how we want to allow user hiding of projects that are
			// assigned at group lvl
			colNames = new String[] { "ID", "TYPE", "PROJECTID", "PERMISSION", "DATEADDED", "ENDDATE",
					"PERMISSIONGRANTEDBY", "PERMISSIONGRANTEDBYTYPE" };
			types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", INTEGER_DATATYPE_NAME,
					TIMESTAMP_DATATYPE_NAME, TIMESTAMP_DATATYPE_NAME, "VARCHAR(255)", "VARCHAR(255)" };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists("GROUPPROJECTPERMISSION", colNames, types);
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "GROUPPROJECTPERMISSION", database, schema)) {
					// make the table
					String sql = queryUtil.createTable("GROUPPROJECTPERMISSION", colNames, types);
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}

			// TEMPORARY CHECK! - ADDED 10/04/2023
			{
				List<String> allCols = queryUtil.getTableColumns(conn, "GROUPPROJECTPERMISSION", database, schema);
				for (int i = 0; i < colNames.length; i++) {
					String col = colNames[i];
					if (!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
						classLogger.info("Column '{}' is not present in current list of columns: {}", col, allCols);
						String addColumnSql = queryUtil.alterTableAddColumn("GROUPPROJECTPERMISSION", col, types[i]);
						classLogger.info("Running sql {}", addColumnSql);
						securityDb.insertData(addColumnSql);
					}
				}
			}

			// GROUP INSIGHT PERMISSION
			colNames = new String[] { "ID", "TYPE", "PROJECTID", "INSIGHTID", "PERMISSION", "DATEADDED", "ENDDATE",
					"PERMISSIONGRANTEDBY", "PERMISSIONGRANTEDBYTYPE" };
			types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)",
					INTEGER_DATATYPE_NAME, TIMESTAMP_DATATYPE_NAME, TIMESTAMP_DATATYPE_NAME, "VARCHAR(255)",
					"VARCHAR(255)" };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists("GROUPINSIGHTPERMISSION", colNames, types);
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "GROUPINSIGHTPERMISSION", database, schema)) {
					// make the table
					String sql = queryUtil.createTable("GROUPINSIGHTPERMISSION", colNames, types);
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}

			// TEMPORARY CHECK! - ADDED 10/04/2023
			{
				List<String> allCols = queryUtil.getTableColumns(conn, "GROUPINSIGHTPERMISSION", database, schema);
				for (int i = 0; i < colNames.length; i++) {
					String col = colNames[i];
					if (!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
						classLogger.info("Column '{}' is not present in current list of columns: {}", col, allCols);
						String addColumnSql = queryUtil.alterTableAddColumn("GROUPINSIGHTPERMISSION", col, types[i]);
						classLogger.info("Running sql {}", addColumnSql);
						securityDb.insertData(addColumnSql);
					}
				}
			}

			{
				// 2023-08-03
				// RENAME DATABASEACCESSREQUEST TO ENGINEACCESSREQUEST
				if (allowIfExistsTable) {
					String sql = queryUtil.dropTableIfExists("DATABASEACCESSREQUEST");
					classLogger.info("Running sql {}", sql);
					securityDb.removeData(sql);
				} else {
					if (queryUtil.tableExists(conn, "DATABASEACCESSREQUEST ", database, schema)) {
						String sql = queryUtil.dropTable("DATABASEACCESSREQUEST");
						classLogger.info("Running sql {}", sql);
						securityDb.removeData(sql);
					}
				}
			}
			// ENGINEACCESSREQUEST
			colNames = new String[] { "ID", "REQUEST_USERID", "REQUEST_TYPE", "REQUEST_TIMESTAMP", "ENGINEID",
					"PERMISSION", "REQUEST_REASON", "APPROVER_USERID", "APPROVER_TYPE", "APPROVER_DECISION",
					"APPROVER_TIMESTAMP", "SUBMITTED_BY_USERID", "SUBMITTED_BY_TYPE" };
			types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", TIMESTAMP_DATATYPE_NAME,
					"VARCHAR(255)", INTEGER_DATATYPE_NAME, CLOB_DATATYPE_NAME, "VARCHAR(255)", "VARCHAR(255)",
					"VARCHAR(255)", TIMESTAMP_DATATYPE_NAME, "VARCHAR(255)", "VARCHAR(255)" };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists("ENGINEACCESSREQUEST ", colNames, types);
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "ENGINEACCESSREQUEST ", database, schema)) {
					// make the table
					String sql = queryUtil.createTable("ENGINEACCESSREQUEST ", colNames, types);
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}
			// 2023-09-11
			// HAVE A LOT OF COLUMN CHECKS SO NOW JUST LOOPING THROUGH ALL OF THEM
			{
				List<String> allCols = queryUtil.getTableColumns(conn, "ENGINEACCESSREQUEST", database, schema);
				for (int i = 0; i < colNames.length; i++) {
					String col = colNames[i];
					if (!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
						classLogger.info("Column '{}' is not present in current list of columns: {}", col, allCols);
						String addColumnSql = queryUtil.alterTableAddColumn("ENGINEACCESSREQUEST", col, types[i]);
						classLogger.info("Running sql {}", addColumnSql);
						securityDb.insertData(addColumnSql);
					}
				}
			}

			// PROJECTACCESSREQUEST
			colNames = new String[] { "ID", "REQUEST_USERID", "REQUEST_TYPE", "REQUEST_TIMESTAMP", "PROJECTID",
					"PERMISSION", "REQUEST_REASON", "APPROVER_USERID", "APPROVER_TYPE", "APPROVER_DECISION",
					"APPROVER_TIMESTAMP", "SUBMITTED_BY_USERID", "SUBMITTED_BY_TYPE" };
			types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", TIMESTAMP_DATATYPE_NAME,
					"VARCHAR(255)", INTEGER_DATATYPE_NAME, CLOB_DATATYPE_NAME, "VARCHAR(255)", "VARCHAR(255)",
					"VARCHAR(255)", TIMESTAMP_DATATYPE_NAME, "VARCHAR(255)", "VARCHAR(255)" };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists("PROJECTACCESSREQUEST ", colNames, types);
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "PROJECTACCESSREQUEST ", database, schema)) {
					// make the table
					String sql = queryUtil.createTable("PROJECTACCESSREQUEST ", colNames, types);
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}
			// 2023-09-11
			// HAVE A LOT OF COLUMN CHECKS SO NOW JUST LOOPING THROUGH ALL OF THEM
			{
				List<String> allCols = queryUtil.getTableColumns(conn, "PROJECTACCESSREQUEST", database, schema);
				for (int i = 0; i < colNames.length; i++) {
					String col = colNames[i];
					if (!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
						classLogger.info("Column '{}' is not present in current list of columns: {}", col, allCols);
						String addColumnSql = queryUtil.alterTableAddColumn("PROJECTACCESSREQUEST", col, types[i]);
						classLogger.info("Running sql {}", addColumnSql);
						securityDb.insertData(addColumnSql);
					}
				}
			}

			// INSIGHTACCESSREQUEST
			colNames = new String[] { "ID", "REQUEST_USERID", "REQUEST_TYPE", "REQUEST_TIMESTAMP", "PROJECTID",
					"INSIGHTID", "PERMISSION", "REQUEST_REASON", "APPROVER_USERID", "APPROVER_TYPE",
					"APPROVER_DECISION", "APPROVER_TIMESTAMP", "SUBMITTED_BY_USERID", "SUBMITTED_BY_TYPE" };
			types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", TIMESTAMP_DATATYPE_NAME,
					"VARCHAR(255)", "VARCHAR(255)", INTEGER_DATATYPE_NAME, CLOB_DATATYPE_NAME, "VARCHAR(255)",
					"VARCHAR(255)", "VARCHAR(255)", TIMESTAMP_DATATYPE_NAME, "VARCHAR(255)", "VARCHAR(255)" };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists("INSIGHTACCESSREQUEST ", colNames, types);
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "INSIGHTACCESSREQUEST ", database, schema)) {
					// make the table
					String sql = queryUtil.createTable("INSIGHTACCESSREQUEST ", colNames, types);
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}
			// 2023-09-11
			// HAVE A LOT OF COLUMN CHECKS SO NOW JUST LOOPING THROUGH ALL OF THEM
			{
				List<String> allCols = queryUtil.getTableColumns(conn, "INSIGHTACCESSREQUEST", database, schema);
				for (int i = 0; i < colNames.length; i++) {
					String col = colNames[i];
					if (!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
						classLogger.info("Column '{}' is not present in current list of columns: {}", col, allCols);
						String addColumnSql = queryUtil.alterTableAddColumn("INSIGHTACCESSREQUEST", col, types[i]);
						classLogger.info("Running sql {}", addColumnSql);
						securityDb.insertData(addColumnSql);
					}
				}
			}

			// TOKEN
			colNames = new String[] { "IPADDR", "VAL", "DATEADDED", "CLIENTID" };
			types = new String[] { "VARCHAR(255)", "VARCHAR(255)", TIMESTAMP_DATATYPE_NAME, "VARCHAR(255)" };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists("TOKEN", colNames, types);
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "TOKEN", database, schema)) {
					// make the table
					String sql = queryUtil.createTable("TOKEN", colNames, types);
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}
			// MAKING MODIFICATION FOR ADDING ID COLUMN - 10/03/2022
			{
				List<String> allCols = queryUtil.getTableColumns(conn, "TOKEN", database, schema);
				// this should return in all upper case
				// ... but sometimes it is not -_- i.e. postgres always lowercases
				if (!allCols.contains("CLIENTID") && !allCols.contains("clientid")) {
					String addIdColumn = queryUtil.alterTableAddColumn("TOKEN", "CLIENTID", "VARCHAR(255)");
					classLogger.info("Running sql {}", addIdColumn);
					securityDb.insertData(addIdColumn);
				}
			}
			if (allowIfExistsIndexs) {
				String sql = queryUtil.createIndexIfNotExists("TOKEN_IPADDR_INDEX", "TOKEN", "IPADDR");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if index exists
				if (!queryUtil.indexExists(securityDb, "TOKEN_IPADDR_INDEX", "TOKEN", database, schema)) {
					String sql = queryUtil.createIndex("TOKEN_IPADDR_INDEX", "TOKEN", "IPADDR");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}

			// PERMISSION
			colNames = new String[] { "ID", "NAME" };
			types = new String[] { INTEGER_DATATYPE_NAME, "VARCHAR(255)" };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists("PERMISSION", colNames, types);
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "PERMISSION", database, schema)) {
					// make the table
					String sql = queryUtil.createTable("PERMISSION", colNames, types);
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}
			if (allowIfExistsIndexs) {
				List<String> iCols = new ArrayList<String>();
				iCols.add("ID");
				iCols.add("NAME");
				String sql = queryUtil.createIndexIfNotExists("PERMISSION_ID_NAME_INDEX", "PERMISSION", iCols);
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if index exists
				if (!queryUtil.indexExists(securityDb, "PERMISSION_ID_NAME_INDEX", "PERMISSION", database, schema)) {
					List<String> iCols = new ArrayList<String>();
					iCols.add("ID");
					iCols.add("NAME");
					String sql = queryUtil.createIndex("PERMISSION_ID_NAME_INDEX", "PERMISSION", iCols);
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}

			{
				try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb,
						"select count(*) from permission")) {
					if (wrapper.hasNext()) {
						int numrows = ((Number) wrapper.next().getValues()[0]).intValue();
						if (numrows > 3) {
							securityDb.removeData("DELETE FROM PERMISSION WHERE 1=1;");
							securityDb.insertData(queryUtil.insertIntoTable("PERMISSION", colNames, types,
									new Object[] { 1, "OWNER" }));
							securityDb.insertData(queryUtil.insertIntoTable("PERMISSION", colNames, types,
									new Object[] { 2, "EDIT" }));
							securityDb.insertData(queryUtil.insertIntoTable("PERMISSION", colNames, types,
									new Object[] { 3, "READ_ONLY" }));
						} else if (numrows == 0) {
							securityDb.insertData(queryUtil.insertIntoTable("PERMISSION", colNames, types,
									new Object[] { 1, "OWNER" }));
							securityDb.insertData(queryUtil.insertIntoTable("PERMISSION", colNames, types,
									new Object[] { 2, "EDIT" }));
							securityDb.insertData(queryUtil.insertIntoTable("PERMISSION", colNames, types,
									new Object[] { 3, "READ_ONLY" }));
						}
					}
				} catch (Exception e) {
					classLogger.error("Error initializing default PERMISSION rows.", e);
				}
			}

			// PASSWORD RULES
			colNames = new String[] { "PASS_LENGTH", "REQUIRE_UPPER", "REQUIRE_LOWER", "REQUIRE_NUMERIC",
					"REQUIRE_SPECIAL", "EXPIRATION_DAYS", "ADMIN_RESET_EXPIRATION", "ALLOW_USER_PASS_CHANGE",
					"PASS_REUSE_COUNT", "DAYS_TO_LOCK", "DAYS_TO_LOCK_WARNING" };
			types = new String[] { INTEGER_DATATYPE_NAME, BOOLEAN_DATATYPE_NAME, BOOLEAN_DATATYPE_NAME,
					BOOLEAN_DATATYPE_NAME, BOOLEAN_DATATYPE_NAME, INTEGER_DATATYPE_NAME, BOOLEAN_DATATYPE_NAME,
					BOOLEAN_DATATYPE_NAME, INTEGER_DATATYPE_NAME, INTEGER_DATATYPE_NAME, INTEGER_DATATYPE_NAME };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists("PASSWORD_RULES", colNames, types);
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "PASSWORD_RULES", database, schema)) {
					// make the table
					String sql = queryUtil.createTable("PASSWORD_RULES", colNames, types);
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}
			// see if there are any default values
			{
				try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb,
						"select count(*) from password_rules")) {
					if (wrapper.hasNext()) {
						int numrows = ((Number) wrapper.next().getValues()[0]).intValue();
						if (numrows == 0) {
							securityDb.insertData(queryUtil.insertIntoTable("PASSWORD_RULES", colNames, types,
									new Object[] { 8, true, true, true, true, 90, false, true, 10, 0, 14 }));
						}
					}
				} catch (Exception e) {
					classLogger.error("Error initializing default PASSWORD_RULES row.", e);
				}
			}
			// 2022-03-03
			{
				// this should return in all upper case
				// ... but sometimes it is not -_- i.e. postgres always lowercases
				List<String> passwordRulesCols = queryUtil.getTableColumns(conn, "PASSWORD_RULES", database, schema);
				if (!passwordRulesCols.contains("DAYS_TO_LOCK") && !passwordRulesCols.contains("days_to_lock")) {
					String addColumnSql = queryUtil.alterTableAddColumn("PASSWORD_RULES", "DAYS_TO_LOCK",
							INTEGER_DATATYPE_NAME);
					classLogger.info("Running sql {}", addColumnSql);
					securityDb.insertData(addColumnSql);
				}
				if (!passwordRulesCols.contains("DAYS_TO_LOCK_WARNING")
						&& !passwordRulesCols.contains("days_to_lock_warning")) {
					String addColumnSql = queryUtil.alterTableAddColumn("PASSWORD_RULES", "DAYS_TO_LOCK_WARNING",
							INTEGER_DATATYPE_NAME);
					classLogger.info("Running sql {}", addColumnSql);
					securityDb.insertData(addColumnSql);
				}
			}
			// 2022-02-16
			// renamed permission rules to password rules
			if (queryUtil.tableExists(conn, "PERMISSION_RULES", database, schema)) {
				String sql = queryUtil.dropTable("PERMISSION_RULES");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			}

			// PASSWORD HISTORY
			colNames = new String[] { "ID", "USERID", "TYPE", "PASSWORD", "SALT", "DATE_ADDED" };
			types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)",
					TIMESTAMP_DATATYPE_NAME };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists("PASSWORD_HISTORY", colNames, types);
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "PASSWORD_HISTORY", database, schema)) {
					// make the table
					String sql = queryUtil.createTable("PASSWORD_HISTORY", colNames, types);
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}
			List<String> passReuseCols = queryUtil.getTableColumns(conn, "PASSWORD_HISTORY", database, schema);
			// 2022-02-16
			// this should return in all upper case
			// ... but sometimes it is not -_- i.e. postgres always lowercases
			if (!passReuseCols.contains("USERID") && !passReuseCols.contains("userid")) {
				String addColumnSql = queryUtil.alterTableAddColumn("PASSWORD_HISTORY", "USERID", "VARCHAR(255)");
				classLogger.info("Running sql {}", addColumnSql);
				securityDb.insertData(addColumnSql);
			}
			// 2022-02-16
			// renamed + old had a typo.... -_-
			if (queryUtil.tableExists(conn, "PASSWORD_RESUSE", database, schema)) {
				String sql = queryUtil.dropTable("PASSWORD_RESUSE");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			}

			// PASSWORD RESET
			colNames = new String[] { "EMAIL", "TYPE", "TOKEN", "DATE_ADDED" };
			types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", TIMESTAMP_DATATYPE_NAME };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists("PASSWORD_RESET", colNames, types);
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "PASSWORD_RESET", database, schema)) {
					// make the table
					String sql = queryUtil.createTable("PASSWORD_RESET", colNames, types);
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}

			// SESSION SHARE
			colNames = new String[] { "SHARE_VAL", "SESSION_VAL", "ROUTE_VAL", "IS_SESSION_SHARE", "IS_AUTH_SHARE",
					"DATE_ADDED", "DATE_USED", "USE_VALID", "USERID", "TYPE" };
			types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", BOOLEAN_DATATYPE_NAME,
					BOOLEAN_DATATYPE_NAME, TIMESTAMP_DATATYPE_NAME, TIMESTAMP_DATATYPE_NAME, BOOLEAN_DATATYPE_NAME,
					"VARCHAR(255)", "VARCHAR(255)" };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists("SESSION_SHARE", colNames, types);
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "SESSION_SHARE", database, schema)) {
					// make the table
					String sql = queryUtil.createTable("SESSION_SHARE", colNames, types);
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}
			// make sure all the columns are still valid
			{
				List<String> allCols = queryUtil.getTableColumns(conn, "SESSION_SHARE", database, schema);
				for (int i = 0; i < colNames.length; i++) {
					String col = colNames[i];
					if (!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
						classLogger.info("Column '{}' is not present in current list of columns: {}", col, allCols);
						String addColumnSql = queryUtil.alterTableAddColumn("SESSION_SHARE", col, types[i]);
						classLogger.info("Running sql {}", addColumnSql);
						securityDb.insertData(addColumnSql);
					}
				}
			}

			// "ENGINEMETAKEYS", "PROJECTMETAKEYS", "INSIGHTMETAKEYS"
			List<String> metaKeyTableNames = Arrays.asList(Constants.ENGINE_METAKEYS, Constants.PROJECT_METAKEYS,
					Constants.INSIGHT_METAKEYS);
			for (String tableName : metaKeyTableNames) {
				// all have the same columns and default values
				colNames = new String[] { "METAKEY", "SINGLEMULTI", "DISPLAYORDER", "DISPLAYOPTIONS", "DEFAULTVALUES" };
				types = new String[] { "VARCHAR(255)", "VARCHAR(255)", INTEGER_DATATYPE_NAME, "VARCHAR(255)",
						"VARCHAR(500)" };
				defaultValues = new Object[] { null, null, null, true, false };
				if (allowIfExistsTable) {
					String sql = queryUtil.createTableIfNotExists(tableName, colNames, types);
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				} else {
					// see if table exists
					if (!queryUtil.tableExists(conn, tableName, database, schema)) {
						// make the table
						String sql = queryUtil.createTable(tableName, colNames, types);
						classLogger.info("Running sql {}", sql);
						securityDb.insertData(sql);
					}
				}
				// check all the columns we want are there
				{
					List<String> allCols = queryUtil.getTableColumns(conn, tableName, database, schema);
					for (int i = 0; i < colNames.length; i++) {
						String col = colNames[i];
						if (!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
							classLogger.info("Column '{}' is not present in current list of columns: {}", col, allCols);
							String addColumnSql = queryUtil.alterTableAddColumn(tableName, col, types[i]);
							classLogger.info("Running sql {}", addColumnSql);
							securityDb.insertData(addColumnSql);
						}
					}
				}
				// see if there are any default values
				{
					try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb,
							"select count(*) from " + tableName)) {
						if (wrapper.hasNext()) {
							int numrows = ((Number) wrapper.next().getValues()[0]).intValue();
							if (numrows < 6) {
								securityDb.removeData("DELETE FROM " + tableName + " WHERE 1=1");
								int order = 0;
								securityDb.insertData(queryUtil.insertIntoTable(tableName, colNames, types,
										new Object[] { "description", "single", order++, "textarea", null }));
								securityDb.insertData(queryUtil.insertIntoTable(tableName, colNames, types,
										new Object[] { Constants.MARKDOWN, "single", order++, "markdown", null }));
								securityDb.insertData(queryUtil.insertIntoTable(tableName, colNames, types,
										new Object[] { "tag", "multi", order++, "multi-typeahead", null }));
								securityDb.insertData(queryUtil.insertIntoTable(tableName, colNames, types,
										new Object[] { "domain", "multi", order++, "multi-typeahead", null }));
								securityDb.insertData(queryUtil.insertIntoTable(tableName, colNames, types,
										new Object[] { "data classification", "multi", order++, "select-box",
												"Confidential,FOUO,Internal Only,IP,PII,PHI,Public,Restricted" }));
								securityDb.insertData(queryUtil.insertIntoTable(tableName, colNames, types,
										new Object[] { "data restrictions", "multi", order++, "select-box",
												"Confidential Allowed,FOUO Allowed,Internal Allowed,IP Allowed,PII Allowed,PHI Allowed,Restricted Allowed" }));
							}
						}
					} catch (Exception e) {
						classLogger.error("Error initializing default rows for {}.", tableName, e);
					}
				}
			}

			// USERMETA
			colNames = new String[] { "USERID", "TYPE", "METAKEY", "METAVALUE", "METAORDER" };
			types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", CLOB_DATATYPE_NAME,
					INTEGER_DATATYPE_NAME };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists("USERMETA", colNames, types);
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "USERMETA", database, schema)) {
					// make the table
					String sql = queryUtil.createTable("USERMETA", colNames, types);
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}
			if (allowIfExistsIndexs) {
				String sql = queryUtil.createIndexIfNotExists("USERMETA_USERID_INDEX", "USERMETA", "USERID");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if index exists
				if (!queryUtil.indexExists(securityDb, "USERMETA_USERID_INDEX", "USERMETA", database, schema)) {
					String sql = queryUtil.createIndex("USERMETA_USERID_INDEX", "USERMETA", "USERID");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}

			// USERMETAKEYS
			colNames = new String[] { "METAKEY", "SINGLEMULTI", "DISPLAYORDER", "DISPLAYOPTIONS", "DEFAULTVALUES" };
			types = new String[] { "VARCHAR(255)", "VARCHAR(255)", INTEGER_DATATYPE_NAME, "VARCHAR(255)",
					"VARCHAR(500)" };
			defaultValues = new Object[] { null, null, null, true, false };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists(Constants.USER_METAKEYS, colNames, types);
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, Constants.USER_METAKEYS, database, schema)) {
					// make the table
					String sql = queryUtil.createTable(Constants.USER_METAKEYS, colNames, types);
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}
			{
				// check all the columns we want are there
				List<String> allCols = queryUtil.getTableColumns(conn, Constants.USER_METAKEYS, database, schema);
				for (int i = 0; i < colNames.length; i++) {
					String col = colNames[i];
					if (!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
						classLogger.info("Column '{}' is not present in current list of columns: {}", col, allCols);
						String addColumnSql = queryUtil.alterTableAddColumn(Constants.USER_METAKEYS, col, types[i]);
						classLogger.info("Running sql {}", addColumnSql);
						securityDb.insertData(addColumnSql);
					}
				}
			}

			// Insert default row for DEFAULTMODEL into USERMETAKEYS if not exists
			try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb,
					"select count(*) from " + Constants.USER_METAKEYS)) {
				if (wrapper.hasNext()) {
					int count = ((Number) wrapper.next().getValues()[0]).intValue();
					if (count == 0) {
						int order = 0;
						securityDb.insertData(queryUtil.insertIntoTable(Constants.USER_METAKEYS, colNames, types,
								new Object[] { Constants.DEFAULT_TEXT_GENERATION_MODEL_KEY, "single", order++,
										"select-box", null }));
						securityDb.insertData(queryUtil.insertIntoTable(Constants.USER_METAKEYS, colNames, types,
								new Object[] { Constants.DEFAULT_CODE_GENERATION_MODEL_KEY, "single", order++,
										"select-box", null }));
					}
				}
			} catch (Exception e) {
				classLogger.error("Error initializing default USER_METAKEYS rows.", e);
			}

			// JIRA_CONNECTIONS
			colNames = new String[] { "ID", "ALIAS", "CLIENTID", "CLIENTSECRET", "SCOPE", "USERPROFILEURL" };
			types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(1000)", "VARCHAR(255)",
					"VARCHAR(255)" };

			if (allowIfExistsTable) {
				securityDb.insertData(queryUtil.createTableIfNotExists("JIRA_CONNECTIONS", colNames, types));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "JIRA_CONNECTIONS", database, schema)) {
					// make the table
					securityDb.insertData(queryUtil.createTable("JIRA_CONNECTIONS", colNames, types));
				}
			}
			{
				List<String> allCols = queryUtil.getTableColumns(conn, "JIRA_CONNECTIONS", database, schema);
				for (int i = 0; i < colNames.length; i++) {
					String col = colNames[i];
					if (!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
						classLogger.info("Column '{}' is not present in current list of columns: {}", col, allCols);
						String addColumnSql = queryUtil.alterTableAddColumn("JIRA_CONNECTIONS", col, types[i]);
						securityDb.insertData(addColumnSql);
					}
				}
			}

			// SALESFORCE_CONNECTIONS
			colNames = new String[] { "ID", "ALIAS", "CLIENTID", "CLIENTSECRET" };
			types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)" };

			if (allowIfExistsTable) {
				securityDb.insertData(queryUtil.createTableIfNotExists("SALESFORCE_CONNECTIONS", colNames, types));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "SALESFORCE_CONNECTIONS", database, schema)) {
					// make the table
					securityDb.insertData(queryUtil.createTable("SALESFORCE_CONNECTIONS", colNames, types));
				}
			}
			{
				List<String> allCols = queryUtil.getTableColumns(conn, "SALESFORCE_CONNECTIONS", database, schema);
				for (int i = 0; i < colNames.length; i++) {
					String col = colNames[i];
					if (!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
						classLogger.info("Column '{}' is not present in current list of columns: {}", col, allCols);
						String addColumnSql = queryUtil.alterTableAddColumn("SALESFORCE_CONNECTIONS", col, types[i]);
						securityDb.insertData(addColumnSql);
					}
				}
			}

			// SERVICENOW_CONNECTIONS
			colNames = new String[] { "ID", "INSTANCEURL", "ALIAS", "CLIENTID", "CLIENTSECRET", "USERPROFILEURL" };
			types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)",
					"VARCHAR(255)" };

			if (allowIfExistsTable) {
				securityDb.insertData(queryUtil.createTableIfNotExists("SERVICENOW_CONNECTIONS", colNames, types));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "SERVICENOW_CONNECTIONS", database, schema)) {
					// make the table
					securityDb.insertData(queryUtil.createTable("SERVICENOW_CONNECTIONS", colNames, types));
				}
			}
			{
				List<String> allCols = queryUtil.getTableColumns(conn, "SERVICENOW_CONNECTIONS", database, schema);
				for (int i = 0; i < colNames.length; i++) {
					String col = colNames[i];
					if (!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
						classLogger.info("Column '{}' is not present in current list of columns: {}", col, allCols);
						String addColumnSql = queryUtil.alterTableAddColumn("SERVICENOW_CONNECTIONS", col, types[i]);
						securityDb.insertData(addColumnSql);
					}
				}
			}

			// GITHUB_APP
			// output of the GitHub app-manifest conversion flow (the app's own credentials)
			colNames = new String[] { "APP_ID", "SLUG", "APP_NAME", "OWNER_LOGIN", "HTML_URL", "WEBHOOK_URL",
					"CLIENT_ID", "CLIENT_SECRET", "WEBHOOK_SECRET", "PRIVATE_KEY", "CREATED_ON", "UPDATED_ON" };
			types = new String[] { "BIGINT", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(500)",
					"VARCHAR(500)", "VARCHAR(255)", CLOB_DATATYPE_NAME, CLOB_DATATYPE_NAME, CLOB_DATATYPE_NAME,
					TIMESTAMP_DATATYPE_NAME, TIMESTAMP_DATATYPE_NAME };
			if (allowIfExistsTable) {
				securityDb.insertData(queryUtil.createTableIfNotExists("GITHUB_APP", colNames, types));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "GITHUB_APP", database, schema)) {
					// make the table
					securityDb.insertData(queryUtil.createTable("GITHUB_APP", colNames, types));
				}
			}
			{
				List<String> allCols = queryUtil.getTableColumns(conn, "GITHUB_APP", database, schema);
				for (int i = 0; i < colNames.length; i++) {
					String col = colNames[i];
					if (!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
						classLogger.info("Column '{}' is not present in current list of columns: {}", col, allCols);
						String addColumnSql = queryUtil.alterTableAddColumn("GITHUB_APP", col, types[i]);
						securityDb.insertData(addColumnSql);
					}
				}
			}

			// GITHUB_PROJECT_LINK
			// per-project link between a project and a GitHub repo/installation
			colNames = new String[] { "PROJECT_ID", "APP_ID", "INSTALLATION_ID", "REPO_ID", "REPO_FULL_NAME", "BRANCH",
					"CREATED_ON", "UPDATED_ON" };
			types = new String[] { "VARCHAR(255)", "BIGINT", "BIGINT", "BIGINT", "VARCHAR(511)", "VARCHAR(255)",
					TIMESTAMP_DATATYPE_NAME, TIMESTAMP_DATATYPE_NAME };
			if (allowIfExistsTable) {
				securityDb.insertData(queryUtil.createTableIfNotExists("GITHUB_PROJECT_LINK", colNames, types));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "GITHUB_PROJECT_LINK", database, schema)) {
					// make the table
					securityDb.insertData(queryUtil.createTable("GITHUB_PROJECT_LINK", colNames, types));
				}
			}
			{
				List<String> allCols = queryUtil.getTableColumns(conn, "GITHUB_PROJECT_LINK", database, schema);
				for (int i = 0; i < colNames.length; i++) {
					String col = colNames[i];
					if (!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
						classLogger.info("Column '{}' is not present in current list of columns: {}", col, allCols);
						String addColumnSql = queryUtil.alterTableAddColumn("GITHUB_PROJECT_LINK", col, types[i]);
						securityDb.insertData(addColumnSql);
					}
				}
			}
			// indexes for webhook routing (resolve project by repo id / installation id)
			if (allowIfExistsIndexs) {
				String sql = queryUtil.createIndexIfNotExists("IX_GHPL_REPO", "GITHUB_PROJECT_LINK", "REPO_ID");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);

				sql = queryUtil.createIndexIfNotExists("IX_GHPL_INSTALL", "GITHUB_PROJECT_LINK", "INSTALLATION_ID");
				classLogger.info("Running sql {}", sql);
				securityDb.insertData(sql);
			} else {
				// see if index exists
				if (!queryUtil.indexExists(securityDb, "IX_GHPL_REPO", "GITHUB_PROJECT_LINK", database, schema)) {
					String sql = queryUtil.createIndex("IX_GHPL_REPO", "GITHUB_PROJECT_LINK", "REPO_ID");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
				if (!queryUtil.indexExists(securityDb, "IX_GHPL_INSTALL", "GITHUB_PROJECT_LINK", database, schema)) {
					String sql = queryUtil.createIndex("IX_GHPL_INSTALL", "GITHUB_PROJECT_LINK", "INSTALLATION_ID");
					classLogger.info("Running sql {}", sql);
					securityDb.insertData(sql);
				}
			}

			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} finally {
			// clean up the connection used for this method
			if (conn != null && securityDb.isConnectionPooling()) {
				conn.close();
			}
		}
	}

	private static void updateUserTypeEnum() {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Map<String, String[]> allValues = new HashMap<>();
		allValues.put("ASSETENGINE", new String[] { "TYPE" });
		allValues.put("CUSTOMGROUPASSIGNMENT", new String[] { "TYPE", "PERMISSIONGRANTEDBYTYPE" });
		allValues.put("ENGINE", new String[] { "CREATEDBYTYPE" });
		allValues.put("ENGINEACCESSREQUEST", new String[] { "REQUEST_TYPE", "APPROVER_TYPE", "SUBMITTED_BY_TYPE" });
		allValues.put("ENGINEPERMISSION", new String[] { "PERMISSIONGRANTEDBYTYPE" });
		allValues.put("GROUPENGINEPERMISSION", new String[] { "TYPE", "PERMISSIONGRANTEDBYTYPE" });
		allValues.put("GROUPINSIGHTPERMISSION", new String[] { "TYPE", "PERMISSIONGRANTEDBYTYPE" });
		allValues.put("GROUPPROJECTPERMISSION", new String[] { "TYPE", "PERMISSIONGRANTEDBYTYPE" });
		allValues.put("INSIGHTACCESSREQUEST", new String[] { "REQUEST_TYPE", "APPROVER_TYPE", "SUBMITTED_BY_TYPE" });
		allValues.put("PASSWORD_HISTORY", new String[] { "TYPE" });
		allValues.put("PASSWORD_RESET", new String[] { "TYPE" });
		allValues.put("PROJECT", new String[] { "CREATEDBYTYPE", "PORTALPUBLISHEDTYPE", "REACTORSCOMPILEDTYPE" });
		allValues.put("PROJECTACCESSREQUEST", new String[] { "REQUEST_TYPE", "APPROVER_TYPE", "SUBMITTED_BY_TYPE" });
		allValues.put("PROJECTDEPENDENCIES", new String[] { "TYPE" });
		allValues.put("SESSION_SHARE", new String[] { "TYPE" });
		allValues.put("SMSS_GROUP", new String[] { "TYPE", "USERIDTYPE" });
		allValues.put("SMSS_USER", new String[] { "TYPE" });
		allValues.put("SMSS_USER_ACCESS_KEYS", new String[] { "TYPE" });
		allValues.put("USERINSIGHTPERMISSION", new String[] { "PERMISSIONGRANTEDBYTYPE" });

		// grab the new fixed names to the old names
		Map<String, String> newTypesMap = AuthProvider.getLabelToLegacyName();

		// repeat for all tables
		for (String tableName : allValues.keySet()) {
			// repeat for all columns
			String[] columns = allValues.get(tableName);
			for (String columnName : columns) {
				// update every table -> column pair
				Connection conn = null;
				PreparedStatement ps = null;
				try {
					conn = securityDb.getConnection();
					StringBuilder query = new StringBuilder();
					query.append("UPDATE ").append(tableName).append(" SET ").append(columnName).append("=? WHERE ")
							.append(columnName).append("=?");
					ps = conn.prepareStatement(query.toString());

					for (String newType : newTypesMap.keySet()) {
						ps.setString(1, newType);
						ps.setString(2, newTypesMap.get(newType));
						ps.addBatch();
					}
					ps.executeBatch();
					if (!conn.getAutoCommit()) {
						conn.commit();
					}
				} catch (SQLException e) {
					classLogger.error("Error updating legacy auth provider label in {}.{}.", tableName, columnName, e);
				} finally {
					ConnectionUtils.closeAllConnectionsIfPooling(securityDb, conn, ps, null);
				}
			}
		}
	}

	@Deprecated
	private static void performSmssUserTemporaryUpdate(IRDBMSEngine securityDb, AbstractSqlQueryUtil queryUtil,
			String[] colNames, String[] types, Connection conn, String database, String schema,
			boolean allowIfExistsTable) throws Exception {
		// we will move over all the data and create SMSS_USER
		if (allowIfExistsTable) {
			securityDb.insertData(queryUtil.createTableIfNotExists("SMSS_USER", colNames, types));
		} else {
			// see if table exists
			if (!queryUtil.tableExists(conn, "SMSS_USER", database, schema)) {
				// make the table
				securityDb.insertData(queryUtil.createTable("SMSS_USER", colNames, types));
			}
		}
		StringBuilder query = new StringBuilder("SELECT ");
		Object[] input = new Object[colNames.length + 1];
		input[0] = "SMSS_USER";
		for (int i = 0; i < colNames.length; i++) {
			input[i + 1] = colNames[i];
			if (i > 0) {
				query.append(", ");
			}
			query.append(colNames[i]);
		}
		query.append(" FROM USER");
		PreparedStatement insertPs = securityDb.bulkInsertPreparedStatement(input);
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query.toString())) {
			while (wrapper.hasNext()) {
				Object[] values = wrapper.next().getValues();
				int index = 0;
				String name = (String) values[index++];
				String email = (String) values[index++];
				String type = (String) values[index++];
				String id = (String) values[index++];
				String password = (String) values[index++];
				String salt = (String) values[index++];
				String username = (String) values[index++];
				Boolean admin = Boolean.parseBoolean(values[index++] + "");
				Boolean publisher = Boolean.parseBoolean(values[index++] + "");

				index = 1;
				if (name == null) {
					insertPs.setNull(index++, java.sql.Types.VARCHAR);
				} else {
					insertPs.setString(index++, name);
				}
				if (email == null) {
					insertPs.setNull(index++, java.sql.Types.VARCHAR);
				} else {
					insertPs.setString(index++, email);
				}
				if (type == null) {
					insertPs.setNull(index++, java.sql.Types.VARCHAR);
				} else {
					insertPs.setString(index++, type);
				}
				if (id == null) {
					insertPs.setNull(index++, java.sql.Types.VARCHAR);
				} else {
					insertPs.setString(index++, id);
				}
				if (password == null) {
					insertPs.setNull(index++, java.sql.Types.VARCHAR);
				} else {
					insertPs.setString(index++, password);
				}
				if (salt == null) {
					insertPs.setNull(index++, java.sql.Types.VARCHAR);
				} else {
					insertPs.setString(index++, salt);
				}
				if (username == null) {
					insertPs.setNull(index++, java.sql.Types.VARCHAR);
				} else {
					insertPs.setString(index++, username);
				}
				insertPs.setBoolean(index++, admin);
				insertPs.setBoolean(index++, publisher);
				insertPs.addBatch();
			}
		}
		insertPs.executeBatch();
		if (!insertPs.getConnection().getAutoCommit()) {
			insertPs.getConnection().commit();
		}
		if (securityDb.isConnectionPooling()) {
			insertPs.getConnection().close();
		}
		// now delete the user table
		securityDb.insertData(queryUtil.alterTableName("USER", "OLD_USER_TABLE"));
	}

	@Deprecated
	private static void performDependencyUpdate(IRDBMSEngine securityDb, AbstractSqlQueryUtil queryUtil,
			String[] colNames, String[] types, Connection conn, String database, String schema,
			boolean allowIfExistsTable) throws Exception {
		String[] queryArray = new String[] { """
				SELECT PROJECTDEPENDENCIES.ENGINEID, ENGINE.ENGINETYPE FROM ENGINE \
				INNER JOIN PROJECTDEPENDENCIES on PROJECTDEPENDENCIES.ENGINEID=ENGINE.ENGINEID \
				WHERE PROJECTDEPENDENCIES.ENGINETYPE IS NULL
				""", """
				SELECT PROJECTDEPENDENCIES.ENGINEID, 'PROJECT' AS PROJECTTYPE FROM PROJECT \
				INNER JOIN PROJECTDEPENDENCIES on PROJECTDEPENDENCIES.ENGINEID=PROJECT.PROJECTID \
				WHERE PROJECTDEPENDENCIES.ENGINETYPE IS NULL
				""" };

		for (String query : queryArray) {
			Map<String, String> existing = new HashMap<>();

			Connection newConn = null;
			PreparedStatement newPs = null;
			ResultSet rs = null;
			try {
				newConn = securityDb.getConnection();
				newPs = newConn.prepareStatement(query);
				rs = newPs.executeQuery();
				while (rs.next()) {
					existing.put(rs.getString(1), rs.getString(2));
				}
			} catch (SQLException e) {
				classLogger.error("Error reading PROJECTDEPENDENCIES engine types using query: {}", query, e);
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(securityDb, newConn, newPs, rs);
			}

			if (!existing.isEmpty()) {
				String updateQuery = "UPDATE PROJECTDEPENDENCIES SET ENGINETYPE=? WHERE ENGINEID=?";
				try {
					newConn = securityDb.getConnection();
					newPs = newConn.prepareStatement(updateQuery);
					for (String engineId : existing.keySet()) {
						String engineType = existing.get(engineId);
						newPs.setString(1, engineType);
						newPs.setString(2, engineId);
						newPs.addBatch();
					}
					newPs.executeBatch();
					if (!newPs.getConnection().getAutoCommit()) {
						newPs.getConnection().commit();
					}
				} catch (SQLException e) {
					classLogger.error("Error updating PROJECTDEPENDENCIES engine types using query: {}", updateQuery,
							e);
				} finally {
					ConnectionUtils.closeAllConnectionsIfPooling(securityDb, newConn, newPs, null);
				}
			}
		}
	}

	/**
	 * 
	 * @param engineName
	 * @return
	 */
	public static boolean containsEngineName(String engineName) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (ignoreDatabase(engineName)) {
			// dont add local master or security db to security db
			return true;
		}

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINENAME", "==", engineName));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			classLogger.error("Error checking if engine name exists: {}", engineName, e);
		}

		return false;
	}

	/**
	 * 
	 * @param projectName
	 * @return
	 */
	public static boolean containsProjectName(String projectName) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTNAME", "==", projectName));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			classLogger.error("Error checking if project name exists: {}", projectName, e);
		}

		return false;
	}

	public static boolean containsEngineId(String databaseId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (ignoreDatabase(databaseId)) {
			// dont add local master or security db to security db
			return true;
		}

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", databaseId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			classLogger.error("Error checking if engine id exists: {}", databaseId, e);
		}

		return false;
	}

	public static boolean containsProjectId(String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (ignoreDatabase(projectId)) {
			// dont add local master or security db to security db
			return true;
		}
		// String query = "SELECT ENGINEID FROM ENGINE WHERE ENGINEID='" + appId + "'";
		// IRawSelectWrapper wrapper =
		// WrapperManager.getInstance().getRawWrapper(securityDb,
		// query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			classLogger.error("Error checking if project id exists: {}", projectId, e);
		}

		return false;
	}

	public static boolean ignoreDatabase(String databaseId) {
		// dont add default semoss databases to security
		if (SystemDefaultDatabases.getDatabaseIgnoreSecurity().contains(databaseId)) {
			return true;
		}
		// engine is an asset
		if (UserAssetUtils.isAssetProject(databaseId)) {
			return true;
		}
		// so that way all those Asset apps do not appear a bunch of times
		String smssFile = DIHelper.getInstance().getEngineProperty(databaseId + "_" + Constants.STORE) + "";
		File smssF = new File(smssFile);
		if (smssFile != null && smssF.exists() && smssF.isFile()) {
			Properties prop = Utility.loadProperties(smssFile);
			return Boolean.parseBoolean(prop.get(Constants.IS_ASSET_APP) + "");
		}

		return false;
	}

	/**
	 * Get default image for insight
	 * 
	 * @param databaseId
	 * @param insightId
	 * @return
	 */
	public static File getStockImage(String databaseId, String insightId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String imageDir = Utility.getBaseFolder() + "/images/stock/";
		String layout = null;

		// String query = "SELECT LAYOUT FROM INSIGHT WHERE INSIGHT.ENGINEID='" + appId
		// + "' AND INSIGHT.INSIGHTID='" + insightId + "'";
		// IRawSelectWrapper wrapper =
		// WrapperManager.getInstance().getRawWrapper(securityDb,
		// query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("INSIGHT__LAYOUT"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__PROJECTID", "==", databaseId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__INSIGHTID", "==", insightId));

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				layout = wrapper.next().getValues()[0].toString();
			}
		} catch (Exception e) {
			classLogger.error("Error retrieving stock image layout for project {} and insight {}.", databaseId,
					insightId, e);
		}

		// if no layout defined, also return the default
		if (layout == null) {
			return new File(imageDir + "color-logo.png");
		}

		if (layout.equalsIgnoreCase("area")) {
			return new File(imageDir + "area.png");
		} else if (layout.equalsIgnoreCase("column")) {
			return new File(imageDir + "bar.png");
		} else if (layout.equalsIgnoreCase("boxwhisker")) {
			return new File(imageDir + "boxwhisker.png");
		} else if (layout.equalsIgnoreCase("bubble")) {
			return new File(imageDir + "bubble.png");
		} else if (layout.equalsIgnoreCase("choropleth")) {
			return new File(imageDir + "choropleth.png");
		} else if (layout.equalsIgnoreCase("cloud")) {
			return new File(imageDir + "cloud.png");
		} else if (layout.equalsIgnoreCase("cluster")) {
			return new File(imageDir + "cluster.png");
		} else if (layout.equalsIgnoreCase("dendrogram")) {
			return new File(imageDir + "dendrogram-echarts.png");
		} else if (layout.equalsIgnoreCase("funnel")) {
			return new File(imageDir + "funnel.png");
		} else if (layout.equalsIgnoreCase("gauge")) {
			return new File(imageDir + "gauge.png");
		} else if (layout.equalsIgnoreCase("graph")) {
			return new File(imageDir + "graph.png");
		} else if (layout.equalsIgnoreCase("grid")) {
			return new File(imageDir + "grid.png");
		} else if (layout.equalsIgnoreCase("heatmap")) {
			return new File(imageDir + "heatmap.png");
		} else if (layout.equalsIgnoreCase("infographic")) {
			return new File(imageDir + "infographic.png");
		} else if (layout.equalsIgnoreCase("line")) {
			return new File(imageDir + "line.png");
		} else if (layout.equalsIgnoreCase("map")) {
			return new File(imageDir + "map.png");
		} else if (layout.equalsIgnoreCase("pack")) {
			return new File(imageDir + "pack.png");
		} else if (layout.equalsIgnoreCase("parallelcoordinates")) {
			return new File(imageDir + "parallel-coordinates.png");
		} else if (layout.equalsIgnoreCase("pie")) {
			return new File(imageDir + "pie.png");
		} else if (layout.equalsIgnoreCase("polar")) {
			return new File(imageDir + "polar-bar.png");
		} else if (layout.equalsIgnoreCase("radar")) {
			return new File(imageDir + "radar.png");
		} else if (layout.equalsIgnoreCase("sankey")) {
			return new File(imageDir + "sankey.png");
		} else if (layout.equalsIgnoreCase("scatter")) {
			return new File(imageDir + "scatter.png");
		} else if (layout.equalsIgnoreCase("scatterplotmatrix")) {
			return new File(imageDir + "scatter-matrix.png");
		} else if (layout.equalsIgnoreCase("singleaxiscluster")) {
			return new File(imageDir + "single-axis.png");
		} else if (layout.equalsIgnoreCase("sunburst")) {
			return new File(imageDir + "sunburst.png");
		} else if (layout.equalsIgnoreCase("text-widget")) {
			return new File(imageDir + "text-widget.png");
		} else if (layout.equalsIgnoreCase("treemap")) {
			return new File(imageDir + "treemap.png");
		} else {
			return new File(imageDir + "color-logo.png");
		}
	}

	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////

	/*
	 * Utility methods
	 */

	static String createFilter(String... filterValues) {
		StringBuilder b = new StringBuilder();
		boolean hasData = false;
		if (filterValues.length > 0) {
			hasData = true;
			b.append(" IN (");
			b.append("'").append(filterValues[0]).append("'");
			for (int i = 1; i < filterValues.length; i++) {
				b.append(", '").append(filterValues[i]).append("'");
			}
		}
		if (hasData) {
			b.append(")");
		}
		return b.toString();
	}

	static String createFilter(Collection<String> filterValues) {
		if (filterValues.isEmpty()) {
			return " IN () ";
		}
		StringBuilder b = new StringBuilder();
		boolean hasData = false;
		if (filterValues.size() > 0) {
			hasData = true;
			b.append(" IN (");
			Iterator<String> iterator = filterValues.iterator();
			b.append("'").append(AbstractSqlQueryUtil.escapeForSQLStatement(iterator.next())).append("'");
			while (iterator.hasNext()) {
				b.append(", '").append(AbstractSqlQueryUtil.escapeForSQLStatement(iterator.next())).append("'");
			}
		}
		if (hasData) {
			b.append(")");
		}
		return b.toString();
	}

	/**
	 * Get all ids from user object
	 * 
	 * @param user
	 * @return
	 */
	static String getUserFilters(User user) {
		StringBuilder b = new StringBuilder();
		b.append("(");
		if (user != null) {
			List<AuthProvider> logins = user.getLogins();
			if (!logins.isEmpty()) {
				int numLogins = logins.size();
				b.append("'")
						.append(AbstractSqlQueryUtil.escapeForSQLStatement(user.getAccessToken(logins.get(0)).getId()))
						.append("'");
				for (int i = 1; i < numLogins; i++) {
					b.append(", '").append(
							AbstractSqlQueryUtil.escapeForSQLStatement(user.getAccessToken(logins.get(i)).getId()))
							.append("'");
				}
			}
		}
		b.append(")");
		return b.toString();
	}

	/**
	 * Get a vector of the user ids
	 * 
	 * @param user
	 * @return
	 */
	static Collection<String> getUserFiltersQs(User user) {
		List<String> filters = new ArrayList<String>();
		if (user != null) {
			List<AuthProvider> logins = user.getLogins();
			for (AuthProvider thisLogin : logins) {
				filters.add(Utility.inputSQLSanitizer(user.getAccessToken(thisLogin).getId()));
			}
		}

		return filters;
	}

	/**
	 * 
	 * @param user
	 * @return
	 */
	static Collection<String> getUserGroupFiltersQs(User user) {
		List<String> filters = new ArrayList<String>();
		if (user != null) {
			List<AuthProvider> logins = user.getLogins();
			for (AuthProvider thisLogin : logins) {
				AccessToken accessToken = user.getAccessToken(thisLogin);
				Collection<String> userGroups = new ArrayList<>();
				Collection<String> groups = accessToken.getUserGroups();
				if (!groups.isEmpty()) {
					userGroups.addAll(groups);
				}
				Collection<String> customGroups = AdminSecurityGroupUtils.getUserCustomGroups(accessToken);
				if (!customGroups.isEmpty()) {
					userGroups.addAll(customGroups);
				}
				for (String group : userGroups) {
					filters.add(Utility.inputSQLSanitizer(group));
				}
			}
		}
		return filters;
	}

	////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////

	/**
	 * Returns a list of values given a query with one column/variable.
	 * 
	 * @param qs Query Struct to be executed
	 * @return
	 */
	static List<Map<String, Object>> getSimpleQuery(SelectQueryStruct qs) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		List<Map<String, Object>> ret = new ArrayList<Map<String, Object>>();
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				IHeadersDataRow row = wrapper.next();
				String[] headers = row.getHeaders();
				Object[] values = row.getValues();
				Map<String, Object> rowData = new HashMap<String, Object>();
				for (int idx = 0; idx < headers.length; idx++) {
					// if(values[idx] == null) {
					// rowData.put(headers[idx].toLowerCase(), "null");
					// } else {
					rowData.put(headers[idx].toLowerCase(), values[idx]);
					// }
				}
				ret.add(rowData);
			}
		} catch (Exception e) {
			classLogger.error("Error executing simple security query.", e);
		}

		return ret;
	}

	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////

	/**
	 * 
	 * @param email
	 * @param isNewUser
	 * @throws Exception
	 */
	public static void validEmail(String email, boolean isNewUser) throws Exception {
		if (email == null || !email.matches("^[^\\s@]+@[^\\s@]+\\.[^\\s@]{2,}$")) {
			throw new IllegalArgumentException(email + " is not a valid email address. ");
		}
		if (isNewUser && SecurityNativeUserUtils.userEmailExists(email)) {
			throw new IllegalArgumentException("This email already exists. Please login. ");
		}
	}

	/**
	 * 
	 * @param userId
	 * @param type
	 * @param password
	 * @throws Exception
	 */
	public static void validPassword(String userId, AuthProvider type, String password) throws Exception {
		if (password == null || password.isEmpty()) {
			throw new IllegalArgumentException("Password cannot be empty. ");
		}
		PasswordRequirements.getInstance().validatePassword(password);
		if (SecurityNativeUserUtils.isPreviousPassword(userId, type, password)) {
			throw new IllegalArgumentException("Cannot reuse old password. ");
		}
	}

	/**
	 * 
	 * @param phone
	 * @return
	 * @throws Exception
	 */
	public static String formatPhone(String phone) throws Exception {
		if (phone != null && !phone.isEmpty()) {
			if (!phone.matches("[\\d\\s.()-]+")) {
				throw new IllegalArgumentException("Phone number " + phone + " contains invalid characters. ");
			}
			phone = phone.replaceAll("[^\\d]", "");
			// phone numbers can have at max 12 digits
			if (phone.length() < 8 || phone.length() > 12) {
				throw new IllegalArgumentException(phone + " is not a valid phone number. ");
			}
		}
		return phone;
	}

	/**
	 * 
	 * @param username
	 * @throws IllegalArgumentException
	 */
	public static void validUsername(String username) throws IllegalArgumentException {
		if (username == null || username.trim().isEmpty()) {
			throw new IllegalArgumentException("Username cannot be empty. ");
		}
		if (SecurityQueryUtils.checkUsernameExist(username)) {
			throw new IllegalArgumentException("Username already exists. ");
		}
	}

	/**
	 * Current salt generation by BCrypt
	 * 
	 * @return salt
	 */
	public static String generateSalt() {
		return BCrypt.gensalt();
	}

	/**
	 * Create the password hash based on the password and salt provided.
	 * 
	 * @param password
	 * @param salt
	 * @return hash
	 */
	public static String hash(String password, String salt) {
		return BCrypt.hashpw(password, salt);
	}

	/**
	 * Calculate end date of permission
	 * 
	 * @param endDate
	 * @return verifiedEndDate
	 */
	public static Timestamp calculateEndDate(String endDate) {
		ZonedDateTime zdt = ZonedDateTime.parse(endDate);
		ZonedDateTime gmt = zdt.withZoneSameInstant(ZoneId.of("UTC"));
		return java.sql.Timestamp.valueOf(gmt.toLocalDateTime());
	}

	/**
	 * Check if permission end date has expired
	 * 
	 * @param endDate
	 */
	public static boolean endDateIsExpired(SemossDate endDate) throws Exception {
		LocalDateTime currentTime = LocalDateTime.now();
		if (endDate == null) {
			return false;
		}
		LocalDateTime formattedEndDate = endDate.getLocalDateTime();
		return formattedEndDate.isBefore(currentTime);
	}

}
