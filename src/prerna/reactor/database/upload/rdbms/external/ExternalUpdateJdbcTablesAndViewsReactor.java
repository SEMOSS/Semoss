package prerna.reactor.database.upload.rdbms.external;

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

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;

public class ExternalUpdateJdbcTablesAndViewsReactor extends AbstractReactor {
	
	private static final String CLASS_NAME = ExternalUpdateJdbcTablesAndViewsReactor.class.getName();
	private static final Logger classLogger = LogManager.getLogger(ExternalUpdateJdbcSchemaReactor.class);

	public ExternalUpdateJdbcTablesAndViewsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);

		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		User user = this.insight.getUser();
		databaseId = SecurityQueryUtils.testUserEngineIdForAlias(user, databaseId);
		if(!SecurityEngineUtils.userCanEditEngine(user, databaseId)) {
			throw new IllegalArgumentException("User does not have permission to edit this database schema");
		}

		IDatabaseEngine database = Utility.getDatabase(databaseId);
		IRDBMSEngine nativeDatabase = null;
		if(database instanceof IRDBMSEngine) {
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
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException(e.getMessage());
			}
			
			// keep a list of tables and views
			List<String> tables = new ArrayList<String>();
			List<String> views = new ArrayList<String>();
	
			try {
				meta = connection.getMetaData();
			} catch (SQLException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new SemossPixelException(new NounMetadata("Unable to get the database metadata", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			}
			
			RdbmsTypeEnum driverEnum = nativeDatabase.getDbType();
	
			String catalogFilter = queryUtil.getDatabaseMetadataCatalogFilter();
			if(catalogFilter == null) {
				try {
					catalogFilter = connection.getCatalog();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
	
			String schemaFilter = queryUtil.getDatabaseMetadataSchemaFilter();
			if(schemaFilter == null) {
				schemaFilter = nativeDatabase.getSchema();
			}
			
			boolean close = false;
			Statement tableStmt = null;
			ResultSet tablesRs = null;
			try {
				tableStmt = connection.createStatement();
				tablesRs = RdbmsConnectionHelper.getTables(connection, tableStmt, meta, catalogFilter, schemaFilter, driverEnum);
			} catch (SQLException e) {
				classLogger.error(Constants.STACKTRACE, e);
				close = true;
				throw new SemossPixelException(new NounMetadata("Unable to get tables and views from database metadata", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			} finally {
				if(close) {
					closeAutoClosable(tablesRs, logger);
					closeAutoClosable(tableStmt, logger);
				}
			}
			
			String[] tableKeys = RdbmsConnectionHelper.getTableKeys(driverEnum);
			final String TABLE_NAME_STR = tableKeys[0];
			final String TABLE_TYPE_STR = tableKeys[1];
	
			try {
				while (tablesRs.next()) {
					String table = tablesRs.getString(TABLE_NAME_STR);
					// this will be table or view
					String tableType = tablesRs.getString(TABLE_TYPE_STR).toUpperCase();
					if(tableType.toUpperCase().contains("TABLE")) {
						logger.info("Found table = " + Utility.cleanLogString(table));
						tables.add(table);
					} else {
						// there may be views built from sys or information schema
						// we want to ignore these
						logger.info("Found view = " + Utility.cleanLogString(table));
						views.add(table);
					}
				}
			} catch (SQLException e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				closeAutoClosable(tablesRs, logger);
				closeAutoClosable(tableStmt, logger);
			}
			logger.info("Done parsing database metadata");
			
			Map<String, List<String>> ret = new HashMap<String, List<String>>();
			ret.put("tables", tables);
			ret.put("views", views);
	
			return new NounMetadata(ret, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} finally {
			if(nativeDatabase.isConnectionPooling()) {
				if(connection != null) {
					try {
						connection.close();
					} catch (SQLException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
	}

	/**
	 * Close a connection, statement, or result set
	 * @param closeable
	 */
	private void closeAutoClosable(AutoCloseable closeable, Logger logger) {
		if(closeable != null) {
			try {
				closeable.close();
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
	}

}
