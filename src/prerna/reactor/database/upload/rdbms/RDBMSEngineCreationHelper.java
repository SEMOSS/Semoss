package prerna.reactor.database.upload.rdbms;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
import prerna.om.HeadersException;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.reactor.database.upload.rdbms.external.CustomTableAndViewIterator;
import prerna.util.Constants;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;

public class RDBMSEngineCreationHelper {

	private static final Logger classLogger = LogManager.getLogger(RDBMSEngineCreationHelper.class);
	
	private RDBMSEngineCreationHelper() {
		
	}
	
	public static Map<String, Map<String, String>> getExistingRDBMSStructure(IDatabaseEngine rdbmsEngine) {
		return getExistingRDBMSStructure(rdbmsEngine, null);
	}
	
	public static Map<String, Map<String, String>> getExistingRDBMSStructure(IDatabaseEngine rdbmsEngine, Set<String> tablesToRetrieve) {
		// get the metadata from the connection
		IRDBMSEngine rdbms = null;
		if(rdbmsEngine instanceof IRDBMSEngine) {
			rdbms = (IRDBMSEngine) rdbmsEngine;
		} else {
			throw new IllegalArgumentException("Engine must be a valid JDBC engine");
		}
		AbstractSqlQueryUtil queryUtil = rdbms.getQueryUtil();
		RdbmsTypeEnum driverEnum = rdbms.getDbType();

		// table that will store 
		// table_name -> {
		// 					colname1 -> coltype,
		//					colname2 -> coltype,
		//				}
		Map<String, Map<String, String>> tableColumnMap = new Hashtable<String, Map<String, String>>();

		Connection con = null;
		try {
			DatabaseMetaData meta = null;
			try {
				con = rdbms.makeConnection();
				meta = con.getMetaData();
			} catch (SQLException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException(e.getMessage());
			}
			
			String catalogFilter = queryUtil.getDatabaseMetadataCatalogFilter();
			if(catalogFilter == null) {
				try {
					catalogFilter = con.getCatalog();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
	
			String schemaFilter = queryUtil.getDatabaseMetadataSchemaFilter();
			if(schemaFilter == null) {
				schemaFilter = (String) rdbms.getSchema();
			}
	
			String[] columnKeys = RdbmsConnectionHelper.getColumnKeys(driverEnum);
			final String COLUMN_NAME_STR = columnKeys[0];
			final String COLUMN_TYPE_STR = columnKeys[1];
	
			CustomTableAndViewIterator tableViewIterator = new CustomTableAndViewIterator(con, meta, catalogFilter, schemaFilter, driverEnum, tablesToRetrieve); 
			try {
				while (tableViewIterator.hasNext()) {
					String[] nextRow = tableViewIterator.next();
					String tableOrView = nextRow[0];
	
					// keep a map of the columns
					Map<String, String> colDetails = new HashMap<String, String>();
					// iterate through the columns
					
					ResultSet columnsRs = null;
					try {
						columnsRs = RdbmsConnectionHelper.getColumns(meta, tableOrView, catalogFilter, schemaFilter, driverEnum);
						while(columnsRs.next()) {
							colDetails.put(columnsRs.getString(COLUMN_NAME_STR), columnsRs.getString(COLUMN_TYPE_STR));
						}
					} catch(SQLException e) {
						classLogger.error(Constants.STACKTRACE, e);
					} finally {
						try {
							if(columnsRs != null) {
								columnsRs.close();
							}
						} catch (SQLException e1) {
							classLogger.error(Constants.STACKTRACE, e1);
						}
					}
					tableColumnMap.put(tableOrView, colDetails);
				}
			} finally {
				if(tableViewIterator != null) {
					tableViewIterator.close();
				}
			}
		} finally {
			if(con != null && rdbms.isConnectionPooling()) {
				try {
					con.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return tableColumnMap;
	}

	/**
	 * Remove all non alpha-numeric underscores from form name
	 * @param s
	 * @return
	 */
	public static String cleanTableName(String s) {
		s = s.trim();
		s = s.replaceAll(" ", "_");
		s = s.replaceAll("[^a-zA-Z0-9\\_]", ""); // matches anything that is not alphanumeric or underscore
		while(s.contains("__")){
			s = s.replace("__", "_");
		}
		// can't start with a digit in rdbms
		// have it start with an underscore and it will work
		if(Character.isDigit(s.charAt(0))) {
			s = "_" + s;
		}
		HeadersException check = HeadersException.getInstance();
		if(check.isIllegalHeader(s)) {
			s = check.appendNumOntoHeader(s);
		}

		return s;
	}
	
	public static boolean conceptExists(IDatabaseEngine engine, String tableName, String colName, Object instanceValue) {
		String query = "SELECT DISTINCT " + colName + " FROM " + tableName + " WHERE " + colName + "='" + AbstractSqlQueryUtil.escapeForSQLStatement(instanceValue + "") + "'";
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext()) {
			return true;
		}
		return false;
	}
}
