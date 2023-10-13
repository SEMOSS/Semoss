package prerna.reactor.database.upload.rdbms.external;

import java.io.File;
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
import prerna.util.sql.SqlQueryUtilFactory;

public class ExternalJdbcSchemaReactor extends AbstractReactor {
	
	private static final String CLASS_NAME = ExternalJdbcSchemaReactor.class.getName();
	private static final Logger classLogger = LogManager.getLogger(ExternalJdbcSchemaReactor.class);

	public static final String TABLES_KEY = "tables";
	public static final String RELATIONS_KEY = "relationships";
	
	public ExternalJdbcSchemaReactor() {
		this.keysToGet = new String[]{ ReactorKeysEnum.CONNECTION_DETAILS.getKey(), ReactorKeysEnum.FILTERS.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		
		Map<String, Object> connectionDetails = getConDetails();
		if(connectionDetails != null) {
			String host = (String) connectionDetails.get(AbstractSqlQueryUtil.HOSTNAME);
			if(host != null) {
				String testUpdatedHost = this.insight.getAbsoluteInsightFolderPath(host);
				if(new File(testUpdatedHost).exists()) {
					host = testUpdatedHost;
					connectionDetails.put(AbstractSqlQueryUtil.HOSTNAME, host);
				}
			}
		}
		
		String driver = (String) connectionDetails.get(AbstractSqlQueryUtil.DRIVER_NAME);
		RdbmsTypeEnum driverEnum = RdbmsTypeEnum.getEnumFromString(driver);
		AbstractSqlQueryUtil queryUtil = SqlQueryUtilFactory.initialize(driverEnum);
		
		List<String> tableAndViewFilters = getFilters();
		boolean hasFilters = !tableAndViewFilters.isEmpty();
		
		Connection con = null;
		DatabaseMetaData meta = null;
		try {
			String connectionUrl = null;
			try {
				connectionUrl = queryUtil.setConnectionDetailsfromMap(connectionDetails);
			} catch (RuntimeException e) {
				throw new SemossPixelException(new NounMetadata("Unable to generation connection url with message " + e.getMessage(), PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			}
			
			try {
				con = AbstractSqlQueryUtil.makeConnection(queryUtil, connectionUrl, connectionDetails);
				queryUtil.enhanceConnection(con);
			} catch (SQLException e) {
				classLogger.error(Constants.STACKTRACE, e);
				String driverError = e.getMessage();
				String errorMessage = "Unable to establish connection given the connection details.\nDriver produced error: \" ";
				errorMessage += driverError;
				errorMessage += " \"";
				throw new SemossPixelException(new NounMetadata(errorMessage, PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			}
			
			try {
				meta = con.getMetaData();
			} catch (SQLException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new SemossPixelException(new NounMetadata("Unable to get the database metadata", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			}
			
			// tablename
			List<Map<String, Object>> databaseTables = new ArrayList<Map<String, Object>>();
			List<Map<String, String>> databaseJoins = new ArrayList<Map<String, String>>();
	
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
				schemaFilter = (String) connectionDetails.get(AbstractSqlQueryUtil.SCHEMA);
			}
			if(schemaFilter == null) {
				schemaFilter = RdbmsConnectionHelper.getSchema(meta, con, connectionUrl, driverEnum);
			}
			
			CustomTableAndViewIterator tableViewIterator = new CustomTableAndViewIterator(con, meta, catalogFilter, schemaFilter, driverEnum, tableAndViewFilters); 
			
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
					if(isTable) {
						logger.info("Processing table = " + Utility.cleanLogString(tableOrView));
					} else {
						// there may be views built from sys or information schema
						// we want to ignore these
						if(schem != null) {
							if(schem.equalsIgnoreCase("INFORMATION_SCHEMA") || schem.equalsIgnoreCase("SYS")) {
								continue;
							}
						}
						logger.info("Processing view = " + Utility.cleanLogString(tableOrView));
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
						if(keys != null) {
							while(keys.next()) {
								primaryKeys.add(keys.getString(COLUMN_NAME_STR));
							}
						}
					} catch (SQLException e) {
						classLogger.error(Constants.STACKTRACE, e);
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
						
						columnsRs = RdbmsConnectionHelper.getColumns(meta, tableOrView, catalogFilter, schemaFilter, driverEnum);
						
						while (columnsRs.next()) {
							String cName = columnsRs.getString(COLUMN_NAME_STR);
							columnNames.add(cName);
							columnTypes.add(columnsRs.getString(COLUMN_TYPE_STR));
							cleanColumnTypes.add(SemossDataType.convertStringToDataType(columnsRs.getString(COLUMN_TYPE_STR)).toString());
							if(primaryKeys.contains(cName)) {
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
						classLogger.error(Constants.STACKTRACE, e);
					} finally {
						closeRs(columnsRs);
					}
					databaseTables.add(tableDetails);
	
					// we are now done with the table info
					// let us go to the joins
					// only do this for tables, not for views
					ResultSet relRs = null;
					if(isTable) {
						try {
							logger.info("....Processing table foreign keys");
							relRs = meta.getExportedKeys(catalogFilter, schemaFilter, tableOrView);
							if(relRs != null) {
								while (relRs.next()) {
									String otherTableName = relRs.getString("FKTABLE_NAME");
									
									// add filter check
									if(hasFilters && !tableAndViewFilters.contains(otherTableName)) {
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
							classLogger.error(Constants.STACKTRACE, e);
						} finally {
							closeRs(relRs);
						}
					}
				}
			} finally {
				if(tableViewIterator != null) {
					tableViewIterator.close();
				}
			}
			logger.info("Done parsing database metadata");
			
			HashMap<String, Object> ret = new HashMap<String, Object>();
			ret.put(TABLES_KEY, databaseTables);
			ret.put(RELATIONS_KEY, databaseJoins);
			Map<String, Map<String, Double>> positions = GenerateMetamodelLayout.generateMetamodelLayoutForExternal(databaseTables, databaseJoins);
			ret.put(Constants.POSITION_PROP, positions);
			return new NounMetadata(ret, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} finally {
			if(con != null) {
				try {
					con.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	/**
	 * Close the result set
	 * @param rs
	 */
	private void closeRs(ResultSet rs) {
		if(rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
	}

	///////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////

	private Map<String, Object> getConDetails() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.CONNECTION_DETAILS.getKey());
		if(grs != null && !grs.isEmpty()) {
			List<Object> mapInput = grs.getValuesOfType(PixelDataType.MAP);
			if(mapInput != null && !mapInput.isEmpty()) {
				return (Map<String, Object>) mapInput.get(0);
			}
		}
		
		List<Object> mapInput = grs.getValuesOfType(PixelDataType.MAP);
		if(mapInput != null && !mapInput.isEmpty()) {
			return (Map<String, Object>) mapInput.get(0);
		}
		
		return null;
	}
	
	/**
	 * Get a list of table / view column filters
	 * @return
	 */
	private List<String> getFilters() {
		List<String> filterValues = new Vector<String>();
		
		GenRowStruct valueGrs = this.store.getNoun(this.keysToGet[1]);
		if(valueGrs != null && !valueGrs.isEmpty()) {
			int length = valueGrs.size();
			for(int i = 0; i < length; i++) {
				filterValues.add(valueGrs.get(i).toString());
			}
			return filterValues;
		}
		
		for(int i = 7; i < this.curRow.size(); i++) {
			filterValues.add(this.curRow.get(i).toString());
		}
		
		return filterValues;
	}
}
