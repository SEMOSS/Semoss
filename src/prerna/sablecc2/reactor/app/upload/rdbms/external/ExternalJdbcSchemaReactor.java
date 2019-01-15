package prerna.sablecc2.reactor.app.upload.rdbms.external;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class ExternalJdbcSchemaReactor extends AbstractReactor {
	
	private static final String CLASS_NAME = ExternalJdbcSchemaReactor.class.getName();
	
	public static final String TABLES_KEY = "tables";
	public static final String RELATIONS_KEY = "relationships";
	
	public ExternalJdbcSchemaReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DB_DRIVER_KEY.getKey(), ReactorKeysEnum.HOST.getKey(), 
				ReactorKeysEnum.PORT.getKey(), ReactorKeysEnum.USERNAME.getKey(), 
				ReactorKeysEnum.PASSWORD.getKey(), ReactorKeysEnum.SCHEMA.getKey(),
				ReactorKeysEnum.ADDITIONAL_CONNECTION_PARAMS_KEY.getKey(),
				ReactorKeysEnum.FILTERS.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		
		organizeKeys();
		String driver = getStringInput(0);
		String host = getStringInput(1);
		String port = getStringInput(2);
		String username = getStringInput(3);
		String password = getStringInput(4);
		String schema = getStringInput(5);
		String additionalProperties = getStringInput(6);
	
		List<String> tableAndViewFilters = getFilters();
		boolean hasFilters = !tableAndViewFilters.isEmpty();
		
		String connectionUrl = null;;
		Connection con = null;
		try {
			connectionUrl = RdbmsConnectionHelper.getConnectionUrl(driver, host, port, schema, additionalProperties);
			con = RdbmsConnectionHelper.buildConnection(connectionUrl, username, password, driver);
		} catch (SQLException e) {
			String driverError = e.getMessage();
			String errorMessage = "Unable to establish connection given the connection details.\nDriver produced error: \" ";
			errorMessage += driverError;
			errorMessage += " \"";
			throw new SemossPixelException(new NounMetadata(errorMessage, PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}
		
		// tablename
		List<Map<String, Object>> databaseTables = new ArrayList<Map<String, Object>>();
		List<Map<String, String>> databaseJoins = new ArrayList<Map<String, String>>();

		DatabaseMetaData meta;
		try {
			meta = con.getMetaData();
		} catch (SQLException e) {
			throw new SemossPixelException(new NounMetadata("Unable to get the database metadata", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}
		
		String catalogFilter = null;
		try {
			catalogFilter = con.getCatalog();
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		String schemaFilter = RdbmsConnectionHelper.getSchema(meta, con, connectionUrl);

		CustomTableAndViewIterator tableViewIterator = new CustomTableAndViewIterator(meta, catalogFilter, schemaFilter, tableAndViewFilters); 
		
		final String TABLE_KEY = "table";
		final String COLUMNS_KEY = "columns";
		final String TYPES_KEY = "type";
		final String PRIM_KEY = "isPrimKey";
		
		final String TO_TABLE_KEY = "toTable";
		final String TO_COL_KEY = "toCol";
		final String FROM_TABLE_KEY = "fromTable";
		final String FROM_COL_KEY = "fromCol";
		
		try {
			while (tableViewIterator.hasNext()) {
				String[] nextRow = tableViewIterator.next();
				String tableOrView = nextRow[0];
				String tableType = nextRow[1];
				String schem = nextRow[2];
				boolean isTable = tableType.toUpperCase().contains("TABLE");

				// this will be table or view
				if(isTable) {
					logger.info("Processing table = " + tableOrView);
				} else {
					// there may be views built from sys or information schema
					// we want to ignore these
					if(schem != null) {
						if(schem.equalsIgnoreCase("INFORMATION_SCHEMA") || schem.equalsIgnoreCase("SYS")) {
							continue;
						}
					}
					logger.info("Processing view = " + tableOrView);
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
					while(keys.next()) {
						primaryKeys.add(keys.getString("column_name"));
					}
				} catch (SQLException e) {
					e.printStackTrace();
				} finally {
					closeRs(keys);
				}
				
				List<String> columnNames = new ArrayList<String>();
				List<String> columnTypes = new ArrayList<String>();
				List<Boolean> isPrimKeys = new ArrayList<Boolean>();

				ResultSet columnsRs = null;
				try {
					logger.info("....Processing columns");
					columnsRs = meta.getColumns(catalogFilter, schemaFilter, tableOrView, null);
					
					while (columnsRs.next()) {
						String cName = columnsRs.getString("column_name");
						columnNames.add(cName);
						columnTypes.add(columnsRs.getString("type_name"));
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
					tableDetails.put(PRIM_KEY, isPrimKeys);

				} catch (SQLException e) {
					e.printStackTrace();
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
					} catch (SQLException e) {
						e.printStackTrace();
					} finally {
						closeRs(relRs);
					}
				}
			}
		} finally {
			try {
				tableViewIterator.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			if(con != null) {
				try {
					con.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		logger.info("Done parsing database metadata");
		
		HashMap<String, Object> ret = new HashMap<String, Object>();
		ret.put(TABLES_KEY, databaseTables);
		ret.put(RELATIONS_KEY, databaseJoins);
		return new NounMetadata(ret, PixelDataType.CUSTOM_DATA_STRUCTURE);
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
				e.printStackTrace();
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
	 * @param index
	 * @return
	 */
	public String getStringInput(int index) {
		GenRowStruct valueGrs = this.store.getNoun(this.keysToGet[index]);
		if(valueGrs != null) {
			return valueGrs.get(0).toString();
		}
		
		if(this.curRow.size() > index) {
			return this.curRow.get(index).toString();
		}
		
		return null;
	}
	
	/**
	 * Get a list of table / view column filters
	 * @return
	 */
	private List<String> getFilters() {
		List<String> filterValues = new Vector<String>();
		
		GenRowStruct valueGrs = this.store.getNoun(this.keysToGet[7]);
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
