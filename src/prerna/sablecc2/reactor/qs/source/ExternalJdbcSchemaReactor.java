package prerna.sablecc2.reactor.qs.source;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class ExternalJdbcSchemaReactor extends AbstractReactor {
	
	private static final String CLASS_NAME = ExternalJdbcSchemaReactor.class.getName();
	
	public ExternalJdbcSchemaReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DB_DRIVER_KEY.getKey(), ReactorKeysEnum.HOST.getKey(), 
				ReactorKeysEnum.PORT.getKey(), ReactorKeysEnum.USERNAME.getKey(), 
				ReactorKeysEnum.PASSWORD.getKey(), ReactorKeysEnum.SCHEMA.getKey(),
				ReactorKeysEnum.ADDITIONAL_CONNECTION_PARAMS_KEY.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		
		organizeKeys();
		String driver = this.keyValue.get(this.keysToGet[0]);
		String host = this.keyValue.get(this.keysToGet[1]);
		String port = this.keyValue.get(this.keysToGet[2]);
		String username = this.keyValue.get(this.keysToGet[3]);
		String password = this.keyValue.get(this.keysToGet[4]);
		String schema = this.keyValue.get(this.keysToGet[5]);
		String additionalProperties = this.keyValue.get(this.keysToGet[6]);

		Connection con;
		try {
			con = RdbmsConnectionHelper.buildConnection(driver, host, port, username, password, schema, additionalProperties);
		} catch (SQLException e) {
			throw new SemossPixelException(new NounMetadata("Unable to establish connection given the connection details", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
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
		
		ResultSet tablesRs;
		try {
			tablesRs = meta.getTables(schema, null, null, new String[] { "TABLE", "VIEW" });
		} catch (SQLException e) {
			throw new SemossPixelException(new NounMetadata("Unable to get tables from database metadata", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}
		
		final String TABLE_KEY = "table";
		final String COLUMNS_KEY = "columns";
		final String TYPES_KEY = "type";
		final String PRIM_KEY = "isPrimKey";
		
		final String TO_TABLE_KEY = "toTable";
		final String TO_COL_KEY = "toCol";
		final String FROM_TABLE_KEY = "fromTable";
		final String FROM_COL_KEY = "fromCol";
		
		try {
			while (tablesRs.next()) {
				String table = tablesRs.getString("table_name");
				// this will be table or view
				String tableType = tablesRs.getString("table_type").toUpperCase();
				if(tableType.equals("TABLE")) {
					logger.info("Processing table = " + table);
				} else {
					// there may be views built from sys or information schema
					// we want to ignore these
					String schem = tablesRs.getString("table_schem");
					if(schem != null) {
						if(schem.equalsIgnoreCase("INFORMATION_SCHEMA") || schem.equalsIgnoreCase("SYS")) {
							continue;
						}
					}
					logger.info("Processing view = " + table);
				}
				// grab the table
				// we want to get the following information
				// table name
				// column name
				// column type
				// is primary key
				Map<String, Object> tableDetails = new HashMap<String, Object>(); 
				tableDetails.put(TABLE_KEY, table);
				
				List<String> primaryKeys = new ArrayList<String>();
				ResultSet keys = null;
				try {
					keys = meta.getPrimaryKeys(null, null, table);
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

				try {
					logger.info("....Processing table columns");
					keys = meta.getColumns(null, null, table, null);
					while (keys.next()) {
						String cName = keys.getString("column_name");
						columnNames.add(cName);
						columnTypes.add(keys.getString("type_name"));
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
					closeRs(keys);
				}
				databaseTables.add(tableDetails);

				// we are now done with the table info
				// let us go to the joins
				// only do this for tables, not for views
				if(tableType.equals("TABLE")) {
					try {
						logger.info("....Processing table foreign keys");
						keys = meta.getExportedKeys(null, null, table);
						while (keys.next()) {
							Map<String, String> joinInfo = new HashMap<String, String>();
							joinInfo.put(FROM_TABLE_KEY, table);
							joinInfo.put(FROM_COL_KEY, keys.getString("PKCOLUMN_NAME"));
							joinInfo.put(TO_TABLE_KEY, keys.getString("FKTABLE_NAME"));
							joinInfo.put(TO_COL_KEY, keys.getString("FKCOLUMN_NAME"));
	
							databaseJoins.add(joinInfo);
						}
					} catch (SQLException e) {
						e.printStackTrace();
					} finally {
						closeRs(keys);
					}
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeRs(tablesRs);
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
		ret.put("tables", databaseTables);
		ret.put("relationships", databaseJoins);
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

}
