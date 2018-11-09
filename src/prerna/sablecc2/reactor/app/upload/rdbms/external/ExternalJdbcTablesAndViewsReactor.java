package prerna.sablecc2.reactor.app.upload.rdbms.external;

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

public class ExternalJdbcTablesAndViewsReactor extends AbstractReactor {
	
	private static final String CLASS_NAME = ExternalJdbcTablesAndViewsReactor.class.getName();
	
	public ExternalJdbcTablesAndViewsReactor() {
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
		
		// keep a list of tables and views
		List<String> tables = new ArrayList<String>();
		List<String> views = new ArrayList<String>();

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
		String schemaFilter = null;
		// THIS IS BECAUSE ONLY JAVA 7 REQUIRES
		// THIS METHOD OT BE IMPLEMENTED ON THE
		// DRIVERS
		if(meta.getDriverMinorVersion() >= 7) {
			try {
				schemaFilter = con.getSchema();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		ResultSet tablesRs;
		try {
			tablesRs = meta.getTables(catalogFilter, schemaFilter, null, new String[] { "TABLE", "VIEW" });
		} catch (SQLException e) {
			throw new SemossPixelException(new NounMetadata("Unable to get tables and views from database metadata", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}
		
		try {
			while (tablesRs.next()) {
				String table = tablesRs.getString("table_name");
				// this will be table or view
				String tableType = tablesRs.getString("table_type").toUpperCase();
				if(tableType.equals("TABLE")) {
					logger.info("Found table = " + table);
					tables.add(table);
				} else {
					// there may be views built from sys or information schema
					// we want to ignore these
					String schem = tablesRs.getString("table_schem");
					if(schem != null) {
						if(schem.equalsIgnoreCase("INFORMATION_SCHEMA") || schem.equalsIgnoreCase("SYS")) {
							continue;
						}
					}
					logger.info("Found view = " + table);
					views.add(table);
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
		
		Map<String, List<String>> ret = new HashMap<String, List<String>>();
		ret.put("tables", tables);
		ret.put("views", views);
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
