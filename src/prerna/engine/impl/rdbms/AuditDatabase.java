package prerna.engine.impl.rdbms;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.tools.Server;

import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.impl.SmssUtilities;
import prerna.query.querystruct.update.UpdateQueryStruct;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class AuditDatabase {

	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	
	private Connection conn;
	private Server server;
	private String serverUrl;
	
	private String engineId;
	private String engineName;
	
	public AuditDatabase() {
		
	}
	
	/**
	 * First method that needs to be run to generate the actual connection details
	 * @param engineId
	 * @param engineName
	 */
	public void init(String engineId, String engineName) {
		this.engineId = engineId;
		this.engineName = engineName;
		
		String dbFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		dbFolder += DIR_SEPARATOR + SmssUtilities.getUniqueName(engineName, engineId);
		
		String fileLocation = dbFolder + DIR_SEPARATOR + "audit_log_database";
		File f = new File(fileLocation + ".mv.db");
		if(!f.exists()) {
			try {
				f.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		RdbmsConnectionBuilder builder = new RdbmsConnectionBuilder(RdbmsConnectionBuilder.CONN_TYPE.DIRECT_CONN_URL);
		try {
			String port = Utility.findOpenPort();
			// create a random user and password
			// get the connection object and start up the frame
			server = Server.createTcpServer("-tcpPort", port, "-tcpAllowOthers");
			serverUrl = "jdbc:h2:" + server.getURL() + "/nio:" + fileLocation;
			server.start();
			
			// update the builder
			builder.setConnectionUrl(serverUrl);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		builder.setDriver("H2_DB");
		builder.setUserName("sa");
		builder.setPassword("");
		
		System.out.println("Audit connection url is " + builder.getConnectionUrl());
		System.out.println("Audit connection url is " + builder.getConnectionUrl());
		System.out.println("Audit connection url is " + builder.getConnectionUrl());

		try {
			this.conn = builder.build();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		// create the tables if necessary
		String[] headers = new String[]{"AUTO_INCREMENT", "TYPE", "ID", "TABLE", "KEY_COLUMN", "KEY_COLUMN_VALUE", "ALTERED_COLUMN", "OLD_VALUE", "NEW_VALUE", "TIMESTAMP", "USER"};
		String[] types = new String[]{"IDENTITY", "VARCHAR(50)", "VARCHAR(50)", "VARCHAR(200)", "VARCHAR(200)", "VARCHAR(200)", "VARCHAR(200)", "VARCHAR(200)", "VARCHAR(200)", "TIMESTAMP", "VARCHAR(200)"};
		execQ(RdbmsQueryBuilder.makeOptionalCreate("TEST_TABLE", headers, types));
	}
	
	public void auditUpdateQuery(UpdateQueryStruct updateQs) {
		
	}
	
	public String getAuditInsert(Object[] data) {
		String[] headers = new String[]{"ID", "TYPE", "TABLE", "KEY_COLUMN", "KEY_COLUMN_VALUE", "ALTERED_COLUMN", "OLD_VALUE", "NEW_VALUE", "TIMESTAMP", "USER"};
		String[] types = new String[]{"VARCHAR(50)", "VARCHAR(50)", "VARCHAR(200)", "VARCHAR(200)", "VARCHAR(200)", "VARCHAR(200)", "VARCHAR(200)", "VARCHAR(200)", "TIMESTAMP", "VARCHAR(200)"};
		return RdbmsQueryBuilder.makeInsert("TEST_TABLE", headers, types, data);
	}
	
	/**
	 * 
	 * @param q
	 */
	private void execQ(String q) {
		Statement stmt = null;
		try {
			stmt = this.conn.createStatement();
			stmt.execute(q);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if(stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public String getEngineId() {
		return this.engineId;
	}
}
