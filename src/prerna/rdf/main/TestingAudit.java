//package prerna.rdf.main;
//
//import java.io.File;
//import java.io.IOException;
//import java.sql.Connection;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.sql.Statement;
//import java.time.LocalDateTime;
//import java.util.UUID;
//
//import org.h2.tools.Server;
//
//import prerna.algorithm.api.SemossDataType;
//import prerna.ds.util.RdbmsQueryBuilder;
//import prerna.engine.impl.rdbms.RdbmsConnectionBuilder;
//import prerna.poi.main.helper.CSVFileHelper;
//import prerna.util.Utility;
//
//public class TestingAudit {
//
//	private static boolean init = false;
//	
//	private Connection conn;
//	private Server server;
//	private String serverUrl;
//	
//	public TestingAudit() {
//		
//	}
//	
//	public void genModTables() {
//		String[] headers = new String[]{"ENGINE_ID", "MOD_TABLE"};
//		String[] types = new String[]{"VARCHAR(200)", "VARCHAR(200)"};
////		execQ(RdbmsQueryBuilder.makeOptionalCreate("ENGINE_TABLE_LOOKUP", headers, types));
////		execQ(RdbmsQueryBuilder.makeInsert("ENGINE_TABLE_LOOKUP", headers, types, new Object[]{"test_id", "TEST_TABLE"}));
//
//		headers = new String[]{"AUTO_INCREMENT", "ID", "TABLE", "KEY_COLUMN", "KEY_COLUMN_VALUE", "ALTERED_COLUMN", "OLD_VALUE", "NEW_VALUE", "TIMESTAMP", "USER"};
//		types = new String[]{"IDENTITY", "VARCHAR(50)", "VARCHAR(200)", "VARCHAR(200)", "VARCHAR(200)", "VARCHAR(200)", "VARCHAR(200)", "VARCHAR(200)", "TIMESTAMP", "VARCHAR(200)"};
//		execQ(RdbmsQueryBuilder.makeOptionalCreate("TEST_TABLE", headers, types));
//	}
//	
//	
//	public void performMods() {
//		// transaction 1
//		{
//			String table = "DATA";
//			String keyColumn = "TITLE";
//			String keyValue = "American Hustle";
//			String alteredColumn = "NOMINATED";
//			String oldValue = "Y";
//			String newValue = "N";
//			
//			// run on the table
//			String updateQ = "UPDATE " + table + " SET " + alteredColumn + "='" + newValue + "' WHERE " + keyColumn + "='" + keyValue + "' AND " + alteredColumn + "='" + oldValue + "'";
//			execQ(updateQ);
//			
//			// add audit log
//			Object[] data = new Object[]{UUID.randomUUID().toString(), table, keyColumn, keyValue, alteredColumn, oldValue, newValue, getTime(), "maher khalil"};
//			runAudit(data);
//		}
//		
//		// transaction 2
//		{
//			String table = "DATA";
//			String keyColumn = "TITLE";
//			String keyValue = "Captain Phillips";
//			String alteredColumn = "NOMINATED";
//			String oldValue = "Y";
//			String newValue = "N";
//			
//			// run on the table
//			String updateQ = "UPDATE " + table + " SET " + alteredColumn + "='" + newValue + "' WHERE " + keyColumn + "='" + keyValue + "' AND " + alteredColumn + "='" + oldValue + "'";
//			execQ(updateQ);
//			
//			// add audit log
//			String tId = UUID.randomUUID().toString();
//			String time = getTime();
//			Object[] data = new Object[]{tId, table, keyColumn, keyValue, alteredColumn, oldValue, newValue, time, "maher khalil"};
//			runAudit(data);
//			
//			table = "DATA";
//			keyColumn = "TITLE";
//			keyValue = "Captain Phillips";
//			alteredColumn = "STUDIO";
//			oldValue = "Sony";
//			newValue = "WB";
//			
//			// run on the table
//			updateQ = "UPDATE " + table + " SET " + alteredColumn + "='" + newValue + "' WHERE " + keyColumn + "='" + keyValue + "' AND " + alteredColumn + "='" + oldValue + "'";
//			execQ(updateQ);
//			
//			// add audit log
//			data = new Object[]{tId, table, keyColumn, keyValue, alteredColumn, oldValue, newValue, time, "maher khalil"};
//			runAudit(data);
//		}
//		
//		// transaction 3
//		{
//			String table = "DATA";
//			String keyColumn = "TITLE";
//			String keyValue = "American Hustle";
//			String alteredColumn = "TITLE";
//			String oldValue = "American Hustle";
//			String newValue = "American Hustler";
//			
//			// run on the table
//			String updateQ = "UPDATE " + table + " SET " + alteredColumn + "='" + newValue + "' WHERE " + keyColumn + "='" + keyValue + "' AND " + alteredColumn + "='" + oldValue + "'";
//			execQ(updateQ);
//			
//			// add audit log
//			Object[] data = new Object[]{UUID.randomUUID().toString(), table, keyColumn, keyValue, alteredColumn, oldValue, newValue, getTime(), "maher khalil"};
//			runAudit(data);
//		}
//	}
//	
//	public void revertToId(String tId) {
//		StringBuilder revertQ = new StringBuilder();
//		StringBuilder logQ = new StringBuilder();
//		
//		String logTable = "TEST_TABLE";
//		String undoRowNumQ = "select min(auto_increment) from " + logTable + " where id ='" + tId + "'";
//		long row_number = getNumberFromQ(undoRowNumQ);
//		if(row_number > 0) {
//			String logId = null;
//			String prevQueriedId = null;
//			
//			String getUndoQuery = "select id, table, key_column, key_column_value, altered_column, old_value, new_value from " + logTable + " where auto_increment >= " + row_number + " order by auto_increment desc";
//			Statement stmt = null;
//			ResultSet rs = null;
//			try {
//				stmt = this.conn.createStatement();
//				rs = stmt.executeQuery(getUndoQuery);
//				
//				while(rs.next()) {
//					String id = rs.getString("id");
//					String table = rs.getString("table");
//					String keyColumn = rs.getString("key_column");
//					String keyValue = rs.getString("key_column_value");
//					String alteredColumn = rs.getString("altered_column");
//					String oldValue = rs.getString("old_value");
//					String newValue = rs.getString("new_value");
//					
//					// WE WANT TO SWITCH THE OLD AND NEW VALUES!!!
//					String temp = newValue;
//					newValue = oldValue;
//					oldValue = temp;
//					String updateQ = null;
//					if(keyColumn.equals(alteredColumn)) {
//						updateQ = "UPDATE " + table + " SET " + alteredColumn + "='" + newValue + "' WHERE " + alteredColumn + "='" + oldValue + "'";
//					} else {
//						updateQ = "UPDATE " + table + " SET " + alteredColumn + "='" + newValue + "' WHERE " + keyColumn + "='" + keyValue + "' AND " + alteredColumn + "='" + oldValue + "'";
//					}
//					
//					// add to bulk set
//					revertQ.append(updateQ);
//					revertQ.append(";");
//					
//					// do same for audit
//					if(prevQueriedId == null) {
//						prevQueriedId = id;
//						logId = UUID.randomUUID().toString();
//					} else {
//						if(!prevQueriedId.equals(id)) {
//							// get a new id
//							logId = UUID.randomUUID().toString();
//						}
//					}
//					Object[] data = new Object[]{logId, table, keyColumn, keyValue, alteredColumn, oldValue, newValue, getTime(), "maher khalil"};
//					logQ.append(getAuditInsert(data));
//					logQ.append(";");
//				}
//			} catch (SQLException e) {
//				classLogger.error(Constants.STACKTRACE, e);
//			} finally {
//			      if(stmt != null) {
//		                try {
//		            stmt.close();
//		          } catch (SQLException e) {
//		            // TODO Auto-generated catch block
//		            classLogger.error(Constants.STACKTRACE, e);
//		          }
//		        }
//			}
//			
//			// actually run
//			execQ(revertQ.toString());
////			execQ(logQ.toString());
//		}
//	}
//	
//	public String getAuditInsert(Object[] data) {
//		String[] headers = new String[]{"ID", "TABLE", "KEY_COLUMN", "KEY_COLUMN_VALUE", "ALTERED_COLUMN", "OLD_VALUE", "NEW_VALUE", "TIMESTAMP", "USER"};
//		String[] types = new String[]{"VARCHAR(50)", "VARCHAR(200)", "VARCHAR(200)", "VARCHAR(200)", "VARCHAR(200)", "VARCHAR(200)", "VARCHAR(200)", "TIMESTAMP", "VARCHAR(200)"};
//		return RdbmsQueryBuilder.makeInsert("TEST_TABLE", headers, types, data);
//	}
//	
//	public void runAudit(Object[] data) {
//		execQ(getAuditInsert(data));
//	}
//	
//	public String getTime() {
//		java.sql.Timestamp t = java.sql.Timestamp.valueOf(LocalDateTime.now());
//		return t.toString();
//	}
//	
//	/**
//	 * Testing
//	 * @param args
//	 */
//	public static void main(String[] args) {
//		TestingAudit t = new TestingAudit();
//		if(init) {
//			t.initDb();
//			t.loadCsv("C:\\Users\\SEMOSS\\Desktop\\Movie Data.csv");
//			t.genModTables();
//			t.performMods();
//		} else {
//			t.connect("5355", "jdbc:h2:tcp://10.10.11.149:5355/nio:C:\\workspace\\Semoss_Dev\\testingAudit");
//			t.revertToId("3b1b7072-151d-47b9-a14a-09fe7b56d616");
//		}
//	}
//	
//	
//	/**
//	 * Generate an audit db
//	 */
//	public void initDb() {
//		String loc = "C:\\workspace\\Semoss_Dev\\testingAudit";
//		File f = new File(loc + ".mv.db");
//		if(f.exists()) {
//			f.delete();
//		}
//		try {
//			f.createNewFile();
//		} catch (IOException e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		}
//		
//		RdbmsConnectionBuilder builder = new RdbmsConnectionBuilder(RdbmsConnectionBuilder.CONN_TYPE.DIRECT_CONN_URL);
//		try {
//			String port = Utility.findOpenPort();
//			// create a random user and password
//			// get the connection object and start up the frame
//			server = Server.createTcpServer("-tcpPort", port, "-tcpAllowOthers");
//			serverUrl = "jdbc:h2:" + server.getURL() + "/nio:" + loc;
//			server.start();
//			
//			// update the builder
//			builder.setConnectionUrl(serverUrl);
//		} catch (SQLException e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		}
//		
//		builder.setDriver("H2_DB");
//		builder.setUserName("sa");
//		builder.setPassword("");
//		
//		System.out.println("Connection url is " + builder.getConnectionUrl());
//		System.out.println("Connection url is " + builder.getConnectionUrl());
//		System.out.println("Connection url is " + builder.getConnectionUrl());
//
//		try {
//			this.conn = builder.build();
//		} catch (SQLException e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		}
//	}
//	
//	private void connect(String port, String serverUrl) {
//		RdbmsConnectionBuilder builder = new RdbmsConnectionBuilder(RdbmsConnectionBuilder.CONN_TYPE.DIRECT_CONN_URL);
//		builder.setConnectionUrl(serverUrl);
//		builder.setDriver("H2_DB");
//		builder.setUserName("sa");
//		builder.setPassword("");
//		
//		System.out.println("Connection url is " + builder.getConnectionUrl());
//		System.out.println("Connection url is " + builder.getConnectionUrl());
//		System.out.println("Connection url is " + builder.getConnectionUrl());
//
//		try {
//			this.conn = builder.build();
//		} catch (SQLException e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		}
//	}
//	
//	/**
//	 * 
//	 * @param fileLoc
//	 */
//	public void loadCsv(String fileLoc) {
//		CSVFileHelper h = new CSVFileHelper();
//		h.parse(fileLoc);
//		String[] headers = h.getHeaders();
//		Object[][] typesMatrix = h.predictTypes();
//		
//		SemossDataType[] eTypes = new SemossDataType[headers.length];
//		String[] types = new String[headers.length];
//		for(int i = 0; i < headers.length; i++) {
//			eTypes[i] = (SemossDataType) typesMatrix[i][0];
//			types[i] = typesMatrix[i][0].toString();
//			if(types[i].equals("STRING")) {
//				types[i] = "VARCHAR(800)";
//			}
//		}
//		execQ(RdbmsQueryBuilder.makeOptionalCreate("DATA", headers, types));
//		
//		String psQuery = RdbmsQueryBuilder.createInsertPreparedStatementString("DATA", headers);
//		PreparedStatement ps = null;
//		try {
//			ps = this.conn.prepareStatement(psQuery);
//			
//			String[] nextRow = null;
//			while( (nextRow = h.getNextRow()) != null) {
//				for(int colIndex = 0; colIndex < nextRow.length; colIndex++) {
//					if(eTypes[colIndex] == SemossDataType.STRING) {
//						ps.setString(colIndex + 1, nextRow[colIndex]);
//					} else if(eTypes[colIndex] == SemossDataType.INT){
//						Integer value = Utility.getInteger(nextRow[colIndex] + "");
//						if (value != null) {
//							ps.setInt(colIndex + 1, value);
//						} else {
//							ps.setNull(colIndex + 1, java.sql.Types.DOUBLE);
//						}
//					} else if(eTypes[colIndex] == SemossDataType.DOUBLE){
//						Double value = Utility.getDouble(nextRow[colIndex] + "");
//						if (value != null) {
//							ps.setDouble(colIndex + 1, value);
//						} else {
//							ps.setNull(colIndex + 1, java.sql.Types.DOUBLE);
//						}
//					}
//				}
//				ps.addBatch();
//			}
//		} catch (SQLException e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		} finally {
//			if(ps != null) {
//				try {
//					ps.executeBatch();
//				} catch (SQLException e) {
//					classLogger.error(Constants.STACKTRACE, e);
//				}
//				try {
//					ps.close();
//				} catch (SQLException e) {
//					classLogger.error(Constants.STACKTRACE, e);
//				}
//			}
//		}
//	}
//	
//	/**
//	 * 
//	 * @param q
//	 */
//	private void execQ(String q) {
//		try(PreparedStatement statement = this.conn.prepareStatement(q)){
//			statement.execute();
//		} catch(SQLException e){
//			classLogger.error(Constants.STACKTRACE, e);
//		}
//	}
//	
//	private long getNumberFromQ(String q) {
//		Statement stmt = null;
//		ResultSet rs = null;
//		try {
//			stmt = this.conn.createStatement();
//			rs = stmt.executeQuery(q);
//			while(rs.next()) {
//				return rs.getLong(1);
//			}
//		} catch (SQLException e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		} finally {
//			if(rs != null) {
//				try {
//					rs.close();
//				} catch (SQLException e) {
//					classLogger.error(Constants.STACKTRACE, e);
//				}
//			}
//			if(stmt != null) {
//				try {
//					stmt.close();
//				} catch (SQLException e) {
//					classLogger.error(Constants.STACKTRACE, e);
//				}
//			}
//		}
//		return -1;
//	}
//}
