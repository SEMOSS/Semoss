package prerna.rdf.main;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.impl.rdbms.RdbmsConnectionBuilder;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.util.Utility;

public class TestingAudit {

	public Connection conn;
	
	public TestingAudit() {
		
	}
	
	public void performMods() {
		// TODO Auto-generated method stub
		
	}
	
	
	/**
	 * Testing
	 * @param args
	 */
	public static void main(String[] args) {
		TestingAudit t = new TestingAudit();
		t.initDb();
		t.loadCsv("C:\\Users\\SEMOSS\\Desktop\\Movie Data.csv");
		t.performMods();
	}
	
	
	/**
	 * Generate an audit db
	 */
	public void initDb() {
		String baseUrl = "jdbc:h2:nio:C:\\workspace\\Semoss_Dev";
		RdbmsConnectionBuilder builder = new RdbmsConnectionBuilder(RdbmsConnectionBuilder.CONN_TYPE.DIRECT_CONN_URL);
		builder.setConnectionUrl(baseUrl + "\\testingAudit");
		builder.setDriver("H2_DB");
		builder.setUserName("sa");
		builder.setPassword("");
		
		System.out.println("Connection url is " + builder.getConnectionUrl());
		System.out.println("Connection url is " + builder.getConnectionUrl());
		System.out.println("Connection url is " + builder.getConnectionUrl());

		try {
			this.conn = builder.build();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param fileLoc
	 */
	public void loadCsv(String fileLoc) {
		CSVFileHelper h = new CSVFileHelper();
		h.parse(fileLoc);
		String[] headers = h.getHeaders();
		Object[][] typesMatrix = h.predictTypes();
		
		SemossDataType[] eTypes = new SemossDataType[headers.length];
		String[] types = new String[headers.length];
		for(int i = 0; i < headers.length; i++) {
			eTypes[i] = (SemossDataType) typesMatrix[i][0];
			types[i] = typesMatrix[i][0].toString();
			if(types[i].equals("STRING")) {
				types[i] = "VARCHAR(800)";
			}
		}
		insertQuery(RdbmsQueryBuilder.makeOptionalCreate("DATA", headers, types));
		
		String psQuery = RdbmsQueryBuilder.createInsertPreparedStatementString("DATA", headers);
		PreparedStatement ps = null;
		try {
			ps = this.conn.prepareStatement(psQuery);
			
			String[] nextRow = null;
			while( (nextRow = h.getNextRow()) != null) {
				for(int colIndex = 0; colIndex < nextRow.length; colIndex++) {
					if(eTypes[colIndex] == SemossDataType.STRING) {
						ps.setString(colIndex + 1, nextRow[colIndex]);
					} else if(eTypes[colIndex] == SemossDataType.INT){
						Integer value = Utility.getInteger(nextRow[colIndex] + "");
						if (value != null) {
							ps.setInt(colIndex + 1, value);
						} else {
							ps.setNull(colIndex + 1, java.sql.Types.DOUBLE);
						}
					} else if(eTypes[colIndex] == SemossDataType.DOUBLE){
						Double value = Utility.getDouble(nextRow[colIndex] + "");
						if (value != null) {
							ps.setDouble(colIndex + 1, value);
						} else {
							ps.setNull(colIndex + 1, java.sql.Types.DOUBLE);
						}
					}
				}
				ps.addBatch();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if(ps != null) {
				try {
					ps.executeBatch();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				try {
					ps.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * 
	 * @param q
	 */
	private void insertQuery(String q) {
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

}
