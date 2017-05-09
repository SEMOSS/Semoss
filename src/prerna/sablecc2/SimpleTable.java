package prerna.sablecc2;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ds.h2.H2Builder;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.poi.main.RDBMSEngineCreationHelper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class SimpleTable {

	private static final Logger LOGGER = LogManager.getLogger(H2Builder.class.getName());

	private Connection conn = null;
	protected String schema = "test"; // assign a default schema which is test
	protected String options = ":LOG=0;CACHE_SIZE=65536;LOCK_MODE=1;UNDO_LOG=0";

	// keep track of the indices that exist in the table for optimal speed in sorting
	protected Hashtable<String, String> columnIndexMap = new Hashtable<String, String>();
	protected boolean isInMem;
	protected int LIMIT_SIZE;
	private String tableName;
	private int varCharSize;
	
	public SimpleTable() {
		setDefaults();
	}
	
	private void setDefaults() {
		this.isInMem = true;
		this.LIMIT_SIZE = 10_000;
		this.varCharSize = 2000;
		this.tableName = getNewTableName();
	}

	/******************************************************************
	 * SETTER/GETTER METHODS
	 ******************************************************************/
	
	public String getTableName() {
		return this.tableName;
	}
	
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public void setInMemoryLimit(int memoryLimit) {
		this.LIMIT_SIZE = memoryLimit;
	}
	
	public void setCharacterLimit(int charLimit) {
		this.varCharSize = charLimit;
	}
	
	/******************************************************************
	 * END SETTER/GETTER METHODS
	 ******************************************************************/
	
	
	
	/******************************************************************
	 * UPDATE
	 ******************************************************************/
	
	/**
	 * 
	 * adds new row to the table
	 * 
	 * @param tableName
	 *            - name of table to add to
	 * @param cells
	 *            - values to add to table
	 * @param headers
	 *            - headers for the table
	 * @param types
	 *            - types of the table
	 * 
	 *            will add new row to the table, will create table if table does
	 *            not already exist
	 */
	public void addRow(String[] cells, String[] headers, String[] types) {
		
		try {
			String inserter = RdbmsQueryBuilder.makeInsert(headers, types, cells, new Hashtable<String, String>(), tableName);
			runQuery(inserter);
		} catch (SQLException ex) {
			ex.printStackTrace();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
	public void createTable(String tableName, String[] headers, String[] types) {
		try {
			String createTable = RdbmsQueryBuilder.makeCreate(tableName, headers, types);
			runQuery(createTable);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/******************************************************************
	 * UPDATE 
	 ******************************************************************/
	
	
	/******************************************************************
	 * CONNECTION METHODS
	 ******************************************************************/
	
	public Connection getConnection() {
		if (this.conn == null) {
			setConnection();
		}
		return this.conn;
	}
	
	private void setConnection() {
		if(isInMem) {
			try {
				Class.forName("org.h2.Driver");
				String url = "jdbc:h2:mem:" + this.schema + options;
				this.conn = DriverManager.getConnection(url, "sa", "");
				LOGGER.info("Simple Table Connection: "+this.conn);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		} else {
			
		}
	}
	
	public Connection convertFromInMemToPhysical(String physicalDbLocation) {
		try {
			File dbLocation = null;
			Connection previousConnection = null;
			if (isInMem) {
				LOGGER.info("CONVERTING FROM IN-MEMORY H2-DATABASE TO ON-DISK H2-DATABASE!");
				
				// if was in mem but want to push to specific existing location
				if (physicalDbLocation != null && !physicalDbLocation.isEmpty()) {
					dbLocation = new File(physicalDbLocation);
				}
			} else {
				LOGGER.info("CHANGEING SCHEMA FOR EXISTING ON-DISK H2-DATABASE!");
				
				if (physicalDbLocation == null || physicalDbLocation.isEmpty()) {
					LOGGER.info("SCHEMA IS ALREADY ON DISK AND DID NOT PROVIDE NEW SCHEMA TO CHAGNE TO!");
					return this.conn;
				}

				dbLocation = new File(physicalDbLocation);
				previousConnection = this.conn;
			}
			Class.forName("org.h2.Driver");

			// first need get the data in the memory table
			Date date = new Date();
			String dateStr = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss_SSSS").format(date);

			String folderToUse = null;
			String inMemScript = null;
			// this is the case where i do not care where the on-disk is created
			// so just create some random stuff
			if (dbLocation == null) {
				folderToUse = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "\\"
						+ RDBMSEngineCreationHelper.cleanTableName(this.schema) + dateStr + "\\";
				inMemScript = folderToUse + "_" + dateStr;
				physicalDbLocation = folderToUse.replace('/', '\\') + "_" + dateStr + "_database";
			} else {
				// this is the case when we have a specific schema we want to move the frame into
				// this is set when the physicalDbLocation parameter is not null or empty
				folderToUse = dbLocation.getParent();
				inMemScript = folderToUse + "_" + dateStr;
			}

			// if there is a current frame that we need to push to on disk
			// we need to save that data and then move it over
			boolean existingTable = tableExists(this.tableName);
			if (existingTable) {
				
				Connection newConnection = DriverManager.getConnection("jdbc:h2:nio:" + physicalDbLocation, "sa", "");
				copyTable(this.conn, this.tableName, newConnection, this.tableName);
				
				// drop the current table from in-memory or from old physical db
				runQuery(RdbmsQueryBuilder.makeDropTable(this.tableName));
				this.conn = newConnection;
			}

			this.schema = physicalDbLocation;
			this.isInMem = false;
			// close the existing connection if it was a previous on disk
			// connection
			// so we can clean up the file
			if (previousConnection != null) {
				previousConnection.close();
			}

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return this.conn;
	}
	
	public void closeConnection() {
		try {
			this.conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/******************************************************************
	 * END CONNECTION METHODS
	 ******************************************************************/
	
	
	
	/******************************************************************
	 * QUERY EXECUTION METHODS
	 ******************************************************************/
	
	// use this when result set is not expected back
	public void runQuery(String query) throws Exception {
		Statement stat = getConnection().createStatement();
		stat.execute(query);
		stat.close();
	}

	// use this when result set is expected
	public ResultSet executeQuery(String query) {
		try {
			return getConnection().createStatement().executeQuery(query);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/******************************************************************
	 * END QUERY EXECUTION METHODS
	 ******************************************************************/
	
	
	
	/******************************************************************
	 * INDEXING METHODS
	 ******************************************************************/

	protected void addColumnIndex(String tableName, String colName) {
		if (!columnIndexMap.containsKey(tableName + "+++" + colName)) {
			long start = System.currentTimeMillis();
	
			String indexSql = null;
			LOGGER.info("CREATING INDEX ON TABLE = " + tableName + " ON COLUMN = " + colName);
			try {
				String indexName = colName + "_INDEX_" + getNextNumber();
				indexSql = "CREATE INDEX " + indexName + " ON " + tableName + "(" + colName + ")";
				runQuery(indexSql);
				columnIndexMap.put(tableName + "+++" + colName, indexName);
				
				long end = System.currentTimeMillis();
	
				LOGGER.info("TIME FOR INDEX CREATION = " + (end - start) + " ms");
			} catch (Exception e) {
				LOGGER.info("ERROR WITH INDEX !!! " + indexSql);
				e.printStackTrace();
			}
		}
	}
	
	protected void removeColumnIndex(String tableName, String colName) {
		if (columnIndexMap.containsKey(tableName + "+++" + colName)) {
			LOGGER.info("DROPPING INDEX ON TABLE = " + tableName + " ON COLUMN = " + colName);
			String indexName = columnIndexMap.remove(tableName +  "+++" + colName);
			try {
				runQuery("DROP INDEX " + indexName);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	/******************************************************************
	 * END INDEXING METHODS
	 ******************************************************************/
	
	
	
	/******************************************************************
	 * UTILITY METHODS
	 ******************************************************************/
	
	//This method copies the table from the 'fromConnection' to the 'toConnection'
	private void copyTable(Connection fromConnection, String fromTable, Connection toConnection, String toTable) throws Exception {

		//We want to query the fromConnection to collect the columns and types to copy
		ResultSet rs = fromConnection.createStatement().executeQuery("SELECT * FROM "+fromTable+" LIMIT 1");
		ResultSetMetaData rmsd = rs.getMetaData();
		
		//collect column names and types
		int numOfCols = rmsd.getColumnCount();
		List<String> columns = new ArrayList<>(numOfCols);
		List<String> types = new ArrayList<>(numOfCols);
		
		for(int colCount = 1; colCount <= numOfCols; colCount++) {
			columns.add(rmsd.getColumnName(colCount));
			String type = rmsd.getColumnTypeName(colCount);
			if(type.equalsIgnoreCase("VARCHAR")) {
				type = "VARCHAR(800)";
			}
			types.add(type);
		}
		
		//generate the toTable using the toConnection with the columns and types we created
		String createTable = RdbmsQueryBuilder.makeCreate(toTable, columns.toArray(new String[]{}), types.toArray(new String[]{}));
		toConnection.createStatement().execute(createTable);
		
		
		//copy the data from fromTable to toTable
		String insertPreparedStatement = RdbmsQueryBuilder.createInsertPreparedStatementString(toTable, columns.toArray(new String[columns.size()]));
		
		//select the data we want to copy
		String selectFromTableQuery = RdbmsQueryBuilder.makeSelect(fromTable, columns, false);
		
		try {
			ResultSet resultSet = fromConnection.createStatement().executeQuery(selectFromTableQuery);
			
			//update the insert statement with the data we collected
			PreparedStatement insertStatement = toConnection.prepareStatement(insertPreparedStatement);
			int maxBatchSize = 500;
			int batchCount = 0;
			while(resultSet.next()) {
				 // Get the values from the table1 record
				insertStatement.clearParameters();
				for(int i = 0; i < columns.size(); i++) {
					String column = columns.get(i);
					String type = types.get(i).toUpperCase();
					if(type.startsWith("VARCHAR")) {
						insertStatement.setString(i+1, resultSet.getString(i+1));
					} else if(type.equals("DOUBLE")) {
						insertStatement.setDouble(i+1, resultSet.getDouble(i+1));
					} else if(type.equals("DATE")) {
						insertStatement.setDate(i+1, resultSet.getDate(i+1));
					}
					
				}
				
				insertStatement.addBatch();
				
				if(batchCount == maxBatchSize) {
					batchCount = 0;
					insertStatement.executeBatch();
				}
				batchCount++;
				
			}
			
			insertStatement.executeBatch();
			insertStatement.close();
 		} catch(Exception e) {
			
		}
	}
	
	/**
	 * 
	 * @param tableName
	 * @return true if table with name tableName exists, false otherwise
	 */
	protected boolean tableExists(String tableName) {
		String query = "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '" + tableName + "'";
		ResultSet rs = executeQuery(query);
		try {
			if (rs.next()) {
				return true;
			} else {
				return false;
			}
		} catch (SQLException e) {
			return false;
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	// get a new unique table name
	public String getNewTableName() {
		String name = "SIMPLETABLE" + getNextNumber();
		return name;
	}
	
	private String getNextNumber() {
		String uuid = UUID.randomUUID().toString();
		uuid = uuid.replaceAll("-", "_");
		// table names will be upper case because that is how it is set in
		// information schema
		return uuid.toUpperCase();
	}
	/******************************************************************
	 * END UTILITY METHODS
	 ******************************************************************/
	
}
