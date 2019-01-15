package prerna.ds.h2;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.h2.tools.RunScript;
import org.h2.tools.Server;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.ds.EmptyIteratorException;
import prerna.ds.util.RdbmsFrameUtility;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IHeadersDataRow;
import prerna.poi.main.RDBMSEngineCreationHelper;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class H2Builder {

	private static final String CLASS_NAME = H2Builder.class.getName();
	private Logger logger = LogManager.getLogger(CLASS_NAME);
	
	protected static final String tempTable = "TEMP_TABLE98793";
	protected final String H2FRAME = "H2FRAME";

	protected Connection conn = null;
	protected String schema = "test"; // assign a default schema which is test
	protected String options = ":LOG=0;CACHE_SIZE=65536;LOCK_MODE=1;UNDO_LOG=0";
	protected Server server = null;
	protected String serverURL = null;
	protected Hashtable<String, String[]> tablePermissions = new Hashtable<String, String[]>();

	// keep track of the indices that exist in the table for optimal speed in sorting
	protected Hashtable<String, String> columnIndexMap = new Hashtable<String, String>();
	protected Hashtable<String, String> multiColumnIndexMap = new Hashtable<String, String>();

	// for writing the frame on disk
	// currently not used
	// would require reconsideration of dashboard, etc.
	// since frames are not in same schema
	protected boolean isInMem = true;
	protected final int LIMIT_SIZE;

	// provides a translation for incoming types into something H2 can understand
	protected Map<String, String> typeConversionMap = new HashMap<String, String>(); {
		typeConversionMap.clear();
		
		typeConversionMap.put("INT", "INT");
		typeConversionMap.put("LONG", "INT");
		
		typeConversionMap.put("NUMBER", "DOUBLE");
		typeConversionMap.put("FLOAT", "DOUBLE");
		typeConversionMap.put("DOUBLE", "DOUBLE");

		typeConversionMap.put("DATE", "DATE");
		typeConversionMap.put("TIMESTAMP", "TIMESTAMP");
		
		typeConversionMap.put("STRING", "VARCHAR(800)");
	}

	// name of the main table for H2
	protected String tableName;

	/*************************** CONSTRUCTORS **************************************/

	protected H2Builder() {
		this.tableName = getNewTableName();
		this.LIMIT_SIZE = RdbmsFrameUtility.getLimitSize();
		this.logger = LogManager.getLogger(CLASS_NAME);
	}
	
	protected H2Builder(String tableName) {
		this.tableName = tableName;
		this.LIMIT_SIZE = RdbmsFrameUtility.getLimitSize();
		this.logger = LogManager.getLogger(CLASS_NAME);
	}

	// get a new unique table name
	public String getNewTableName() {
		String name = H2FRAME + getNextNumber();
		return name;
	}
	
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	protected void setLogger(Logger logger) {
		this.logger = logger;
	}


	/*************************** 
	 * 	CREATE 
	 * *************************/

	public void addRowsViaIterator(Iterator<IHeadersDataRow> iterator, String tableName, Map<String, SemossDataType> typesMap) {
		try {
			// keep a batch size so we dont get heapspace
			final int batchSize = 5000;
			int count = 0;

			PreparedStatement ps = null;
			SemossDataType[] types = null;
			String[] strTypes = null;

			// we loop through every row of the csv
			while (iterator.hasNext()) {
				IHeadersDataRow headerRow = iterator.next();
				Object[] nextRow = headerRow.getValues();

				// need to set values on the first iteration
				if (ps == null) {
					String[] headers = headerRow.getHeaders();
					// get the data types
					types = new SemossDataType[headers.length];
					strTypes = new String[headers.length];
					for (int i = 0; i < types.length; i++) {
						types[i] = typesMap.get(headers[i]);
						strTypes[i] = SemossDataType.convertDataTypeToString(types[i] );
					}
					// alter the table to have the column information if not
					// already present
					// this will also create a new table if the table currently
					// doesn't exist
					alterTableNewColumns(tableName, headers, strTypes);

					// set the PS based on the headers
					ps = createInsertPreparedStatement(tableName, headers);
				}

				// we need to loop through every value and cast appropriately
				for (int colIndex = 0; colIndex < nextRow.length; colIndex++) {
					SemossDataType type = types[colIndex];
					if (type == SemossDataType.INT) {
						if(nextRow[colIndex] instanceof Number) {
							ps.setInt(colIndex + 1, ((Number) nextRow[colIndex]).intValue());
						} else {
							Integer value = Utility.getInteger(nextRow[colIndex] + "");
							if (value != null) {
								ps.setInt(colIndex + 1, value);
							} else {
								ps.setNull(colIndex + 1, java.sql.Types.DOUBLE);
							}
						}
					} else if(type == SemossDataType.DOUBLE) {
						if(nextRow[colIndex] instanceof Number) {
							ps.setDouble(colIndex + 1, ((Number) nextRow[colIndex]).doubleValue());
						} else {
							Double value = Utility.getDouble(nextRow[colIndex] + "");
							if (value != null) {
								ps.setDouble(colIndex + 1, value);
							} else {
								ps.setNull(colIndex + 1, java.sql.Types.DOUBLE);
							}
						}
					} else if (type == SemossDataType.DATE) {
						if (nextRow[colIndex] == null) {
							ps.setNull(colIndex + 1, java.sql.Types.DATE);
						} else if(nextRow[colIndex] instanceof SemossDate) {
							Date d = ((SemossDate) nextRow[colIndex]).getDate();
							if(d != null) {
								ps.setDate(colIndex + 1, new java.sql.Date( d.getTime() ) );
							} else {
								ps.setNull(colIndex + 1, java.sql.Types.DATE);
							}
						} else {
							SemossDate value = SemossDate.genDateObj(nextRow[colIndex] + "");
							if (value != null) {
								ps.setDate(colIndex + 1, new java.sql.Date(value.getDate().getTime()));
							} else {
								ps.setNull(colIndex + 1, java.sql.Types.DATE);
							}
						}
					} else if (type == SemossDataType.TIMESTAMP) {
						if (nextRow[colIndex] == null) {
							ps.setNull(colIndex + 1, java.sql.Types.DATE);
						} else if(nextRow[colIndex] instanceof SemossDate) {
							Date d = ((SemossDate) nextRow[colIndex]).getDate();
							if(d != null) {
								ps.setTimestamp(colIndex + 1, new java.sql.Timestamp( d.getTime() ) );
							} else {
								ps.setNull(colIndex + 1, java.sql.Types.TIMESTAMP);
							}
						} else {
							SemossDate value = SemossDate.genDateObj(nextRow[colIndex] + "");
							if (value != null) {
								ps.setTimestamp(colIndex + 1, new java.sql.Timestamp(value.getDate().getTime()));
							} else {
								ps.setNull(colIndex + 1, java.sql.Types.TIMESTAMP);
							}
						}
					} else {
						if(nextRow[colIndex] == null) {
							ps.setNull(colIndex + 1, java.sql.Types.VARCHAR);
						} else {
							String value = nextRow[colIndex] + "";
							if(value.length() > 800) {
								value = value.substring(0, 796) + "...";
							}
							ps.setString(colIndex + 1, value + "");
						}
					}
				}
				// add it
				ps.addBatch();

				// batch commit based on size
				if (++count % batchSize == 0) {
					logger.info("Executing batch .... row num = " + count);
					ps.executeBatch();
				}
			}

			if(ps == null) {
				throw new EmptyIteratorException("Iterator generated returned no values");
			}
			
			// well, we are done looping through now
			logger.info("Executing final batch .... row num = " + count);
			ps.executeBatch(); // insert any remaining records
			ps.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		// shift to on disk if number of records is getting large
		if (isInMem && getNumRecords(tableName) > LIMIT_SIZE) {
			// let the method determine where the new schema will be
			convertFromInMemToPhysical(null);
		}
	}

	/**
	 * Create a prepared statement in order to perform bulk inserts into a table
	 * 
	 * @param TABLE_NAME
	 *            The name of the table
	 * @param columns
	 *            The columns that will be used in the inserting
	 * @return The prepared statement
	 */
	public PreparedStatement createInsertPreparedStatement(final String TABLE_NAME, final String[] columns) {
		String sql = RdbmsQueryBuilder.createInsertPreparedStatementString(TABLE_NAME, columns);

		PreparedStatement ps = null;
		try {
			// create the prepared statement using the sql query defined
			ps = getConnection().prepareStatement(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return ps;
	}

	public PreparedStatement createUpdatePreparedStatement(final String TABLE_NAME, final String[] columnsToUpdate, final String[] whereColumns) {
		// generate the sql for the prepared statement
		String sql = RdbmsQueryBuilder.createUpdatePreparedStatementString(TABLE_NAME, columnsToUpdate, whereColumns);
		
		PreparedStatement ps = null;
		try {
			// create the prepared statement using the sql query defined
			ps = getConnection().prepareStatement(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return ps;
	}
	
	public PreparedStatement createMergePreparedStatement(final String TABLE_NAME, final String[] keyColumns, final String[] updateColumns) {
		String sql = RdbmsQueryBuilder.createMergePreparedStatementString(TABLE_NAME, keyColumns, updateColumns);
		return createPreparedStatement(sql);
	}

	
	private PreparedStatement createPreparedStatement(String sql) {
		PreparedStatement ps = null;
		try {
			// create the prepared statement using the sql query defined
			ps = getConnection().prepareStatement(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return ps;
	}
	/*************************** 
	 * 	END CREATE 
	 * *************************/

	public List<Object[]> getFlatTableFromQuery(String query) {
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = getConnection().createStatement();
			rs = stmt.executeQuery(query);
			if (rs != null) {
				ResultSetMetaData rsmd = rs.getMetaData();
				int numOfCol = rsmd.getColumnCount();
				List<Object[]> data = new Vector<Object[]>(numOfCol);
				while (rs.next()) {
					Object[] row = new Object[numOfCol];
					for (int i = 1; i <= numOfCol; i++) {
						row[i - 1] = rs.getObject(i);
					}
					data.add(row);
				}
				return data;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if(rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if(stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return new Vector<Object[]>(0);
	}


	/*************************** 
	 * 	END READ 
	 * *************************/

	/*************************** 
	 * 	UPDATE
	 * *************************/
	
	/**
	 * Adds new headers with associated types to the table
	 * 
	 * @param tableName
	 *            - table name to modify
	 * @param headers
	 *            - header names
	 * @param types
	 *            - types
	 * 
	 */
	public void alterTableNewColumns(String tableName, String[] headers, String[] types) {
		types = cleanTypes(types);
		try {
			if (tableExists(tableName)) {
				List<String> newHeaders = new ArrayList<String>();
				List<String> newTypes = new ArrayList<String>();

				// determine the new headers and types
				for (int i = 0; i < headers.length; i++) {
					if (!ArrayUtilityMethods.arrayContainsValueIgnoreCase(getHeaders(tableName), headers[i].toUpperCase())) {
						// these are the columns to create
						newHeaders.add(headers[i]);
						newTypes.add(types[i]);
					}
				}

				// if we have new headers add them to the table
				if (!newHeaders.isEmpty()) {
					// if there is an index
					// definitely get rid of it
					// or this takes forever on big data
					List<String[]> indicesToAdd = new Vector<String[]>();
					Set<String> colIndexMapKeys = new HashSet<String>(this.columnIndexMap.keySet());
					for(String tableColConcat : colIndexMapKeys) {
						// table name and col name are appended together with +++
						String[] tableCol = tableColConcat.split("\\+\\+\\+");
						indicesToAdd.add(tableCol);
						removeColumnIndex(tableCol[0], tableCol[1]);
					}
					
					String alterQuery = RdbmsQueryBuilder.makeAlter(tableName, newHeaders.toArray(new String[] {}), newTypes.toArray(new String[] {}));
					logger.debug("ALTERING TABLE: " + alterQuery);
					runQuery(alterQuery);
					logger.debug("DONE ALTER TABLE");
					
					for(String[] tableColIndex : indicesToAdd ) {
						addColumnIndex(tableColIndex[0], tableColIndex[1]);
					}
				}
			} else {
				// if table doesn't exist then create one with headers and types
				String createTable = RdbmsQueryBuilder.makeCreate(tableName, headers, types);
				logger.info("Generating SQL table");
				logger.debug("CREATING TABLE: " + createTable);
				runQuery(createTable);
				logger.info("Finished generating SQL table");
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}

	/**
	 * adds new row to the table
	 * @param tableName				name of table to add to
	 * @param cells					values to add to table
	 * @param headers				headers for the table
	 * @param types					types of the table
	 * will add new row to the table, will create table if table does
	 * not already exist
	 */
	public void addRow(String tableName, String[] cells, String[] headers, String[] types) {
		boolean create = true;
		types = cleanTypes(types);

		// create table if it does not already exist
		try {
			if (!tableExists(tableName)) {
				String createTable = RdbmsQueryBuilder.makeCreate(tableName, headers, types);
				runQuery(createTable);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			create = false;
		}

		// add the row to the table
		try {
			if (create) {
				cells = Utility.castToTypes(cells, types);
				String inserter = RdbmsQueryBuilder.makeInsert(headers, types, cells, new Hashtable<String, String>(), tableName);
				runQuery(inserter);
			}
		} catch (SQLException ex) {
			System.out.println("Exception: " + ex);
		} catch (Exception ex) {
			System.out.println("Errored.. nothing to do");
		}
	}
	
	/*************************** 
	 * 	END UPDATE 
	 * *************************/
	
	/*************************** 
	 * 	DELETE
	 * *************************/
	

	/**
	 * Drops the column from the main table
	 * 
	 * @param columnHeader
	 *            - column to drop
	 */
	public void dropColumn(String columnHeader) {
		try {
			String dropColumnQuery = RdbmsQueryBuilder.makeDropColumn(columnHeader, tableName);
			runQuery(dropColumnQuery);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * used to drop the table when the insight is closed
	 */
	protected void dropTable() {
		if(tableExists(tableName)) {
			try {
				String dropTableQuery = RdbmsQueryBuilder.makeDropTable(tableName);
				runQuery(dropTableQuery);
			} catch (Exception e) {
				e.printStackTrace();
			}
			logger.info("DROPPED SQL TABLE ::: " + tableName);
		} else {
			logger.info("TABLE " + tableName + " DOES NOT EXIST");
		}
	}

	
	/*************************** 
	 * 	END DELETE
	 * *************************/

	/*************************** 
	 * 	UTILITY
	 * *************************/
	
	/**
	 * 
	 * @param tableName
	 * @return true if table with name tableName exists, false otherwise
	 */
	protected boolean tableExists(String tableName) {
		String query = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '" + tableName + "'";
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = getConnection().createStatement();
			rs = stmt.executeQuery(query);
			if (rs.next()) {
				return true;
			} else {
				return false;
			}
		} catch (SQLException e) {
			return false;
		} finally {
			if(rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if(stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}


	protected void addColumnIndex(String tableName, String colName) {
		if (!columnIndexMap.containsKey(tableName + "+++" + colName)) {
			long start = System.currentTimeMillis();

			String indexSql = null;
			logger.info("Generating index on SQL Table on column = " + colName);
			logger.debug("CREATING INDEX ON TABLE = " + tableName + " ON COLUMN = " + colName);
			try {
				String indexName = colName + "_INDEX_" + getNextNumber();
				indexSql = "CREATE INDEX " + indexName + " ON " + tableName + "(" + colName + ")";
				runQuery(indexSql);
				columnIndexMap.put(tableName + "+++" + colName, indexName);
				
				long end = System.currentTimeMillis();

				logger.debug("TIME FOR INDEX CREATION = " + (end - start) + " ms");
				logger.info("Finished generating indices on SQL Table on column = " + colName);
			} catch (Exception e) {
				logger.debug("ERROR WITH INDEX !!! " + indexSql);
				e.printStackTrace();
			}
		}
	}
	
	protected void addColumnIndex(String tableName, String[] colNames) {
		StringBuilder multiColIndexNameBuilder = new StringBuilder(colNames[0]);
		for(int i = 1; i < colNames.length; i++) {
			multiColIndexNameBuilder.append("__").append(colNames[i]);
		}
		String multiColIndexName = multiColIndexNameBuilder.toString();
		if (!multiColumnIndexMap.containsKey(tableName + "+++" + multiColIndexName)) {
			long start = System.currentTimeMillis();

			StringBuilder indexSqlBuilder = new StringBuilder();
			logger.info("Generating index on SQL Table columns = " + StringUtils.join(colNames,", "));
			logger.debug("CREATING INDEX ON TABLE = " + tableName + " ON COLUMNS = " + multiColIndexNameBuilder);
			try {
				String indexName = multiColIndexNameBuilder + "_INDEX_" + getNextNumber();
				indexSqlBuilder.append("CREATE INDEX ").append(indexName).append(" ON ").append(tableName)
						.append("(").append(colNames[0]);
				for(int i = 1; i < colNames.length; i++) {
					indexSqlBuilder.append(",").append(colNames[i]);
				}
				indexSqlBuilder.append(")");
				String indexSql = indexSqlBuilder.toString();
				runQuery(indexSql);
				multiColumnIndexMap.put(tableName + "+++" + multiColIndexName, indexName);
				
				long end = System.currentTimeMillis();

				logger.debug("TIME FOR INDEX CREATION = " + (end - start) + " ms");
				logger.info("Finished generating indices on SQL Table on columns = " + StringUtils.join(colNames,", "));
			} catch (Exception e) {
				logger.debug("ERROR WITH INDEX !!! " + multiColIndexName);
				e.printStackTrace();
			}
		}
	}

	protected void removeColumnIndex(String tableName, String colName) {
		if (columnIndexMap.containsKey(tableName + "+++" + colName)) {
			logger.info("Removing index on SQL Table column = " + colName);
			logger.debug("DROPPING INDEX ON TABLE = " + tableName + " ON COLUMN = " + colName);
			String indexName = columnIndexMap.remove(tableName +  "+++" + colName);
			try {
				runQuery("DROP INDEX " + indexName);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	protected void removeColumnIndex(String tableName, String[] colNames) {
		StringBuilder multiColIndexNameBuilder = new StringBuilder(colNames[0]);
		for(int i = 1; i < colNames.length; i++) {
			multiColIndexNameBuilder.append("__").append(colNames[i]);
		}
		String multiColIndexName = multiColIndexNameBuilder.toString();
		if (multiColumnIndexMap.containsKey(tableName + "+++" + multiColIndexName)) {
			logger.info("DROPPING INDEX ON TABLE = " + tableName + " ON COLUMNS = " + multiColIndexName);
			String indexName = multiColumnIndexMap.remove(tableName +  "+++" + multiColIndexName);
			try {
				runQuery("DROP INDEX " + indexName);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	protected void clearColumnIndexMap() {
		this.columnIndexMap.clear();
	}
	
	private String getNextNumber() {
		String uuid = UUID.randomUUID().toString();
		uuid = uuid.replaceAll("-", "_");
		// table names will be upper case because that is how it is set in
		// information schema
		return uuid.toUpperCase();
	}

	// changing from private to public access to get connection url
	// to pkql console
	public Connection getConnection() {
		if (this.conn == null) {
			try {

				Class.forName("org.h2.Driver");
				// jdbc:h2:~/test

				// this will have to update
				String url = "jdbc:h2:mem:" + this.schema + options;
				this.conn = DriverManager.getConnection(url, "sa", "");
				// register the MEDIAN Aggregation Function we have defined
				Statement stmt = this.conn.createStatement();
				stmt.execute("DROP AGGREGATE IF EXISTS MEDIAN");
				stmt.close();
				stmt = this.conn.createStatement();
				stmt.execute("CREATE AGGREGATE MEDIAN FOR \"prerna.ds.h2.H2MedianAggregation\";");
				stmt.close();
				
				logger.debug("The connection is.. " + url);
				// getConnection("jdbc:h2:C:/Users/pkapaleeswaran/h2/test.db;LOG=0;CACHE_SIZE=65536;LOCK_MODE=0;UNDO_LOG=0",
				// "sa", "");

				// Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
				// conn =
				// DriverManager.getConnection("jdbc:monetdb://localhost:50000/demo",
				// "monetdb", "monetdb");
				// ResultSet rs = conn.createStatement().executeQuery("Select
				// count(*) from voyages");

			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return this.conn;
	}

	public Connection convertFromInMemToPhysical(String physicalDbLocation) {
		// get the directory separator
		final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

		try {
			File dbLocation = null;
			Connection previousConnection = null;
			if (isInMem) {
				logger.debug("CONVERTING FROM IN-MEMORY H2-DATABASE TO ON-DISK H2-DATABASE!");
				
				// if was in mem but want to push to specific existing location
				if (physicalDbLocation != null && !physicalDbLocation.isEmpty()) {
					dbLocation = new File(physicalDbLocation);
				}
			} else {
				logger.debug("CHANGEING SCHEMA FOR EXISTING ON-DISK H2-DATABASE!");
				
				if (physicalDbLocation == null || physicalDbLocation.isEmpty()) {
					logger.debug("SCHEMA IS ALREADY ON DISK AND DID NOT PROVIDE NEW SCHEMA TO CHAGNE TO!");
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
			// this is the case where i do not care where the on-disk is created
			// so just create some random stuff
			if (dbLocation == null) {
				folderToUse = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + 
						DIR_SEPARATOR + RDBMSEngineCreationHelper.cleanTableName(this.schema) + dateStr + 
						DIR_SEPARATOR + "_" + dateStr + "_database";
				physicalDbLocation = folderToUse;
			} else {
				// this is the case when we have a specific schema we want to move the frame into
				// this is set when the physicalDbLocation parameter is not null or empty
				folderToUse = dbLocation.getParent();
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
			} else {
				// just create a new connection
				this.conn = DriverManager.getConnection("jdbc:h2:nio:" + physicalDbLocation, "sa", "");
			}

			this.schema = physicalDbLocation;
			this.isInMem = false;
			this.conn.commit();
			// register the MEDIAN Aggregation Function we have defined
			Statement stmt = this.conn.createStatement();
			stmt.execute("DROP AGGREGATE IF EXISTS MEDIAN");
			stmt.close();
			stmt = this.conn.createStatement();
			stmt.execute("CREATE AGGREGATE MEDIAN FOR \"prerna.ds.h2.H2MedianAggregation\";");
			stmt.close();

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
	
	public void closeConnection() {
		try {
			this.conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public String getTableName() {
		return tableName;
	}

	// TODO: should this be private?
	public String[] getHeaders(String tableName) {
		List<String> headers = new ArrayList<String>();

		String columnQuery = "SHOW COLUMNS FROM " + tableName;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = getConnection().createStatement();
			rs = stmt.executeQuery(columnQuery);
			while (rs.next()) {
				String header = rs.getString("FIELD");
				headers.add(header);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if(rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if(stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return headers.toArray(new String[] {});
	}

	protected String cleanType(String type) {
		if (type == null) {
			type = "VARCHAR(800)";
		}
		type = type.toUpperCase();
		if (typeConversionMap.containsKey(type)) {
			type = typeConversionMap.get(type);
		} else {
			if (typeConversionMap.containsValue(type)) {
				return type;
			}
			type = "VARCHAR(800)";
		}
		return type;
	}

	protected String[] cleanTypes(String[] types) {
		String[] cleanTypes = new String[types.length];
		for (int i = 0; i < types.length; i++) {
			cleanTypes[i] = cleanType(types[i]);
		}

		return cleanTypes;
	}
	
	public String getSchema() {
		return this.schema;
	}

	/**
	 * Sets the schema for the connection This is used to create a different
	 * schema for each user to facilitate BE join
	 * 
	 * @param schema
	 */
	public void setSchema(String schema) {
		if (schema != null) {
			if (!this.schema.equals(schema)) {
				logger.debug("Schema being modified from: '" + this.schema + "' to new schema for user: '" + schema + "'");
				logger.debug("SCHEMA NOW... >>> " + schema);
				this.schema = schema;
				if (schema.equalsIgnoreCase("-1")) {
					this.schema = "test";
				}
				this.conn = null;
				getConnection();
			}
		}
	}

	// use this when result set is not expected back
	public void runQuery(String query) throws Exception {
		long start = System.currentTimeMillis();
		logger.info("Running SQL query");
		logger.debug("Running query : " + query);
		Statement stmt = null;
		try {
			stmt = getConnection().createStatement();
			stmt.execute(query);
		} finally {
			if(stmt != null) {
				stmt.close();
			}
		}
		long end = System.currentTimeMillis();
		logger.info("Time to execute = " + (end-start) + "ms");
	}

	// use this when result set is expected
	protected ResultSet executeQuery(String query) {
		try {
			return getConnection().createStatement().executeQuery(query);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	// save the main table
	protected void save(String fileName, String frameName) throws IOException {
		String saveScript = "SCRIPT TO '" + fileName + "' COMPRESSION GZIP TABLE " + frameName;
		try {
			runQuery(saveScript);
			if (new File(fileName).length() == 0){
				throw new IllegalArgumentException("Attempting to save an empty H2 frame");
			}
		} catch (Exception e) {
			throw new IOException("Error occured attempting to cache SQL Frame");
		}
	}

	/**
	 * 
	 * @param filePath
	 */
	protected void open(String filePath) throws IOException {
		// drop the aggregate if it exists since the opening of the script will
		// fail otherwise
		Statement stmt = null;
		try {
			stmt = this.conn.createStatement();
			stmt.execute("DROP AGGREGATE IF EXISTS MEDIAN");
		} catch (SQLException e1) {
			e1.printStackTrace();
		} finally {
			if(stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		Reader r = null;
		GZIPInputStream gis = null;
		FileInputStream fis = null;
		try {
			//load the frame
			fis = new FileInputStream(filePath);
			gis = new GZIPInputStream(fis);
			r = new InputStreamReader(gis);
			RunScript.execute(this.conn, r);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IOException("Error occured opening cached SQL Frame");
		} finally {
			try {
				if(fis != null) {
					fis.close();
				}
				if(gis != null) {
					gis.close();
				}
				if(r != null) {
					r.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	// Connects the frame
	public String connectFrame() {
		if (server == null) {
			try {
				String port = Utility.findOpenPort();
				// create a random user and password
				// get the connection object and start up the frame
				server = Server.createTcpServer("-tcpPort", port, "-tcpAllowOthers");
				// server = Server.createPgServer("-baseDir", "~",
				// "-pgAllowOthers"); //("-tcpPort", "9999");
				if(isInMem) {
					serverURL = "jdbc:h2:" + server.getURL() + "/mem:" + this.schema + options;
				} else {
					serverURL = "jdbc:h2:" + server.getURL() + "/nio:" + this.schema;
				}
				server.start();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		printSchemaTables();
		System.out.println("URL... " + serverURL);
		return serverURL;
	}

	private void printSchemaTables() {
		try {
			Class.forName("org.h2.Driver");
			String url = serverURL;
			Connection conn = DriverManager.getConnection(url, "sa", "");
			ResultSet rs = conn.createStatement()
					.executeQuery("SELECT TABLE_NAME FROM INFORMATIOn_SCHEMA.TABLES WHERE TABLE_SCHEMA='PUBLIC'");

			while (rs.next())
				System.out.println("Table name is " + rs.getString(1));

			url = "jdbc:h2:mem:test";
			conn = this.conn;
			rs = conn.createStatement()
					.executeQuery("SELECT TABLE_NAME FROM INFORMATIOn_SCHEMA.TABLES WHERE TABLE_SCHEMA='PUBLIC'");

			// String schema = this.conn.getSchema();
			System.out.println(".. " + conn.getMetaData().getURL());
			System.out.println(".. " + conn.getMetaData().getUserName());
			// System.out.println(".. " + conn.getMetaData().getS);

			while (rs.next())
				System.out.println("Table name is " + rs.getString(1));

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public int getNumRecords(String tableName) {
		String query = "SELECT COUNT(*) * " + getHeaders(tableName).length + " FROM " + tableName;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = getConnection().createStatement();
			rs = stmt.executeQuery(query);
			while (rs.next()) {
				return rs.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if(rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if(stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}

		return 0;
	}
	
	public String[] createUser(String tableName) {
		// really simple
		// find an open port
		// once found
		// create url with connection string and send it back

		// need to pass the username and password back
		// the username is specific to an insight and possibly gives access only
		// to that insight
		// I need to get the insight table - i.e. the table backing the insight
		String[] retString = new String[2];

		if (!tablePermissions.containsKey(tableName)) {
			try {

				// create a random user and password
				Statement stmt = conn.createStatement();
				String userName = Utility.getRandomString(23);
				String password = Utility.getRandomString(23);
				retString[0] = userName;
				retString[1] = password;
				String query = "CREATE USER " + userName + " PASSWORD '" + password + "'";

				stmt.executeUpdate(query);

				// should not give admin permission
				// query = "ALTER USER " + userName + " ADMIN TRUE";

				// create a new role for this table
				query = "CREATE ROLE IF NOT EXISTS " + tableName + "READONLY";
				stmt.executeUpdate(query);
				query = "GRANT SELECT, INSERT, UPDATE ON " + tableName + " TO " + tableName + "READONLY";
				stmt.executeUpdate(query);

				// assign this to our new user
				query = "GRANT " + tableName + "READONLY TO " + userName;
				stmt.executeUpdate(query);

				System.out.println("username " + userName);
				System.out.println("Pass word " + password);

				tablePermissions.put(tableName, retString);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return tablePermissions.get(tableName);
	}

	public void disconnectFrame() {
		server.stop();
		server = null;
		serverURL = null;
	}

	public boolean isEmpty(String tableName) {
		// first check if the table exists
		if (tableExists(tableName)) {
			// now check if there is at least one row
			String query = "SELECT * FROM " + tableName + " LIMIT 1";
			Statement stmt = null;
			ResultSet rs = null;
			try {
				stmt = getConnection().createStatement();
				rs = stmt.executeQuery(query);
				if (rs.next()) {
					return false;
				}
			} catch (SQLException e) {
				e.printStackTrace();
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
		return true;
	}

	/**
	 * Determine if the frame is in-memory or off-heap
	 */
	public boolean isInMem() {
		return this.isInMem;
	}
	
	public DatabaseMetaData getBuilderMetadata() {
		try {
			return this.conn.getMetaData();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	
	/********************************************************************************************************
	 * 			LEGACY CODE FOR OLD/NON PKQL DMC INSIGHTS
	 ********************************************************************************************************/
	
	protected void deleteAllRows(String tableName) {
		String query = "DELETE FROM " + tableName + " WHERE 1 != 0";
		try {
			runQuery(query);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public LinkedHashMap<String, String> connectToExistingTable(String tableName) {
		String query = "SELECT COLUMN_NAME, TYPE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='"
				+ tableName + "'";
		this.conn = getConnection();
		try {
			if(this.conn.isClosed()) {
				this.conn = null;
				this.conn = getConnection();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		LinkedHashMap<String, String> dataTypeMap = new LinkedHashMap<String, String>();
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = getConnection().createStatement();
			rs = stmt.executeQuery(query);
			while(rs.next()) {
				String colName = rs.getString(1).toUpperCase();
				String dataType = rs.getString(2).toUpperCase();
				dataTypeMap.put(colName, dataType);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if(rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if(stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
		if(dataTypeMap.isEmpty()) {
			throw new IllegalArgumentException("Table name " + tableName + " does not exist or is empty");
		}
		
		this.tableName = tableName;
		return dataTypeMap;
	}
	
}
