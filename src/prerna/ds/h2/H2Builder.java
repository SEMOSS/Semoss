package prerna.ds.h2;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
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
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;
import java.util.zip.GZIPInputStream;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.h2.tools.RunScript;
import org.h2.tools.Server;

import com.google.gson.Gson;

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
	protected Map<String, String> typeConversionMap = new HashMap<String, String>();
	{
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

//	// specifies the join types for an H2 frame
//	public enum Join {
//		INNER("INNER JOIN"), 
//		LEFT_OUTER("LEFT OUTER JOIN"), 
//		RIGHT_OUTER("RIGHT OUTER JOIN"), 
//		FULL_OUTER("FULL OUTER JOIN"), 
//		CROSS("CROSS JOIN");
//		
//		String name;
//
//		Join(String n) {
//			name = n;
//		}
//
//		public String getName() {
//			return name;
//		}
//	}

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
						String value = nextRow[colIndex] + "";
						if(value.length() > 800) {
							value = value.substring(0, 796) + "...";
						}
						ps.setString(colIndex + 1, value + "");
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
					logger.info("ALTERING TABLE: " + alterQuery);
					runQuery(alterQuery);
					logger.info("DONE ALTER TABLE");
					
					for(String[] tableColIndex : indicesToAdd ) {
						addColumnIndex(tableColIndex[0], tableColIndex[1]);
					}
				}
			} else {
				// if table doesn't exist then create one with headers and types
				String createTable = RdbmsQueryBuilder.makeCreate(tableName, headers, types);
				logger.info("CREATING TABLE: " + createTable);
				runQuery(createTable);
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}

	// only use this for analytics for now
	// update the table with new values
	// the last values and the last columnHeaders are what is updated
	// used only for updating one column
//	public void updateTable(String[] headers, Object[] values, String[] columnHeaders) {
//		headers = cleanHeaders(headers);
//		columnHeaders = cleanHeaders(columnHeaders);
//		
//		try {
//
//			Object[] joinColumn = new Object[columnHeaders.length - 1];
//			System.arraycopy(columnHeaders, 0, joinColumn, 0, columnHeaders.length - 1);
//			Object[] newColumn = new Object[1];
//			newColumn[0] = columnHeaders[columnHeaders.length - 1];
//
//			Object[] joinValue = new Object[values.length - 1];
//			System.arraycopy(values, 0, joinValue, 0, values.length - 1);
//			Object[] newValue = new Object[1];
//			newValue[0] = values[columnHeaders.length - 1];
//
//			String updateQuery = RdbmsQueryBuilder.makeUpdate(tableName, joinColumn, newColumn, joinValue, newValue);
//			runQuery(updateQuery);
//
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
//
//	/**
//	 * This method is responsible for processing the data associated with an
//	 * iterator and adding it to the H2 table
//	 * 
//	 * @param iterator
//	 * @param oldHeaders
//	 * @param newHeaders
//	 * @param types
//	 * @param joinType
//	 */
//	public void processIterator(Iterator<IHeadersDataRow> iterator, String[] oldHeaders, String[] newHeaders, String[] types, Join joinType) {
//		newHeaders = cleanHeaders(newHeaders);
//		int numHeaders = newHeaders.length;
//		
//		types = cleanTypes(types);
//		String newTableName = getNewTableName();
//		generateTable(iterator, newHeaders, types, newTableName);
//
//		// add the data
//		if (joinType.equals(Join.FULL_OUTER)) {
//			processAlterData(newTableName, newHeaders, oldHeaders, Join.LEFT_OUTER);
//		} else {
//			processAlterData(newTableName, newHeaders, oldHeaders, joinType);
//		}
//
//		// if we are doing a full outer join (which h2 does not natively have)
//		// we have done the left outer join above
//		// now just add the rows we are missing via a merge query for each row
//		// not efficient but don't see another way to do it
//		// Ex: merge into table (column1, column2) key (column1, column2) values
//		// ('value1', 'value2')
//		// TODO: change this to be a union of right outer and left outer instead
//		// of inserting values
//		if (joinType.equals(Join.FULL_OUTER)) {
//			try {
//				Statement stmt = getConnection().createStatement();
//				String selectQuery = RdbmsQueryBuilder.makeSelect(newTableName, Arrays.asList(newHeaders), false) + makeFilterSubQuery();
//				ResultSet rs = stmt.executeQuery(selectQuery);
//				
//				String mergeQuery = "MERGE INTO " + tableName;
//				String columns = "(";
//				for (int i = 0; i < newHeaders.length; i++) {
//					if (i != 0) {
//						columns += ", ";
//					}
//					columns += newHeaders[i];
//
//				}
//				columns += ")";
//
//				mergeQuery += columns + " KEY " + columns;
//
//				while (rs.next()) {
//					String values = " VALUES(";
//					for (int i = 0; i < numHeaders; i++) {
//						if (i != 0) {
//							values += ", ";
//						}
//						values += " '" + rs.getString(i+1).toString() + "' ";
//					}
//					values += ")";
//					try {
//						runQuery(mergeQuery + values);
//					} catch (Exception e) {
//						e.printStackTrace();
//					}
//				}
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//
//		try {
//			runQuery(RdbmsQueryBuilder.makeDropTable(newTableName));
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//
//		// shift to on disk if number of records is getting large
//		if (isInMem && getNumRecords() > LIMIT_SIZE) {
//			// let the method determine where the new schema will be
//			convertFromInMemToPhysical(null);
//		}
//	}

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
			logger.info("DROPPED H2 TABLE ::: " + tableName);
		} else {
			logger.info("TABLE " + tableName + " DOES NOT EXIST");
		}
	}

	
	/*************************** 
	 * 	END DELETE
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

//	/**
//	 * 
//	 * @param iterator
//	 * @param typesMap
//	 * @param updateColumns
//	 * @throws Exception 
//	 */
//	public void mergeRowsViaIterator(Iterator<IHeadersDataRow> iterator, String[] newHeaders, SemossDataType[] types, String[] startingHeaders, String[] joinColumns) throws Exception {
//		//step 1
//		//generate a table from the iterator
//		String tempTable = getNewTableName();
//		int size = types.length;
//		String[] cleanTypes = new String[size];
//		for(int i = 0; i < size; i++) {
//			cleanTypes[i] = cleanType(types[i].name());
//		}
//		generateTable(iterator, newHeaders, cleanTypes, tempTable);
//
//		//Step 2
//		//inner join the curTable with the tempTable
//		String curTable = getTableName();
//		
//		// for the alter to work,
//		// i want to merge the existing columns in the table
//		// with the new columns we want to get
//		// so we need to remove it as if it doesn't currenlty exist in the frame
//		if(startingHeaders.length != newHeaders.length) {
//			// so we only need to pass in the join columns
//			Set<String> curColsToUse = new HashSet<String>();
//			// add all the join columns
//			for(int i = 0; i < joinColumns.length; i++) {
//				curColsToUse.add(joinColumns[i]);
//			}
//			// add any column not in newHeaders
//			for(int i = 0; i < startingHeaders.length; i++) {
//				if(!ArrayUtilityMethods.arrayContainsValue(newHeaders, startingHeaders[i])) {
//					curColsToUse.add(startingHeaders[i]);
//				}
//			}
//			processAlterData(tempTable, curTable, curColsToUse.toArray(new String[]{}), newHeaders, Join.INNER);
//		}
//		
//		// get all the curTable headers
//		// so the insert into does it in the right order
//		String[] curTableHeaders = getHeaders(curTable);
//		
//		//Step 3
//		//merge the rows of the table with main table
//		StringBuilder mergeQuery = new StringBuilder("MERGE INTO ");
//		mergeQuery.append(curTable).append(" KEY(").append(joinColumns[0]);
//		int keySize = joinColumns.length;
//		for(int i = 1; i < keySize; i++) {
//			mergeQuery.append(", ").append(joinColumns[i]);
//		}
//		mergeQuery.append(") (SELECT ").append(curTableHeaders[0]);
//		for(int i = 1; i < curTableHeaders.length; i++) {
//			mergeQuery.append(", ").append(curTableHeaders[i]);
//		}
//		mergeQuery.append(" FROM ").append(tempTable).append(")");
//		System.out.println(mergeQuery);
//		runQuery(mergeQuery.toString());
//		
//		//Step 4
//		//drop tempTable
//		runQuery(RdbmsQueryBuilder.makeDropTable(tempTable));
//	}
//
//	/**
//	 * 
//	 * @param newTableName				new table to join onto main table
//	 * @param newHeaders				headers in new table
//	 * @param headers					headers in current table
//	 * @param joinType					how to join
//	 */
//	private void processAlterData(String newTableName, String[] newHeaders, String[] headers, Join joinType) {
//		processAlterData(this.tableName, newTableName, newHeaders, headers, joinType);
//	}
//
//	private void processAlterData(String table1, String table2, String[] newHeaders, String[] headers, Join joinType) {
//		// this currently doesnt handle many to many joins and such
//		// try {
//		getConnection();
//
//		// I need to do an evaluation here to find if this one to many
//		String[] oldHeaders = headers;
//
//		// headers for the joining table
//
//		// int curHeadCount = headers.length;
//		Vector<String> newHeaderIndices = new Vector<String>();
//		Vector<String> oldHeaderIndices = new Vector<String>();
//		Hashtable<Integer, Integer> matchers = new Hashtable<Integer, Integer>();
//
//		// I need to find which ones are already there and which ones are new
//		for (int hIndex = 0; hIndex < newHeaders.length; hIndex++) {
//			String uheader = newHeaders[hIndex];
//			uheader = cleanHeader(uheader);
//
//			boolean old = false;
//			for (int oIndex = 0; oIndex < headers.length; oIndex++) {
//				if (headers[oIndex].equalsIgnoreCase(uheader)) {
//					old = true;
//					oldHeaderIndices.add(hIndex + "");
//					matchers.put(hIndex, oIndex);
//					break;
//				}
//			}
//
//			if (!old)
//				newHeaderIndices.add((hIndex) + "");
//		}
//
//		boolean one2Many = true;
//		if (matchers == null || matchers.isEmpty()) {
//			one2Many = false;
//		}
//		// I also need to accomodate when there are no common ones
//
//		// now I need to assimilate everything into one
//		if (one2Many) {
//			mergeTables(table1, table2, matchers, oldHeaders, newHeaders, joinType.getName());
//		}
//	}
//
//	// Obviously I need the table names
//	// I also need the matching properties
//	// I have found that out upfront- I need to also keep what it is called in
//	// the old table
//	// as well as the new table
//	protected void mergeTables(String tableName1, String tableName2, Hashtable<Integer, Integer> matchers, String[] oldHeaders, String[] newHeaders, String join) {
//		getConnection();
//
//		String origTableName = tableName1;
//		// now create a third table
//		String tempTableName = RdbmsFrameUtility.getNewTableName();
//
//		String newCreate = "CREATE Table " + tempTableName + " AS (";
//
//		// now I need to create a join query
//		// first the froms
//
//		// want to create indices on the join columns to speed up the process
//		for (Integer table1JoinIndex : matchers.keySet()) {
//			Integer table2JoinIndex = matchers.get(table1JoinIndex);
//
//			String table1JoinCol = newHeaders[table1JoinIndex];
//			String table2JoinCol = oldHeaders[table2JoinIndex];
//
//			addColumnIndex(tableName1, table1JoinCol);
//			addColumnIndex(tableName2, table2JoinCol);
//			// note that this creates indices on table1 and table2
//			// but these tables are later dropped so no indices are kept
//			// through the flow
//		}
//
//		String froms = " FROM " + tableName1 + " AS  A ";
//		String joins = " " + join + " " + tableName2 + " AS B ON (";
//
//		Enumeration<Integer> keys = matchers.keys();
//		for (int jIndex = 0; jIndex < matchers.size(); jIndex++) {
//			Integer newIndex = keys.nextElement();
//			Integer oldIndex = matchers.get(newIndex);
//
//			String oldCol = oldHeaders[oldIndex];
//			String newCol = newHeaders[newIndex];
//
//			// need to make sure the data types are good to go
//			String oldColType = getDataType(tableName1, oldCol);
//			String newColType = getDataType(tableName2, newCol);
//
//			// syntax modification for each addition join column
//			if (jIndex != 0) {
//				joins = joins + " AND ";
//			}
//
//			if(oldColType.equals(newColType)) {
//				// data types are the same, no need to do anything
//				joins = joins + "A." + oldCol + " = " + "B." + newCol;
//			} else {
//				// data types are different... 
//				// if both are different numbers -> convert both to double
//				// else -> convert to strings
//
//				if( (oldColType.equals("DOUBLE") || oldColType.equals("INT") )
//						&& (newColType.equals("DOUBLE") || newColType.equals("INT") ) ) {
//					// both are numbers
//					if(!oldColType.equals("DOUBLE")) {
//						joins = joins + " A." + oldCol;
//					} else {
//						joins = joins + " CAST(A." + oldCol + " AS DOUBLE)";
//					}
//					joins = joins + " = ";
//					if(!newColType.equals("DOUBLE")) {
//						joins = joins + " B." + newCol;
//					} else {
//						joins = joins + " CAST(B." + newCol + " AS DOUBLE)";
//					}
//				}
//				// case when old col type is double and new col type is string
//				else if( (oldColType.equals("DOUBLE") || oldColType.equals("INT") )
//						&& newColType.equals("VARCHAR") ) 
//				{
//					// if it is not a double, convert it
//					if(!oldColType.equals("DOUBLE")) {
//						joins = joins + " CAST(A." + oldCol + " AS DOUBLE)";
//					} else {
//						joins = joins + " A." + oldCol;
//					}
//					joins = joins + " = ";
//
//					// new col is a string
//					// so cast to double
//					joins = joins + " CAST(B." + newCol + " AS DOUBLE)";
//				}
//				// case when old col type is string and new col type is double
//				else if(  oldColType.equals("VARCHAR") && 
//						(newColType.equals("DOUBLE") || newColType.equals("INT") ) ) 
//				{
//					// old col is a string
//					// so cast to double
//					joins = joins + " CAST(A." + oldCol + " AS DOUBLE)";
//					joins = joins + " = ";
//					// if it is not a double, convert it
//					if(!newColType.equals("DOUBLE")) {
//						joins = joins + " B." + newCol;
//					} else {
//						joins = joins + " CAST(B." + newCol + " AS DOUBLE)";
//					}
//				}
//				else {
//					// not sure... just make everything a string
//					if(oldColType.equals("VARCHAR")) {
//						joins = joins + " A." + oldCol;
//					} else {
//						joins = joins + " CAST( A." + oldCol + " AS VARCHAR(800))";
//					}
//					joins = joins + " = ";
//					if(newColType.equals("VARCHAR")) {
//						joins = joins + " B." + newCol;
//					} else {
//						joins = joins + " CAST(B." + newCol + " AS VARCHAR(800))";
//					}
//				}
//			}
//		}
//
//		joins = joins + " )";
//
//		// first table A
//		String selectors = "";
//		for (int oldIndex = 0; oldIndex < oldHeaders.length; oldIndex++) {
//			if (oldIndex == 0)
//				selectors = "A." + oldHeaders[oldIndex];
//			else
//				selectors = selectors + " , " + "A." + oldHeaders[oldIndex];
//		}
//
//		// next table 2
//		for (int newIndex = 0; newIndex < newHeaders.length; newIndex++) {
//			if (!matchers.containsKey(newIndex))
//				selectors = selectors + " , " + "B." + newHeaders[newIndex];
//		}
//
//		String finalQuery = newCreate + "SELECT " + selectors + " " + froms + "  " + joins + " )";
//
//		System.out.println(finalQuery);
//
//		try {
//			long start = System.currentTimeMillis();
//			runQuery(finalQuery);
//			long end = System.currentTimeMillis();
//			System.out.println("TIME FOR JOINING TABLES = " + (end - start) + " ms");
//
//			// Statement stmt = conn.createStatement();
//			// stmt.execute(finalQuery);
//
//			runQuery(RdbmsQueryBuilder.makeDropTable(tableName1));
//
//			// DONT DROP THIS due to need to preserve for outer joins, method
//			// outside will handle dropping new table
//			// runQuery(makeDropTable(tableName2));
//
//			// rename back to the original table
//			runQuery("ALTER TABLE " + tempTableName + " RENAME TO " + origTableName);
//
//			// this created a new table
//			// need to clear the index map
//			clearColumnIndexMap();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}

	protected void addColumnIndex(String tableName, String colName) {
		if (!columnIndexMap.containsKey(tableName + "+++" + colName)) {
			long start = System.currentTimeMillis();

			String indexSql = null;
			logger.info("CREATING INDEX ON TABLE = " + tableName + " ON COLUMN = " + colName);
			try {
				String indexName = colName + "_INDEX_" + getNextNumber();
				indexSql = "CREATE INDEX " + indexName + " ON " + tableName + "(" + colName + ")";
				runQuery(indexSql);
				columnIndexMap.put(tableName + "+++" + colName, indexName);
				
				long end = System.currentTimeMillis();

				logger.info("TIME FOR INDEX CREATION = " + (end - start) + " ms");
			} catch (Exception e) {
				logger.info("ERROR WITH INDEX !!! " + indexSql);
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

			StringBuilder indexSqlBuilder = new StringBuilder();;
			logger.info("CREATING INDEX ON TABLE = " + tableName + " ON COLUMNS = " + multiColIndexNameBuilder);
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

				logger.info("TIME FOR INDEX CREATION = " + (end - start) + " ms");
			} catch (Exception e) {
				logger.info("ERROR WITH INDEX !!! " + multiColIndexName);
				e.printStackTrace();
			}
		}
	}

	protected void removeColumnIndex(String tableName, String colName) {
		if (columnIndexMap.containsKey(tableName + "+++" + colName)) {
			logger.info("DROPPING INDEX ON TABLE = " + tableName + " ON COLUMN = " + colName);
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
	
//	protected String getDataType(String tableName, String colName) {
//		String query = "select type_name from information_schema.columns where table_name='" + 
//					tableName.toUpperCase() + "' and column_name='" + colName.toUpperCase() + "'";
//		Statement stmt = null;
//		ResultSet rs = null;
//		try {
//			stmt = getConnection().createStatement();
//			rs = stmt.executeQuery(query);
//			if(rs.next()) {
//				return rs.getString(1);
//			}
//		} catch (SQLException e) {
//			e.printStackTrace();
//		} finally {
//			if(rs != null) {
//				try {
//					rs.close();
//				} catch (SQLException e) {
//					e.printStackTrace();
//				}
//			}
//			if(stmt != null) {
//				try {
//					stmt.close();
//				} catch (SQLException e) {
//					e.printStackTrace();
//				}
//			}
//		}
//		return null;
//	}

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
				logger.info("CONVERTING FROM IN-MEMORY H2-DATABASE TO ON-DISK H2-DATABASE!");
				
				// if was in mem but want to push to specific existing location
				if (physicalDbLocation != null && !physicalDbLocation.isEmpty()) {
					dbLocation = new File(physicalDbLocation);
				}
			} else {
				logger.info("CHANGEING SCHEMA FOR EXISTING ON-DISK H2-DATABASE!");
				
				if (physicalDbLocation == null || physicalDbLocation.isEmpty()) {
					logger.info("SCHEMA IS ALREADY ON DISK AND DID NOT PROVIDE NEW SCHEMA TO CHAGNE TO!");
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

//	private String[] cleanHeaders(String[] headers) {
//		String[] cleanHeaders = new String[headers.length];
//		for (int i = 0; i < headers.length; i++) {
//			cleanHeaders[i] = cleanHeader(headers[i]);
//		}
//		return cleanHeaders;
//	}
//
//
//	// TODO: this is done outside now, need to remove
//	protected static String cleanHeader(String header) {
//		/*
//		 * header = header.replaceAll(" ", "_"); header = header.replace("(",
//		 * "_"); header = header.replace(")", "_"); header = header.replace("-",
//		 * "_"); header = header.replace("'", "");
//		 */
//		header = header.replaceAll("[#%!&()@#$'./-]*\"*", ""); // replace all
//																// the useless
//																// shit in one
//																// go
//		header = header.replaceAll("\\s+", "_");
//		header = header.replaceAll(",", "_");
//		if (Character.isDigit(header.charAt(0)))
//			header = "c_" + header;
//		return header;
//	}

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
		logger.info("Running query : " + query);
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
	protected void save(String fileName, String frameName) {
		String saveScript = "SCRIPT TO '" + fileName + "' COMPRESSION GZIP TABLE " + frameName;
		try {
			runQuery(saveScript);
			if (new File(fileName).length() == 0){
				throw new IllegalArgumentException("Attempting to save an empty H2 frame");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Runs the script for a cached Insight
	 * 
	 * @param fileName
	 *            The file containing the script to create the frame
	 */
	protected void open(String fileName) {
		// get the open sql script
		String openScript = "RUNSCRIPT FROM '" + fileName + "' COMPRESSION GZIP ";
		try {
			// load the frame from file
			runQuery(openScript);
		} catch (Exception e) {
			e.printStackTrace();
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

	
	
	
	
	
	
	
	
	
//	/**
//	 * Generates a new H2 table from the paramater data
//	 * 
//	 * Assumptions headers and types are of same length types are H2 readable
//	 * 
//	 * 
//	 * @param iterator
//	 *            - iterates over the data
//	 * @param headers
//	 *            - headers for the table data
//	 * @param types
//	 *            - data type for each column
//	 * @param tableName
//	 * @throws Exception 
//	 */
//	private void generateTable(Iterator<IHeadersDataRow> iterator, String[] headers, String[] types, String tableName) {
//		String createTable = RdbmsQueryBuilder.makeCreate(tableName, headers, types);
//		logger.info(" >>> CREATING TABLE : " + createTable);
//		try {
//			runQuery(createTable);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		
//		Map<String, SemossDataType> typesMap = new HashMap<String, SemossDataType>();
//		for(int i = 0; i < headers.length; i++) {
//			typesMap.put(headers[i], SemossDataType.convertStringToDataType(types[i]));
//		}
//		addRowsViaIterator(iterator, tableName, typesMap);
//	}
//
//	/**
//	 * 
//	 * @param column
//	 *            - the column to join on
//	 * @param newColumnName
//	 *            - new column name with group by values
//	 * @param valueColumn
//	 *            - the column to do calculations on
//	 * @param mathType
//	 *            - the type of group by
//	 * @param headers
//	 */
//	public void processGroupBy(String column, String newColumnName, String valueColumn, String mathType, String[] headers) {
//
//		// String[] tableHeaders = getHeaders(tableName);
//		// String inserter = makeGroupBy(column, valueColumn, mathType,
//		// newColumnName, this.tableName, headers);
//		// processAlterAsNewTable(inserter, Join.LEFT_OUTER.getName(),
//		// tableHeaders);
//
//		String reservedColumn = getExtraColumn();
//		if (reservedColumn == null) {
//			addEmptyDerivedColumns();
//			reservedColumn = getExtraColumn();
//		}
//		renameColumn(reservedColumn, newColumnName);
//
//		// String[] tableHeaders = getHeaders(tableName);
//		String inserter = makeGroupBy(column, valueColumn, mathType, newColumnName, this.tableName, headers);
//		Statement stmt = null;
//		ResultSet rs = null;
//		try {
//			stmt = getConnection().createStatement();
//			rs = stmt.executeQuery(inserter);
//			while (rs.next()) {
//				Object[] values = { rs.getObject(column), rs.getObject(newColumnName) };
//				String[] columnHeaders = { column, newColumnName };
//				updateTable(headers, values, columnHeaders);
//				// String updateQuery = makeUpdate(tableName, column,
//				// newColumnName, groupBySet.getObject(column),
//				// groupBySet.getObject(newColumnName));
//				// runQuery(updateQuery);
//			}
//		} catch (SQLException e) {
//			e.printStackTrace();
//		} finally {
//			if(rs != null) {
//				try {
//					rs.close();
//				} catch (SQLException e) {
//					e.printStackTrace();
//				}
//			}
//			if(stmt != null) {
//				try {
//					stmt.close();
//				} catch (SQLException e) {
//					e.printStackTrace();
//				}
//			}
//		}
//		// makeUpdate(mathType, joinColumn, newColumn, joinValue, newValue)
//		// processAlterAsNewTable(inserter, Join.LEFT_OUTER.getName(),
//		// tableHeaders);
//
//		// all group bys are doubles?
//		// addHeader(newColumnName, "double", tableHeaders);
//		// addType("double");
//	}
//	
//	
//	// process a group by - calculate then make a table then merge the table
//	public void processGroupBy(String[] column, String newColumnName, String valueColumn, String mathType,
//			String[] headers) {
//		String tableName = getTableName();
//		if (column.length == 1) {
//			processGroupBy(column[0], newColumnName, valueColumn, mathType, headers);
//			return;
//		}
//		// String[] tableHeaders = getHeaders(tableName);
//		// String inserter = makeGroupBy(column, valueColumn, mathType,
//		// newColumnName, this.tableName, headers);
//		// processAlterAsNewTable(inserter, Join.LEFT_OUTER.getName(),
//		// tableHeaders);
//
//		// all group bys are doubles?
//		String reservedColumn = getExtraColumn();
//		if (reservedColumn == null) {
//			addEmptyDerivedColumns();
//			reservedColumn = getExtraColumn();
//		}
//		renameColumn(reservedColumn, newColumnName);
//
//		// String[] tableHeaders = getHeaders(tableName);
//		String inserter = makeGroupBy(column, valueColumn, mathType, newColumnName, tableName, headers);
//		Statement stmt = null;
//		ResultSet rs = null;
//		try {
//			stmt = getConnection().createStatement();
//			rs = stmt.executeQuery(inserter);
//			while (rs.next()) {
//				List<Object> values = new ArrayList<>();
//				List<Object> columnHeaders = new ArrayList<>();
//				for (String c : column) {
//					values.add(rs.getObject(c));// {groupBySet.getObject(column),
//														// groupBySet.getObject(newColumnName)};
//					columnHeaders.add(c);
//				}
//				values.add(rs.getObject(newColumnName));
//				columnHeaders.add(newColumnName);
//				updateTable(headers, values.toArray(), columnHeaders.toArray(new String[] {}));
//
//				// String updateQuery = makeUpdate(tableName, column,
//				// newColumnName, groupBySet.getObject(column),
//				// groupBySet.getObject(newColumnName));
//				// runQuery(updateQuery);
//			}
//		} catch (SQLException e) {
//			e.printStackTrace();
//		} finally {
//			if(rs != null) {
//				try {
//					rs.close();
//				} catch (SQLException e) {
//					e.printStackTrace();
//				}
//			}
//			if(stmt != null) {
//				try {
//					stmt.close();
//				} catch (SQLException e) {
//					e.printStackTrace();
//				}
//			}
//		}
//	}
//	
//	private String makeGroupBy(String column, String valueColumn, String mathType, String alias, String tableName, String[] headers) {
//
//		column = cleanHeader(column);
//		valueColumn = cleanHeader(valueColumn);
//		alias = cleanHeader(alias);
//
//		String functionString = "";
//
//		String type = getType(tableName, column);
//
//		switch (mathType.toUpperCase()) {
//		case "COUNT": {
//			String func = "COUNT(";
//			if (type.toUpperCase().startsWith("VARCHAR"))
//				func = "COUNT( DISTINCT ";
//			functionString = func + valueColumn + ")";
//			break;
//		}
//		case "AVERAGE": {
//			functionString = "AVG(" + valueColumn + ")";
//			break;
//		}
//		case "MIN": {
//			functionString = "MIN(" + valueColumn + ")";
//			break;
//		}
//		case "MAX": {
//			functionString = "MAX(" + valueColumn + ")";
//			break;
//		}
//		case "SUM": {
//			functionString = "SUM(" + valueColumn + ")";
//			break;
//		}
//		default: {
//			String func = "COUNT(";
//			if (type.toUpperCase().startsWith("VARCHAR"))
//				func = "COUNT( DISTINCT ";
//			functionString = func + valueColumn + ")";
//			break;
//		}
//		}
//
//		// String filterSubQuery = makeFilterSubQuery(this.filterHash,
//		// this.filterComparator);
//		String filterSubQuery = makeFilterSubQuery();
//		String groupByStatement = "SELECT " + column + ", " + functionString + " AS " + alias + " FROM " + tableName
//				+ filterSubQuery + " GROUP BY " + column;
//
//		return groupByStatement;
//	}
//
//	// TODO : don't assume a double group by here
//	private String makeGroupBy(String[] column, String valueColumn, String mathType, String alias, String tableName, String[] headers) {
//		if (column.length == 1)
//			return makeGroupBy(column[0], valueColumn, mathType, alias, tableName, headers);
//		String column1 = cleanHeader(column[0]);
//		String column2 = cleanHeader(column[1]);
//		valueColumn = cleanHeader(valueColumn);
//		alias = cleanHeader(alias);
//
//		String functionString = "";
//
//		String type = getType(tableName, valueColumn);
//
//		switch (mathType.toUpperCase()) {
//		case "COUNT": {
//			String func = "COUNT(";
//			if (type.toUpperCase().startsWith("VARCHAR"))
//				func = "COUNT( DISTINCT ";
//			functionString = func + valueColumn + ")";
//			break;
//		}
//		case "AVERAGE": {
//			functionString = "AVG(" + valueColumn + ")";
//			break;
//		}
//		case "MIN": {
//			functionString = "MIN(" + valueColumn + ")";
//			break;
//		}
//		case "MAX": {
//			functionString = "MAX(" + valueColumn + ")";
//			break;
//		}
//		case "SUM": {
//			functionString = "SUM(" + valueColumn + ")";
//			break;
//		}
//		default: {
//			String func = "COUNT(";
//			if (type.toUpperCase().startsWith("VARCHAR"))
//				func = "COUNT( DISTINCT ";
//			functionString = func + valueColumn + ")";
//			break;
//		}
//		}
//
//		// String filterSubQuery = makeFilterSubQuery(this.filterHash,
//		// this.filterComparator);
//		String filterSubQuery = makeFilterSubQuery();
//		String groupByStatement = "SELECT " + column1 + ", " + column2 + ", " + functionString + " AS " + alias
//				+ " FROM " + tableName + filterSubQuery + " GROUP BY " + column1 + ", " + column2;
//
//		return groupByStatement;
//	}
//	
//	private String getType(String tableName, String column) {
//		String type = null;
//		String typeQuery = "SELECT TABLE_NAME, COLUMN_NAME, TYPE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '"
//				+ tableName.toUpperCase() + "' AND COLUMN_NAME = '" + column.toUpperCase() + "'";
//		Statement stmt = null;
//		ResultSet rs = null;
//		try {
//			stmt = getConnection().createStatement();
//			rs = stmt.executeQuery(typeQuery);
//			if (rs.next()) {
//				type = rs.getString("TYPE_NAME");
//			}
//		} catch (SQLException e) {
//			e.printStackTrace();
//		} finally {
//			if(rs != null) {
//				try {
//					rs.close();
//				} catch (SQLException e) {
//					e.printStackTrace();
//				}
//			}
//			if(stmt != null) {
//				try {
//					stmt.close();
//				} catch (SQLException e) {
//					e.printStackTrace();
//				}
//			}
//		}
//		return type;
//	}
	
	
	
	
	
	
	
	
	
	
	
	
	/*************************** TEST **********************************************/
	public static void main(String[] a) throws Exception {

		// concurrencyTest();
		// H2Builder test = new H2Builder();
		// test.castToType("6/2/2015");
		// String fileName = "C:/Users/rluthar/Desktop/datasets/Movie.csv";
		// long before, after;
		// fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/Remedy
		// New.csv";
		// //fileName =
		// "C:/Users/pkapaleeswaran/workspacej3/datasets/Movie.csv";
		//
		// /******* Used Primarily for Streaming *******/
		// /**
		// long before = System.nanoTime();
		// test.processFile(fileName);
		// long after = System.nanoTime();
		// System.out.println("Time Taken.. the usual way.. " + (after - before)
		// / 1000000);
		// **/
		//
		// before = System.nanoTime();
		// test.processCreateData(fileName);
		// after = System.nanoTime();
		// System.out.println("Time Taken.. Univocity.. " + (after - before) /
		// 1000000);
		//
		// fileName = "C:/Users/rluthar/Desktop/datasets/Actor.csv";
		// //before = System.nanoTime();
		// test.processAlterData(fileName);
		// after = System.nanoTime();
		// System.out.println("Time Taken.. Univocity.. " + (after - before) /
		// 1000000);
		//
		//
		//
		// /*
		// * These 2 are invalid.. it requires significant cleanup
		// before = System.nanoTime();
		// test.loadCSV(fileName);
		// after = System.nanoTime();
		// System.out.println("Time Taken.. H2.. " + (after - before) /
		// 1000000);
		//
		// before = System.nanoTime();
		// test.processCreateDataH2(fileName);
		// after = System.nanoTime();
		// System.out.println("Time Taken.. H2.. Types " + (after - before) /
		// 1000000);
		// */
		// //
		// test.predictTypes("");
		// //String [] headers = {"A", "b", "d", "e", "f", "g", "h"};
		// //test.predictRowTypes("1.0, 2, \"$123,33.22\", wwewewe, \"Hello, I
		// am doing good\", hola, 9/12/2012");
	}

//	private static void concurrencyTest() throws SQLException {
//		Connection conn = DriverManager.getConnection("jdbc:h2:mem:test:LOG=0;CACHE_SIZE=65536;LOCK_MODE=1;UNDO_LOG=0",
//				"sa", "");
//
//		int tableLength = 1000000;
//		// create a table
//		conn.createStatement()
//				.execute("CREATE TABLE TEST (COLUMN1 VARCHAR(800), COLUMN2 VARCHAR(800), COLUMN3 VARCHAR(800))");
//
//		// put in a LOT of data
//		for (int i = 1; i <= tableLength; i++) {
//			conn.createStatement().execute("INSERT INTO TEST (COLUMN1, COLUMN2, COLUMN3) VALUES ('" + tableLength
//					+ "', '" + tableLength + "col2" + "', '" + tableLength + "col3')");
//		}
//
//		// alter the table (should take some time)
//		conn.createStatement().execute("ALTER TABLE TEST ADD COLUMN4 VARCHAR(800)");
//
//		// see if we try and update before alter finishes
//		conn.createStatement()
//				.execute("UPDATE TEST SET COLUMN4 = 'THIS IS COLUMN 4' WHERE COLUMN1 = '" + tableLength + "'");
//
//		System.out.println("Finished");
//	}
//
//	// Test method
//	public void testDB() throws Exception {
//		Class.forName("org.h2.Driver");
//		Connection conn = DriverManager.getConnection("jdbc:h2:C:/Users/pkapaleeswaran/workspacej3/Exp/database", "sa",
//				"");
//		Statement stmt = conn.createStatement();
//		String query = "select t.title, s.studio from title t, studio s where t.title = s.title_fk";
//		query = "select t.title, concat(t.title,':', s.studio), s.studio from title t, studio s where t.title = s.title_fk";
//		// ResultSet rs = stmt.executeQuery("SELECT * FROM TITLE");
//		ResultSet rs = stmt.executeQuery(query);
//		// stmt.execute("CREATE TABLE MOVIES AS SELECT * From
//		// CSVREAD('../Movie.csv')");
//
//		ResultSetMetaData rsmd = rs.getMetaData();
//		int columnCount = rsmd.getColumnCount();
//
//		ArrayList records = new ArrayList();
//		System.err.println("Number of columns " + columnCount);
//
//		while (rs.next()) {
//			Object[] data = new Object[columnCount];
//			for (int colIndex = 0; colIndex < columnCount; colIndex++) {
//				data[colIndex] = rs.getObject(colIndex + 1);
//				System.out.print(rsmd.getColumnType(colIndex + 1));
//				System.out.print(rsmd.getColumnName(colIndex + 1) + ":");
//				System.out.print(rs.getString(colIndex + 1));
//				System.out.print(">>>>" + rsmd.getTableName(colIndex + 1) + "<<<<");
//			}
//			System.out.println();
//		}
//
//		// add application code here
//		conn.close();
//	}
//
//	// Test method
//	public void predictTypes(String csv) {
//		ST values = new ST("(Hello world " + "'<Hello_x_(Y)>' <y;null = \"'0'\">" + ")");
//		values.add("(Hello_x(Y)", "Try");
//		System.out.println(values.getAttributes());
//		System.out.println(">> " + values.render());
//
//		System.out.println("Return is ..  " + castToType("$12.3344"));
//
//		String templateString = "Yo baby ${x}";
//		Map valuesMap = new HashMap();
//
//		valuesMap.put("x", "yo");
//
//		StrSubstitutor ss = new StrSubstitutor(valuesMap);
//		System.out.println(ss.replace(templateString));
//
//	}
//
//	public void loadCSV(String fileName) {
//		try {
//			// String fileName =
//			// "C:/Users/pkapaleeswaran/workspacej3/datasets/consumer_complaints.csv";
//
//			// fileName =
//			// "C:/Users/pkapaleeswaran/workspacej3/datasets/pregnancyS.csv";
//
//			Class.forName("org.h2.Driver");
//			Connection conn = DriverManager.getConnection("jdbc:h2:mem:test", "sa", "");
//			Statement stmt = conn.createStatement();
//
//			long now = System.nanoTime();
//			/*
//			 * stmt.execute("Create table test(" + "Complaint_ID varchar(255), "
//			 * + "product varchar(255)," + "sub_product varchar(255)," +
//			 * "issue varchar(255)," + "sub_issue varchar(255)," +
//			 * "state varchar(10)," + "zipcode int," +
//			 * "submitted_via varchar(20)," + "date_received date," +
//			 * "date_sent date," + "company varchar(255)," +
//			 * "company_response varchar(255)," +
//			 * "timely_response varchar(255)," +
//			 * "consumer_disputed varchar(5))  as select * from csvread('" +
//			 * fileName + "')");
//			 */
//
//			System.out.println("File Name ...  " + fileName);
//			stmt.execute("Create table test as select * from csvread('" + fileName + "')");
//			long graphTime = System.nanoTime();
//			System.out.println("Time taken.. " + ((graphTime - now) / 1000000) + " milli secs");
//
//			String query = "Select  bencat, count(*) from test where sex='F' Group By Bencat";
//
//			ResultSet rs = stmt.executeQuery(query);
//
//			while (rs.next()) {
//				System.out.print("Bencat.. " + rs.getObject(1));
//				System.out.println("Count.. " + rs.getObject(2));
//			}
//
//			graphTime = System.nanoTime();
//
//			stmt.execute("Alter table test add dummy varchar2(200)");
//
//			long rightNow = System.nanoTime();
//			System.out.println("Update Time taken.. " + ((rightNow - graphTime) / 1000000) + "milli secs");
//
//			graphTime = System.nanoTime();
//
//			// stmt.execute("Update test set dummy='try' where bencat = 'ADFMLY'
//			// ");
//			stmt.execute("Update test set dummy='try' where bencat = 'ADN' ");
//			rightNow = System.nanoTime();
//
//			// stmt.execute("Delete from Test where State = 'TX'");
//			System.out.println("Update Time taken.. " + ((rightNow - graphTime) / 1000000) + "milli secs");
//			graphTime = rightNow;
//
//			rightNow = System.nanoTime();
//
//			// rs = stmt.executeQuery("Select count(State) from test where
//			// zipcode > 22000");
//			/*
//			 * rs = stmt.executeQuery(
//			 * "Select  sum(zipcode) from test where zipcode > 22000");
//			 * 
//			 * while(rs.next()) { System.out.println("Count.. " +
//			 * rs.getObject(1)); } System.out.println("Query Time taken.. " +
//			 * ((rightNow - graphTime) / 1000000000) + " secs");
//			 */
//
//		} catch (ClassNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//
//	}
//
//	private void testData() throws SQLException {
//
//		ResultSet rs = conn.createStatement().executeQuery("Select count(*) from " + tableName);
//		while (rs.next())
//			System.out.println("Inserted..  " + rs.getInt(1));
//
//		/*
//		 * String query =
//		 * "Select  bencat, count(*) from H2FRAME where sex='F' Group By Bencat"
//		 * ;
//		 * 
//		 * rs = conn.createStatement().executeQuery(query);
//		 * 
//		 * 
//		 * while(rs.next()) { System.out.print("Bencat.. " + rs.getObject(1));
//		 * System.out.println("Count.. " + rs.getObject(2)); }
//		 */
//	}
	
	
	
}
