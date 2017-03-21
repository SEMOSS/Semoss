package prerna.ds.h22;

import java.io.File;
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
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;

import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.h2.tools.Server;
import org.stringtemplate.v4.ST;

import com.google.gson.Gson;

import prerna.algorithm.api.IMetaData;
import prerna.cache.ICache;
import prerna.ds.AbstractTableDataFrame;
import prerna.ds.RdbmsTableMetaData;
import prerna.ds.h2.H2Iterator;
import prerna.ds.util.RdbmsFrameUtility;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IHeadersDataRow;
import prerna.poi.main.RDBMSEngineCreationHelper;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class H2Builder2 {

	private static final Logger LOGGER = LogManager.getLogger(H2Builder2.class.getName());

	static int rowCount = 0;
	private static final String tempTable = "TEMP_TABLE98793";
	static final String H2FRAME = "H2FRAME";
	String brokenLines = "";
	String options = ":LOG=0;CACHE_SIZE=65536;LOCK_MODE=1;UNDO_LOG=0";
	Server server = null;
	String serverURL = null;
	Hashtable<String, String[]> tablePermissions = new Hashtable<String, String[]>();


	// for writing the frame on disk
	// currently not used
	// would require reconsideration of dashboard, etc.
	// since frames are not in same schema
	private final int LIMIT_SIZE;

	// Provides a translation for incoming types into something H2 can
	// understand
	private static Map<String, String> typeConversionMap = new HashMap<String, String>();
	static {
		typeConversionMap.put("NUMBER", "DOUBLE");
		typeConversionMap.put("STRING", "VARCHAR(800)");
		typeConversionMap.put("DATE", "DATE");
		// TODO: for now, also assume timestamp as date
		typeConversionMap.put("TIMESTAMP", "DATE");
		typeConversionMap.put("FLOAT", "DOUBLE");
		typeConversionMap.put("LONG", "DOUBLE");
	}

	RdbmsTableMetaData tableMetaData;
//	private String tableName;
	// name of the main table for H2
//	String tableName;

	// use to determine if we are joined to another viz or not
	// all Reads will be conducted on the view when this is true, all Creates,
	// Updates, and Deletes will affect only the mainTable
//	private boolean joinMode;
//	private String viewTableName;
//	Hashtable<String, String> joinColumnTranslation = new Hashtable<>();
//
//	H2Joiner joiner;

	// specifies the join types for an H2 frame
	public enum Join {
		INNER("INNER JOIN"), LEFT_OUTER("LEFT OUTER JOIN"), RIGHT_OUTER("RIGHT OUTER JOIN"), FULL_OUTER(
				"FULL OUTER JOIN"), CROSS("CROSS JOIN");
		String name;

		Join(String n) {
			name = n;
		}

		public String getName() {
			return name;
		}
	}

	/*************************** TEST **********************************************/


	/***************************
	 * END TEST
	 ******************************************/

	/*************************** CONSTRUCTORS **************************************/

	protected H2Builder2() {
		// //initialize a connection
		// getConnection();
		String limitSize = (String) DIHelper.getInstance().getProperty(Constants.H2_IN_MEM_SIZE);
		if (limitSize == null) {
			this.LIMIT_SIZE = 10_000;
		} else {
			this.LIMIT_SIZE = Integer.parseInt(limitSize.trim());
		}
	}

	/***************************
	 * END CONSTRUCTORS
	 **********************************/



	
	/*************************** 
	 * 	CREATE 
	 * *************************/

	/**
	 * Generates a new H2 table from the paramater data
	 * 
	 * Assumptions headers and types are of same length types are H2 readable
	 * 
	 * 
	 * @param iterator
	 *            - iterates over the data
	 * @param headers
	 *            - headers for the table data
	 * @param types
	 *            - data type for each column
	 * @param tableName
	 */
	private void generateTable(Iterator<IHeadersDataRow> iterator, String[] headers, String[] types, String tableName) {
		try {
			String createTable = RdbmsQueryBuilder.makeCreate(tableName, headers, types);
			runQuery(createTable);

			PreparedStatement ps = createInsertPreparedStatement(tableName, headers);
			// keep a batch size so we dont get heapspace
			final int batchSize = 5000;
			int count = 0;

			// we loop through every row of the csv
			while (iterator.hasNext()) {
				IHeadersDataRow headerRow = iterator.next();
				Object[] nextRow = headerRow.getValues();
				// we need to loop through every value and cast appropriately
				for (int colIndex = 0; colIndex < nextRow.length; colIndex++) {
					String type = types[colIndex];
					if (type.equalsIgnoreCase("DATE")) {
						java.util.Date value = Utility.getDateAsDateObj(nextRow[colIndex] + "");
						if (value != null) {
							ps.setDate(colIndex + 1, new java.sql.Date(value.getTime()));
						}
					} else if (type.equalsIgnoreCase("DOUBLE") || type.equalsIgnoreCase("FLOAT")) {
						Double value = Utility.getDouble(nextRow[colIndex] + "");
						if (value != null) {
							ps.setDouble(colIndex + 1, value);
						}
					} else {
						String value = nextRow[colIndex] + "";
						ps.setString(colIndex + 1, value);
					}
				}
				// add it
				ps.addBatch();

				// batch commit based on size
				if (++count % batchSize == 0) {
					ps.executeBatch();
				}
			}

			// well, we are done looping through now
			ps.executeBatch(); // insert any remaining records
			ps.close();

			// while(iterator.hasNext()) {
			// IHeadersDataRow nextData = iterator.next();
			// Object[] row = nextData.getValues();
			// String[] stringRow = new String[row.length];
			// for(int i = 0; i < row.length; i++) {
			// stringRow[i] = row[i].toString();
			// }
			//
			// String[] cells = Utility.castToTypes(stringRow, types);
			// String inserter = makeInsert(headers, types, cells, new
			// Hashtable<String, String>(), tableName);
			// runQuery(inserter);
			// }

		} catch (Exception e) {

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
		// generate the sql for the prepared statement
		StringBuilder sql = new StringBuilder("INSERT INTO ");
		sql.append(TABLE_NAME).append(" (").append(columns[0]);
		for (int colIndex = 1; colIndex < columns.length; colIndex++) {
			sql.append(", ");
			sql.append(columns[colIndex]);
		}
		sql.append(") VALUES (?"); // remember, we already assumed one col
		for (int colIndex = 1; colIndex < columns.length; colIndex++) {
			sql.append(", ?");
		}
		sql.append(")");

		PreparedStatement ps = null;
		try {
			// create the prepared statement using the sql query defined
			ps = getConnection().prepareStatement(sql.toString());
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return ps;
	}

	public PreparedStatement createUpdatePreparedStatement(final String TABLE_NAME, final String[] columnsToUpdate, final String[] whereColumns) {
		// generate the sql for the prepared statement
		StringBuilder sql = new StringBuilder("UPDATE ");
		sql.append(TABLE_NAME).append(" SET ").append(columnsToUpdate[0]).append(" = ?");
		for (int colIndex = 1; colIndex < columnsToUpdate.length; colIndex++) {
			sql.append(", ");
			sql.append(columnsToUpdate[colIndex]).append(" = ?");
		}
		sql.append(" WHERE ").append(whereColumns[0]).append(" = ?");
		for (int colIndex = 1; colIndex < whereColumns.length; colIndex++) {
			sql.append(" AND ");
			sql.append(whereColumns[colIndex]).append(" = ?");
		}
		sql.append("");

		PreparedStatement ps = null;
		try {
			// create the prepared statement using the sql query defined
			ps = getConnection().prepareStatement(sql.toString());
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return ps;
	}

	/*************************** 
	 * 	END CREATE 
	 * *************************/

	
	
	
	/*************************** 
	 * 	READ
	 * *************************/

	// get scaled version of above method
	public List<Object[]> getScaledData(List<String> selectors, Map<String, String> headerTypeMap, String column, Object value, Double[] maxArr, Double[] minArr) {

		String tableName = this.tableMetaData.getViewTableName();
		selectors = this.tableMetaData.cleanColumns(selectors);
		column = this.tableMetaData.cleanColumn(column);

		Map<String, String> newHeaderTypeMap = new HashMap<>();
		for (String selector : headerTypeMap.keySet()) {
			newHeaderTypeMap.put(this.tableMetaData.cleanColumn(column), headerTypeMap.get(selector));
		}
		headerTypeMap = newHeaderTypeMap;

		int cindex = selectors.indexOf(column);

		List<Object[]> data;
		String[] types = new String[headerTypeMap.size()];

		int index = 0;
		for (String selector : selectors) {
			types[index] = headerTypeMap.get(selector);
			index++;
		}

		types = cleanTypes(types);

		try {
			String selectQuery = makeSpecificSelect(tableName, selectors, column, value);
			ResultSet rs = executeQuery(selectQuery);

			if (rs != null) {
				ResultSetMetaData rsmd = rs.getMetaData();
				int NumOfCol = rsmd.getColumnCount();
				data = new ArrayList<>(NumOfCol);
				while (rs.next()) {
					Object[] row = new Object[NumOfCol];

					for (int i = 1; i <= NumOfCol; i++) {
						Object val = rs.getObject(i);
						// if null, will stay null 
						if(val == null) {
							continue;
						}
						if (cindex != (i - 1)
								&& (types[i - 1].equalsIgnoreCase("int") || types[i - 1].equalsIgnoreCase("double"))) {
							row[i - 1] = (((Number) val).doubleValue() - minArr[i - 1])
									/ (maxArr[i - 1] - minArr[i - 1]);
						} else {
							row[i - 1] = val;
						}
					}
					data.add(row);
				}
				// make sure to close the iterator upon completion
				rs.close();

				return data;
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return new ArrayList<Object[]>(0);
	}

	public List<Object[]> getFlatTableFromQuery(String query) {
		ResultSet rs = null;
		try {
			rs = executeQuery(query);
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
		}
		return new Vector<Object[]>(0);
	}

	public HashSet<String> getHashSetFromQuery(String query) {
		ResultSet rs = null;
		try {
			rs = executeQuery(query);
			if (rs != null) {
				ResultSetMetaData rsmd = rs.getMetaData();
				HashSet<String> data = new HashSet<String>();
				while (rs.next()) {
					data.add(rs.getString(1));
				}
				return data;
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
		return new HashSet<String>();
	}


	/**
	 * This method returns the max/min/count/sum/avg of a column returns null if
	 * statType parameter is invalid
	 * 
	 * @param columnHeader
	 * @param statType
	 * @return
	 */
	public Double getStat(String columnHeader, String statType, boolean ignoreFilter) {
		String tableName = getReadTable();
		columnHeader = tableMetaData.cleanColumn(columnHeader);

		String function = RdbmsQueryBuilder.makeFunction(columnHeader, statType, tableName);
		if (!ignoreFilter) {
			function += makeFilterSubQuery();
		}

		ResultSet rs = executeQuery(function);
		try {
			if (rs.next()) {
				return rs.getDouble(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
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
					if (!ArrayUtilityMethods.arrayContainsValue(getHeaders(tableName), headers[i].toUpperCase())) {
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
					Set<String> colIndexMapKeys = new HashSet<String>(this.tableMetaData.getColumnIndexMap().keySet());
					for(String tableColConcat : colIndexMapKeys) {
						// table name and col name are appended together with +++
						String[] tableCol = tableColConcat.split("\\+\\+\\+");
						indicesToAdd.add(tableCol);
						removeColumnIndex(tableCol[0], tableCol[1]);
					}
					
					String alterQuery = RdbmsQueryBuilder.makeAlter(tableName, newHeaders.toArray(new String[] {}), newTypes.toArray(new String[] {}));
					LOGGER.info("ALTERING TABLE: " + alterQuery);
					runQuery(alterQuery);
					LOGGER.info("DONE ALTER TABLE");
					
					for(String[] tableColIndex : indicesToAdd ) {
						addColumnIndex(tableColIndex[0], tableColIndex[1]);
					}
				}
			} else {
				// if table doesn't exist then create one with headers and types
				String createTable = RdbmsQueryBuilder.makeCreate(tableName, headers, types);
				LOGGER.info("creating table: " + createTable);
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
	public void updateTable(String[] headers, Object[] values, String[] columnHeaders) {
		try {

			Object[] joinColumn = new Object[columnHeaders.length - 1];
			System.arraycopy(columnHeaders, 0, joinColumn, 0, columnHeaders.length - 1);
			Object[] newColumn = new Object[1];
			newColumn[0] = columnHeaders[columnHeaders.length - 1];

			Object[] joinValue = new Object[values.length - 1];
			System.arraycopy(values, 0, joinValue, 0, values.length - 1);
			Object[] newValue = new Object[1];
			newValue[0] = values[columnHeaders.length - 1];

			String updateQuery = RdbmsQueryBuilder.makeUpdate(getTableName(), joinColumn, newColumn, joinValue, newValue);
			runQuery(updateQuery);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// used only for Col Split PKQL for now to update multiple columns - more
	// generic to updateTable() method
	public void updateTable2(String[] origColumns, Object[] origValues, String[] newColumns, Object[] newValues) {
		
		try {
			String updateQuery = RdbmsQueryBuilder.makeUpdate(getTableName(), origColumns, newColumns, origValues, newValues);
			runQuery(updateQuery);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * This method is responsible for processing the data associated with an
	 * iterator and adding it to the H2 table
	 * 
	 * @param iterator
	 * @param oldHeaders
	 * @param newHeaders
	 * @param types
	 * @param joinType
	 */
	public void processIterator(Iterator<IHeadersDataRow> iterator, String[] oldHeaders, String[] newHeaders, String[] types, Join joinType) {

		types = cleanTypes(types);
		String newTableName = RdbmsFrameUtility.getNewTableName();
		generateTable(iterator, newHeaders, types, newTableName);

		// create table if doesn't exist
		try {
			runQuery("CREATE TABLE IF NOT EXISTS " + getReadTable());
		} catch (Exception e) {
			e.printStackTrace();
		}

		// add the data
		if (joinType.equals(Join.FULL_OUTER)) {
			processAlterData(newTableName, newHeaders, oldHeaders, Join.LEFT_OUTER);
		} else {
			processAlterData(newTableName, newHeaders, oldHeaders, joinType);
		}

		// if we are doing a full outer join (which h2 does not natively have)
		// we have done the left outer join above
		// now just add the rows we are missing via a merge query for each row
		// not efficient but don't see another way to do it
		// Ex: merge into table (column1, column2) key (column1, column2) values
		// ('value1', 'value2')
		// TODO: change this to be a union of right outer and left outer instead
		// of inserting values
		if (joinType.equals(Join.FULL_OUTER)) {

			try {
				Statement stmt = getConnection().createStatement();
				String selectQuery = RdbmsQueryBuilder.makeSelect(newTableName, Arrays.asList(newHeaders), false) + makeFilterSubQuery();
				ResultSet rs = stmt.executeQuery(selectQuery);
				H2Iterator h2iterator = new H2Iterator(rs);

				String mergeQuery = "MERGE INTO " + getTableName();
				String columns = "(";
				for (int i = 0; i < newHeaders.length; i++) {
					if (i != 0) {
						columns += ", ";
					}
					columns += newHeaders[i];

				}
				columns += ")";

				mergeQuery += columns + " KEY " + columns;

				while (h2iterator.hasNext()) {
					Object[] row = h2iterator.next();
					String values = " VALUES(";
					for (int i = 0; i < row.length; i++) {
						if (i != 0) {
							values += ", ";
						}
						values += " '" + row[i].toString() + "' ";
					}
					values += ")";
					try {
						runQuery(mergeQuery + values);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		try {
			runQuery(RdbmsQueryBuilder.makeDropTable(newTableName));
		} catch (Exception e) {
			e.printStackTrace();
		}

		// shift to on disk if number of records is getting large
		if (this.tableMetaData.isInMem() && getNumRecords() > LIMIT_SIZE) {
			// let the method determine where the new schema will be
			convertFromInMemToPhysical(null);
		}
	}

	public void addRowsViaIterator(Iterator<IHeadersDataRow> iterator, Map<String, IMetaData.DATA_TYPES> typesMap) {
		try {
			// keep a batch size so we dont get heapspace
			final int batchSize = 5000;
			int count = 0;

			String tableName = getTableName();
			PreparedStatement ps = null;
			String[] types = null;

			// we loop through every row of the csv
			while (iterator.hasNext()) {
				IHeadersDataRow headerRow = iterator.next();
				Object[] nextRow = headerRow.getValues();

				// need to set values on the first iteration
				if (ps == null) {
					String[] headers = headerRow.getHeaders();
					// get the data types
					types = new String[headers.length];
					for (int i = 0; i < types.length; i++) {
						types[i] = Utility.convertDataTypeToString(typesMap.get(headers[i]));
					}
					// alter the table to have the column information if not
					// already present
					// this will also create a new table if the table currently
					// doesn't exist
					alterTableNewColumns(tableName, headers, types);

					// set the PS based on the headers
					ps = createInsertPreparedStatement(tableName, headers);
				}

				// we need to loop through every value and cast appropriately
				for (int colIndex = 0; colIndex < nextRow.length; colIndex++) {
					String type = types[colIndex];
					if (type.equalsIgnoreCase("DATE")) {
						java.util.Date value = Utility.getDateAsDateObj(nextRow[colIndex] + "");
						if (value != null) {
							ps.setDate(colIndex + 1, new java.sql.Date(value.getTime()));
						} else {
							ps.setNull(colIndex + 1, java.sql.Types.DATE);
						}
					} else if (type.equalsIgnoreCase("DOUBLE") || type.equalsIgnoreCase("FLOAT")) {
						Double value = Utility.getDouble(nextRow[colIndex] + "");
						if (value != null) {
							ps.setDouble(colIndex + 1, value);
						} else {
							ps.setNull(colIndex + 1, java.sql.Types.DOUBLE);
						}
					} else {
						String value = nextRow[colIndex] + "";
						ps.setString(colIndex + 1, value + "");
					}
				}
				// add it
				ps.addBatch();

				// batch commit based on size
				if (++count % batchSize == 0) {
					ps.executeBatch();
				}
			}

			if(ps == null) {
				throw new IllegalArgumentException("Iterator generated returned no values");
			}
			
			// well, we are done looping through now
			ps.executeBatch(); // insert any remaining records
			ps.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		// shift to on disk if number of records is getting large
		if (tableMetaData.isInMem() && getNumRecords() > LIMIT_SIZE) {
			// let the method determine where the new schema will be
			convertFromInMemToPhysical(null);
		}
	}

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
		boolean create = true;
		types = cleanTypes(types);
		String tableName = getTableName();
		// create table if it does not already exist
		try {
			if (!tableExists(tableName)) {
				String createTable = RdbmsQueryBuilder.makeCreate(tableName, headers, types);
				runQuery(createTable);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			brokenLines = "Headers are not properly formatted - Discontinued processing";
			create = false;
		}

		// add the row to the table
		try {
			if (create) {
				rowCount++;
				cells = Utility.castToTypes(cells, types);
				String inserter = RdbmsQueryBuilder.makeInsert(headers, types, cells, new Hashtable<String, String>(), tableName);
				runQuery(inserter);
			}
		} catch (SQLException ex) {
			System.out.println("SQL error while inserting row at " + rowCount);
			System.out.println("Exception: " + ex);
		} catch (Exception ex) {
			System.out.println("Errored.. nothing to do");
			brokenLines = brokenLines + " : " + rowCount;
		}
	}
	
	
	/**
	 * 
	 * @param newTableName
	 *            - new table to join onto main table
	 * @param newHeaders
	 *            - headers in new table
	 * @param headers
	 *            - headers in current table
	 * @param joinType
	 *            - how to join
	 */
	private void processAlterData(String newTableName, String[] newHeaders, String[] headers, Join joinType) {
		// this currently doesnt handle many to many joins and such
		// try {

		// I need to do an evaluation here to find if this one to many
		String[] oldHeaders = headers;

		// headers for the joining table

		// int curHeadCount = headers.length;
		Vector<String> newHeaderIndices = new Vector<String>();
		Vector<String> oldHeaderIndices = new Vector<String>();
		Hashtable<Integer, Integer> matchers = new Hashtable<Integer, Integer>();

		// I need to find which ones are already there and which ones are new
		for (int hIndex = 0; hIndex < newHeaders.length; hIndex++) {
			String uheader = newHeaders[hIndex];

			boolean old = false;
			for (int oIndex = 0; oIndex < headers.length; oIndex++) {
				if (headers[oIndex].equalsIgnoreCase(uheader)) {
					old = true;
					oldHeaderIndices.add(hIndex + "");
					matchers.put(hIndex, oIndex);
					break;
				}
			}

			if (!old)
				newHeaderIndices.add((hIndex) + "");
		}

		boolean one2Many = true;
		if (matchers == null || matchers.isEmpty()) {
			one2Many = false;
		}
		// I also need to accomodate when there are no common ones

		// now I need to assimilate everything into one
		String tableName1 = getTableName();

		if (one2Many)
			mergeTables(tableName1, newTableName, matchers, oldHeaders, newHeaders, joinType.getName());

		// testData();

		// } catch (SQLException e) {
		// e.printStackTrace();
		// }
	}

	// Obviously I need the table names
	// I also need the matching properties
	// I have found that out upfront- I need to also keep what it is called in
	// the old table
	// as well as the new table
	private void mergeTables(String tableName1, String tableName2, Hashtable<Integer, Integer> matchers, String[] oldTypes, String[] newTypes, String join) {
		String origTableName = tableName1;
		// now create a third table
		String tempTableName = RdbmsFrameUtility.getNewTableName();

		String newCreate = "CREATE Table " + tempTableName + " AS (";

		// now I need to create a join query
		// first the froms

		String froms = " FROM " + tableName1 + " AS  A ";
		String joins = " " + join + " " + tableName2 + " AS B ON (";

		Enumeration<Integer> keys = matchers.keys();
		for (int jIndex = 0; jIndex < matchers.size(); jIndex++) {
			Integer newIndex = keys.nextElement();
			Integer oldIndex = matchers.get(newIndex);
			
			String oldCol = oldTypes[oldIndex];
			String newCol = newTypes[newIndex];
			
			// need to make sure the data types are good to go
			String oldColType = getDataType(tableName1, oldCol);
			String newColType = getDataType(tableName2, newCol);
			
			// syntax modification for each addition join column
			if (jIndex != 0) {
				joins = joins + " AND ";
			}
			
			joins = joins + "A." + oldCol + " = " + "B." + newCol;
			
			if(!oldColType.equals(newColType)) {
				try {
					// data types are different... 
					// if both are different numbers -> convert both to double
					// else if one is double and one is string -> convert both to double
					// else -> convert both to strings
					
					// both are numbers
					if( (oldColType.equals("DOUBLE") || oldColType.equals("INT") )
						&& (newColType.equals("DOUBLE") || newColType.equals("INT") ) ) {
						// both are numbers but one is a double and the other an int
						// make both doubles
						
						if(!oldColType.equals("DOUBLE")) {
							// alter such that it is a double
							String query = "ALTER TABLE " + tableName1 + " ALTER COLUMN " + oldCol + " DOUBLE";
							LOGGER.info(query);
							runQuery(query);
						}
						if(!newColType.equals("DOUBLE")) {
							// alter such that it is a double
							String query = "ALTER TABLE " + tableName2 + " ALTER COLUMN " + newCol + " DOUBLE";
							LOGGER.info(query);
							runQuery(query);
						}
					} 
					// case when old col type is double and new col type is string
					else if( (oldColType.equals("DOUBLE") || oldColType.equals("INT") )
							&& newColType.equals("VARCHAR") ) 
					{
						// if it is not a double, convert it
						if(!oldColType.equals("DOUBLE")) {
							// alter such that it is a double
							String query = "ALTER TABLE " + tableName1 + " ALTER COLUMN " + oldCol + " DOUBLE";
							LOGGER.info(query);
							runQuery(query);
						}
						
						// new col is a string
						// so cast to double
						String query = "ALTER TABLE " + tableName2 + " ALTER COLUMN " + newCol + " DOUBLE";
						LOGGER.info(query);
						runQuery(query);
					}
					// case when old col type is string and new col type is double
					else if(  oldColType.equals("VARCHAR") && 
							(newColType.equals("DOUBLE") || newColType.equals("INT") ) ) 
					{
						// if it is not a double, convert it keep as is
						if(!newColType.equals("DOUBLE")) {
							String query = "ALTER TABLE " + tableName2 + " ALTER COLUMN " + newCol + " DOUBLE";
							LOGGER.info(query);
							runQuery(query);
						}
						
						// alter such that it is a double
						String query = "ALTER TABLE " + tableName1 + " ALTER COLUMN " + oldCol + " DOUBLE";
						LOGGER.info(query);
						runQuery(query);
					}
					// idk.. just cast both to string
					else {
						if(!oldColType.equals("VARCHAR")) {
							// if not a string, make it one
							String query = "ALTER TABLE " + tableName1 + " ALTER COLUMN " + oldCol + " VARCHAR(800)";
							LOGGER.info(query);
							runQuery(query);
						}
						if(newColType.equals("VARCHAR")) {
							// alter such that it is a double
							String query = "ALTER TABLE " + tableName2 + " ALTER COLUMN " + newCol + " VARCHAR(800)";
							LOGGER.info(query);
							runQuery(query);
						}
					}
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
		joins = joins + " )";

		// first table A
		String selectors = "";
		for (int oldIndex = 0; oldIndex < oldTypes.length; oldIndex++) {
			if (oldIndex == 0)
				selectors = "A." + oldTypes[oldIndex];
			else
				selectors = selectors + " , " + "A." + oldTypes[oldIndex];
		}

		// next table 2
		for (int newIndex = 0; newIndex < newTypes.length; newIndex++) {
			if (!matchers.containsKey(newIndex))
				selectors = selectors + " , " + "B." + newTypes[newIndex];
		}

		// want to create indices on the join columns to speed up the process
		// do this right before we execute because we alter the column types
		// to be the same
		for (Integer table1JoinIndex : matchers.keySet()) {
			Integer table2JoinIndex = matchers.get(table1JoinIndex);

			String table1JoinCol = newTypes[table1JoinIndex];
			String table2JoinCol = oldTypes[table2JoinIndex];

			addColumnIndex(tableName1, table1JoinCol);
			addColumnIndex(tableName2, table2JoinCol);
			// note that this creates indices on table1 and table2
			// but these tables are later dropped so no indices are kept
			// through the flow
		}

		String finalQuery = newCreate + "SELECT " + selectors + " " + froms + "  " + joins + " )";
		System.out.println(finalQuery);

		try {
			long start = System.currentTimeMillis();
			runQuery(finalQuery);
			long end = System.currentTimeMillis();
			System.out.println("TIME FOR JOINING TABLES = " + (end - start) + " ms");

			// Statement stmt = conn.createStatement();
			// stmt.execute(finalQuery);

			runQuery(RdbmsQueryBuilder.makeDropTable(tableName1));

			// DONT DROP THIS due to need to preserve for outer joins, method
			// outside will handle dropping new table
			// runQuery(makeDropTable(tableName2));

			// rename back to the original table
			runQuery("ALTER TABLE " + tempTableName + " RENAME TO " + origTableName);

			// this created a new table
			// need to clear the index map
			clearColumnIndexMap();
		} catch (Exception e) {
			e.printStackTrace();
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
			String dropColumnQuery = RdbmsQueryBuilder.makeDropColumn(columnHeader, getTableName());
			runQuery(dropColumnQuery);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Deletes all rows which have the associated values for the columns
	 * 
	 * @param columns
	 * @param values
	 */
	public void deleteRow(String[] columns, Object[] values) {
		try {
			String deleteRowQuery = RdbmsQueryBuilder.makeDeleteData(getTableName(), columns, values);
			runQuery(deleteRowQuery);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * used to drop the table when the insight is closed
	 */
	protected void dropTable() {
		String tableName = getTableName();
		if(tableExists(tableName)) {
			try {
				String dropTableQuery = RdbmsQueryBuilder.makeDropTable(tableName);
				runQuery(dropTableQuery);
			} catch (Exception e) {
				e.printStackTrace();
			}
			LOGGER.info("DROPPED H2 TABLE ::: " + tableName);
		} else {
			LOGGER.info("TABLE " + tableName + " DOES NOT EXIST");
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
	private boolean tableExists(String tableName) {
		String query = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '" + tableName + "'";
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

	protected void addColumnIndex(String tableName, String colName) {
//		if (!columnIndexMap.containsKey(tableName + "+++" + colName)) {
//			long start = System.currentTimeMillis();
//
//			String indexSql = null;
//			LOGGER.info("CREATING INDEX ON TABLE = " + tableName + " ON COLUMN = " + colName);
//			try {
//				String indexName = colName + "_INDEX_" + RdbmsFrameUtility.getUUID();
//				indexSql = "CREATE INDEX " + indexName + " ON " + tableName + "(" + colName + ")";
//				runQuery(indexSql);
//				columnIndexMap.put(tableName + "+++" + colName, indexName);
//				
//				long end = System.currentTimeMillis();
//
//				LOGGER.info("TIME FOR INDEX CREATION = " + (end - start) + " ms");
//			} catch (Exception e) {
//				LOGGER.info("ERROR WITH INDEX !!! " + indexSql);
//				e.printStackTrace();
//			}
//		}
	}

	protected void removeColumnIndex(String tableName, String colName) {
//		if (columnIndexMap.containsKey(tableName + "+++" + colName)) {
//			LOGGER.info("DROPPING INDEX ON TABLE = " + tableName + " ON COLUMN = " + colName);
//			String indexName = columnIndexMap.remove(tableName +  "+++" + colName);
//			try {
//				runQuery("DROP INDEX " + indexName);
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
	}

	protected void clearColumnIndexMap() {
//		this.columnIndexMap.clear();
	}
	
	protected String getDataType(String tableName, String colName) {
		String query = "select type_name from information_schema.columns where table_name='" + 
					tableName.toUpperCase() + "' and column_name='" + colName.toUpperCase() + "'";
		ResultSet rs = executeQuery(query);
		try {
			if(rs.next()) {
				return rs.getString(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	// changing from private to public access to get connection url
	// to pkql console
	public Connection getConnection() {
		return this.tableMetaData.getConnection();
	}

	public Connection convertFromInMemToPhysical(String physicalDbLocation) {
		try {
			String tableName = getTableName();
			File dbLocation = null;
			Connection previousConnection = null;
			if (this.tableMetaData.isInMem()) {
				LOGGER.info("CONVERTING FROM IN-MEMORY H2-DATABASE TO ON-DISK H2-DATABASE!");
				
				// if was in mem but want to push to specific existing location
				if (physicalDbLocation != null && !physicalDbLocation.isEmpty()) {
					dbLocation = new File(physicalDbLocation);
				}
			} else {
				LOGGER.info("CHANGEING SCHEMA FOR EXISTING ON-DISK H2-DATABASE!");
				
				if (physicalDbLocation == null || physicalDbLocation.isEmpty()) {
					LOGGER.info("SCHEMA IS ALREADY ON DISK AND DID NOT PROVIDE NEW SCHEMA TO CHAGNE TO!");
					return getConnection();
				}

				dbLocation = new File(physicalDbLocation);
				previousConnection = getConnection();
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
						+ RDBMSEngineCreationHelper.cleanTableName(getSchema()) + dateStr + "\\";
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
			boolean existingTable = tableExists(tableName);
			if (existingTable) {
				String saveScript = "SCRIPT TO '" + inMemScript + "' COMPRESSION GZIP TABLE " + getTableName();
				runQuery(saveScript);

				// drop the current table from in-memory or from old physical db
				runQuery(RdbmsQueryBuilder.makeDropTable(tableName));
			}

			// create the new conneciton
			this.tableMetaData.setConnection(DriverManager.getConnection("jdbc:h2:nio:" + physicalDbLocation, "sa", ""));

			// if previous table existed
			// we need to load it
			if (existingTable) {
				// we run the script
				runQuery("RUNSCRIPT FROM '" + inMemScript + "' COMPRESSION GZIP ");

				// clean up and remove the script file
				ICache.deleteFile(inMemScript);
			}

			setSchema(physicalDbLocation);
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

		return getConnection();
	}

	public void closeConnection() {
		try {
			getConnection().close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// TODO: should this be private?
	public String[] getHeaders(String tableName) {
		List<String> headers = new ArrayList<String>();

		String columnQuery = "SHOW COLUMNS FROM " + tableName;
		ResultSet rs = executeQuery(columnQuery);
		try {
			while (rs.next()) {
				String header = rs.getString("FIELD");
				headers.add(header);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return headers.toArray(new String[] {});
	}

	protected static String cleanType(String type) {
		if (type == null)
			type = "VARCHAR(800)";
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

	protected static String[] cleanTypes(String[] types) {
		String[] cleanTypes = new String[types.length];
		for (int i = 0; i < types.length; i++) {
			cleanTypes[i] = cleanType(types[i]);
		}

		return cleanTypes;
	}
	
	public String getSchema() {
		return this.tableMetaData.getSchema();
	}

	/**
	 * Sets the schema for the connection This is used to create a different
	 * schema for each user to facilitate BE join
	 * 
	 * @param schema
	 */
	public void setSchema(String schema) {
		this.tableMetaData.setSchema(schema);
	}

	/***************************
	 * QUERY BUILDERS
	 ***************************/

	//TODO : This is not necessary if we add temporal filters
	private String makeSpecificSelect(String tableName, List<String> selectors, String columnHeader, Object value) {
		value = RdbmsFrameUtility.cleanInstance(value.toString());

		// SELECT column1, column2, column3
		String selectStatement = "SELECT ";
		for (int i = 0; i < selectors.size(); i++) {
			String selector = selectors.get(i);

			if (i < selectors.size() - 1) {
				selectStatement += selector + ", ";
			} else {
				selectStatement += selector;
			}
		}

		// SELECT column1, column2, column3 from table1
		selectStatement += " FROM " + tableName;
		String filterSubQuery = makeFilterSubQuery();
		if (filterSubQuery.length() > 1) {
			selectStatement += filterSubQuery;
			selectStatement += " AND " + columnHeader + " = " + "'" + value + "'";
		} else {
			selectStatement += " WHERE " + columnHeader + " = " + "'" + value + "'";
		}

		return selectStatement;
	}

	/***************************
	 * END QUERY BUILDERS
	 ***************************/

	// use this when result set is not expected back
	private void runQuery(String query) throws Exception {
		Statement stat = getConnection().createStatement();
		stat.execute(query);
		stat.close();
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

	public void runExternalQuery(String query) throws Exception {
		if (checkQuery(query)) {
			runQuery(query);
		} else {
			throw new IllegalArgumentException("No permissions to run this query");
		}
	}
	
	private boolean checkQuery(String query) {
//		query = query.toUpperCase().trim();
//		return query.startsWith("CREATE ") || query.startsWith("CREATE OR REPLACE VIEW ") || query.startsWith("DROP VIEW ");
		return true;
	}
	
	// save the main table
	// need to update this if we are saving multiple tables
	public Properties save(String fileName, String[] headers) {
		Properties props = new Properties();

		List<String> selectors = new ArrayList<String>(headers.length);
		for (String header : headers) {
			selectors.add(header);
		}
		try {
			String newTable = RdbmsFrameUtility.getNewTableName();
			String createQuery = "CREATE TABLE " + newTable + " AS " + RdbmsQueryBuilder.makeSelect(getTableName(), selectors, false);
			runQuery(createQuery);
			String saveScript = "SCRIPT TO '" + fileName + "' COMPRESSION GZIP TABLE " + newTable;
			runQuery(saveScript);

			props.setProperty("tableName", newTable);

			Gson gson = new Gson();
			props.setProperty("filterHash", gson.toJson(this.tableMetaData.getFilters().getFilterHash()));

			String dropQuery = RdbmsQueryBuilder.makeDropTable(newTable);
			runQuery(dropQuery);

			props.setProperty("inMemDb", tableMetaData.isInMem() + "");

		} catch (Exception e) {
			e.printStackTrace();
		}

		return props;
	}

	/**
	 * Runs the script for a cached Insight
	 * 
	 * @param fileName
	 *            The file containing the script to create the frame
	 */
	public void open(String fileName, Properties prop) {
		// get a unique table name
		// set the table name for the instance
//		tableName = H2FRAME + getNextNumber();
		this.tableMetaData = new RdbmsTableMetaData();
		String tempTableName = null;
		String isInMemStr = null;

		if (prop != null) {
			tempTableName = prop.getProperty("tableName");
			if (tempTableName == null) {
				tempTableName = H2Builder2.tempTable;
			}
			// determine if the cache should be loaded in mem or on disk
			isInMemStr = prop.getProperty("inMemDb");
			if (isInMemStr != null) {
				boolean isInMemBool = Boolean.parseBoolean(isInMemStr.trim());
				if (!isInMemBool) {
					convertFromInMemToPhysical(null);
				}
			}
		} else {
			// this is for things that are old do not have the props file
			tempTableName = H2Builder2.tempTable;
		}
		// get the open sql script
		String openScript = "RUNSCRIPT FROM '" + fileName + "' COMPRESSION GZIP ";
		// get an alter table name sql
		String createQuery = "ALTER TABLE " + tempTableName + " RENAME TO " + getTableName();
		try {
			// we run the script in the file which automatically creates a temp
			// temple
			runQuery(openScript);
			// then we rename the temp table to the new unqiue table name
			runQuery(createQuery);

			// call the set filter to get right insight cache filter
			Gson gson = new Gson();
			if (prop.containsKey("filterHash")) {
				Map<String, Map<String, Set<Object>>> filter = gson.fromJson(prop.getProperty("filterHash"), new HashMap<>().getClass());
				for (Map.Entry<String, Map<String, Set<Object>>> entry : filter.entrySet()) {
					String columnName = entry.getKey();
					Map<String, Set<Object>> value = entry.getValue();
					for (Map.Entry<String, Set<Object>> filterList : value.entrySet()) {
						List<Object> list = new ArrayList<Object>(filterList.getValue());
						this.tableMetaData.getFilters().setFilters(columnName, list, filterList.getKey());
					}
				}
			}
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
				if(this.tableMetaData.isInMem())
					serverURL = "jdbc:h2:" + server.getURL() + "/mem:" + this.tableMetaData.getSchema() + options;
				else
					serverURL = "jdbc:h2:" + server.getURL() + "/nio:" + this.tableMetaData.getSchema();
				// System.out.println("URL: jdbc:h2:" + server.getURL() +
				// "/mem:test");
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

	public void disconnectFrame() {
		server.stop();
		server = null;
		serverURL = null;
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
			conn = getConnection();
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
	
	public int getNumRows() {
		String query = "SELECT COUNT(1) FROM " + getReadTable();
		ResultSet rs = executeQuery(query);
		try {
			while (rs.next()) {
				return rs.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return 0;
	}
	
	public int getNumRecords() {
		String tableName = getTableName();
		String query = "SELECT COUNT(*) * " + getHeaders(tableName).length + " FROM " + tableName;
		ResultSet rs = executeQuery(query);
		try {
			while (rs.next()) {
				return rs.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return 0;
	}
	
	public String getReadTable() {
		return this.tableMetaData.getViewTableName();
	}
	
	public String getTableName() {
		return this.tableMetaData.getTableName();
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
				Statement stmt = getConnection().createStatement();
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return tablePermissions.get(tableName);
	}

	public boolean isEmpty() {
		return getNumRows() == 0;
	}
	
	public DatabaseMetaData getBuilderMetadata() {
		try {
			return getConnection().getMetaData();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	private String makeFilterSubQuery() {
		return this.tableMetaData.getFilters().makeFilterSubQuery();
	}
}
