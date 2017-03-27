package prerna.ds.h2;

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
import java.util.LinkedHashMap;
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
import prerna.algorithm.api.IMetaData.DATA_TYPES;
import prerna.cache.ICache;
import prerna.ds.AbstractTableDataFrame;
import prerna.ds.AbstractTableDataFrame.Comparator;
import prerna.ds.DataFrameJoiner;
import prerna.ds.QueryStruct;
import prerna.ds.util.H2FilterHash;
import prerna.ds.util.RdbmsFrameUtility;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IHeadersDataRow;
import prerna.poi.main.RDBMSEngineCreationHelper;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class H2Builder {

	private static final Logger LOGGER = LogManager.getLogger(H2Builder.class.getName());

	protected Connection conn = null;
	
	protected String schema = "test"; // assign a default schema which is test
	// boolean create = false;
	// static int tableRunNumber = 1;
	static int rowCount = 0;
	protected static final String tempTable = "TEMP_TABLE98793";
	protected final String H2FRAME = "H2FRAME";
	protected String brokenLines = "";
	protected String options = ":LOG=0;CACHE_SIZE=65536;LOCK_MODE=1;UNDO_LOG=0";
	protected Server server = null;
	protected String serverURL = null;
	protected Hashtable<String, String[]> tablePermissions = new Hashtable<String, String[]>();

	// keep track of the indices that exist in the table for optimal speed in
	// sorting
	protected Hashtable<String, String> columnIndexMap = new Hashtable<String, String>();

	// for writing the frame on disk
	// currently not used
	// would require reconsideration of dashboard, etc.
	// since frames are not in same schema
	protected boolean isInMem = true;
	protected final int LIMIT_SIZE;

	// Provides a translation for incoming types into something H2 can
	// understand
	protected Map<String, String> typeConversionMap = new HashMap<String, String>();
	{
		typeConversionMap.clear();
		typeConversionMap.put("NUMBER", "DOUBLE");
		typeConversionMap.put("FLOAT", "DOUBLE");
		typeConversionMap.put("LONG", "DOUBLE");
		typeConversionMap.put("STRING", "VARCHAR(800)");
		typeConversionMap.put("DATE", "DATE");
		typeConversionMap.put("TIMESTAMP", "DATE");
	}

	// consists of which values to keep for each header when gathering data
	// Ex: Studio -> {EQUALS -> [WB, Universal]}
	// we want to focus on only Studios which are WB and Universal
	// RottenTomatoesCritics -> {LESS_THAN -> [0.9], GREATER_THAN -> [0.8]}
	// we want to focus on RottenTomatoesCritics < 0.9 AND > 0.8
	protected Map<String, Map<AbstractTableDataFrame.Comparator, Set<Object>>> filterHash2 = new HashMap<>();

	// name of the main table for H2
	protected String tableName;

	protected DataFrameJoiner joiner;

	// specifies the join types for an H2 frame
	public enum Join {
		INNER("INNER JOIN"), LEFT_OUTER("LEFT OUTER JOIN"), RIGHT_OUTER("RIGHT OUTER JOIN"), FULL_OUTER("FULL OUTER JOIN"), CROSS("CROSS JOIN");
		String name;

		Join(String n) {
			name = n;
		}

		public String getName() {
			return name;
		}
	}

	// These were used for adding extra numerical columns to tables so that they
	// could be used when adding a numerical column such as a group by
	// this process is no longer used since control of adding columns is
	// performed in the same place as meta data which sits outside of this class
	private static final String extraColumnBase = "ExtraColumn92917289";
	private static int columnCount = 0;
	List<String> extraColumn = new ArrayList<String>();

	/*************************** TEST **********************************************/
	public static void main(String[] a) throws Exception {

		// concurrencyTest();

		new H2Builder().testMakeFilter();
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

	private static void concurrencyTest() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:h2:mem:test:LOG=0;CACHE_SIZE=65536;LOCK_MODE=1;UNDO_LOG=0",
				"sa", "");

		int tableLength = 1000000;
		// create a table
		conn.createStatement()
				.execute("CREATE TABLE TEST (COLUMN1 VARCHAR(800), COLUMN2 VARCHAR(800), COLUMN3 VARCHAR(800))");

		// put in a LOT of data
		for (int i = 1; i <= tableLength; i++) {
			conn.createStatement().execute("INSERT INTO TEST (COLUMN1, COLUMN2, COLUMN3) VALUES ('" + tableLength
					+ "', '" + tableLength + "col2" + "', '" + tableLength + "col3')");
		}

		// alter the table (should take some time)
		conn.createStatement().execute("ALTER TABLE TEST ADD COLUMN4 VARCHAR(800)");

		// see if we try and update before alter finishes
		conn.createStatement()
				.execute("UPDATE TEST SET COLUMN4 = 'THIS IS COLUMN 4' WHERE COLUMN1 = '" + tableLength + "'");

		System.out.println("Finished");
	}

	// Test method
	public void testDB() throws Exception {
		Class.forName("org.h2.Driver");
		Connection conn = DriverManager.getConnection("jdbc:h2:C:/Users/pkapaleeswaran/workspacej3/Exp/database", "sa",
				"");
		Statement stmt = conn.createStatement();
		String query = "select t.title, s.studio from title t, studio s where t.title = s.title_fk";
		query = "select t.title, concat(t.title,':', s.studio), s.studio from title t, studio s where t.title = s.title_fk";
		// ResultSet rs = stmt.executeQuery("SELECT * FROM TITLE");
		ResultSet rs = stmt.executeQuery(query);
		// stmt.execute("CREATE TABLE MOVIES AS SELECT * From
		// CSVREAD('../Movie.csv')");

		ResultSetMetaData rsmd = rs.getMetaData();
		int columnCount = rsmd.getColumnCount();

		ArrayList records = new ArrayList();
		System.err.println("Number of columns " + columnCount);

		while (rs.next()) {
			Object[] data = new Object[columnCount];
			for (int colIndex = 0; colIndex < columnCount; colIndex++) {
				data[colIndex] = rs.getObject(colIndex + 1);
				System.out.print(rsmd.getColumnType(colIndex + 1));
				System.out.print(rsmd.getColumnName(colIndex + 1) + ":");
				System.out.print(rs.getString(colIndex + 1));
				System.out.print(">>>>" + rsmd.getTableName(colIndex + 1) + "<<<<");
			}
			System.out.println();
		}

		// add application code here
		conn.close();
	}

	// Test method
	public void predictTypes(String csv) {
		ST values = new ST("(Hello world " + "'<Hello_x_(Y)>' <y;null = \"'0'\">" + ")");
		values.add("(Hello_x(Y)", "Try");
		System.out.println(values.getAttributes());
		System.out.println(">> " + values.render());

		System.out.println("Return is ..  " + castToType("$12.3344"));

		String templateString = "Yo baby ${x}";
		Map valuesMap = new HashMap();

		valuesMap.put("x", "yo");

		StrSubstitutor ss = new StrSubstitutor(valuesMap);
		System.out.println(ss.replace(templateString));

	}

	public void loadCSV(String fileName) {
		try {
			// String fileName =
			// "C:/Users/pkapaleeswaran/workspacej3/datasets/consumer_complaints.csv";

			// fileName =
			// "C:/Users/pkapaleeswaran/workspacej3/datasets/pregnancyS.csv";

			Class.forName("org.h2.Driver");
			Connection conn = DriverManager.getConnection("jdbc:h2:mem:test", "sa", "");
			Statement stmt = conn.createStatement();

			long now = System.nanoTime();
			/*
			 * stmt.execute("Create table test(" + "Complaint_ID varchar(255), "
			 * + "product varchar(255)," + "sub_product varchar(255)," +
			 * "issue varchar(255)," + "sub_issue varchar(255)," +
			 * "state varchar(10)," + "zipcode int," +
			 * "submitted_via varchar(20)," + "date_received date," +
			 * "date_sent date," + "company varchar(255)," +
			 * "company_response varchar(255)," +
			 * "timely_response varchar(255)," +
			 * "consumer_disputed varchar(5))  as select * from csvread('" +
			 * fileName + "')");
			 */

			System.out.println("File Name ...  " + fileName);
			stmt.execute("Create table test as select * from csvread('" + fileName + "')");
			long graphTime = System.nanoTime();
			System.out.println("Time taken.. " + ((graphTime - now) / 1000000) + " milli secs");

			String query = "Select  bencat, count(*) from test where sex='F' Group By Bencat";

			ResultSet rs = stmt.executeQuery(query);

			while (rs.next()) {
				System.out.print("Bencat.. " + rs.getObject(1));
				System.out.println("Count.. " + rs.getObject(2));
			}

			graphTime = System.nanoTime();

			stmt.execute("Alter table test add dummy varchar2(200)");

			long rightNow = System.nanoTime();
			System.out.println("Update Time taken.. " + ((rightNow - graphTime) / 1000000) + "milli secs");

			graphTime = System.nanoTime();

			// stmt.execute("Update test set dummy='try' where bencat = 'ADFMLY'
			// ");
			stmt.execute("Update test set dummy='try' where bencat = 'ADN' ");
			rightNow = System.nanoTime();

			// stmt.execute("Delete from Test where State = 'TX'");
			System.out.println("Update Time taken.. " + ((rightNow - graphTime) / 1000000) + "milli secs");
			graphTime = rightNow;

			rightNow = System.nanoTime();

			// rs = stmt.executeQuery("Select count(State) from test where
			// zipcode > 22000");
			/*
			 * rs = stmt.executeQuery(
			 * "Select  sum(zipcode) from test where zipcode > 22000");
			 * 
			 * while(rs.next()) { System.out.println("Count.. " +
			 * rs.getObject(1)); } System.out.println("Query Time taken.. " +
			 * ((rightNow - graphTime) / 1000000000) + " secs");
			 */

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void testData() throws SQLException {

		ResultSet rs = conn.createStatement().executeQuery("Select count(*) from " + tableName);
		while (rs.next())
			System.out.println("Inserted..  " + rs.getInt(1));

		/*
		 * String query =
		 * "Select  bencat, count(*) from H2FRAME where sex='F' Group By Bencat"
		 * ;
		 * 
		 * rs = conn.createStatement().executeQuery(query);
		 * 
		 * 
		 * while(rs.next()) { System.out.print("Bencat.. " + rs.getObject(1));
		 * System.out.println("Count.. " + rs.getObject(2)); }
		 */
	}

	public void testMakeFilter() {

		Set<Object> list1 = new HashSet<>();
		list1.add("value1");
		list1.add("value2");
		list1.add("value3");
		list1.add("value4");
		Map<AbstractTableDataFrame.Comparator, Set<Object>> innerMap = new HashMap<>();
		innerMap.put(AbstractTableDataFrame.Comparator.EQUAL, list1);
		filterHash2.put("column1", innerMap);

		Set<Object> list2 = new HashSet<>();
		list2.add("valueA");
		list2.add("valueB");
		list2.add("valueC");
		list2.add("valueD");
		Map<AbstractTableDataFrame.Comparator, Set<Object>> innerMap2 = new HashMap<>();
		innerMap2.put(AbstractTableDataFrame.Comparator.EQUAL, list2);
		filterHash2.put("column2", innerMap2);

		Set<Object> list3 = new HashSet<>();
		list3.add("value1A");
		list3.add("value2B");
		list3.add("value3C");
		list3.add("value4D");
		Map<AbstractTableDataFrame.Comparator, Set<Object>> innerMap3 = new HashMap<>();
		innerMap3.put(AbstractTableDataFrame.Comparator.NOT_EQUAL, list3);
		filterHash2.put("column3", innerMap3);

		Set<String> headers = new HashSet<>();
		headers.add("column1");
		headers.add("column2");
		headers.add("column3");

		// String makeQuery = this.makeSelect("TestTable", headers);
		// System.out.println(makeQuery);

	}

	/***************************
	 * END TEST
	 ******************************************/

	/*************************** CONSTRUCTORS **************************************/

	protected H2Builder() {
		// //initialize a connection
		// getConnection();
		tableName = getNewTableName();
		this.LIMIT_SIZE = RdbmsFrameUtility.getLimitSize();
	}

	/***************************
	 * END CONSTRUCTORS
	 **********************************/

	private Object[] castToType(String input) {
		return Utility.findTypes(input);
	}

	// get a new unique table name
	public String getNewTableName() {
		String name = H2FRAME + getNextNumber();
		return name;
	}

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
			LOGGER.info(" >>> CREATING TABLE : " + createTable);
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
					String type = types[colIndex].toUpperCase();
					if (type.contains("DATE")) {
						java.util.Date value = Utility.getDateAsDateObj(nextRow[colIndex] + "");
						if (value != null) {
							ps.setDate(colIndex + 1, new java.sql.Date(value.getTime()));
						}
					} else if (type.contains("DOUBLE") || type.contains("DECIMAL") || type.contains("FLOAT")) {
						Double value = Utility.getDouble(nextRow[colIndex] + "");
						if (value != null) {
							ps.setDouble(colIndex + 1, value);
						}
					} else {
						String value = nextRow[colIndex] + "";
						if(value.length() > 800) {
							value = value.substring(0, 796) + "...";
						}
						ps.setString(colIndex + 1, value);
					}
				}
				// add it
				ps.addBatch();

				// batch commit based on size
				if (++count % batchSize == 0) {
					LOGGER.info("Executing batch .... row num = " + count);
					ps.executeBatch();
				}
			}

			// well, we are done looping through now
			LOGGER.info("Executing final batch .... row num = " + count);
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

	
	
	
	/*************************** 
	 * 	READ
	 * *************************/

	// get scaled version of above method
	public List<Object[]> getScaledData(String tableName, List<String> selectors, List<DATA_TYPES> dataTypes,
			String column, Object value, Double[] maxArr, Double[] minArr) {

	
		int cindex = selectors.indexOf(column);
		if (tableName == null) {
			tableName = this.tableName;
		}
		
		List<Object[]> data;

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
						if (cindex != (i - 1) && (dataTypes.get(i - 1).equals(IMetaData.DATA_TYPES.NUMBER))) {
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
					
					String result = rs.getString(1);
					if(rs.wasNull()){
						data.add(null);
					} else {
						data.add(rs.getString(1));
					}
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
	 * Get the column values from the table as an Object array
	 * 
	 * @param columnHeader
	 * @param distinct
	 *            - true indicates only distinct values, false otherwise
	 * @return
	 */
	public Object[] getColumn(String columnHeader, boolean distinct) {
		String tableName = getReadTable();

		
		ArrayList<Object> column = new ArrayList<>();

		columnHeader = cleanHeader(columnHeader);
		List<String> headers = new ArrayList<String>(1);
		headers.add(columnHeader);
		ResultSet rs;
		String selectQuery = RdbmsQueryBuilder.makeSelect(tableName, headers, distinct) + makeFilterSubQuery();

		try {
			rs = executeQuery(selectQuery);
			while (rs.next()) {
				column.add(rs.getObject(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return column.toArray();
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
				LOGGER.info("CREATING TABLE: " + createTable);
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
		headers = cleanHeaders(headers);
		columnHeaders = cleanHeaders(columnHeaders);
		
		try {

			Object[] joinColumn = new Object[columnHeaders.length - 1];
			System.arraycopy(columnHeaders, 0, joinColumn, 0, columnHeaders.length - 1);
			Object[] newColumn = new Object[1];
			newColumn[0] = columnHeaders[columnHeaders.length - 1];

			Object[] joinValue = new Object[values.length - 1];
			System.arraycopy(values, 0, joinValue, 0, values.length - 1);
			Object[] newValue = new Object[1];
			newValue[0] = values[columnHeaders.length - 1];

			String updateQuery = RdbmsQueryBuilder.makeUpdate(tableName, joinColumn, newColumn, joinValue, newValue);
			runQuery(updateQuery);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// used only for Col Split PKQL for now to update multiple columns - more
	// generic to updateTable() method
	public void updateTable2(String[] origColumns, Object[] origValues, String[] newColumns, Object[] newValues) {
		origColumns = cleanHeaders(origColumns);
		newColumns = cleanHeaders(newColumns);
		
		try {
			String updateQuery = RdbmsQueryBuilder.makeUpdate(tableName, origColumns, newColumns, origValues, newValues);
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

		newHeaders = cleanHeaders(newHeaders);
		types = cleanTypes(types);
		String newTableName = getNewTableName();
		generateTable(iterator, newHeaders, types, newTableName);

		// create table if doesn't exist
//		try {
//			runQuery("CREATE TABLE IF NOT EXISTS " + tableName);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}

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

				String mergeQuery = "MERGE INTO " + tableName;
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
		if (isInMem && getNumRecords() > LIMIT_SIZE) {
			// let the method determine where the new schema will be
			convertFromInMemToPhysical(null);
		}
	}

	public void addRowsViaIterator(Iterator<IHeadersDataRow> iterator, Map<String, IMetaData.DATA_TYPES> typesMap) {
		try {
			// keep a batch size so we dont get heapspace
			final int batchSize = 5000;
			int count = 0;

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
					String type = types[colIndex].toUpperCase();
					if (type.contains("DATE")) {
						java.util.Date value = Utility.getDateAsDateObj(nextRow[colIndex] + "");
						if (value != null) {
							ps.setDate(colIndex + 1, new java.sql.Date(value.getTime()));
						} else {
							ps.setNull(colIndex + 1, java.sql.Types.DATE);
						}
					} else if (type.contains("DOUBLE") || type.contains("DECIMAL") || type.contains("FLOAT")) {
						Double value = Utility.getDouble(nextRow[colIndex] + "");
						if (value != null) {
							ps.setDouble(colIndex + 1, value);
						} else {
							ps.setNull(colIndex + 1, java.sql.Types.DOUBLE);
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
					LOGGER.info("Executing batch .... row num = " + count);
					ps.executeBatch();
				}
			}

			if(ps == null) {
				throw new IllegalArgumentException("Iterator generated returned no values");
			}
			
			// well, we are done looping through now
			LOGGER.info("Executing final batch .... row num = " + count);
			ps.executeBatch(); // insert any remaining records
			ps.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		// shift to on disk if number of records is getting large
		if (isInMem && getNumRecords() > LIMIT_SIZE) {
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
	
	// rename a column
	private void renameColumn(String fromColumn, String toColumn) {
		String renameQuery = RdbmsQueryBuilder.makeRenameColumn(fromColumn, toColumn, tableName);
		try {
			runQuery(renameQuery);
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
			columnHeader = cleanHeader(columnHeader);
			String dropColumnQuery = RdbmsQueryBuilder.makeDropColumn(columnHeader, tableName);
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
			columns = cleanHeaders(columns);
			String deleteRowQuery = RdbmsQueryBuilder.makeDeleteData(tableName, columns, values);
			runQuery(deleteRowQuery);
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
			LOGGER.info("DROPPED H2 TABLE ::: " + tableName);
		} else {
			LOGGER.info("TABLE " + tableName + " DOES NOT EXIST");
		}
	}

	
	/*************************** 
	 * 	END DELETE
	 * *************************/

	
	
	
	/*************************** 
	 * 	FILTER
	 * *************************/

	/**
	 * Aggregates filters for a columnheader In the case of a numerical filter
	 * such as greater than, less than, filters are replaced
	 * 
	 * @param columnHeader
	 *            - column filters to modify
	 * @param values
	 *            - values to add to filters
	 * @param comparator
	 */
	public void addFilters(String columnHeader, List<Object> values, AbstractTableDataFrame.Comparator comparator) {
		columnHeader = cleanHeader(columnHeader);

		
		
		Map<String, Map<AbstractTableDataFrame.Comparator, Set<Object>>> filterHash = getFilterHash();
		String column = columnHeader;

		if (filterHash.get(column) == null) {
			setFilters(column, values, comparator);
		} else {
			Set<Object> set = new HashSet<>(values);
			boolean addNull = set.remove(AbstractTableDataFrame.VALUE.NULL);
			
			Map<AbstractTableDataFrame.Comparator, Set<Object>> innerMap = filterHash.get(column);
			if (innerMap.get(comparator) == null || (comparator != AbstractTableDataFrame.Comparator.EQUAL && comparator != AbstractTableDataFrame.Comparator.NOT_EQUAL) && set.size() > 0) {
				innerMap.put(comparator, set);
			} else if(set.size() > 0) {
				innerMap.get(comparator).addAll(set);
			}
			
			
			if(addNull) {
				if(comparator.equals(AbstractTableDataFrame.Comparator.EQUAL))
					innerMap.put(AbstractTableDataFrame.Comparator.IS_NULL, new HashSet<>());
				else if(comparator.equals(AbstractTableDataFrame.Comparator.NOT_EQUAL))
					innerMap.put(AbstractTableDataFrame.Comparator.IS_NOT_NULL, new HashSet<>());
			}
		}
	}

	/**
	 * Overwrites filters for a specific column with the values and comparator
	 * specified
	 * 
	 * @param columnHeader
	 * @param values
	 * @param comparator
	 */
	public void setFilters(String columnHeader, List<Object> values, AbstractTableDataFrame.Comparator comparator) {
		columnHeader = cleanHeader(columnHeader);

		Map<AbstractTableDataFrame.Comparator, Set<Object>> innerMap = new LinkedHashMap<>();
		
		Set<Object> set = new HashSet<>(values);
		boolean containsNull = set.remove(AbstractTableDataFrame.VALUE.NULL);
		if(set.size() > 0) {
			innerMap.put(comparator, set);
		}
		if(containsNull) {
			if(comparator.equals(AbstractTableDataFrame.Comparator.EQUAL))
				innerMap.put(AbstractTableDataFrame.Comparator.IS_NULL, new HashSet<>());
			else if(comparator.equals(AbstractTableDataFrame.Comparator.NOT_EQUAL))
				innerMap.put(AbstractTableDataFrame.Comparator.IS_NOT_NULL, new HashSet<>());
		}

		filterHash2.put(columnHeader, innerMap);
	}

	/**
	 * Clears the filters for the columnHeader
	 * 
	 * @param columnHeader
	 */
	public void removeFilter(String columnHeader) {
		columnHeader = cleanHeader(columnHeader);
		filterHash2.remove(columnHeader);
	}

	/**
	 * Clears all filters associated with the main table
	 */
	public void clearFilters() {
		filterHash2.clear();
	}

	/**
	 * 
	 * @param filterHash
	 * @param filterComparator
	 * @return
	 */
	protected String makeFilterSubQuery(Map<String, List<Object>> filterHash, Map<String, AbstractTableDataFrame.Comparator> filterComparator) {
		// need translation of filter here
		String filterStatement = "";
		if (filterHash.keySet().size() > 0) {

			List<String> filteredColumns = new ArrayList<String>(filterHash.keySet());
			for (int x = 0; x < filteredColumns.size(); x++) {

				String header = filteredColumns.get(x);
				// String tableHeader = joinMode ? translateColumn(header) :
				// header;
				String tableHeader = header;

				AbstractTableDataFrame.Comparator comparator = filterComparator.get(header);

				switch (comparator) {
				case EQUAL: {
					List<Object> filterValues = filterHash.get(header);
					String listString = getQueryStringList(filterValues);
					filterStatement += tableHeader + " in " + listString;
					break;
				}
				case NOT_EQUAL: {
					List<Object> filterValues = filterHash.get(header);
					String listString = getQueryStringList(filterValues);
					filterStatement += tableHeader + " not in " + listString;
					break;
				}
				case LESS_THAN: {
					List<Object> filterValues = filterHash.get(header);
					String listString = filterValues.get(0).toString();
					filterStatement += tableHeader + " < " + listString;
					break;
				}
				case GREATER_THAN: {
					List<Object> filterValues = filterHash.get(header);
					String listString = filterValues.get(0).toString();
					filterStatement += tableHeader + " > " + listString;
					break;
				}
				case GREATER_THAN_EQUAL: {
					List<Object> filterValues = filterHash.get(header);
					String listString = filterValues.get(0).toString();
					filterStatement += tableHeader + " >= " + listString;
					break;
				}
				case LESS_THAN_EQUAL: {
					List<Object> filterValues = filterHash.get(header);
					String listString = filterValues.get(0).toString();
					filterStatement += tableHeader + " <= " + listString;
					break;
				}
				default: {
					List<Object> filterValues = filterHash.get(header);
					String listString = getQueryStringList(filterValues);

					filterStatement += tableHeader + " in " + listString;
				}
				}

				// put appropriate ands
				if (x < filteredColumns.size() - 1) {
					filterStatement += " AND ";
				}
			}

			if (filterStatement.length() > 0) {
				filterStatement = " WHERE " + filterStatement;
			}
		}

		return filterStatement;
	}

	protected String makeFilterSubQuery() {
		Map<String, Map<AbstractTableDataFrame.Comparator, Set<Object>>> filterHash = getFilterHash();
		return makeFilterSubQuery(filterHash);
	}

	private String makeFilterSubQuery(Map<String, Map<AbstractTableDataFrame.Comparator, Set<Object>>> filterHash) {
		// need to also pass in translationMap


		String filterStatement = "";
		if (filterHash.keySet().size() > 0) {

			List<String> filteredColumns = new ArrayList<String>(filterHash.keySet());
			for (int x = 0; x < filteredColumns.size(); x++) {

				String header = filteredColumns.get(x);
				// String tableHeader = joinMode ? translateColumn(header) :
				// header;
				String tableHeader = header;

				Map<AbstractTableDataFrame.Comparator, Set<Object>> innerMap = filterHash.get(header);
				int i = 0;
				boolean addOr = false;
				for (AbstractTableDataFrame.Comparator comparator : innerMap.keySet()) {
					if(i==0) {
						filterStatement += "(";
					}
					if (i > 0) {
						if(comparator.equals(AbstractTableDataFrame.Comparator.IS_NULL) || addOr) {
							filterStatement += " OR ";
						} else {
							filterStatement += " AND ";
						}
					} else if(comparator.equals(AbstractTableDataFrame.Comparator.IS_NULL)) {
						addOr = true;
					}
					switch (comparator) {

					case EQUAL: {
						Set<Object> filterValues = innerMap.get(comparator);
						String listString = getQueryStringList(filterValues);
						filterStatement += tableHeader + " IN " + listString;
						break;
					}
					case NOT_EQUAL: {
						Set<Object> filterValues = innerMap.get(comparator);
						String listString = getQueryStringList(filterValues);
						filterStatement += tableHeader + " NOT IN " + listString;
						break;
					}
					case LESS_THAN: {
						Set<Object> filterValues = innerMap.get(comparator);
						String listString = filterValues.iterator().next().toString();
						filterStatement += tableHeader + " < " + listString;
						break;
					}
					case GREATER_THAN: {
						Set<Object> filterValues = innerMap.get(comparator);
						String listString = filterValues.iterator().next().toString();
						filterStatement += tableHeader + " > " + listString;
						break;
					}
					case GREATER_THAN_EQUAL: {
						Set<Object> filterValues = innerMap.get(comparator);
						String listString = filterValues.iterator().next().toString();
						filterStatement += tableHeader + " >= " + listString;
						break;
					}
					case LESS_THAN_EQUAL: {
						Set<Object> filterValues = innerMap.get(comparator);
						String listString = filterValues.iterator().next().toString();
						filterStatement += tableHeader + " <= " + listString;
						break;
					}
					case IS_NOT_NULL: {
						filterStatement += tableHeader + " IS NOT NULL ";
						break;
					}
					case IS_NULL: {
						filterStatement += tableHeader + " IS NULL ";
						break;
					}
					default: {
						Set<Object> filterValues = innerMap.get(comparator);
						String listString = getQueryStringList(filterValues);
						filterStatement += tableHeader + " in " + listString;
					}

					}
					i++;
				}

				// put appropriate ands
				filterStatement += ")";
				if (x < filteredColumns.size() - 1) {
					filterStatement += " AND ";
				}
			}

			if (filterStatement.length() > 0) {
				filterStatement = " WHERE " + filterStatement;
			}
		}

		return filterStatement;
	}

	public Object[] getFilterHashJoinedMode() {
		Map<String, Map<AbstractTableDataFrame.Comparator, Set<Object>>> filterHash = getFilterHash();
		return new Object[] { tableName, filterHash };
	}

	/**
	 * 
	 * @param selectors
	 *            - list of headers to grab from table
	 * @return
	 * 
	 * 		return the filtered values portion of the filter model that is
	 *         returned by the H2Frame
	 */
	public Map<String, List<Object>> getFilteredValues(List<String> selectors, String limitOffset) {

		// Header name -> [val1, val2, ...]
		// Ex: Studio -> [WB, Universal]
		// WB and Universal are filtered from Studio column
		String tableName = this.tableName;
		Map<String, Map<AbstractTableDataFrame.Comparator, Set<Object>>> filterHash = getFilterHash();
		Map<String, Map<AbstractTableDataFrame.Comparator, Set<Object>>> hardfilterHash = getHardFilterHash();//FilterHash();
		Map<String, List<Object>> returnFilterMap = new HashMap<>();

		try {
			for (String selector : selectors) {

				String thisSelector = selector;

				if (hardfilterHash.get(thisSelector) != null) {
					String query = makeNotSelect(tableName, thisSelector, filterHash) + limitOffset;
					ResultSet rs = executeQuery(query);

					List<Object> filterData = null;
					if (rs != null) {
						// ResultSetMetaData rsmd = rs.getMetaData();
						filterData = new ArrayList<>();
						while (rs.next()) {
							Object nextValue = rs.getObject(1);
							filterData.add(nextValue);
						}
					}

					returnFilterMap.put(selector, filterData);
				} else {
					returnFilterMap.put(selector, new ArrayList<Object>());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return returnFilterMap;
	}

	public Map<String, Map<AbstractTableDataFrame.Comparator, Set<Object>>> getFilterHash() {
		if(joiner != null) {
			H2FilterHash joinerFilters = joiner.getFilters(getTableName());
			return mergeFilters(this.filterHash2, joinerFilters);
		}
		return this.filterHash2;
	}
	
	public Map<String, Map<AbstractTableDataFrame.Comparator, Set<Object>>> getHardFilterHash() {
		return this.filterHash2;
	}

	//This is a mess but will be merged into H2FilterHash and much simpler once we use that and not the long map
	//also will rely upon strings moving forward, shifting away from comparator
	private Map<String, Map<AbstractTableDataFrame.Comparator, Set<Object>>> mergeFilters(Map<String, Map<AbstractTableDataFrame.Comparator, Set<Object>>> curFilters, H2FilterHash filters) {
		
		Map<String, Map<String, Set<Object>>> filterHash = filters.getFilterHash();
		if(filterHash.isEmpty()) return curFilters;

		List<String> addedKeys = new ArrayList<String>();
		
		Map<String, Map<AbstractTableDataFrame.Comparator, Set<Object>>> mergedFilters = new HashMap<>();
		for(String key : filterHash.keySet()) {
			addedKeys.add(key);
			if(curFilters.containsKey(key)) {
				//pick the more restrictive filters
				Map<AbstractTableDataFrame.Comparator, Set<Object>> curFilterMap = curFilters.get(key);
				Map<String, Set<Object>> filterMap = filterHash.get(key);
				List<Comparator> addedFilterKeys = new ArrayList<>();
				
				Map<AbstractTableDataFrame.Comparator, Set<Object>> newInnerMap = new HashMap<>();
				for(String filterKey : filterMap.keySet()) {
					Comparator comp = RdbmsFrameUtility.getStringComparatorValue(filterKey);
					addedFilterKeys.add(comp);
					if(curFilterMap.containsKey(comp)) {
						Set<Object> set1 = curFilterMap.get(comp);
						Set<Object> set2 = filterMap.get(filterKey);
						Set<Object> restrictiveSet = getMoreRestrictiveSet(comp, set1, set2);
						newInnerMap.put(comp, restrictiveSet);
					} else {
						newInnerMap.put(comp, filterMap.get(filterKey));
					}
				}
				
				for(Comparator comparator : curFilterMap.keySet()) {
					if(!addedFilterKeys.contains(comparator)) {
						newInnerMap.put(comparator, curFilterMap.get(comparator));
					}
				}
				mergedFilters.put(key, newInnerMap);
			} else {
				Map<String, Set<Object>> innerFilterHash = filterHash.get(key);
				Map<AbstractTableDataFrame.Comparator, Set<Object>> newInnerMap = new HashMap<>();
				for(String comparator : innerFilterHash.keySet()) {
					Set<Object> set = innerFilterHash.get(comparator);
					Comparator comp = RdbmsFrameUtility.getStringComparatorValue(comparator);
					newInnerMap.put(comp, set);
				}
				mergedFilters.put(key, newInnerMap);
			}
		}
		
		for(String key : curFilters.keySet()) {
			if(!addedKeys.contains(key)) {
				mergedFilters.put(key, curFilters.get(key));
			}
		}
		
		return mergedFilters;
	}
	
	//assumes one set is a subset of the other
	private Set<Object> getMoreRestrictiveSet(Comparator comparator, Set<Object> set1, Set<Object> set2) {
		
		if(set1 == null || set1.isEmpty()) return set2;
		if(set2 == null || set2.isEmpty()) return set1;
		
		switch(comparator) {
		case EQUAL: {
			if(set1.size() < set2.size()) {
				return set1;
			} else return set2;
		}
		case NOT_EQUAL: {
			if(set1.size() > set2.size()) {
				return set1;
			} else return set2;
		}
		
		//TODO : need to revisit how we do this
//		case LESS_THAN: {
//			Object firstObj = set1.iterator().next();
//			Object secondObj = set2.iterator().next();
//			if()
//			break;
//		}
//		case GREATER_THAN: {
//			Object firstObj = set1.iterator().next();
//			Object secondObj = set2.iterator().next();
//			break;
//		}
//		case GREATER_THAN_EQUAL: {
//			Object firstObj = set1.iterator().next();
//			Object secondObj = set2.iterator().next();
//			break;
//		}
//		case LESS_THAN_EQUAL: {
//			Object firstObj = set1.iterator().next();
//			Object secondObj = set2.iterator().next();
//			break;
//		} 
		default : {
			
		}
		return set1;
	}
	}
	
	/**
	 * 
	 * @param filters
	 * @return
	 * 
	 * This method is used to convert the current filter hash to an object that can be used within a querystruct
	 */
	private Map<String,Map<String,List>> convertToQueryStuctFilter(Map<String, Map<AbstractTableDataFrame.Comparator, Set<Object>>> filters) {
		Map<String,Map<String,List>> qsFilters = new Hashtable<>();
		String tableName = getTableName();
		for(String key : filters.keySet()) {
			Map<AbstractTableDataFrame.Comparator, Set<Object>> innerFilters = filters.get(key);
			Hashtable innerQsFilters = new Hashtable<String, Vector>();
			for(Comparator innerKey : innerFilters.keySet()) {
				
				String stringInnerKey = RdbmsFrameUtility.getComparatorStringValue(innerKey);
				
				Vector v = new Vector(innerFilters.get(innerKey));
				innerQsFilters.put(stringInnerKey, v);
			}
			qsFilters.put(tableName+"__"+key, innerQsFilters);
		}
		return qsFilters;
	}
	
	/**
	 * 
	 * @param filters1
	 * @param filters2
	 * @return
	 * 
	 * return the combination of filters1 and filters2 such that it produces the most restrictive filtering
	 * 
	 * ASSUMPTION: for equivalent filters such as (Title = list1) in filters1 and (Title = list2) in filters2, list1 is a sublist of list2 or vice versa
	 */
	private Map<String, Map<String, List>> mergeFilters(Map <String, Map<String, List>> filters1, Map <String, Map<String, List>> filters2) {
		Map<String, Map<String, List>> retFilters = new Hashtable<>();
		
		retFilters.putAll(filters2);
		
		for(String key : filters1.keySet()) {
			Map<String, List> hash1 = filters1.get(key);
			Map<String, List> newHash = new Hashtable<String, List>();
			retFilters.put(key, newHash);
			//need to determine which set is more restrictive
			//assumption is that smaller vector is a subset of bigger vector
			if(filters2.containsKey(key)) {
				Map<String, List> hash2 = filters2.get(key);
				for(String relationKey : hash1.keySet()) {
					List v;
					if(hash2.containsKey(relationKey)) {
						List v2 = hash2.get(relationKey);
						List v1 = hash1.get(relationKey);
						switch(relationKey) {
						case "=": {
							//pick the smaller vector
							if(v2.size() < v1.size()) {
								newHash.put(relationKey, v2);
							} else {
								newHash.put(relationKey, v1);
							}
							break;
						}
						case "!=": {
							//pick the bigger vector
							if(v2.size() < v1.size()) {
								newHash.put(relationKey, v1);
							} else {
								newHash.put(relationKey, v2);
							}
							break;
						}
						case "<=":
						case "<": {
							//pick the smaller number
							Object thisObj = v2.get(0);
							Object incomingObj = v1.get(0);
							
							Double thisDbl = Double.NEGATIVE_INFINITY;
							Double incomingDbl = Double.POSITIVE_INFINITY;
							
							if(thisObj instanceof Double) {
								thisDbl = (Double)thisObj;
							} else {
								//i shouldn't need to cast to string here, pkql should be taking care of that
							}
							
							if(incomingObj instanceof Double) {
								incomingObj = (Double)incomingObj;
							} else {
								//i shouldn't need to cast to string here, pkql should be taking care of that
							}
							
							if(thisDbl > incomingDbl) {
								Vector newVec = new Vector(0);
								newVec.add(incomingObj);
								hash2.put(relationKey, newVec);
							} else {
								Vector newVec = new Vector(0);
								newVec.add(thisObj);
								hash2.put(relationKey, newVec);
							}
							
							break;
						}
						case ">=":
						case ">": {
							//pick the bigger number
							Object thisObj = v2.get(0);
							Object incomingObj = v1.get(0);
							
							Double thisDbl = Double.POSITIVE_INFINITY;
							Double incomingDbl = Double.NEGATIVE_INFINITY;
							
							if(thisObj instanceof Double) {
								thisDbl = (Double)thisObj;
							} else {
								//i shouldn't need to cast to string here, pkql should be taking care of that
							}
							
							if(incomingObj instanceof Double) {
								incomingObj = (Double)incomingObj;
							} else {
								//i shouldn't need to cast to string here, pkql should be taking care of that
							}
							
							if(thisDbl < incomingDbl) {
								Vector newVec = new Vector(0);
								newVec.add(incomingObj);
								newHash.put(relationKey, newVec);
							} else {
								Vector newVec = new Vector(0);
								newVec.add(thisObj);
								newHash.put(relationKey, newVec);
							}
							
							break;
						}						
						}
					} else {
						v = new Vector();
						v.addAll(hash1.get(relationKey));
						newHash.put(relationKey, v);
					}
				}
			} 
			
			//these are new filters, add them to the db filters
			else {
				for(String relationKey : hash1.keySet()) {
					Vector v = new Vector();
					v.addAll(hash1.get(relationKey));
					newHash.put(relationKey, v);
				}
				retFilters.put(key, newHash);
			}
		}
		
		return retFilters;
	}
	
	public QueryStruct mergeFilters(QueryStruct queryStruct) {
		Map<String, Map<String, List>> curFilters = convertToQueryStuctFilter(this.filterHash2);
		Map<String, Map<String, List>> mergedFilters = mergeFilters(queryStruct.andfilters, curFilters);
		queryStruct.andfilters = mergedFilters;
		return queryStruct;
	}
 	/*************************** 
	 * 	END FILTER
	 * *************************/


	

	/**
	 * 
	 * @param tableName
	 * @return true if table with name tableName exists, false otherwise
	 */
	protected boolean tableExists(String tableName) {
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

	/**
	 * 
	 * @param selectors
	 * @return
	 * 
	 * 		returns an iterator that returns data from the selectors
	 */
	public Iterator buildIterator(List<String> selectors) {
		String tableName = getReadTable();

		try {
			Statement stmt = getConnection().createStatement();
			String selectQuery = RdbmsQueryBuilder.makeSelect(tableName, selectors, false) + makeFilterSubQuery();

			long startTime = System.currentTimeMillis();
			ResultSet rs = stmt.executeQuery(selectQuery);
			long endTime = System.currentTimeMillis();
			LOGGER.info("Executed Select Query on H2 FRAME: " + (endTime - startTime) + " ms");

			return new H2Iterator(rs);
		} catch (SQLException s) {
			s.printStackTrace();
		}

		return null;
	}
	
	/**
	 * 
	 * @param selectors
	 * @return
	 * 
	 * 		returns an iterator that returns data from the selectors
	 */
	public H2Iterator buildIteratorFromQuery(String query) {
		String tableName = getReadTable();

		try {
			Statement stmt = getConnection().createStatement();

			long startTime = System.currentTimeMillis();
			ResultSet rs = stmt.executeQuery(query);
			long endTime = System.currentTimeMillis();
			LOGGER.info("Executed Select Query on H2 FRAME: " + (endTime - startTime) + " ms");

			return new H2Iterator(rs);
		} catch (SQLException s) {
			s.printStackTrace();
		}

		return null;
	}

	/**
	 * 
	 * @param selectors
	 * @return
	 * 
	 * 		returns an iterator that returns data from the selectors
	 */
	public Iterator buildIterator(List<String> selectors, boolean ignoreFilters) {
		String tableName = getReadTable();

		try {
			Statement stmt = getConnection().createStatement();
			String selectQuery = RdbmsQueryBuilder.makeSelect(tableName, selectors, true);;
			if (!ignoreFilters) {
				selectQuery += makeFilterSubQuery();
			}
			long startTime = System.currentTimeMillis();
			ResultSet rs = stmt.executeQuery(selectQuery);
			long endTime = System.currentTimeMillis();
			LOGGER.info("Executed Select Query on H2 FRAME: " + (endTime - startTime) + " ms");

			return new H2Iterator(rs);
		} catch (SQLException s) {
			s.printStackTrace();
		}

		return null;
	}

	// build the new way to create iterator with all the options
	/**
	 * 
	 * @param options
	 *            - options needed to build the iterator
	 * @return
	 * 
	 * 		returns an iterator based on the options parameter
	 */
	public Iterator buildIterator(Map<String, Object> options) {
		String tableName = getReadTable();

		String sortDir = (String) options.get(AbstractTableDataFrame.SORT_BY_DIRECTION);

		// dedup = true indicates remove duplicates
		// TODO: rename de_dup to something more meaningful for cases outside of
		// tinkergraph
		Boolean dedup = (Boolean) options.get(AbstractTableDataFrame.DE_DUP);
		if (dedup == null)
			dedup = false;

		Boolean ignoreFilters = (Boolean) options.get(AbstractTableDataFrame.IGNORE_FILTERS);
		if (ignoreFilters == null)
			ignoreFilters = false;

		// how many rows to get
		Integer limit = (Integer) options.get(AbstractTableDataFrame.LIMIT);

		// at which row to start
		Integer offset = (Integer) options.get(AbstractTableDataFrame.OFFSET);

		// column to sort by
		String sortBy = (String) options.get(AbstractTableDataFrame.SORT_BY);

		// selectors to gather
		List<String> selectors = (List<String>) options.get(AbstractTableDataFrame.SELECTORS);
		selectors = cleanHeaders(selectors);

		String selectQuery = RdbmsQueryBuilder.makeSelect(tableName, selectors, dedup);
		if(!ignoreFilters) {
			selectQuery += makeFilterSubQuery();
		}

		// temporary filters to apply only to this iterator
		Map<String, List<Object>> temporalBindings = (Map<String, List<Object>>) options.get(AbstractTableDataFrame.TEMPORAL_BINDINGS);
		Map<String, AbstractTableDataFrame.Comparator> compHash = new HashMap<String, AbstractTableDataFrame.Comparator>();
		for (String key : temporalBindings.keySet()) {
			compHash.put(key, AbstractTableDataFrame.Comparator.EQUAL);
		}

		// create a new filter substring and add/replace old filter substring
		String temporalFiltering = makeFilterSubQuery(temporalBindings, compHash); // default comparator is equals
		if (temporalFiltering != null && temporalFiltering.length() > 0) {
			if (selectQuery.contains(" WHERE ")) {
				temporalFiltering = temporalFiltering.replaceFirst(" WHERE ", "");
				selectQuery = selectQuery + temporalFiltering;
			} else {
				selectQuery = selectQuery + temporalFiltering;
			}
		}

		if (sortBy != null) {
			if(sortDir == null) {
				sortDir = " ASC ";
			} else {
				sortDir = " " + sortDir.toUpperCase() + " ";
			}
			sortBy = cleanHeader(sortBy);
			// add an index to the table to make the sort much faster
			// note h2 view does not support index
			addColumnIndex(tableName, sortBy);
			selectQuery += " ORDER BY " + sortBy + sortDir;
		}
		if (limit != null && limit > 0) {
			selectQuery += " LIMIT " + limit;
		}
		if (offset != null && offset > 0) {
			selectQuery += " OFFSET " + offset;
		}

		long startTime = System.currentTimeMillis();
		System.out.println("TABLE NAME IS: " + this.tableName);
		System.out.println("RUNNING QUERY : " + selectQuery);
		ResultSet rs = executeQuery(selectQuery);
		long endTime = System.currentTimeMillis();
		LOGGER.info("Executed Select Query on H2 FRAME: " + (endTime - startTime) + " ms");
		return new H2Iterator(rs);
	}

	/**
	 * 
	 * @param iterator
	 * @param typesMap
	 * @param updateColumns
	 * @throws Exception 
	 */
	public void mergeRowsViaIterator(Iterator<IHeadersDataRow> iterator, String[] newHeaders, DATA_TYPES[] types, String[] startingHeaders, String[] joinColumns) throws Exception {
		//step 1
		//generate a table from the iterator
		String tempTable = getNewTableName();
		int size = types.length;
		String[] cleanTypes = new String[size];
		for(int i = 0; i < size; i++) {
			cleanTypes[i] = cleanType(types[i].name());
		}
		generateTable(iterator, newHeaders, cleanTypes, tempTable);

		//Step 2
		//inner join the curTable with the tempTable
		String curTable = getTableName();
		
		// for the alter to work,
		// i want to merge the existing columns in the table
		// with the new columns we want to get
		// so we need to remove it as if it doesn't currenlty exist in the frame
		if(startingHeaders.length != newHeaders.length) {
			// so we only need to pass in the join columns
			Set<String> curColsToUse = new HashSet<String>();
			// add all the join columns
			for(int i = 0; i < joinColumns.length; i++) {
				curColsToUse.add(joinColumns[i]);
			}
			// add any column not in newHeaders
			for(int i = 0; i < startingHeaders.length; i++) {
				if(!ArrayUtilityMethods.arrayContainsValue(newHeaders, startingHeaders[i])) {
					curColsToUse.add(startingHeaders[i]);
				}
			}
			processAlterData(tempTable, curTable, curColsToUse.toArray(new String[]{}), newHeaders, Join.INNER);
		}
		
		// get all the curTable headers
		// so the insert into does it in the right order
		String[] curTableHeaders = getHeaders(curTable);
		
		//Step 3
		//merge the rows of the table with main table
		StringBuilder mergeQuery = new StringBuilder("MERGE INTO ");
		mergeQuery.append(curTable).append(" KEY(").append(joinColumns[0]);
		int keySize = joinColumns.length;
		for(int i = 1; i < keySize; i++) {
			mergeQuery.append(", ").append(joinColumns[i]);
		}
		mergeQuery.append(") (SELECT ").append(curTableHeaders[0]);
		for(int i = 1; i < curTableHeaders.length; i++) {
			mergeQuery.append(", ").append(curTableHeaders[i]);
		}
		mergeQuery.append(" FROM ").append(tempTable).append(")");
		System.out.println(mergeQuery);
		runQuery(mergeQuery.toString());
		
		//Step 4
		//drop tempTable
		runQuery(RdbmsQueryBuilder.makeDropTable(tempTable));
	}

	/**
	 * 
	 * @param newTableName				new table to join onto main table
	 * @param newHeaders				headers in new table
	 * @param headers					headers in current table
	 * @param joinType					how to join
	 */
	private void processAlterData(String newTableName, String[] newHeaders, String[] headers, Join joinType) {
		processAlterData(this.tableName, newTableName, newHeaders, headers, joinType);
	}

	private void processAlterData(String table1, String table2, String[] newHeaders, String[] headers, Join joinType) {
		// this currently doesnt handle many to many joins and such
		// try {
		getConnection();

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
			uheader = cleanHeader(uheader);

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
		if (one2Many) {
			mergeTables(table1, table2, matchers, oldHeaders, newHeaders, joinType.getName());
		}
	}

	// Obviously I need the table names
	// I also need the matching properties
	// I have found that out upfront- I need to also keep what it is called in
	// the old table
	// as well as the new table
	protected void mergeTables(String tableName1, String tableName2, Hashtable<Integer, Integer> matchers, String[] oldHeaders, String[] newHeaders, String join) {
		getConnection();

		String origTableName = tableName1;
		// now create a third table
		String tempTableName = RdbmsFrameUtility.getNewTableName();

		String newCreate = "CREATE Table " + tempTableName + " AS (";

		// now I need to create a join query
		// first the froms

		// want to create indices on the join columns to speed up the process
		for (Integer table1JoinIndex : matchers.keySet()) {
			Integer table2JoinIndex = matchers.get(table1JoinIndex);

			String table1JoinCol = newHeaders[table1JoinIndex];
			String table2JoinCol = oldHeaders[table2JoinIndex];

			addColumnIndex(tableName1, table1JoinCol);
			addColumnIndex(tableName2, table2JoinCol);
			// note that this creates indices on table1 and table2
			// but these tables are later dropped so no indices are kept
			// through the flow
		}

		String froms = " FROM " + tableName1 + " AS  A ";
		String joins = " " + join + " " + tableName2 + " AS B ON (";

		Enumeration<Integer> keys = matchers.keys();
		for (int jIndex = 0; jIndex < matchers.size(); jIndex++) {
			Integer newIndex = keys.nextElement();
			Integer oldIndex = matchers.get(newIndex);

			String oldCol = oldHeaders[oldIndex];
			String newCol = newHeaders[newIndex];

			// need to make sure the data types are good to go
			String oldColType = getDataType(tableName1, oldCol);
			String newColType = getDataType(tableName2, newCol);

			// syntax modification for each addition join column
			if (jIndex != 0) {
				joins = joins + " AND ";
			}

			if(oldColType.equals(newColType)) {
				// data types are the same, no need to do anything
				joins = joins + "A." + oldCol + " = " + "B." + newCol;
			} else {
				// data types are different... 
				// if both are different numbers -> convert both to double
				// else -> convert to strings

				if( (oldColType.equals("DOUBLE") || oldColType.equals("INT") )
						&& (newColType.equals("DOUBLE") || newColType.equals("INT") ) ) {
					// both are numbers
					if(!oldColType.equals("DOUBLE")) {
						joins = joins + " A." + oldCol;
					} else {
						joins = joins + " CAST(A." + oldCol + " AS DOUBLE)";
					}
					joins = joins + " = ";
					if(!newColType.equals("DOUBLE")) {
						joins = joins + " B." + newCol;
					} else {
						joins = joins + " CAST(B." + newCol + " AS DOUBLE)";
					}
				}
				// case when old col type is double and new col type is string
				else if( (oldColType.equals("DOUBLE") || oldColType.equals("INT") )
						&& newColType.equals("VARCHAR") ) 
				{
					// if it is not a double, convert it
					if(!oldColType.equals("DOUBLE")) {
						joins = joins + " CAST(A." + oldCol + " AS DOUBLE)";
					} else {
						joins = joins + " A." + oldCol;
					}
					joins = joins + " = ";

					// new col is a string
					// so cast to double
					joins = joins + " CAST(B." + newCol + " AS DOUBLE)";
				}
				// case when old col type is string and new col type is double
				else if(  oldColType.equals("VARCHAR") && 
						(newColType.equals("DOUBLE") || newColType.equals("INT") ) ) 
				{
					// old col is a string
					// so cast to double
					joins = joins + " CAST(A." + oldCol + " AS DOUBLE)";
					joins = joins + " = ";
					// if it is not a double, convert it
					if(!newColType.equals("DOUBLE")) {
						joins = joins + " B." + newCol;
					} else {
						joins = joins + " CAST(B." + newCol + " AS DOUBLE)";
					}
				}
				else {
					// not sure... just make everything a string
					if(oldColType.equals("VARCHAR")) {
						joins = joins + " A." + oldCol;
					} else {
						joins = joins + " CAST( A." + oldCol + " AS VARCHAR(800))";
					}
					joins = joins + " = ";
					if(newColType.equals("VARCHAR")) {
						joins = joins + " B." + newCol;
					} else {
						joins = joins + " CAST(B." + newCol + " AS VARCHAR(800))";
					}
				}
			}
		}

		joins = joins + " )";

		// first table A
		String selectors = "";
		for (int oldIndex = 0; oldIndex < oldHeaders.length; oldIndex++) {
			if (oldIndex == 0)
				selectors = "A." + oldHeaders[oldIndex];
			else
				selectors = selectors + " , " + "A." + oldHeaders[oldIndex];
		}

		// next table 2
		for (int newIndex = 0; newIndex < newHeaders.length; newIndex++) {
			if (!matchers.containsKey(newIndex))
				selectors = selectors + " , " + "B." + newHeaders[newIndex];
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

	protected void clearColumnIndexMap() {
		this.columnIndexMap.clear();
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
				System.out.println("The connection is.. " + url);
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
//				String saveScript = "SCRIPT TO '" + inMemScript + "' COMPRESSION GZIP TABLE " + this.tableName;
//				runQuery(saveScript);
				
				Connection newConnection = DriverManager.getConnection("jdbc:h2:nio:" + physicalDbLocation, "sa", "");
				copyTable(this.conn, this.tableName, newConnection, this.tableName);
				
				// drop the current table from in-memory or from old physical db
				runQuery(RdbmsQueryBuilder.makeDropTable(this.tableName));
				this.conn = newConnection;
			}

			// create the new conneciton
//			this.conn = DriverManager.getConnection("jdbc:h2:nio:" + physicalDbLocation, "sa", "");

//			// if previous table existed
//			// we need to load it
//			if (existingTable) {
//				// we run the script
//				runQuery("RUNSCRIPT FROM '" + inMemScript + "' COMPRESSION GZIP ");
//
//				// clean up and remove the script file
//				ICache.deleteFile(inMemScript);
//			}
			
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

	private String[] cleanHeaders(String[] headers) {
		String[] cleanHeaders = new String[headers.length];
		for (int i = 0; i < headers.length; i++) {
			cleanHeaders[i] = cleanHeader(headers[i]);
		}
		return cleanHeaders;
	}

	protected List<String> cleanHeaders(List<String> headers) {
		List<String> cleanedHeaders = new ArrayList<>(headers.size());
		for (String header : headers) {
			cleanedHeaders.add(cleanHeader(header));
		}
		return cleanedHeaders;
	}

	// TODO: this is done outside now, need to remove
	protected static String cleanHeader(String header) {
		/*
		 * header = header.replaceAll(" ", "_"); header = header.replace("(",
		 * "_"); header = header.replace(")", "_"); header = header.replace("-",
		 * "_"); header = header.replace("'", "");
		 */
		header = header.replaceAll("[#%!&()@#$'./-]*\"*", ""); // replace all
																// the useless
																// shit in one
																// go
		header = header.replaceAll("\\s+", "_");
		header = header.replaceAll(",", "_");
		if (Character.isDigit(header.charAt(0)))
			header = "c_" + header;
		return header;
	}

	protected String cleanType(String type) {
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
				LOGGER.info(
						"Schema being modified from: '" + this.schema + "' to new schema for user: '" + schema + "'");
				System.err.println("SCHEMA NOW... >>> " + schema);
				this.schema = schema;
				if (schema.equalsIgnoreCase("-1"))
					this.schema = "test";
				this.conn = null;
				getConnection();
			}
		}
	}

	/***************************
	 * QUERY BUILDERS
	 ******************************************/

	protected String getSqlFitler() {
		return makeFilterSubQuery();
	}

	private String makeSpecificSelect(String tableName, List<String> selectors, String columnHeader, Object value) {
		if(value != null) {
			value = RdbmsFrameUtility.cleanInstance(value.toString());
		}

		// SELECT column1, column2, column3
		String selectStatement = "SELECT ";
		for (int i = 0; i < selectors.size(); i++) {
			String selector = selectors.get(i);
			selector = cleanHeader(selector);

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
			if (value != null) {
				selectStatement += " AND " + columnHeader + " = " + "'" + value + "'";
			} else {
				selectStatement += " AND " + columnHeader + " IS NULL";
			}
		} else {
			if (value != null) {
				selectStatement += " WHERE " + columnHeader + " = " + "'" + value + "'";
			} else {
				selectStatement += " WHERE " + columnHeader + " IS NULL";
			}
		}

		return selectStatement;
	}

	/**
	 * This method returns all the distinct values that are filtered out for a
	 * particular column This method is mainly used to retrieve values for the
	 * filter model displayed on the front end
	 * 
	 * @param tableName
	 *            - table in which selector exists
	 * @param selector
	 *            - column to grab values from
	 * @return
	 */
	private String makeNotSelect(String tableName, String selector, Map<String, Map<AbstractTableDataFrame.Comparator, Set<Object>>> filterHash) {
		String selectStatement = "SELECT DISTINCT ";

		selector = cleanHeader(selector);
		selectStatement += selector + " FROM " + tableName;

		String filterStatement = "";

		Map<AbstractTableDataFrame.Comparator, Set<Object>> filterMap = filterHash.get(selector);
		int i = 0;
		
		//if the filterMap does not contain a comparator referring to null (is not null or is null) we will asssume to include them
		//for the following comparators
		//equal, greater than, greater than equal, less than, less than equal
		//we want to do this because the following query does not return null values 
		//	SELECT DISTINCT column FROM table WHERE value IN ('val1', 'val2', ..., 'valn');
		Set<Comparator> keySet = filterMap.keySet();
		boolean addNulls = false;
		if(!keySet.contains(AbstractTableDataFrame.Comparator.IS_NOT_NULL) || !keySet.contains(AbstractTableDataFrame.Comparator.IS_NULL)) {
			addNulls = true;
		}
		
		
		// what ever is listed in the filter hash, we want the get the values
		// that would be the logical opposite
		// i.e. if filter hash indicates 'X < 0.9 AND X > 0.8', return 'X > =0.9
		// OR X <= 0.8'
		for (AbstractTableDataFrame.Comparator comparator : filterMap.keySet()) {
			if (i > 0) {
				if(comparator.equals(AbstractTableDataFrame.Comparator.IS_NULL)) {
					filterStatement += " AND ";
				} else {
					filterStatement += " OR ";
				}
			}
			Set<Object> filterValues = filterMap.get(comparator);
			if (filterValues.size() == 0 && !comparator.equals(AbstractTableDataFrame.Comparator.IS_NOT_NULL) && !comparator.equals(AbstractTableDataFrame.Comparator.IS_NULL)) {
				continue;
			}

			if (comparator.equals(AbstractTableDataFrame.Comparator.EQUAL)) {
				String listString = getQueryStringList(filterValues);
				filterStatement += selector + " NOT IN " + listString;
				if(addNulls) {
					filterStatement += " OR " + selector + " IS NULL";
				}
			} else if (comparator.equals(AbstractTableDataFrame.Comparator.NOT_EQUAL)) {
				String listString = getQueryStringList(filterValues);
				filterStatement += selector + " IN " + listString;
			} else if (comparator.equals(AbstractTableDataFrame.Comparator.GREATER_THAN)) {
				filterStatement += selector + " <= " + filterValues.iterator().next().toString();
				if(addNulls) {
					filterStatement += " OR " + selector + " IS NULL";
				}
			} else if (comparator.equals(AbstractTableDataFrame.Comparator.GREATER_THAN_EQUAL)) {
				filterStatement += selector + " < " + filterValues.iterator().next().toString();
				if(addNulls) {
					filterStatement += " OR " + selector + " IS NULL";
				}
			} else if (comparator.equals(AbstractTableDataFrame.Comparator.LESS_THAN)) {
				filterStatement += selector + " >= " + filterValues.iterator().next().toString();
				if(addNulls) {
					filterStatement += " OR " + selector + " IS NULL";
				}
			} else if (comparator.equals(AbstractTableDataFrame.Comparator.LESS_THAN_EQUAL)) {
				filterStatement += selector + " > " + filterValues.iterator().next().toString();
				if(addNulls) {
					filterStatement += " OR " + selector + " IS NULL";
				}
			} else if (comparator.equals(AbstractTableDataFrame.Comparator.IS_NOT_NULL)) {
				filterStatement += selector + " IS NULL ";
			} else if (comparator.equals(AbstractTableDataFrame.Comparator.IS_NULL)) {
				filterStatement += selector + " IS NOT NULL ";
			}
			i++;
		}

		Map<String, Map<Comparator, Set<Object>>> copyHash = copyFilterHash(filterHash);
		copyHash.remove(selector);
		String additionalFilters = makeFilterSubQuery(copyHash);
		if(additionalFilters.length() > 0) {
			additionalFilters = additionalFilters.replace("WHERE", "AND");
		}
		
				
		selectStatement += " WHERE " + filterStatement+" "+additionalFilters;

		return selectStatement;
	}
	
	private Map<String, Map<AbstractTableDataFrame.Comparator, Set<Object>>> copyFilterHash(Map<String, Map<AbstractTableDataFrame.Comparator, Set<Object>>> filterHash) {
	
		Map<String, Map<AbstractTableDataFrame.Comparator, Set<Object>>> copy = new HashMap<>();
		for(String key : filterHash.keySet()) {
			copy.put(key, filterHash.get(key));
		}
		return copy;
	}

	private String getQueryStringList(List<Object> values) {
		String listString = "(";
 
		for (int i = 0; i < values.size(); i++) {
			Object value = values.get(i);
			value = RdbmsFrameUtility.cleanInstance(value.toString());
			listString += "'" + value + "'";
			if (i < values.size() - 1) {
				listString += ", ";
			}
		}

		listString += ")";
		return listString;
	}

	private String getQueryStringList(Set<Object> values) {
		String listString = "(";

		Iterator<Object> iterator = values.iterator();
		int i = 0;
		while (iterator.hasNext()) {
			Object value = iterator.next();
			value = RdbmsFrameUtility.cleanInstance(value.toString());
			listString += "'" + value + "'";
			if (i < values.size() - 1) {
				listString += ", ";
			}
			i++;
		}

		listString += ")";
		return listString;
	}

	/***************************
	 * END QUERY BUILDERS
	 **************************************/

	// use this when result set is not expected back
	protected void runQuery(String query) throws Exception {
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
		query = query.toUpperCase().trim();
		if (checkQuery(query)) {
			runQuery(query);
		} else {
			throw new IllegalArgumentException("Can only run 'create view/create or replace view/drop view' queries externally");
		}
	}
	
	private boolean checkQuery(String query) {
//		return query.startsWith("CREATE ") || query.startsWith("CREATE OR REPLACE VIEW ") || query.startsWith("DROP VIEW ");
		return false;
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
			String newTable = getNewTableName();
			String createQuery = "CREATE TABLE " + newTable + " AS " + RdbmsQueryBuilder.makeSelect(tableName, selectors, false);
			runQuery(createQuery);
			String saveScript = "SCRIPT TO '" + fileName + "' COMPRESSION GZIP TABLE " + newTable;
			runQuery(saveScript);

			props.setProperty("tableName", newTable);

			Gson gson = new Gson();
			props.setProperty("filterHash", gson.toJson(filterHash2));

			String dropQuery = RdbmsQueryBuilder.makeDropTable(newTable);
			runQuery(dropQuery);

			props.setProperty("inMemDb", this.isInMem + "");

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

		String tempTableName = null;
		String isInMemStr = null;

		if (prop != null) {
			tempTableName = prop.getProperty("tableName");
			if (tempTableName == null) {
				tempTableName = H2Builder.tempTable;
			}
			// determine if the cache should be loaded in mem or on disk
			isInMemStr = prop.getProperty("inMemDb");
			if (isInMemStr != null) {
				boolean isInMemBool = Boolean.parseBoolean(isInMemStr.trim());
				if (!isInMemBool) {
					convertFromInMemToPhysical(null);
				}
				this.isInMem = isInMemBool;
			}
		} else {
			// this is for things that are old do not have the props file
			tempTableName = H2Builder.tempTable;
		}
		// get the open sql script
		String openScript = "RUNSCRIPT FROM '" + fileName + "' COMPRESSION GZIP ";
		// get an alter table name sql
		String createQuery = "ALTER TABLE " + tempTableName + " RENAME TO " + tableName;
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
						setFilters(columnName, list, AbstractTableDataFrame.Comparator.valueOf(filterList.getKey()));
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

	public int getNumRecords() {
		String query = "SELECT COUNT(*) * " + getHeaders(this.tableName).length + " FROM " + this.tableName;
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
	
	public int getNumRows() {
		String query = "SELECT COUNT(*) FROM " + getReadTable();
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
		return this.tableName;
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
				// TODO Auto-generated catch block
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
			ResultSet rs = executeQuery(query);
			try {
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
	
	private void addEmptyDerivedColumns() {
		// make query
		if (extraColumn.size() == 0) {
			populateExtraColumns();
			for (String column : extraColumn) {
				String addColumnQuery = "ALTER TABLE " + tableName + " ADD " + column + " double";
				try {
					runQuery(addColumnQuery);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	private void populateExtraColumns() {
		if (this.extraColumn == null) {
			this.extraColumn = new ArrayList<String>();
		}

		for (int i = columnCount; i < 10; i++) {
			String nextColumn = extraColumnBase + "_" + i;
			extraColumn.add(nextColumn);
		}
	}

	private String getExtraColumn() {
		if (extraColumn.size() > 0)
			return extraColumn.remove(0);
		else
			return null;
	}
	
	/**
	 * 
	 * @param column
	 *            - the column to join on
	 * @param newColumnName
	 *            - new column name with group by values
	 * @param valueColumn
	 *            - the column to do calculations on
	 * @param mathType
	 *            - the type of group by
	 * @param headers
	 */
	public void processGroupBy(String column, String newColumnName, String valueColumn, String mathType, String[] headers) {

		// String[] tableHeaders = getHeaders(tableName);
		// String inserter = makeGroupBy(column, valueColumn, mathType,
		// newColumnName, this.tableName, headers);
		// processAlterAsNewTable(inserter, Join.LEFT_OUTER.getName(),
		// tableHeaders);

		String reservedColumn = getExtraColumn();
		if (reservedColumn == null) {
			addEmptyDerivedColumns();
			reservedColumn = getExtraColumn();
		}
		renameColumn(reservedColumn, newColumnName);

		// String[] tableHeaders = getHeaders(tableName);
		String inserter = makeGroupBy(column, valueColumn, mathType, newColumnName, this.tableName, headers);
		try {
			ResultSet groupBySet = executeQuery(inserter);
			while (groupBySet.next()) {
				Object[] values = { groupBySet.getObject(column), groupBySet.getObject(newColumnName) };
				String[] columnHeaders = { column, newColumnName };
				updateTable(headers, values, columnHeaders);

				// String updateQuery = makeUpdate(tableName, column,
				// newColumnName, groupBySet.getObject(column),
				// groupBySet.getObject(newColumnName));
				// runQuery(updateQuery);
			}
		} catch (Exception e) {

		}
		// makeUpdate(mathType, joinColumn, newColumn, joinValue, newValue)
		// processAlterAsNewTable(inserter, Join.LEFT_OUTER.getName(),
		// tableHeaders);

		// all group bys are doubles?
		// addHeader(newColumnName, "double", tableHeaders);
		// addType("double");
	}

	// process a group by - calculate then make a table then merge the table
	public void processGroupBy(String[] column, String newColumnName, String valueColumn, String mathType,
			String[] headers) {
		String tableName = getTableName();
		if (column.length == 1) {
			processGroupBy(column[0], newColumnName, valueColumn, mathType, headers);
			return;
		}
		// String[] tableHeaders = getHeaders(tableName);
		// String inserter = makeGroupBy(column, valueColumn, mathType,
		// newColumnName, this.tableName, headers);
		// processAlterAsNewTable(inserter, Join.LEFT_OUTER.getName(),
		// tableHeaders);

		// all group bys are doubles?
		String reservedColumn = getExtraColumn();
		if (reservedColumn == null) {
			addEmptyDerivedColumns();
			reservedColumn = getExtraColumn();
		}
		renameColumn(reservedColumn, newColumnName);

		// String[] tableHeaders = getHeaders(tableName);
		String inserter = makeGroupBy(column, valueColumn, mathType, newColumnName, tableName, headers);
		try {
			ResultSet groupBySet = executeQuery(inserter);
			while (groupBySet.next()) {
				List<Object> values = new ArrayList<>();
				List<Object> columnHeaders = new ArrayList<>();
				for (String c : column) {
					values.add(groupBySet.getObject(c));// {groupBySet.getObject(column),
														// groupBySet.getObject(newColumnName)};
					columnHeaders.add(c);
				}
				values.add(groupBySet.getObject(newColumnName));
				columnHeaders.add(newColumnName);
				updateTable(headers, values.toArray(), columnHeaders.toArray(new String[] {}));

				// String updateQuery = makeUpdate(tableName, column,
				// newColumnName, groupBySet.getObject(column),
				// groupBySet.getObject(newColumnName));
				// runQuery(updateQuery);
			}
		} catch (Exception e) {

		}
	}
	
	private String makeGroupBy(String column, String valueColumn, String mathType, String alias, String tableName, String[] headers) {

		column = cleanHeader(column);
		valueColumn = cleanHeader(valueColumn);
		alias = cleanHeader(alias);

		String functionString = "";

		String type = getType(tableName, column);

		switch (mathType.toUpperCase()) {
		case "COUNT": {
			String func = "COUNT(";
			if (type.toUpperCase().startsWith("VARCHAR"))
				func = "COUNT( DISTINCT ";
			functionString = func + valueColumn + ")";
			break;
		}
		case "AVERAGE": {
			functionString = "AVG(" + valueColumn + ")";
			break;
		}
		case "MIN": {
			functionString = "MIN(" + valueColumn + ")";
			break;
		}
		case "MAX": {
			functionString = "MAX(" + valueColumn + ")";
			break;
		}
		case "SUM": {
			functionString = "SUM(" + valueColumn + ")";
			break;
		}
		default: {
			String func = "COUNT(";
			if (type.toUpperCase().startsWith("VARCHAR"))
				func = "COUNT( DISTINCT ";
			functionString = func + valueColumn + ")";
			break;
		}
		}

		// String filterSubQuery = makeFilterSubQuery(this.filterHash,
		// this.filterComparator);
		String filterSubQuery = makeFilterSubQuery();
		String groupByStatement = "SELECT " + column + ", " + functionString + " AS " + alias + " FROM " + tableName
				+ filterSubQuery + " GROUP BY " + column;

		return groupByStatement;
	}

	// TODO : don't assume a double group by here
	private String makeGroupBy(String[] column, String valueColumn, String mathType, String alias, String tableName, String[] headers) {
		if (column.length == 1)
			return makeGroupBy(column[0], valueColumn, mathType, alias, tableName, headers);
		String column1 = cleanHeader(column[0]);
		String column2 = cleanHeader(column[1]);
		valueColumn = cleanHeader(valueColumn);
		alias = cleanHeader(alias);

		String functionString = "";

		String type = getType(tableName, valueColumn);

		switch (mathType.toUpperCase()) {
		case "COUNT": {
			String func = "COUNT(";
			if (type.toUpperCase().startsWith("VARCHAR"))
				func = "COUNT( DISTINCT ";
			functionString = func + valueColumn + ")";
			break;
		}
		case "AVERAGE": {
			functionString = "AVG(" + valueColumn + ")";
			break;
		}
		case "MIN": {
			functionString = "MIN(" + valueColumn + ")";
			break;
		}
		case "MAX": {
			functionString = "MAX(" + valueColumn + ")";
			break;
		}
		case "SUM": {
			functionString = "SUM(" + valueColumn + ")";
			break;
		}
		default: {
			String func = "COUNT(";
			if (type.toUpperCase().startsWith("VARCHAR"))
				func = "COUNT( DISTINCT ";
			functionString = func + valueColumn + ")";
			break;
		}
		}

		// String filterSubQuery = makeFilterSubQuery(this.filterHash,
		// this.filterComparator);
		String filterSubQuery = makeFilterSubQuery();
		String groupByStatement = "SELECT " + column1 + ", " + column2 + ", " + functionString + " AS " + alias
				+ " FROM " + tableName + filterSubQuery + " GROUP BY " + column1 + ", " + column2;

		return groupByStatement;
	}
	
	private String getType(String tableName, String column) {
		String type = null;
		String typeQuery = "SELECT TABLE_NAME, COLUMN_NAME, TYPE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '"
				+ tableName.toUpperCase() + "' AND COLUMN_NAME = '" + column.toUpperCase() + "'";
		ResultSet rs = executeQuery(typeQuery);
		try {
			if (rs.next()) {
				type = rs.getString("TYPE_NAME");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return type;
	}

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
		ResultSet rs = executeQuery(query);
		try {
			while(rs.next()) {
				String colName = rs.getString(1).toUpperCase();
				String dataType = rs.getString(2).toUpperCase();
				dataTypeMap.put(colName, dataType);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		if(dataTypeMap.isEmpty()) {
			throw new IllegalArgumentException("Table name " + tableName + " does not exist or is empty");
		}
		
		this.tableName = tableName;
		return dataTypeMap;
	}

	public void changeDataType(String tableName, String columnName, String newType) {
		String query = "ALTER TABLE " + tableName + " MODIFY COLUMN " + columnName + " " + this.typeConversionMap.get(newType);
		Connection thisCon = getConnection();
		Statement stat = null;
		try {
			stat = thisCon.createStatement();
			stat.execute(query);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if(stat != null) {
				try {
					stat.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/***************************
	 * ORIGINAL UNUSED CODE
	 **************************************/

	//////////////////////////////////////////////////////////////////////////////////////////
	/*
	 * There are a bunch of methods that are not used in this class... I have no
	 * idea what to do with them I am just going to have them commented out
	 * here... hopefully we can delete these at some point
	 */

	// private String makeCreate(String tableName, String subquery) {
	// String createQuery = "CREATE TABLE "+tableName+" AS " + subquery;
	// return createQuery;
	// }
	// //build the query to create a new tinker table given the types with
	// default values
	// private String makeTemplate(String[] headers, String[] types,
	// Hashtable<String, String> defaultValues)
	// {
	//
	// //building query
	// StringBuilder inserter = new StringBuilder("INSERT INTO "+tableName+"("
	// );
	// StringBuilder template = new StringBuilder("(");
	//
	// for(int colIndex = 0;colIndex < headers.length;colIndex++)
	// {
	// // I need to find the type and based on that adjust what I want
	// String type = types[colIndex];
	//
	// // if null on integer - empty
	// // if null on string - ''
	// // if currenncy - empty
	// // if date - empty
	//
	// String name = headers[colIndex];
	// StringBuilder thisTemplate = new StringBuilder(name);
	//
	//
	// if(type.equalsIgnoreCase("int") || type.equalsIgnoreCase("double") ||
	// type.equalsIgnoreCase("date"))
	// {
	// if(!defaultValues.containsKey(type))
	// thisTemplate = new StringBuilder("<" + thisTemplate + "; null=\"\">");
	// else
	// thisTemplate = new StringBuilder("<" + thisTemplate + "; null=\"" +
	// defaultValues.get(type) + "\">");
	// }
	// else
	// {
	// if(!defaultValues.containsKey(type))
	// thisTemplate = new StringBuilder("'<" + thisTemplate + ";
	// null=\"''\">'");
	// else
	// thisTemplate = new StringBuilder("'<" + thisTemplate + "; null=\"" +
	// defaultValues.get(type) + "\">'");
	//
	// }
	// if(colIndex == 0)
	// {
	// inserter = new StringBuilder(inserter + "\"" + name + "\"");
	// template = new StringBuilder(template + "" + thisTemplate);
	// }
	// else
	// {
	// inserter = new StringBuilder(inserter + " , \"" + name + "\"");
	// template = new StringBuilder(template + " , " + thisTemplate);
	// }
	// }
	//
	// inserter = new StringBuilder(inserter + ") VALUES ");
	// template = new StringBuilder(inserter + "" + template + ")");
	//
	// //System.out.println(".. Template.. " + template);
	//
	//
	// return template.toString();
	// }
	// private boolean isOne2Many(String fileName, Vector <String> oldHeaders)
	// {
	// // I need to compare one by one to see there are many
	// // or should I just make it into a separate table and do a cartesian
	// product ?
	//
	// // Idea is really simple.. I see everything that is there and then try to
	// see if there are more than one record of the same kind
	// boolean retValue = false;
	// Hashtable <String, Integer> valueCounter = new Hashtable<String,
	// Integer>();
	//
	// CsvParserSettings settings = new CsvParserSettings();
	// settings.setNullValue("");
	//
	// settings.setEmptyValue(""); // for CSV only
	// settings.setSkipEmptyLines(true);
	//
	// CsvParser parser = new CsvParser(settings);
	//
	//
	// try {
	// RandomAccessFile thisReader = new RandomAccessFile(fileName, "r");
	//
	// String nextString = null;
	// while((nextString = thisReader.readLine()) != null && !retValue)
	// {
	// String [] cells = parser.parseLine(nextString);
	// String composite = "";
	// for(int oldIndex = 0;oldIndex < oldHeaders.size();oldIndex++)
	// {
	// composite = composite + "_" +
	// cells[Integer.parseInt(oldHeaders.elementAt(oldIndex))];
	// }
	// // now check if this is there already
	// if(!valueCounter.containsKey(composite))
	// valueCounter.put(composite, 1);
	// else
	// retValue = true;
	// }
	// } catch (NumberFormatException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// } catch (IOException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// return retValue;
	// }
	//
	// //turn the wrapper data to double array
	// private String[][] getData(ISelectWrapper wrapper) {
	// List<String[]> data = new ArrayList<String[]>();
	// String[] colHeaders = wrapper.getDisplayVariables();
	// colHeaders = cleanHeaders(colHeaders);
	// int length = colHeaders.length;
	// while(wrapper.hasNext()) {
	// String[] rowData = new String[length];
	// ISelectStatement statement = wrapper.next();
	// Map<String, Object> nextRow = statement.getPropHash();
	// for(int i = 0; i < length; i++) {
	// rowData[i] = nextRow.get(colHeaders[i]).toString();
	// }
	// data.add(rowData);
	// }
	//
	// return data.toArray(new String[][]{});
	// }
	//
	// private String makeUpdate(String tableName, String joinColumn, String
	// newColumn, Object joinValue, Object newValue) {
	// joinValue = cleanInstance(joinValue.toString());
	// newValue = cleanInstance(newValue.toString());
	// String updateQuery = "UPDATE "+tableName+" SET
	// "+newColumn+"="+"'"+newValue+"'"+" WHERE
	// "+joinColumn+"="+"'"+joinValue+"'";
	// return updateQuery;
	// }
	//
	// //generate a query to update a table
	// private String makeUpdate(Vector<String> newHeaders, Vector<String>
	// oldHeaders, String[] types, String[] values, int oldCount,
	// Hashtable<String, String> defaultValues, String[] headers)
	// {
	// oldCount = 0;
	// StringBuilder inserter = new StringBuilder("UPDATE " + tableName + " SET
	// " );
	//
	// StringBuilder sets = new StringBuilder(" ");
	// StringBuilder wheres = new StringBuilder(" WHERE ");
	//
	// // first get all of the new headers
	// for(int newIndex = 0;newIndex < newHeaders.size();newIndex++)
	// {
	// int id = Integer.parseInt(newHeaders.get(newIndex));
	// //id = id - oldCount;
	// String type = types[id];
	// String value = null;
	// if(type.equalsIgnoreCase("int") || type.equalsIgnoreCase("double"))
	// {
	// value = values[id-oldCount];
	// if(value == null || value.length() == 0)
	// value = "null";
	// }
	// else
	// {
	// value = values[id-oldCount];
	// value = value.replace("'", "''");
	// value = value.replace("\"", "");
	// value = "'" + value + "'";
	// }
	//
	// // values
	// if(newIndex == 0)
	// sets = new StringBuilder(headers[id] + " = " + value);
	// else
	// sets = new StringBuilder(sets + ", " + headers[id] + " = " + value);
	// }
	//
	// for(int newIndex = 0;newIndex < oldHeaders.size();newIndex++)
	// {
	// int id = Integer.parseInt(oldHeaders.get(newIndex));
	// //id = id + oldCount;
	// String type = types[id];
	// String value = null;
	// if(type.equalsIgnoreCase("int") || type.equalsIgnoreCase("double"))
	// {
	// value = values[id];
	// if(value == null || value.length() == 0)
	// value = "null";
	// }
	// else
	// {
	// value = values[id];
	// value = value.replace("'", "''");
	// value = value.replace("\"", "");
	// value = "'" + value + "'";
	// }
	//
	// if(newIndex == 0)
	// wheres = new StringBuilder(wheres + headers[id] + " = " + value);
	// else
	// wheres = new StringBuilder(wheres + ", " + headers[id] + " = " + value);
	// }
	//
	// // and the final
	// inserter = new StringBuilder(inserter + sets.toString() +
	// wheres.toString());
	//
	// return inserter.toString();
	// }

	// //make a query to alter a table (add headers)
	// private String makeAlter(Vector<String> newHeaders, String tableName)
	// {
	// createString = "ALTER TABLE " + tableName + " ADD (";
	//
	// for(int headerIndex = 0;headerIndex < newHeaders.size();headerIndex++)
	// {
	// int newIndex = Integer.parseInt(newHeaders.elementAt(headerIndex)); //
	// get the index you want
	// String header = headers[newIndex]; // this is a new header - cool
	//
	// if(headerIndex == 0)
	// createString = createString + header + " " + types[newIndex];
	// else
	// createString = createString + ", " + header + " " + types[newIndex];
	// }
	//
	// //this.headers = capHeaders;
	// createString = createString + ")";
	//
	// return createString;
	// }

	// make a query to alter a table (add headers)
	// private String makeAlter(String[] newHeaders, String tableName, String[]
	// headers, String[] types)
	// {
	// String createString = "ALTER TABLE " + tableName + " ADD (";
	//
	// for(int headerIndex = 0;headerIndex < newHeaders.length;headerIndex++)
	// {
	//// int newIndex = Integer.parseInt(newHeaders[headerIndex]); // get the
	// index you want
	// int newIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(headers,
	// newHeaders[headerIndex]);
	// String header = headers[newIndex]; // this is a new header - cool
	//
	// if(headerIndex == 0)
	// createString = createString + header + " " + types[newIndex];
	// else
	// createString = createString + ", " + header + " " + types[newIndex];
	// }
	//
	// //this.headers = capHeaders;
	// createString = createString + ")";
	//
	// return createString;
	// }

	// private void processAlterAsNewTable(String selectQuery, String joinType,
	// String[] headers)
	// {
	// try {
	// // I need to get the names and types here
	// // and then do the same magic as before
	//
	// ResultSet rs = executeQuery(selectQuery);
	//
	// String [] oldHeaders = headers;
	//// String [] oldTypes = types;
	//
	//
	// int numCols = rs.getMetaData().getColumnCount();
	// String [] newHeaders = new String[numCols];
	// String [] newTypes = new String[numCols];
	//
	// ResultSetMetaData rsmd = rs.getMetaData();
	// for(int colIndex = 1;colIndex <= numCols;colIndex++)
	// {
	// // set the name and type
	// int arrIndex = colIndex - 1;
	// newHeaders[arrIndex] = rsmd.getColumnLabel(colIndex);
	// newTypes[arrIndex] = rsmd.getColumnTypeName(colIndex);
	// }
	//
	// Hashtable <Integer, Integer> matchers = new Hashtable <Integer,
	// Integer>();
	// String oldTable = tableName;
	// String newTable = H2FRAME + getNextNumber() ;
	// // now I need a creator
	// String creator = "Create table " + newTable+ " AS " + selectQuery;
	// getConnection().createStatement().execute(creator);
	//
	// // find the matchers
	// // I need to find which ones are already there and which ones are new
	// for(int hIndex = 0;hIndex < newHeaders.length;hIndex++)
	// {
	// String uheader = newHeaders[hIndex];
	// uheader = cleanHeader(uheader);
	//
	// boolean old = false;
	// for(int oIndex = 0;oIndex < headers.length;oIndex++)
	// {
	// if(headers[oIndex].equalsIgnoreCase(uheader))
	// {
	// old = true;
	// matchers.put(hIndex, oIndex);
	// break;
	// }
	// }
	// }
	//
	// mergeTables(oldTable, newTable, matchers, oldHeaders, newHeaders,
	// joinType);
	//
	// } catch (SQLException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// }

	// /**
	// *
	// * @param query - H2 query
	// * @return
	// *
	// * returns an iterator based on the query parameter, returns null for an
	// invalid query or if exception occurs running the query
	// */
	// public Iterator buildIterator(String query){
	// try {
	// Statement stmt = getConnection().createStatement();
	// ResultSet rs = stmt.executeQuery(query);
	// return new H2Iterator(rs);
	// } catch(SQLException s) {
	// s.printStackTrace();
	// }
	//
	// return null;
	// }

	// /**
	// *
	// * @param thisOutput - row to predict types on
	// * @return
	// *
	// * Ex:
	// * thisOutput = ["0.1", "Value", "1"] will return ["Double",
	// "VarChar(800)", "Double"]
	// */
	// public String[] predictRowTypes(String[] thisOutput)
	// {
	// String[] types = new String[thisOutput.length];
	//
	// for(int outIndex = 0;outIndex < thisOutput.length;outIndex++) {
	// String curOutput = thisOutput[outIndex];
	// Object [] cast = castToType(curOutput);
	// if(cast == null) {
	// cast = new Object[2];
	// cast[0] = types[outIndex];
	// cast[1] = ""; // make it into an empty String
	// }
	//
	// types[outIndex] = cast[0] + "";
	// }
	//
	// return types;
	// }

	// /**
	// * get all data from the table given the columnheaders as selectors
	// *
	// * @param columnHeaders
	// * @return
	// */
	// public List<Object[]> getData(List<String> columnHeaders) {
	//
	// columnHeaders = cleanHeaders(columnHeaders);
	// List<Object[]> data;
	// try {
	// String selectQuery = makeSelect(tableName, columnHeaders);
	// ResultSet rs = executeQuery(selectQuery);
	//
	// if(rs != null) {
	//
	// ResultSetMetaData rsmd = rs.getMetaData();
	// int NumOfCol = rsmd.getColumnCount();
	// data = new ArrayList<>(NumOfCol);
	// //collect and return
	// while (rs.next()){
	// Object[] row = new Object[NumOfCol];
	//
	// for(int i = 1; i <= NumOfCol; i++) {
	// row[i-1] = rs.getObject(i);
	// }
	// data.add(row);
	// }
	//
	// return data;
	// }
	//
	// } catch (SQLException e) {
	// e.printStackTrace();
	// }
	//
	// return new ArrayList<Object[]>(0);
	// }

	// //get all rows that have value = value in column header = column
	// /**
	// * Returns all data from the table with the given value for a specific
	// column
	// * Ex:
	// * columnHeaders = [Title, Studio]
	// * column = Studio
	// * value = WB
	// *
	// * All titles and studios will be returned such that the studio is 'WB'
	// *
	// * @param columnHeaders
	// * @param column
	// * @param value
	// * @return
	// */
	// public List<Object[]> getData(List<String> columnHeaders, String column,
	// Object value) {
	//
	// List<Object[]> data;
	// column = cleanHeader(column);
	// columnHeaders = cleanHeaders(columnHeaders);
	// try {
	// String selectQuery = makeSpecificSelect(tableName, columnHeaders, column,
	// value);
	// ResultSet rs = executeQuery(selectQuery);
	//
	// if(rs != null) {
	//
	// ResultSetMetaData rsmd = rs.getMetaData();
	// int NumOfCol = rsmd.getColumnCount();
	// data = new ArrayList<>(NumOfCol);
	// while (rs.next()){
	// Object[] row = new Object[NumOfCol];
	//
	// for(int i = 1; i <= NumOfCol; i++) {
	// row[i-1] = rs.getObject(i);
	// }
	// data.add(row);
	// }
	//
	// return data;
	// }
	//
	// } catch (SQLException e) {
	// e.printStackTrace();
	// }
	//
	// return new ArrayList<Object[]>(0);
	// }
	
	// public String[] getTypes(String tableName) {
	// List<String> headers = new ArrayList<String>();
	//
	// String columnQuery = "SHOW COLUMNS FROM "+tableName;
	// ResultSet rs = executeQuery(columnQuery);
	// try {
	// while(rs.next()) {
	// String header = rs.getString("TYPE");
	// headers.add(header);
	// }
	// } catch (SQLException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// return headers.toArray(new String[]{});
	// }

	// public Map<String, String> getHeadersAndTypes(String tableName) {
	// Map<String, String> typeMap = new HashMap<>();
	// String columnQuery = "SHOW COLUMNS FROM "+tableName;
	// ResultSet rs = executeQuery(columnQuery);
	// try {
	// while(rs.next()) {
	// String header = rs.getString("FIELD");
	// String type = rs.getString("TYPE");
	// typeMap.put(header, type);
	// }
	// } catch (SQLException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// return typeMap;
	// }

	// process to join the data and new headers to existing table
	// private void processAlterData(String[][] data, String[] newHeaders,
	// String[] headers, Join joinType)
	// {
	// // this currently doesnt handle many to many joins and such
	// String[] types = null;
	// try {
	// getConnection();
	//
	// // I need to do an evaluation here to find if this one to many
	// String [] oldHeaders = headers;
	//
	// //headers for the joining table
	//
	//// int curHeadCount = headers.length;
	// Vector <String> newHeaderIndices = new Vector<String>();
	// Vector <String> oldHeaderIndices = new Vector<String>();
	// Hashtable<Integer, Integer> matchers = new Hashtable<Integer, Integer>();
	//// if(matchers == null || matchers.isEmpty()) {
	//// matchers = new Hashtable<Integer, Integer>();
	//
	// // I need to find which ones are already there and which ones are new
	// for(int hIndex = 0;hIndex < newHeaders.length;hIndex++)
	// {
	// String uheader = newHeaders[hIndex];
	// uheader = cleanHeader(uheader);
	//
	// boolean old = false;
	// for(int oIndex = 0;oIndex < headers.length;oIndex++)
	// {
	// if(headers[oIndex].equalsIgnoreCase(uheader))
	// {
	// old = true;
	// oldHeaderIndices.add(hIndex+"");
	// matchers.put(hIndex, oIndex);
	// break;
	// }
	// }
	//
	// if(!old)
	// newHeaderIndices.add((hIndex) + "");
	// }
	//// }
	//
	//// headers = newHeaders;
	//
	//// String [] oldTypes = types; // I am not sure if I need this we will see
	// - yes I do now yipee
	//
	//// String[] cells = data[0];
	//// predictRowTypes(cells);
	//
	//// String[] newTypes = types;
	//
	// //stream.close();
	//
	// boolean one2Many = false;
	// // I also need to accomodate when there are no common ones
	//// if(oldHeaderIndices.size() == 0)
	// if(matchers.isEmpty())
	// {
	// // this is the case where it has not been created yet
	// tableName = getNewTableName();
	// generateTable(data, newHeaders, tableName);
	// }
	// else
	// {
	// // close the stream and open it again
	//
	// // do a different process here
	// // I need to create a new database here
	// one2Many = true;
	// generateTable(data, newHeaders, H2FRAME + getNextNumber());
	// // not bad -- this does it ?
	// // whoa hang on I need to handle the older headers and such
	// // done in reset headers
	// }
	//
	// // need to create a new database with everything now
	// // reset all the headers
	//// if(one2Many)
	//// resetHeaders(oldHeaders, headers, newHeaderIndices, oldTypes, types,
	// curHeadCount);
	// // just test to see if it finally came through
	//
	// // now I need to assimilate everything into one
	// String tableName2 = H2FRAME + tableRunNumber;
	// String tableName1 = tableName;
	//
	// if(one2Many)
	// mergeTables(tableName1, tableName2, matchers, oldHeaders, newHeaders,
	// joinType.getName());
	//
	// testData();
	//
	// } catch (SQLException e) {
	// e.printStackTrace();
	// }
	// }
	//
}
