package prerna.ds.H2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.sql.Connection;
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

import prerna.algorithm.api.IMetaData;
import prerna.ds.TinkerFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.poi.main.RDBMSEngineCreationHelper;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class H2Builder {

	private static final Logger LOGGER = LogManager.getLogger(H2Builder.class.getName());

	Vector <String> castTargets = new Vector<String>();
	Connection conn = null;
	private String schema = "test"; // assign a default schema which is test
	//	boolean create = false;
//	static int tableRunNumber = 1;
	static int rowCount = 0;
	private static final String tempTable = "TEMP_TABLE98793";
	static final String H2FRAME = "H2FRAME";
	String brokenLines = "";
	String options = ":LOG=0;CACHE_SIZE=65536;LOCK_MODE=1;UNDO_LOG=0";
	Server server = null;
	String serverURL = null;
	Hashtable <String, String[]> tablePermissions = new Hashtable <String, String []>();
	
	// for writing the frame on disk
	// currently not used
	// would require reconsideration of dashboard, etc. 
	// since frames are not in same schema
	private boolean isInMem = true;
	private final int MAX_IN_MEM_INSERT = 100_000;
	
	//Provides a translation for incoming types into something H2 can understand
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

	//consists of which values to keep for each header when gathering data
	//Ex: Studio -> {EQUALS -> [WB, Universal]}
	// 		we want to focus on only Studios which are WB and Universal
	//	RottenTomatoesCritics -> {LESS_THAN -> [0.9], GREATER_THAN -> [0.8]}
	//		we want to focus on RottenTomatoesCritics < 0.9 AND > 0.8
	Map<String, Map<Comparator, Set<Object>>> filterHash2 = new HashMap<>();

	//name of the main table for H2
	String tableName; 

	//use to determine if we are joined to another viz or not
	//all Reads will be conducted on the view when this is true, all Creates, Updates, and Deletes will affect only the mainTable
	private boolean joinMode;
	private String viewTableName;
	Hashtable<String, String> joinColumnTranslation;
	
	H2Joiner joiner;
	
	//specifies the join types for an H2 frame
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


	//types of comparators
	public enum Comparator {
		EQUAL, LESS_THAN, GREATER_THAN, LESS_THAN_EQUAL, GREATER_THAN_EQUAL, NOT_EQUAL;
	}


	//These were used for adding extra numerical columns to tables so that they could be used when adding a numerical column such as a group by
	//this process is no longer used since control of adding columns is performed in the same place as meta data which sits outside of this class
	private static final String extraColumnBase = "ExtraColumn92917289";
	private static int columnCount = 0;
	List<String> extraColumn = new ArrayList<String>();

	/*************************** TEST **********************************************/
	public static void main(String[] a) throws Exception {


		//		concurrencyTest();

		new H2Builder().testMakeFilter();
		//    	H2Builder test = new H2Builder();
		//    	test.castToType("6/2/2015");
		//		String fileName = "C:/Users/rluthar/Desktop/datasets/Movie.csv";
		//		long before, after;
		//		fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/Remedy New.csv";
		//		//fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/Movie.csv";
		//
		//		/******* Used Primarily for Streaming *******/
		//		/**
		//		long before = System.nanoTime();
		//test.processFile(fileName);
		//		long after = System.nanoTime();			
		//		System.out.println("Time Taken.. the usual way..  " + (after - before) / 1000000);
		//		**/
		//		
		//		before = System.nanoTime();
		//		test.processCreateData(fileName);
		//		after = System.nanoTime();
		//		System.out.println("Time Taken.. Univocity..  " + (after - before) / 1000000);
		//
		//		fileName = "C:/Users/rluthar/Desktop/datasets/Actor.csv";
		//		//before = System.nanoTime();
		//		test.processAlterData(fileName);
		//		after = System.nanoTime();
		//		System.out.println("Time Taken.. Univocity..  " + (after - before) / 1000000);
		//		
		//		
		//		
		//		/*
		//		 * These 2 are invalid.. it requires significant cleanup
		//		before = System.nanoTime();
		//		test.loadCSV(fileName);
		//		after = System.nanoTime();
		//		System.out.println("Time Taken.. H2..  " + (after - before) / 1000000);
		//
		//		before = System.nanoTime();
		//		test.processCreateDataH2(fileName);
		//		after = System.nanoTime();
		//		System.out.println("Time Taken.. H2..  Types " + (after - before) / 1000000);
		//		*/
		//		//
		//    	test.predictTypes("");
		//    	//String [] headers = {"A", "b", "d", "e", "f", "g", "h"};
		//    	//test.predictRowTypes("1.0, 2, \"$123,33.22\", wwewewe, \"Hello, I am doing good\", hola, 9/12/2012");
	}

	private static void concurrencyTest() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:h2:mem:test:LOG=0;CACHE_SIZE=65536;LOCK_MODE=1;UNDO_LOG=0", "sa", "");

		int tableLength = 1000000;
		//create a table
		conn.createStatement().execute("CREATE TABLE TEST (COLUMN1 VARCHAR(800), COLUMN2 VARCHAR(800), COLUMN3 VARCHAR(800))");

		//put in a LOT of data
		for(int i = 1; i <= tableLength; i++) {
			conn.createStatement().execute("INSERT INTO TEST (COLUMN1, COLUMN2, COLUMN3) VALUES ('"+tableLength+"', '"+tableLength+"col2"+"', '"+tableLength+"col3')");
		}

		//alter the table (should take some time)
		conn.createStatement().execute("ALTER TABLE TEST ADD COLUMN4 VARCHAR(800)");

		//see if we try and update before alter finishes
		conn.createStatement().execute("UPDATE TEST SET COLUMN4 = 'THIS IS COLUMN 4' WHERE COLUMN1 = '"+tableLength+"'");

		System.out.println("Finished");
	}

	//Test method
	public void testDB() throws Exception
	{
		Class.forName("org.h2.Driver");
		Connection conn = DriverManager.
				getConnection("jdbc:h2:C:/Users/pkapaleeswaran/workspacej3/Exp/database", "sa", "");
		Statement stmt = conn.createStatement();
		String query = "select t.title, s.studio from title t, studio s where t.title = s.title_fk";
		query = "select t.title, concat(t.title,':', s.studio), s.studio from title t, studio s where t.title = s.title_fk";
		//ResultSet rs = stmt.executeQuery("SELECT * FROM TITLE");
		ResultSet rs = stmt.executeQuery(query);
		//		stmt.execute("CREATE TABLE MOVIES AS SELECT * From CSVREAD('../Movie.csv')");


		ResultSetMetaData rsmd = rs.getMetaData();
		int columnCount = rsmd.getColumnCount();

		ArrayList records = new ArrayList();
		System.err.println("Number of columns " + columnCount);

		while(rs.next())
		{
			Object [] data = new Object[columnCount];
			for(int colIndex = 0;colIndex < columnCount;colIndex++)
			{
				data[colIndex] = rs.getObject(colIndex+1);
				System.out.print(rsmd.getColumnType(colIndex+1));
				System.out.print(rsmd.getColumnName(colIndex+1)+ ":");
				System.out.print(rs.getString(colIndex+1));
				System.out.print(">>>>" + rsmd.getTableName(colIndex+1) + "<<<<");
			}
			System.out.println();
		}

		// add application code here
		conn.close();
	}

	//Test method
	public void predictTypes(String csv)
	{
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

	public void loadCSV(String fileName)
	{
		try {
			//String fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/consumer_complaints.csv";

			//fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/pregnancyS.csv";

			Class.forName("org.h2.Driver");
			Connection conn = DriverManager.
					getConnection("jdbc:h2:mem:test", "sa", "");
			Statement stmt = conn.createStatement();

			long now = System.nanoTime();				
			/*stmt.execute("Create table test("
					+ "Complaint_ID varchar(255), "
					+ "product varchar(255),"
					+ "sub_product varchar(255),"
					+ "issue varchar(255),"
					+ "sub_issue varchar(255),"
					+ "state varchar(10),"
					+ "zipcode int,"
					+ "submitted_via varchar(20),"
					+ "date_received date,"
					+ "date_sent date,"
					+ "company varchar(255),"
					+ "company_response varchar(255),"
					+ "timely_response varchar(255),"
					+ "consumer_disputed varchar(5))  as select * from csvread('" + fileName + "')");
			 */

			System.out.println("File Name ...  " + fileName);
			stmt.execute("Create table test as select * from csvread('" + fileName + "')");
			long graphTime = System.nanoTime();
			System.out.println("Time taken.. " + ((graphTime - now) / 1000000) + " milli secs");

			String query = "Select  bencat, count(*) from test where sex='F' Group By Bencat";

			ResultSet rs = stmt.executeQuery(query);


			while(rs.next())
			{
				System.out.print("Bencat.. " + rs.getObject(1));
				System.out.println("Count.. " + rs.getObject(2));
			}



			graphTime = System.nanoTime();

			stmt.execute("Alter table test add dummy varchar2(200)");

			long rightNow = System.nanoTime();				
			System.out.println("Update Time taken.. " + ((rightNow - graphTime) / 1000000) + "milli secs");

			graphTime = System.nanoTime();

			//stmt.execute("Update test set dummy='try' where bencat = 'ADFMLY' ");
			stmt.execute("Update test set dummy='try' where bencat = 'ADN' ");
			rightNow = System.nanoTime();				

			//stmt.execute("Delete from Test where State = 'TX'");
			System.out.println("Update Time taken.. " + ((rightNow - graphTime) / 1000000) + "milli secs");
			graphTime = rightNow;

			rightNow = System.nanoTime();				

			//rs = stmt.executeQuery("Select  count(State) from test where zipcode > 22000");
			/*rs = stmt.executeQuery("Select  sum(zipcode) from test where zipcode > 22000");

			while(rs.next())
			{
				System.out.println("Count.. " + rs.getObject(1));
			}
			System.out.println("Query Time taken.. " + ((rightNow - graphTime) / 1000000000) + " secs");
			 */

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void testData() throws SQLException
	{

		ResultSet rs = conn.createStatement().executeQuery("Select count(*) from " + tableName);
		while(rs.next())
			System.out.println("Inserted..  "  + rs.getInt(1));

		/*String query = "Select  bencat, count(*) from H2FRAME where sex='F' Group By Bencat";

		rs = conn.createStatement().executeQuery(query);


		while(rs.next())
		{
			System.out.print("Bencat.. " + rs.getObject(1));
			System.out.println("Count.. " + rs.getObject(2));
		}*/
	}

	public void testMakeFilter() {

		Set<Object> list1 = new HashSet<>();
		list1.add("value1");
		list1.add("value2");
		list1.add("value3");
		list1.add("value4");
		Map<Comparator, Set<Object>> innerMap = new HashMap<>();
		innerMap.put(Comparator.EQUAL, list1);
		filterHash2.put("column1", innerMap);

		Set<Object> list2 = new HashSet<>();
		list2.add("valueA");
		list2.add("valueB");
		list2.add("valueC");
		list2.add("valueD");
		Map<Comparator, Set<Object>> innerMap2 = new HashMap<>();
		innerMap2.put(Comparator.EQUAL, list2);
		filterHash2.put("column2", innerMap2);

		Set<Object> list3 = new HashSet<>();
		list3.add("value1A");
		list3.add("value2B");
		list3.add("value3C");
		list3.add("value4D");
		Map<Comparator, Set<Object>> innerMap3 = new HashMap<>();
		innerMap3.put(Comparator.NOT_EQUAL, list3);
		filterHash2.put("column3", innerMap3);

		Set<String> headers = new HashSet<>();
		headers.add("column1");
		headers.add("column2");
		headers.add("column3");


		//		String makeQuery = this.makeSelect("TestTable", headers);
		//		System.out.println(makeQuery);

	}
	/*************************** END TEST ******************************************/

	/*************************** CONSTRUCTORS **************************************/

	public H2Builder() {
		//    	//initialize a connection
		//    	getConnection();
		tableName = getNewTableName();
	}

	/*************************** END CONSTRUCTORS **********************************/
	
    /*************************** JOIN METHODS **********************************/
    protected void setView(String viewTable) {
    	this.viewTableName = viewTable;
    	this.joinMode = true;
    }
    
    protected void setView(String viewTable, Hashtable<String, String> translationMap) {
    	this.viewTableName = viewTable;
    	this.joinMode = true;
    	this.joinColumnTranslation = translationMap;
    }
    
    protected void setTranslationMap(Hashtable<String, String> translationMap) {
    	this.joinColumnTranslation = translationMap;
    }
    
    protected void addTranslation(String nameInTable, String nameInView) {
    	if(this.joinColumnTranslation == null) {
    		this.joinColumnTranslation = new Hashtable<>();
    	}
    	this.joinColumnTranslation.put(nameInTable, nameInView);
    }
    
    protected void unJoin() {
    	this.viewTableName = null;
    	this.joinMode = false;
    	this.joinColumnTranslation = null;
    }
    
    protected boolean getJoinMode() {
    	return this.joinMode;
    }
    
    protected List<String> translateColumns(List<String> columns) {
    	List<String> translatedColumns = new ArrayList<>();
    	for(String column : columns) {
    		translatedColumns.add(translateColumn(column));
    	}
    	return translatedColumns;
    }
    
    protected String[] translateColumns(String[] columns) {
    	String[] translatedColumns = new String[columns.length];
    	for(int i = 0; i < translatedColumns.length; i++) {
    		translatedColumns[i] = translateColumn(columns[i]);
    	}
    	return translatedColumns;
    }
    
    protected String translateColumn(String column) {
    	return joinColumnTranslation.get(column);
    }
    
    protected void setJoiner(H2Joiner joiner) {
    	this.joiner = joiner;
    }
    /*************************** END JOIN METHODS ********************************/


	private Object[] castToType(String input){
		return Utility.findTypes(input);
	}

	//get a new unique table name
	public String getNewTableName() {
		String name = H2FRAME + getNextNumber();
		return name; 
	}


	/*************************** CREATE ******************************************/

	/**
	 * Generates a new H2 table from the paramater data
	 * 
	 * Assumptions
	 * 		headers and types are of same length
	 * 		types are H2 readable
	 * 		
	 * 
	 * @param iterator - iterates over the data
	 * @param headers - headers for the table data
	 * @param types - data type for each column
	 * @param tableName
	 */
	private void generateTable(Iterator<IHeadersDataRow> iterator, String[] headers, String[] types, String tableName) {
		try {
			String createTable = makeCreate(tableName, headers, types);
			runQuery(createTable);

			PreparedStatement ps = createInsertPreparedStatement(tableName, headers);
			// keep a batch size so we dont get heapspace
			final int batchSize = 5000;
			int count = 0;

			// we loop through every row of the csv
			while( iterator.hasNext() ) {
				IHeadersDataRow headerRow = iterator.next();
				Object[] nextRow = headerRow.getValues();
				// we need to loop through every value and cast appropriately
				for(int colIndex = 0; colIndex < nextRow.length; colIndex++) {
					String type = types[colIndex];
					if(type.equalsIgnoreCase("DATE")) {
						java.util.Date value = Utility.getDateAsDateObj(nextRow[colIndex] + "");
						if(value != null) {
							ps.setDate(colIndex+1, new java.sql.Date(value.getTime()));
						}
					} else if(type.equalsIgnoreCase("DOUBLE") || type.equalsIgnoreCase("FLOAT")) {
						Double value = Utility.getDouble(nextRow[colIndex] + "");
						if(value != null) {
							ps.setDouble(colIndex+1, value);
						}
					} else {
						String value = nextRow[colIndex] + "";
						ps.setString(colIndex+1, value + "");
					}
				}
				// add it
				ps.addBatch();

				// batch commit based on size
				if(++count % batchSize == 0) {
					ps.executeBatch();
				}
			}

			// well, we are done looping through now
			ps.executeBatch(); // insert any remaining records
			ps.close();

			//			while(iterator.hasNext()) {
			//				IHeadersDataRow nextData = iterator.next();
			//				Object[] row = nextData.getValues();
			//				String[] stringRow = new String[row.length];
			//				for(int i = 0; i < row.length; i++) {
			//					stringRow[i] = row[i].toString();
			//				}
			//
			//				String[] cells = Utility.castToTypes(stringRow, types);
			//				String inserter = makeInsert(headers, types, cells, new Hashtable<String, String>(), tableName);
			//				runQuery(inserter);
			//			}

		} catch(Exception e) {

		}
	}

	/**
	 * Create a prepared statement in order to perform bulk inserts into a table
	 * @param TABLE_NAME			The name of the table
	 * @param columns				The columns that will be used in the inserting
	 * @return						The prepared statement
	 */
	private PreparedStatement createInsertPreparedStatement(final String TABLE_NAME, final String[] columns) {
		// generate the sql for the prepared statement
		StringBuilder sql = new StringBuilder("INSERT INTO ");
		sql.append(TABLE_NAME).append(" (").append(columns[0]);
		for(int colIndex = 1; colIndex < columns.length; colIndex++) {
			sql.append(", ");
			sql.append(columns[colIndex]);
		}
		sql.append(") VALUES (?"); // remember, we already assumed one col
		for(int colIndex = 1; colIndex < columns.length; colIndex++) {
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


	private void addEmptyDerivedColumns() {
		//make query
		if(extraColumn.size() == 0) {
			populateExtraColumns();
			for(String column : extraColumn) {
				String addColumnQuery = "ALTER TABLE "+tableName+" ADD " +column+" double";
				try {
					runQuery(addColumnQuery);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	private void populateExtraColumns() {
		if(this.extraColumn == null) {
			this.extraColumn = new ArrayList<String>();
		}

		for(int i = columnCount; i < 10; i++) {
			String nextColumn = extraColumnBase+"_"+i;
			extraColumn.add(nextColumn);
		}
	}

	private String getExtraColumn() {
		if(extraColumn.size() > 0)
			return extraColumn.remove(0);
		else return null;
	}


	/*************************** END CREATE **************************************/


	/*************************** READ ********************************************/

	//get scaled version of above method
	public List<Object[]> getScaledData(String tableName, List<String> selectors, Map<String, String> headerTypeMap, String column, Object value, Double[] maxArr, Double[] minArr) {

		if(joinMode) {
			tableName = this.viewTableName;
			selectors = translateColumns(selectors);
			column = translateColumn(column);
			Map<String, String> newHeaderTypeMap = new HashMap<>();
			for(String selector : headerTypeMap.keySet()) {
				newHeaderTypeMap.put(translateColumn(selector), headerTypeMap.get(selector));
			}
			headerTypeMap = newHeaderTypeMap;
		} else {
			
		}
//		tableName = joinMode ? this.viewTableName : this.tableName;
		
		int cindex = selectors.indexOf(column);
		if(tableName == null) tableName = this.tableName;

		List<Object[]> data;
		String[] types = new String[headerTypeMap.size()];

		int index = 0;
		for(String selector : selectors) {
			types[index] = headerTypeMap.get(selector);
			index++;
		}

		types = cleanTypes(types);

		try {
			String selectQuery = makeSpecificSelect(tableName, selectors, column, value);
			ResultSet rs = executeQuery(selectQuery);

			if(rs != null) {

				ResultSetMetaData rsmd = rs.getMetaData();
				int NumOfCol = rsmd.getColumnCount();
				data = new ArrayList<>(NumOfCol);
				while (rs.next()){
					Object[] row = new Object[NumOfCol];

					for(int i = 1; i <= NumOfCol; i++) {
						Object val = rs.getObject(i);
						if(cindex != (i-1) && (types[i-1].equalsIgnoreCase("int") || types[i-1].equalsIgnoreCase("double"))) {
							row[i-1] = ( ((Number)val).doubleValue() - minArr[i-1])/(maxArr[i-1] - minArr[i-1]);
						} else {
							row[i-1] = val;
						}
					}
					data.add(row);
				}

				return data;
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return new ArrayList<Object[]>(0);
	}

	/**
	 * Get the column values from the table as an Object array
	 * 
	 * @param columnHeader
	 * @param distinct - true indicates only distinct values, false otherwise
	 * @return
	 */
	public Object[] getColumn(String columnHeader, boolean distinct) {
		String tableName = this.tableName;
		if(joinMode) {
			tableName = this.viewTableName;
			columnHeader = translateColumn(columnHeader);
		} else {
			
		}
//		String tableName = joinMode ? this.viewTableName : this.tableName;

		ArrayList<Object> column = new ArrayList<>();

		columnHeader = cleanHeader(columnHeader);
		List<String> headers = new ArrayList<String>(1);
		headers.add(columnHeader);
		ResultSet rs;
		String selectQuery;
		if(distinct) {
			selectQuery = makeSelectDistinct(tableName, headers);
		} else {
			selectQuery = makeSelect(tableName, headers);
		}
		try {
			rs = executeQuery(selectQuery);
			while(rs.next()) {
				column.add(rs.getObject(1));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return column.toArray();
	}

	/**
	 * This method returns the max/min/count/sum/avg of a column
	 * returns null if statType parameter is invalid
	 * 
	 * @param columnHeader
	 * @param statType
	 * @return
	 */
	public Double getStat(String columnHeader, String statType) {
		String tableName = this.tableName;
		if(joinMode) {
			tableName = this.viewTableName;
			columnHeader = translateColumn(columnHeader);
		} else {
			
		}
//		String tableName = joinMode ? this.viewTableName : this.tableName;
		columnHeader = cleanHeader(columnHeader);
		ResultSet rs = executeQuery(makeFunction(columnHeader, statType, tableName));
		try {
			if(rs.next()) {
				return rs.getDouble(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	public Map<Map<Object, Object>, Object> getStat(String columnHeader, String statType, List<String> groupByCols) {
		String tableName = this.tableName;
		if(joinMode) {
			tableName = this.viewTableName;
			columnHeader = translateColumn(columnHeader);
			groupByCols = translateColumns(groupByCols);
		} else {
			
		}
//		String tableName = joinMode ? this.viewTableName : this.tableName;
		ResultSet rs = executeQuery(makeFunction(columnHeader, statType, tableName, groupByCols));
		try {
			Map<Map<Object, Object>, Object> results = new Hashtable<Map<Object, Object>, Object>();
			ResultSetMetaData metaData = rs.getMetaData();
			int numReturns = metaData.getColumnCount();

			int[] typeArr = new int[numReturns];
			// index starts at 1 for metaData
			for(int i = 1; i <= numReturns; i++) {
				typeArr[i-1] = metaData.getColumnType(i);
			}

			while(rs.next()) {
				// first return is the stat routine
				Double val = rs.getDouble(1);
				// get the unique set of group by values
				Map<Object, Object> groupByVals = new Hashtable<Object, Object>();
				for(int i = 2; i <= numReturns; i++) {
					// group by cols are added in the same position as the list passed in
					groupByVals.put(groupByCols.get(i-2), rs.getObject(i));
				}
				// add the row into the result set
				results.put(groupByVals, val);
			}

			return results;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Adds new headers with associated types to the table
	 * 
	 * @param tableName - table name to modify
	 * @param headers - header names
	 * @param types - types
	 * 
	 */
	public void alterTableNewColumns(String tableName, String[] headers, String[] types) {
		types = cleanTypes(types);
		try {
			if(tableExists(tableName)) {
				List<String> newHeaders = new ArrayList<String>();
				List<String> newTypes = new ArrayList<String>();

				//determine the new headers and types
				for(int i = 0; i < headers.length; i++) {
					if(!ArrayUtilityMethods.arrayContainsValue(getHeaders(tableName), headers[i].toUpperCase())) {
						//these are the columns to create
						newHeaders.add(headers[i]);
						newTypes.add(types[i]);
					}
				}

				//if we have new headers add them to the table
				if(!newHeaders.isEmpty()) {
					String alterQuery = makeAlter(tableName, newHeaders.toArray(new String[]{}), newTypes.toArray(new String[]{}));
					System.out.println("altering table: " + alterQuery);
					runQuery(alterQuery);
				}
			} else {
				//if table doesn't exist then create one with headers and types
				String createTable = makeCreate(tableName, headers, types);
				System.out.println("creating table: " + createTable);
				runQuery(createTable);
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}


	//only use this for analytics for now
	//update the table with new values
	//the last values and the last columnHeaders are what is updated
	//used only for updating one column
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

			String updateQuery = makeUpdate2(tableName, joinColumn, newColumn, joinValue, newValue);
			runQuery(updateQuery);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}	

	//used only for Col Split PKQL for now to update multiple columns - more generic to updateTable() method
	public void updateTable2(String[] origColumns, Object[] origValues, String[] newColumns, Object[] newValues) {

		try{
			String updateQuery = makeUpdate2(tableName, origColumns, newColumns, origValues, newValues);
			runQuery(updateQuery);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*************************** END READ ****************************************/


	/*************************** DELETE ******************************************/

	/**
	 * Drops the column from the main table
	 * @param columnHeader - column to drop
	 */
	public void dropColumn(String columnHeader) {

		try {
			runQuery(makeDropColumn(columnHeader, tableName));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Deletes all rows which have the associated values for the columns
	 * @param columns
	 * @param values
	 */
	public void deleteRow(String[] columns, String[] values) {
		try {
			String deleteRowQuery = makeDeleteData(tableName, columns, values);
			runQuery(deleteRowQuery);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * used to drop the table when the insight is closed
	 */
	protected void dropTable() {
		String finalQuery = makeDropTable(tableName);
		try {
			runQuery(finalQuery);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("DROPPED H2 TABLE" + tableName);
	}

	/**
	 * used to drop the table when the insight is closed
	 */
	protected void dropView() {
		if(viewTableName == null) return;

		String finalQuery = makeDropView(this.viewTableName);
		try {
			runQuery(finalQuery);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("DROPPED H2 TABLE" + tableName);
	}

	/*************************** END DELETE **************************************/

	/*************************** FILTER ******************************************/

	/**
	 * Aggregates filters for a columnheader
	 * In the case of a numerical filter such as greater than, less than, filters are replaced
	 * 
	 * @param columnHeader - column filters to modify
	 * @param values - values to add to filters
	 * @param comparator
	 */
	public void addFilters(String columnHeader, List<Object> values, Comparator comparator) {
		columnHeader = cleanHeader(columnHeader);
		//always overwrite for numerical filters (greater than, less than, etc.)
		//    	if(filterHash.get(columnHeader) == null || (comparator != Comparator.EQUAL && comparator != Comparator.NOT_EQUAL)) {
		//    		setFilters(columnHeader, values, comparator);
		//    	} else {
		//    		filterHash.get(columnHeader).addAll(values);
		//    		filterComparator.put(columnHeader, comparator);
		//    	}

		Map<String, Map<Comparator, Set<Object>>> filterHash;
		String column;
		if(joinMode) {
			filterHash = this.joiner.getJoinedFilterHash(viewTableName);
			column = translateColumn(columnHeader);
		} else {
			filterHash = this.filterHash2;
			column = columnHeader;
		}
		
		
		if(filterHash.get(column) == null) {
			setFilters(column, values, comparator);
		} else {
			Map<Comparator, Set<Object>> innerMap = filterHash.get(column);
			if(innerMap.get(comparator) == null || (comparator != Comparator.EQUAL && comparator != Comparator.NOT_EQUAL)) {
				innerMap.put(comparator, new HashSet<>(values));
			} else {
				innerMap.get(comparator).addAll(values);
			}
		}
	}

	/**
	 * Overwrites filters for a specific column with the values and comparator specified
	 * @param columnHeader
	 * @param values
	 * @param comparator
	 */
	public void setFilters(String columnHeader, List<Object> values, Comparator comparator) {
		columnHeader = cleanHeader(columnHeader);
		//    	filterHash.put(columnHeader, values);
		//    	filterComparator.put(columnHeader, comparator);

		Map<Comparator, Set<Object>> innerMap = new HashMap<>();
		innerMap.put(comparator, new HashSet<>(values));
		
		Map<String, Map<Comparator, Set<Object>>> filterHash;
		String column;
		if(joinMode) {
			filterHash = this.joiner.getJoinedFilterHash(viewTableName);
			column = translateColumn(columnHeader);
		} else {
			filterHash = this.filterHash2;
			column = columnHeader;
		}
		
		filterHash.put(column, innerMap);
	}

	/**
	 * Clears the filters for the columnHeader
	 * @param columnHeader
	 */
	public void removeFilter(String columnHeader) {
		columnHeader = cleanHeader(columnHeader);
		//    	filterHash.remove(columnHeader);
		//    	filterComparator.remove(columnHeader);

		Map<String, Map<Comparator, Set<Object>>> filterHash;
		String column;
		if(joinMode) {
			filterHash = this.joiner.getJoinedFilterHash(viewTableName);
			column = translateColumn(columnHeader);
		} else {
			filterHash = this.filterHash2;
			column = columnHeader;
		}
		
		filterHash.remove(column);
	}

	/**
	 * Clears all filters associated with the main table
	 */
	public void clearFilters() {
		//    	filterHash.clear();
		//    	filterComparator.clear();

		if(joinMode) {
			Map<String, Map<Comparator, Set<Object>>> filterHash = this.joiner.getJoinedFilterHash(viewTableName);
			for(String key : this.joinColumnTranslation.keySet()) {
				filterHash.remove(joinColumnTranslation.get(key));
			}
		} else {
			filterHash2.clear();
		}
	}

	/**
	 * 
	 * @param filterHash
	 * @param filterComparator
	 * @return
	 */
	private String makeFilterSubQuery(Map<String, List<Object>> filterHash, Map<String, Comparator> filterComparator) {
		//need translation of filter here
		String filterStatement = "";
		if(filterHash.keySet().size() > 0) {

			List<String> filteredColumns = new ArrayList<String>(filterHash.keySet());
			for(int x = 0; x < filteredColumns.size(); x++) {

				String header = filteredColumns.get(x);
//				String tableHeader = joinMode ? translateColumn(header) : header;
				String tableHeader = header;
				
				Comparator comparator = filterComparator.get(header);

				switch(comparator) {
				case EQUAL:{
					List<Object> filterValues = filterHash.get(header);
					String listString = getQueryStringList(filterValues);
					filterStatement += tableHeader+" in " + listString;
					break;
				}
				case NOT_EQUAL: {
					List<Object> filterValues = filterHash.get(header);
					String listString = getQueryStringList(filterValues);
					filterStatement += tableHeader+" not in " + listString;
					break;
				}
				case LESS_THAN: {
					List<Object> filterValues = filterHash.get(header);
					String listString = filterValues.get(0).toString();
					filterStatement += tableHeader+" < " + listString;
					break;
				}
				case GREATER_THAN: {
					List<Object> filterValues = filterHash.get(header);
					String listString = filterValues.get(0).toString();
					filterStatement += tableHeader+" > " + listString;
					break;
				}
				case GREATER_THAN_EQUAL: {
					List<Object> filterValues = filterHash.get(header);
					String listString = filterValues.get(0).toString();
					filterStatement += tableHeader+" >= " + listString;
					break;
				}
				case LESS_THAN_EQUAL: {
					List<Object> filterValues = filterHash.get(header);
					String listString = filterValues.get(0).toString();
					filterStatement += tableHeader+" <= " + listString;
					break;
				}
				default: {
					List<Object> filterValues = filterHash.get(header);
					String listString = getQueryStringList(filterValues);

					filterStatement += tableHeader+" in " + listString;
				}
				}

				//put appropriate ands
				if(x < filteredColumns.size() - 1) {
					filterStatement += " AND ";
				}
			}

			if(filterStatement.length() > 0) {
				filterStatement = " WHERE " + filterStatement;
			}
		}

		return filterStatement;
	}

	private String makeFilterSubQuery() {
		Map<String, Map<Comparator, Set<Object>>> filterHash;
		if(joinMode) {
			filterHash = joiner.getJoinedFilterHash(viewTableName);
		} else {
			filterHash = this.filterHash2;
		}
		return makeFilterSubQuery(filterHash);
	}
	
	private String makeFilterSubQuery(Map<String, Map<Comparator, Set<Object>>> filterHash) {
		//need to also pass in translationMap
		if(joinMode) filterHash = joiner.getJoinedFilterHash(viewTableName);

		String filterStatement = "";
		if(filterHash.keySet().size() > 0) {

			List<String> filteredColumns = new ArrayList<String>(filterHash.keySet());
			for(int x = 0; x < filteredColumns.size(); x++) {

				String header = filteredColumns.get(x);
//				String tableHeader = joinMode ? translateColumn(header) : header;
				String tableHeader = header;
				
				Map<Comparator, Set<Object>> innerMap = filterHash.get(header);
				int i = 0;
				for(Comparator comparator : innerMap.keySet()) {
					if(i > 0) {
						filterStatement += " AND ";
					}
					switch(comparator) {

					case EQUAL:{
						Set<Object> filterValues = innerMap.get(comparator);
						String listString = getQueryStringList(filterValues);
						filterStatement += tableHeader+" in " + listString;
						break;
					}
					case NOT_EQUAL: {
						Set<Object> filterValues = innerMap.get(comparator);
						String listString = getQueryStringList(filterValues);
						filterStatement += tableHeader+" not in " + listString;
						break;
					}
					case LESS_THAN: {
						Set<Object> filterValues = innerMap.get(comparator);
						String listString = filterValues.iterator().next().toString();
						filterStatement += tableHeader+" < " + listString;
						break;
					}
					case GREATER_THAN: {
						Set<Object> filterValues = innerMap.get(comparator);
						String listString = filterValues.iterator().next().toString();
						filterStatement += tableHeader+" > " + listString;
						break;
					}
					case GREATER_THAN_EQUAL: {
						Set<Object> filterValues = innerMap.get(comparator);
						String listString = filterValues.iterator().next().toString();
						filterStatement += tableHeader+" >= " + listString;
						break;
					}
					case LESS_THAN_EQUAL: {
						Set<Object> filterValues = innerMap.get(comparator);
						String listString = filterValues.iterator().next().toString();
						filterStatement += tableHeader+" <= " + listString;
						break;
					}
					default: {
						Set<Object> filterValues = innerMap.get(comparator);
						String listString = getQueryStringList(filterValues);
						filterStatement += tableHeader+" in " + listString;
					}

					}
					i++;
				}

				//put appropriate ands
				if(x < filteredColumns.size() - 1) {
					filterStatement += " AND ";
				}
			}

			if(filterStatement.length() > 0) {
				filterStatement = " WHERE " + filterStatement;
			}
		}

		return filterStatement;
	}

	/**
	 * 
	 * @param selectors - list of headers to grab from table
	 * @return
	 * 
	 * return the filtered values portion of the filter model that is returned by the H2Frame
	 */
	public Map<String, List<Object>> getFilteredValues(List<String> selectors) {

		//Header name -> [val1, val2, ...]
		//Ex: Studio -> [WB, Universal]
		//	WB and Universal are filtered from Studio column
		String tableName = this.tableName;
		Map<String, Map<Comparator, Set<Object>>> filterHash = this.filterHash2;
		if(joinMode) {
			tableName = this.viewTableName;
			filterHash = joiner.getJoinedFilterHash(viewTableName);
		}

		Map<String, List<Object>> returnFilterMap = new HashMap<>();

		try {
			for(String selector : selectors) {
				
				String thisSelector = joinMode ? translateColumn(selector) : selector;
				
				if(filterHash.get(thisSelector) != null) {
					String query = makeNotSelect(tableName, thisSelector, filterHash);
					ResultSet rs = executeQuery(query);

					List<Object> filterData = null;
					if(rs != null) {
						//						ResultSetMetaData rsmd = rs.getMetaData();
						filterData = new ArrayList<>();
						while (rs.next()){
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

	public Map<String, Map<Comparator, Set<Object>>> getFilterHash() {
		return this.filterHash2;
	}
	/*************************** END FILTER **************************************/


	/*************************** UPDATE ******************************************/

	/**
	 * This method is responsible for processing the data associated with an iterator and adding it to the H2 table
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

		//create table if doesn't exist
		try {
			runQuery("CREATE TABLE IF NOT EXISTS " + tableName);
		} catch (Exception e) {
			e.printStackTrace();
		}

		//add the data
		if(joinType.equals(Join.FULL_OUTER)) {
			processAlterData(newTableName, newHeaders, oldHeaders, Join.LEFT_OUTER);
		} else {
			processAlterData(newTableName, newHeaders, oldHeaders, joinType);
		}

		//if we are doing a full outer join (which h2 does not natively have)
		//we have done the left outer join above
		//now just add the rows we are missing via a merge query for each row
		//not efficient but don't see another way to do it
		//Ex: merge into table (column1, column2) key (column1, column2) values ('value1', 'value2')
		//TODO: change this to be a union of right outer and left outer instead of inserting values
		if(joinType.equals(Join.FULL_OUTER)) {

			try {
				Statement stmt = getConnection().createStatement();
				String selectQuery = makeSelect(newTableName, Arrays.asList(newHeaders));
				ResultSet rs = stmt.executeQuery(selectQuery);
				H2Iterator h2iterator = new H2Iterator(rs);

				String mergeQuery = "MERGE INTO "+tableName;
				String columns = "(";
				for(int i = 0; i < newHeaders.length; i++) {
					if(i!=0) {
						columns += ", ";
					} 
					columns += newHeaders[i];

				}
				columns += ")";

				mergeQuery += columns + " KEY " + columns;

				while(h2iterator.hasNext()) {
					Object[] row = h2iterator.next();
					String values = " VALUES("; 
					for(int i = 0; i < row.length; i++) {
						if(i != 0) {
							values += ", ";
						}
						values += " '"+row[i].toString()+"' ";
					}
					values+= ")";
					try {
						runQuery(mergeQuery+values);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
		}

		try {
			runQuery(makeDropTable(newTableName));
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		// shift to on disk if number of records is getting large
//		if(isInMem && getNumRecords() > MAX_IN_MEM_INSERT) {
//			this.conn = convertFromInMemToPhysical();
//			isInMem = false;
//		}
	}

	/**
	 * 
	 * @param column - the column to join on
	 * @param newColumnName - new column name with group by values
	 * @param valueColumn - the column to do calculations on
	 * @param mathType - the type of group by
	 * @param headers
	 */
	public void processGroupBy(String column, String newColumnName, String valueColumn, String mathType, String[] headers) {

		//    	String[] tableHeaders = getHeaders(tableName);
		//    	String inserter = makeGroupBy(column, valueColumn, mathType, newColumnName, this.tableName, headers);
		//    	processAlterAsNewTable(inserter, Join.LEFT_OUTER.getName(), tableHeaders);

		String reservedColumn = getExtraColumn();
		if(reservedColumn == null) {
			addEmptyDerivedColumns();
			reservedColumn = getExtraColumn();
		} 
		renameColumn(reservedColumn, newColumnName);

		//    	String[] tableHeaders = getHeaders(tableName);
		String inserter = makeGroupBy(column, valueColumn, mathType, newColumnName, this.tableName, headers);
		try {
			ResultSet groupBySet = executeQuery(inserter);
			while(groupBySet.next()) {
				Object[] values = {groupBySet.getObject(column), groupBySet.getObject(newColumnName)};
				String[] columnHeaders = {column, newColumnName};
				updateTable(headers, values, columnHeaders);

				//	            String updateQuery = makeUpdate(tableName, column, newColumnName, groupBySet.getObject(column), groupBySet.getObject(newColumnName));
				//	            runQuery(updateQuery);
			}
		} catch(Exception e) {

		}
		//    	makeUpdate(mathType, joinColumn, newColumn, joinValue, newValue)
		//    	processAlterAsNewTable(inserter, Join.LEFT_OUTER.getName(), tableHeaders);

		//all group bys are doubles?
		//    	addHeader(newColumnName, "double", tableHeaders);
		//    	addType("double");
	}

	//process a group by - calculate then make a table then merge the table
	public void processGroupBy(String[] column, String newColumnName, String valueColumn, String mathType, String[] headers) {
		String tableName = joinMode ? this.viewTableName : this.tableName;
		if(column.length == 1) {
			processGroupBy(column[0], newColumnName, valueColumn, mathType, headers);
			return;
		}
		//    	String[] tableHeaders = getHeaders(tableName);
		//    	String inserter = makeGroupBy(column, valueColumn, mathType, newColumnName, this.tableName, headers);
		//    	processAlterAsNewTable(inserter, Join.LEFT_OUTER.getName(), tableHeaders);

		//all group bys are doubles?
		String reservedColumn = getExtraColumn();
		if(reservedColumn == null) {
			addEmptyDerivedColumns();
			reservedColumn = getExtraColumn();
		} 
		renameColumn(reservedColumn, newColumnName);

		//    	String[] tableHeaders = getHeaders(tableName);
		String inserter = makeGroupBy(column, valueColumn, mathType, newColumnName, tableName, headers);
		try {
			ResultSet groupBySet = executeQuery(inserter);
			while(groupBySet.next()) {
				List<Object> values = new ArrayList<>();
				List<Object> columnHeaders = new ArrayList<>();
				for(String c : column) {
					values.add(groupBySet.getObject(c));//{groupBySet.getObject(column), groupBySet.getObject(newColumnName)};
					columnHeaders.add(c);
				}
				values.add(groupBySet.getObject(newColumnName));
				columnHeaders.add(newColumnName);
				updateTable(headers, values.toArray(), columnHeaders.toArray(new String[]{}));

				//	            String updateQuery = makeUpdate(tableName, column, newColumnName, groupBySet.getObject(column), groupBySet.getObject(newColumnName));
				//	            runQuery(updateQuery);
			}
		} catch(Exception e) {

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
			while( iterator.hasNext() ) {
				IHeadersDataRow headerRow = iterator.next();
				Object[] nextRow = headerRow.getValues();

				// need to set values on the first iteration
				if(ps == null) {
					String[] headers = headerRow.getHeaders();
					// get the data types
					types = new String[headers.length];
					for(int i = 0; i < types.length; i++) {
						types[i] = Utility.convertDataTypeToString(typesMap.get(headers[i]));
					}
					// set the PS based on the headers
					ps = createInsertPreparedStatement(tableName, headers);
				}

				// we need to loop through every value and cast appropriately
				for(int colIndex = 0; colIndex < nextRow.length; colIndex++) {
					String type = types[colIndex];
					if(type.equalsIgnoreCase("DATE")) {
						java.util.Date value = Utility.getDateAsDateObj(nextRow[colIndex] + "");
						if(value != null) {
							ps.setDate(colIndex+1, new java.sql.Date(value.getTime()));
						}
					} else if(type.equalsIgnoreCase("DOUBLE") || type.equalsIgnoreCase("FLOAT")) {
						Double value = Utility.getDouble(nextRow[colIndex] + "");
						if(value != null) {
							ps.setDouble(colIndex+1, value);
						}
					} else {
						String value = nextRow[colIndex] + "";
						ps.setString(colIndex+1, value + "");
					}
				}
				// add it
				ps.addBatch();

				// batch commit based on size
				if(++count % batchSize == 0) {
					ps.executeBatch();
				}
			}

			// well, we are done looping through now
			ps.executeBatch(); // insert any remaining records
			ps.close();
		} catch(Exception e) {

		}

		// shift to on disk if number of records is getting large
//		if(isInMem && getNumRecords() > MAX_IN_MEM_INSERT) {
//			this.conn = convertFromInMemToPhysical();
//			isInMem = false;
//		}
	}

	/**
	 * 
	 * adds new row to the table
	 * 
	 * @param tableName - name of table to add to
	 * @param cells - values to add to table
	 * @param headers - headers for the table
	 * @param types - types of the table
	 * 
	 * will add new row to the table, will create table if table does not already exist
	 */
	public void addRow(String tableName, String[] cells, String[] headers, String[] types) {
		boolean create = true;
		types = cleanTypes(types);

		//create table if it does not already exist
		try
		{
			if(!tableExists(tableName)) {
				String createTable = makeCreate(tableName, headers, types);
				runQuery(createTable);
			}
		}catch(Exception ex)
		{
			ex.printStackTrace();
			brokenLines = "Headers are not properly formatted - Discontinued processing";
			create = false;
		}

		//add the row to the table
		try
		{
			if(create)
			{
				rowCount++;
				cells = Utility.castToTypes(cells, types);
				String inserter = makeInsert(headers, types, cells, new Hashtable<String, String>(), tableName);
				runQuery(inserter);
			}
		}catch (SQLException ex)
		{
			System.out.println("SQL error while inserting row at " + rowCount); 
			System.out.println("Exception: "+ex);
		}
		catch (Exception ex)
		{
			System.out.println("Errored.. nothing to do");
			brokenLines = brokenLines + " : " + rowCount;
		}
	}

	/**
	 * 
	 * @param tableName 
	 * @return true if table with name tableName exists, false otherwise
	 */
	private boolean tableExists(String tableName) {
		String query = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '"+tableName+"'";
		ResultSet rs = executeQuery(query);
		try {
			return rs.next();
		} catch(SQLException e) {
			return false;
		}
	}


	/**
	 * 
	 * @param selectors
	 * @return
	 * 
	 * returns an iterator that returns data from the selectors
	 */
	public Iterator buildIterator(List<String> selectors) {
		String tableName;
		if(joinMode) {
			tableName = this.viewTableName;
			selectors = translateColumns(selectors);
		} else {
			tableName = this.tableName;
		}
//		String tableName = joinMode ? this.viewTableName : this.tableName;

		try {
			Statement stmt = getConnection().createStatement();
			String selectQuery = makeSelect(tableName, selectors);
			
			long startTime = System.currentTimeMillis();
			ResultSet rs = stmt.executeQuery(selectQuery);
			long endTime = System.currentTimeMillis();
			LOGGER.info("Executed Select Query on H2 FRAME: "+(endTime - startTime)+" ms");
			
			return new H2Iterator(rs);
		} catch(SQLException s) {
			s.printStackTrace();
		}

		return null;
	}
	
	/**
	 * 
	 * @param selectors
	 * @return
	 * 
	 * returns an iterator that returns data from the selectors
	 */
	public Iterator buildIterator(List<String> selectors, boolean ignoreFilters) {
		String tableName;
		if(joinMode) {
			tableName = this.viewTableName;
			selectors = translateColumns(selectors);
		} else {
			tableName = this.tableName;
		}
//		String tableName = joinMode ? this.viewTableName : this.tableName;

		try {
			Statement stmt = getConnection().createStatement();
			String selectQuery = "";
			if(ignoreFilters) {
				selectQuery = makeSelectDistinctIgnoreFilters(tableName, selectors);
			} else {
				selectQuery = makeSelectDistinct(tableName, selectors);
			}
			long startTime = System.currentTimeMillis();
			ResultSet rs = stmt.executeQuery(selectQuery);
			long endTime = System.currentTimeMillis();
			LOGGER.info("Executed Select Query on H2 FRAME: "+(endTime - startTime)+" ms");
			
			return new H2Iterator(rs);
		} catch(SQLException s) {
			s.printStackTrace();
		}

		return null;
	}

	//build the new way to create iterator with all the options
	/**
	 * 
	 * @param options - options needed to build the iterator
	 * @return
	 * 
	 * returns an iterator based on the options parameter
	 */
	public Iterator buildIterator(Map<String, Object> options) {
		String tableName = joinMode ? this.viewTableName : this.tableName;

		
		String sortDir = (String)options.get(TinkerFrame.SORT_BY_DIRECTION);

		//dedup = true indicates remove duplicates
		//TODO: rename de_dup to something more meaningful for cases outside of tinkergraph
		Boolean dedup = (Boolean) options.get(TinkerFrame.DE_DUP);
		if(dedup == null) dedup = false;

		Boolean ignoreFilters = (Boolean)options.get(TinkerFrame.IGNORE_FILTERS);
		if(ignoreFilters == null) ignoreFilters = false;
		
		//how many rows to get
		Integer limit = (Integer) options.get(TinkerFrame.LIMIT);

		//at which row to start
		Integer offset = (Integer) options.get(TinkerFrame.OFFSET);

		//column to sort by
		String sortBy = (String)options.get(TinkerFrame.SORT_BY);

		//selectors to gather
		List<String> selectors = (List<String>) options.get(TinkerFrame.SELECTORS);
		selectors = cleanHeaders(selectors);
		if(joinMode) {
			selectors = translateColumns(selectors);
		}
		String selectQuery;

		if(dedup) {
			if(ignoreFilters) {
				selectQuery = makeSelectDistinctIgnoreFilters(tableName, selectors);
			} else {
				selectQuery = makeSelectDistinct(tableName, selectors);
			}
		} else {
			if(ignoreFilters) {
				selectQuery = makeSelectIgnoreFilters(tableName, selectors);
			} else {
				selectQuery = makeSelect(tableName, selectors);
			}
		}

		//temporary filters to apply only to this iterator
		Map<String, List<Object>> temporalBindings = (Map<String, List<Object>>) options.get(TinkerFrame.TEMPORAL_BINDINGS); 
		Map<String, Comparator> compHash = new HashMap<String, Comparator>();
		for(String key : temporalBindings.keySet()) {
			compHash.put(key, Comparator.EQUAL);
		}

		//create a new filter substring and add/replace old filter substring
		String temporalFiltering = makeFilterSubQuery(temporalBindings, compHash); // default comparator is equals
		if(temporalFiltering != null && temporalFiltering.length() > 0) {
			if(selectQuery.contains(" WHERE ")) {
				temporalFiltering = temporalFiltering.replaceFirst(" WHERE ", "");
				selectQuery = selectQuery + temporalFiltering;
			} else {
				selectQuery = selectQuery + temporalFiltering;
			}
		}

		if(sortBy != null) {
			sortBy = cleanHeader(sortBy);
			if(joinMode) {
				sortBy = translateColumn(sortBy);
			}
			selectQuery += " order by " + sortBy;
		}
		if(limit != null && limit > 0) {
			selectQuery += " limit "+limit;
		}
		if(offset != null && offset > 0) {
			selectQuery += " offset "+offset;
		}

		long startTime = System.currentTimeMillis();
		ResultSet rs = executeQuery(selectQuery);
		long endTime = System.currentTimeMillis();
		LOGGER.info("Executed Select Query on H2 FRAME: "+(endTime - startTime)+" ms");
		return new H2Iterator(rs);
	}

	//process to join the data and new headers to existing table
	//    private void processAlterData(String[][] data, String[] newHeaders, String[] headers, Join joinType)
	//    {
	//    	// this currently doesnt handle many to many joins and such
	//    	String[] types = null;
	//    	try {
	//    		getConnection();
	//	    	
	//	    	// I need to do an evaluation here to find if this one to many
	//	    	String [] oldHeaders = headers;
	//
	//	    	//headers for the joining table
	//    		
	////    		int curHeadCount = headers.length;
	//    		Vector <String> newHeaderIndices = new Vector<String>();
	//    		Vector <String> oldHeaderIndices = new Vector<String>();
	//    		Hashtable<Integer, Integer> matchers = new Hashtable<Integer, Integer>();
	////    		if(matchers == null || matchers.isEmpty()) {
	////	    		matchers = new Hashtable<Integer, Integer>();
	//	    		
	//	    		// I need to find which ones are already there and which ones are new
	//	    		for(int hIndex = 0;hIndex < newHeaders.length;hIndex++)
	//	    		{
	//	    			String uheader = newHeaders[hIndex];
	//	    			uheader = cleanHeader(uheader);
	//	
	//	    			boolean old = false;
	//	    			for(int oIndex = 0;oIndex < headers.length;oIndex++)
	//	    			{
	//	    				if(headers[oIndex].equalsIgnoreCase(uheader))
	//	    				{
	//	    					old = true;
	//	    					oldHeaderIndices.add(hIndex+"");
	//	    					matchers.put(hIndex, oIndex);
	//	    					break;
	//	    				}
	//	    			}
	//	    			
	//	    			if(!old)
	//	    				newHeaderIndices.add((hIndex) + "");
	//	    		}
	////    		}
	//    		
	////    		headers = newHeaders;
	//				    		
	////    		String [] oldTypes = types; // I am not sure if I need this we will see - yes I do now yipee
	//    		
	////			String[] cells = data[0];
	////			predictRowTypes(cells);
	//
	////			String[] newTypes = types;
	//			
	//			//stream.close();
	//			
	//			boolean one2Many = false;
	//			// I also need to accomodate when there are no common ones
	////			if(oldHeaderIndices.size() == 0)
	//			if(matchers.isEmpty())	
	//			{
	//				// this is the case where it has not been created yet
	//				tableName = getNewTableName();
	//				generateTable(data, newHeaders, tableName);
	//			}
	//			else
	//			{
	//				// close the stream and open it again
	//				
	//				// do a different process here
	//				// I need to create a new database here
	//				one2Many = true;
	//				generateTable(data, newHeaders, H2FRAME + getNextNumber());
	//				// not bad -- this does it ?
	//				// whoa hang on I need to handle the older headers and such
	//				// done in reset headers
	//			}
	//			
	//			// need to create a new database with everything now
	//			// reset all the headers
	////			if(one2Many)
	////			resetHeaders(oldHeaders, headers, newHeaderIndices, oldTypes, types, curHeadCount);
	//    		// just test to see if it finally came through
	//			
	//			// now I need to assimilate everything into one
	//			String tableName2 = H2FRAME + tableRunNumber;
	//			String tableName1 = tableName;
	//			
	//			if(one2Many)
	//				mergeTables(tableName1, tableName2, matchers, oldHeaders, newHeaders, joinType.getName());
	//			
	//			testData();
	//			
	//		} catch (SQLException e) {
	//			e.printStackTrace();
	//		}
	//    }
	//    
	/**
	 * 
	 * @param newTableName - new table to join onto main table
	 * @param newHeaders - headers in new table
	 * @param headers - headers in current table
	 * @param joinType - how to join
	 */
	private void processAlterData(String newTableName, String[] newHeaders, String[] headers, Join joinType)
	{
		// this currently doesnt handle many to many joins and such
		try {
			getConnection();

			// I need to do an evaluation here to find if this one to many
			String [] oldHeaders = headers;

			//headers for the joining table

			//    		int curHeadCount = headers.length;
			Vector <String> newHeaderIndices = new Vector<String>();
			Vector <String> oldHeaderIndices = new Vector<String>();
			Hashtable<Integer, Integer> matchers = new Hashtable<Integer, Integer>();

			// I need to find which ones are already there and which ones are new
			for(int hIndex = 0;hIndex < newHeaders.length;hIndex++)
			{
				String uheader = newHeaders[hIndex];
				uheader = cleanHeader(uheader);

				boolean old = false;
				for(int oIndex = 0;oIndex < headers.length;oIndex++)
				{
					if(headers[oIndex].equalsIgnoreCase(uheader))
					{
						old = true;
						oldHeaderIndices.add(hIndex+"");
						matchers.put(hIndex, oIndex);
						break;
					}
				}

				if(!old)
					newHeaderIndices.add((hIndex) + "");
			}


			boolean one2Many = true;
			if(matchers == null || matchers.isEmpty()) {
				one2Many = false;
			}
			// I also need to accomodate when there are no common ones


			// now I need to assimilate everything into one
			String tableName1 = tableName;

			if(one2Many)
				mergeTables(tableName1, newTableName, matchers, oldHeaders, newHeaders, joinType.getName());

			testData();

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// Obviously I need the table names
	// I also need the matching properties
	// I have found that out upfront- I need to also keep what it is called in the old table
	// as well as the new table
	private void mergeTables(String tableName1, String tableName2, Hashtable <Integer, Integer> matchers, String[] oldTypes, String[] newTypes, String join)
	{
		getConnection();

		String origTableName = tableName;
		// now create a third table
		tableName = H2FRAME+getNextNumber();
		String newCreate = "CREATE Table " + tableName +" AS (";

		// now I need to create a join query
		// first the froms

		String froms = " FROM " + tableName1 + " AS  A ";
		String joins = " " + join  + " " + tableName2 + " AS B ON (";

		Enumeration <Integer> keys = matchers.keys();
		for(int jIndex = 0;jIndex < matchers.size();jIndex++)
		{
			Integer newIndex = keys.nextElement();
			Integer oldIndex = matchers.get(newIndex); 
			if(jIndex == 0)
				joins = joins + "A." + oldTypes[oldIndex] + " = " + "B." + newTypes[newIndex];
			else
				joins = joins + " AND " + "A." + oldTypes[oldIndex] + " = " + "B." + newTypes[newIndex];

		}
		joins = joins + " )";

		// first table A
		String selectors = "";
		for(int oldIndex = 0;oldIndex < oldTypes.length;oldIndex++)
		{
			if(oldIndex == 0)
				selectors = "A." + oldTypes[oldIndex];
			else
				selectors = selectors + " , " + "A." + oldTypes[oldIndex];
		}

		// next table 2
		for(int newIndex = 0;newIndex < newTypes.length;newIndex++)
		{
			if(!matchers.containsKey(newIndex))
				selectors = selectors + " , " + "B." + newTypes[newIndex];
		}

		String finalQuery = newCreate + "SELECT " + selectors + " " + froms + "  " +  joins + " )";

		System.out.println(finalQuery);

		try {
			Statement stmt = conn.createStatement();
			stmt.execute(finalQuery);

			runQuery(makeDropTable(tableName1));

			//DONT DROP THIS due to need to preserve for outer joins, method outside will handle dropping new table 
			//			runQuery(makeDropTable(tableName2));

			//rename back to the original table
			runQuery("ALTER TABLE " + tableName + " RENAME TO " + origTableName);
			this.tableName = origTableName;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private String getNextNumber()
	{
		String uuid = UUID.randomUUID().toString();
		uuid = uuid.replaceAll("-", "_");
		// table names will be upper case because that is how it is set in information schema
		return uuid.toUpperCase();
	}

	//changing from private to public access to get connection url
	//to pkql console
	public Connection getConnection()
	{
		if(this.conn == null)
		{
			try {

				Class.forName("org.h2.Driver");
				//jdbc:h2:~/test

				//this will have to update
				String url = "jdbc:h2:mem:" + this.schema + options;
				this.conn = DriverManager.getConnection(url, "sa", "");
				System.out.println("The connection is.. " + url);
				//	getConnection("jdbc:h2:C:/Users/pkapaleeswaran/h2/test.db;LOG=0;CACHE_SIZE=65536;LOCK_MODE=0;UNDO_LOG=0", "sa", "");

				//Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
				//conn = DriverManager.getConnection("jdbc:monetdb://localhost:50000/demo", "monetdb", "monetdb");
				//ResultSet rs = conn.createStatement().executeQuery("Select count(*) from voyages");

			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return this.conn;
	}
	
	public Connection convertFromInMemToPhysical() {
		try {
			Class.forName("org.h2.Driver");

			// first need get the data in the memory table
			Date date = new Date();
			String dateStr = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss_SSSS").format(date);
			String folderToUse = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "\\" + RDBMSEngineCreationHelper.cleanTableName(this.schema);
			String inMemScript = folderToUse + "_" + dateStr;
			String saveScript = "SCRIPT TO '" + inMemScript + "' COMPRESSION GZIP TABLE "+ this.tableName;
			runQuery(saveScript);
			
			String physicalDbLocation = folderToUse.replace('/', '\\') + "_" + dateStr + "_database";
			this.conn = DriverManager.getConnection("jdbc:h2:" + physicalDbLocation, "sa", "");
			
			// we run the script
			runQuery("RUNSCRIPT FROM '"+inMemScript+"' COMPRESSION GZIP ");
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return this.conn;
	}

	public String getTableName() {
		return tableName;
	}

	protected String getViewTableName() {
		return this.viewTableName;
	}

	//TODO: should this be private?
	public String[] getHeaders(String tableName) {
		List<String> headers = new ArrayList<String>();

		String columnQuery = "SHOW COLUMNS FROM "+tableName;
		ResultSet rs = executeQuery(columnQuery);
		try {
			while(rs.next()) {
				String header = rs.getString("FIELD");
				headers.add(header);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return headers.toArray(new String[]{});
	}

	//    public String[] getTypes(String tableName) {
	//    	List<String> headers = new ArrayList<String>();
	//    	
	//    	String columnQuery = "SHOW COLUMNS FROM "+tableName;
	//    	ResultSet rs = executeQuery(columnQuery);
	//    	try {
	//			while(rs.next()) {
	//				String header = rs.getString("TYPE");
	//				headers.add(header);
	//			}
	//		} catch (SQLException e) {
	//			// TODO Auto-generated catch block
	//			e.printStackTrace();
	//		}
	//    	return headers.toArray(new String[]{});
	//    }

	//    public Map<String, String> getHeadersAndTypes(String tableName) {
	//    	Map<String, String> typeMap = new HashMap<>();
	//    	String columnQuery = "SHOW COLUMNS FROM "+tableName;
	//    	ResultSet rs = executeQuery(columnQuery);
	//    	try {
	//			while(rs.next()) {
	//				String header = rs.getString("FIELD");
	//				String type = rs.getString("TYPE");
	//				typeMap.put(header, type);
	//			}
	//		} catch (SQLException e) {
	//			// TODO Auto-generated catch block
	//			e.printStackTrace();
	//		}
	//    	return typeMap;
	//    }

	private String[] cleanHeaders(String[] headers) {
		String[] cleanHeaders = new String[headers.length];
		for(int i = 0; i < headers.length; i++) {
			cleanHeaders[i] = cleanHeader(headers[i]);
		}
		return cleanHeaders;
	}

	private List<String> cleanHeaders(List<String> headers) {
		List<String> cleanedHeaders = new ArrayList<>(headers.size());
		for(String header : headers) {
			cleanedHeaders.add(cleanHeader(header));
		}
		return cleanedHeaders;
	}

	//TODO: this is done outside now, need to remove
	protected static String cleanHeader(String header) {
		/*header = header.replaceAll(" ", "_");
    	header = header.replace("(", "_");
    	header = header.replace(")", "_");
    	header = header.replace("-", "_");
    	header = header.replace("'", "");*/
		header = header.replaceAll("[#%!&()@#$'./-]*\"*", ""); // replace all the useless shit in one go
		header = header.replaceAll("\\s+","_");
		header = header.replaceAll(",","_"); 
		if(Character.isDigit(header.charAt(0)))
			header = "c_" + header;
		return header;
	}

	protected static String cleanType(String type) {
		if(type == null) type = "VARCHAR(800)";
		type = type.toUpperCase();
		if(typeConversionMap.containsKey(type)) {
			type = typeConversionMap.get(type);
		} else {
			if(typeConversionMap.containsValue(type)) {
				return type;
			}
			type = "VARCHAR(800)";
		}
		return type;
	}

	protected static String[] cleanTypes(String[] types) {
		String[] cleanTypes = new String[types.length];
		for(int i = 0; i < types.length; i++) {
			cleanTypes[i] = cleanType(types[i]);
		}

		return cleanTypes;
	}

	private String cleanInstance(String value) {
		return value.replace("'", "''");
	}

	//rename a column
	private void renameColumn(String fromColumn, String toColumn) {
		String renameQuery = makeRenameColumn(fromColumn, toColumn, tableName);
		try {
			runQuery(renameQuery);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String getSchema() {
		return this.schema;
	}

	/**
	 * Sets the schema for the connection
	 * This is used to create a different schema for each user to facilitate BE join
	 * @param schema
	 */
	public void setSchema(String schema) {
		if(schema != null) {
			if(!this.schema.equals(schema)) {
				LOGGER.info("Schema being modified from: '" +  this.schema + "' to new schema for user: '" + schema + "'");
				System.err.println("SCHEMA NOW... >>> " + schema);
				this.schema = schema;
				if(schema.equalsIgnoreCase("-1")) this.schema = "test";
				this.conn = null;
				getConnection();
			}
		}
	}

	/*************************** QUERY BUILDERS ******************************************/

	//make an insert query
	private String makeInsert(String[] headers, String[] types, String[] values, Hashtable<String, String> defaultValues, String tableName)
	{
		StringBuilder inserter = new StringBuilder("INSERT INTO " + tableName + " (" ); 
		StringBuilder template = new StringBuilder("(");

		for(int colIndex = 0;colIndex < headers.length;colIndex++)
		{
			// I need to find the type and based on that adjust what I want
			String type = types[colIndex];

			// if null on integer - empty
			// if null on string - ''
			// if currenncy - empty
			// if date - empty

			String name = cleanHeader(headers[colIndex]);
			StringBuilder thisTemplate = new StringBuilder(name);


			if(type.equalsIgnoreCase("int") || type.equalsIgnoreCase("double"))
			{
				//if(!defaultValues.containsKey(type))
				String value = values[colIndex];
				if(value == null || value.length() == 0) {
					value = "null";
				}
				thisTemplate = new StringBuilder(value);
				//else
				//	thisTemplate = new StringBuilder(defaultValues.get(type));
			}

			else if(type.equalsIgnoreCase("date")) {
				String value = values[colIndex];
				if(value == null || value.length() == 0) {
					value = "null";
					thisTemplate = new StringBuilder(value);
				} else {
					value = value.replace("'", "''");
					thisTemplate = new StringBuilder("'" + value + "'");
				}
			}
			else
			{
				//    			if(value != null)
				//comments will come in handy some day
				//if(!defaultValues.containsKey(type))
				String value = values[colIndex];
				value = value.replace("'", "''");
				thisTemplate = new StringBuilder("'" + value + "'");
				//else 
				//	thisTemplate = new StringBuilder("'" + defaultValues.get(type) + "'");
			}
			if(colIndex == 0)
			{
				inserter.append(name);// = new StringBuilder(inserter +  name);
				template.append(thisTemplate);// = new StringBuilder(template + "" + thisTemplate);
			}
			else
			{
				inserter.append(" , " + name);// = new StringBuilder(inserter + " , " + name );
				template.append(" , " + thisTemplate); //= new StringBuilder(template + " , " + thisTemplate);
			}
		}

		inserter.append(")  VALUES  ");// = new StringBuilder(inserter + ")  VALUES  ");
		//    	template.append(inserter + "" + template + ")");// = new StringBuilder(inserter + "" + template + ")");
		inserter.append(template+")");
		//System.out.println("Insert Values: " +template);

		//    	return template.toString();
		return inserter.toString();
	}

	private String makeAlter(String tableName, String[] newHeaders, String[] newTypes)
	{
		String createString = "ALTER TABLE " + tableName + " ADD (";

		for(int i = 0; i < newHeaders.length; i++) {
			if(i == 0) {
				createString = createString +  newHeaders[i] + "  " + newTypes[i];
			}
			else {
				createString = createString + ", " + newHeaders[i] + "  " + newTypes[i];
			}
		}

		createString = createString + ")";

		return createString;
	}

	//drop a table
	private String makeDropTable(String name) {
		return "DROP TABLE " + name;
	}

	//drop a view
	private String makeDropView(String name) {
		return "DROP VIEW " + name;
	}

	//make a select query
	private String makeSelect(String tableName, List<String> selectors) {

		String selectStatement = "SELECT ";

		for(int i = 0; i < selectors.size(); i++) {
			String selector = selectors.get(i);
			//    		selector = cleanHeader(selector);

			if(i < selectors.size() - 1) {
				selectStatement += selector + ", ";
			}
			else {
				selectStatement += selector;	
			}
		}

		//    	String filterSubQuery = makeFilterSubQuery(this.filterHash, this.filterComparator);
		String filterSubQuery = makeFilterSubQuery();
		selectStatement += " FROM " + tableName + filterSubQuery;
		return selectStatement;
	}
	
	//make a select query
	private String makeSelectIgnoreFilters(String tableName, List<String> selectors) {

		String selectStatement = "SELECT ";

		for(int i = 0; i < selectors.size(); i++) {
			String selector = selectors.get(i);
			//    		selector = cleanHeader(selector);

			if(i < selectors.size() - 1) {
				selectStatement += selector + ", ";
			}
			else {
				selectStatement += selector;	
			}
		}

		//    	String filterSubQuery = makeFilterSubQuery(this.filterHash, this.filterComparator);
		selectStatement += " FROM " + tableName;
		return selectStatement;
	}
	
	private String makeSpecificSelect(String tableName, List<String> selectors, String columnHeader, Object value) {
		value = cleanInstance(value.toString());

		//SELECT column1, column2, column3
		String selectStatement = "SELECT ";
		for(int i = 0; i < selectors.size(); i++) {
			String selector = selectors.get(i);
			selector = cleanHeader(selector);

			if(i < selectors.size() - 1) {
				selectStatement += selector + ", ";
			}
			else {
				selectStatement += selector;
			}
		}

		//SELECT column1, column2, column3 from table1
		selectStatement += " FROM " + tableName;
		//		String filterSubQuery = makeFilterSubQuery(this.filterHash, this.filterComparator);
		String filterSubQuery = makeFilterSubQuery(); 
		if(filterSubQuery.length() > 1) {
			selectStatement += filterSubQuery;
			selectStatement += " AND " + columnHeader + " = " + "'"+value+"'";
		} else {
			selectStatement += " WHERE " + columnHeader + " = " + "'"+value+"'";
		}

		return selectStatement;
	}

	//make a select query
	private String makeSelectDistinct(String tableName, List<String> selectors) {

		String selectStatement = "SELECT DISTINCT ";

		for(int i = 0; i < selectors.size(); i++) {
			String selector = selectors.get(i);
			selector = cleanHeader(selector);

			if(i < selectors.size() - 1) {
				selectStatement += selector + ",";
			}
			else {
				selectStatement += selector;
			}
		}

		//    	String filterSubQuery = makeFilterSubQuery(this.filterHash, this.filterComparator);
		String filterSubQuery = makeFilterSubQuery();
		selectStatement += " FROM " + tableName + filterSubQuery;

		return selectStatement;
	}

	//make a select query
	private String makeSelectDistinctIgnoreFilters(String tableName, List<String> selectors) {

		String selectStatement = "SELECT DISTINCT ";

		for(int i = 0; i < selectors.size(); i++) {
			String selector = selectors.get(i);
			//    		selector = cleanHeader(selector);

			if(i < selectors.size() - 1) {
				selectStatement += selector + ", ";
			}
			else {
				selectStatement += selector;	
			}
		}

		//    	String filterSubQuery = makeFilterSubQuery(this.filterHash, this.filterComparator);
//		String filterSubQuery = makeFilterSubQuery();
		selectStatement += " FROM " + tableName;// + filterSubQuery;
		return selectStatement;
	}

	/**
	 * This method returns all the distinct values that are filtered out for a particular column
	 * This method is mainly used to retrieve values for the filter model displayed on the front end
	 * 
	 * @param tableName - table in which selector exists
	 * @param selector - column to grab values from
	 * @return
	 */
	private String makeNotSelect(String tableName, String selector, Map<String, Map<Comparator, Set<Object>>> filterHash) {
		String selectStatement = "SELECT DISTINCT ";

		selector = cleanHeader(selector);
		selectStatement += selector + " FROM " + tableName;    	

		String filterStatement = "";

		Map<Comparator, Set<Object>> filterMap = filterHash.get(selector);
		int i = 0;
		//what ever is listed in the filter hash, we want the get the values that would be the logical opposite
		//i.e. if filter hash indicates 'X < 0.9 AND X > 0.8', return 'X > =0.9 OR X <= 0.8'
		for(Comparator comparator : filterMap.keySet()) {
			if(i > 0) {
				filterStatement += " OR ";
			}
			Set<Object> filterValues = filterMap.get(comparator);
			if(filterValues.size() == 0) continue;

			if(comparator.equals(Comparator.EQUAL)) {
				String listString = getQueryStringList(filterValues);
				filterStatement += selector+" NOT IN " + listString;
			} else if(comparator.equals(Comparator.NOT_EQUAL)) {
				String listString = getQueryStringList(filterValues);
				filterStatement += selector+" IN " + listString;
			} else if(comparator.equals(Comparator.GREATER_THAN)) {
				filterStatement += selector+" <= "+filterValues.iterator().next().toString();
			} else if(comparator.equals(Comparator.GREATER_THAN_EQUAL)) {
				filterStatement += selector+" < "+filterValues.iterator().next().toString();
			} else if(comparator.equals(Comparator.LESS_THAN)) {
				filterStatement += selector+" >= "+filterValues.iterator().next().toString();
			} else if(comparator.equals(Comparator.LESS_THAN_EQUAL)) {
				filterStatement += selector+" > "+filterValues.iterator().next().toString();
			}
			i++;
		}

		selectStatement += " WHERE " + filterStatement;

		return selectStatement;
	}

	private String getQueryStringList(List<Object> values) {
		String listString = "(";

		for(int i = 0; i < values.size(); i++) {
			Object value = values.get(i);
			value = cleanInstance(value.toString());
			listString += "'"+value+"'";
			if(i < values.size() - 1) {
				listString += ", ";
			}
		}

		listString+=")";
		return listString;
	}

	private String getQueryStringList(Set<Object> values) {
		String listString = "(";

		Iterator<Object> iterator = values.iterator();
		int i = 0;
		while(iterator.hasNext()) {
			Object value = iterator.next();
			value = cleanInstance(value.toString());
			listString += "'"+value+"'";
			if(i < values.size() - 1) {
				listString += ", ";
			}
			i++;
		}

		listString+=")";
		return listString;
	}

	//create a table with headers and name
	private String makeCreate(String tableName, String[] headers, String[] types)
	{
		String createString = "CREATE TABLE " + tableName + " (";	    	

		String [] capHeaders = new String[headers.length];

		for(int headerIndex = 0;headerIndex < headers.length;headerIndex++)
		{
			String header = cleanHeader(headers[headerIndex]);    		
			capHeaders[headerIndex] = header;

			if(headerIndex == 0)
				createString = createString +  header + "  " + types[headerIndex];
			else
				createString = createString + ", " + header + "  " + types[headerIndex];		
		}

		//    	this.headers = capHeaders;
		createString = createString + ");";

		return createString;
	}

	private String makeGroupBy(String column, String valueColumn, String mathType, String alias, String tableName, String[] headers) {

		column = cleanHeader(column);
		valueColumn = cleanHeader(valueColumn);
		alias = cleanHeader(alias);

		String functionString = "";

		String type = getType(tableName, column);

		switch(mathType.toUpperCase()) {
		case "COUNT": {
			String func = "COUNT(";
			if(type.toUpperCase().startsWith("VARCHAR"))
				func = "COUNT( DISTINCT ";
			functionString = func +valueColumn+")"; break; 
		}
		case "AVERAGE": {functionString = "AVG("+valueColumn+")";  break; }
		case "MIN": {functionString = "MIN("+valueColumn+")";  break; }
		case "MAX": {functionString = "MAX("+valueColumn+")"; break; }
		case "SUM": {functionString = "SUM("+valueColumn+")"; break; }
		default: {
			String func = "COUNT(";
			if(type.toUpperCase().startsWith("VARCHAR"))
				func = "COUNT( DISTINCT ";
			functionString = func +valueColumn+")"; break; }
		}

		//    	String filterSubQuery = makeFilterSubQuery(this.filterHash, this.filterComparator);
		String filterSubQuery = makeFilterSubQuery();
		String groupByStatement = "SELECT " + column+", "+functionString + " AS " + alias +" FROM "+tableName + filterSubQuery + " GROUP BY "+ column;

		return groupByStatement;
	}

	//TODO : don't assume a double group by here
	private String makeGroupBy(String[] column, String valueColumn, String mathType, String alias, String tableName, String[] headers) {
		if(column.length == 1) return makeGroupBy(column[0], valueColumn, mathType, alias, tableName, headers);
		String column1 = cleanHeader(column[0]);
		String column2 = cleanHeader(column[1]);
		valueColumn = cleanHeader(valueColumn);
		alias = cleanHeader(alias);

		String functionString = "";

		String type = getType(tableName, valueColumn);

		switch(mathType.toUpperCase()) {
		case "COUNT": {
			String func = "COUNT(";
			if(type.toUpperCase().startsWith("VARCHAR"))
				func = "COUNT( DISTINCT ";
			functionString = func +valueColumn+")"; break; 
		}
		case "AVERAGE": {functionString = "AVG("+valueColumn+")";  break; }
		case "MIN": {functionString = "MIN("+valueColumn+")";  break; }
		case "MAX": {functionString = "MAX("+valueColumn+")"; break; }
		case "SUM": {functionString = "SUM("+valueColumn+")"; break; }
		default: {
			String func = "COUNT(";
			if(type.toUpperCase().startsWith("VARCHAR"))
				func = "COUNT( DISTINCT ";
			functionString = func +valueColumn+")"; break; }
		}

		//    	String filterSubQuery = makeFilterSubQuery(this.filterHash, this.filterComparator);
		String filterSubQuery = makeFilterSubQuery();
		String groupByStatement = "SELECT " + column1+", "+column2+", "+functionString + " AS " + alias +" FROM "+tableName + filterSubQuery + " GROUP BY "+ column1+", "+column2;

		return groupByStatement;
	}

	private String makeFunction(String column, String function, String tableName) {
		column = cleanHeader(column);
		String functionString = "SELECT ";
		switch(function.toUpperCase()) {
		case "COUNT": functionString += "COUNT("+column+")"; break;
		case "AVERAGE": functionString += "AVG("+column+")"; break;
		case "MIN": functionString += "MIN("+column+")"; break;
		case "MAX": functionString += "MAX("+column+")"; break;
		case "SUM": functionString += "SUM("+column+")"; break;
		default: functionString += column;
		}

		functionString += "FROM "+tableName;
		return functionString;
	}

	private String makeFunction(String column, String function, String tableName, List<String> groupByCols) {
		if(groupByCols == null || groupByCols.isEmpty()) {
			return makeFunction(column, function, tableName);
		}

		// clean all the headers and group by cols
		column = cleanHeader(column);
		List<String> cleanGroupByCols = new Vector<String>();
		for(String col : groupByCols) {
			cleanGroupByCols.add(cleanHeader(col));
		}

		// the first return is the column you are modifying
		String functionString = "SELECT DISTINCT ";
		switch(function.toUpperCase()) {
		case "COUNT": functionString += "COUNT("+column+")"; break;
		case "AVERAGE": functionString += "AVG("+column+")"; break;
		case "MIN": functionString += "MIN("+column+")"; break;
		case "MAX": functionString += "MAX("+column+")"; break;
		case "SUM": functionString += "SUM("+column+")"; break;
		default: functionString += column;
		}

		// also want to return the group by cols
		for(String col : groupByCols) {
			functionString += ", " + col;
		}

		// add group by
		functionString += " FROM " + tableName + " GROUP BY ";
		boolean isFirst = true;
		for(String col : groupByCols) {
			if(isFirst) {
				functionString += col;
				isFirst = false;
			} else {
				functionString += ", " + col;
			}
		}

		return functionString;
	}

	private String makeDropColumn(String column, String tableName) {
		column = cleanHeader(column);
		String dropColumnQuery = "ALTER TABLE "+tableName+" DROP COLUMN " + column;
		return dropColumnQuery;
	}

	//update only one column at a time
	private String makeUpdate(String tableName, String[] joinColumn, String newColumn, Object[] joinValue, Object newValue) {
		//    	joinValue = cleanInstance(joinValue.toString());
		newValue = cleanInstance(newValue.toString());
		String updateQuery = "UPDATE "+tableName+" SET "+newColumn+"="+"'"+newValue+"'"+" WHERE ";
		for(int i = 0; i < joinColumn.length; i++) {
			String joinInstance = cleanInstance(joinValue[i].toString());
			if(i == 0) {
				updateQuery += joinColumn[i]+"="+"'"+joinInstance+"'";
			} else {
				updateQuery += " AND "+joinColumn[i]+"="+"'"+joinInstance+"'";
			}
		}

		return updateQuery;
	}

	//as for now used only for col split PKQL, to update multiple new split columns using one update query
	//more generic to makeUpdate() method - should eventually replace it
	private String makeUpdate2(String tableName, Object[] joinColumn, Object[] newColumn, Object[] joinValue, Object[] newValue) {

		String updateQuery = "UPDATE "+tableName+" SET ";

		for(int i = 0; i < newColumn.length; i++) {
			String valueInstance = cleanInstance(newValue[i].toString());
			if(i==0){
				updateQuery += newColumn[i].toString()+"="+"'"+valueInstance+"'";
			}else{
				updateQuery += ", " + newColumn[i].toString()+"="+"'"+valueInstance+"'";
			}			
		}

		updateQuery += " WHERE ";

		for(int i = 0; i < joinColumn.length; i++) {
			String joinInstance = cleanInstance(joinValue[i].toString());
			if(i == 0) {
				updateQuery += joinColumn[i].toString()+"="+"'"+joinInstance+"'";
			} else {
				updateQuery += " AND "+joinColumn[i].toString()+"="+"'"+joinInstance+"'";
			}
		}

		return updateQuery;
	}

	private String getType(String tableName, String column)
	{
		String type = null;
		String typeQuery = "SELECT TABLE_NAME, COLUMN_NAME, TYPE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '"+tableName.toUpperCase()+"' AND COLUMN_NAME = '"+column.toUpperCase()+"'";
		ResultSet rs = executeQuery(typeQuery);
		try {
			if(rs.next()) {
				type = rs.getString("TYPE_NAME");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return type;

	}

	//rename column 
	private String makeRenameColumn(String fromColumn, String toColumn, String tableName) {
		return "ALTER TABLE "+tableName+" ALTER COLUMN "+fromColumn+" RENAME TO "+toColumn;
	}

	private String makeDeleteData(String tableName, String[] columnName, String[] values) {
		String deleteQuery = "DELETE FROM " + tableName +" WHERE ";
		for(int i = 0; i < columnName.length; i++) {
			if(i > 0) {
				deleteQuery += " AND ";
			}
			deleteQuery += columnName[i]+" = '"+values[i]+"'";
		}
		return deleteQuery;
	}

	/*************************** END QUERY BUILDERS **************************************/

	//use this when result set is not expected back
	private void runQuery(String query) throws Exception{
		getConnection().createStatement().execute(query);
	}

	//use this when result set is expected
	private ResultSet executeQuery(String query) {
		try {
			return getConnection().createStatement().executeQuery(query);
		} catch(SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	protected ResultSet runBackDoorQuery(String query) {
		try {
			System.out.println("RUNNING executeQuery....");
			return getConnection().createStatement().executeQuery(query);
		} catch(Exception e) {
			try {
				System.out.println("FAILED...RUNNING execute...");
				getConnection().createStatement().execute(query);
			} catch(Exception ee) {
				System.out.println("FAILED AGAIN..");
				System.out.print("executeQuery Error:  ");
				e.printStackTrace();
				System.out.print("\nexecute Error: ");
				ee.printStackTrace();
			}
		}
		return null;
	}
	
	public void runExternalQuery(String query) throws Exception {
		query = query.toUpperCase().trim();
		if(query.startsWith("CREATE VIEW")) {
			runQuery(query);
		} 

		else if(query.startsWith("CREATE OR REPLACE VIEW")) {
			runQuery(query);
		}
		//possibly need to drop view table externally as well
		else if(query.startsWith("DROP VIEW")) {
			runQuery(query);
		} 

		else {
			//could also run select queries directly
			throw new IllegalArgumentException("Can only run 'create view/create or replace view/drop view' queries externally");
		}
	}

	//save the main table
	//need to update this if we are saving multiple tables
	public Properties save(String fileName, String[] headers) {
		
		Properties props = new Properties();
		
		List<String> selectors = new ArrayList<String>(headers.length);
		for(String header : headers) {
			selectors.add(header);
		}
		try {
			String newTable = getNewTableName();
			String createQuery = "CREATE TABLE "+newTable+" AS "+makeSelect(tableName, selectors);
			runQuery(createQuery);
			String saveScript = "SCRIPT TO '"+fileName+"' COMPRESSION GZIP TABLE "+newTable;
			runQuery(saveScript);
			
			props.setProperty("tableName", newTable);
			
			String dropQuery = makeDropTable(newTable);
			runQuery(dropQuery);
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return props;
	}

	/**
	 * Runs the script for a cached Insight
	 * @param fileName				The file containing the script to create the frame
	 */
	public void open(String fileName, Properties prop) {
		// get a unique table name
		// set the table name for the instance
		tableName = H2FRAME + getNextNumber(); 
		
		String tempTableName = null;
		
		if(prop != null) {
			tempTableName = prop.getProperty("tableName");
			if(tempTableName == null) {
				tempTableName = H2Builder.tempTable;
			}
		} else {
			tempTableName = H2Builder.tempTable;
		}
		// get the open sql script
		String openScript = "RUNSCRIPT FROM '"+fileName+"' COMPRESSION GZIP ";
		// get an alter table name sql
		String createQuery = "ALTER TABLE " + tempTableName + " RENAME TO " + tableName;
		try {
			// we run the script in the file which automatically creates a temp temple
			runQuery(openScript);
			// then we rename the temp table to the new unqiue table name
			runQuery(createQuery);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	// Connects the frame
	public String connectFrame()
	{
		if(server == null)
		{
	    	try {
				String port = Utility.findOpenPort();
				// create a random user and password 				
				// get the connection object and start up the frame
				server = Server.createTcpServer("-tcpPort", port, "-tcpAllowOthers");
				//server = Server.createPgServer("-baseDir", "~", "-pgAllowOthers"); //("-tcpPort", "9999");	
				serverURL = "jdbc:h2:" + server.getURL() + "/mem:" + this.schema + options;
				//System.out.println("URL: jdbc:h2:" + server.getURL() + "/mem:test");
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
	
	private void printSchemaTables()
	{
    	try {
			Class.forName("org.h2.Driver");
			String url = serverURL;
			Connection conn = DriverManager.getConnection(url, "sa", "");
			ResultSet rs = conn.createStatement().executeQuery("SELECT TABLE_NAME FROM INFORMATIOn_SCHEMA.TABLES WHERE TABLE_SCHEMA='PUBLIC'");
			
			while(rs.next())
				System.out.println("Table name is " + rs.getString(1));
			
			url = "jdbc:h2:mem:test";
			conn = this.conn;
			rs = conn.createStatement().executeQuery("SELECT TABLE_NAME FROM INFORMATIOn_SCHEMA.TABLES WHERE TABLE_SCHEMA='PUBLIC'");
			
			//String schema = this.conn.getSchema();
			System.out.println(".. " + conn.getMetaData().getURL());
			System.out.println(".. " + conn.getMetaData().getUserName());
		//	System.out.println(".. " + conn.getMetaData().getS);
			
			while(rs.next())
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
		String query = "SELECT COUNT(*) FROM " + this.tableName;
		ResultSet rs = executeQuery(query);
		try {
			while(rs.next()) {
				return rs.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return 0;
	}



    public String[] createUser(String tableName)
    {
    	// really simple
    	// find an open port
    	// once found
    	// create url with connection string and send it back
    	
    	// need to pass the username and password back
    	// the username is specific to an insight and possibly gives access only to that insight
    	// I need to get the insight table - i.e. the table backing the insight
		String [] retString = new String[2];
		
		if(!tablePermissions.containsKey(tableName))
		{
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
				//query = "ALTER USER " + userName + " ADMIN TRUE";
				
				// create a new role for this table
				query = "CREATE ROLE IF NOT EXISTS " + tableName + "READONLY";
				stmt.executeUpdate(query);
				query = "GRANT SELECT, INSERT, UPDATE ON " +  tableName + " TO " + tableName + "READONLY";
				stmt.executeUpdate(query);
	
				// assign this to our new user
				query = "GRANT " + tableName  + "READONLY TO " + userName;
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
    
    public void disconnectFrame()
    {
    	server.stop();
    	server = null;
    	serverURL = null;
    }

	/*************************** ORIGINAL UNUSED CODE **************************************/



	//////////////////////////////////////////////////////////////////////////////////////////
	/*
	 * There are a bunch of methods that are not used in this class...
	 * I have no idea what to do with them
	 * I am just going to have them commented out here...
	 * hopefully we can delete these at some point
	 */

	//	private String makeCreate(String tableName, String subquery) {
	//		String createQuery = "CREATE TABLE "+tableName+" AS " + subquery;
	//		return createQuery;
	//	}
	//	//build the query to create a new tinker table given the types with default values
	//	private String makeTemplate(String[] headers, String[] types, Hashtable<String, String> defaultValues)
	//	{
	//
	//		//building query
	//		StringBuilder inserter = new StringBuilder("INSERT INTO "+tableName+"(" ); 
	//		StringBuilder template = new StringBuilder("(");
	//
	//		for(int colIndex = 0;colIndex < headers.length;colIndex++)
	//		{
	//			// I need to find the type and based on that adjust what I want
	//			String type = types[colIndex];
	//
	//			// if null on integer - empty
	//			// if null on string - ''
	//			// if currenncy - empty
	//			// if date - empty
	//
	//			String name = headers[colIndex];
	//			StringBuilder thisTemplate = new StringBuilder(name);
	//
	//
	//			if(type.equalsIgnoreCase("int") || type.equalsIgnoreCase("double") || type.equalsIgnoreCase("date"))
	//			{
	//				if(!defaultValues.containsKey(type))
	//					thisTemplate = new StringBuilder("<" + thisTemplate + "; null=\"\">");
	//				else
	//					thisTemplate = new StringBuilder("<" + thisTemplate + "; null=\"" + defaultValues.get(type) + "\">");
	//			}
	//			else
	//			{
	//				if(!defaultValues.containsKey(type))
	//					thisTemplate = new StringBuilder("'<" + thisTemplate + "; null=\"''\">'");
	//				else
	//					thisTemplate = new StringBuilder("'<" + thisTemplate + "; null=\"" + defaultValues.get(type) + "\">'");
	//
	//			}
	//			if(colIndex == 0)
	//			{
	//				inserter = new StringBuilder(inserter + "\"" + name + "\"");
	//				template = new StringBuilder(template + "" + thisTemplate);
	//			}
	//			else
	//			{
	//				inserter = new StringBuilder(inserter + " , \"" + name + "\"");
	//				template = new StringBuilder(template + " , " + thisTemplate);
	//			}
	//		}
	//
	//		inserter = new StringBuilder(inserter + ")  VALUES  ");
	//		template = new StringBuilder(inserter + "" + template + ")");
	//
	//		//System.out.println(".. Template..  " + template);
	//
	//
	//		return template.toString();
	//	}    
	//	private boolean isOne2Many(String  fileName, Vector <String> oldHeaders)
	//	{
	//		// I need to compare one by one to see there are many
	//		// or should I just make it into a separate table and do a cartesian product ?
	//
	//		// Idea is really simple.. I see everything that is there and then try to see if there are more than one record of the same kind 
	//		boolean retValue = false;
	//		Hashtable <String, Integer> valueCounter = new Hashtable<String, Integer>();
	//
	//		CsvParserSettings settings = new CsvParserSettings();
	//		settings.setNullValue("");
	//
	//		settings.setEmptyValue(""); // for CSV only
	//		settings.setSkipEmptyLines(true);
	//
	//		CsvParser parser = new CsvParser(settings);		    
	//
	//
	//		try {
	//			RandomAccessFile thisReader = new RandomAccessFile(fileName, "r");
	//
	//			String nextString = null;
	//			while((nextString = thisReader.readLine()) != null && !retValue)
	//			{
	//				String [] cells = parser.parseLine(nextString);
	//				String composite = "";
	//				for(int oldIndex = 0;oldIndex < oldHeaders.size();oldIndex++)
	//				{
	//					composite = composite + "_" + cells[Integer.parseInt(oldHeaders.elementAt(oldIndex))];
	//				}
	//				// now check if this is there already
	//				if(!valueCounter.containsKey(composite))
	//					valueCounter.put(composite, 1);
	//				else
	//					retValue = true;
	//			}
	//		} catch (NumberFormatException e) {
	//			// TODO Auto-generated catch block
	//			e.printStackTrace();
	//		} catch (IOException e) {
	//			// TODO Auto-generated catch block
	//			e.printStackTrace();
	//		}
	//		return retValue;
	//	}
	//	
	//	//turn the wrapper data to double array
	//	private String[][] getData(ISelectWrapper wrapper) {
	//		List<String[]> data = new ArrayList<String[]>();
	//		String[] colHeaders = wrapper.getDisplayVariables();
	//		colHeaders = cleanHeaders(colHeaders);
	//		int length = colHeaders.length;
	//		while(wrapper.hasNext()) {
	//			String[] rowData = new String[length];
	//			ISelectStatement statement = wrapper.next();
	//			Map<String, Object> nextRow = statement.getPropHash();
	//			for(int i = 0; i < length; i++) {
	//				rowData[i] = nextRow.get(colHeaders[i]).toString();
	//			}
	//			data.add(rowData);
	//		}
	//
	//		return data.toArray(new String[][]{});
	//	}
	//
	//	private String makeUpdate(String tableName, String joinColumn, String newColumn, Object joinValue, Object newValue) {
	//		joinValue = cleanInstance(joinValue.toString());
	//		newValue = cleanInstance(newValue.toString());
	//		String updateQuery = "UPDATE "+tableName+" SET "+newColumn+"="+"'"+newValue+"'"+" WHERE "+joinColumn+"="+"'"+joinValue+"'";
	//		return updateQuery;
	//	}
	//
	//	//generate a query to update a table
	//	private String makeUpdate(Vector<String> newHeaders, Vector<String> oldHeaders, String[] types, String[] values, int oldCount, Hashtable<String, String> defaultValues, String[] headers)
	//	{
	//		oldCount = 0;
	//		StringBuilder inserter = new StringBuilder("UPDATE " + tableName + " SET " ); 
	//
	//		StringBuilder sets = new StringBuilder(" ");
	//		StringBuilder wheres = new StringBuilder(" WHERE  ");
	//
	//		// first get all of the new headers
	//		for(int newIndex = 0;newIndex < newHeaders.size();newIndex++)
	//		{
	//			int id = Integer.parseInt(newHeaders.get(newIndex));
	//			//id = id - oldCount;
	//			String type = types[id];
	//			String value = null;
	//			if(type.equalsIgnoreCase("int") || type.equalsIgnoreCase("double"))
	//			{
	//				value = values[id-oldCount];
	//				if(value == null || value.length() == 0)
	//					value = "null";
	//			}
	//			else
	//			{
	//				value = values[id-oldCount];
	//				value = value.replace("'", "''");
	//				value = value.replace("\"", "");
	//				value = "'" + value + "'";
	//			}
	//
	//			// values
	//			if(newIndex == 0)
	//				sets = new StringBuilder(headers[id] + " = " + value);
	//			else
	//				sets = new StringBuilder(sets + ", " + headers[id] + " = " + value);
	//		}
	//
	//		for(int newIndex = 0;newIndex < oldHeaders.size();newIndex++)
	//		{
	//			int id = Integer.parseInt(oldHeaders.get(newIndex));
	//			//id = id + oldCount;
	//			String type = types[id];
	//			String value = null;
	//			if(type.equalsIgnoreCase("int") || type.equalsIgnoreCase("double"))
	//			{
	//				value = values[id];
	//				if(value == null || value.length() == 0)
	//					value = "null";
	//			}
	//			else
	//			{
	//				value = values[id];
	//				value = value.replace("'", "''");
	//				value = value.replace("\"", "");
	//				value = "'" + value + "'";
	//			}
	//
	//			if(newIndex == 0)
	//				wheres = new StringBuilder(wheres + headers[id] + " = " + value);
	//			else
	//				wheres = new StringBuilder(wheres + ", " + headers[id] + " = " + value);
	//		}
	//
	//		// and the final
	//		inserter = new StringBuilder(inserter + sets.toString() + wheres.toString());
	//
	//		return inserter.toString();
	//	}

	//    //make a query to alter a table (add headers)
	//    private String makeAlter(Vector<String> newHeaders, String tableName)
	//    {
	//    	createString = "ALTER TABLE " + tableName + " ADD (";
	//    	  		    	
	//    	for(int headerIndex = 0;headerIndex < newHeaders.size();headerIndex++)
	//    	{
	//    		int newIndex = Integer.parseInt(newHeaders.elementAt(headerIndex)); // get the index you want
	//    		String header = headers[newIndex]; // this is a new header - cool
	//    		
	//    		if(headerIndex == 0)
	//    			createString = createString +  header + "  " + types[newIndex];
	//    		else
	//    			createString = createString + ", " + header + "  " + types[newIndex];		
	//    	}
	//    	
	//    	//this.headers = capHeaders;
	//    	createString = createString + ")";
	//    	
	//    	return createString;
	//    }

	//make a query to alter a table (add headers)
	//    private String makeAlter(String[] newHeaders, String tableName, String[] headers, String[] types)
	//    {
	//    	String createString = "ALTER TABLE " + tableName + " ADD (";
	//    	  		    	
	//    	for(int headerIndex = 0;headerIndex < newHeaders.length;headerIndex++)
	//    	{
	////    		int newIndex = Integer.parseInt(newHeaders[headerIndex]); // get the index you want
	//    		int newIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(headers, newHeaders[headerIndex]);
	//    		String header = headers[newIndex]; // this is a new header - cool
	//    		
	//    		if(headerIndex == 0)
	//    			createString = createString +  header + "  " + types[newIndex];
	//    		else
	//    			createString = createString + ", " + header + "  " + types[newIndex];		
	//    	}
	//    	
	//    	//this.headers = capHeaders;
	//    	createString = createString + ")";
	//    	
	//    	return createString;
	//    }

	//  private void processAlterAsNewTable(String selectQuery, String joinType, String[] headers)
	//    {
	//    	try {
	//			// I need to get the names and types here
	//			// and then do the same magic as before
	//
	//    		ResultSet rs = executeQuery(selectQuery);
	//			
	//			String [] oldHeaders = headers;
	////			String [] oldTypes = types;
	//			
	//			
	//			int numCols = rs.getMetaData().getColumnCount();
	//			String [] newHeaders = new String[numCols];
	//			String [] newTypes = new String[numCols];
	//			
	//			ResultSetMetaData rsmd = rs.getMetaData();	
	//			for(int colIndex = 1;colIndex <= numCols;colIndex++)
	//			{
	//				// set the name and type
	//				int arrIndex = colIndex - 1;
	//				newHeaders[arrIndex] = rsmd.getColumnLabel(colIndex);
	//				newTypes[arrIndex] = rsmd.getColumnTypeName(colIndex);
	//			}
	//			
	//			Hashtable <Integer, Integer> matchers = new Hashtable <Integer, Integer>();
	//			String oldTable = tableName;
	//			String newTable = H2FRAME + getNextNumber() ;
	//			// now I need a creator
	//			String creator = "Create table " + newTable+ " AS "  + selectQuery;
	//			getConnection().createStatement().execute(creator);
	//			
	//			// find the matchers
	//    		// I need to find which ones are already there and which ones are new
	//    		for(int hIndex = 0;hIndex < newHeaders.length;hIndex++)
	//    		{
	//    			String uheader = newHeaders[hIndex];
	//    			uheader = cleanHeader(uheader);
	//
	//    			boolean old = false;
	//    			for(int oIndex = 0;oIndex < headers.length;oIndex++)
	//    			{
	//    				if(headers[oIndex].equalsIgnoreCase(uheader))
	//    				{
	//    					old = true;
	//    					matchers.put(hIndex, oIndex);
	//    					break;
	//    				}
	//    			}
	//    		}
	//			
	//			mergeTables(oldTable, newTable, matchers, oldHeaders, newHeaders, joinType);
	//			
	//		} catch (SQLException e) {
	//			// TODO Auto-generated catch block
	//			e.printStackTrace();
	//		}
	//    }


	//	/**
	//	 * 
	//	 * @param query - H2 query
	//	 * @return
	//	 * 
	//	 * returns an iterator based on the query parameter, returns null for an invalid query or if exception occurs running the query
	//	 */
	//	public Iterator buildIterator(String query){
	//		try {
	//			Statement stmt = getConnection().createStatement();
	//			ResultSet rs = stmt.executeQuery(query);
	//			return new H2Iterator(rs);
	//		} catch(SQLException s) {
	//			s.printStackTrace();
	//		}
	//
	//		return null;
	//	}


	//	/**
	//	 * 
	//	 * @param thisOutput - row to predict types on
	//	 * @return
	//	 * 
	//	 * Ex:
	//	 * 		thisOutput = ["0.1", "Value", "1"] will return ["Double", "VarChar(800)", "Double"]
	//	 */
	//	public String[] predictRowTypes(String[] thisOutput)
	//	{
	//		String[] types = new String[thisOutput.length];
	//
	//		for(int outIndex = 0;outIndex < thisOutput.length;outIndex++) {
	//			String curOutput = thisOutput[outIndex];
	//			Object [] cast = castToType(curOutput);
	//			if(cast == null) {
	//				cast = new Object[2];
	//				cast[0] = types[outIndex];
	//				cast[1] = ""; // make it into an empty String
	//			}
	//
	//			types[outIndex] = cast[0] + "";
	//		}
	//
	//		return types;
	//	}

	//	/**
	//	 * get all data from the table given the columnheaders as selectors
	//	 * 
	//	 * @param columnHeaders
	//	 * @return
	//	 */
	//	public List<Object[]> getData(List<String> columnHeaders) {
	//
	//		columnHeaders = cleanHeaders(columnHeaders);
	//		List<Object[]> data;
	//		try {
	//			String selectQuery = makeSelect(tableName, columnHeaders);
	//			ResultSet rs = executeQuery(selectQuery);
	//
	//			if(rs != null) {
	//
	//				ResultSetMetaData rsmd = rs.getMetaData();
	//				int NumOfCol = rsmd.getColumnCount();
	//				data = new ArrayList<>(NumOfCol);
	//				//collect and return
	//				while (rs.next()){
	//					Object[] row = new Object[NumOfCol];
	//
	//					for(int i = 1; i <= NumOfCol; i++) {
	//						row[i-1] = rs.getObject(i);
	//					}
	//					data.add(row);
	//				}
	//
	//				return data;
	//			}
	//
	//		} catch (SQLException e) {
	//			e.printStackTrace();
	//		}
	//
	//		return new ArrayList<Object[]>(0);
	//	}

	//	//get all rows that have value = value in column header = column
	//	/**
	//	 * Returns all data from the table with the given value for a specific column
	//	 * Ex: 
	//	 * 		columnHeaders = [Title, Studio]
	//	 * 		column = Studio
	//	 * 		value = WB
	//	 * 
	//	 * 		All titles and studios will be returned such that the studio is 'WB'
	//	 * 
	//	 * @param columnHeaders
	//	 * @param column
	//	 * @param value
	//	 * @return
	//	 */
	//	public List<Object[]> getData(List<String> columnHeaders, String column, Object value) {
	//
	//		List<Object[]> data;
	//		column = cleanHeader(column);
	//		columnHeaders = cleanHeaders(columnHeaders);
	//		try {
	//			String selectQuery = makeSpecificSelect(tableName, columnHeaders, column, value);
	//			ResultSet rs = executeQuery(selectQuery);
	//
	//			if(rs != null) {
	//
	//				ResultSetMetaData rsmd = rs.getMetaData();
	//				int NumOfCol = rsmd.getColumnCount();
	//				data = new ArrayList<>(NumOfCol);
	//				while (rs.next()){
	//					Object[] row = new Object[NumOfCol];
	//
	//					for(int i = 1; i <= NumOfCol; i++) {
	//						row[i-1] = rs.getObject(i);
	//					}
	//					data.add(row);
	//				}
	//
	//				return data;
	//			}
	//
	//		} catch (SQLException e) {
	//			e.printStackTrace();
	//		}
	//
	//		return new ArrayList<Object[]>(0);
	//	}


}



