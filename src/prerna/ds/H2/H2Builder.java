package prerna.ds.H2;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.h2.tools.RunScript;
import org.stringtemplate.v4.ST;

import prerna.ds.TinkerFrame;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

public class H2Builder {
	
	private static final Logger LOGGER = LogManager.getLogger(H2Builder.class.getName());
	
	String [] headers = null;
	String [] types = null;
	ST insertTemplate = null;
	String createString = null;
	String alterString = null;
	Vector <String> castTargets = new Vector<String>();
	Connection conn = null;
	boolean create = false;
	static int tableRunNumber = 1;
	static int rowCount = 0;
	private static final String tempTable = "TEMP_TABLE98793";
	String brokenLines = "";
	
	Map<String, List<Object>> filterHash = new HashMap<>();
	
	String tableName; 
	
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
	
	/*************************** TEST **********************************************/
	public static void main(String[] a) throws Exception {
		
		String testString = "0.27";
		System.out.println(NumberUtils.isNumber(testString));
		
		
//		new H2Builder().testMakeFilter();
    	H2Builder test = new H2Builder();
//    	test.castToType("6/2/2015");
		String fileName = "C:/Users/rluthar/Desktop/datasets/Movie.csv";
//		long before, after;
		fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/Remedy New.csv";
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
		
		/*String query = "Select  bencat, count(*) from tinkertable where sex='F' Group By Bencat";
		
		rs = conn.createStatement().executeQuery(query);
		
		
		while(rs.next())
		{
			System.out.print("Bencat.. " + rs.getObject(1));
			System.out.println("Count.. " + rs.getObject(2));
		}*/
    }
    /*************************** END TEST ******************************************/
    
    /*************************** CONSTRUCTORS **************************************/
    
    public H2Builder() {
    	//initialize a connection
    	getConnection();
    	tableName = "TINKERFRAME"+tableRunNumber;
    	tableRunNumber++;
    }
    
    /*************************** END CONSTRUCTORS **********************************/
    
    private Object[] castToType(String input)
    {
    	//System.out.println("String that came in.. " + input);
    	Object [] retObject = null;
    	if(input != null)
    	{
	    	Object retO = null;
//	    	if(input.equalsIgnoreCase("1") || input.equalsIgnoreCase("0"))
	    	if(input.equalsIgnoreCase("true") || input.equalsIgnoreCase("false"))
	    	{
	    		retObject = new Object[2];
	    		retObject[0] = "boolean";
	    		retObject[1] = retO;
	    		
	    	}	    	
	    	else if(NumberUtils.isDigits(input))
	    	{
	    		retO = Integer.parseInt(input);
	    		retObject = new Object[2];
	    		retObject[0] = "int";
	    		retObject[1] = retO;
	    	}
//	    	else if(NumberUtils.isNumber(input))
	    	else if((retO = getDouble(input)) != null )
	    	{
//	    		retO = Double.parseDouble(input);
	    		retObject = new Object[2];
	    		retObject[0] = "double";
	    		retObject[1] = retO;
	    	}
	    	else if((retO = getDate(input)) != null )// try dates ? - yummy !!
	    	{
	    		retObject = new Object[2];
	    		retObject[0] = "date";
	    		retObject[1] = retO;
	    		
	    	}
	    	else if((retO = getCurrency(input)) != null )
	    	{
	    		retObject = new Object[2];
	    		
	    		if(retO.toString().equalsIgnoreCase(input)) {
	    			retObject[0] = "varchar(800)";
	    		} else {
	    			retObject[0] = "double";
	    		}
	    		retObject[1] = retO;
	    	}
	    	else
	    	{
	    		retObject = new Object[2]; // need to do some more stuff to determine this
	    		retObject[0] = "varchar(800)";
	    		retObject[1] = input;
	    	}
    	}
    	return retObject;
    }
	    
    private String getDate(String input)
    {
    	String[] date_formats = {
                //"dd/MM/yyyy",
                "MM/dd/yyyy",
                //"dd-MM-yyyy",
                "yyyy-MM-dd",
                "yyyy/MM/dd", 
                "yyyy MMM dd",
                "yyyy dd MMM",
                "dd MMM yyyy",
                "dd MMM",
                "MMM dd",
                "dd MMM yyyy",
                "MMM yyyy"
        };

		String output_date = null;
		boolean itsDate = false;
		for (String formatString : date_formats)
		{
		try
		{    
		 Date mydate = new SimpleDateFormat(formatString).parse(input);
		 SimpleDateFormat outdate = new SimpleDateFormat("yyyy-MM-dd");
		 output_date = outdate.format(mydate);
		 itsDate = true;
		 break;
		}
			catch (ParseException e) {
				//System.out.println("Next!");
			}
		}
		
		return output_date;	
    }
	    
    private Object getCurrency(String input)
    {
    	if(input.indexOf("-") > 0)
			return input;
    	
    	Number nm = null;
    	NumberFormat nf = NumberFormat.getCurrencyInstance();
    	
//    	if(input.indexOf("-") > 0) {
    		try {
	    		nm = nf.parse(input);
	    	} catch (Exception ex) {
	 
	    	}
//    	}
    		// a simpler way to test is to see if the $ removed matches the value

    	
    	return nm;
    }
    
    private Double getDouble(String input) {
    	try {
    		Double num = Double.parseDouble(input);
    		return num;
    	} catch(NumberFormatException e) {
    		return null;
    	}
    }


    //build a query to insert values into a new table 
   



	    	    
    public String[] predictRowTypes(String[] thisOutput)
    {
    	types = new String[thisOutput.length];
    	String [] values = new String[thisOutput.length];
    	
    	for(int outIndex = 0;outIndex < thisOutput.length;outIndex++)
    	{
    		String curOutput = thisOutput[outIndex];
    		//if(headers != null)
    		//	System.out.println("Cur Output...  " + headers[outIndex] + " >> " + curOutput );
    		Object [] cast = castToType(curOutput);
    		if(cast == null)
    		{
    			cast = new Object[2];
    			cast[0] = types[outIndex];
    			cast[1] = ""; // make it into an empty String
    		}
    		if((cast[0] + "").equalsIgnoreCase("Date") || (cast[0] + "").equalsIgnoreCase("Currency"))
    			castTargets.addElement(outIndex + "");
    		types[outIndex] = cast[0] + "";
    		values[outIndex] = cast[1] + "";
    		
    		//System.out.println(curOutput + types[outIndex] + " <<>>" + values[outIndex]);
    	}
    	
    	//insertTemplate = makeTemplate(types, types, new Hashtable<String, String>());
    	//System.out.println("The output is ..  " + thisOutput);
    	
    	return values;
    }
    
    public String[] castRowTypes(String [] thisOutput)
    {
//    	return Utility.castToTypes(thisOutput, types);
    	String [] values = new String[thisOutput.length];
    	
    	for(int outIndex = 0;outIndex < thisOutput.length;outIndex++)
    	{
    		if(thisOutput[outIndex] != null)
    		{
    			values[outIndex] = thisOutput[outIndex] + "";
    		
	    		if(thisOutput[outIndex] != null && castTargets.contains(outIndex + ""))
	    		{
	    			if(types[outIndex].equalsIgnoreCase("Date"))
	    				values[outIndex] = getDate(thisOutput[outIndex]);
	    			else // this is a currency
	    				values[outIndex] = getCurrency(thisOutput[outIndex]) + "";
	    		}
	    		else if(thisOutput[outIndex].length() > 800)
	    		{
	    			values[outIndex] = thisOutput[outIndex].substring(0, 798);
	    		}
    		}
    		else values[outIndex] = "";
    	}
    	return values;
    }


    public void testMakeFilter() {
    	
    	List<Object> list1 = new ArrayList<>();
    	list1.add("value1");
    	list1.add("value2");
    	list1.add("value3");
    	list1.add("value4");
    	filterHash.put("column1", list1);
    	
    	List<Object> list2 = new ArrayList<>();
    	list2.add("valueA");
    	list2.add("valueB");
    	list2.add("valueC");
    	list2.add("valueD");
    	filterHash.put("column2", list2);
    	
    	List<Object> list3 = new ArrayList<>();
    	list3.add("value1A");
    	list3.add("value2B");
    	list3.add("value3C");
    	list3.add("value4D");
    	filterHash.put("column3", list3);
    	
    	List<String> headers = new ArrayList<>();
    	headers.add("column1");
    	headers.add("column2");
    	headers.add("column3");
    	
    	
    	String makeQuery = this.makeSelect("TestTable", headers);
    	System.out.println(makeQuery);

    }
    /*************************** CREATE ******************************************/
	    
    //need safety checking for header names
    public void create(String[] headers) {
    	this.headers = headers;		
    }

    //create a table with the data given the columnheaders with the tableName
    // I am not sure if we even use it now
    private void generateTable(String[][] data, String[] columnHeaders, String tableName) {
    	
    	try {
			predictRowTypes(data[0]);
			String createTable = makeCreate(columnHeaders, tableName);
			runQuery(createTable);
			
			
			for(String[] row : data) {
//    		String[] cells = castRowTypes(row);
				String[] cells = Utility.castToTypes(row, types);
				String inserter = makeInsert(columnHeaders, types, cells, new Hashtable<String, String>(), tableName);
				runQuery(inserter);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    /*************************** END CREATE **************************************/
    
    
    /*************************** READ ********************************************/
    
    //get all data from the table given the columnheaders as selectors
    public List<Object[]> getData(List<String> columnHeaders) {
        
    	columnHeaders = cleanHeaders(columnHeaders);
        List<Object[]> data;
    	try {
    		String selectQuery = makeSelect(tableName, columnHeaders);
    		ResultSet rs = executeQuery(selectQuery);
			
			if(rs != null) {
			
				ResultSetMetaData rsmd = rs.getMetaData();
		        int NumOfCol = rsmd.getColumnCount();
		        data = new ArrayList<>(NumOfCol);
		        while (rs.next()){
		            Object[] row = new Object[NumOfCol];

		            for(int i = 1; i <= NumOfCol; i++) {
		                row[i-1] = rs.getObject(i);
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
    
    //get all rows that have value = value in column header = column
    public List<Object[]> getData(List<String> columnHeaders, String column, Object value) {
        
        List<Object[]> data;
        column = cleanHeader(column);
        columnHeaders = cleanHeaders(columnHeaders);
    	try {
    		String selectQuery = makeSpecificSelect(tableName, columnHeaders, column, value);
    		ResultSet rs = executeQuery(selectQuery);
			
			if(rs != null) {
			
				ResultSetMetaData rsmd = rs.getMetaData();
		        int NumOfCol = rsmd.getColumnCount();
		        data = new ArrayList<>(NumOfCol);
		        while (rs.next()){
		            Object[] row = new Object[NumOfCol];

		            for(int i = 1; i <= NumOfCol; i++) {
		                row[i-1] = rs.getObject(i);
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
    
    //get scaled version of above method
    public List<Object[]> getScaledData(List<String> columnHeaders, String column, Object value, Double[] maxArr, Double[] minArr) {
        
        List<Object[]> data;
        column = cleanHeader(column);
        columnHeaders = cleanHeaders(columnHeaders);
    	try {
    		String selectQuery = makeSpecificSelect(tableName, columnHeaders, column, value);
    		ResultSet rs = executeQuery(selectQuery);
			
			if(rs != null) {
			
				ResultSetMetaData rsmd = rs.getMetaData();
		        int NumOfCol = rsmd.getColumnCount();
		        data = new ArrayList<>(NumOfCol);
		        while (rs.next()){
		            Object[] row = new Object[NumOfCol];

		            for(int i = 1; i <= NumOfCol; i++) {
		                Object val = rs.getObject(i);
		                if(types[i-1].equalsIgnoreCase("int") || types[i-1].equalsIgnoreCase("double")) {
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
    
    //get the column from the table, distinct values - true or false
    public Object[] getColumn(String columnHeader, boolean distinct) {
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
    
    //get the max/min/count/sum/avg of the column
    public Double getStat(String columnHeader, String statType) {
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
    
    
    public String[] getTypes() {
    	return types;
    }
    
    //drop the column from the table
    public void dropColumn(String columnHeader) {
    	
    	try {
			runQuery(makeDropColumn(columnHeader, tableName));
			removeHeader(columnHeader);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    //only use this for analytics for now
    public void updateTable(Object[] values, String[] columnHeaders) {
    	if(columnHeaders.length != 2) {
    		throw new UnsupportedOperationException("multi column join not implemented");
    	}
    	
    	try {
			String joinColumn = columnHeaders[0];
			String newColumn = columnHeaders[1];
			
			Object joinValue = values[0];
			Object newValue = values[1];
			
			if(!ArrayUtilityMethods.arrayContainsValue(headers, newColumn)) {
				//alter the table
				
				//update the types
				String type;
				if(NumberUtils.isDigits(newValue.toString())) {
					type = "int";
				}
				else if(getDouble(newValue.toString()) != null ) {
					type = "double";
				}
				else {
					type = "varchar(800)";
				}
				
				//update the headers
				addHeader(newColumn, type);
				
				String[] newHeaders = new String[]{newColumn};
				String alterQuery = makeAlter(newHeaders, tableName);
				runQuery(alterQuery);
			}
			
			String updateQuery = makeUpdate(tableName, joinColumn, newColumn, joinValue, newValue);
			runQuery(updateQuery);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    /*************************** END READ ****************************************/
    
    
    /*************************** UPDATE ******************************************/
   
    //process the wrapper from processDataMakerComponent in TinkerH2Frame to generate a table then add that table
    public void processWrapper(ISelectWrapper wrapper) {
    	String[][] data = getData(wrapper);
    	String[] newHeaders = wrapper.getDisplayVariables();
    	newHeaders = cleanHeaders(newHeaders);
    	if(headers == null) {
    		this.headers = newHeaders;
    		generateTable(data, newHeaders, tableName);
    	} else
    		processAlterData(data, newHeaders);
    }
    
    //process a group by - calculate then make a table then merge the table
    public void processGroupBy(String column, String newColumnName, String valueColumn, String mathType) {
    	column = cleanHeader(column);
    	newColumnName = cleanHeader(newColumnName);
    	valueColumn = cleanHeader(valueColumn);
    	
    	String inserter = makeGroupBy(column, valueColumn, mathType, newColumnName, this.tableName);
    	processAlterAsNewTable(inserter, Join.LEFT_OUTER.getName());

    	//all group bys are doubles?
    	addHeader(newColumnName, "double");
    }
    
    private void processAlterAsNewTable(String selectQuery, String joinType)
    {
    	try {
			// I need to get the names and types here
			// and then do the same magic as before

    		ResultSet rs = getConnection().createStatement().executeQuery(selectQuery);
			
			String [] oldHeaders = headers;
			String [] oldTypes = types;
			
			
			int numCols = rs.getMetaData().getColumnCount();
			String [] newHeaders = new String[numCols];
			String [] newTypes = new String[numCols];
			
			ResultSetMetaData rsmd = rs.getMetaData();	
			for(int colIndex = 1;colIndex <= numCols;colIndex++)
			{
				// set the name and type
				int arrIndex = colIndex - 1;
				newHeaders[arrIndex] = rsmd.getColumnLabel(colIndex);
				newTypes[arrIndex] = rsmd.getColumnTypeName(colIndex);
			}
			
			Hashtable <Integer, Integer> matchers = new Hashtable <Integer, Integer>();
			String oldTable = tableName;
			String newTable = "TINKERTABLE" + getNextNumber() ;
			// now I need a creator
			String creator = "Create table " + newTable+ " AS "  + selectQuery;
			getConnection().createStatement().execute(creator);
			
			// find the matchers
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
    					matchers.put(hIndex, oIndex);
    					break;
    				}
    			}
    		}
			
			mergeTables(oldTable, newTable, matchers, oldHeaders, newHeaders, joinType);
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	
    	
    }
    
    //turn the wrapper data to double array
    private String[][] getData(ISelectWrapper wrapper) {
    	List<String[]> data = new ArrayList<String[]>();
    	String[] colHeaders = wrapper.getDisplayVariables();
    	colHeaders = cleanHeaders(colHeaders);
    	int length = colHeaders.length;
    	while(wrapper.hasNext()) {
    		String[] rowData = new String[length];
    		ISelectStatement statement = wrapper.next();
    		Map<String, Object> nextRow = statement.getPropHash();
    		for(int i = 0; i < length; i++) {
    			rowData[i] = nextRow.get(colHeaders[i]).toString();
    		}
    		data.add(rowData);
    	}
    	
    	return data.toArray(new String[][]{});
    }
    
//    private String[] getRow(Map<String, Object> row) {
//    	String[] rowData = new String[row.size()];
//		for(int i = 0; i < length; i++) {
//			rowData[i] = (String)rowRow.get(colHeaders[i]);
//		}
//    }
   
    /**
     * 
     * @param cells
     * 
     * add cells to the table 
     */
    public void addRow(String[] cells) {
    	
    	//if first row being added to the table establish the types
    	try
    	{
	    	if(types == null) {
	    		predictRowTypes(cells);
	    		String createTable = makeCreate(headers, tableName);
	    		runQuery(createTable);
	    		create = true;
	    	}
    	}catch(Exception ex)
    	{
    		brokenLines = "Headers are not properly formatted - Discontinued processing";
    	}
	    
    	try
    	{
	    	if(create)
	    	{
	    		rowCount++;
		    	cells = Utility.castToTypes(cells, types);
				String inserter = makeInsert(headers, types, cells, new Hashtable<String, String>(), tableName);
				runQuery(inserter);
	    	}
    	}catch (Exception ex)
    	{
    		System.out.println("Errored.. nothing to do");
    		brokenLines = brokenLines + " : " + rowCount;
    	}
    }
    
    //aggregate filters
    public void addFilters(String columnHeader, List<Object> values) {
    	columnHeader = cleanHeader(columnHeader);
    	if(filterHash.get(columnHeader) == null) {
    		filterHash.put(columnHeader, values);
    	} else {
    		filterHash.get(columnHeader).addAll(values);
    	}
    }
    
    //overwrite previous filters with new list
    public void setFilters(String columnHeader, List<Object> values) {
    	columnHeader = cleanHeader(columnHeader);
    	filterHash.put(columnHeader, values);
    }
    
    public void removeFilter(String columnHeader) {
    	columnHeader = cleanHeader(columnHeader);
    	filterHash.remove(columnHeader);
    }
    
    public void clearFilters() {
    	filterHash.clear();
    }
    
    public void setHeaders(String[] headers) {
    	this.headers = cleanHeaders(headers);
    }
    
    //use this for the filtered half of filter model
    public Map<String, List<Object>> getFilteredValues(List<String> selectors) {
    	
    	Map<String, List<Object>> returnFilterMap = new HashMap<>();
    	
    	try {
    	for(String selector : selectors) {
    		
    		if(filterHash.get(selector) != null) {
	    		String query = makeNotSelect(tableName, selector);
	    		ResultSet rs = executeQuery(query);
	    		
	    		List<Object> filterData = null;
	    		if(rs != null) {
					ResultSetMetaData rsmd = rs.getMetaData();
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
 
    //build an iterator with the selectors
    public Iterator buildIterator(List<String> selectors) {
    	try {
	    	Statement stmt = getConnection().createStatement();
			String selectQuery = makeSelect(tableName, selectors);
			ResultSet rs = stmt.executeQuery(selectQuery);
	    	return new TinkerH2Iterator(rs);
    	} catch(SQLException s) {
    		s.printStackTrace();
    	}
    	
    	return null;
    }
    
    //build the new way to create iterator with all the options
    public Iterator buildIterator(Map<String, Object> options) {
    		
		String sortDir = (String)options.get(TinkerFrame.SORT_BY_DIRECTION);
		
		Boolean dedup = (Boolean) options.get(TinkerFrame.DE_DUP);
		if(dedup == null) dedup = false;
		
		Integer limit = (Integer) options.get(TinkerFrame.LIMIT);
		Integer offset = (Integer) options.get(TinkerFrame.OFFSET);
		String sortBy = (String)options.get(TinkerFrame.SORT_BY);
		List<String> selectors = (List<String>) options.get(TinkerFrame.SELECTORS);
		selectors = cleanHeaders(selectors);
		String selectQuery;
		if(dedup) {
			selectQuery = makeSelectDistinct(tableName, selectors);
		} else {
			selectQuery = makeSelect(tableName, selectors);
		}
		
		if(sortBy != null) {
			
			sortBy = cleanHeader(sortBy);
			selectQuery += " sort by " + sortBy;
//			if(sortDir != null) {
//				
//			}
		}
		if(limit != null && limit > 0) {
			selectQuery += " limit "+limit;
		}
		if(offset != null && offset > 0) {
			selectQuery += " offset "+offset;
		}
		
		ResultSet rs = executeQuery(selectQuery);
    	return new TinkerH2Iterator(rs);
    	
    }
    
    //process to join the data and new headers to existing table
    private void processAlterData(String[][] data, String[] newHeaders)
    {
    	// this currently doesnt handle many to many joins and such
    	try {
    		getConnection();
	    	
	    	// I need to do an evaluation here to find if this one to many
	    	String [] oldHeaders = headers;

	    	//headers for the joining table
    		
    		int curHeadCount = headers.length;
    		Vector <String> newHeaderIndices = new Vector<String>();
    		Vector <String> oldHeaderIndices = new Vector<String>();
    		
    		Hashtable <Integer, Integer> matchers = new Hashtable<Integer, Integer>();
    		
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
    		
    		headers = newHeaders;
				    		
    		String [] oldTypes = types; // I am not sure if I need this we will see - yes I do now yipee
    		
			String[] cells = data[0];
			predictRowTypes(cells);

			String[] newTypes = types;
			
			//stream.close();
			
			boolean one2Many = false;
			// I also need to accomodate when there are no common ones
			if(oldHeaderIndices.size() == 0)
			{
				// this is the case where it has not been created yet
				generateTable(data, newHeaders, tableName);
			}
			else
			{
				// close the stream and open it again
				
				// do a different process here
				// I need to create a new database here
				one2Many = true;
				generateTable(data, newHeaders, "TINKERTABLE" + getNextNumber());
				// not bad -- this does it ?
				// whoa hang on I need to handle the older headers and such
				// done in reset headers
			}
			
			// need to create a new database with everything now
			// reset all the headers
			if(one2Many)
			resetHeaders(oldHeaders, headers, newHeaderIndices, oldTypes, types, curHeadCount);
    		// just test to see if it finally came through
			
			// now I need to assimilate everything into one
			String tableName2 = "TINKERTABLE" + tableRunNumber;
			String tableName1 = tableName;
			
			if(one2Many)
				mergeTables(tableName1, tableName2, matchers, oldHeaders, newHeaders, Join.INNER.getName());
			
			testData();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
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
    	
    	// now create a third table
    	tableName = "TINKERTABLE"+getNextNumber();
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
			
			//stmt.execute("DROP TABLE " + tableName1);
			//stmt.execute("DROP TABLE " + tableName2);
		} catch (SQLException e) {
			e.printStackTrace();
		}
    }
    
    private int getNextNumber()
    {
    	tableRunNumber++;
    	return tableRunNumber;
    }
    
    private void resetHeaders(String [] oldHeaders, String [] curHeaders, Vector <String> newHeaders, String [] oldTypes, String [] types, int curHeadCount)
    {
		// reset the headers
		String [] finalHeaders = new String[oldHeaders.length + newHeaders.size()];
		String [] finalTypes = new String[oldTypes.length + newHeaders.size()];
		
		System.arraycopy(oldHeaders, 0, finalHeaders, 0, oldHeaders.length);
		System.arraycopy(oldTypes, 0, finalTypes, 0, oldTypes.length);
		
		Vector <String> modNewHeaders = new Vector<String>(); //add the counts and push it
		    		
		for(int newHeadIndex = 0;newHeadIndex < newHeaders.size();newHeadIndex++)
		{
			// cast into the header
			int headIndex = Integer.parseInt(newHeaders.elementAt(newHeadIndex));
			String uheader = curHeaders[headIndex];
			uheader = cleanHeader(uheader);
   
			finalHeaders[newHeadIndex + curHeadCount] = uheader; 	    			
			
			modNewHeaders.add((newHeadIndex + curHeadCount) + "");
			
			// cast into types
			finalTypes[newHeadIndex + curHeadCount] = types[headIndex];	    			
		}
		
		newHeaders = modNewHeaders;
		this.headers = finalHeaders;
		this.types = finalTypes;

    }
    
    private Connection getConnection()
    {
    	if(this.conn == null)
    	{
			try {
				
				Class.forName("org.h2.Driver");
				//jdbc:h2:~/test
				
				//this will have to update
				this.conn = DriverManager.getConnection("jdbc:h2:mem:test:LOG=0;CACHE_SIZE=65536;LOCK_MODE=1;UNDO_LOG=0", "sa", "");
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
    
    public String getTableName() {
    	return tableName;
    }
	
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
    
    private String cleanHeader(String header) {
    	/*header = header.replaceAll(" ", "_");
    	header = header.replace("(", "_");
    	header = header.replace(")", "_");
    	header = header.replace("-", "_");
    	header = header.replace("'", "");*/
    	header = header.replaceAll("[#%!&()@#$'./_-]*", ""); // replace all the useless shit in one go
    	header = header.replaceAll("\\s+","_"); 
    	if(Character.isDigit(header.charAt(0)))
    		header = "c_" + header;
    	return header;
    }
    
    private String cleanInstance(String value) {
    	return value.replace("'", "''");
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
    		
    		String name = headers[colIndex];
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
    	
//    	return template.toString();
    	return inserter.toString();
    }

    //generate a query to update a table
    private String makeUpdate(Vector<String> newHeaders, Vector<String> oldHeaders, String[] types, String[] values, int oldCount, Hashtable<String, String> defaultValues)
    {
    	oldCount = 0;
    	StringBuilder inserter = new StringBuilder("UPDATE " + tableName + " SET " ); 
    	
    	StringBuilder sets = new StringBuilder(" ");
    	StringBuilder wheres = new StringBuilder(" WHERE  ");
    	
    	// first get all of the new headers
    	for(int newIndex = 0;newIndex < newHeaders.size();newIndex++)
    	{
    		int id = Integer.parseInt(newHeaders.get(newIndex));
    		//id = id - oldCount;
    		String type = types[id];
    		String value = null;
    		if(type.equalsIgnoreCase("int") || type.equalsIgnoreCase("double"))
    		{
    			value = values[id-oldCount];
    			if(value == null || value.length() == 0)
    				value = "null";
    		}
    		else
    		{
    			value = values[id-oldCount];
    			value = value.replace("'", "''");
    			value = value.replace("\"", "");
    			value = "'" + value + "'";
    		}

    		// values
    		if(newIndex == 0)
    			sets = new StringBuilder(headers[id] + " = " + value);
    		else
    			sets = new StringBuilder(sets + ", " + headers[id] + " = " + value);
    	}

    	for(int newIndex = 0;newIndex < oldHeaders.size();newIndex++)
    	{
    		int id = Integer.parseInt(oldHeaders.get(newIndex));
    		//id = id + oldCount;
    		String type = types[id];
    		String value = null;
    		if(type.equalsIgnoreCase("int") || type.equalsIgnoreCase("double"))
    		{
    			value = values[id];
    			if(value == null || value.length() == 0)
    				value = "null";
    		}
    		else
    		{
    			value = values[id];
    			value = value.replace("'", "''");
    			value = value.replace("\"", "");
    			value = "'" + value + "'";
    		}

    		if(newIndex == 0)
    			wheres = new StringBuilder(wheres + headers[id] + " = " + value);
    		else
    			wheres = new StringBuilder(wheres + ", " + headers[id] + " = " + value);
    	}

    	// and the final
    	inserter = new StringBuilder(inserter + sets.toString() + wheres.toString());
    	
    	return inserter.toString();
    }

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
    private String makeAlter(String[] newHeaders, String tableName)
    {
    	createString = "ALTER TABLE " + tableName + " ADD (";
    	  		    	
    	for(int headerIndex = 0;headerIndex < newHeaders.length;headerIndex++)
    	{
//    		int newIndex = Integer.parseInt(newHeaders[headerIndex]); // get the index you want
    		int newIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(headers, newHeaders[headerIndex]);
    		String header = headers[newIndex]; // this is a new header - cool
    		
    		if(headerIndex == 0)
    			createString = createString +  header + "  " + types[newIndex];
    		else
    			createString = createString + ", " + header + "  " + types[newIndex];		
    	}
    	
    	//this.headers = capHeaders;
    	createString = createString + ")";
    	
    	return createString;
    }

    //drop a table
    private String makeDropTable(String name) {
    	return "DROP TABLE " + name;
    }
    
    //make a select query
    private String makeSelect(String tableName, List<String> selectors) {
    	
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
    	
    	String filterSubQuery = makeFilterSubQuery();
    	selectStatement += " FROM " + tableName + filterSubQuery;
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
    	
    	String filterSubQuery = makeFilterSubQuery();
    	selectStatement += " FROM " + tableName + filterSubQuery;
    	
    	return selectStatement;
    }
    
    
    private String makeNotSelect(String tableName, String selector) {
    	String selectStatement = "SELECT DISTINCT ";
    	
		selector = cleanHeader(selector);
		selectStatement += selector + " FROM " + tableName;    	
    	
    	String filterStatement = "";
    		
		List<Object> filterValues = filterHash.get(selector);
		String listString = getQueryStringList(filterValues);
    		
		filterStatement += selector+" NOT IN " + listString;
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
    
    //create a table with headers and name
    private String makeCreate(String[] headers, String tableName)
    {
    	headers = cleanHeaders(headers);
    	createString = "CREATE TABLE " + tableName + " (";	    	
    	
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
    	
    	this.headers = capHeaders;
    	createString = createString + ")";
    	
    	return createString;
    }
    
    private String makeCreate(String tableName, String subquery) {
    	String createQuery = "CREATE TABLE "+tableName+" AS " + subquery;
    	return createQuery;
    }
    //build the query to create a new tinker table given the types with default values
    private String makeTemplate(String[] headers, String[] types, Hashtable<String, String> defaultValues)
    {
    	
    	//building query
    	StringBuilder inserter = new StringBuilder("INSERT INTO "+tableName+"(" ); 
    	StringBuilder template = new StringBuilder("(");
    	
    	for(int colIndex = 0;colIndex < headers.length;colIndex++)
    	{
    		// I need to find the type and based on that adjust what I want
    		String type = types[colIndex];
    		
    		// if null on integer - empty
    		// if null on string - ''
    		// if currenncy - empty
    		// if date - empty
    		
    		String name = headers[colIndex];
    		StringBuilder thisTemplate = new StringBuilder(name);
    		
    		
    		if(type.equalsIgnoreCase("int") || type.equalsIgnoreCase("double") || type.equalsIgnoreCase("date"))
    		{
    			if(!defaultValues.containsKey(type))
    				thisTemplate = new StringBuilder("<" + thisTemplate + "; null=\"\">");
    			else
    				thisTemplate = new StringBuilder("<" + thisTemplate + "; null=\"" + defaultValues.get(type) + "\">");
    		}
    		else
    		{
    			if(!defaultValues.containsKey(type))
    				thisTemplate = new StringBuilder("'<" + thisTemplate + "; null=\"''\">'");
    			else
    				thisTemplate = new StringBuilder("'<" + thisTemplate + "; null=\"" + defaultValues.get(type) + "\">'");

    		}
    		if(colIndex == 0)
    		{
    			inserter = new StringBuilder(inserter + "\"" + name + "\"");
	    		template = new StringBuilder(template + "" + thisTemplate);
    		}
    		else
    		{
    			inserter = new StringBuilder(inserter + " , \"" + name + "\"");
	    		template = new StringBuilder(template + " , " + thisTemplate);
    		}
    	}
    	
    	inserter = new StringBuilder(inserter + ")  VALUES  ");
    	template = new StringBuilder(inserter + "" + template + ")");
    	
    	//System.out.println(".. Template..  " + template);
    	
    	
    	return template.toString();
    }
    
    private String makeGroupBy(String column, String valueColumn, String mathType, String alias, String tableName) {
    	
    	column = cleanHeader(column);
    	valueColumn = cleanHeader(valueColumn);
    	alias = cleanHeader(alias);
    	
    	String functionString = "";
    	
    	String type = getTypeOfColumn(column);
    			
    	switch(mathType.toUpperCase()) {
    	case "COUNT": {
    		String func = "COUNT(";
    		if(type.startsWith("varchar"))
    			func = "COUNT( DISTINCT ";
    		functionString = func +valueColumn+")"; break; 
    		}
    	case "AVERAGE": {functionString = "AVG("+valueColumn+")";  break; }
    	case "MIN": {functionString = "MIN("+valueColumn+")";  break; }
    	case "MAX": {functionString = "MAX("+valueColumn+")"; break; }
    	case "SUM": {functionString = "SUM("+valueColumn+")"; break; }
    	default: {
    		
    		String func = "COUNT(";
    		if(type.startsWith("varchar"))
    			func = "COUNT( DISTINCT ";
    		functionString = func +valueColumn+")"; break; }
    	}
    	
    	String filterSubQuery = makeFilterSubQuery();
    	String groupByStatement = "SELECT " + column+", "+functionString + " AS " + alias +" FROM "+tableName + filterSubQuery + " GROUP BY "+ column;
    	
    	return groupByStatement;
    }
    
private String makeGroupBy(String[] column, String valueColumn, String mathType, String alias, String tableName) {
    	if(column.length == 1) return makeGroupBy(column[0], valueColumn, mathType, alias, tableName);
    	String column1 = cleanHeader(column[0]);
    	String column2 = cleanHeader(column[1]);
    	valueColumn = cleanHeader(valueColumn);
    	alias = cleanHeader(alias);
    	
    	String functionString = "";
    	switch(mathType.toUpperCase()) {
    	case "COUNT": {functionString = "COUNT("+valueColumn+")"; break; }
    	case "AVERAGE": {functionString = "AVG("+valueColumn+")";  break; }
    	case "MIN": {functionString = "MIN("+valueColumn+")";  break; }
    	case "MAX": {functionString = "MAX("+valueColumn+")"; break; }
    	case "SUM": {functionString = "SUM("+valueColumn+")"; break; }
    	default: {functionString = "COUNT("+valueColumn+")"; break; }
    	}
    	
    	String filterSubQuery = makeFilterSubQuery();
    	String groupByStatement = "SELECT " + column1+", "+column2+", "+functionString + " AS " + alias +" FROM "+tableName + filterSubQuery + " GROUP BY "+ column1+", "+column2;
    	
    	return groupByStatement;
    }
    
    private String getTypeOfColumn(String column)
    {
    	int index = 0;
    	for(int headIndex = 0;headIndex < headers.length;headIndex++)
    	{
    		if(headers[headIndex].equalsIgnoreCase(column))
    		{
    			index = headIndex;
    			break;
    		}
    	}	
    	return types[index];
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
    
    private String makeDropColumn(String column, String tableName) {
    	column = cleanHeader(column);
    	String dropColumnQuery = "ALTER TABLE "+tableName+" DROP COLUMN " + column;
    	return dropColumnQuery;
    }
    
    private String makeUpdate(String tableName, String joinColumn, String newColumn, Object joinValue, Object newValue) {
    	joinColumn = cleanHeader(joinColumn);
    	newColumn = cleanHeader(newColumn);
    	joinValue = cleanInstance(joinValue.toString());
    	newValue = cleanInstance(newValue.toString());
    	String updateQuery = "UPDATE "+tableName+" SET "+newColumn+"="+"'"+newValue+"'"+" WHERE "+joinColumn+"="+"'"+joinValue+"'";
    	return updateQuery;
    }
    
    private String makeFilterSubQuery() {
    	
    	String filterStatement = "";
    	if(filterHash.keySet().size() > 0) {
	    	
	    	List<String> filteredColumns = new ArrayList<String>(filterHash.keySet());
	    	for(int x = 0; x < filteredColumns.size(); x++) {
	    		
	    		String header = filteredColumns.get(x);
	    		List<Object> filterValues = filterHash.get(header);
	    		String listString = getQueryStringList(filterValues);
	    		
	    		filterStatement += header+" in " + listString;
	    		
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
    
    //save the main table
    //need to update this if we are saving multiple tables
    public void save(String fileName) {
    	List<String> selectors = new ArrayList<String>(headers.length);
    	for(String header : headers) {
    		selectors.add(header);
    	}
    	try {
			String createQuery = "CREATE TABLE "+tempTable+" AS "+makeSelect(tableName, selectors);
			runQuery(createQuery);
			String saveScript = "SCRIPT TO '"+fileName+"' COMPRESSION GZIP TABLE "+tempTable;
			runQuery(saveScript);
			String dropQuery = makeDropTable(tempTable);
			runQuery(dropQuery);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public static H2Builder open(String fileName) {
    	H2Builder builder = new H2Builder();
    	String tableName = "TINKERTABLE" + builder.getNextNumber(); 
    	String openScript = "RUNSCRIPT FROM '"+fileName+"' COMPRESSION GZIP ";
    	String createQuery = "CREATE TABLE "+tableName+" AS SELECT * FROM "+tempTable;
//    	RunScript.execute(builder.getConnection(), new FileReader(fileName))
    	try {
			builder.runQuery(openScript);
			builder.runQuery(createQuery);
			builder.tableName = tableName;
			
			String dropQuery = builder.makeDropTable(tempTable);
			builder.runQuery(dropQuery);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	return builder;
    }
    
    private void addHeader(String columnHeader, String type) {
    	//update the headers
    	columnHeader = cleanHeader(columnHeader);
		String[] updatedHeaders = new String[headers.length+1];
		String[] updatedTypes = new String[types.length+1];
		updatedHeaders[updatedHeaders.length - 1] = columnHeader;
		updatedTypes[updatedTypes.length -1 ] = type;
		for(int i = 0; i < headers.length; i++) {
			updatedHeaders[i] = headers[i];
			updatedTypes[i] = types[i];
		}
		this.headers = updatedHeaders;
		this.types = updatedTypes;
    }
    
    //remove header and associated type, reassign
    private void removeHeader(String columnHeader) {
    	columnHeader = cleanHeader(columnHeader);
		String[] updatedHeaders = new String[headers.length-1];
		String[] updatedTypes = new String[types.length-1];
		int x = 0;
		for(int i = 0; i < headers.length; i++) {
			if(!headers[i].equals(columnHeader)) {
				updatedHeaders[x] = headers[i];
				updatedTypes[x] = types[i];
				x++;
			}
		}
		this.headers = updatedHeaders;
		this.types = updatedTypes;
    }

    /*************************** ORIGINAL UNUSED CODE **************************************/


    private boolean isOne2Many(String  fileName, Vector <String> oldHeaders)
    {
    	// I need to compare one by one to see there are many
    	// or should I just make it into a separate table and do a cartesian product ?
    	
    	// Idea is really simple.. I see everything that is there and then try to see if there are more than one record of the same kind 
    	boolean retValue = false;
    	Hashtable <String, Integer> valueCounter = new Hashtable<String, Integer>();
    	
		CsvParserSettings settings = new CsvParserSettings();
    	settings.setNullValue("");

        settings.setEmptyValue(""); // for CSV only
        settings.setSkipEmptyLines(true);

    	CsvParser parser = new CsvParser(settings);		    

        
        try {
	    	RandomAccessFile thisReader = new RandomAccessFile(fileName, "r");
	        
	        String nextString = null;
			while((nextString = thisReader.readLine()) != null && !retValue)
			{
				String [] cells = parser.parseLine(nextString);
				String composite = "";
				for(int oldIndex = 0;oldIndex < oldHeaders.size();oldIndex++)
				{
					composite = composite + "_" + cells[Integer.parseInt(oldHeaders.elementAt(oldIndex))];
				}
				// now check if this is there already
				if(!valueCounter.containsKey(composite))
					valueCounter.put(composite, 1);
				else
					retValue = true;
			}
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return retValue;
    }
    
    @Override
	protected void finalize() {
//    	String finalQuery = makeDropTable(tableName);
//    	runQuery(finalQuery);
//    	System.out.println("DROPPED TABLE" + tableName);
    }
}



