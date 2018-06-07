//package prerna.ds.spark;
//
//import java.util.ArrayList;
//import java.util.Enumeration;
//import java.util.HashMap;
//import java.util.Hashtable;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.Vector;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.launcher.SparkLauncher;
//import org.apache.spark.sql.DataFrame;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.RowFactory;
//import org.apache.spark.sql.SQLContext;
//import org.apache.spark.sql.types.DataType;
//import org.apache.spark.sql.types.DataTypes;
//import org.apache.spark.sql.types.StructField;
//import org.apache.spark.sql.types.StructType;
//
//import prerna.ds.TinkerFrame;
//import prerna.ds.util.flatfile.CsvFileIterator;
//import prerna.engine.api.IHeadersDataRow;
//import prerna.util.ArrayUtilityMethods;
//
////import prerna.engine.api.IHeadersDataRow;
//
//public class SparkBuilder {
//
//	static SparkConf sparkConf;  
//	static JavaSparkContext ctx;
//	static SQLContext sqlContext;
//	static {
//		System.setProperty("hadoop.home.dir", "c:\\winutil\\");
////		System.load("c:\\Hadoop\\hadoop-2.6.0\\bin\\hadoop.dll");
////		System.load("c:\\Hadoop\\hadoop-2.6.0\\bin\\hdfs.dll");
//		sparkConf = new SparkConf().setMaster("local[*]").setAppName("JavaSparkSQL");
////		sparkConf = new SparkConf().setMaster("spark://10.13.229.89:7077").setAppName("JavaSparkSQL");
////		sparkConf = new SparkConf().setMaster("spark://159.203.90.91:7077").setAppName("JavaSparkSQL");
////		org.apache.spark.deploy.SparkHadoopUtil.get().conf().set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
////		org.apache.spark.deploy.SparkHadoopUtil.get().conf().set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
////		org.apache.spark.deploy.SparkHadoopUtil.get().conf().set("fs.C.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
////		sparkConf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
////		sparkConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
////		ctx.setLocalProperty("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
////		ctx.setLocalProperty("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
////		ctx.addJar("C:/Users/rluthar/.m2/repository/com/databricks/spark-csv_2.10/1.4.0/spark-csv_2.10-1.4.0.jar");
////		ctx.addJar("C:\\Users\\rluthar\\.m2\\repository\\org\\apache\\hadoop\\hadoop-hdfs\\2.6.0\\hadoop-hdfs-2.6.0.jar"); //do i need to add hadoop jars?
////		ctx.addJar("C:\\Users\\rluthar\\.m2\\repository\\org\\apache\\hadoop\\hadoop-client\\2.6.0\\hadoop-client-2.6.0.jar");
//		
////		scala.collection.Seq<String> jars = new scala.collection.mutable.LinkedList();
////		jars.addString(new scala.collection.mutable.StringBuilder("C:/Users/mahkhalil/.m2/repository/com/databricks/spark-csv_2.10/1.4.0/spark-csv_2.10-1.4.0.jar"));
//		ctx = new JavaSparkContext(sparkConf);
////		ctx = new JavaSparkContext("spark://159.203.90.91:7077", "whatver", null, "C:/Users/mahkhalil/.m2/repository/com/databricks/spark-csv_2.10/1.4.0/spark-csv_2.10-1.4.0.jar" );
//		sqlContext = new SQLContext(ctx);
//	}
//	
//	DataFrame frame;
//	Map<String, DataFrame> frameHash = new HashMap<>();
//	private static long count = 0;
//	public SparkBuilder() {
//	
//	}
//	
//	public void loadFile(String fileName) {
//		// Generate the schema based on the string of schema
////		List<StructField> fields = new ArrayList<StructField>();
////		for (String fieldName: schemaString.split(" ")) {
////		  fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
////		}
////		StructType schema = DataTypes.createStructType(fields);
////
////		// Convert records of the RDD (people) to Rows.
////		JavaRDD<Row> rowRDD = people.map(
////		  new Function<String, Row>() {
////		    public Row call(String record) throws Exception {
////		      String[] fields = record.split(",");
////		      return RowFactory.create(fields[0], fields[1].trim());
////		    }
////		  });
//	}
//	
//	public void processIterator(Iterator<IHeadersDataRow> iterator, String[] oldHeaders, String[] newHeaders, String[] types, String joinType) {
//		
//		if(iterator instanceof CsvFileIterator) {
//			processIterator((CsvFileIterator) iterator, oldHeaders, newHeaders, types, joinType);
//			return;
//		}
//		// Generate the schema based on the string of schema
//		
//		//create the schema
//		StructType schema = getSchema(newHeaders, types);
//
//		List<Row> rows = new ArrayList<>();
//		//grab the IHeaderDataRow and convert to a row
//		while(iterator.hasNext()) {
//			IHeadersDataRow val = iterator.next();
//			Row nextRow = RowFactory.create(val.getValues());
//			rows.add(nextRow);
//		}
//		
//		
//		JavaRDD<Row> data = ctx.parallelize(rows);
//		DataFrame newFrame = sqlContext.createDataFrame(data, schema);
//		
//		Hashtable<Integer, Integer> matches = getMatches(oldHeaders, newHeaders);
//		
//		String newTable = "newTable";
//		String oldTable = "oldTable";
//		String mergeQuery = mergeTableQuery(oldTable, newTable, matches, oldHeaders, newHeaders, joinType);
//		
//		if(frame == null) {
//			frame = newFrame;
//		} else {
//			frame.registerTempTable(oldTable);
//			newFrame.registerTempTable(newTable);
//			frame = sqlContext.sql(mergeQuery);
//		}
//	}
//	
//	public void processList(List<Object[]> data, String[] oldHeaders, String[] newHeaders, String[] types, String joinType) {	
//
//		// Generate the schema based on the string of schema
//		
//		//create the schema
//		StructType schema = getSchema(newHeaders, types);
//
//		List<Row> rows = new ArrayList<>();
//		//grab the IHeaderDataRow and convert to a row
//		for(Object[] row : data) {
//			Row nextRow = RowFactory.create(row);
//			rows.add(nextRow);
//		}
//		
//		//TODO: use an approach like this for importing csv's or files?
//		//TODO: load is deprecated...find the new way
////		HashMap<String, String> options = new HashMap<String, String>();
////		options.put("header", "true");
////		options.put("path", "cars.csv");
////		DataFrame df = sqlContext.load("com.databricks.spark.csv", options);
//		
//		JavaRDD<Row> rdd = ctx.parallelize(rows);
//		DataFrame newFrame = sqlContext.createDataFrame(rdd, schema);
//		
//		Hashtable<Integer, Integer> matches = getMatches(oldHeaders, newHeaders);
//		
//		String newTable = "newTable";
//		String oldTable = "oldTable";
//		String mergeQuery = mergeTableQuery(oldTable, newTable, matches, oldHeaders, newHeaders, joinType);
//		
//		if(frame == null) {
//			frame = newFrame;
//		} else {
//			frame.registerTempTable(oldTable);
//			newFrame.registerTempTable(newTable);
//			frame = sqlContext.sql(mergeQuery);
//		}
//	}
//	
//	public void processIterator(CsvFileIterator iterator, String[] oldHeaders, String[] newHeaders, String[] types, String joinType) {
//		// Generate the schema based on the string of schema
//
//		org.apache.spark.deploy.SparkHadoopUtil.get().conf().set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//		org.apache.spark.deploy.SparkHadoopUtil.get().conf().set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
//		
//		//TODO: use an approach like this for importing csv's or files?
//		//TODO: load is deprecated...find the new way
//		HashMap<String, String> options = new HashMap<String, String>();
//		options.put("header", "true");
//		String fileLocation = iterator.getFileLocation();
////		fileLocation = "file:///" + fileLocation;
////		fileLocation = "file:///" + fileLocation.replace("C:\\\\", "");
////		String fileLocation = "file:///" + "D://Data_Set/Movie_Data.csv";
////		options.put("path", fileLocation);
//		
//		//need to not infer and use what is sent
//		options.put("inferSchema", "true");
//		
////		DataFrame newFrame = sqlContext.load("com.databricks.spark.csv", options);
//		DataFrame newFrame = sqlContext.read().format("com.databricks.spark.csv").options(options).load(fileLocation);
//		
//		if(frame == null) {
//			frame = newFrame;
//		} else {
//			Hashtable<Integer, Integer> matches = getMatches(oldHeaders, newHeaders);
//			String newTable = "newTable";
//			String oldTable = "oldTable";
//			String mergeQuery = mergeTableQuery(oldTable, newTable, matches, oldHeaders, newHeaders, joinType);
//			
//			frame.registerTempTable(oldTable);
//			newFrame.registerTempTable(newTable);
//			frame = sqlContext.sql(mergeQuery);
//		}
//	}
//	
//	//use this to write the iterator to a file so spark can read/load it efficiently without bloating the memory...downside is higher I/O
//	private String writeToFile(Iterator iterator) {
//		return "";
//	}
//	
//	private StructType getSchema(String[] headers, String[] types) {
//		if(headers.length != types.length) {
//			throw new IllegalArgumentException("Header array and Types array not of equal length");
//		}
//		
//		List<StructField> fields = new ArrayList<StructField>();
//		for (int i = 0; i < headers.length; i++) {
//		  fields.add(DataTypes.createStructField(headers[i], getDataType(types[i]), true));
//		}
//		StructType schema = DataTypes.createStructType(fields);
//		return schema;
//	}
//	
//	private DataType getDataType(String type) {
//		if(type == null) return DataTypes.StringType;
//		
//		switch(type.toUpperCase()) {
//		case "STRING": return DataTypes.StringType;
//		case "NUMBER": return DataTypes.DoubleType;
//		case "DOUBLE": return DataTypes.DoubleType;
//		case "INTEGER": return DataTypes.IntegerType;
//		case "INT": return DataTypes.IntegerType;
//		case "BOOLEAN": return DataTypes.BooleanType;
//		case "DATE": return DataTypes.DateType;
//		default: return DataTypes.StringType;
//		}
//	}
//	
//	//does not account for situation where newHeaders has a column header which exists in oldHeaders but is not part of the join
//	private Hashtable<Integer, Integer> getMatches(String[] headers, String[] newHeaders) {
//		Vector <String> newHeaderIndices = new Vector<String>();
//		Vector <String> oldHeaderIndices = new Vector<String>();
//		Hashtable<Integer, Integer> matchers = new Hashtable<Integer, Integer>();
//    		
//		// I need to find which ones are already there and which ones are new
//		for(int hIndex = 0;hIndex < newHeaders.length;hIndex++)
//		{
//			String uheader = newHeaders[hIndex];
////			uheader = cleanHeader(uheader);
//
//			boolean old = false;
//			for(int oIndex = 0;oIndex < headers.length;oIndex++)
//			{
//				if(headers[oIndex].equalsIgnoreCase(uheader))
//				{
//					old = true;
//					oldHeaderIndices.add(hIndex+"");
//					matchers.put(hIndex, oIndex);
//					break;
//				}
//			}
//			
//			if(!old)
//				newHeaderIndices.add((hIndex) + "");
//		}
//		return matchers;
//	}
//	
//    // Obviously I need the table names
//    // I also need the matching properties
//    // I have found that out upfront- I need to also keep what it is called in the old table
//    // as well as the new table
//    private String mergeTableQuery(String tableName1, String tableName2, Hashtable <Integer, Integer> matchers, String[] oldTypes, String[] newTypes, String join) {
//    	// now I need to create a join query
//    	// first the froms
//    	
//    	String froms = " FROM " + tableName1 + " AS  A ";
//    	String joins = " " + join  + " " + tableName2 + " AS B ON (";
//
//    	Enumeration <Integer> keys = matchers.keys();
//    	for(int jIndex = 0;jIndex < matchers.size();jIndex++)
//    	{
//    		Integer newIndex = keys.nextElement();
//    		Integer oldIndex = matchers.get(newIndex); 
//    		if(jIndex == 0)
//    			joins = joins + "A." + oldTypes[oldIndex] + " = " + "B." + newTypes[newIndex];
//    		else
//    			joins = joins + " AND " + "A." + oldTypes[oldIndex] + " = " + "B." + newTypes[newIndex];
//    				
//    	}
//    	joins = joins + " )";
//    	
//    	// first table A
//    	String selectors = "";
//    	for(int oldIndex = 0;oldIndex < oldTypes.length;oldIndex++)
//    	{
//    		if(oldIndex == 0)
//    			selectors = "A." + oldTypes[oldIndex];
//    		else
//    			selectors = selectors + " , " + "A." + oldTypes[oldIndex];
//    	}
//    	
//    	// next table 2
//    	for(int newIndex = 0;newIndex < newTypes.length;newIndex++)
//    	{
//    		if(!matchers.containsKey(newIndex))
//    			selectors = selectors + " , " + "B." + newTypes[newIndex];
//    	}
//    	
//    	String mergeQuery = "SELECT " + selectors + " " + froms + "  " +  joins;
//    	
//    	System.out.println(mergeQuery);
//    	return mergeQuery;
//    	
//    }
//    
//    private String mergeTableQuery(String tableName1, String tableName2, Hashtable <Integer, Integer> matchers, String[] oldTypes, String[] newTypes, String join, Map<String, String> aliases) {
//    	// now I need to create a join query
//    	// first the froms
//    	
//    	String froms = " FROM " + tableName1 + " AS  A ";
//    	String joins = " " + join  + " " + tableName2 + " AS B ON (";
//
//    	Enumeration <Integer> keys = matchers.keys();
//    	for(int jIndex = 0;jIndex < matchers.size();jIndex++)
//    	{
//    		Integer newIndex = keys.nextElement();
//    		Integer oldIndex = matchers.get(newIndex); 
//    		if(jIndex == 0)
//    			joins = joins + "A." + oldTypes[oldIndex] + " = " + "B." + newTypes[newIndex];
//    		else
//    			joins = joins + " AND " + "A." + oldTypes[oldIndex] + " = " + "B." + newTypes[newIndex];
//    				
//    	}
//    	joins = joins + " )";
//    	
//    	// first table A
//    	String selectors = "";
//    	for(int oldIndex = 0;oldIndex < oldTypes.length;oldIndex++)
//    	{
//    		if(oldIndex == 0)
//    			selectors = "A." + oldTypes[oldIndex];
//    		else
//    			selectors = selectors + " , " + "A." + oldTypes[oldIndex];
//    		
//    		if(aliases.containsKey(oldTypes[oldIndex])) {
//    			selectors += " AS " +aliases.get(oldTypes[oldIndex]);
//    		}
//    	}
//    	
//    	// next table 2
//    	for(int newIndex = 0;newIndex < newTypes.length;newIndex++)
//    	{
//    		if(!matchers.containsKey(newIndex))
//    			selectors = selectors + " , " + "B." + newTypes[newIndex];
//    		
//    		if(aliases.containsKey(newTypes[newIndex])) {
//    			selectors += " AS " +aliases.get(newTypes[newIndex]);
//    		}
//    	}
//    	
//    	String mergeQuery = "SELECT " + selectors + " " + froms + "  " +  joins;
//    	
//    	System.out.println(mergeQuery);
//    	return mergeQuery;
//    	
//    }
//    
//    public DataFrame getUniqueValues(String columnHeader) {
//		String tableName = "tempTable";
//    	frame.registerTempTable(tableName);
//		return sqlContext.sql("SELECT DISTINCT "+columnHeader+" FROM "+tableName);
//    }
//    
//    public DataFrame buildIterator(Map<String, Object> options) {
//    	List<String> selectors = (List<String>) options.get(TinkerFrame.SELECTORS);
//    	String tableName = "tempTable";
//    	frame.registerTempTable(tableName);
//    	String selectQuery = makeSelectDistinct(tableName, selectors);
//    	Integer limit = (Integer) options.get(TinkerFrame.LIMIT);
////		Integer offset = (Integer) options.get(TinkerFrame.OFFSET);
//		if(limit != null && limit > 0) {
//			selectQuery += " limit "+limit;
//		} else {
//			selectQuery += " limit "+ 100;
//		}
////		if(offset != null && offset > 0) {
////			selectQuery += " offset "+offset;
////		}
//		return sqlContext.sql(selectQuery);
//    }
//    
//    
//    //algorithm for now will be average/min/max/count/sum
//    public Object mapReduce(Vector<String> columns, Vector<String> groupBys, String algorithm, String key) {
//    	
//    	String query;
//    	String tableName = getTempTableName();
//    	frame.registerTempTable(tableName);
//    	
//    	if(groupBys == null || groupBys.isEmpty()) {
//    		query = makeSingleGroupBy(null, columns.get(0), algorithm, "NEWCOLUMN", tableName);
//    		DataFrame newFrame = sqlContext.sql(query);
//    		frameHash.put(key, newFrame);
//        	return newFrame.take(1)[0].get(0);
//    	} else {
//    		query = makeGroupBy(groupBys.toArray(new String[]{}), columns.get(0), algorithm, "NEWCOLUMN", tableName);
//    		DataFrame newFrame = sqlContext.sql(query);
//    		frameHash.put(key, newFrame);
//    		Map<Map<String, Object>, Object> groupByMap = new HashMap<>();
//    		String[] columnHeaders = newFrame.columns();
//    		int index = ArrayUtilityMethods.arrayContainsValueAtIndex(columnHeaders, "NEWCOLUMN");
//    		Row[] rows = newFrame.collect();
//    		for(Row row : rows) {
//    			Map<String, Object> groupByKey = new HashMap<>();
//    			Object value = null;
//    			for(int i = 0; i < row.size(); i++) {
//    				if(i == index) {
//    					value = row.get(i);
//    				} else {
//    					groupByKey.put(columnHeaders[i], row.get(i));
//    				}
//    			}
//    			groupByMap.put(groupByKey, value);
//    		}
//    		return groupByMap;
//    	}    	
//    }
//    private String makeSelect(String tableName, List<String> selectors) {
//    	
//    	String selectStatement = "SELECT ";
//    	
//    	for(int i = 0; i < selectors.size(); i++) {
//    		String selector = selectors.get(i);
////    		selector = cleanHeader(selector);
//    		
//    		if(i < selectors.size() - 1) {
//    			selectStatement += selector + ", ";
//    		}
//    		else {
//    			selectStatement += selector;	
//    		}
//    	}
//    	
//    	selectStatement += " FROM " + tableName;
//    	return selectStatement;
//    }
//    
//    private String makeSelectDistinct(String tableName, List<String> selectors) {
//    	
//    	String selectStatement = "SELECT DISTINCT ";
//    	
//    	for(int i = 0; i < selectors.size(); i++) {
//    		String selector = selectors.get(i);
////    		selector = cleanHeader(selector);
//    		
//    		if(i < selectors.size() - 1) {
//    			selectStatement += selector + ", ";
//    		}
//    		else {
//    			selectStatement += selector;	
//    		}
//    	}
//    	
//    	selectStatement += " FROM " + tableName;
//    	return selectStatement;
//    }
//    
//    //column = group by
//    private String makeSingleGroupBy(String column, String valueColumn, String mathType, String alias, String tableName) {
//    	
//    	String functionString = "";
//    	
////    	String type = getType(tableName, column);
//    	String type = "VARCHAR";
//    			
//    	switch(mathType.toUpperCase()) {
//    	case "COUNT": {
//    		String func = "COUNT(";
//    		if(type.toUpperCase().startsWith("VARCHAR"))
//    			func = "COUNT( DISTINCT ";
//    		functionString = func +valueColumn+")"; break; 
//    		}
//    	case "AVERAGE": {functionString = "AVG("+valueColumn+")";  break; }
//    	case "MIN": {functionString = "MIN("+valueColumn+")";  break; }
//    	case "MAX": {functionString = "MAX("+valueColumn+")"; break; }
//    	case "SUM": {functionString = "SUM("+valueColumn+")"; break; }
//    	default: {
//    		String func = "COUNT(";
//    		if(type.toUpperCase().startsWith("VARCHAR"))
//    			func = "COUNT( DISTINCT ";
//    		functionString = func +valueColumn+")"; break; }
//    	}
//    	
//    	String groupByStatement = "";
//    	if(column != null) {
//    		groupByStatement += "SELECT " + column+", "+functionString + " AS " + alias +" FROM "+tableName + " GROUP BY "+ column;
//    	} else {
//    		groupByStatement = "SELECT " + functionString + " AS " + alias +" FROM "+tableName;
//    	}
//    	
//    	return groupByStatement;
//    }
//    
//    //TODO : don't assume a double group by here
//    private String makeGroupBy(String[] column, String valueColumn, String mathType, String alias, String tableName) {
//    	if(column.length == 1) return makeSingleGroupBy(column[0], valueColumn, mathType, alias, tableName);
//    	String column1 = column[0];
//    	String column2 = column[1];
//    	valueColumn = valueColumn;
//    	alias = alias;
//    	
//    	String functionString = "";
//    	
////    	String type = getType(tableName, valueColumn);
//    	String type = "VARCHAR";
//    	
//    	switch(mathType.toUpperCase()) {
//    	case "COUNT": {
//    		String func = "COUNT(";
//    		if(type.toUpperCase().startsWith("VARCHAR"))
//    			func = "COUNT( DISTINCT ";
//    		functionString = func +valueColumn+")"; break; 
//    		}
//    	case "AVERAGE": {functionString = "AVG("+valueColumn+")";  break; }
//    	case "MIN": {functionString = "MIN("+valueColumn+")";  break; }
//    	case "MAX": {functionString = "MAX("+valueColumn+")"; break; }
//    	case "SUM": {functionString = "SUM("+valueColumn+")"; break; }
//    	default: {
//    		String func = "COUNT(";
//    		if(type.toUpperCase().startsWith("VARCHAR"))
//    			func = "COUNT( DISTINCT ";
//    		functionString = func +valueColumn+")"; break; }
//    	}
//    	
//    	String groupByStatement = "SELECT " + column1+", "+column2+", "+functionString + " AS " + alias +" FROM "+tableName + " GROUP BY "+ column1+", "+column2;
//    	
//    	return groupByStatement;
//    }
//    
//    public void mergeFrame(String key, String newCol) {
//    	String[] existingColumns = frame.columns();
//    	DataFrame mergeFrame = frameHash.get(key);
////    	DataFrame mergeFrame = frameHash.get("");
//    	String[] mergeColumns = mergeFrame.columns();
//    		
//    	String tempTable = getTempTableName();
//    	String mergeTempTable = getTempTableName();
//    	
//    	sqlContext.registerDataFrameAsTable(frame, tempTable);
//    	sqlContext.registerDataFrameAsTable(mergeFrame, mergeTempTable);
//    	
//    	//do the merging here
//    	Hashtable<Integer, Integer> matches = getMatches(existingColumns, mergeColumns);
//
//    	Map<String, String> aliases = new HashMap<String, String>();
//    	aliases.put("NEWCOLUMN", newCol);
//    	
//		String mergeQuery = mergeTableQuery(tempTable, mergeTempTable, matches, existingColumns, mergeColumns, " inner join ", aliases);
//		frame = sqlContext.sql(mergeQuery);
//		
//    	sqlContext.dropTempTable(tempTable);
//    	sqlContext.dropTempTable(mergeTempTable);
//    }
//    
//    public void getFrame(String key) {
//    	
//    }
//    
//    private static long getUniqueID() {
//    	return count++;
//    }
//    
//    private static String getTempTableName() {
//    	return "TempTable"+getUniqueID();
//    }
//    
//    private static String getTempColName() {
//    	return "TempColumn"+getUniqueID();
//    }
//    
//    public static void main(String[] args) throws Exception{
//    	
//    	//JavaSparkContext sc = new JavaSparkContext("spark://159.203.90.91:7077", "whatver", null, "C:/Users/mahkhalil/.m2/repository/com/databricks/spark-csv_2.10/1.4.0/spark-csv_2.10-1.4.0.jar" );
//		//sqlContext = new SQLContext(sc);
//    	
//		
//		       new SparkLauncher()
//		         .setAppResource("/my/app.jar")
//		         .setMainClass("my.spark.app.Main")
//		         .setMaster("spark://semossphabricator.com:7077")
//		         .setConf(SparkLauncher.DRIVER_MEMORY, "2g").launch();
//		         //.startApplication();
//		       // Use handle API to monitor / control application.
////    	sparkConf = new SparkConf().setMaster("spark://10.13.229.89:7077").setAppName("JavaSparkSQL");
////    	JavaSparkContext sc = new JavaSparkContext("spark://159.203.90.91:7077", "Da App");
////    	sc.addJar("C:/Users/rluthar/.m2/repository/com/databricks/spark-csv_2.10/1.4.0/spark-csv_2.10-1.4.0.jar");
////		sqlContext = new SQLContext(sc);
//		
////		org.apache.spark.deploy.SparkHadoopUtil.get().conf().set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
////		org.apache.spark.deploy.SparkHadoopUtil.get().conf().set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
//		
//		//TODO: use an approach like this for importing csv's or files?
//		//TODO: load is deprecated...find the new way
////		HashMap<String, String> options = new HashMap<String, String>();
////		options.put("header", "true");
////		String fileLocation = "file:///" + "D:/Data_Set/Movie_Data.csv";
////		options.put("path", fileLocation);
////		
////		DataFrame newFrame = sqlContext.load("com.databricks.spark.csv", options);
////				
////		Row[] rows = newFrame.collect();
////		for(Row r : rows) {
////			System.out.println(r.toString());
////		}
//    }
//}
