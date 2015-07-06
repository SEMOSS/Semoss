/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.poi.main;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.sql.SQLException;
//import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
//import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.rdfxml.util.RDFXMLPrettyWriter;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.memory.MemoryStore;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ParseBool;
import org.supercsv.cellprocessor.ParseDate;
import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvMapReader;
import org.supercsv.io.ICsvMapReader;
import org.supercsv.prefs.CsvPreference;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.QuestionAdministrator;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.error.EngineException;
import prerna.error.FileReaderException;
import prerna.error.FileWriterException;
import prerna.error.HeaderClassException;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.sql.MariaDbQueryUtil;
import prerna.util.sql.SQLQueryUtil;
import prerna.util.sql.SQLServerQueryUtil;

import com.hp.hpl.jena.vocabulary.OWL;

/**
 * Loading data into SEMOSS using comma separated value (CSV) files
 */
public class RDBMSReader {

	private static final String GENERIC_PERSPECTIVE = "Generic-Perspective";
	private static final Logger logger = LogManager.getLogger(RDBMSReader.class.getName());

	private String propFile; // the file that serves as the property file
	private ICsvMapReader mapReader;
	private String [] header; // array of headers
	private List<String> headerList; // list of headers
	private CellProcessor[] processors;
	private static Hashtable <String, CellProcessor> typeHash = new Hashtable<String, CellProcessor>();
	public final static String NUMCOL = "NUM_COLUMNS";
	public final static String NOT_OPTIONAL = "NOT_OPTIONAL";
	private ArrayList<String> relationArrayList = new ArrayList<String>();
	private ArrayList<String> recreateIndexesArr = new ArrayList<String>(); 
	private ArrayList<String> tempIndexArray = new ArrayList<String>();
	private ArrayList<String> tempDropArrays = new ArrayList<String>();
	private int indexUniqueId = 1;
	//private ArrayList<String> nodePropArrayList = new ArrayList<String>();
	//private ArrayList<String> relPropArrayList = new ArrayList<String>();
	private int count = 0;
	private int totalrowcount = 0;
	private boolean propFileExist = true;
	private Hashtable<String, String>[] rdfMapArr;
	private Hashtable <String, String> sqlHash = new Hashtable<String, String>();
	private Hashtable <String, String> typeIndices = new Hashtable(); // this basically says for a given column what is its index
	private Hashtable <String, Hashtable> tableHash = new Hashtable();
	private Hashtable <String, Hashtable<String, String>> availableTables = new Hashtable(); // all the existing tables, tablename is the key, the value is another hashtable with field name and property
	private Hashtable <String, String> availableTablesInfo = new Hashtable();
	private Hashtable <String, Hashtable<String,String>> whereColumns = new Hashtable(); // same as previous table, but this are what get added to the where on an update query 
	private Hashtable <String, String> relHash = new Hashtable<String,String>(); // keeps it in the format of name of the relationship, the value being the name of the classes separated by @
	String dbBaseFolder = null;
	IEngine engine = null;


	// stuff from abstract reader
	protected Hashtable<String, String> rdfMap = new Hashtable<String, String>();
	protected String bdPropFile;
	protected Properties bdProp = new Properties(); // properties for big data
	protected Sail bdSail;
	protected ValueFactory vf;

	protected String customBaseURI = "";
	public String basePropURI= "";

	protected SailConnection sc;
	protected String semossURI;
	protected String propURI = "http://semoss.org/ontologies/property";
	protected final static String CONTAINS = "Contains";

	public Hashtable<String,String> baseConceptURIHash = new Hashtable<String,String>(); 
	public Hashtable<String,String> conceptURIHash = new Hashtable<String,String>();
	public Hashtable<String,String> baseRelationURIHash = new Hashtable<String,String>(); 
	public Hashtable<String,String> relationURIHash = new Hashtable<String,String>();
	public Hashtable<String,String> basePropURIHash = new Hashtable<String,String>();
	public Hashtable<String,String> basePropRelations = new Hashtable<String,String>();

	protected Hashtable<String, String[]> baseRelations = new Hashtable<String, String[]>();
	protected Vector <String> tables = new Vector<String>();
	protected Vector <String> allTables = new Vector<String>();
	protected Vector <String> allTablesModified = new Vector<String>();

	// OWL variables
	protected RepositoryConnection rcOWL;
	protected ValueFactory vfOWL;
	protected SailConnection scOWL;
	protected String owlFile = "";

	protected String scriptFileName = "DBScript.sql";
	protected PrintWriter scriptFile = null;

	//reload base data
	protected RDFFileSesameEngine baseDataEngine;
	protected Hashtable<String, String> baseDataHash = new Hashtable<String, String>();

	private SQLQueryUtil queryUtil;

	public static void main(String [] args) throws Exception
	{
		RDBMSReader reader = new RDBMSReader();
		String engineName = "MovieRDBMS";
		reader.dbBaseFolder = "C:/Users/pkapaleeswaran/workspacej2/SemossWeb";
		String fileName = "C:/Users/pkapaleeswaran/workspacej2/Data/Movie.csv";
		reader.propFile = "C:/Users/pkapaleeswaran/workspacej2/SemossWeb/db/MovieRDBMS/MovieRDBMS_Movie_PROP.prop";
		reader.propFileExist = true;
		reader.semossURI = "http://semoss.org/ontologies";
		reader.customBaseURI = "http://semoss.org/ontologies";
		reader.owlFile = "C:/Users/pkapaleeswaran/workspacej2/SemossWeb/db/MovieRDBMS/Movie_DB_OWL2.OWL";
		String outputFile = reader.writePropFile(engineName);
		reader.importFileWithOutConnection(outputFile, fileName, reader.customBaseURI, reader.owlFile,engineName, SQLQueryUtil.DB_TYPE.MARIA_DB);

		System.out.println("Trying the new one now");

		reader.cleanAll();

		fileName = "C:/Users/pkapaleeswaran/workspacej2/Data/Movie2.csv";
		reader.propFile = "C:/Users/pkapaleeswaran/workspacej2/SemossWeb/db/MovieRDBMS/MovieRDBMS_Movie_PROP2.prop";
		reader.importFileWithOutConnection(outputFile, fileName, reader.customBaseURI, reader.owlFile,engineName, SQLQueryUtil.DB_TYPE.H2_DB);

	}

	private void cleanAll()
	{
		tableHash.clear();
		availableTables.clear();
		availableTablesInfo.clear();
		whereColumns.clear();
		rdfMap.clear();
	}

	private String writePropFile(String engineName)
	{
		Properties prop = new Properties();

		prop.put(Constants.CONNECTION_URL, queryUtil.getConnectionURL(dbBaseFolder,engineName));
		prop.put(Constants.USERNAME, queryUtil.getDefaultDBUserName());
		prop.put(Constants.PASSWORD, queryUtil.getDefaultDBPassword());
		prop.put(Constants.DRIVER,queryUtil.getDatabaseDriverClassName());
		if(queryUtil.getDatabaseType() == SQLQueryUtil.DB_TYPE.MARIA_DB){
			prop.put(Constants.TEMP_CONNECTION_URL, MariaDbQueryUtil.getTempConnectionURL());
		}
		if(queryUtil.getDatabaseType() == SQLQueryUtil.DB_TYPE.SQL_SERVER){
			prop.put(Constants.TEMP_CONNECTION_URL, SQLServerQueryUtil.getTempConnectionURL());
		}
		prop.put(Constants.RDBMS_TYPE,queryUtil.getDatabaseType().toString());
		prop.put("TEMP", "TRUE");

		// write this to a file
		String tempFile = dbBaseFolder + "/db/" + engineName + "/conn.prop";
		try {
			File file = new File(tempFile);
			FileOutputStream fo = new FileOutputStream(file);
			prop.store(fo, "Temporary Properties file for the RDBMS");
			fo.close();
			openScriptFile(engineName);

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


		return tempFile;

	}

	/**
	 * Open script file that contains all the sql statements run during the upload process
	 * @param engineName name of the engine/db
	 */
	private void openScriptFile(String engineName){
		try {
			scriptFileName = dbBaseFolder + "/db/" + engineName + "/" + scriptFileName;
			if(scriptFile == null)
				scriptFile = new PrintWriter(new OutputStreamWriter(new FileOutputStream(new File(scriptFileName),true)));//set append to true
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Loading data into SEMOSS to create a new database
	 * @param dbName 		String grabbed from the user interface that would be used as the name for the database
	 * @param fileNames		Absolute paths of files the user wants to load into SEMOSS, paths are separated by ";"
	 * @param customBase	String grabbed from the user interface that is used as the URI base for all instances 
	 * @param customMap		
	 * @param owlFile		String automatically generated within SEMOSS to determine the location of the OWL file that is produced
	 * @throws EngineException 
	 * @throws FileReaderException 
	 * @throws FileWriterException 
	 * @throws HeaderClassException 
	 */
	public void importFileWithOutConnection(String engineFile, String fileNames, String customBase, String owlFile, String engineName, SQLQueryUtil.DB_TYPE dbType) throws EngineException, FileWriterException, FileReaderException, HeaderClassException {

		queryUtil = SQLQueryUtil.initialize(dbType);

		logger.setLevel(Level.WARN);
		String[] files = fileNames.split(";"); //)prepareReader(fileNames, customBase, owlFile);
		this.owlFile = owlFile;

		//openEngineWithoutConnection(engineName);
		openOWLWithOutConnection();
		createTypes();

		if(!customBase.equals("")) customBaseURI = customBase;

		// check if I am in the environment
		getBaseFolder();

		createSQLTypes();
		System.out.println("Owl File is " + this.owlFile);
		openDB(engineName); //scriptfile opened in here.

		for(int i = 0; i<files.length;i++)
		{
			String fileName = files[i];

			if(i ==0 )scriptFile.println("-- ********* begin load process ********* ");
			scriptFile.println("-- ********* begin load " + fileName + " ********* ");
			// find the tables
			findTables(engineName);
			// load the prop file for the CSV file 
			if(propFileExist){
				openProp(propFile);
			} else {
				rdfMap = rdfMapArr[i];
			}

			// determine the type of data in each column of CSV file
			createProcessors();
			openCSVFile(fileName); //open further down in the process because we need variables defined in createProcessors
			try {
				processConceptURIs();
				processProperties();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			recreateRelations();
			createTables();
			skipRows();
			//System.out.println(currentDate() + " before insertRecords stuff, for " + fileName );
			insertRecords();
			//System.out.println(currentDate() + " after insertRecords stuff, for " + fileName );
			cleanAll();
			commitDB();
			scriptFile.println("-- ********* completed processing file " + fileName + " ********* ");
		}
		cleanUpDBTables(engineName);
		closeDB();
		cleanAll(); //do it again because we reset availableTables and availableTablesInfo
		writeDefaultQuestionSheet(engineName);

		createBaseRelations();
		try {
			scriptFile.println("-- ********* completed load process ********* ");
			scriptFile.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	//get current indexes that are saved off.  If some exist we will reexecute them when the upload process completes
	private void findIndexes(String engineName){
		// this gets all the existing tables
		String query = queryUtil.getDialectAllIndexesInDB(engineName);
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
		while(wrapper.hasNext())
		{
			String tablename = "";
			String dropCurrentIndexText = "";
			ISelectStatement stmt = wrapper.next();
			String indexName = stmt.getVar(queryUtil.getResultAllIndexesInDBIndexName()) + "";
			//only storing off custom indexes, recreating the non custom ones on the fly on the cleanUpDBTables method

			String indexInfoQry = queryUtil.getDialectIndexInfo(indexName, engineName);
			ISelectWrapper indexInfo = WrapperManager.getInstance().getSWrapper(engine, indexInfoQry);
			ArrayList<String> columnsInIndex = new ArrayList();
			String columnName = "";
			while(indexInfo.hasNext()){
				ISelectStatement stmtIndx = indexInfo.next();
				tablename = stmtIndx.getVar(queryUtil.getResultAllIndexesInDBTableName()) + "";
				columnName = stmtIndx.getVar(queryUtil.getResultAllIndexesInDBColumnName()) + "";
				columnsInIndex.add(columnName);
			}
			if(indexName.startsWith("CUST_")){
				recreateIndexesArr.add(queryUtil.getDialectCreateIndex(indexName,tablename,columnsInIndex));
			}
			//drop all indexes, recreate the custom ones, the non custom ones will be systematically recreated.
			dropCurrentIndexText = queryUtil.getDialectDropIndex(indexName, tablename);
			singleDBModTransaction(dropCurrentIndexText);
		}
	}

	private void runDBModTransactions(ArrayList<String> itemsArray){
		for(String eachSQL: itemsArray){
			singleDBModTransaction(eachSQL);
		}
	}

	private void singleDBModTransaction(String sql){

		try {
			if(!sql.endsWith(";")) sql+= ";";
			scriptFile.println(sql);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		modifyDB(sql);	
	}

	//remove duplicates and create standard indexes on table
	private void cleanUpDBTables(String engineName){
		String createTable = "", verifyTable="", dropTable = "", alterTableName = "";  //, createIndex = "";
		String tableName = "", currentTable ="", columnName = "", fullColumnNameList = "";
		ArrayList<String> createIndex = new ArrayList();
		Enumeration allTablesEnum = null, columns = null;
		Hashtable availableTableColumns = null;
		ISelectWrapper wrapper = null;
		boolean tableAltered = false; //includes tables created/modified appended to etc.

		//fill up the availableTables and availableTablesInfo maps
		findTables(engineName);

		allTablesEnum = availableTables.keys();
		while(allTablesEnum.hasMoreElements()){
			fullColumnNameList = "";
			createIndex.clear();
			tableAltered = false;
			tableName = (String)allTablesEnum.nextElement();
			allTables.add(tableName);
			availableTableColumns = availableTables.get(tableName);
			columns = availableTableColumns.keys();
			int indexCount = 1;
			while(columns.hasMoreElements()){

				columnName = (String)columns.nextElement();
				if(fullColumnNameList.length()==0 ) { 
					fullColumnNameList += tableName; //put the table name column first
				} 
				if(!columnName.equals(tableName) && fullColumnNameList.length()!=0) {
					fullColumnNameList += " , " + columnName;
				} 
				//index should be created on each individual primary and foreign key column
				if(columnName.equals(tableName) || columnName.endsWith("_FK")){
					createIndex.add(queryUtil.getDialectCreateIndex(tableName + "_INDX_"+indexCount,tableName, columnName));
					indexCount++;
				}
			}

			//find out if the table was altered on this time through (or if it was new...)

			for(int tableIndex = 0;tableIndex < allTablesModified.size();tableIndex++)
			{
				currentTable = allTablesModified.elementAt(tableIndex);
				if(tableName.equals(currentTable.toUpperCase())){
					tableAltered = true;
					break;
				}
			}

			//do this duplicates removal for only the tables that were modified
			if(tableAltered){
				//create new temporary table that has ONLY distinct values, also make sure you are removing those null values from the PK column
				createTable = queryUtil.getDialectRemoveDuplicates(tableName, fullColumnNameList);
				singleDBModTransaction(createTable);

				//check that the temp table was created before dropping the table.
				verifyTable = queryUtil.dialectVerifyTableExists(tableName + "_TEMP"); //query here would return a row count 
				//if temp table wasnt successfully created, go to the next table.
				wrapper = WrapperManager.getInstance().getSWrapper(engine, verifyTable);
				while(wrapper.hasNext()){ 
					ISelectStatement stmtTblCount = wrapper.next();
					String numberOfRows = stmtTblCount.getVar(queryUtil.getResultSelectRowCountFromRowCount()) + "";
					if(numberOfRows.equals(0)){
						//This REALLY shouldnt happen, but its here just in case...
						logger.error("**** Error***** occurred during database clean up on table " + tableName);
						continue;
					}
				}

				//drop existing table
				dropTable = queryUtil.getDialectDropTable(tableName);//dropTable = "DROP TABLE " + tableName;
				singleDBModTransaction(dropTable);

				//rename our temporary table to the new table name				
				alterTableName = queryUtil.getDialectAlterTableName(tableName+"_TEMP",tableName); //alterTableName = "ALTER TABLE " + tableName + "_TEMP RENAME TO " + tableName;
				singleDBModTransaction(alterTableName);

				commitDB();

			}
			for(String singleIndex: createIndex){
				//create indexs for ALL tables since we deleted all indexes before
				singleDBModTransaction(singleIndex);
			}
		}
		// clear out the availableTables and availableTablesInfo maps that were created in findTables method call
		//availableTables.clear();
		//availableTablesInfo.clear();
	}

	private void findTables(String engineName){
		// this gets all the existing tables
		String query = queryUtil.getDialectAllTables();
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
		while(wrapper.hasNext())
		{
			ISelectStatement stmt = wrapper.next();
			String tableNameVar = queryUtil.getResultAllTablesTableName();
			if(queryUtil.getDatabaseType() == SQLQueryUtil.DB_TYPE.MARIA_DB)
				tableNameVar += engineName.toLowerCase(); //Maria db wants a lower case table name, but the first letter is capital

			//tableNameVar = Utility.toCamelCase(tableNameVar);
			String tableName = stmt.getVar(tableNameVar) + "";
			//tableName = Utility.toCamelCase(tableName);
			findColumns(tableName);
			String tableCountQuery = queryUtil.getDialectSelectRowCountFrom(tableName,"");
			ISelectWrapper tableCount = WrapperManager.getInstance().getSWrapper(engine, tableCountQuery);
			while(tableCount.hasNext()){
				ISelectStatement stmtTblCount = tableCount.next();
				String numberOfRows = stmtTblCount.getVar(queryUtil.getResultSelectRowCountFromRowCount()) + "";
				availableTablesInfo.put(tableName.toUpperCase(), numberOfRows);
			}
		}

	}

	private void findColumns(String tableName){
		String query = queryUtil.getDialectAllColumns(tableName);
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
		Hashtable <String, String> fieldHash = new Hashtable();
		if(availableTables.containsKey(tableName))
			fieldHash = availableTables.get(tableName);
		while(wrapper.hasNext())
		{
			ISelectStatement stmt = wrapper.next();
			String colName = stmt.getVar(queryUtil.getAllColumnsResultColumnName()) + "";
			String type = stmt.getVar(queryUtil.getResultAllColumnsColumnType()) + "";
			fieldHash.put(colName.toUpperCase(), type);
			availableTables.put(tableName.toUpperCase(), fieldHash);
		}
	}


	private void writeDefaultQuestionSheet(String engineName)
	{		
		// deciding whether to read it again or just pass the name

		// delete the file
		try {
			String tempFile = dbBaseFolder + "/db/" + engineName + "/conn.prop";
			File file2 = new File(tempFile);
			file2.delete();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		String fileName = dbBaseFolder + System.getProperty("file.separator") + "db" + System.getProperty("file.separator") + engineName + System.getProperty("file.separator") + engineName + "_Questions.properties";

		Properties prop = new Properties();

		int newTableSeq = allTables.size() - tables.size();
		boolean addtoExisting = false;
		if(newTableSeq != 0 ){ //ie you are doing add to existing logic
			addtoExisting = true;
			newTableSeq += 1; //pad for that one concept question we added below for the initial run through when we created the questions.xml
			try{
				prop = ((AbstractEngine)engine).loadProp(fileName);
			} catch (Exception e){
				e.printStackTrace();
			}
		}

		String genericQueries = "";

		int questionOrder = 0;
		int tableIndex = 0;

		for(;tableIndex < tables.size();tableIndex++)
		{
			questionOrder = newTableSeq + tableIndex;

			String key = tables.elementAt(tableIndex);
			key = realClean(key);
			if(tableIndex == 0)
				genericQueries = genericQueries + "GQ" + questionOrder;
			else
				genericQueries = genericQueries + ";" + "GQ" + questionOrder;				
			prop.put("GQ" + questionOrder, "Show all from " + key );
			prop.put("GQ" + questionOrder +"_LAYOUT", "prerna.ui.components.playsheets.GridPlaySheet");
			prop.put("GQ" + questionOrder +"_QUERY", "SELECT * FROM " + key);
		}
		if(!addtoExisting){ 
			genericQueries = genericQueries + ";" + "GQ" + tableIndex;
			prop.put("GQ" + tableIndex, "Explore a concept from the database");
			prop.put("GQ" + tableIndex + "_LAYOUT", "prerna.ui.components.playsheets.GraphPlaySheet");
			prop.put("GQ" + tableIndex +"_QUERY", "SELECT @Concept-Concept:Concept@, 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'http://semoss.org/ontologies/Concept' "
					+ "From @Concept-Concept:Concept@ WHERE @Concept-Concept:Concept@='@Instance-Instance:Instance@'");
			prop.put("GQ" + tableIndex + "_Instance_DEPEND", "Concept");
			//Added for 
			if(queryUtil.getDatabaseType().equals(SQLQueryUtil.DB_TYPE.SQL_SERVER))
				prop.put("GQ" + tableIndex + "_Concept_QUERY", "SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES");
			else
				prop.put("GQ" + tableIndex + "_Concept_QUERY", queryUtil.getDialectForceGraph(engineName)); 
			prop.put("GQ" + tableIndex + "_Instance_QUERY", "SELECT Distinct @Concept@ FROM @Concept@");

			prop.put("Generic-Perspective", genericQueries);
		}
		//prop.put("PERSPECTIVE", "Generic-Perspective");

		if(addtoExisting ){
			String savedGenericQueries = (String) prop.get(GENERIC_PERSPECTIVE);
			genericQueries = savedGenericQueries + ";" + genericQueries;
			prop.remove(GENERIC_PERSPECTIVE); //remove so it can be recreated
		}
		prop.put(GENERIC_PERSPECTIVE, genericQueries);
		if(!addtoExisting )
			prop.put("PERSPECTIVE", GENERIC_PERSPECTIVE);

		try {
			File file = new File(fileName);
			FileOutputStream fo = new FileOutputStream(file);
			prop.store(fo, "Questions for RDBMS");
			fo.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void updateDefaultQuestionSheet(String engineName){

		QuestionAdministrator questionAdmin = new QuestionAdministrator(((AbstractEngine)engine));

		//determine the # where the new questions should start
		int newTableSeq = allTables.size() - tables.size();
		newTableSeq = newTableSeq + 1; //we need to add 1 to the question order to account for  the explore a concept question
		String questionOrder = "", question = "", sql = "", layout = "", questionDescription = ""; 

		try {
			String questionKey = questionAdmin.createQuestionKey(GENERIC_PERSPECTIVE);
			for(int tableIndex = 0;tableIndex < tables.size();tableIndex++)
			{
				questionOrder = Integer.toString(newTableSeq + tableIndex);
				questionKey = "GQ"+questionOrder;
				String key = tables.elementAt(tableIndex);
				key = realClean(key);
				question = "Show all from " + key;
				questionDescription = question;
				layout = "prerna.ui.components.playsheets.GridPlaySheet";
				sql = "SELECT * FROM " + key;

				questionAdmin.cleanAddQuestion(GENERIC_PERSPECTIVE, questionKey, questionOrder,
						question, sql, layout, questionDescription, null, null, null); // parameterDependList, parameterQueryList, parameterOptionList);
				questionAdmin.createQuestionXMLFile();
			}
		} catch(RuntimeException e) {
			System.out.println("caught exception while adding question.................");
			e.printStackTrace();
			System.out.println("reverting xml........................");
			questionAdmin.revertQuestionXML();
			//return Response.status(500).entity(WebUtility.getSO(e.toString().substring(0, (e.toString().length() < MAX_CHAR)?e.toString().length():MAX_CHAR))).build();
		}

	}

	/**
	 * Loads the prop file for the CSV file
	 * @param fileName	Absolute path to the prop file specified in the last column of the CSV file
	 * @throws FileReaderException 
	 */
	protected void openProp(String fileName) throws FileReaderException {
		Properties rdfPropMap = new Properties();
		FileInputStream fileIn = null;
		try {
			fileIn = new FileInputStream(fileName);
			rdfPropMap.load(fileIn);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			throw new FileReaderException("Could not find user-specified prop file with CSV metamodel data located at: " + fileName);
		} catch (IOException e) {
			e.printStackTrace();
			throw new FileReaderException("Could not read user-specified prop file with CSV metamodel data located at: " + fileName);
		} finally{
			try{
				if(fileIn!=null)
					fileIn.close();
			}catch(IOException e) {
				e.printStackTrace();
			}
		}
		rdfMap.clear();
		for(String name: rdfPropMap.stringPropertyNames()){
			rdfMap.put(name, rdfPropMap.getProperty(name).toString());
		}
	}

	protected void openEngineWithConnection(String engineName) throws EngineException {
		engine = (IEngine)DIHelper.getInstance().getLocalProp(engineName);
		openOWLWithConnection(engine);
	}


	public void importFileWithConnection(String engineName, String fileNames, String customBase, String owlFile,SQLQueryUtil.DB_TYPE dbType) throws EngineException, FileWriterException, FileReaderException, HeaderClassException {
		queryUtil = SQLQueryUtil.initialize(dbType);

		logger.setLevel(Level.WARN);
		this.owlFile = owlFile;
		System.out.println("Owl File is " + this.owlFile);
		String[] files = fileNames.split(";"); //)prepareReader(fileNames, customBase, owlFile);
		//logger.setLevel(Level.WARN);
		openEngineWithConnection(engineName);
		createTypes();

		// check if I am in the environment
		getBaseFolder();
		createSQLTypes();

		//try to open the script file
		openScriptFile(engineName);

		scriptFile.println("-- ********* begin load process ********* ");
		engine.openDB(null);//dont need to specify file name, data source should exist at this point so we should be 
		
		//first find all indexes, drop current ones, store off those current ones to recreate them when the process completes
		findIndexes(engineName);

		for(int i = 0; i<files.length;i++)
		{
			String fileName = files[i];
			scriptFile.println("-- ********* begin load " + fileName + " ********* ");
			findTables(engineName);		
			// load the prop file for the CSV file 
			if(propFileExist){
				openProp(propFile);
			} else {
				rdfMap = rdfMapArr[i];
			}
			// determine the type of data in each column of CSV file
			createProcessors();
			openCSVFile(fileName); //moved openCSVFile down because we need the variables that are set in createProcessors
			try {
				processConceptURIs();
				processProperties();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			recreateRelations();
			createTables();
			skipRows();
			insertRecords();
			cleanAll();
			commitDB();
			scriptFile.println("-- ********* completed processing file " + fileName + " ********* ");
		}
		cleanUpDBTables(engineName);
		runDBModTransactions(recreateIndexesArr); 
		closeDB();
		cleanAll(); //clean again because we reset the values for availableTables and availableTablesInfo
		writeDefaultQuestionSheet(engineName);
		updateDefaultQuestionSheet(engineName);

		createBaseRelations();
		try{
			scriptFile.println("-- ********* completed load process ********* ");
			scriptFile.close();
		} catch (Exception e){
			e.printStackTrace();
		}
	}


	public void closeDB()
	{
		engine.closeDB();
	}

	public void commitDB(){
		engine.commit();
	}

	private void getBaseFolder()
	{
		if(dbBaseFolder == null || dbBaseFolder.length() == 0)
		{
			dbBaseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\", System.getProperty("file.separator"));
		}
		if(semossURI == null || semossURI.length() == 0)
			semossURI = DIHelper.getInstance().getProperty(Constants.SEMOSS_URI);

	}

	private void openOWLWithOutConnection() throws EngineException {
		Repository myRepository = new SailRepository(new MemoryStore());
		try {
			myRepository.initialize();
			rcOWL = myRepository.getConnection();
		} catch (RepositoryException e) {
			e.printStackTrace();
			throw new EngineException("Could not create new repository connection to store OWL information");
		} 

		scOWL = ((SailRepositoryConnection) rcOWL).getSailConnection();
		vfOWL = rcOWL.getValueFactory();
		vf = vfOWL;
	}

	private void openOWLWithConnection(IEngine engine) throws EngineException {
		Repository myRepository = new SailRepository(new MemoryStore());
		try {
			myRepository.initialize();
			rcOWL = myRepository.getConnection();
			rcOWL.begin();
			scOWL = ((SailRepositoryConnection) rcOWL).getSailConnection();
			vfOWL = rcOWL.getValueFactory();
			vf = vfOWL;
		} catch (RepositoryException e) {
			e.printStackTrace();
			throw new EngineException("Could not create new repository connection to store OWL information");
		} 

		baseDataEngine = ((AbstractEngine)engine).getBaseDataEngine();
		baseDataHash = ((AbstractEngine)engine).getBaseHash();

		RepositoryConnection existingRC = ((RDFFileSesameEngine) baseDataEngine).getRc();
		// load pre-existing base data
		RepositoryResult<org.openrdf.model.Statement> rcBase = null;
		try {
			rcBase = existingRC.getStatements(null, null, null, false);
			List<org.openrdf.model.Statement> rcBaseList = null;
			rcBaseList = rcBase.asList();
			Iterator<org.openrdf.model.Statement> iterator = rcBaseList.iterator();
			while(iterator.hasNext()){
				logger.info(iterator.next());
			}
			rcOWL.add(rcBaseList);
		} catch (RepositoryException e) {
			e.printStackTrace();
			throw new EngineException("Could not load OWL information from existing database");
		}
	}


	// How the logic for the RDBMS creation works
	// Open the CSV file - this will give the header list
	// Get all the concepts first - Each Concept will end up being a new table
	// For each concept get the list of properties - the properties will eventually become columns of the tables
	// For the nodes that have been linked through relationships
	// find what is the property that connects one table to another - there has to be some way to specify this, we can predict it for now
	// Now process the relationships
	// the relationships identify what are the concepts we are going after and then try to find what is the common property - If there is more than one we have an issue here
	// there needs to be something in the UI that shows it - for resolution

	private void processConceptURIs() throws Exception
	{
		if(rdfMap.get("RELATION") != null)
		{
			String relationNames = rdfMap.get("RELATION");
			StringTokenizer relationTokens = new StringTokenizer(relationNames, ";");
			relationArrayList = new ArrayList<String>();
			// process each relationship
			while(relationTokens.hasMoreElements())
			{
				String relation = relationTokens.nextToken();
				// just in case the end of the prop string is empty string or spaces
				if(!relation.contains("@"))
					break;

				relationArrayList.add(relation);
				logger.info("Loading relation " + relation);            	
				String[] strSplit = relation.split("@");
				// get the subject and object for triple (the two indexes)
				String sub = strSplit[0];
				String subject = "";
				String predicate = strSplit[1]; // this needs to be ignored
				String obj = strSplit[2];
				String object = "";

				//guide FK creation by grabbing where the asterisk is on the predicate if one exists
				String objTemp = obj;
				String subTemp = sub;
				int locateAsterisk = predicate.indexOf('*');
				if(locateAsterisk == 0){ //if its at the beginning then the subject should be the FK
					subTemp+="*";
				} else if(locateAsterisk > 0){ //if its anywhere else supposedly the end then the object should be the FK
					objTemp+="*";
				}

				// check if prop file entries are not in excel and if nodes are concatenations
				// throw exception if prop file entries not in excel
				boolean headException = true;
				if(sub.contains("+"))
				{
					headException = isProperConcatHeader(sub);
				}
				else
				{
					if(!headerList.contains(sub))
						headException = false;
				}
				if(headException == false) {
					throw new HeaderClassException(sub + " cannot be found as a header");
				}
				if(obj.contains("+"))
				{
					headException = isProperConcatHeader(obj);
				}
				else
				{
					if(!headerList.contains(obj))
						headException = false;
				}
				if(headException == false) {
					throw new HeaderClassException(obj + " cannot be found as a header");
				}


				// sub now has the fully qualified tableName
				tableHash.put(sub, new Hashtable());
				processClassMetaData(sub, semossURI + "/" + Constants.DEFAULT_NODE_CLASS +"/", baseConceptURIHash);
				processClassMetaData(sub, customBaseURI+ "/" + Constants.DEFAULT_NODE_CLASS +"/", conceptURIHash);
				// see if object node instance URI exists in prop file
				tableHash.put(obj, new Hashtable());
				processClassMetaData(obj, semossURI + "/" + Constants.DEFAULT_NODE_CLASS +"/", baseConceptURIHash);
				processClassMetaData(obj, customBaseURI + "/" + Constants.DEFAULT_NODE_CLASS + "/", conceptURIHash);
				// put the relationship
				// I need something that does the other way around or may be note
				relHash.put(predicate + sub + obj, subTemp + "@" + objTemp);
			}
		}		
	}

	private void processClassMetaData(String sub, String URI, Hashtable uriHash)
	{
		String subject = "";
		String idxBaseURI = "";
		// add to base concept URI
		if(rdfMap.containsKey(sub+Constants.CLASS))
		{
			uriHash.put(sub+Constants.CLASS,rdfMap.get(sub+Constants.CLASS));
		}
		// if no user specific URI, use generic SEMOSS base URI
		else
		{
			if(sub.contains("+"))
			{
				subject = processAutoConcat(sub);
				idxBaseURI = URI + subject;
			}
			else
			{
				subject = sub;
				idxBaseURI = URI + subject;
			}
			uriHash.put(subject+Constants.CLASS, idxBaseURI);
		}
	}

	private void processProperties() throws Exception
	{
		if(rdfMap.get("NODE_PROP") != null)
		{
			String nodePropNames = rdfMap.get("NODE_PROP");
			StringTokenizer nodePropTokens = new StringTokenizer(nodePropNames, ";");

			if(basePropURI.equals("")){
				basePropURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + CONTAINS +"/";
			}

			while(nodePropTokens.hasMoreElements())
			{
				String relation = nodePropTokens.nextToken();
				// in case the end of the prop string is empty string or spaces
				if(!relation.contains("%"))
					break;
				logger.info("Loading Node Prop " + relation);            	
				String[] strSplit = relation.split("%");
				// get the subject and object for triple (the two indexes)
				String sub = strSplit[0];
				// loop through all properties on the node
				for(int i = 1; i < strSplit.length; i++)
				{
					String prop = strSplit[i];

					boolean headException = true;
					if(sub.contains("+"))
					{
						headException = isProperConcatHeader(sub);
					}
					else
					{
						if(!headerList.contains(sub))
							headException = false;
					}
					if(headException == false) {
						throw new HeaderClassException(sub + " cannot be found as a header");
					}
					if(prop.contains("+"))
					{
						headException = isProperConcatHeader(prop);
					}
					else
					{
						if(!headerList.contains(prop))
							headException = false;
					}
					if(headException == false) {
						throw new HeaderClassException(prop + " cannot be found as a header");
					}
					// add it to the subject
					//see if the subject key exists
					Hashtable columnHash = new Hashtable();
					if(tableHash.containsKey(sub))
						columnHash = tableHash.get(sub);
					else
					{
						processClassMetaData(sub, semossURI + "/" + Constants.DEFAULT_NODE_CLASS +"/", baseConceptURIHash);
						processClassMetaData(sub, customBaseURI + "/" + Constants.DEFAULT_NODE_CLASS +"/", conceptURIHash);
					}
					// now get the type
					String index = typeIndices.get(prop);
					String type = rdfMap.get(index);
					columnHash.put(prop, type);
					tableHash.put(sub, columnHash);

					// for now I am not adding properties to it
					processPropertyMetaData(semossURI + "/" + Constants.DEFAULT_NODE_CLASS +"/" + processAutoConcat(sub), prop);
				}
			}
		}
	}

	private void processPropertyMetaData(String parentNodeURI, String property)
	{
		// add the base property first
		property = processAutoConcat(property);
		String propURI = semossURI + "/" + Constants.DEFAULT_PROPERTY_CLASS + "/" + property;
		basePropURIHash.put(propURI, propURI);

		// now need to add a new one for the class as well
		basePropRelations.put(propURI, parentNodeURI);
	}

	private void recreateRelations()
	{
		Enumeration <String> relKeys = relHash.keys();
		while(relKeys.hasMoreElements())
		{
			// get the relationship key as it is first
			String relKey = relKeys.nextElement();
			String tables = relHash.get(relKey);

			String [] tableTokens = tables.split("@");
			String table1 = tableTokens[0];
			String table2 = tableTokens[1];
			boolean table1IsTheFK = table1.contains("*") && !table2.contains("*");
			boolean table2IsTheFK = table2.contains("*") && !table1.contains("*");
			table1 = table1.replaceAll("\\*", "");
			table2 = table2.replaceAll("\\*", "");
			//System.out.println("table1: " +table1 + " table2: "+ table2);

			Hashtable columnHash1 = tableHash.get(table1);
			Hashtable columnHash1Tmp = null;
			//create a temp column hash to store the available table column's if the new table has 0 columns.  This is so we have an accurate check below.
			if(columnHash1.size() == 0 && availableTables.containsKey(table1.toUpperCase())){ //check available tables to see if they have any columns
				columnHash1Tmp = availableTables.get(table1.toUpperCase());
			} else {
				columnHash1Tmp = columnHash1;
			}
			String rowCount1Obj = availableTablesInfo.get(table1.toUpperCase());
			int rowCount1 = (rowCount1Obj==null)?totalrowcount:Integer.parseInt(rowCount1Obj);

			Hashtable columnHash2 = tableHash.get(table2);
			Hashtable columnHash2Tmp = null;
			//create a temp column hash to store the available table column's if the new table has 0 columns.  This is so we have an accurate check below.
			if(columnHash2.size() == 0 && availableTables.contains(table2.toUpperCase())){ //check available tables to see if they have any columns
				columnHash2Tmp = availableTables.get(table2.toUpperCase());
			} else {
				columnHash2Tmp = columnHash2;
			}
			String rowCount2Obj = availableTablesInfo.get(table2.toUpperCase());
			int rowCount2 = (rowCount2Obj==null)?totalrowcount:Integer.parseInt(rowCount2Obj);

			String commonKey = findCommon(columnHash1Tmp, columnHash2Tmp);
			relHash.remove(relKey);
			String newRelationName = null;
			String [] subPredObj = new String[3];
			if(commonKey == null)
			{
				// set the relationship based on property
				// see which one of these has most columns
				String modifyingTable = table1;
				Hashtable targetHash = columnHash1;
				if((table1IsTheFK) || //if table 1 is the fk column
						(!table2IsTheFK && rowCount1 < rowCount2) ||  //or if table2 is NOT the fk column and table two has more rows than table 1
						(!table2IsTheFK && (rowCount1 == rowCount2) && (columnHash1Tmp.size() > columnHash2Tmp.size()))) //or if table 2 is NOT the fk and the table row counts are the same and table one has more columns than table two
				{
					modifyingTable = table2;
					targetHash = columnHash2;
					// add FK to foreign key so that I 
					targetHash.put(table1 + "_FK", "STRING");
					tableHash.put(table2, targetHash);
					createRelation(table1, table2, table1, table1 + "_FK");
				}
				else
				{
					modifyingTable = table1;
					targetHash = columnHash1;
					targetHash.put(table2 + "_FK", "STRING");
					tableHash.put(table1, targetHash);
					createRelation(table2, table1, table2, table2 + "_FK");
				}
			}
			if(commonKey != null)
			{
				createRelation(table1, table2, commonKey, commonKey);
			}
		}
	}

	public void createRelation(String fromTable, String toTable, String fromProp, String toProp)
	{
		String [] subPredObj = new String[3];

		if(fromTable.contains("+")){
			fromTable = processAutoConcat(fromTable);
			fromProp = processAutoConcat(fromTable);
		}
		if(toTable.contains("+")){
			toTable = processAutoConcat(toTable);
			toProp = processAutoConcat(toTable);
		}

		fromTable = realClean(fromTable);
		toTable = realClean(toTable);
		fromProp = realClean(fromProp);
		toProp = realClean(toProp);

		String newRelationName = fromTable + "." + fromProp + "." + toTable + "." + toProp;

		// set the relationURI for the front end
		String relSemossBaseURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + newRelationName + "/" + fromTable + "_" + toTable; // this is the culprit

		subPredObj[0] = semossURI + "/" + Constants.DEFAULT_NODE_CLASS +"/" + fromTable; // turn this into URI
		subPredObj[1] = relSemossBaseURI;
		subPredObj[2] = semossURI + "/" + Constants.DEFAULT_NODE_CLASS +"/" + toTable; // turn this into an URI

		baseRelations.put(newRelationName, subPredObj);	

		baseRelationURIHash.put(relSemossBaseURI+Constants.CLASS, relSemossBaseURI);
	}

	private String realClean(String inputString)
	{
		inputString = inputString.replaceAll("\\+", "_");
		// now clean it up
		inputString = Utility.cleanString(inputString,true,true);//should the last param (replacequotes) be true or false?
		// finally finish it up with the replacing -
		inputString = inputString.replaceAll("-", "_");

		return inputString;

	}

	private void createTables()
	{
		// the job here is to take the table hash and create tables
		Enumeration <String> tableKeys = tableHash.keys();
		while(tableKeys.hasMoreElements())
		{
			boolean modifiedTable = true;
			String tableKey = tableKeys.nextElement();
			String modString = "";
			if(!availableTables.containsKey(tableKey.toUpperCase()))
			{
				modString = getCreateString(tableKey);
				tables.add(tableKey);
			}
			else if(!allColumnsMatch(tableKey))
				modString = getAlterTable(tableKey);
			else{
				modifiedTable = false;
			}

			allTablesModified.add(tableKey);

			if(modString.length()>0){
				try {
					scriptFile.println(modString + ";");
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				modifyDB(modString);
			} /* for debugging : else {
				try {
					scriptFile.println("-- no create or alter statement needed for table: "+ tableKey);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} */
		}
	}
	/**
	 * 
	 * @return a master list of all of the columns in each of the tables up to this point.
	 */
	private Hashtable <String, Hashtable<String, String>> getAllColumnsHash(){
		Hashtable <String, Hashtable<String, String>> allColsHash = new Hashtable();

		//for each table modified during this process, get the columns and put them into a hashtable
		for(String table: allTablesModified){
			if(tableHash.containsKey(table)){
				Hashtable<String, String> cols = new Hashtable();
				if(availableTables.containsKey(table.toUpperCase()))
					cols=(Hashtable)availableTables.get(table.toUpperCase()).clone();//default it to hold all the columns that are existing that are being updated
				//cols = availableTables.get(table); //default it to hold all the columns that are existing that are being updated

				//Hashtable availableTableColumns = null;
				//availableTableColumns = availableTables.get(table.toUpperCase());
				Hashtable newColumnsHash = tableHash.get(table);
				//check each column in the availableTableColumns to see if the new columnHash has a new column
				Enumeration <String> newColumnKeys = newColumnsHash.keys();

				//loop through the new columns + the keys (pks, fks) and add any columns that werent already available
				while(newColumnKeys.hasMoreElements()){
					String column = newColumnKeys.nextElement();
					if(cols==null || !cols.containsKey(column.toUpperCase())){
						cols.put(column.toUpperCase(), (String) newColumnsHash.get(column) );
					}
				}
				allColsHash.put(table, cols);
			}
		}
		return allColsHash;
	}

	private boolean hasNewColumns(String table) {
		boolean hasNewColumn = false;
		Hashtable availableTableColumns = null;
		availableTableColumns = availableTables.get(table.toUpperCase());
		Hashtable newColumnsHash = tableHash.get(table);
		//check each column in the availableTableColumns to see if the new columnHash has a new column
		Enumeration <String> newColumnKeys = newColumnsHash.keys();
		while(newColumnKeys.hasMoreElements()){
			String column = newColumnKeys.nextElement();
			if(!availableTableColumns.containsKey(column.toUpperCase()))
			{
				hasNewColumn = true;
				break;
			}
		}
		return hasNewColumn;
	}

	private boolean allColumnsMatch(String table){
		boolean hasNewColumn = false;
		Hashtable availableTableColumns = availableTables.get(table.toUpperCase());
		Hashtable newColumnsHash = tableHash.get(table);
		//String upperAllNewCols = table.toUpperCase()+",";
		ArrayList<String> upperAllNewColsArr = new ArrayList();
		upperAllNewColsArr.add(table.toUpperCase());

		//convert all newColumnsHash values to upper case and only store off the FK cols
		Enumeration <String> newColumnKeys = newColumnsHash.keys();
		while(newColumnKeys.hasMoreElements()){
			String newColVal = newColumnKeys.nextElement().toUpperCase() ;
			//upperAllNewCols += newColVal +",";
			upperAllNewColsArr.add(newColVal);
		}

		//check each column in the availableTableColumns to see if the new columnHash has a new column
		newColumnKeys = newColumnsHash.keys();
		while(newColumnKeys.hasMoreElements()){
			String column = newColumnKeys.nextElement();
			if(!availableTableColumns.containsKey(column.toUpperCase()))
			{
				hasNewColumn = true;
				break;
			}
		}
		boolean allColumnsMatch = true;

		//check that the tables have the exact FK same columns
		Enumeration <String> availableColumnKeys = availableTableColumns.keys();
		while(availableColumnKeys.hasMoreElements()){
			String column = availableColumnKeys.nextElement();
			if(!upperAllNewColsArr.contains(column))
			{
				allColumnsMatch = false;
				break;
			}
		}
		if(allColumnsMatch && !hasNewColumn)
			return true;
		else 
			return false;
	}

	private boolean alteredTableHasOnlyPk(String table){
		boolean hasOnlyPk = false;
		Hashtable availableTableColumns = availableTables.get(table.toUpperCase());
		Hashtable newColumnsHash = tableHash.get(table);

		//if new columns is 0 for this table, and available columns is greater than 1 and the new columns has only contains the tables primary key
		if(availableTableColumns.size()>1 && newColumnsHash.size() == 0 ){
			hasOnlyPk = true;
		}

		return hasOnlyPk;
	}


	private boolean hasNewTableKeys(String table){
		boolean hasNewFKs = false;
		Hashtable availableTableColumns = availableTables.get(table.toUpperCase());
		Hashtable newColumnsHash = tableHash.get(table);
		String upperAllExistingCols = "";

		//check that the tables have the exact FK same columns
		Enumeration <String> availableColumnKeys = availableTableColumns.keys();
		while(availableColumnKeys.hasMoreElements()){
			String column = availableColumnKeys.nextElement();
			if(column.endsWith("_FK"))
			{
				upperAllExistingCols += column + ",";
			}
		}

		//convert all newColumnsHash values to upper case and only store off the FK cols and PK col 
		Enumeration <String> newColumnKeys = newColumnsHash.keys();
		while(newColumnKeys.hasMoreElements()){
			String newColVal = newColumnKeys.nextElement().toUpperCase() ;
			if(newColVal.endsWith("_FK") && !upperAllExistingCols.contains(newColVal)){
				hasNewFKs = true;
				break;
			}
		}

		return hasNewFKs;
	}

	private String getCreateString(String tableKey)
	{
		//if(tableKey.contains("+"))
		//	tableKey = processAutoConcat(tableKey);
		String SQLCREATE = "CREATE TABLE " + realClean(tableKey) + "(";
		SQLCREATE = SQLCREATE + " " + realClean(tableKey) + "  " + sqlHash.get("STRING"); // add its own column first as name

		boolean key1 = true;

		Hashtable columns = tableHash.get(tableKey);
		Enumeration <String> columnKeys = columns.keys();
		while(columnKeys.hasMoreElements())
		{
			if(key1)
			{
				SQLCREATE = SQLCREATE + " , ";
				key1 = false;
			}

			String column = columnKeys.nextElement();
			String type = (String)columns.get(column);
			// clean up the + first
			//column = column.replaceAll("\\+", "_");
			// now clean it up
			column = realClean(column); //,true);
			// finally finish it up with the replacing -
			//column = column.replaceAll("-", "_");
			SQLCREATE = SQLCREATE + column + "  " + sqlHash.get(type);
			if(columnKeys.hasMoreElements())
				SQLCREATE = SQLCREATE + " , ";
		}

		SQLCREATE = SQLCREATE + " )";

		//System.out.println("SQL CREATE IS " + SQLCREATE);
		return SQLCREATE;
	}

	private String getAlterTable(String tableKey)
	{
		//if(tableKey.contains("+"))
		//	tableKey = processAutoConcat(tableKey);
		String SQLALTER = "";
		if(queryUtil.getDatabaseType().equals(SQLQueryUtil.DB_TYPE.SQL_SERVER))			
			SQLALTER = "ALTER TABLE " + realClean(tableKey) + " ADD ";//For SQL Server, no "(", i.e. 'ALTER TABLE table_name ADD column_name datatype'
		else
			SQLALTER = "ALTER TABLE " + realClean(tableKey) + " ADD (";
		String columnString = "";

		Hashtable fieldHash = availableTables.get(tableKey.toUpperCase());

		Hashtable columns = tableHash.get(tableKey);

		// create the where column hash as well
		Hashtable whereColumnHash = new Hashtable();
		if(whereColumns.containsKey(tableKey.toUpperCase()))
			whereColumnHash = whereColumns.get(tableKey.toUpperCase());
		whereColumnHash.put(tableKey, "VARCHAR");


		Enumeration <String> columnKeys = columns.keys();
		while(columnKeys.hasMoreElements())
		{
			String column = columnKeys.nextElement();
			System.out.println("Table: " +tableKey + " column: "+column);
			column = realClean(column); //,true);
			if(!fieldHash.containsKey(column.toUpperCase()))
			{
				String type = (String)columns.get(column);
				type = sqlHash.get(type);
				// clean up the + first
				//column = column.replaceAll("\\+", "_");
				// now clean it up
				// finally finish it up with the replacing -
				//column = column.replaceAll("-", "_");
				column = column.toUpperCase();
				if(columnString.length() > 0)
					columnString = columnString + ", " + column + " " + type;
				else
					columnString = column + " " + type;
			}
			/*
			 * Dont know if we are over-riding it
			 */
			else
			{
				// remove this from tableHash
				whereColumnHash.put(column, fieldHash.get(column.toUpperCase())); // unless we are overriding it ?
				// remove it from table hash
				tableHash.remove(column);
			}
		}
		// put the list of where columns
		whereColumns.put(tableKey.toUpperCase(), whereColumnHash);

		SQLALTER = SQLALTER + columnString + " )";

		System.out.println("SQL ALTER IS " + SQLALTER);
		if(columnString.length() == 0){
			return "";
		}
		return SQLALTER;
	}

	private String getInsertString(String tableKey, Map <String, Object> jcrMap)
	{
		String VALUES = " (";		
		String value = createInstanceValue(tableKey, jcrMap);
		value = value.replaceAll("'", "''");	
		value = "'" +  value + "'"; // would the value be always string ?
		VALUES = VALUES + value;
		boolean key1 = true;
		Hashtable columns = tableHash.get(tableKey);
		Enumeration <String> columnKeys = columns.keys();
		StringBuffer valuesBuffer = new StringBuffer();
		valuesBuffer.append(VALUES);

		while(columnKeys.hasMoreElements())
		{
			if(key1)
			{
				valuesBuffer.append( " , ");
				key1 = false;
			}
			String key = columnKeys.nextElement();
			String type = (String)columns.get(key);
			key = key.replace("_FK", "");
			value = createInstanceValue(key, jcrMap);
			//if(!sqlHash.get(type).contains("FLOAT"))
			//	value = realClean(value);
			// escape SQL Values
			if(sqlHash.get(type).contains("VARCHAR") && value.length() != 0)
			{
				value = value.replaceAll("'", "''");
				value = "'" + value + "'" ;
			} else if (value.length() == 0) { //for all other data types OR when varchar and value is blank
				value = "null";
			} 

			valuesBuffer.append(value);
			if(columnKeys.hasMoreElements())
				valuesBuffer.append(" , ");

		}
		String SQLINSERT = valuesBuffer.toString() + ")";
		return SQLINSERT;
	}


	private String getAlterString(String tableKey, Map <String, Object> jcrMap, String insertTemplate, Hashtable <String, Hashtable<String, String>> allColumnsTableHash)
	{
		String VALUES = "";
		String SQLALTER = "";		
		Hashtable columns = tableHash.get(tableKey);

		Enumeration <String> columnKeys = columns.keys();
		// generate the set portion first
		StringBuffer valuesBuffer = new StringBuffer();
		StringBuffer selectClauseWhereBuffer = new StringBuffer();
		StringBuffer insertValsClauseBuffer = new StringBuffer();

		ArrayList<String> insertValsAliasClause = new ArrayList();
		valuesBuffer.append(VALUES);//yes this append doesn't do anything, BUT if someone changes the value of the where variable we wont have problems
		while(columnKeys.hasMoreElements())
		{
			String key = columnKeys.nextElement();
			String tempkey = key.replace("_FK", "");
			String value = createInstanceValue(tempkey, jcrMap);
			String type = (String)columns.get(key);

			boolean string = false;

			if(sqlHash.get(type).contains("VARCHAR") && value.length() != 0) //check for value length != 0 here before you append the quotes 
			{
				value = value.replaceAll("'", "''");
				value = "'" + value + "'" ;
				string = true;
			}

			String cleankey = realClean(key).toUpperCase();
			if(valuesBuffer.toString().length() == 0)
			{
				if(string || (value != null && value.length() != 0)){
					valuesBuffer.append(cleankey + " = " +  value); 
					selectClauseWhereBuffer.append( cleankey + " IS NULL ");
					insertValsClauseBuffer.append(value + " AS " + cleankey);
					insertValsAliasClause.add(cleankey);
					//TODO append to index creation logic?
				}
			}
			else
			{
				if(string || (value != null && value.length() != 0)){
					valuesBuffer.append(" , " + cleankey + " = " +  value);
					selectClauseWhereBuffer.append( " AND " + cleankey + " IS NULL ");
					insertValsClauseBuffer.append(" , " + value + " AS " + cleankey);	
					insertValsAliasClause.add(cleankey);
					//TODO append to index creation logic?					
				}					
			}
		}

		VALUES = valuesBuffer.toString();

		// now generate the where
		SQLALTER = insertTemplate + VALUES + " WHERE ";

		Hashtable whereHash = whereColumns.get(tableKey.toUpperCase());
		String where  = "";

		Enumeration <String> whereKeys = whereHash.keys();
		StringBuffer whereBuffer = new StringBuffer();
		StringBuffer indexBuffer = new StringBuffer();
		whereBuffer.append(where);//yes this append doesn't do anything, BUT if someone changes the value of the where variable we wont have problems
		while(whereKeys.hasMoreElements())
		{
			String key = whereKeys.nextElement();
			String value = createInstanceValue(key, jcrMap);

			String type = (String)whereHash.get(key);
			boolean string = false;

			if(type.contains("VARCHAR"))
			{
				value = value.replaceAll("'", "''");
				value = "'" + value + "'" ;
				string = true;
			}

			if(whereBuffer.length() == 0)
			{
				if(string || (value != null && value.length() != 0)){
					whereBuffer.append( key + " = " + value );
					indexBuffer.append( key );
				}
			}
			else
			{
				if(string || (value != null && value.length() != 0)){
					whereBuffer.append(" AND " + key + " = " + value ); //change to and instead of comma
					indexBuffer.append( " , " + key );
				}
			}			
		}

		SQLALTER = SQLALTER + " ( " + whereBuffer.toString() + ") ";

		//create indexes
		String indexOnTable =  tableKey + " ( " +  indexBuffer.toString() + " ) ";
		String indexName = "INDX_" + tableKey + indexUniqueId;
		String createIndex = "CREATE INDEX " + indexName + " ON " + indexOnTable;
		String dropIndex = queryUtil.getDialectDropIndex(indexName,tableKey);//"DROP INDEX " + indexName;
		if(tempIndexArray.size() ==0){
			singleDBModTransaction(createIndex);
			tempIndexArray.add(indexOnTable);
			tempDropArrays.add(dropIndex);
			indexUniqueId++;
		} else {
			boolean indexAlreadyExists = false; 
			for(String index: tempIndexArray){
				if(index.equals(indexOnTable)){//TODO check various order of keys since they are comma seperated
					indexAlreadyExists = true;
					break;
				}

			}
			if(!indexAlreadyExists){
				singleDBModTransaction(createIndex);
				tempDropArrays.add(dropIndex);
				tempIndexArray.add(indexOnTable);
				indexUniqueId++;
			}
		}

		//FIRST decide if you want to do an update or insert, run a select to figure out if you have any data where
		//the new column you just added has any null values for the criteria that you are doing the update for
		String whereclause = whereBuffer.toString();
		if(selectClauseWhereBuffer.length()>0)
			whereclause += " AND " + selectClauseWhereBuffer.toString();
		boolean isInsert = false;

		String getRowCount = queryUtil.getDialectSelectRowCountFrom(tableKey,whereclause);//"SELECT COUNT(1) AS ROWCOUNT FROM " + tableKey + " WHERE " + whereclause;
		//execute query
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, getRowCount);
		if(wrapper.hasNext()){
			ISelectStatement stmt = wrapper.next();
			String rowcount = stmt.getVar(queryUtil.getResultSelectRowCountFromRowCount()) + "";
			if(rowcount.equals("0")){
				isInsert = true;
			}
		}

		if(isInsert){
			String SQLINSERT = "";
			if(VALUES.length()==0) { //this is a true insert
				SQLINSERT = getInsertIntoClause(tableKey) + getInsertString(tableKey, jcrMap);
			} else {
				//doing a distinct here, alternatively we can just let it create all the dups and then delete the 
				//later anyway when we do the database clean up in the cleanUpDbTables method.
				String allColumns = ""; 
				String insertIntoClause = "";
				String insValsTmp[] = insertValsClauseBuffer.toString().split(",");

				//list of columns that we were going to update but are going to be the varying value when it comes to the insert statement
				for(String singleClause : insertValsAliasClause){
					if(insertIntoClause.length()>0) insertIntoClause+=" , ";
					insertIntoClause += singleClause;
				}

				//get all the columns you'll need for the insert statement
				Hashtable tableCols = allColumnsTableHash.get(tableKey);
				Enumeration<String> cols = tableCols.keys();

				//we want to pull the value columns out of the insert clause columns so that you only have the columns that are being copied
				// and not the ones we are setting the individual values for (so pulling out the the [xyz AS columnName]  columns)
				while(cols.hasMoreElements()){
					String colToAdd = cols.nextElement();
					if(!insertValsAliasClause.contains(colToAdd.toUpperCase()) ){
						if(allColumns.length() != 0) allColumns+= " , ";
						allColumns += realClean(colToAdd);
					} 
				}

				//now add the columns that you pulled out of the allColumns string back in 
				//(doing it this way because we can control the order of the insert and select clause) since the ORDER IS VERY IMPORTANT HERE
				String insertIntoClauseValues = "";
				if(allColumns.length() > 0) insertIntoClause = " , " + insertIntoClause; 
				if(allColumns.length() > 0) insertIntoClauseValues = " , " + insertValsClauseBuffer.toString(); 
				insertIntoClause = allColumns + insertIntoClause;
				insertIntoClauseValues = allColumns + insertIntoClauseValues;

				SQLINSERT = queryUtil.getDialectMergeStatement(tableKey, insertIntoClause, allColumns+ ", " +insertValsClauseBuffer.toString(), whereBuffer.toString());
			}
			return SQLINSERT;
		} else {

			if(VALUES.length()==0) {
				SQLALTER = "";
			}
			return SQLALTER;
		}
	}

	/**
	 * count the number of times a substring exists in a string.
	 * @param subStr the substring that you are searching/trying to get a count for
	 * @param str the string you are searching for the subStr in
	 * @return the number of times the subStr occurs in the str
	 */
	private static int countSubstring(String subStr, String str){
		return (str.length() - str.replace(subStr, "").length()) / subStr.length();
	}

	/**
	 * gets the merge statement, current unused
	 * @param tableKey
	 * @param jcrMap
	 * @param insertTemplate
	 * @return
	 */
	private String getMergeString(String tableKey, Map <String, Object> jcrMap, String insertTemplate)
	{
		String VALUES = "";		
		Hashtable columns = tableHash.get(tableKey);

		Enumeration <String> columnKeys = columns.keys();
		// generate the set portion first
		StringBuffer valuesBuffer = new StringBuffer();
		StringBuffer varIntoKeysBuffer = new StringBuffer();
		String commaStr = "";
		valuesBuffer.append(VALUES);//yes this append doesn't do anything, BUT if someone changes the value of the where variable we wont have problems

		while(columnKeys.hasMoreElements())
		{
			String key = columnKeys.nextElement();
			String value = createInstanceValue(key, jcrMap);
			String type = (String)columns.get(key);

			boolean string = false;

			if(sqlHash.get(type).contains("VARCHAR"))
			{
				value = value.replaceAll("'", "''");
				value = "'" + value + "'" ;
				string = true;
			}

			if(varIntoKeysBuffer.length() == 0)
			{
				if(string || (value != null && value.length() != 0)){
					varIntoKeysBuffer.append(realClean(key)); //VALUES = realClean(key) + " = " +  value;
					commaStr = (valuesBuffer.length() != 0)? " , " : ""; 
					valuesBuffer.append(commaStr + value);
				}

			}
			else
			{
				if(string || (value != null && value.length() != 0)){
					varIntoKeysBuffer.append(" , " + realClean(key)); //VALUES = VALUES + " , " + realClean(key) + " = " +  value;
					valuesBuffer.append(" , " + value);
				}
			}
		}

		Hashtable whereHash = whereColumns.get(tableKey.toUpperCase());
		String keysStr  = "";

		Enumeration <String> whereKeys = whereHash.keys();
		StringBuffer whereBuffer = new StringBuffer();
		StringBuffer keysBuffer = new StringBuffer();
		keysBuffer.append(keysStr);//yes this append doesn't do anything, BUT if someone changes the value of the where variable we wont have problems
		while(whereKeys.hasMoreElements())
		{
			String key = whereKeys.nextElement();
			String value = createInstanceValue(key, jcrMap);

			String type = (String)whereHash.get(key);
			boolean string = false;

			if(type.contains("VARCHAR"))
			{
				value = value.replaceAll("'", "''");
				value = "'" + value + "'" ;
				string = true;
			}

			if(whereBuffer.length() == 0)
			{
				if(string || (value != null && value.length() != 0))
					keysBuffer.append(key);
				commaStr = (varIntoKeysBuffer.length() != 0)? " , " : ""; 
				varIntoKeysBuffer.append(commaStr + key);
				commaStr = (valuesBuffer.length() != 0)? " , " : ""; 
				valuesBuffer.append(commaStr + value);
			}
			else
			{
				if(string || (value != null && value.length() != 0))
					keysBuffer.append(" , " + key);
				varIntoKeysBuffer.append(" , " + key); 
				valuesBuffer.append(" , " + value);
			}			
		}

		String SQLALTER = insertTemplate + "( " + varIntoKeysBuffer.toString() + " ) KEY ( "+ keysBuffer.toString() +" )";
		SQLALTER = SQLALTER + " SELECT " + valuesBuffer.toString() + " FROM DUAL ";

		return SQLALTER;
	}


	//duplicate check. unused at this time.
	private boolean findIfRecordAvailable(String tableKey, Map <String, Object> jcrMap)
	{
		Hashtable columns = tableHash.get(tableKey);

		Enumeration <String> columnKeys = columns.keys();
		String selfValue = createInstanceValue(tableKey, jcrMap);
		selfValue = selfValue.replaceAll("'", "''");
		selfValue = "'" + selfValue + "'" ;
		String query = "SELECT * FROM " + realClean(tableKey) + " WHERE " ;
		String VALUES = realClean(tableKey) + "=" + selfValue;

		// generate the set portion first
		while(columnKeys.hasMoreElements())
		{
			String key = columnKeys.nextElement();
			String value = createInstanceValue(key, jcrMap);
			String type = (String)columns.get(key);

			boolean string = false;

			if(sqlHash.get(type).contains("VARCHAR"))
			{
				value = value.replaceAll("'", "''");
				value = "'" + value + "'" ;
				//value = realClean(value);
				string = true;
			}
			if(string || (value != null && value.length() != 0))
				VALUES = VALUES + " AND " + realClean(key) + " = " +  value;			
		}

		query = query + VALUES;
		ISelectWrapper sWrapper = WrapperManager.getInstance().getSWrapper(engine, query);

		boolean hasNext = false;
		hasNext = sWrapper.hasNext();

		return hasNext;

	}

	private void insertRecords()
	{
		String [] insertTemplates = new String[tableHash.size()];
		// the job here is to take the table hash and create tables
		Enumeration <String> tableKeys = tableHash.keys();

		Hashtable <String, Hashtable<String, String>> allColumnsTableHash = getAllColumnsHash();//

		// get all the relation
		// first block is for creating the templates
		int tableIndex = 0;
		while(tableKeys.hasMoreElements())
		{
			String tableKey = tableKeys.nextElement();

			String SQLINSERT = null;

			// if the table was just created 
			// OR if the table already exists but so do all of the columns so we are actually just inserting to the existing table
			boolean tableAlreadyExists = availableTables.containsKey(tableKey.toUpperCase());
			if( !tableAlreadyExists || ( tableAlreadyExists && ( (!hasNewTableKeys(tableKey) && !alteredTableHasOnlyPk(tableKey)) || allColumnsMatch(tableKey) ) )){
				SQLINSERT = getInsertIntoClause(tableKey);
			}
			else
				SQLINSERT = "UPDATE " + tableKey.toUpperCase() + " SET "; // the paranthesis seems to change between dialect to dialect //SQLINSERT = "MERGE INTO " + tableKey.toUpperCase();

			insertTemplates[tableIndex] = SQLINSERT;
			tableIndex++;
		}

		// this block is for inserting the data
		Map<String, Object> jcrMap;

		// max row predetermined value
		int maxRows = 10000;
		// overwrite this value if user specified the max rows to load
		if (rdfMap.get("END_ROW") != null)
		{
			maxRows =  Integer.parseInt(rdfMap.get("END_ROW"));
		}
		// only start from the maxRow - the startRow
		// added -1 is because of index nature
		// the earlier rows should already have been skipped
		int numberOfTables = tableHash.size();

		StringBuffer[] insertBufferSql = new StringBuffer[numberOfTables];		
		ArrayList<String> updateBufferSqlList = new ArrayList<String>(); 

		int countIndividualUpdateStatements = 0, countInserts = 0, countUpdates = 0;
		try {
			while( (jcrMap = mapReader.read(header, processors)) != null && count<(maxRows))
			{
				// for each template do the mapping
				tableKeys = tableHash.keys();
				int index = 0;

				while(tableKeys.hasMoreElements()){
					String tableKey = tableKeys.nextElement();
					String SQL = "";

					if(insertBufferSql[index] == null){//init the string buffer through the first go
						insertBufferSql[index] = new StringBuffer();
					}

					//Important note: do inserts table by table so that you can generate one long insert statement, update statements dont go 
					//table by table, we need to search one array list for duplicate/similar update statements and make those more efficient
					// if the table was just created 
					// OR if the table already exists but so do all of the columns so we are actually just inserting to the existing table
					boolean tableAlreadyExists = availableTables.containsKey(tableKey.toUpperCase());
					if( !tableAlreadyExists || ( tableAlreadyExists && ( (!hasNewTableKeys(tableKey) && !alteredTableHasOnlyPk(tableKey)) || allColumnsMatch(tableKey)) ) ){
						SQL = insertTemplates[index] + getInsertString(tableKey, jcrMap);
					}
					else
					{
						//run query to see if you can just do an update statement, if so, run the generated update sql
						SQL = getAlterString(tableKey, jcrMap, insertTemplates[index], allColumnsTableHash);
					}

					if(SQL.length() > 0 ){
						try{
							if(!SQL.endsWith(";")) SQL+= ";";
							scriptFile.println(SQL);
							insertData(SQL);
						} catch (Exception e){
							e.printStackTrace();
						}
					}

					index++;
				}

			}


			// delete the indexes created and clear the arrays
			runDBModTransactions(tempDropArrays);
			tempDropArrays.clear();//clear the drop index sql text
			tempIndexArray.clear();//clear the index array text


		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	private String getInsertIntoClause(String tableKey){
		String SQLINSERT = "INSERT INTO   " + realClean(tableKey) +  "  (" + realClean(tableKey);

		Hashtable columns = tableHash.get(tableKey);
		Enumeration <String> columnKeys = columns.keys();
		boolean key1 = true;
		while(columnKeys.hasMoreElements())
		{	
			if(key1)
			{
				SQLINSERT = SQLINSERT + " , ";
				key1 = false;
			}
			String column = columnKeys.nextElement();
			SQLINSERT = SQLINSERT + realClean(column);
			if(columnKeys.hasMoreElements())
				SQLINSERT = SQLINSERT + ",";
		}
		SQLINSERT = SQLINSERT + ") VALUES "; // append the text 'values' here
		return SQLINSERT;
	}

	/**
	 * execute all queries in ArrayList queries
	 * @param queries contains all queries that are to be execute
	 * @param splitString string that splits the queries 
	 */
	private void runIncrementalTransaction(ArrayList<String> queries, String splitString){
		StringBuffer queryBuffer = new StringBuffer(); 
		String queryString = "";
		try{

			for(String query : queries){
				queryBuffer.append(query + splitString + " ");
			}
			queryString = queryBuffer.toString();
			if(queryString.length()>0){

				scriptFile.println(queryString); 		
				insertData(queryString); //write query to the sql file, then execute it

			}
			queries.clear();
		} catch (Exception e){
			e.printStackTrace();
			System.exit(0);
		}
	}

	/**
	 * execute all insert statements
	 * @param insertTemplates contains the insert statement portion of the statement
	 * @param bufferSql contains the values portion of the statement
	 */
	private void runIncrementalInsert(String[] insertTemplates,StringBuffer[] bufferSql){
		try{

			Enumeration <String> tableKeys = tableHash.keys();
			int indexForInsert = 0;
			while(tableKeys.hasMoreElements()){
				String tableKey = tableKeys.nextElement();
				String insertClause = "";
				if(insertTemplates[indexForInsert] != null) {
					insertClause = insertTemplates[indexForInsert];
				}

				if(bufferSql[indexForInsert] == null || bufferSql[indexForInsert].toString().equals("")){
					indexForInsert++;
					continue;
				}
				String SQL = insertClause + bufferSql[indexForInsert].toString();

				SQL = SQL.substring(0,SQL.length()-1); //get rid of that last comma

				scriptFile.println( SQL + ";");
				insertData(SQL); //write query to the sql file, then execute it

				indexForInsert++;
			}
		} catch (Exception e){
			e.printStackTrace();
			System.exit(0);
		}
	}

	private void openDB(String engineName)
	{
		try {
			//Class.forName("org.h2.Driver");
			String dbProp = writePropFile(engineName);
			engine = new RDBMSNativeEngine();
			engine.openDB(dbProp);
			//conn = DriverManager.
			//getConnection("jdbc:h2:" + dbFolder + "/" + dbName + "/database", "sa", "");
		} catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}

	private void insertData(String insert) throws SQLException
	{
		engine.insertData(insert);
	}

	private void modifyDB(String tableCreate)
	{
		engine.insertData(tableCreate);
	}



	// finds a common column between hash1 and hash2
	private String findCommon(Hashtable hash1, Hashtable hash2)
	{
		String retString = null;
		Enumeration keys = hash1.keys();
		while(keys.hasMoreElements())
		{
			String key = (String)keys.nextElement();
			if(hash2.containsKey(key))
			{
				retString = key;
				break;
			}
		}
		return retString;
	}



	/**
	 * Stores all possible variable types that the user can input from the CSV file into hashtable
	 * Hashtable is then used to match each column in CSV to a specific type based on user input in prop file
	 */
	public static void createTypes()
	{
		typeHash.put("DECIMAL", new ParseDouble());
		typeHash.put("DOUBLE", new ParseDouble());
		typeHash.put("STRING", new NotNull());
		typeHash.put("DATE", new ParseDate("yyyy-MM-dd hh:mm:ss"));
		typeHash.put("SIMPLEDATE", new ParseDate("MM/dd/yyyy"));
		// currently only add in numbers as doubles
		typeHash.put("NUMBER", new ParseDouble());
		typeHash.put("INTEGER", new ParseDouble());
		//		typeHash.put("NUMBER", new ParseInt());
		//		typeHash.put("INTEGER", new ParseInt());
		typeHash.put("BOOLEAN", new ParseBool());

		// now the optionals
		typeHash.put("DECIMAL_OPTIONAL", new Optional(new ParseDouble()));
		typeHash.put("DOUBLE_OPTIONAL", new Optional(new ParseDouble()));
		typeHash.put("STRING_OPTIONAL", new Optional());
		typeHash.put("DATE_OPTIONAL", new Optional(new ParseDate("yyyy-MM-dd HH:mm:ss")));
		typeHash.put("SIMPLEDATE_OPTIONAL", new Optional(new ParseDate("MM/dd/yyyy")));
		// currently only add in numbers as doubles
		typeHash.put("NUMBER_OPTIONAL", new Optional(new ParseDouble()));
		typeHash.put("INTEGER_OPTIONAL", new Optional(new ParseDouble()));
		//		typeHash.put("NUMBER_OPTIONAL", new Optional(new ParseInt()));
		//		typeHash.put("INTEGER_OPTIONAL", new Optional(new ParseInt()));
		typeHash.put("BOOLEAN_OPTIONAL", new Optional(new ParseBool()));
	}

	public void createSQLTypes()
	{
		sqlHash.put("DECIMAL", "FLOAT");
		sqlHash.put("DOUBLE", "FLOAT");
		sqlHash.put("STRING", "VARCHAR(400)");
		sqlHash.put("DATE", "TIME");
		sqlHash.put("SIMPLEDATE", "DATE");
		// currently only add in numbers as doubles
		sqlHash.put("NUMBER", "FLOAT");
		sqlHash.put("INTEGER", "FLOAT");
		//		typeHash.put("NUMBER", new ParseInt());
		//		typeHash.put("INTEGER", new ParseInt());
		sqlHash.put("BOOLEAN", "BOOLEAN");

		// not sure the optional is needed
	}


	/**
	 * Matches user inputed column type in prop file to the specific variable type name within Java SuperCSV API
	 */
	public void createProcessors()
	{
		// get the number columns in CSV file
		int numColumns = Integer.parseInt(rdfMap.get(NUMCOL));
		// Columns in prop file that are NON_OPTIMAL must contain a value
		String optional = ";";
		if(rdfMap.get(NOT_OPTIONAL) != null)
		{
			optional  = rdfMap.get(NOT_OPTIONAL);
		}

		int offset = 0;
		if(propFileExist){
			offset = 0;
		}
		processors = new CellProcessor[numColumns+offset];
		for(int procIndex = 1;procIndex <= processors.length;procIndex++)
		{
			// find the type for each column
			String type = rdfMap.get(procIndex+"");
			boolean opt = true;
			if(optional.indexOf(";" + procIndex + ";") > 1)
				opt = false;

			if(type != null && opt)
				processors[procIndex-1] = typeHash.get(type.toUpperCase() + "_OPTIONAL");
			else if(type != null)
				processors[procIndex-1] = typeHash.get(type.toUpperCase());
			else if(type == null)
				processors[procIndex-1] = typeHash.get("STRING_OPTIONAL");
		}
	}

	/**
	 * Specifies which rows in the CSV to load based on user input in the prop file
	 * @throws FileReaderException 
	 */
	public void skipRows() throws FileReaderException {
		//start count at 1 just row 1 is the header
		count = 1;
		int startRow = 2;
		if (rdfMap.get("START_ROW") != null)
			startRow = Integer.parseInt(rdfMap.get("START_ROW")); 
		try {
			while( count<startRow-1 && mapReader.read(header, processors) != null)// && count<maxRows)
			{
				count++;
				//logger.info("Skipping line: " + count);
			}

		} catch (IOException e) {
			e.printStackTrace();
			throw new FileReaderException("Error processing CSV headers");
		}
	}


	/**
	 * count the rows in the csv file
	 * @param fileName
	 * @throws FileReaderException
	 */
	public void getRowsCount(String fileName) throws FileReaderException{
		//get row count
		try{
			totalrowcount=0;//reset row count

			Map<String, Object> jcrMap;

			while( (jcrMap = mapReader.read(header, processors)) != null)
				totalrowcount++;


		} catch (IOException e) {
			e.printStackTrace();
			throw new FileReaderException("Error processing CSV headers");
		}		
	}

	/**
	 * Change the name of nodes that are concatenations of multiple CSV columns
	 * Example: changes the string "Cat+Dog" into "CatDog"
	 * @param 	input String name of the node that is a concatenation
	 * @return 	String name of the node removing the "+" to indicate a concatenation
	 */
	public String processAutoConcat(String input)
	{
		String[] split = input.split("\\+");
		String output = "";
		for (int i=0;i<split.length;i++)
		{
			if(i > 0)
				output = output+"_" + split[i];
			else
				output = split[i];
		}
		return output;
	}

	/**
	 * Determine if the node is a concatenation of multiple columns in the CSV file
	 * @param input 	String containing the name of the node
	 * @return boolean	Boolean that is true when the node is a concatenation 
	 */
	public boolean isProperConcatHeader(String input)
	{
		boolean ret = true;
		String[] split = input.split("\\+");
		for (int i=0;i<split.length;i++)
		{
			if (!headerList.contains(split[i]))
			{
				ret = false;
				break;
			}
		}
		return ret;
	}

	/**
	 * Constructs the node instance name
	 * @param subject 		String containing the node type name
	 * @param jcrMap 		Map containing the data in the CSV file
	 * @return retString 	String containing the instance level name
	 */
	public String createInstanceValue(String subject, Map <String, Object> jcrMap)
	{
		String retString ="";
		// if node is a concatenation
		if(subject.contains("+")) 
		{
			String elements[] = subject.split("\\+");
			for (int i = 0; i<elements.length; i++)
			{
				String subjectElement = elements[i];
				if(jcrMap.containsKey(subjectElement) && jcrMap.get(subjectElement)!= null)
				{
					String value = jcrMap.get(subjectElement) + "";
					value = realClean(value); //, true);

					retString = retString  + value + "-";
				}
				else
				{
					retString = retString  + "null-";
				}
			}
			// a - will show up at the end of this and we need to get rid of that
			if(!retString.equals(""))
				retString = retString.substring(0,retString.length()-1);
		}
		else
		{
			if(jcrMap.containsKey(subject) && jcrMap.get(subject)!= null)
			{
				// clean it only if the type happens to be something other than number
				String value = jcrMap.get(subject) + "";
				//value = realClean(value);
				retString = value;
			}
		}
		return Utility.cleanString(retString,true,false); //clean string 
	}

	/**
	 * Retrieves the data in the CSV file for a specified string
	 * @param object 	String containing the object to retrieve from the CSV data
	 * @param jcrMap 	Map containing the data in the CSV file
	 * @return Object	The CSV data mapped to the object string
	 */
	public Object createObject(String object, Map <String, Object> jcrMap)
	{
		// need to do the class vs. object magic
		if(object.contains("+"))
		{
			StringBuilder strBuilder = new StringBuilder();
			String[] objList = object.split("\\+");
			for(int i = 0; i < objList.length; i++){
				strBuilder.append(jcrMap.get(objList[i])); 
			}
			return strBuilder.toString();
		}

		return jcrMap.get(object);
	}

	/**
	 * Setter to store the metamodel created by user as a Hashtable
	 * @param data	Hashtable<String, String> containing all the information in a properties file
	 */
	public void setRdfMapArr(Hashtable<String, String>[] rdfMapArr) {
		this.rdfMapArr = rdfMapArr;
		propFileExist = false;
	}

	/**
	 * Load the CSV file
	 * Gets the headers for each column and reads the property file
	 * @param fileName String
	 * @throws FileReaderException 
	 * @throws FileNotFoundException 
	 */
	public void openCSVFile(String fileName) throws FileReaderException {
		FileReader readCSVFile;
		// loop through twice, first time open up the mapreader so we can loop through and get the rows count, then reopen it for
		// processing in the insertrecords method
		for(int i = 0; i < 2; i++){ 
			try {
				readCSVFile = new FileReader(fileName);
				mapReader = new CsvMapReader(readCSVFile, CsvPreference.STANDARD_PREFERENCE);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				throw new FileReaderException("Could not find CSV file located at " + fileName);
			}		
			try {
				header = mapReader.getHeader(true);
				headerList = Arrays.asList(header);
				// last header in CSV file is the absolute path to the prop file
				//propFile = header[header.length-1];
				// also keep this in the index now
				for(int headerIndex = 1;headerIndex <= header.length;headerIndex++)
					typeIndices.put(header[headerIndex-1], headerIndex+"");

			} catch (IOException e) {
				e.printStackTrace();
				throw new FileReaderException("Could not close reader input stream for CSV file " + fileName);
			}
			if(i==0){ // first time through loop through the csv file to get the total row count, 
				// need to loop through a second time to reset the current line position on the csv reader back to the beginning.
				getRowsCount(fileName);
			}
		}
	}

	protected void storeBaseStatement(String sub, String pred, String obj) throws EngineException {
		try {
			if(!scOWL.isActive() || !scOWL.isOpen()) {
				scOWL.begin();
			}
			scOWL.addStatement(vf.createURI(sub), vf.createURI(pred), vf.createURI(obj));
			scOWL.commit();
		} catch (SailException e) {
			e.printStackTrace();
			throw new EngineException("Error adding triple {<" + sub + "> <" + pred + "> <" + obj + ">}");
		}
		if(baseDataEngine != null && baseDataHash != null)
		{
			baseDataEngine.addStatement(new Object[]{sub, pred, obj, true});
			baseDataHash.put(sub, sub);
			baseDataHash.put(pred, pred);
			baseDataHash.put(obj,obj);
		}
	}


	protected void createBaseRelations() throws EngineException, FileWriterException {
		// necessary triple saying Concept is a type of Class
		String sub = semossURI + "/" + Constants.DEFAULT_NODE_CLASS;
		String pred = RDF.TYPE.stringValue();
		String obj = Constants.CLASS_URI;

		//I dont need it to go to the database
		// create base relations for concepts

		storeBaseStatement(sub, pred, obj);

		// necessary triple saying Relation is a type of Property
		sub =  semossURI + "/" + Constants.DEFAULT_RELATION_CLASS;
		pred = RDF.TYPE.stringValue();
		obj = Constants.DEFAULT_PROPERTY_URI;
		//createStatement(vf.createURI(sub), vf.createURI(pred), vf.createURI(obj));
		storeBaseStatement(sub, pred, obj);

		// samething with property
		if(basePropURI.equals("")){
			basePropURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + CONTAINS;
		}
		storeBaseStatement(basePropURI, Constants.SUBPROPERTY_URI, basePropURI);

		// concepts go first
		Iterator<String> baseHashIt = baseConceptURIHash.keySet().iterator();
		//now add all of the base relations that have been stored in the hash.
		while(baseHashIt.hasNext()){
			String subjectInstance = baseHashIt.next() +"";
			String predicate = Constants.SUBCLASS_URI;
			//convert instances to URIs
			String subject = baseConceptURIHash.get(subjectInstance); // +"", false);
			String object = semossURI + "/Concept";
			// create the statement now
			//createStatement(vf.createURI(subject), vf.createURI(predicate), vf.createURI(object));
			// add base relations URIs to OWL
			storeBaseStatement(subject, predicate, object);
		}
		// relations go next
		baseHashIt = baseRelationURIHash.keySet().iterator();
		while(baseHashIt.hasNext()){
			String subjectInstance = baseHashIt.next() +"";
			String predicate = Constants.SUBPROPERTY_URI;
			//convert instances to URIs
			String subject = baseRelationURIHash.get(subjectInstance);// +"", false);
			String object = semossURI + "/Relation";
			// create the statement now
			//createStatement(vf.createURI(subject), vf.createURI(predicate), vf.createURI(object));
			// add base relationship URIs to OWL
			storeBaseStatement(subject, predicate, object);
		}

		// now write the actual relations
		// relation instances go next
		for(String[] relArray : baseRelations.values()){
			String subject = relArray[0];
			String predicate = relArray[1];
			String object = relArray[2];

			//			createStatement(vf.createURI(subject), vf.createURI(predicate), vf.createURI(object));
			storeBaseStatement(subject, predicate, object);
			//			logger.info("RELATION TRIPLE:::: " + subject +" "+ predicate +" "+ object);
		}
		// I need to write one now for creating properties as well
		// this is where I will do properties
		// add the base relation first
		storeBaseStatement(semossURI + "/" + Constants.DEFAULT_PROPERTY_CLASS, RDF.TYPE+"", semossURI + "/" + Constants.DEFAULT_RELATION_CLASS);

		baseHashIt = basePropURIHash.keySet().iterator();
		while(baseHashIt.hasNext()){
			String subjectInstance = baseHashIt.next() +"";
			String predicate = RDF.TYPE +"";
			//convert instances to URIs
			String subject = subjectInstance; //baseRelationURIHash.get(subjectInstance);// +"", false);
			String object = semossURI + "/" + Constants.DEFAULT_PROPERTY_CLASS;
			// create the statement now
			// base property uri is like
			// Relation/Contains/MovieBudget RDFS:SUBCLASSOF /Relation/Contains
			storeBaseStatement(subject, predicate, object);
		}

		// now write the actual relations
		// relation instances go next
		for(String relArray : basePropRelations.keySet()){
			String property = relArray;
			String parent = basePropRelations.get(property);

			//			createStatement(vf.createURI(subject), vf.createURI(predicate), vf.createURI(object));
			storeBaseStatement(parent, OWL.DatatypeProperty+"", property);
			//			logger.info("RELATION TRIPLE:::: " + subject +" "+ predicate +" "+ object);
		}

		try {
			scOWL.commit();
		} catch (SailException e) {
			throw new EngineException("Could not commit base relationships into OWL database");
		}
		if(baseDataEngine != null) {
			baseDataEngine.commit();
		}
		// create the OWL File
		FileWriter fWrite = null;
		try {
			fWrite = new FileWriter(owlFile);
			RDFXMLPrettyWriter owlWriter  = new RDFXMLPrettyWriter(fWrite); 
			rcOWL.export(owlWriter);
			fWrite.close();
			owlWriter.close();
		} catch (RepositoryException e) {
			e.printStackTrace();
			throw new FileWriterException("Could not export base relationships from OWL database");
		} catch (RDFHandlerException e) {
			e.printStackTrace();
			throw new FileWriterException("Could not export base relationships from OWL database");
		} catch (IOException e) {
			e.printStackTrace();
			throw new FileWriterException("Could not close OWL file writer");
		} finally {
			try {
				if(fWrite!=null)
					fWrite.close();
			}catch(IOException e) {
				e.printStackTrace();
			}
		}

		closeOWL();
	}

	/**
	 * Close the OWL engine
	 * @throws EngineException 
	 */
	protected void closeOWL() throws EngineException {
		try {
			scOWL.close();
			rcOWL.close();
		} catch (SailException e1) {
			e1.printStackTrace();
			throw new EngineException("Could not close OWL database connection");
		} catch (RepositoryException e) {
			e.printStackTrace();
			throw new EngineException("Could not close OWL database connection");
		}
	}

	/* function used for debugging, leaving it here for now 
	private String currentDate(){
		Date dt = new Date();
		SimpleDateFormat dtFormatter = new SimpleDateFormat("MM/dd/yy HH:mm:ss:SSS");

		return dtFormatter.format(dt);
	}*/

}