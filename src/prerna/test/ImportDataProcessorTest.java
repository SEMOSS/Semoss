package prerna.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

import prerna.error.EngineException;
import prerna.error.FileReaderException;
import prerna.error.FileWriterException;
import prerna.error.HeaderClassException;
import prerna.error.NLPException;
import prerna.om.Insight;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.ui.components.ImportDataProcessor;
import prerna.ui.components.ImportDataProcessor.DB_TYPE;
import prerna.ui.components.ImportDataProcessor.IMPORT_METHOD;
import prerna.ui.components.ImportDataProcessor.IMPORT_TYPE;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
* ImportDataProcessorTest checks the 3 main Components of the ImportDataProcessor class
* 1) Creating a new Database
* 2) Adding to and existing Database
* 3) Overriding/Replacing an existing Database
* There are two tests for each one: CSV and Excel
*
* @author  August Bender
* @version 1.1
* @since   05-26-2015 
* Questions? Email abender@deloitte.com
*/
public class ImportDataProcessorTest {

	//Core obj
	private static ImportDataProcessor processor;
	@SuppressWarnings("unused")
	private BigDataEngine engine;
	
	private static String semossDirectory;
	private static String dbDirectory;
	private static String customBaseURI = "http://theTest";
	
	//CSV Var
	private static String newDBnameCSV = "OneData";
	private static String fileNameCSV;
	private static String mapFileCSV;
	private static String dbPropFileCSV;
	private static String CSVsmss; 
	//CSV Replace 
	private String repoCSV = newDBnameCSV;
	private static String replacementCSV;
	private static String replacementMapFileCSV;
	private static String replacementdbPropFileCSV;
	
	
	//EXCEL Var
	private static String newDBnameEXCEL = "ExcelTest";
	private static String fileNameEXCEL;
	private static String mapFileEXCEL; 
	private static String dbPropFileEXCEL;
	private static String EXCELsmss;
	//Excel Replace
	private static String replacementExcel;
	
	//NLP Var
	private static String fileNameNLP;
	
	//OCR Var
	private static String fileNameOCR;
	
	//General Var
	static String newDBname = "DeleteMe";
	static String newDBnameSmss;
	static int  testCounter = 1;
	
	@BeforeClass 
	public static void setUpOnce() throws EngineException, FileReaderException, HeaderClassException, FileWriterException, NLPException{
		
		//Set up file paths
		semossDirectory = System.getProperty("user.dir");
		semossDirectory = semossDirectory.replace("\\", "\\\\");
		dbDirectory = semossDirectory + "\\db\\";
		String testFolder = semossDirectory + "\\test\\";
		
		newDBnameSmss = dbDirectory + "\\" + newDBname + ".smss";
		
		//CSV Var
		fileNameCSV = testFolder + "SEMOSSus-500.csv";
		mapFileCSV = testFolder + newDBnameCSV +"_Custom_Map.prop";
		dbPropFileCSV = testFolder + newDBnameCSV +"_Test_PROP.prop";
		CSVsmss = dbDirectory + "\\" + newDBnameCSV + ".smss";
		//CSV Replace 
		replacementCSV = testFolder + "TwoData.csv";
		replacementMapFileCSV = testFolder + "TwoData_Custom_Map.prop";
		replacementdbPropFileCSV = testFolder + "TwoData_Test_PROP.prop";
		
		//EXCEL Var
		newDBnameEXCEL =				 "ExcelTest";
		fileNameEXCEL = testFolder + newDBnameEXCEL+".xlsm";
		mapFileEXCEL = testFolder + newDBnameEXCEL+"_Custom_Map.prop"; 
		dbPropFileEXCEL = testFolder + newDBnameEXCEL+"_Custom_Map.prop";
		EXCELsmss = dbDirectory + "\\" + newDBnameEXCEL + ".smss";
		//Excel Replace
		replacementExcel = testFolder + "ExcelReplace.xlsm";
		
		System.out.println("Cleaning...");
		if(new File(dbDirectory+newDBnameCSV).exists()){
			clean();
		}
	}

	@Before
	public void setUp() throws Exception {
		System.out.println("Test " + testCounter + "...");
		testCounter++;
		processor = new ImportDataProcessor();
		engine = new BigDataEngine();
		
		// Set the Sudo-Prop (Created in Prerna.ui.main.Starter.java)
		System.setProperty("file.separator", "/");
		String workingDir = System.getProperty("user.dir");
		String propFile = workingDir + "/RDF_Map.prop";
		DIHelper.getInstance().loadCoreProp(propFile);
		PropertyConfigurator.configure(workingDir + "/log4j.prop");
							
		//Set BaseDirectory
		processor.setBaseDirectory(semossDirectory);
	}

	@After
	public void tearDown() throws Exception {
		//After tests are run
		clean();
	}
	
	@AfterClass
	public static void finalTearDown() throws Exception {
		//After all tests are run
		clean();
		
	}

	//CSV DB created
	@Test
	public void Test_CreateNew_CSV() throws EngineException, FileReaderException, HeaderClassException, FileWriterException, NLPException, NullPointerException{
		prerna.ui.components.ImportDataProcessor.IMPORT_METHOD testMethod = IMPORT_METHOD.CREATE_NEW;
		prerna.ui.components.ImportDataProcessor.IMPORT_TYPE testType = IMPORT_TYPE.CSV;
		prerna.ui.components.ImportDataProcessor.DB_TYPE dbType = DB_TYPE.RDF;
		
		//Run Processor
		processor.runProcessor(testMethod, testType, fileNameCSV, customBaseURI, newDBnameCSV, mapFileCSV, "", "", "", dbType);
		System.out.println("	CSV Db proccesor ran successfully. CSV DB Created.");
		
		//TESTING ASSERTIONS
		//Files Created and in the right place
		File f = new File(dbDirectory+newDBnameCSV);
		assertTrue("DB Folder exists.", f.exists());
		f = new File(dbDirectory+newDBnameCSV+"\\"+newDBnameCSV+".jnl");
		assertTrue("DB .jnl exists.", f.exists());
		f = new File(dbDirectory+newDBnameCSV+"\\"+newDBnameCSV+"_Custom_Map.prop");
		assertTrue("DB Custom_Map.prop exists.", f.exists());
		f = new File(dbDirectory+newDBnameCSV+"\\"+newDBnameCSV+"_OWL.OWL");
		assertTrue("DB .OWL exists.", f.exists());
		f = new File(dbDirectory+newDBnameCSV+"\\"+newDBnameCSV+"_Questions.XML");
		assertTrue("DB Questions.xml exists.", f.exists());
		f = new File(dbDirectory+"\\"+newDBnameCSV+".smss");
		assertTrue("DB smss exists.", f.exists());
		System.out.println("	All Files Exist.");
		
		//Setup for Header Tests(asserts) & Querys
		BigDataEngine engine = loadEngine(CSVsmss);
		engine.commitOWL();
		
		//Checks All Generic Queries
		Vector<String> perspec = engine.getPerspectives();
		String currentPerspec = perspec.get(0); //get only first Perspective
		Vector<String> insights = engine.getInsights(currentPerspec, engine.getEngineName());
		for(int i = 0; i < insights.size(); i++){
			String query = engine.getInsight(insights.get(i)).getSparql();
			query = prepQuery(query);
			String queryHome = "Engine: "+engine.getEngineName()+", Perspective: " + currentPerspec + ", Insight: "+insights.get(i);
			try {
				query = prepQuery(query); //Prep the Query for engine use
				assertTrue(queryHome +": is broken...", checkQuery(engine, query));
			} catch (MalformedQueryException e) {
				assertTrue(queryHome +": is MALFORMED...", false);
				e.printStackTrace();
			} catch (NullPointerException e) {
				assertTrue(queryHome +": is NULL...", false);
				e.printStackTrace();
			}
		}
		
		System.out.println(" Information is correct");
		
		//Tear down
		engine.commitOWL();
		engine.commit();
		engine.closeDB();
	}
	
	//EXCEL DB created
	@Test
	public void Test_CreateNew_EXCEL() throws EngineException, FileReaderException, HeaderClassException, FileWriterException, NLPException{
		prerna.ui.components.ImportDataProcessor.IMPORT_METHOD testMethod = IMPORT_METHOD.CREATE_NEW;
		prerna.ui.components.ImportDataProcessor.IMPORT_TYPE testType = IMPORT_TYPE.EXCEL;	
		prerna.ui.components.ImportDataProcessor.DB_TYPE dbType = DB_TYPE.RDF;
		
		//Run Processor
		processor.runProcessor(testMethod, testType, fileNameEXCEL, customBaseURI , newDBnameEXCEL, mapFileEXCEL, "", "", "", dbType);
		System.out.println("	EXCEL Db proccesor ran successfully. Excel DB created.");
		
		//TESTING ASSERTIONS
		//Files Created and in the right place
		File f = new File(dbDirectory+newDBnameEXCEL);
		assertTrue("DB Folder exists.", f.exists());
		/*DBUG*/System.out.println(".jnl: "+ "\\" + dbDirectory+"\\"+newDBnameEXCEL+"\\\\"+newDBnameEXCEL+".jnl");
		f = new File(dbDirectory+newDBnameEXCEL+"\\"+newDBnameEXCEL+".jnl");
		assertTrue("DB .jnl exists.", f.exists());
		f = new File(dbDirectory+newDBnameEXCEL+"\\"+newDBnameEXCEL+"_Custom_Map.prop");
		assertTrue("DB Custom_Map.prop exists.", f.exists());
		f = new File(dbDirectory+newDBnameEXCEL+"\\"+newDBnameEXCEL+"_OWL.OWL");
		assertTrue("DB .OWL exists.", f.exists());
		f = new File(dbDirectory+newDBnameEXCEL+"\\"+newDBnameEXCEL+"_Questions.XML");
		assertTrue("DB Questions.xml exists.", f.exists());
		f = new File(dbDirectory+newDBnameEXCEL+".smss");
		assertTrue("DB smss exists.", f.exists());
		System.out.println("	All Files Exist.");
				
		//Setup for Header Tests(asserts) & Querys
		BigDataEngine engine = loadEngine(EXCELsmss);
		engine.commitOWL();
				
		//Checks All Generic Queries
		Vector<String> perspec = engine.getPerspectives();
		String currentPerspec = perspec.get(0); //get only first Perspective
		Vector<String> insights = engine.getInsights(currentPerspec, engine.getEngineName());
		for(int i = 0; i < insights.size(); i++){
			String query = engine.getInsight(insights.get(i)).getSparql();
			query = prepQuery(query);
			String queryHome = "Engine: "+engine.getEngineName()+", Perspective: " + currentPerspec + ", Insight: "+insights.get(i);
			try {
				query = prepQuery(query); //Prep the Query for engine use
				assertTrue(queryHome +": is BROKEN...", checkQuery(engine, query));
			} catch (MalformedQueryException e) {
				assertTrue(queryHome +": is MALFORMED...", false);
				e.printStackTrace();
			} catch (NullPointerException e) {
				assertTrue(queryHome +": is NULL...", false);
				e.printStackTrace();
			}
		}

		//Tear down
		engine.commitOWL();
		engine.commit();
		engine.closeDB();
	}
	
	//CSV add to existing
	@Test
	public void Test_AddToExisting_CSV() throws EngineException, FileReaderException, HeaderClassException, FileWriterException, NLPException, IOException, MalformedQueryException, NullPointerException{
		//Make CSV
		makeCSV();
		
		prerna.ui.components.ImportDataProcessor.IMPORT_METHOD testMethod = IMPORT_METHOD.ADD_TO_EXISTING;
		prerna.ui.components.ImportDataProcessor.IMPORT_TYPE testType = IMPORT_TYPE.CSV;
		prerna.ui.components.ImportDataProcessor.DB_TYPE dbType = DB_TYPE.RDF;
		
		//TESTING ASSERTIONS
		//Files Created and in the right place
		File f = new File(dbDirectory+newDBnameCSV);
		assertTrue("DB Folder exists.", f.exists());
		f = new File(dbDirectory+newDBnameCSV+"\\"+newDBnameCSV+".jnl");
		assertTrue("DB .jnl exists.", f.exists());
		f = new File(dbDirectory+newDBnameCSV+"\\"+newDBnameCSV+"_Custom_Map.prop");
		assertTrue("DB Custom_Map.prop exists.", f.exists());
		f = new File(dbDirectory+newDBnameCSV+"\\"+newDBnameCSV+"_OWL.OWL");
		assertTrue("DB .OWL exists.", f.exists());
		f = new File(dbDirectory+newDBnameCSV+"\\"+newDBnameCSV+"_Questions.XML");
		assertTrue("DB Questions.xml exists.", f.exists());
		f = new File(dbDirectory+"\\"+newDBnameCSV+".smss");
		assertTrue("DB smss exists.", f.exists());
		System.out.println("	All Files Exist.");
		
		//ADD TO EXISTING
		//Reset Prop
		BigDataEngine engine = loadEngine(CSVsmss);

		//Run Processor
		processor.runProcessor(testMethod, testType, replacementCSV, customBaseURI, "", "", "", "", newDBnameCSV, dbType);
		System.out.println("CSV Db proccesor ran successfully. CSV DB Altered.");
		engine.commitOWL();
		engine.commit();
		engine.closeDB();

		//Setup for Header Tests(asserts) & Querys
		engine = loadEngine(CSVsmss);
				
		//Checks All Generic Queries
		Vector<String> perspec = engine.getPerspectives();
		String currentPerspec = perspec.get(0); //get only first Perspective
		Vector<String> insights = engine.getInsights(currentPerspec, engine.getEngineName());
		for(int i = 0; i < insights.size(); i++){
			String query = engine.getInsight(insights.get(i)).getSparql();
			query = prepQuery(query);
			String queryHome = "Engine: "+engine.getEngineName()+", Perspective: " + currentPerspec + ", Insight: "+insights.get(i);
			try {
				query = prepQuery(query); //Prep the Query for engine use
				assertTrue(queryHome +": is BROKEN...", checkQuery(engine, query));
			} catch (MalformedQueryException e) {
				assertTrue(queryHome +": is MALFORMED...", false);
				e.printStackTrace();
			} catch (NullPointerException e) {
				assertTrue(queryHome +": is NULL...", false);
				e.printStackTrace();
			}
		}
		
		//Tear down
		engine.commit();
		engine.closeDB();
	}

	//EXCEL add to existing
	@Test
	public void Test_AddToExisting_EXCEL() throws EngineException, FileReaderException, HeaderClassException, FileWriterException, NLPException, MalformedQueryException, NullPointerException{
		//Make Test Excel
		makeExcel();
		
		prerna.ui.components.ImportDataProcessor.IMPORT_METHOD testMethod = IMPORT_METHOD.ADD_TO_EXISTING;
		prerna.ui.components.ImportDataProcessor.IMPORT_TYPE testType = IMPORT_TYPE.EXCEL;
		prerna.ui.components.ImportDataProcessor.DB_TYPE dbType = DB_TYPE.RDF;
		
		//TESTING ASSERTIONS
		//Files Created and in the right place
		File f = new File(dbDirectory+newDBnameEXCEL);
		assertTrue("DB Folder doesn't exists.", f.exists());
		f = new File(dbDirectory+newDBnameEXCEL+"\\"+newDBnameEXCEL+".jnl");
		assertTrue("DB .jnl doesn't exists.", f.exists());
		f = new File(dbDirectory+newDBnameEXCEL+"\\"+newDBnameEXCEL+"_Custom_Map.prop");
		assertTrue("DB Custom_Map.prop doesn't exists.", f.exists());
		f = new File(dbDirectory+newDBnameEXCEL+"\\"+newDBnameEXCEL+"_OWL.OWL");
		assertTrue("DB .OWL doesn't exists.", f.exists());
		f = new File(dbDirectory+newDBnameEXCEL+"\\"+newDBnameEXCEL+"_Questions.XML");
		assertTrue("DB Questions.xml doesn't exists.", f.exists());
		f = new File(dbDirectory+newDBnameEXCEL+".smss");
		assertTrue("DB smss doesn't exists.", f.exists());
		System.out.println("	All Files Exist.");
			
		//Reset Prop
		BigDataEngine engine = loadEngine(EXCELsmss);

		//Run Processor
		processor.runProcessor(testMethod, testType, replacementExcel, customBaseURI , "", "", "", "", newDBnameEXCEL, dbType);
		System.out.println("	EXCEL Db proccesor ran successfully. Excel DB created.");
		engine.commitOWL();
		engine.commit();
		engine.closeDB();

		//Setup for Header Tests(asserts) & Querys
		engine = loadEngine(EXCELsmss);
				
		//Checks All Generic Queries
		Vector<String> perspec = engine.getPerspectives();
		String currentPerspec = perspec.get(0); //get only first Perspective
		Vector<String> insights = engine.getInsights(currentPerspec, engine.getEngineName());
		for(int i = 0; i < insights.size(); i++){
			String query = engine.getInsight(insights.get(i)).getSparql();
			query = prepQuery(query);
			String queryHome = "Engine: "+engine.getEngineName()+", Perspective: " + currentPerspec + ", Insight: "+insights.get(i);
			try {
				query = prepQuery(query); //Prep the Query for engine use
				assertTrue(queryHome +": is BROKEN...", checkQuery(engine, query));
			} catch (MalformedQueryException e) {
				assertTrue(queryHome +": is MALFORMED...", false);
				e.printStackTrace();
			} catch (NullPointerException e) {
				assertTrue(queryHome +": is NULL...", false);
				e.printStackTrace();
			}
		}

		//Tear down
		engine.commitOWL();
		engine.commit();
		engine.closeDB();
	}
	
	//CSV Override
	@Ignore
	@Test
	public void Test_OVERRIDE_CSV() throws EngineException, FileReaderException, HeaderClassException, FileWriterException, NLPException{
		makeCSV();
		
		prerna.ui.components.ImportDataProcessor.IMPORT_METHOD testMethod = IMPORT_METHOD.OVERRIDE;
		prerna.ui.components.ImportDataProcessor.IMPORT_TYPE testType = IMPORT_TYPE.CSV;
		prerna.ui.components.ImportDataProcessor.DB_TYPE dbType = DB_TYPE.RDF;
		
		//TESTING ASSERTIONS
		//Files Created and in the right place
		File f = new File(dbDirectory+newDBnameCSV);
		assertTrue("DB Folder exists.", f.exists());
		f = new File(dbDirectory+newDBnameCSV+"\\"+newDBnameCSV+".jnl");
		assertTrue("DB .jnl exists.", f.exists());
		f = new File(dbDirectory+newDBnameCSV+"\\"+newDBnameCSV+"_Custom_Map.prop");
		assertTrue("DB Custom_Map.prop exists.", f.exists());
		f = new File(dbDirectory+newDBnameCSV+"\\"+newDBnameCSV+"_OWL.OWL");
		assertTrue("DB .OWL exists.", f.exists());
		f = new File(dbDirectory+newDBnameCSV+"\\"+newDBnameCSV+"_Questions.XML");
		assertTrue("DB Questions.xml exists.", f.exists());
		f = new File(dbDirectory+"\\"+newDBnameCSV+".smss");
		assertTrue("DB smss exists.", f.exists());
		System.out.println("	All Files Exist.");
		
		//OVERRIDE
		IEngine engine = new BigDataEngine();

		FileInputStream fileIn = null;
		Properties prop = new Properties();
		try {
			fileIn = new FileInputStream(CSVsmss);
			prop.load(fileIn);
			System.out.println("prop:     "+prop);
			engine = Utility.loadEngine(CSVsmss, prop);
			fileIn.close();
			//engine.closeDB();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		//Run Processor
		processor.runProcessor(testMethod, testType, replacementCSV, customBaseURI, "", "", "", "", newDBnameCSV, dbType);
		System.out.println("CSV Db proccesor ran successfully. CSV DB Replaced.");
	}
		
	//EXCEL Override
	@Ignore
	@Test
	public void Test_OVERRIDE_EXCEL() throws EngineException, FileReaderException, HeaderClassException, FileWriterException, NLPException{
		makeExcel();
		
		prerna.ui.components.ImportDataProcessor.IMPORT_METHOD testMethod = IMPORT_METHOD.OVERRIDE;
		prerna.ui.components.ImportDataProcessor.IMPORT_TYPE testType = IMPORT_TYPE.EXCEL;
		prerna.ui.components.ImportDataProcessor.DB_TYPE dbType = DB_TYPE.RDF;
			
		String SudoSmss = dbDirectory+newDBnameEXCEL+".smss";
		
		//TESTING ASSERTIONS
		//Files Created and in the right place
		File f = new File(dbDirectory+newDBnameEXCEL);
		assertTrue("DB Folder exists.", f.exists());
		f = new File(dbDirectory+newDBnameEXCEL+"\\"+newDBnameEXCEL+".jnl");
		assertTrue("DB .jnl exists.", f.exists());
		f = new File(dbDirectory+newDBnameEXCEL+"\\"+newDBnameEXCEL+"_Custom_Map.prop");
		assertTrue("DB Custom_Map.prop exists.", f.exists());
		f = new File(dbDirectory+newDBnameEXCEL+"\\"+newDBnameEXCEL+"_OWL.OWL");
		assertTrue("DB .OWL exists.", f.exists());
		f = new File(dbDirectory+newDBnameEXCEL+"\\"+newDBnameEXCEL+"_Questions.XML");
		assertTrue("DB Questions.xml exists.", f.exists());
		f = new File(dbDirectory+newDBnameEXCEL+".smss");
		assertTrue("DB smss exists.", f.exists());
		System.out.println("	All Files Exist.");
			
		//Reset Prop
		IEngine engine = new BigDataEngine();

		FileInputStream fileIn = null;
		Properties prop = new Properties();
		try {
			fileIn = new FileInputStream(SudoSmss);
			prop.load(fileIn);
			System.out.println("prop:     "+prop);
			engine = Utility.loadEngine(SudoSmss, prop);
			fileIn.close();
			//engine.closeDB();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		
		//Run Processor
		processor.runProcessor(testMethod, testType, replacementExcel, customBaseURI , "", "", "", "", newDBnameEXCEL, dbType);
		System.out.println("	EXCEL Db proccesor ran successfully. Excel DB replaced.");
		engine.closeDB();
	}
	
	/* This is the tools section for this Class 
	 * makeExcel
	 * makeCSV
	 * checkQuery
	 * prepQuery
	 * loadEngine
	 */
	private void makeExcel() throws EngineException, FileReaderException, HeaderClassException, FileWriterException, NLPException{
		prerna.ui.components.ImportDataProcessor.IMPORT_METHOD testMethod = IMPORT_METHOD.CREATE_NEW;
		prerna.ui.components.ImportDataProcessor.IMPORT_TYPE testType = IMPORT_TYPE.EXCEL;	
		prerna.ui.components.ImportDataProcessor.DB_TYPE dbType = DB_TYPE.RDF;
		
		//Run Processor
		processor.runProcessor(testMethod, testType, fileNameEXCEL, customBaseURI , newDBnameEXCEL, mapFileEXCEL, "", "", "", dbType);
		System.out.println("	EXCEL Db proccesor ran successfully. Excel DB created.");
	}
	
	private void makeCSV() throws EngineException, FileReaderException, HeaderClassException, FileWriterException, NLPException{
		prerna.ui.components.ImportDataProcessor.IMPORT_METHOD testMethod = IMPORT_METHOD.CREATE_NEW;
		prerna.ui.components.ImportDataProcessor.IMPORT_TYPE testType = IMPORT_TYPE.CSV;
		prerna.ui.components.ImportDataProcessor.DB_TYPE dbType = DB_TYPE.RDF;
	
		//Run Processor
		processor.runProcessor(testMethod, testType, fileNameCSV, customBaseURI, newDBnameCSV, mapFileCSV, "", "", "", dbType);
		System.out.println("	CSV Db proccesor ran successfully. CSV DB Created.");
	}
	
	private boolean checkQuery(BigDataEngine engine, String query) throws MalformedQueryException{
		/*DBUG*/System.out.println("CHECKING "+engine.getEngineName());

		Boolean result = false;
		Object check = engine.execQuery(query);
		
		if(check instanceof Boolean){
			//DO NOTHING
		} else {
			//Limit the results
			if(query.contains("BINDINGS")){
				query = query.replace("BINDINGS", "LIMIT 3 BINDINGS");
			}
			if(query.contains("LIMIT") || query.contains("limit")){
				//DO Nothing
			} else {query += " LIMIT 3";}
		}
		
		System.out.println(query);
		
		if(check instanceof GraphQueryResult){
			try {
				GraphQueryResult res = (GraphQueryResult) check;
				if(res != null){result = true;}
				res.close();
			} catch (NullPointerException | QueryEvaluationException e){
				//Nothing
			}
		} else if(check instanceof Boolean) {
			try {
				result = (Boolean) check;
			} catch (NullPointerException e){
				//Nothing
			}
		} else if(check instanceof TupleQueryResult){
			try {
				TupleQueryResult res = (TupleQueryResult) check;
				if(res != null){result = true;}
				res.close();
			} catch (NullPointerException | QueryEvaluationException e){
				//Nothing
			}
		} else {
			/*DBUG*/System.out.println("NOT A THING!");
			result = true;
		}
		System.out.println("...");
		return result;
	}

	private String prepQuery(String query){
		//Clustering; Remove all clustering info so query can be processed
		if(query.contains("+++")){
			String[] clusterRemoval= {
					"+++@NumberOfClusters-OverrideCluster@",
					"+++J48",
					"+++PART",
					"+++DecisionTable", 
					"+++DecisionStump",
					"+++REPTree", 
					"+++LMT", 
					"+++SimpleLogistic", 
					"+++@KNeighbors-K@"}; //Add new Cluster param types here
			for(int i = 0; i < clusterRemoval.length; i++){
				if(query.contains(clusterRemoval[i])){
					query = query.replace(clusterRemoval[i], "");
					System.out.println("Part Removed: "+clusterRemoval[i]);
				}
			}
		}
		
		//Dates; removed them, they are dealt with in the play-sheet and not the engine
		if(query.contains("CONSTRUCT") && query.contains("-Override")){
			String[] dateRemoval = {
					"@Year-OverrideYear@;", 
					"@Month-OverrideMonth@;"
					//Add more here
					};
			for(int i = 0; i < dateRemoval.length; i++){
				if(query.contains(dateRemoval[i])){
					query = query.replace(dateRemoval[i], "");
					System.out.println("Part Removed: "+dateRemoval[i]);
				}
			}
		}
		
		//Params; find and insert query params normally put in by the user
		if(query.contains("@")){
			query = Utility.fillParam(query, Utility.getParams(query));
		}
		return query.trim();
	}

	private BigDataEngine loadEngine(String engineLocation){
		BigDataEngine engine = new BigDataEngine();
		FileInputStream fileIn;
		Properties prop = new Properties();
		try {
			fileIn = new FileInputStream(engineLocation);
			prop.load(fileIn);
			//SEP
			try {
				String engines = DIHelper.getInstance().getLocalProp(Constants.ENGINES) + "";

				String engineName = prop.getProperty(Constants.ENGINE);
				String engineClass = prop.getProperty(Constants.ENGINE_TYPE);
				//TEMPORARY
				// TODO: remove this
				if(engineClass.equals("prerna.rdf.engine.impl.RDBMSNativeEngine")){
					engineClass = "prerna.engine.impl.rdbms.RDBMSNativeEngine";
				}
				else if(engineClass.startsWith("prerna.rdf.engine.impl.")){
					engineClass = engineClass.replace("prerna.rdf.engine.impl.", "prerna.engine.impl.rdf.");
				}
				engine = (BigDataEngine)Class.forName(engineClass).newInstance();
				engine.setEngineName(engineName);
				if(prop.getProperty("MAP") != null) {
					engine.addProperty("MAP", prop.getProperty("MAP"));
				}
				engine.openDB(engineLocation);
				engine.setDreamer(prop.getProperty(Constants.DREAMER));
				engine.setOntology(prop.getProperty(Constants.ONTOLOGY));
				
				// set the core prop
				if(prop.containsKey(Constants.DREAMER))
					DIHelper.getInstance().getCoreProp().setProperty(engineName + "_" + Constants.DREAMER, prop.getProperty(Constants.DREAMER));
				if(prop.containsKey(Constants.ONTOLOGY))
					DIHelper.getInstance().getCoreProp().setProperty(engineName + "_" + Constants.ONTOLOGY, prop.getProperty(Constants.ONTOLOGY));
				if(prop.containsKey(Constants.OWL)) {
					DIHelper.getInstance().getCoreProp().setProperty(engineName + "_" + Constants.OWL, prop.getProperty(Constants.OWL));
					engine.setOWL(prop.getProperty(Constants.OWL));
				}
				
				// set the engine finally
				engines = engines + ";" + engineName;
				DIHelper.getInstance().setLocalProperty(engineName, engine);
				DIHelper.getInstance().setLocalProperty(Constants.ENGINES, engines);
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			//SEP
			fileIn.close();
			prop.clear();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return engine;
	}
	
	private static void clean(){
		//DeleteMe
				//Delete Source Folder
				File file = new File(dbDirectory+newDBname);
				processor.deleteFile(file);
				//Delete .temp
				file = new File(dbDirectory+newDBname+".temp");
				processor.deleteFile(file);
				//Delete .SMSS
				file = new File(dbDirectory+newDBname+".smss");
				processor.deleteFile(file);
						
				//CSV
				//Delete Source Folder
				file = new File(dbDirectory+newDBnameCSV);
				processor.deleteFile(file);
				//Delete .temp
				file = new File(dbDirectory+newDBnameCSV+".temp");
				processor.deleteFile(file);
				//Delete .SMSS
				file = new File(CSVsmss);
				processor.deleteFile(file);
						
				//Excel
				//Delete Source Folder
				file = new File(dbDirectory+newDBnameEXCEL);
				processor.deleteFile(file);
				//Delete .temp
				file = new File(dbDirectory+newDBnameEXCEL+".temp");
				processor.deleteFile(file);
				//Delete .SMSS
				file = new File(EXCELsmss);
				processor.deleteFile(file);
	}
}

