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
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.BigDataEngine;
import prerna.rdf.engine.impl.SesameJenaSelectCheater;
import prerna.ui.components.ImportDataProcessor;
import prerna.ui.components.ImportDataProcessor.DB_TYPE;
import prerna.ui.components.ImportDataProcessor.IMPORT_METHOD;
import prerna.ui.components.ImportDataProcessor.IMPORT_TYPE;
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
* @version 1.0
* @since   03-23-2015 
* Questions? Email abender@deloitte.com
*/
public class ImportDataProcessorTest {

	//Core obj
	private static ImportDataProcessor processor;
	private SesameJenaSelectCheater selector;
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
		
	}

	@Before
	public void setUp() throws Exception {
		System.out.println("Test "+testCounter+":Started");
		processor = new ImportDataProcessor();
		selector = new SesameJenaSelectCheater();
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
		//After all tests are run
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
	
	@AfterClass
	public static void finalTearDown() throws Exception {
		//After all tests are run
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

	//CSV DB created
	@Test
	public void Test_CreateNew_CSV() throws EngineException, FileReaderException, HeaderClassException, FileWriterException, NLPException{
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
		IEngine engine = new BigDataEngine();
		selector.setEngine(engine);
		engine.openDB(CSVsmss);
		String query = "SELECT DISTINCT ?firstName ?lastName ?lastName__web WHERE { BIND(<http://semoss.org/ontologies/Concept/lastName/Agramonte> AS ?lastName) "
				+ "{?firstName <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/firstName>} "
				+ "{?lastName <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/lastName>} "
				+ "{?firstName <http://semoss.org/ontologies/Relation/has_first_name> ?lastName}"
				+ " {?lastName <http://semoss.org/ontologies/Relation/Contains/web> ?lastName__web}  }";
		selector.setQuery(query);
		selector.execute();
		System.out.println("	Query works.");
		
		//Checks Headers
		String[] var = selector.getVariables();
		assertEquals("CSV DB Headers are correct.",var[0],"firstName");
		assertEquals("CSV DB Headers correct.",var[1],"lastName");
		assertEquals("CSV DB Property Headers correct.",var[2],"lastName__web");
		System.out.println("	Headers are correct.");
		
		//Checks the information is correct
		String askQuery = "ASK {<http://theTest/Concept/firstName/Jenelle> <http://www.w3.org/2000/01/rdf-schema#label> 'Jenelle'}";
		assertTrue("CSV DB Var are correct.",engine.execAskQuery(askQuery));
		askQuery = "ASK {<http://theTest/Concept/firstName/Jennie> <http://semoss.org/ontologies/Relation> <http://theTest/Concept/lastName/Drymon>}";
		assertTrue("CSV DB Relationships correct.",engine.execAskQuery(askQuery));
		askQuery = "ASK {<http://theTest/Concept/lastName/Cetta> <http://semoss.org/ontologies/Relation/Contains/phone2> '808-475-2310'}";
		assertTrue("CSV DB Properties correct.",engine.execAskQuery(askQuery));
		System.out.println("	Information is correct");
		
		//Tear down
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
				
		//Information is correct
		BigDataEngine engine = new BigDataEngine();	
		selector.setEngine(engine);
		engine.openDB(EXCELsmss);
		String query = "SELECT DISTINCT ?Activity ?Level ?Activity__Number WHERE "
				+ "{ {?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>} "
				+ "{?Level <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Level>}"
				+ " {?Activity <http://semoss.org/ontologies/Relation/Has> ?Level}"
				+ " {?Activity <http://semoss.org/ontologies/Relation/Contains/Number> ?Activity__Number}  }";
		selector.setQuery(query);
		selector.execute();
		System.out.println("	Query works.");

		//Checks Headers
		String[] var = selector.getVariables();
		assertEquals("CSV DB Headers are correct.",var[0],"Activity");
		assertEquals("CSV DB Headers correct.",var[1],"Level");
		assertEquals("CSV DB Property Headers correct.",var[2],"Activity__Number");
		System.out.println("	Headers are correct.");
		
		//Checks the information is correct
		//Var are correct
		String askQuery = "ASK {<http://theTest/Concept/Level/Level> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Level>}";
		assertTrue("Excel DB Var are correct.",engine.execAskQuery(askQuery));
		//Properties
		askQuery = "ASK {<http://theTest/Concept/Activity/Assign_Patient_to_Care_Provider> <http://semoss.org/ontologies/Relation> <http://theTest/Concept/Level/Level_2>}";
		assertTrue("Excel DB Relationships correct..",engine.execAskQuery(askQuery));
		//Relationships
		askQuery = "ASK {<http://theTest/Concept/Activity/Capture_Data_and_Documentation_from_External_Sources> <http://semoss.org/ontologies/Relation/Contains/Number> '4.11.5'}";
		assertTrue("Excel DB Properties correct.",engine.execAskQuery(askQuery));
		System.out.println("	Information is correct");
				
		//Tear down
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
		engine.closeDB();

		//Information is correct
		engine = loadEngine(CSVsmss);
		Vector<String> perspec = engine.getPerspectives();
		String currentPerspec = perspec.get(0);
		@SuppressWarnings("unchecked")
		Vector<String> insights = engine.getInsights(currentPerspec, engine.getEngineName());
		String currentInsight = insights.get(1);
		Insight test = engine.getInsight(currentInsight);
		String query = test.getSparql();
		query = prepQuery(query);
		String queryHome = "Engine: "+engine.getEngineName()+", Perspective: " + currentPerspec + ", Insight: "+currentInsight;
				
		//Checks the information is correct
		assertTrue(queryHome +": is broken...",checkQuery(engine, query));
		System.out.println("	Information is correct");
				
		//Tear down
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
		BigDataEngine engine = loadEngine(EXCELsmss);

		//Run Processor
		processor.runProcessor(testMethod, testType, replacementExcel, customBaseURI , "", "", "", "", newDBnameEXCEL, dbType);
		System.out.println("	EXCEL Db proccesor ran successfully. Excel DB created.");
		engine.closeDB();
		System.out.println("Connected: "+engine.isConnected());
		
		//TESTING ASSERTIONS
		//Files Created and in the right place
		f = new File(dbDirectory+newDBnameEXCEL);
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
				
		//Information is correct
		engine = loadEngine(EXCELsmss);
		Vector<String> perspec = engine.getPerspectives();
		String currentPerspec = perspec.get(0);
		@SuppressWarnings("unchecked")
		Vector<String> insights = engine.getInsights(currentPerspec, engine.getEngineName());
		String currentInsight = insights.get(0);
		Insight test = engine.getInsight(currentInsight);
		String query = test.getSparql();
		query = prepQuery(query);
		String queryHome = "Engine: "+engine.getEngineName()+", Perspective: " + currentPerspec + ", Insight: "+currentInsight;
		
		//Checks the information is correct
		assertTrue(queryHome +": is broken...",checkQuery(engine, query));
		System.out.println("	Information is correct");
		
		//Tear down
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
	public void makeExcel() throws EngineException, FileReaderException, HeaderClassException, FileWriterException, NLPException{
		prerna.ui.components.ImportDataProcessor.IMPORT_METHOD testMethod = IMPORT_METHOD.CREATE_NEW;
		prerna.ui.components.ImportDataProcessor.IMPORT_TYPE testType = IMPORT_TYPE.EXCEL;	
		prerna.ui.components.ImportDataProcessor.DB_TYPE dbType = DB_TYPE.RDF;
		
		//Run Processor
		processor.runProcessor(testMethod, testType, fileNameEXCEL, customBaseURI , newDBnameEXCEL, mapFileEXCEL, "", "", "", dbType);
		System.out.println("	EXCEL Db proccesor ran successfully. Excel DB created.");
	}
	
	public void makeCSV() throws EngineException, FileReaderException, HeaderClassException, FileWriterException, NLPException{
		prerna.ui.components.ImportDataProcessor.IMPORT_METHOD testMethod = IMPORT_METHOD.CREATE_NEW;
		prerna.ui.components.ImportDataProcessor.IMPORT_TYPE testType = IMPORT_TYPE.CSV;
		prerna.ui.components.ImportDataProcessor.DB_TYPE dbType = DB_TYPE.RDF;
	
		//Run Processor
		processor.runProcessor(testMethod, testType, fileNameCSV, customBaseURI, newDBnameCSV, mapFileCSV, "", "", "", dbType);
		System.out.println("	CSV Db proccesor ran successfully. CSV DB Created.");
	}
	
	public boolean checkQuery(BigDataEngine engine, String query) throws MalformedQueryException, NullPointerException{
		//Limit the results
		if(query.contains("BINDINGS")){
			query = query.replace("BINDINGS", "LIMIT 3 BINDINGS");
		}
		if(query.contains("LIMIT") || query.contains("limit")){} 
		else {query += " LIMIT 3";}
		
		//get Check
		Boolean result = false;
		System.out.println("Query: "+query);
		
		if(query.contains("CONSTRUCT")){
			GraphQueryResult res  = engine.execGraphQuery(query);
			if(res.toString() != ""){result = true;}
			try {
				res.close();	
			} catch( QueryEvaluationException e) {
				e.printStackTrace();
				System.out.println("Data remains in Cashe...");
			}
		} 
		else if(query.contains("ASK")) {
			result = engine.execAskQuery(query);
		} 
		else if (query.contains("SELECT")){
			TupleQueryResult res = engine.execSelectQuery(query);
			if(res.toString() != ""){result = true;}
			try {
				res.close();	
			} catch( QueryEvaluationException e) {
				e.printStackTrace();
				System.out.println("Data remains in Cashe...");
			}
		}
		System.out.println("");
		return result;
	}
	
	private String prepQuery(String query){
		//Clustering; Remove all clustering info so query can be processed
		String[] clusterRemoval= {"+++@NumberOfClusters-OverrideCluster@","+++J48","+++PART", "+++DecisionTable", "+++DecisionStump"
								,"+++REPTree", "+++LMT", "+++SimpleLogistic", "+++@KNeighbors-K@"}; //Add new Cluster param types here
		if(query.contains("+++")){
			for(int i = 0; i < clusterRemoval.length; i++){
				if(query.contains(clusterRemoval[i])){
					query = query.replace(clusterRemoval[i], "");
					System.out.println("Part Removed: "+clusterRemoval[i]);
				}
			}
		}
		
		//Dates; removed them, they are dealt with in the play-sheet and not the engine
		String[] dateRemoval = {"@Year-OverrideYear@;", "@Month-OverrideMonth@;"};//Add additional time and date params here
		if(query.contains("CONSTRUCT") && query.contains("-Override")){
			for(int i = 0; i < dateRemoval.length; i++){
				if(query.contains(dateRemoval[i])){
					query = query.replace(dateRemoval[i], "");
					System.out.println("Part Removed: "+dateRemoval[i]);
				}
			}
		}
		
		//Params; find and insert query params normally put in by the user
		if(query.contains("BIND") || query.contains("DISTINCT")){
			Hashtable paramHash = Utility.getParams(query);
			Utility.fillParam(query, paramHash);
			
		}
		return query;
	}

	private BigDataEngine loadEngine(String engineLocation){
		BigDataEngine engine = new BigDataEngine();
		FileInputStream fileIn = null;
		Properties prop = new Properties();
		try {
			fileIn = new FileInputStream(engineLocation);
			prop.load(fileIn);
			engine = (BigDataEngine) Utility.loadEngine(engineLocation, prop);
			fileIn.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return engine;
	}
	}

