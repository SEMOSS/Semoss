//package prerna.test;
//
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.FileNotFoundException;
//import java.io.IOException;
//import java.util.HashSet;
//import java.util.Properties;
//import java.util.Set;
//import java.util.Vector;
//
//import org.apache.log4j.PropertyConfigurator;
//import org.junit.After;
//import org.junit.AfterClass;
//import org.junit.Before;
//import org.junit.BeforeClass;
//import org.junit.Ignore;
//import org.junit.Test;
//
//import junit.framework.TestCase;
//import prerna.engine.api.IEngine;
//import prerna.engine.impl.rdf.BigDataEngine;
//import prerna.om.Insight;
//import prerna.ui.components.ImportDataProcessor;
//import prerna.ui.components.ImportDataProcessor.DB_TYPE;
//import prerna.ui.components.ImportDataProcessor.IMPORT_METHOD;
//import prerna.ui.components.ImportDataProcessor.IMPORT_TYPE;
//import prerna.util.Constants;
//import prerna.util.DIHelper;
//import prerna.util.Utility;
//
///**
// * ImportDataProcessorTest checks the 3 main Components of the ImportDataProcessor class
// * 1) Creating a new Database
// * 2) Adding to and existing Database
// * 3) Overriding/Replacing an existing Database
// * There are two tests for each one: CSV and Excel
// *
// * @author  August Bender
// * @version 1.1
// * @since   05-26-2015 
// * Questions? Email abender@deloitte.com
// */
//public class ImportDataProcessorTest extends TestCase {
//
//	//Core obj
//	private ImportDataProcessor processor;
//	@SuppressWarnings("unused")
//	private BigDataEngine engine;
//
//	private String semossDirectory;
//	private String dbDirectory;
//	private String customBaseURI = "http://theTest";
//
//	//CSV Var
//	private String newDBnameCSV = "OneData";
//	private String fileNameCSV;
//	private String mapFileCSV;
//	private String dbPropFileCSV;
//	private String CSVsmss; 
//	//CSV Replace 
//	private String repoCSV = newDBnameCSV;
//	private String replacementCSV;
//	private String replacementMapFileCSV;
//	private String replacementdbPropFileCSV;
//
//
//	//EXCEL Var
//	private String newDBnameEXCEL = "ExcelTest";
//	private String fileNameEXCEL;
//	private String mapFileEXCEL; 
//	private String dbPropFileEXCEL;
//	private String EXCELsmss;
//	//Excel Replace
//	private String replacementExcel;
//
//	//NLP Var
//	private String fileNameNLP;
//
//	//OCR Var
//	private String fileNameOCR;
//
//	//General Var
//	String newDBname = "DeleteMe";
//	String newDBnameSmss;
//	int  testCounter = 1;
//
//	@BeforeClass 
//	public void setUpOnce() {
//
//		//Set up file paths
//		semossDirectory = System.getProperty("user.dir");
//		semossDirectory = semossDirectory.replace("\\", "\\\\");
//		dbDirectory = semossDirectory + "\\db\\";
//		String testFolder = semossDirectory + "\\test\\";
//
//		newDBnameSmss = dbDirectory + "\\" + newDBname + ".smss";
//
//		//CSV Var
//		fileNameCSV = testFolder + "SEMOSSus-500.csv";
//		mapFileCSV = testFolder + newDBnameCSV +"_Custom_Map.prop";
//		dbPropFileCSV = testFolder + newDBnameCSV +"_Test_PROP.prop";
//		CSVsmss = dbDirectory + "\\" + newDBnameCSV + ".smss";
//		//CSV Replace 
//		replacementCSV = testFolder + "TwoData.csv";
//		replacementMapFileCSV = testFolder + "TwoData_Custom_Map.prop";
//		replacementdbPropFileCSV = testFolder + "TwoData_Test_PROP.prop";
//
//		//EXCEL Var
//		newDBnameEXCEL =				 "ExcelTest";
//		fileNameEXCEL = testFolder + newDBnameEXCEL+".xlsm";
//		mapFileEXCEL = testFolder + newDBnameEXCEL+"_Custom_Map.prop"; 
//		dbPropFileEXCEL = testFolder + newDBnameEXCEL+"_Custom_Map.prop";
//		EXCELsmss = dbDirectory + "\\" + newDBnameEXCEL + ".smss";
//		//Excel Replace
//		replacementExcel = testFolder + "ExcelReplace.xlsm";
//
//		System.out.println("Cleaning...");
//		if(new File(dbDirectory+newDBnameCSV).exists()){
//			clean();
//		}
//	}
//
//	@Before
//	public void setUp() throws Exception {
//		System.out.println("Test " + testCounter + "...");
//		testCounter++;
//		processor = new ImportDataProcessor();
//		engine = new BigDataEngine();
//
//		// Set the Sudo-Prop (Created in Prerna.ui.main.Starter.java)
//		System.setProperty("file.separator", "/");
//		String workingDir = System.getProperty("user.dir");
//		String propFile = workingDir + "/RDF_Map.prop";
//		DIHelper.getInstance().loadCoreProp(propFile);
//		PropertyConfigurator.configure(workingDir + "/log4j.prop");
//
//		//Set BaseDirectory
//		processor.setBaseDirectory(semossDirectory);
//	}
//
//	@After
//	public void tearDown() throws Exception {
//		//After tests are run
//		clean();
//	}
//
//	@AfterClass
//	public void finalTearDown() throws Exception {
//		//After all tests are run
//		clean();
//
//	}
//
//	//CSV DB created
//	@Test
//	public void Test_CreateNew_CSV() throws Exception{
//		prerna.ui.components.ImportDataProcessor.IMPORT_METHOD testMethod = IMPORT_METHOD.CREATE_NEW;
//		prerna.ui.components.ImportDataProcessor.IMPORT_TYPE testType = IMPORT_TYPE.CSV;
//		prerna.ui.components.ImportDataProcessor.DB_TYPE dbType = DB_TYPE.RDF;
//		prerna.util.sql.SQLQueryUtil.DB_TYPE rdbmsType = prerna.util.sql.SQLQueryUtil.DB_TYPE.H2_DB;//set for RDBMS
//		boolean allowDuplicates = true;
//
//		//Run Processor
//		processor.runProcessor(testMethod, testType, fileNameCSV, customBaseURI, newDBnameCSV, mapFileCSV, "", "", "", dbType, rdbmsType, allowDuplicates, true);
//		System.out.println("	CSV Db proccesor ran successfully. CSV DB Created.");
//
//		//TESTING ASSERTIONS
//		//Files Created and in the right place
//		File f = new File(dbDirectory+newDBnameCSV);
//		assertTrue("DB Folder exists.", f.exists());
//		f = new File(dbDirectory+newDBnameCSV+"\\"+newDBnameCSV+".jnl");
//		assertTrue("DB .jnl exists.", f.exists());
//		f = new File(dbDirectory+newDBnameCSV+"\\"+newDBnameCSV+"_Custom_Map.prop");
//		assertTrue("DB Custom_Map.prop exists.", f.exists());
//		f = new File(dbDirectory+newDBnameCSV+"\\"+newDBnameCSV+"_OWL.OWL");
//		assertTrue("DB .OWL exists.", f.exists());
//		f = new File(dbDirectory+newDBnameCSV+"\\"+newDBnameCSV+"_Questions.XML");
//		assertTrue("DB Questions.xml exists.", f.exists());
//		f = new File(dbDirectory+"\\"+newDBnameCSV+".smss");
//		assertTrue("DB smss exists.", f.exists());
//		System.out.println("	All Files Exist.");
//
//		//Setup for Header Tests(asserts) & Querys
//		BigDataEngine engine = loadEngine(CSVsmss);
//		engine.commitOWL();
//
//		//Checks All Generic Queries
//		Vector<String> insightIds = engine.getInsights();
//		Vector<Insight> insights = engine.getInsight(insightIds.toArray(new String[]{}));
//		Set<String> orders = new HashSet<String>();
//		for(int i = 0; i < insights.size(); i++){
//			Insight in = insights.get(i);
//			assertTrue("Insight has an id...", in.getInsightID() != null);
//			assertTrue("Insight has a perspective...", in.getPerspective() != null);
//			assertTrue("Insight has an order...", in.getOrder() != null);
//			orders.add(in.getOrder());
//			assertTrue("Insight has a data maker...", in.getDataMakerName() != null);
//			assertTrue("Insight has a layout...", in.getOutput() != null);
//		}
//		assertTrue("All orders are unique...", insights.size() == orders.size());
//
//		engine.closeDB();
//	}
//
//	//EXCEL DB created
//	@Test
//	public void Test_CreateNew_EXCEL() throws Exception{
//		prerna.ui.components.ImportDataProcessor.IMPORT_METHOD testMethod = IMPORT_METHOD.CREATE_NEW;
//		prerna.ui.components.ImportDataProcessor.IMPORT_TYPE testType = IMPORT_TYPE.EXCEL_POI;	
//		prerna.ui.components.ImportDataProcessor.DB_TYPE dbType = DB_TYPE.RDF;
//		prerna.util.sql.SQLQueryUtil.DB_TYPE rdbmsType = prerna.util.sql.SQLQueryUtil.DB_TYPE.H2_DB;//set for RDBMS
//		boolean allowDuplicates = true;//used by RDBMS
//
//		//Run Processor
//		processor.runProcessor(testMethod, testType, fileNameEXCEL, customBaseURI , newDBnameEXCEL, mapFileEXCEL, "", "", "", dbType, rdbmsType, allowDuplicates, true);
//		System.out.println("	EXCEL Db proccesor ran successfully. Excel DB created.");
//
//		//TESTING ASSERTIONS
//		//Files Created and in the right place
//		File f = new File(dbDirectory+newDBnameEXCEL);
//		assertTrue("DB Folder exists.", f.exists());
//		/*DBUG*/System.out.println(".jnl: "+ "\\" + dbDirectory+"\\"+newDBnameEXCEL+"\\\\"+newDBnameEXCEL+".jnl");
//		f = new File(dbDirectory+newDBnameEXCEL+"\\"+newDBnameEXCEL+".jnl");
//		assertTrue("DB .jnl exists.", f.exists());
//		f = new File(dbDirectory+newDBnameEXCEL+"\\"+newDBnameEXCEL+"_Custom_Map.prop");
//		assertTrue("DB Custom_Map.prop exists.", f.exists());
//		f = new File(dbDirectory+newDBnameEXCEL+"\\"+newDBnameEXCEL+"_OWL.OWL");
//		assertTrue("DB .OWL exists.", f.exists());
//		f = new File(dbDirectory+newDBnameEXCEL+"\\"+newDBnameEXCEL+"_Questions.XML");
//		assertTrue("DB Questions.xml exists.", f.exists());
//		f = new File(dbDirectory+newDBnameEXCEL+".smss");
//		assertTrue("DB smss exists.", f.exists());
//		System.out.println("	All Files Exist.");
//
//		//Setup for Header Tests(asserts) & Querys
//		BigDataEngine engine = loadEngine(EXCELsmss);
//		engine.commitOWL();
//
//		//Checks All Generic Queries
//		Vector<String> insightIds = engine.getInsights();
//		Vector<Insight> insights = engine.getInsight(insightIds.toArray(new String[]{}));
//		Set<String> orders = new HashSet<String>();
//		for(int i = 0; i < insights.size(); i++){
//			Insight in = insights.get(i);
//			assertTrue("Insight has an id...", in.getInsightID() != null);
//			assertTrue("Insight has a perspective...", in.getPerspective() != null);
//			assertTrue("Insight has an order...", in.getOrder() != null);
//			orders.add(in.getOrder());
//			assertTrue("Insight has a data maker...", in.getDataMakerName() != null);
//			assertTrue("Insight has a layout...", in.getOutput() != null);
//		}
//		assertTrue("All orders are unique...", insights.size() == orders.size());
//
//		engine.closeDB();
//	}
//
//	//CSV add to existing
//	@Test
//	public void Test_AddToExisting_CSV() throws Exception{
//		//Make CSV
//		makeCSV();
//
//		prerna.ui.components.ImportDataProcessor.IMPORT_METHOD testMethod = IMPORT_METHOD.ADD_TO_EXISTING;
//		prerna.ui.components.ImportDataProcessor.IMPORT_TYPE testType = IMPORT_TYPE.CSV;
//		prerna.ui.components.ImportDataProcessor.DB_TYPE dbType = DB_TYPE.RDF;
//		prerna.util.sql.SQLQueryUtil.DB_TYPE rdbmsType = prerna.util.sql.SQLQueryUtil.DB_TYPE.H2_DB;//set for RDBMS
//		boolean allowDuplicates = true;
//
//		//TESTING ASSERTIONS
//		//Files Created and in the right place
//		File f = new File(dbDirectory+newDBnameCSV);
//		assertTrue("DB Folder exists.", f.exists());
//		f = new File(dbDirectory+newDBnameCSV+"\\"+newDBnameCSV+".jnl");
//		assertTrue("DB .jnl exists.", f.exists());
//		f = new File(dbDirectory+newDBnameCSV+"\\"+newDBnameCSV+"_Custom_Map.prop");
//		assertTrue("DB Custom_Map.prop exists.", f.exists());
//		f = new File(dbDirectory+newDBnameCSV+"\\"+newDBnameCSV+"_OWL.OWL");
//		assertTrue("DB .OWL exists.", f.exists());
//		f = new File(dbDirectory+newDBnameCSV+"\\"+newDBnameCSV+"_Questions.XML");
//		assertTrue("DB Questions.xml exists.", f.exists());
//		f = new File(dbDirectory+"\\"+newDBnameCSV+".smss");
//		assertTrue("DB smss exists.", f.exists());
//		System.out.println("	All Files Exist.");
//
//		//ADD TO EXISTING
//		//Reset Prop
//		BigDataEngine engine = loadEngine(CSVsmss);
//
//		//Run Processor
//		processor.runProcessor(testMethod, testType, replacementCSV, customBaseURI, "", "", "", "", newDBnameCSV, dbType, rdbmsType, allowDuplicates, true);
//		System.out.println("CSV Db proccesor ran successfully. CSV DB Altered.");
//		engine.commitOWL();
//		engine.commit();
//		engine.closeDB();
//
//		//Setup for Header Tests(asserts) & Querys
//		engine = loadEngine(CSVsmss);
//
//		engine.closeDB();
//	}
//
//	//EXCEL add to existing
//	@Test
//	public void Test_AddToExisting_EXCEL() throws Exception{
//		//Make Test Excel
//		makeExcel();
//
//		prerna.ui.components.ImportDataProcessor.IMPORT_METHOD testMethod = IMPORT_METHOD.ADD_TO_EXISTING;
//		prerna.ui.components.ImportDataProcessor.IMPORT_TYPE testType = IMPORT_TYPE.EXCEL_POI;
//		prerna.ui.components.ImportDataProcessor.DB_TYPE dbType = DB_TYPE.RDF;
//		prerna.util.sql.SQLQueryUtil.DB_TYPE rdbmsType = prerna.util.sql.SQLQueryUtil.DB_TYPE.H2_DB;//set for RDBMS
//		boolean allowDuplicates = true;
//
//		//TESTING ASSERTIONS
//		//Files Created and in the right place
//		File f = new File(dbDirectory+newDBnameEXCEL);
//		assertTrue("DB Folder doesn't exists.", f.exists());
//		f = new File(dbDirectory+newDBnameEXCEL+"\\"+newDBnameEXCEL+".jnl");
//		assertTrue("DB .jnl doesn't exists.", f.exists());
//		f = new File(dbDirectory+newDBnameEXCEL+"\\"+newDBnameEXCEL+"_Custom_Map.prop");
//		assertTrue("DB Custom_Map.prop doesn't exists.", f.exists());
//		f = new File(dbDirectory+newDBnameEXCEL+"\\"+newDBnameEXCEL+"_OWL.OWL");
//		assertTrue("DB .OWL doesn't exists.", f.exists());
//		f = new File(dbDirectory+newDBnameEXCEL+"\\"+newDBnameEXCEL+"_Questions.XML");
//		assertTrue("DB Questions.xml doesn't exists.", f.exists());
//		f = new File(dbDirectory+newDBnameEXCEL+".smss");
//		assertTrue("DB smss doesn't exists.", f.exists());
//		System.out.println("	All Files Exist.");
//
//		//Reset Prop
//		BigDataEngine engine = loadEngine(EXCELsmss);
//
//		//Run Processor
//		processor.runProcessor(testMethod, testType, replacementExcel, customBaseURI , "", "", "", "", newDBnameEXCEL, dbType, rdbmsType, allowDuplicates, true);
//		System.out.println("	EXCEL Db proccesor ran successfully. Excel DB created.");
//		engine.commitOWL();
//		engine.commit();
//		engine.closeDB();
//
//		//Setup for Header Tests(asserts) & Querys
//		engine = loadEngine(EXCELsmss);
//
//		engine.closeDB();
//	}
//
//	//CSV Override
//	@Ignore
//	@Test
//	public void Test_OVERRIDE_CSV() throws Exception{
//		makeCSV();
//
//		prerna.ui.components.ImportDataProcessor.IMPORT_METHOD testMethod = IMPORT_METHOD.OVERRIDE;
//		prerna.ui.components.ImportDataProcessor.IMPORT_TYPE testType = IMPORT_TYPE.CSV;
//		prerna.ui.components.ImportDataProcessor.DB_TYPE dbType = DB_TYPE.RDF;
//		prerna.util.sql.SQLQueryUtil.DB_TYPE rdbmsType = prerna.util.sql.SQLQueryUtil.DB_TYPE.H2_DB;//set for RDBMS
//		boolean allowDuplicates = true;
//
//		//TESTING ASSERTIONS
//		//Files Created and in the right place
//		File f = new File(dbDirectory+newDBnameCSV);
//		assertTrue("DB Folder exists.", f.exists());
//		f = new File(dbDirectory+newDBnameCSV+"\\"+newDBnameCSV+".jnl");
//		assertTrue("DB .jnl exists.", f.exists());
//		f = new File(dbDirectory+newDBnameCSV+"\\"+newDBnameCSV+"_Custom_Map.prop");
//		assertTrue("DB Custom_Map.prop exists.", f.exists());
//		f = new File(dbDirectory+newDBnameCSV+"\\"+newDBnameCSV+"_OWL.OWL");
//		assertTrue("DB .OWL exists.", f.exists());
//		f = new File(dbDirectory+newDBnameCSV+"\\"+newDBnameCSV+"_Questions.XML");
//		assertTrue("DB Questions.xml exists.", f.exists());
//		f = new File(dbDirectory+"\\"+newDBnameCSV+".smss");
//		assertTrue("DB smss exists.", f.exists());
//		System.out.println("	All Files Exist.");
//
//		//OVERRIDE
//		IEngine engine = new BigDataEngine();
//
//		FileInputStream fileIn = null;
//		Properties prop = new Properties();
//		try {
//			fileIn = new FileInputStream(CSVsmss);
//			prop.load(fileIn);
//			System.out.println("prop:     "+prop);
//			engine = Utility.loadEngine(CSVsmss, prop);
//			fileIn.close();
//			//engine.closeDB();
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//
//		//Run Processor
//		processor.runProcessor(testMethod, testType, replacementCSV, customBaseURI, "", "", "", "", newDBnameCSV, dbType, rdbmsType, allowDuplicates, true);
//		System.out.println("CSV Db proccesor ran successfully. CSV DB Replaced.");
//	}
//
//	//EXCEL Override
//	@Ignore
//	@Test
//	public void Test_OVERRIDE_EXCEL() throws Exception{
//		makeExcel();
//
//		prerna.ui.components.ImportDataProcessor.IMPORT_METHOD testMethod = IMPORT_METHOD.OVERRIDE;
//		prerna.ui.components.ImportDataProcessor.IMPORT_TYPE testType = IMPORT_TYPE.EXCEL_POI;
//		prerna.ui.components.ImportDataProcessor.DB_TYPE dbType = DB_TYPE.RDF;
//		prerna.util.sql.SQLQueryUtil.DB_TYPE rdbmsType = prerna.util.sql.SQLQueryUtil.DB_TYPE.H2_DB;//set for RDBMS
//		boolean allowDuplicates = true;
//
//		String SudoSmss = dbDirectory+newDBnameEXCEL+".smss";
//
//		//TESTING ASSERTIONS
//		//Files Created and in the right place
//		File f = new File(dbDirectory+newDBnameEXCEL);
//		assertTrue("DB Folder exists.", f.exists());
//		f = new File(dbDirectory+newDBnameEXCEL+"\\"+newDBnameEXCEL+".jnl");
//		assertTrue("DB .jnl exists.", f.exists());
//		f = new File(dbDirectory+newDBnameEXCEL+"\\"+newDBnameEXCEL+"_Custom_Map.prop");
//		assertTrue("DB Custom_Map.prop exists.", f.exists());
//		f = new File(dbDirectory+newDBnameEXCEL+"\\"+newDBnameEXCEL+"_OWL.OWL");
//		assertTrue("DB .OWL exists.", f.exists());
//		f = new File(dbDirectory+newDBnameEXCEL+"\\"+newDBnameEXCEL+"_Questions.XML");
//		assertTrue("DB Questions.xml exists.", f.exists());
//		f = new File(dbDirectory+newDBnameEXCEL+".smss");
//		assertTrue("DB smss exists.", f.exists());
//		System.out.println("	All Files Exist.");
//
//		//Reset Prop
//		IEngine engine = new BigDataEngine();
//
//		FileInputStream fileIn = null;
//		Properties prop = new Properties();
//		try {
//			fileIn = new FileInputStream(SudoSmss);
//			prop.load(fileIn);
//			System.out.println("prop:     "+prop);
//			engine = Utility.loadEngine(SudoSmss, prop);
//			fileIn.close();
//			//engine.closeDB();
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//
//		//Run Processor
//		processor.runProcessor(testMethod, testType, replacementExcel, customBaseURI , "", "", "", "", newDBnameEXCEL, dbType, rdbmsType, allowDuplicates, true);
//		System.out.println("	EXCEL Db proccesor ran successfully. Excel DB replaced.");
//		engine.closeDB();
//	}
//
//	/* This is the tools section for this Class 
//	 * makeExcel
//	 * makeCSV
//	 * checkQuery
//	 * prepQuery
//	 * loadEngine
//	 */
//	private void makeExcel() throws Exception{
//		prerna.ui.components.ImportDataProcessor.IMPORT_METHOD testMethod = IMPORT_METHOD.CREATE_NEW;
//		prerna.ui.components.ImportDataProcessor.IMPORT_TYPE testType = IMPORT_TYPE.EXCEL_POI;	
//		prerna.ui.components.ImportDataProcessor.DB_TYPE dbType = DB_TYPE.RDF;
//		prerna.util.sql.SQLQueryUtil.DB_TYPE rdbmsType = prerna.util.sql.SQLQueryUtil.DB_TYPE.H2_DB;//set for RDBMS
//		boolean allowDuplicates = true;
//
//		//Run Processor
//		processor.runProcessor(testMethod, testType, fileNameEXCEL, customBaseURI , newDBnameEXCEL, mapFileEXCEL, "", "", "", dbType, rdbmsType, allowDuplicates, true);
//		System.out.println("	EXCEL Db proccesor ran successfully. Excel DB created.");
//	}
//
//	private void makeCSV() throws Exception{
//		prerna.ui.components.ImportDataProcessor.IMPORT_METHOD testMethod = IMPORT_METHOD.CREATE_NEW;
//		prerna.ui.components.ImportDataProcessor.IMPORT_TYPE testType = IMPORT_TYPE.CSV;
//		prerna.ui.components.ImportDataProcessor.DB_TYPE dbType = DB_TYPE.RDF;
//		prerna.util.sql.SQLQueryUtil.DB_TYPE rdbmsType = prerna.util.sql.SQLQueryUtil.DB_TYPE.H2_DB;//set for RDBMS
//		boolean allowDuplicates = true;
//
//		//Run Processor
//		processor.runProcessor(testMethod, testType, fileNameCSV, customBaseURI, newDBnameCSV, mapFileCSV, "", "", "", dbType, rdbmsType, allowDuplicates, true);
//		System.out.println("	CSV Db proccesor ran successfully. CSV DB Created.");
//	}
//
//	private BigDataEngine loadEngine(String engineLocation){
//		BigDataEngine engine = new BigDataEngine();
//		FileInputStream fileIn;
//		Properties prop = new Properties();
//		try {
//			fileIn = new FileInputStream(engineLocation);
//			prop.load(fileIn);
//			//SEP
//			try {
//				String engines = DIHelper.getInstance().getLocalProp(Constants.ENGINES) + "";
//
//				String engineName = prop.getProperty(Constants.ENGINE);
//				String engineClass = prop.getProperty(Constants.ENGINE_TYPE);
//				//TEMPORARY
//				// TODO: remove this
//				if(engineClass.equals("prerna.rdf.engine.impl.RDBMSNativeEngine")){
//					engineClass = "prerna.engine.impl.rdbms.RDBMSNativeEngine";
//				}
//				else if(engineClass.startsWith("prerna.rdf.engine.impl.")){
//					engineClass = engineClass.replace("prerna.rdf.engine.impl.", "prerna.engine.impl.rdf.");
//				}
//				engine = (BigDataEngine)Class.forName(engineClass).newInstance();
//				engine.setEngineName(engineName);
//				if(prop.getProperty("MAP") != null) {
//					engine.addProperty("MAP", prop.getProperty("MAP"));
//				}
//				engine.openDB(engineLocation);
//				engine.setDreamer(prop.getProperty(Constants.DREAMER));
////				engine.setOntology(prop.getProperty(Constants.ONTOLOGY));
//
//				// set the core prop
//				if(prop.containsKey(Constants.DREAMER))
//					DIHelper.getInstance().getCoreProp().setProperty(engineName + "_" + Constants.DREAMER, prop.getProperty(Constants.DREAMER));
////				if(prop.containsKey(Constants.ONTOLOGY))
////					DIHelper.getInstance().getCoreProp().setProperty(engineName + "_" + Constants.ONTOLOGY, prop.getProperty(Constants.ONTOLOGY));
//				if(prop.containsKey(Constants.OWL)) {
//					DIHelper.getInstance().getCoreProp().setProperty(engineName + "_" + Constants.OWL, prop.getProperty(Constants.OWL));
//					engine.setOWL(prop.getProperty(Constants.OWL));
//				}
//
//				// set the engine finally
//				engines = engines + ";" + engineName;
//				DIHelper.getInstance().setLocalProperty(engineName, engine);
//				DIHelper.getInstance().setLocalProperty(Constants.ENGINES, engines);
//			} catch (InstantiationException e) {
//				e.printStackTrace();
//			} catch (IllegalAccessException e) {
//				e.printStackTrace();
//			} catch (ClassNotFoundException e) {
//				e.printStackTrace();
//			}
//			//SEP
//			fileIn.close();
//			prop.clear();
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		return engine;
//	}
//
//	private void clean(){
//		//DeleteMe
//		//Delete Source Folder
//		File file = new File(dbDirectory+newDBname);
//		deleteFile(file);
//		//Delete .temp
//		file = new File(dbDirectory+newDBname+".temp");
//		deleteFile(file);
//		//Delete .SMSS
//		file = new File(dbDirectory+newDBname+".smss");
//		deleteFile(file);
//
//		//CSV
//		//Delete Source Folder
//		file = new File(dbDirectory+newDBnameCSV);
//		deleteFile(file);
//		//Delete .temp
//		file = new File(dbDirectory+newDBnameCSV+".temp");
//		deleteFile(file);
//		//Delete .SMSS
//		file = new File(CSVsmss);
//		deleteFile(file);
//
//		//Excel
//		//Delete Source Folder
//		file = new File(dbDirectory+newDBnameEXCEL);
//		deleteFile(file);
//		//Delete .temp
//		file = new File(dbDirectory+newDBnameEXCEL+".temp");
//		deleteFile(file);
//		//Delete .SMSS
//		file = new File(EXCELsmss);
//		deleteFile(file);
//	}
//	
//	/**
//	 * Method deleteFile.  Deletes a file from the directory.
//	 * @param file File
//	 */
//	public void deleteFile(File file) {
//		if(file.isDirectory()) {
//			//directory is empty, then delete it
//			if(file.list().length==0) {
//				file.delete();
//			} else {
//				//list all the directory contents
//				String files[] = file.list();
//
//				for (String temp : files) {
//					//construct the file structure
//					File fileDelete = new File(file, temp);
//
//					//this delete is only for two levels.  At this point, must be file, so just delete it
//					fileDelete.delete();
//				}
//				//check the directory again, if empty then delete it
//				if(file.list().length==0) {
//					file.delete();
//				}
//			}
//		} else {
//			//if file, then delete it
//			file.delete();
//		}
//	}
//}
//
