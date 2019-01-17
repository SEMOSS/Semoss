package prerna.junit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeNoException;
import static org.junit.Assume.assumeThat;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dbunit.DatabaseUnitException;
import org.dbunit.IDatabaseTester;
import org.dbunit.JdbcDatabaseTester;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.operation.DatabaseOperation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.quartz.SchedulerException;

import com.google.gson.Gson;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityUpdateUtils;
import prerna.configure.Me;
import prerna.ds.py.PyExecutorThread;
import prerna.ds.py.PyUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.forms.AbstractFormBuilder;
import prerna.nameserver.DeleteFromMasterDB;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.rpa.quartz.SchedulerUtil;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.reactor.frame.r.util.RJavaTranslatorFactory;
import prerna.theme.AbstractThemeUtils;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.SMSSWebWatcher;
import prerna.util.Utility;
import prerna.util.gson.GsonUtility;

public class PixelUnit {
	
	protected static final Logger LOGGER = LogManager.getLogger(JUnit.class.getName());
	
	protected static final String BASE_DIRECTORY = new File("").getAbsolutePath();
	protected static final String BASE_DB_DIRECTORY = Paths.get(BASE_DIRECTORY, "db").toAbsolutePath().toString();
	protected static final String TEST_RESOURCES_DIRECTORY = Paths.get(BASE_DIRECTORY, "test", "resources").toAbsolutePath().toString();
	protected static final String TEST_DATA_DIRECTORY = Paths.get(TEST_RESOURCES_DIRECTORY, "data").toAbsolutePath().toString();
	
	protected static final Gson GSON = GsonUtility.getDefaultGson();
	
	private static final String TEST_DIR_REGEX = "<<<testDir>>>";
	private static final String BASE_DIR_REGEX = "<<<baseDir>>>";
	private static final String APP_ID_REGEX = "<<<appId>>>(.*?)<<</appId>>>";

	private static final String DATA_EXTENSION = ".csv";
	private static final String IMPORT_EXTENSION = "_import.txt";
	
	private static final Path BASE_RDF_MAP = Paths.get(BASE_DIRECTORY, "RDF_Map.prop");
	private static final Path TEST_RDF_MAP = Paths.get(TEST_RESOURCES_DIRECTORY, "RDF_Map.prop");
		
	protected static Map<String, String> aliasToAppId = new HashMap<>();
	
	private static boolean testDatabasesAreClean = true; // This must be static, as one instance of PixelUnit can dirty databases for all others
	private List<String> cleanTestDatabases = new ArrayList<>();
	
	private Insight insight = null;
	
	private PyExecutorThread jep = null;

	
	
	//////////////////////////////////////////////////////////////////////
	// Before and after class
	//////////////////////////////////////////////////////////////////////
	
	// Allow initialization methods to throw exceptions, halting the rest of the test cases
	@BeforeClass
	public static void initializeContext() {
		try {
			configureLog4j();
			loadDIHelper();
			loadDatabases();
			loadTestDatabases();
		} catch (Exception e) {
			
			// Tests assume that this setup is correct in order to run
			LOGGER.error(e);
			assumeNoException(e);
		}
		checkAssumptions();
	}
	
	// Destroy methods should not throw exceptions, so that subsequent cleanup methods have the opportunity to run
	@AfterClass
	public static void destroyContext() {
		unloadTestDatabases();
		unloadDatabases();
		unloadDIHelper();
		unloadOther(); // Just in case
	}
	
	
	//////////////////////////////
	// Assumptions
	//////////////////////////////
	
	private static void checkAssumptions() {
		
		// Assume that Python must be enabled for tests to run, since we use deepdiff for tests
		assumeThat(PyUtils.pyEnabled(), is(equalTo(true)));
	}
	
	
	//////////////////////////////
	// Log4j
	//////////////////////////////
	
	private static void configureLog4j() {
		String log4JPropFile = Paths.get(TEST_RESOURCES_DIRECTORY, "log4j.properties").toAbsolutePath().toString();
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream(log4JPropFile));
			PropertyConfigurator.configure(prop);
			LOGGER.info("Successfully configured Log4j for JUnit suite.");
		} catch (IOException e) {
			LOGGER.warn("Unable to initialize log4j for testing.", e);
		}
	}
	
	
	//////////////////////////////
	// DIHelper
	//////////////////////////////
	
	private static void loadDIHelper() throws IOException {
		Files.copy(BASE_RDF_MAP, TEST_RDF_MAP, StandardCopyOption.REPLACE_EXISTING);
		Me configurationManager = new Me();
		configurationManager.changeRDFMap(BASE_DIRECTORY.replace('\\', '/'), "80", TEST_RDF_MAP.toAbsolutePath().toString());
		DIHelper.getInstance().loadCoreProp(TEST_RDF_MAP.toAbsolutePath().toString());
		
		// Just in case, manually override USE_PYTHON to be true for testing purposes
		// Warn if this was not the case to begin with
		if (!PyUtils.pyEnabled()) {
			LOGGER.warn("Python must be functional for local testing.");
			Properties coreProps = DIHelper.getInstance().getCoreProp();
			coreProps.setProperty(Constants.USE_PYTHON, "true");
			DIHelper.getInstance().setCoreProp(coreProps);
		}
	}
	
	private static void unloadDIHelper() {
		try {
			Files.delete(TEST_RDF_MAP);
		} catch (IOException e) {
			LOGGER.warn("Unable to delete " + TEST_RDF_MAP, e);
		}
	}

	
	//////////////////////////////
	// Databases
	//////////////////////////////
	
	private static void loadDatabases() throws SQLException, IOException {

		// Local master database
		SMSSWebWatcher.loadNewDB(Constants.LOCAL_MASTER_DB_NAME + ".smss", BASE_DB_DIRECTORY);
		MasterDatabaseUtility.initLocalMaster();
		
		// Security
		SMSSWebWatcher.loadNewDB(Constants.SECURITY_DB + ".smss", BASE_DB_DIRECTORY);
		AbstractSecurityUtils.loadSecurityDatabase();
		
		// Themes
		SMSSWebWatcher.loadNewDB(Constants.THEMING_DB + ".smss", BASE_DB_DIRECTORY);
		AbstractThemeUtils.loadThemingDatabase();
	}
	
	private static void unloadDatabases() {
				
		@SuppressWarnings("deprecation")
		IEngine formBuilder = Utility.getEngine(AbstractFormBuilder.FORM_BUILDER_ENGINE_NAME);
		if(formBuilder != null) {
			formBuilder.closeDB();
		}
		
		@SuppressWarnings("deprecation")
		IEngine localMaster = Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		if(localMaster != null) {
			localMaster.closeDB();
		}
		
		@SuppressWarnings("deprecation")
		IEngine security = Utility.getEngine(Constants.SECURITY_DB);
		if(security != null) {
			security.closeDB();
		}
		
		@SuppressWarnings("deprecation")
		IEngine themes = Utility.getEngine(Constants.THEMING_DB);
		if(themes != null) {
			themes.closeDB();
		}
		
		DIHelper.getInstance().removeLocalProperty(AbstractFormBuilder.FORM_BUILDER_ENGINE_NAME);
		DIHelper.getInstance().removeLocalProperty(Constants.LOCAL_MASTER_DB_NAME);
		DIHelper.getInstance().removeLocalProperty(Constants.SECURITY_DB);
		DIHelper.getInstance().removeLocalProperty(Constants.THEMING_DB);
	}
	
	
	//////////////////////////////
	// Test databases
	//////////////////////////////
	
	private static void loadTestDatabases() throws IOException, ClassNotFoundException, SQLException, DatabaseUnitException {
		
		// List all the files in the test database directory
		String[] fileNames = new File(TEST_DATA_DIRECTORY).list();
		
		// If there are corresponding csv and txt files, then load the test database
		List<String> dataNames = new ArrayList<>();
		List<String> importNames = new ArrayList<>();
		for (String file : fileNames) {
			if (file.endsWith(DATA_EXTENSION)) {
				dataNames.add(file.substring(0, file.length() - DATA_EXTENSION.length()));
			} else if (file.endsWith(IMPORT_EXTENSION)) {
				importNames.add(file.substring(0, file.length() - IMPORT_EXTENSION.length()));
			}
		}
		for (String alias : dataNames) {
			if (importNames.contains(alias)) {
				loadTestDatabase(alias);
			}
		}
	}
	
	private static void unloadTestDatabases() {
		for (String alias : aliasToAppId.keySet()) {
			unloadTestDatabase(alias);
		}
	}
	
	private static void loadTestDatabase(String alias) throws IOException, ClassNotFoundException, SQLException, DatabaseUnitException {
		
		// First, delete any existing databases with the same alias
		List<String> existingAppIds = MasterDatabaseUtility.getEngineIdsForAlias(alias);
		DeleteFromMasterDB remover = new DeleteFromMasterDB();
		for(String appId : existingAppIds) {
			
			// First must catalog the db in order to call the getEngine
			SMSSWebWatcher.catalogDB(alias + "__" + appId + ".smss", BASE_DB_DIRECTORY);
			
			@SuppressWarnings("deprecation")
			IEngine engine = Utility.getEngine(appId);
			
			// If a full-fledged engine, then delete the entire app
			if (engine != null) {
				deleteApp(appId);
			} else {
				
				// Else, its just metadata hanging around
				remover.deleteEngineRDBMS(appId);
				SecurityUpdateUtils.deleteApp(appId);
			}
		}
		
		// Read in the import pixel and set the keys
		String importPixel = FileUtils.readFileToString(Paths.get(TEST_DATA_DIRECTORY, alias + IMPORT_EXTENSION).toFile());
		
		// Run the pixel to import the data
		PixelRunner returnData = runPixelUtil(importPixel);
		@SuppressWarnings("unchecked")
		String appId = ((Map<String, String>) returnData.getResults().get(0).getValue()).get("app_id");		
		
		// Store the appId
		aliasToAppId.put(alias, appId);	
		
		// Save the original state of the database
		@SuppressWarnings("deprecation")
		IEngine engine = Utility.getEngine(appId);
		if (isRelationalDatabase(engine)) {
			exportDatabaseToXml(alias);
		}
	}
	
	private static void unloadTestDatabase(String alias) {
		String appId = aliasToAppId.get(alias);
		if (appId != null) {
			
			// If relational, delete the corresponding xml file
			@SuppressWarnings("deprecation")
			IEngine engine = Utility.getEngine(appId);
			if (isRelationalDatabase(engine)) {
				Path xmlFile = getXMLFileForAlias(alias);
				try {
					Files.delete(xmlFile);
				} catch (IOException e) {
					LOGGER.warn("Unable to delete " + xmlFile, e);
				}
			}
			
			// Next, delete the app
			deleteApp(appId);
		}
	}
	
	private static void deleteApp(String appId) {
		String deletePixel = "DeleteApp(app=[\"" + appId + "\"] );";
		runPixelUtil(deletePixel);
	}
	
	private static void exportDatabaseToXml(String alias) throws IOException, ClassNotFoundException, SQLException, DatabaseUnitException {
		
		// Database connection
		Class.forName("org.h2.Driver");
		
		String jdbcUrl = "jdbc:h2:nio:@BaseFolder@/db/@ENGINE@/database";
		jdbcUrl = jdbcUrl.replaceAll("@BaseFolder@", BASE_DIRECTORY.replace('\\', '/'));
		jdbcUrl = jdbcUrl.replaceAll("@ENGINE@", alias + "__" + aliasToAppId.get(alias));
		
		try (Connection jdbcConnection = DriverManager.getConnection(jdbcUrl, "sa", "")) {
			IDatabaseConnection connection = null;
			try {
				connection = new DatabaseConnection(jdbcConnection);
	
				// Full database export
				IDataSet fullDataSet = connection.createDataSet();
				try (OutputStream stream = new FileOutputStream(getXMLFileForAlias(alias).toAbsolutePath().toString())) {
					FlatXmlDataSet.write(fullDataSet, stream);
				}
			} finally {
				if (connection != null) {
					connection.close();
				}
			}
		}
		
	}
	
	private static Path getXMLFileForAlias(String alias) {
		return Paths.get(TEST_RESOURCES_DIRECTORY, alias + ".xml");
	}
	
	private static PixelRunner runPixelUtil(String pixel) {
		pixel = fillPixelString(pixel);
		long start = System.currentTimeMillis();
		PixelRunner returnData = new Insight().runPixel(pixel);
		LOGGER.info(GSON.toJson(returnData.getResults()));
		long end = System.currentTimeMillis();
		LOGGER.info("Execution time : " + (end - start) + " ms");
		return returnData;
	}
	
	
	//////////////////////////////
	// Other
	//////////////////////////////
	
	private static void unloadOther() {
		
		// Close scheduler
		try {
			SchedulerUtil.shutdownScheduler(true);
		} catch (SchedulerException e) {
			LOGGER.warn("Unable to shutdown scheduler.", e);
		}
		
		// Close r
		RJavaTranslatorFactory.stopRConnection();
	}
	
	
	
	//////////////////////////////////////////////////////////////////////
	// Before and after each test method
	//////////////////////////////////////////////////////////////////////
	
	@Before
	public void initializeTest() {
		initializeInsight();
		initializeJep();
		cleanTestDatabases = new ArrayList<>(); // Reset the list of databases to clean
		assumeThat(testDatabasesAreClean, is(equalTo(true))); // Assume that the databases are clean for tests to work properly
	}
	
	@After
	public void destroyTest() {
		destroyInsight();
		destroyJep();
		cleanTestDatabases();
	}
	
	
	//////////////////////////////
	// Insight
	//////////////////////////////
	
	private void initializeInsight() {
		insight = new Insight();
		InsightStore.getInstance().put(insight);
	}
	
	private void destroyInsight() {
		if (insight != null) {
			InsightStore.getInstance().remove(insight.getInsightId());
		}
		insight = null;
	}
	
	
	//////////////////////////////
	// Python
	//////////////////////////////
	
	private void initializeJep() {
		
		// Initialize jep
		jep = PyUtils.getInstance().getJep();
		
		// Wait for Python to load
		LOGGER.info("Waiting for python to initialize...");
		long start = System.currentTimeMillis();
		boolean timeout = false;
		while (!jep.isReady() && !timeout) {
			timeout = (System.currentTimeMillis() - start) > 30000;
		}
		
		// Assume that Python did not timeout for tests to run
		assumeThat(timeout, is(not(equalTo(true))));
		LOGGER.info("Python has initialized.");

	}
	
	private void destroyJep() {
		PyUtils.getInstance().killPyThread(jep);
		jep = null;
	}	
	
	
	//////////////////////////////
	// Test databases
	//////////////////////////////
	
	// https://www.marcphilipp.de/blog/2012/03/13/database-tests-with-dbunit-part-1/
	@SuppressWarnings("deprecation")
	private void cleanTestDatabases() {
		for (String alias : cleanTestDatabases) {
			String appId = aliasToAppId.get(alias);
			if (appId != null) {
				IEngine engine = Utility.getEngine(appId);
				if (isRelationalDatabase(engine)) {
					
					// Cast to RDBMS to grab the connection details
					RDBMSNativeEngine rdbms = (RDBMSNativeEngine) engine;
					
					try {
						String connectionUrl = rdbms.getConnectionMetadata().getURL();
						String driver = rdbms.getDbType().getDriver();
		
						// Close the db
						rdbms.closeDB();
					
						// Clean insert
						IDataSet dataSet = new FlatXmlDataSetBuilder().build(new File(getXMLFileForAlias(alias).toAbsolutePath().toString()));
						IDatabaseTester databaseTester = new JdbcDatabaseTester(driver, connectionUrl, "sa", "");
						databaseTester.setSetUpOperation(DatabaseOperation.CLEAN_INSERT);
						databaseTester.setDataSet(dataSet);
						databaseTester.onSetup();
						LOGGER.info("Cleaned: " + alias);
					} catch (Exception e) {
						LOGGER.warn("Cannot clean database with the alias " + alias + ", an exception has occurred.", e);
						testDatabasesAreClean = false;
					} finally {
						rdbms.openDB(null);
					}
				} else {
					LOGGER.warn("Cannot clean database with the alias " + alias + ", database is not an RDBMS.");
					testDatabasesAreClean = false;
				}
			} else {
				LOGGER.warn("Cannot clean database with the alias " + alias + ", database not found.");
				testDatabasesAreClean = false;
			}
		}
	}
	
	protected void setCleanTestDatabases(List<String> cleanTestDatabases) {
		this.cleanTestDatabases = cleanTestDatabases;
	}
	
	private static boolean isRelationalDatabase(IEngine engine) {
		return engine != null && engine.getEngineType().equals(ENGINE_TYPE.RDBMS);
	}
	
	
	
	//////////////////////////////////////////////////////////////////////
	// Methods for use during testing (by subclasses)
	//////////////////////////////////////////////////////////////////////

	
	//////////////////////////////
	// Pixel
	//////////////////////////////
	
	// Test pixel methods (overloaded)
	protected void testPixel(String pixel, String expectedJson) {
		testPixel(pixel, expectedJson, new ArrayList<String>(), false);
	}
	
	protected void testPixel(String pixel, String expectedJson, List<String> excludePaths) {
		testPixel(pixel, expectedJson, excludePaths, false);
	}
	
	protected void testPixel(String pixel, String expectedJson, boolean ignoreOrder) {
		testPixel(pixel, expectedJson, new ArrayList<String>(), ignoreOrder);
	}
	
	protected void testPixel(String pixel, String expectedJson, List<String> excludePaths, boolean ignoreOrder) {
		Object result = compareResult(pixel, expectedJson, excludePaths, ignoreOrder);
		assertThat(result, is(equalTo(new HashMap<>())));
	}
	
	// Compare result methods (overloaded)
	protected Object compareResult(String pixel, String expectedJson) {
		return compareResult(pixel, expectedJson, new ArrayList<String>(), false);
	}
	
	protected Object compareResult(String pixel, String expectedJson, List<String> excludePaths) {
		return compareResult(pixel, expectedJson, excludePaths, false);
	}
	
	protected Object compareResult(String pixel, String expectedJson, boolean ignoreOrder) {
		return compareResult(pixel, expectedJson, new ArrayList<String>(), ignoreOrder);
	}
	
	protected Object compareResult(String pixel, String expectedJson, List<String> excludePaths, boolean ignoreOrder) {
		
		// Cleanup the expected json to make sure that it doesn't have any newlines
		expectedJson = expectedJson.replaceAll("\r", "");
		expectedJson = expectedJson.replaceAll("\n", "");
		
		// Run the pixel and get the result
		PixelRunner returnData = runPixel(pixel);
		String actualJson = GSON.toJson(returnData.getResults());
		
		// The Python script to compare the deep difference
		String ignoreOrderString = ignoreOrder ? "True" : "False";
		String ddiffCommand = (excludePaths.size() > 0) ? "DeepDiff(a, b, ignore_order=" + ignoreOrderString + ", exclude_paths={\"" + String.join("\", \"", excludePaths) + "\"})" : 
			"DeepDiff(a, b, ignore_order=" + ignoreOrderString + ")";
		String[] script = new String[] {"import json",
				"from deepdiff import DeepDiff",
				"a = json.loads('" + actualJson + "')",
				"b = json.loads('" + expectedJson +  "')",
				"ddiff = " + ddiffCommand};
		runPy(script);
		Object result = runPy("ddiff");
		
		// Make sure there is no difference, ignoring order
		LOGGER.info("EXPECTED: " + expectedJson);
		LOGGER.info("ACTUAL: " + actualJson);
		LOGGER.info("DIFF: " + result);
		return result;
	}
	
	protected PixelRunner runPixel(String pixel) {
		pixel = fillPixelString(pixel);
		long start = System.currentTimeMillis();
		PixelRunner returnData = insight.runPixel(pixel);
		long end = System.currentTimeMillis();
		LOGGER.info("Execution time : " + (end - start) + " ms");
		return returnData;
	}
	
	protected static String fillPixelString(String pixel) {
		pixel = pixel.replaceAll(TEST_DIR_REGEX, Paths.get(TEST_RESOURCES_DIRECTORY).toAbsolutePath().toString().replace('\\', '/'));
		pixel = pixel.replaceAll(BASE_DIR_REGEX, Paths.get(BASE_DIRECTORY).toAbsolutePath().toString().replace('\\', '/'));
		
		Pattern appIdPattern = Pattern.compile(APP_ID_REGEX);
		Matcher appIdMatcher = appIdPattern.matcher(pixel);
		while(appIdMatcher.find()) {
			String alias = appIdMatcher.group(1);
			String appId = aliasToAppId.containsKey(alias) ? aliasToAppId.get(alias) : "null";
			pixel = appIdMatcher.replaceFirst(appId);
			appIdMatcher = appIdPattern.matcher(pixel);
		}
		return pixel;
	}
	
	
	//////////////////////////////
	// Python
	//////////////////////////////
	
	// Method for running a single Python command
	protected Object runPy(String script) {
		return runPy(new String[] {script}).get(script);
	}
	
	// Method for running multiple python commands
	protected Hashtable<String, Object> runPy(String... script) {
		
		// Set the commands
		jep.command = script;
		
		// Tell the thread to execute its commands
		Object jepMonitor = jep.getMonitor();
		synchronized(jepMonitor) {
			jepMonitor.notifyAll();
			try {
				
				// Wait for the commands to finish execution, but abort after 30s
				jepMonitor.wait(30000);
			} catch (InterruptedException e) {
				LOGGER.error("The following Python script was interrupted: " + String.join("\n", script), e);
			}
		}
		return jep.response;
	}
	
}
