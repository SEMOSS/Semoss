package prerna.junit.pixel;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assume.assumeNoException;
import static org.junit.Assume.assumeThat;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
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
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.core.StreamingOutput;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;
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
import org.junit.AssumptionViolatedException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.quartz.SchedulerException;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.configure.Me;
import prerna.ds.py.PyExecutorThread;
import prerna.ds.py.PyUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.forms.AbstractFormBuilder;
import prerna.nameserver.DeleteFromMasterDB;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.reactor.frame.r.util.RJavaTranslatorFactory;
import prerna.rpa.quartz.SchedulerUtil;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.PixelStreamUtility;
import prerna.theme.AbstractThemeUtils;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.SMSSWebWatcher;
import prerna.util.Utility;
import prerna.util.gson.GsonUtility;

// TODO: import org.json.simple.JSONObject;

public class PixelUnit {

	protected static final Logger classLogger = LogManager.getLogger(JUnit.class.getName());

	protected static final String BASE_DIRECTORY = new File("").getAbsolutePath();
	protected static final String BASE_DB_DIRECTORY = Paths.get(BASE_DIRECTORY, "db").toAbsolutePath().toString();
	protected static final String TEST_RESOURCES_DIRECTORY = Paths.get(BASE_DIRECTORY, "test", "resources")
			.toAbsolutePath().toString();
	protected static final String TEST_DATA_DIRECTORY = Paths.get(TEST_RESOURCES_DIRECTORY, "data").toAbsolutePath()
			.toString();
	protected static final String TEST_TEXT_DIRECTORY = Paths.get(TEST_RESOURCES_DIRECTORY, "text").toAbsolutePath()
			.toString();

	protected static final String ACTUAL_JSON_DIRECOTRY = Paths.get(TEST_TEXT_DIRECTORY, "actual").toAbsolutePath()
			.toString();

	protected static final Gson GSON = GsonUtility.getDefaultGson();
	protected static final Gson GSON_PRETTY = GsonUtility.getDefaultGson(true);

	private static final String TEST_DIR_REGEX = "<<<testDir>>>";
	private static final String BASE_DIR_REGEX = "<<<baseDir>>>";
	private static final String APP_ID_REGEX = "<<<appId>>>(.*?)<<</appId>>>";
	private static final String TEXT_REGEX = "<<<text>>>(.*?)<<</text>>>";

	private static final String TEXT_ENCODING = "UTF-8";

	private static final String DATA_EXTENSION = ".csv";
	private static final String IMPORT_EXTENSION = "_import.txt";
	private static final String METAMODEL_EXTENSION = "_metamodel.txt";

	private static final Path BASE_RDF_MAP = Paths.get(BASE_DIRECTORY, "RDF_Map.prop");
	private static final Path TEST_RDF_MAP = Paths.get(TEST_RESOURCES_DIRECTORY, "RDF_Map.prop");

	private static final Path TEST_LOCAL_DB_MAP = Paths.get(TEST_RESOURCES_DIRECTORY, "Local_DBs.prop");

	private static final String LS = System.getProperty("line.separator");

	protected static Map<String, String> aliasToAppId = new HashMap<>();

	private static boolean testDatabasesAreClean = true; // This must be static, as one instance of PixelUnit can dirty

	private static boolean firstTestRun = true;

	// databases for all others
	private List<String> cleanTestDatabases = new ArrayList<>();

	protected Insight insight = null;

	protected PyExecutorThread jep = null;

	//////////////////////////////////////////////////////////////////////
	// Before and after class
	//////////////////////////////////////////////////////////////////////

	// Allow initialization methods to throw exceptions, halting the rest of the
	// test cases
	@BeforeClass
	public static void initializeContext() {
		try {
			configureLog4j();
			loadDIHelper();
			loadDatabases();
			loadTestDatabases();
			cleanActualDirectory();
		} catch (Exception e) {

			// Tests assume that this setup is correct in order to run
			classLogger.error("Error: ", e);
			assumeNoException(e);
		}
		checkAssumptions();
	}



	// Destroy methods should not throw exceptions, so that subsequent cleanup
	// methods have the opportunity to run
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

		// Assume that Python must be enabled for tests to run, since we use deepdiff
		// for tests
		assumeThat(PyUtils.pyEnabled(), is(equalTo(true)));
	}

	//////////////////////////////
	// Log4j
	//////////////////////////////

	private static void configureLog4j() {
		String log4JPropFile = Paths.get(TEST_RESOURCES_DIRECTORY, "log4j.properties").toAbsolutePath().toString();
		FileInputStream fis = null;
		ConfigurationSource source = null;
		try {
			fis = new FileInputStream(log4JPropFile);
			source = new ConfigurationSource(fis);
			Configurator.initialize(null, source);
			classLogger.info("Successfully configured Log4j for JUnit suite.");
		} catch (IOException e) {
			classLogger.warn("Unable to initialize log4j for testing.", e);
		} finally {
			if(fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	//////////////////////////////
	// DIHelper
	//////////////////////////////

	private static void loadDIHelper() throws IOException {
		Files.copy(BASE_RDF_MAP, TEST_RDF_MAP, StandardCopyOption.REPLACE_EXISTING);
		Me configurationManager = new Me();
		configurationManager.changeRDFMap(BASE_DIRECTORY.replace('\\', '/'), "80",
				TEST_RDF_MAP.toAbsolutePath().toString());
		DIHelper.getInstance().loadCoreProp(TEST_RDF_MAP.toAbsolutePath().toString());

		// Just in case, manually override USE_PYTHON to be true for testing purposes
		// Warn if this was not the case to begin with
		if (!Boolean.parseBoolean(DIHelper.getInstance().getProperty(Constants.USE_PYTHON))) {
			classLogger.warn("Python must be functional for local testing.");
			Properties coreProps = DIHelper.getInstance().getCoreProp();
			coreProps.setProperty(Constants.USE_PYTHON, "true");
			DIHelper.getInstance().setCoreProp(coreProps);
		}

		//override use r to be true
		// set jri to false
		// use user rserve

		Properties corePropsR = DIHelper.getInstance().getCoreProp();
		corePropsR.setProperty(Constants.USE_R, "true");
		corePropsR.setProperty(Constants.R_CONNECTION_JRI, "true");
		corePropsR.setProperty("IS_USER_RSERVE", "false");
		corePropsR.setProperty("R_USER_CONNECTION_TYPE", "dedicated");
		DIHelper.getInstance().setCoreProp(corePropsR);


		// Turn tracking off while testing
		if (Boolean.parseBoolean(DIHelper.getInstance().getProperty(Constants.T_ON))) {
			classLogger.info("Setting tracking off during unit tests.");
			Properties coreProps = DIHelper.getInstance().getCoreProp();
			coreProps.setProperty(Constants.T_ON, "false");
			DIHelper.getInstance().setCoreProp(coreProps);
		}
	}

	private static void unloadDIHelper() {
		try {
			Files.delete(TEST_RDF_MAP);
		} catch (IOException e) {
			classLogger.warn("Unable to delete " + TEST_RDF_MAP, e);
		}
	}

	//////////////////////////////
	// Databases
	//////////////////////////////

	private static void loadDatabases() throws Exception {

		// Local master database
		SMSSWebWatcher.loadNewEngine(Constants.LOCAL_MASTER_DB + ".smss", BASE_DB_DIRECTORY);
		MasterDatabaseUtility.initLocalMaster();

		// Security
		SMSSWebWatcher.loadNewEngine(Constants.SECURITY_DB + ".smss", BASE_DB_DIRECTORY);
		AbstractSecurityUtils.loadSecurityDatabase();

		// Themes
		SMSSWebWatcher.loadNewEngine(Constants.THEMING_DB + ".smss", BASE_DB_DIRECTORY);
		AbstractThemeUtils.loadThemingDatabase();

		// Add local databases (as defined in Local_DBs.prop) to Properties so that users can run tests on their local db's
		Properties propMap = DIHelper.getInstance().getCoreProp();
		Properties localDBPropMap = Utility.loadProperties(TEST_LOCAL_DB_MAP.toString());
		localDBPropMap.forEach((key, value) -> propMap.put(key + "_STORE", BASE_DB_DIRECTORY + "\\" + value));
		DIHelper.getInstance().setCoreProp(propMap);

	}

	private static void unloadDatabases() {
		IDatabaseEngine formBuilder = Utility.getDatabase(AbstractFormBuilder.FORM_BUILDER_ENGINE_NAME);
		if (formBuilder != null) {
			try {
				formBuilder.close();
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}

		IDatabaseEngine localMaster = Utility.getDatabase(Constants.LOCAL_MASTER_DB);
		if (localMaster != null) {
			try {
				localMaster.close();
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}

		IDatabaseEngine security = Utility.getDatabase(Constants.SECURITY_DB);
		if (security != null) {
			try {
				security.close();
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}

		IDatabaseEngine themes = Utility.getDatabase(Constants.THEMING_DB);
		if (themes != null) {
			try {
				themes.close();
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}

		DIHelper.getInstance().removeLocalProperty(AbstractFormBuilder.FORM_BUILDER_ENGINE_NAME);
		DIHelper.getInstance().removeLocalProperty(Constants.LOCAL_MASTER_DB);
		DIHelper.getInstance().removeLocalProperty(Constants.SECURITY_DB);
		DIHelper.getInstance().removeLocalProperty(Constants.THEMING_DB);
	}

	//////////////////////////////
	// Test databases
	//////////////////////////////

	private static void loadTestDatabases()
			throws IOException, ClassNotFoundException, SQLException, DatabaseUnitException {

		// List all the files in the test database directory
		String[] fileNames = new File(TEST_DATA_DIRECTORY).list();

		// If there are corresponding csv, import, and metamodel files, then load the
		// test database
		// There is probably a more efficient way to do this but I can't think of it
		// right now
		List<String> dataNames = new ArrayList<>();
		List<String> importNames = new ArrayList<>();
		List<String> metamodelNames = new ArrayList<>();
		for (String fileName : fileNames) {
			if (fileName.endsWith(DATA_EXTENSION)) {
				dataNames.add(fileName.substring(0, fileName.length() - DATA_EXTENSION.length()));
			} else if (fileName.endsWith(IMPORT_EXTENSION)) {
				importNames.add(fileName.substring(0, fileName.length() - IMPORT_EXTENSION.length()));
			} else if (fileName.endsWith(METAMODEL_EXTENSION)) {
				metamodelNames.add(fileName.substring(0, fileName.length() - METAMODEL_EXTENSION.length()));
			}
		}
		for (String alias : dataNames) {
			if (importNames.contains(alias) & metamodelNames.contains(alias)) {
				loadTestDatabase(alias);
			} else {
				classLogger.warn("Cannot load " + alias + ", missing the corresponding import pixel or metamodel file.");
			}
		}
	}

	private static void unloadTestDatabases() {
		for (String alias : aliasToAppId.keySet()) {
			unloadTestDatabase(alias);
		}
	}

	private static void loadTestDatabase(String alias)
			throws IOException, ClassNotFoundException, SQLException, DatabaseUnitException {

		// First, delete any existing databases with the same alias
		List<String> existingAppIds = MasterDatabaseUtility.getDatabaseIdsForAlias(alias);
		DeleteFromMasterDB remover = new DeleteFromMasterDB();
		for (String appId : existingAppIds) {

			// First must catalog the db in order to call the getEngine
			SMSSWebWatcher.catalogEngine(alias + "__" + appId + ".smss", BASE_DB_DIRECTORY);

			IDatabaseEngine engine = Utility.getDatabase(appId);

			// If a full-fledged engine, then delete the entire app
			if (engine != null) {
				deleteApp(appId);
			} else {

				// Else, its just metadata hanging around
				remover.deleteEngineRDBMS(appId);
				SecurityEngineUtils.deleteEngine(appId);
			}
		}

		// Read in the import pixel and set the keys
		String importPixel = FileUtils
				.readFileToString(Paths.get(TEST_DATA_DIRECTORY, Utility.normalizePath(alias) + IMPORT_EXTENSION).toFile());

		// Run the pixel to import the data
		PixelRunner returnData = runPixelUtil(importPixel);
		@SuppressWarnings("unchecked")
		String appId = ((Map<String, String>) returnData.getResults().get(0).getValue()).get("app_id");

		// Store the appId
		aliasToAppId.put(alias, appId);

		// Save the original state of the database
		IDatabaseEngine engine = Utility.getDatabase(appId);
		if (isRelationalDatabase(engine)) {
			exportDatabaseToXml(alias);
		}
	}

	private static void unloadTestDatabase(String alias) {
		String appId = aliasToAppId.get(alias);
		if (appId != null) {

			// If relational, delete the corresponding xml file
			IDatabaseEngine engine = Utility.getDatabase(appId);
			if (isRelationalDatabase(engine)) {
				Path xmlFile = getXMLFileForAlias(alias);
				try {
					Files.delete(xmlFile);
				} catch (IOException e) {
					classLogger.warn("Unable to delete " + xmlFile, e);
				}
			}

			// Next, delete the app
			try {
				deleteApp(appId);
			} catch (IOException e) {
				classLogger.warn("Unable to app " + appId, e);
			}
		}
	}

	private static void deleteApp(String appId) throws IOException {
		String deletePixel = "DeleteApp(app=[\"" + appId + "\"] );";
		runPixelUtil(deletePixel);
	}

	private static void exportDatabaseToXml(String alias)
			throws IOException, ClassNotFoundException, SQLException, DatabaseUnitException {

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
				try (OutputStream stream = new FileOutputStream(
						getXMLFileForAlias(Utility.normalizePath(alias)).toAbsolutePath().toString())) {
					FlatXmlDataSet.write(fullDataSet, stream);
				}
			} finally {
				if (connection != null) {
					connection.close();
				}
			}
		}

	}
	
	private static void cleanActualDirectory() {
		File actualDir = new File(ACTUAL_JSON_DIRECOTRY);
		actualDir.mkdirs();
		try {
			FileUtils.cleanDirectory(new File(ACTUAL_JSON_DIRECOTRY));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
	}

	private static Path getXMLFileForAlias(String alias) {
		return Paths.get(TEST_RESOURCES_DIRECTORY, Utility.normalizePath(alias) + ".xml");
	}

	private static PixelRunner runPixelUtil(String pixel) throws IOException {
		pixel = formatString(pixel);
		long start = System.currentTimeMillis();
		PixelRunner returnData = new Insight().runPixel(pixel);
		classLogger.info(GSON.toJson(returnData.getResults()));
		long end = System.currentTimeMillis();
		classLogger.info("Execution time : " + (end - start) + " ms");
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
			classLogger.warn("Unable to shutdown scheduler.", e);
		}

		// Close r
		RJavaTranslatorFactory.stopRConnection();
	}

	//////////////////////////////////////////////////////////////////////
	// Before and after each test method
	//////////////////////////////////////////////////////////////////////

	@Before
	public void initializeTest() {
		initializeTest(true);
	}

	public void initializeTest(boolean checkAssumptions) {
		initializeInsight();
		initializeJep();
		cleanTestDatabases = new ArrayList<>(); // Reset the list of databases to clean
		if (checkAssumptions) {
			checkTestAssumptions();
		}
	}

	@After
	public void destroyTest() {
		destroyInsight();
		destroyJep();
		cleanTestDatabases();
	}

	//////////////////////////////
	// Assumptions
	//////////////////////////////
	public void checkTestAssumptions() {
		
		if(firstTestRun){
		// Assume that the databases are clean for tests to work properly
		assumeThat(testDatabasesAreClean, is(equalTo(true)));

		// Assume that Python is returning valid differences
		try {
			PixelComparison result = compareResult("Version();",
					"{\"pixelExpression\":\"Version ( ) ;\",\"output\":{\"datetime\":\"1000-01-01 01:01:01\",\"version\":\"1.0.0-SNAPSHOT\"}}")
					.get(0);

			// Assume that we are getting values changed for this example
			String valuesChangedKey = "values_changed";
			Map<String, Object> differences = result.getDifferences();
			assumeThat(differences.containsKey(valuesChangedKey), is(equalTo(true)));

			// Assume that the values changed are root['datetime'] and root['version']
			@SuppressWarnings("unchecked")
			Map<String, Object> valuesChanged = (HashMap<String, Object>) differences.get(valuesChangedKey);
			assumeThat(valuesChanged.containsKey("root['datetime']"), is(equalTo(true)));
			assumeThat(valuesChanged.containsKey("root['version']"), is(equalTo(true)));

			// Assume these are the only two differences
			assumeThat(valuesChanged.size(), is(equalTo(2)));
		} catch (IOException e) {
			classLogger.error("Error: ", e);
			assumeNoException(e);
		}

		// Assume that database metamodels are correct
		for (Entry<String, String> entry : aliasToAppId.entrySet()) {
			String alias = entry.getKey();
			String appId = entry.getValue();
			String pixel = "GetDatabaseMetamodel(database=[\"" + appId + "\"], options=[\"datatypes\"]);";
			try {
				String expectedJson = FileUtils
						.readFileToString(Paths.get(TEST_DATA_DIRECTORY, alias + METAMODEL_EXTENSION).toFile());
				testPixel(pixel, expectedJson, false, new ArrayList<String>(), true, true, false, true);
			} catch (IOException e) {
				classLogger.error("Error: ", e);
				assumeNoException(e);
			}
		}
		firstTestRun=false;
		}
	}

	//////////////////////////////
	// Insight
	//////////////////////////////

	protected void initializeInsight() {
		insight = new Insight();
		//insight.setPy(PyUtils.getInstance().getJep());
		//InsightStore.getInstance().put(insight);
	}

	protected void destroyInsight() {
		if (insight != null) {
			InsightStore.getInstance().remove(insight.getInsightId());
		}
		insight = null;
	}

	//////////////////////////////
	// Python
	//////////////////////////////

	protected void initializeJep() {
		User user = new User();
		//user.setAnonymous(true);
		String uId = "SemossTester";
		//user.setAnonymousId(uId);
		AccessToken token = new AccessToken();
		token.setId(uId);
		token.setName(uId);
		token.setProvider(AuthProvider.NATIVE);
		user.setAccessToken(token);
		insight.setUser(user);
		//String tempTupleSpace = PyUtils.getInstance().getTempTupleSpace(user, DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR));
		//insight.setTupleSpace(tempTupleSpace);
		jep = PyUtils.getInstance().getJep();
		insight.setPy(jep);
		InsightStore.getInstance().put(insight);

		// Wait for Python to load
		classLogger.info("Waiting for python to initialize...");
		long start = System.currentTimeMillis();
		boolean timeout = false;
		while (!jep.isReady() && !timeout) {
			timeout = (System.currentTimeMillis() - start) > 3000000;
		}

		// Assume that Python did not timeout for tests to run
		assumeThat(timeout, is(not(equalTo(true))));
		classLogger.info("Python has initialized.");

	}

	protected void destroyJep() {
		PyUtils.getInstance().killPyThread(jep);
		jep = null;
	}

	//////////////////////////////
	// Test databases
	//////////////////////////////

	// https://www.marcphilipp.de/blog/2012/03/13/database-tests-with-dbunit-part-1/
	protected void cleanTestDatabases() {
		for (String alias : cleanTestDatabases) {
			String appId = aliasToAppId.get(alias);
			if (appId != null) {
				IDatabaseEngine engine = Utility.getDatabase(appId);
				if (isRelationalDatabase(engine)) {
					// Cast to RDBMS to grab the connection details
					RDBMSNativeEngine rdbms = (RDBMSNativeEngine) engine;
					try {
						String connectionUrl = rdbms.getConnectionMetadata().getURL();
						String driver = rdbms.getDbType().getDriver();

						// Close the db
						rdbms.close();
						// Clean insert
						IDataSet dataSet = new FlatXmlDataSetBuilder()
								.build(new File(getXMLFileForAlias(Utility.normalizePath(alias)).toAbsolutePath().toString()));
						IDatabaseTester databaseTester = new JdbcDatabaseTester(driver, connectionUrl, "sa", "");
						databaseTester.setSetUpOperation(DatabaseOperation.CLEAN_INSERT);
						databaseTester.setDataSet(dataSet);
						databaseTester.onSetup();
						classLogger.info("Cleaned: " + alias);
					} catch (Exception e) {
						classLogger.warn("Cannot clean database with the alias " + alias + ", an exception has occurred.",
								e);
						testDatabasesAreClean = false;
					}
				} else {
					classLogger.warn("Cannot clean database with the alias " + alias + ", database is not an RDBMS.");
					testDatabasesAreClean = false;
				}
			} else {
				classLogger.warn("Cannot clean database with the alias " + alias + ", database not found.");
				testDatabasesAreClean = false;
			}
		}
	}

	protected void setCleanTestDatabases(List<String> cleanTestDatabases) {
		this.cleanTestDatabases = cleanTestDatabases;
	}

	private static boolean isRelationalDatabase(IDatabaseEngine engine) {
		return engine != null && engine.getDatabaseType() == IDatabaseEngine.DATABASE_TYPE.RDBMS;
	}

	//////////////////////////////////////////////////////////////////////
	// Methods for use during testing (by subclasses)
	//////////////////////////////////////////////////////////////////////

	//////////////////////////////
	// Pixel
	//////////////////////////////

	// Test pixel methods (overloaded)
	protected void testPixel(String pixel, String expectedJson) {
		testPixel(pixel, expectedJson, false, new ArrayList<String>(), false, false, false);
	}

	protected void testPixel(String pixel, String expectedJson, boolean compareAll, List<String> excludePaths,
			boolean ignoreOrder, boolean ignoreAddedDictionary, boolean ignoreAddedIterable) {
		testPixel(pixel, expectedJson, compareAll, excludePaths, ignoreOrder, ignoreAddedDictionary,
				ignoreAddedIterable, false);
	}

	protected void testPixel(String pixel, String expectedJson, boolean compareAll, List<String> excludePaths,
			boolean ignoreOrder, boolean ignoreAddedDictionary, boolean ignoreAddedIterable, boolean assume) {
		try {
			List<PixelComparison> result = compareResult(pixel, expectedJson, compareAll, excludePaths, ignoreOrder);

			// Compose the full report
			StringBuilder testReport = new StringBuilder();
			int index = 0;
			boolean testFailed = false;
			for (PixelComparison comparison : result) {
				if (comparison.isDifferent(ignoreAddedDictionary, ignoreAddedIterable)) {
					testFailed = true;
					testReport.append(composeComparisonReport(comparison, index)).append(LS);
				}
				index++;
			}

			// Throw the report if the test failed
			if (testFailed) {
				if (assume) {
					throw new AssumptionViolatedException(testReport.toString());
				} else {
					//createUpdatedJson(pixel, compareAll, expectedJson);
					throw new AssertionError(testReport.toString());
				}
			}
		} catch (AssumptionViolatedException | AssertionError e) {
			throw e;
		} catch (IOException e) {
			classLogger.error("Error: ", e);
			String message = "An I/O exception occured while running this test.";
			if (assume) {
				throw new AssumptionViolatedException(message, e);
			} else {
				throw new AssertionError(message, e);
			}
		} catch (RuntimeException e) {
			classLogger.error("Error: ", e);

			System.out.println("####ERRORED IN TEST PIXEL: ");

			// Try to report the actual output of the pixel recipe
			try {

				// Reset the test state and rerun the pixel
				destroyTest();
				initializeTest(false);

				JsonElement actual;
				PixelRunner returnData = runPixel(pixel);
				JsonArray all = getPixelReturns(returnData);
				if (compareAll) {
					actual = all;
				} else {
					actual = all.get(all.size() - 1);
				}

				String message = "An exception occured while running this test. " + LS
						+ "For your reference, the actual JSON output of the pixel recipe is as follows: " + LS
						+ GSON_PRETTY.toJson(actual);
				if (assume) {
					throw new AssumptionViolatedException(message, e);
				} else {
					throw new AssertionError(message, e);
				}
			} catch (IOException e1) {
				classLogger.warn("Failed to get the actual JSON.", e1);
			}

			// Otherwise just throw something generic
			String message = "An exception occured while running this test.";
			if (assume) {
				throw new AssumptionViolatedException(message, e);
			} else {
				throw new AssertionError(message, e);
			}
		}
	}



		private static String composeComparisonReport(PixelComparison comparison, int index) {
			StringBuilder testReport = new StringBuilder();
			testReport.append(LS);

			addMajorTitle(testReport, "Pixel expression: ");
			testReport.append(comparison.getPixelExpression()).append(LS);

			addTitle(testReport, "Index: ");
			testReport.append(index).append(LS);

			addTitle(testReport, "Differences: ");
			// Need to rearrange the way the differences are reported
			Map<String, Object> typeLocationValueMap = comparison.getDifferences(); // Original
			Map<String, Map<String, String>> locationTypeValueMap = new HashMap<>(); // Rearranged
			for (Entry<String, Object> typeLocationValueEntry : typeLocationValueMap.entrySet()) {

				// Handle the case where the value is not a map
				if (!(typeLocationValueEntry.getValue() instanceof Map)) {
					String location = typeLocationValueEntry.getValue().toString();
					if (location.startsWith("{\""))
						location = location.substring(2, location.length());
					if (location.endsWith("\"}"))
						location = location.substring(0, location.length() - 2);
					testReport.append(location).append(": ").append(LS);

					String type = typeLocationValueEntry.getKey();
					testReport.append(type).append(LS);

					continue;
				}

				// If a map, report all the differences in the map
				@SuppressWarnings("unchecked")
				Map<String, Object> locationValueMap = (Map<String, Object>) typeLocationValueEntry.getValue();
				for (Entry<String, Object> locationValueEntry : locationValueMap.entrySet()) {

					String type = typeLocationValueEntry.getKey();
					String location = locationValueEntry.getKey();
					String value = locationValueEntry.getValue().toString();

					Map<String, String> typeValueMap = locationTypeValueMap.getOrDefault(location, new HashMap<>());
					typeValueMap.put(type, value);
					locationTypeValueMap.put(location, typeValueMap);
				}
			}

			// Then report the results
			for (Entry<String, Map<String, String>> locationTypeValueEntry : locationTypeValueMap.entrySet()) {

				String location = locationTypeValueEntry.getKey();
				testReport.append(location).append(": ").append(LS);

				Map<String, String> typeValueMap = locationTypeValueEntry.getValue();

				// Underline to highlight the difference
				String underline = null;
				if (typeValueMap.keySet().size() == 2) {
					String[] values = typeValueMap.values().toArray(new String[] {});
					String v0 = values[0];
					String v1 = values[1];
					int n = getIndexOfFirstDifference(v0, v1);
					underline = new String(new char[n]).replace('\0', ' ') + "^";
				}

				for (Entry<String, String> typeValueEntry : typeValueMap.entrySet()) {

					String type = typeValueEntry.getKey();
					String value = typeValueEntry.getValue();

					testReport.append(type).append("=").append(LS);
					testReport.append(value).append(LS);
					if (underline != null)
						testReport.append(underline).append(LS); // Only underline if available
				}
				testReport.append(LS);
			}

			addTitle(testReport, "Expected output: ");
			testReport.append(comparison.getExpectedPixelOutput()).append(LS);
			addTitle(testReport, "Actual output: ");
			testReport.append(comparison.getActualPixelOutput()).append(LS);

			return testReport.toString();
		}

		private static int getIndexOfFirstDifference(String a, String b) {
			int min = Math.min(a.length(), b.length());
			for (int i = 0; i < min; i++) {
				if (a.charAt(i) != b.charAt(i)) {
					return i;
				}
			}
			return -1;
		}

		private static void addTitle(StringBuilder builder, String title) {
			builder.append(LS);
			builder.append("---------------------------------------").append(LS);
			builder.append(title).append(LS);
			builder.append("---------------------------------------").append(LS);
		}

		private static void addMajorTitle(StringBuilder builder, String title) {
			builder.append(LS);
			builder.append("-------------------------------------------------------------------------------").append(LS);
			builder.append(title).append(LS);
			builder.append("-------------------------------------------------------------------------------").append(LS);
		}

		// Compare result methods (overloaded)
		protected List<PixelComparison> compareResult(String pixel, String expectedJson) throws IOException {
			return compareResult(pixel, expectedJson, false, new ArrayList<String>(), false);
		}

		protected List<PixelComparison> compareResult(String pixel, String expectedJson, boolean compareAll,
				List<String> excludePaths, boolean ignoreOrder) throws IOException {

			// Run the pixel and get the results
			PixelRunner returnData = runPixel(pixel);
			List<PixelJson> actualPixelJsons = collectPixelJsons(returnData);
			int actualRecipeLength = actualPixelJsons.size();
			String testName = getNameFromExpectedJson(expectedJson);
			// Cleanup the expected json, including formatting it
			expectedJson = formatString(expectedJson);

			// The rest of the comparison is based on whether to compare all pixel returns
			// or not
			List<PixelComparison> differences = new ArrayList<>();
			if (compareAll) {

				// Assume the expectedJson is in the form of an array
				JsonArray expectedJsonArray = new JsonParser().parse(expectedJson).getAsJsonArray();
				List<PixelJson> expectedPixelJsons = collectPixelJsons(expectedJsonArray);

				// The expected and actual arrays must be the same length to perform a valid
				// comparison
				if (actualPixelJsons.size() != expectedPixelJsons.size())
					throw new IllegalArgumentException(
							"Unable to compare the results; the actual and expected json arrays differ in length.");

				// Loop through and compare all
				JsonArray actualJsonArray  = new JsonArray();
				boolean differenceExist = false;
				for (int i = 0; i < actualRecipeLength; i++) {

					// Expected
					PixelJson expectedPixelJson = expectedPixelJsons.get(i);

					// Actual
					PixelJson actualPixelJson = actualPixelJsons.get(i);
					actualJsonArray.add(actualPixelJson.getJson());
					// Difference
					PixelComparison pixelDifference = new PixelComparison(expectedPixelJson, actualPixelJson, excludePaths,
							ignoreOrder, this);
					if (pixelDifference.isDifferent()) {
						differences.add(pixelDifference);
						differenceExist = true;
					}
					if(differenceExist) {
						try (Writer writer = new FileWriter(ACTUAL_JSON_DIRECOTRY + File.separatorChar + testName)) {
							GSON_PRETTY.toJson(actualJsonArray, writer);
						}
					}
				}
			} else {

				// Assume that the expectedJson is in the form of an object
				JsonObject expectedJsonObject = new JsonParser().parse(expectedJson).getAsJsonObject();

				// Expected
				PixelJson expectedPixelJson = new PixelJson(expectedJsonObject);

				// Actual
				PixelJson actualPixelJson = actualPixelJsons.get(actualRecipeLength - 1);

				// This is simple, just compare the one expected pixel json with the last actual
				// pixel json
				PixelComparison pixelDifference = new PixelComparison(expectedPixelJson, actualPixelJson, excludePaths,
						ignoreOrder, this);
				if (pixelDifference.isDifferent()) {
					differences.add(pixelDifference);
				JsonObject x = actualPixelJson.getJson();
					try (Writer writer = new FileWriter(ACTUAL_JSON_DIRECOTRY + File.separatorChar + testName)) {
						GSON_PRETTY.toJson(x, writer);
					}
				}
			}

			// Return
			return differences;
		}

		protected static List<PixelJson> collectPixelJsons(PixelRunner returnData) throws IOException {
			return collectPixelJsons(getPixelReturns(returnData));
		}

		protected static List<PixelJson> collectPixelJsons(JsonArray pixelReturns) throws IOException {
			List<PixelJson> pixelJsons = new ArrayList<>();
			for (int i = 0; i < pixelReturns.size(); i++) {
				JsonObject pixelReturn = pixelReturns.get(i).getAsJsonObject();
				pixelJsons.add(new PixelJson(pixelReturn));
			}
			return pixelJsons;
		}

		protected static JsonArray getPixelReturns(PixelRunner data) throws IOException {
			StreamingOutput so = PixelStreamUtility.collectPixelData(data, null);
			try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
				so.write(output);
				String jsonString = new String(output.toByteArray(), TEXT_ENCODING);
				JsonObject jsonObject = new JsonParser().parse(jsonString).getAsJsonObject();
				JsonArray pixelReturns = jsonObject.get("pixelReturn").getAsJsonArray();
				JsonArray cleanedPixelReturns = new JsonArray();
				for (int i = 0; i < pixelReturns.size(); i++) {
					JsonObject pixelReturn = pixelReturns.get(i).getAsJsonObject();
					JsonObject cleanedPixelReturn = new JsonObject();
					cleanedPixelReturn.add("pixelExpression", pixelReturn.get("pixelExpression"));
					cleanedPixelReturn.add("output", pixelReturn.get("output"));
					cleanedPixelReturns.add(cleanedPixelReturn);
				}
				return cleanedPixelReturns;
			}
		}

		@SuppressWarnings("unchecked")
		public Map<String, Object> deepDiff(String expectedJson, String actualJson, List<String> excludePaths,
				boolean ignoreOrder) throws IOException {

			// Only allow ASCII characters
			expectedJson = expectedJson.replaceAll("[^\\p{ASCII}]", "?");
			actualJson = actualJson.replaceAll("[^\\p{ASCII}]", "?");

			// Write temp files
			String random = Utility.getRandomString(10);
			File expectedJsonFile = Paths.get(TEST_RESOURCES_DIRECTORY, "expected__" + random + ".json").toFile();
			File actualJsonFile = Paths.get(TEST_RESOURCES_DIRECTORY, "actual__" + random + ".json").toFile();
			try {
				FileUtils.writeStringToFile(expectedJsonFile, expectedJson, TEXT_ENCODING);
				FileUtils.writeStringToFile(actualJsonFile, actualJson, TEXT_ENCODING);

				// The Python script to compare the deep difference
				String ignoreOrderString = ignoreOrder ? "True" : "False";
				String ddiffCommand = (excludePaths.size() > 0)
						? "DeepDiff(a, b, ignore_order=" + ignoreOrderString + ", exclude_paths={\""
						+ String.join("\", \"", excludePaths) + "\"})"
						: "DeepDiff(a, b, ignore_order=" + ignoreOrderString + ")";

				String[] script = new String[] { "import json", "from deepdiff import DeepDiff",
						"with open('" + expectedJsonFile.getAbsolutePath().replace('\\', '/') + "', encoding='"
								+ TEXT_ENCODING.toLowerCase() + "') as f:\n" + "    a = json.load(f)",
								"with open('" + actualJsonFile.getAbsolutePath().replace('\\', '/') + "', encoding='"
										+ TEXT_ENCODING.toLowerCase() + "') as f:\n" + "    b = json.load(f)",
										ddiffCommand };

				Hashtable<String, Object> results = runPy(script);
				Object result = results.get(ddiffCommand);

				// Make sure there is no difference, ignoring order
				classLogger.debug("EXPECTED: " + expectedJson);
				classLogger.debug("ACTUAL:   " + actualJson);
				classLogger.info("DIFF:     " + result);

				return (Map<String, Object>) result;
			} finally {
				expectedJsonFile.delete();
				actualJsonFile.delete();
			}
		}

		protected PixelRunner runPixel(String pixel) throws IOException {
			pixel = formatString(pixel);
			long start = System.currentTimeMillis();
			PixelRunner returnData = insight.runPixel(pixel);		
			long end = System.currentTimeMillis();
			classLogger.info("Execution time : " + (end - start) + " ms");
			return returnData;
		}

		protected static String formatString(String string) throws IOException {
			Pattern textPattern = Pattern.compile(TEXT_REGEX);
			Matcher textMatcher = textPattern.matcher(string);
			if (textMatcher.matches()) {
				String file = textMatcher.group(1);
				string = FileUtils.readFileToString(Paths.get(TEST_TEXT_DIRECTORY, Utility.normalizePath(file)).toFile(), TEXT_ENCODING);
			}

			string = string.replaceAll(TEST_DIR_REGEX,
					Paths.get(TEST_RESOURCES_DIRECTORY).toAbsolutePath().toString().replace('\\', '/'));
			string = string.replaceAll(BASE_DIR_REGEX,
					Paths.get(BASE_DIRECTORY).toAbsolutePath().toString().replace('\\', '/'));

			Pattern appIdPattern = Pattern.compile(APP_ID_REGEX);
			Matcher appIdMatcher = appIdPattern.matcher(string);
			while (appIdMatcher.find()) {
				String alias = appIdMatcher.group(1);
				String appId = aliasToAppId.containsKey(alias) ? aliasToAppId.get(alias) : "null";
				string = appIdMatcher.replaceFirst(appId);
				appIdMatcher = appIdPattern.matcher(string);
			}

			return string;
		}

		//////////////////////////////
		// Python
		//////////////////////////////

		// Method for running a single Python command
		protected Object runPy(String script) {
			return runPy(new String[] { script }).get(script);
		}

		// Method for running multiple python commands
		protected Hashtable<String, Object> runPy(String... script) {
			// Try running deepdiff using a new jep thread...
			//	Insight insightDeepDiff = new Insight();
			//    User user = new User();
			//  user.setAnonymous(true);
			//  String uId = "UNK_" + UUID.randomUUID().toString();
			//  user.setAnonymousId(uId);
			//   insightDeepDiff.setUser(user);
			//      String tempTupleSpace = PyUtils.getInstance().getTempTupleSpace(user, DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR));
			//      insight.setTupleSpace(tempTupleSpace);
			jep = PyUtils.getInstance().getJep();
			//   insightDeepDiff.setPy(jep);
			// InsightStore.getInstance().put(insight);
			// Wait for Python to load
			classLogger.info("Waiting for python to initialize...");
			long start = System.currentTimeMillis();
			boolean timeout = false;
			while (!jep.isReady() && !timeout) {
				timeout = (System.currentTimeMillis() - start) > 3000000;
			}

			// Set the commands
			jep.command = script;

			// Tell the thread to execute its commands
			Hashtable<String, Object> response = null;
			Object jepMonitor = jep.getMonitor();
			synchronized (jepMonitor) {
				jepMonitor.notifyAll();
				try {

					// Wait for the commands to finish execution, but abort after 30s
					jepMonitor.wait(30000);
					response = jep.response;
				} catch (InterruptedException e) {
					classLogger.error("The following Python script was interrupted: " + String.join("\n", script), e);
				}
			}
			return response;
		}
		
		public static String getNameFromExpectedJson(String expectedJson){
			String jsonFileName = null;
			if (expectedJson != null && !expectedJson.isEmpty()) {
				String modifiedJsonPath = expectedJson.replaceAll("<<<text>>>", "");
				jsonFileName = modifiedJsonPath.replaceAll("<<</text>>>", "");
			} else {
				jsonFileName = Utility.getRandomString(8) + ".json";
			}
			return jsonFileName;
			
		}

		// Method for running multiple python commands
		//	protected Hashtable<String, Object> runPy(String... script) {
		//
		//		// Set the commands
		//		jep.command = script;
		//
		//		// Tell the thread to execute its commands
		//		Hashtable<String, Object> response = null;
		//		Object jepMonitor = jep.getMonitor();
		//		synchronized (jepMonitor) {
		//			jepMonitor.notifyAll();
		//			try {
		//
		//				// Wait for the commands to finish execution, but abort after 30s
		//				jepMonitor.wait(30000);
		//				response = jep.response;
		//			} catch (InterruptedException e) {
		//				classLogger.error("The following Python script was interrupted: " + String.join("\n", script), e);
		//			}
		//		}
		//		return response;
		//	}

	}
