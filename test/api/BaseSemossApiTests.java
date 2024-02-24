package api;

import static org.junit.Assert.fail;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.google.gson.Gson;

import prerna.auth.User;
import prerna.ds.py.PyExecutorThread;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.gson.GsonUtility;

public class BaseSemossApiTests {
	
	protected static final Logger classLogger = LogManager.getLogger(BaseSemossApiTests.class);

	protected static final String BASE_DIRECTORY = new File("").getAbsolutePath();
	protected static final String TEST_BASE_DIRECTORY = new File("testfolder").getAbsolutePath();
	
	protected static final String TEST_DB_DIRECTORY = Paths.get(TEST_BASE_DIRECTORY, "db").toAbsolutePath().toString();
	protected static final String TEST_PROJECT_DIRECTORY = Paths.get(TEST_BASE_DIRECTORY, "project").toAbsolutePath().toString();
	protected static final String TEST_RESOURCES_DIRECTORY = Paths.get(BASE_DIRECTORY, "test", "resources", "api")
			.toAbsolutePath().toString();
	protected static final Path TEST_CONFIG_DIRECTORY = Paths.get(TEST_BASE_DIRECTORY, "testconfig");

	protected static final Gson GSON = GsonUtility.getDefaultGson();
	protected static final Gson GSON_PRETTY = GsonUtility.getDefaultGson(true);


	static final Path BASE_RDF_MAP = Paths.get(BASE_DIRECTORY, "RDF_Map.prop");
	static final Path TEST_RDF_MAP = Paths.get(TEST_RESOURCES_DIRECTORY, "RDF_Map.prop");
	
	public static Path TEST_INSIGHT_CACHE;
	
	// paths to smss files for testing
	static final String LMD_SMSS = Paths.get(TEST_DB_DIRECTORY, Constants.LOCAL_MASTER_DB + ".smss").toAbsolutePath().toString();
	static final String SECURITY_SMSS = Paths.get(TEST_DB_DIRECTORY, Constants.SECURITY_DB + ".smss").toAbsolutePath().toString();
	static final String SCHEDULER_SMSS = Paths.get(TEST_DB_DIRECTORY, Constants.SCHEDULER_DB + ".smss").toAbsolutePath().toString();
	static final String THEMES_SMSS = Paths.get(TEST_DB_DIRECTORY, Constants.THEMING_DB + ".smss").toAbsolutePath().toString();
	static final String UTDB_SMSS = Paths.get(TEST_DB_DIRECTORY, Constants.USER_TRACKING_DB + ".smss").toAbsolutePath().toString();
	
	protected static Map<String, String> aliasToAppId = new HashMap<>();

	public static Insight insight = null;
	protected static User user = null;

	protected PyExecutorThread jep = null;
	
	private static Boolean USING_DOCKER = null;
	
	private static boolean FIRST_CLASS_RUN = true;
	
    @BeforeClass
    public static void apiTestsSetup() throws Exception {
    	if (FIRST_CLASS_RUN) {
    		FIRST_CLASS_RUN = false;

			ApiSemossTestPropsUtils.loadDIHelper();
			if (runningOnServer()) {
				ApiSemossTestEmailUtils.startEmailDockerContainer();
			} else {
				ApiSemossTestEmailUtils.startEmailLocalServer();
			}
			
			// moved this to the before because its hard to delete databases before each test due to database being in use
			try {
				ApiSemossTestEngineUtils.clearNonCoreDBs();
			} catch (Exception e) {
				System.out.println("Could not clear DBS that are not semoss/project specific. ");
				fail(e.toString());
			}
			
			try {
				ApiSemossTestProjectUtils.clearNonCoreProjects();
			} catch (Exception e) {
				System.out.println("Could not clear DBS that are not semoss/project specific. ");
				fail(e.toString());
			}

			ApiSemossTestInsightUtils.initializeInsight();
			ApiSemossTestEngineUtils.initalizeDatabases();
    	}
    }
    
    @AfterClass
	public static void destroyContext() {
//		ApiInsightAndPropsInitUtils.unloadDIHelper();
//		ApiDatabaseInitUtils.unloadDatabases();
//		ApiInsightAndPropsInitUtils.unloadSocialProps();
	}
    

	// Ensure that everything is pointing in the correct direction before each test to limit damage
    // in case the DIHelper decides to reload with a different rdf map properties. 
    @Before
    public void apiTestsBefore() {
    	ApiSemossTestEngineUtils.checkDatabasePropMapping();
    	
    	try {
    		ApiSemossTestEngineUtils.deleteAllDataAndAddUser();
    	} catch (Exception e) {
    		System.out.println("Could not rebuild core semoss dbs");
    		fail(e.toString());
    	}
    	
    	ApiSemossTestUserUtils.setDefaultTestUser();
    	
    	ApiSemossTestEmailUtils.deleteAllEmails();
    	ApiSemossTestInsightUtils.clearInsightCacheDifferently();
    }
    
	
	public static boolean runningOnServer() {
		if (USING_DOCKER != null) {
			return USING_DOCKER;
		}
		
		Boolean docker = Boolean.valueOf(DIHelper.getInstance().getProperty("USE_DOCKER"));
		
		if (docker == null) {
			docker = false;
		}
		
		USING_DOCKER = docker;
		
		return docker;
	}
	
}
