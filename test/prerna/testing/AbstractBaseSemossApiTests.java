package prerna.testing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

public abstract class AbstractBaseSemossApiTests {

	private static final Logger classLogger = LogManager.getLogger(AbstractBaseSemossApiTests.class);

	protected boolean clearAllDatabasesBetweenTests = true;
	protected boolean clearAllEmailsBetweenTests = true;

	@BeforeAll
	public static void initialSetup() throws Exception {
		long start = System.nanoTime();
		if (ApiSemossTestUtils.isFirstClass()) {
			classLogger.info("Log check");
			classLogger.info("INFO");
			classLogger.debug("DEBUG");
			classLogger.warn("WARN");
			classLogger.error("ERROR");
			classLogger.fatal("FATAL");
			classLogger.info("Log check end");

			ApiSemossTestSetupUtils.ensureTestFolderStructure();

			ApiSemossTestPropsUtils.loadDIHelper();

			// moved this to the before because its hard to delete databases before each
			// test due to database being in use
			ApiSemossTestInsightUtils.initializeInsight();

			ApiSemossTestSetupUtils.setup(true);

			ApiSemossTestEngineUtils.createUser(ApiTestsSemossConstants.USER_NAME, ApiTestsSemossConstants.USER_EMAIL,
					"Native", true);
		}
		classLogger.info("Semoss Before All Time: " + (System.nanoTime() - start) / 1000000000);
	}

	@AfterAll
	public static void destroyContext() {
//		ApiInsightAndPropsInitUtils.unloadDIHelper();
//		ApiDatabaseInitUtils.unloadDatabases();
//		ApiInsightAndPropsInitUtils.unloadSocialProps();
	}

	// Ensure that everything is pointing in the correct direction before each test
	// to limit damage
	// in case the DIHelper decides to reload with a different rdf map properties.
	@BeforeEach
	public void beforeEachTest() throws Exception {
		ApiSemossTestEngineUtils.checkDatabasePropMapping();

		// do we want a clean database
		if (clearAllDatabasesBetweenTests) {
			ApiSemossTestEngineUtils.clearNonCoreDBs();
			ApiSemossTestEngineUtils.deleteAllDataAndAddUser();
		}

		// do we want a clean email server
		if (clearAllEmailsBetweenTests) {
			ApiSemossTestEmailUtils.deleteAllEmails();
		}

		ApiSemossTestProjectUtils.clearNonCoreProjects();

		ApiSemossTestUserUtils.setDefaultTestUser();

		ApiSemossTestInsightUtils.clearInsightCacheDifferently();
	}

}
