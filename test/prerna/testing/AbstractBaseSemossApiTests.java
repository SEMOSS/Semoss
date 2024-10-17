package prerna.testing;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
			ApiSemossTestPropsUtils.loadDIHelper();
			
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
			
			List<Callable<Void>> tasks = new ArrayList<>();
			if (ApiSemossTestUtils.usingDocker()) {
				tasks.add(ApiSemossTestEmailUtils::startEmailDockerContainer);
			} else {
				tasks.add(ApiSemossTestEmailUtils::startEmailLocalServer);
			}
			ApiSemossTestEngineUtils.addDBStartupTasks(tasks);
			
			ExecutorService es = Executors.newCachedThreadPool();
			try {
				es.invokeAll(tasks);
			} catch (Exception e) {
				e.printStackTrace();
				fail("setup failed");
			} finally {
				es.shutdown();
			}
			
			ApiSemossTestEngineUtils.createUser(ApiTestsSemossConstants.USER_NAME, ApiTestsSemossConstants.USER_EMAIL, "Native", true);
    	}
    	System.out.println("Semoss Before All Time: " + (System.nanoTime() - start) / 1000000000);
    }
    
    @AfterAll
	public static void destroyContext() {
//		ApiInsightAndPropsInitUtils.unloadDIHelper();
//		ApiDatabaseInitUtils.unloadDatabases();
//		ApiInsightAndPropsInitUtils.unloadSocialProps();
	}
    

	// Ensure that everything is pointing in the correct direction before each test to limit damage
    // in case the DIHelper decides to reload with a different rdf map properties. 
    @BeforeEach
    public void beforeEachTest() {
    	ApiSemossTestEngineUtils.checkDatabasePropMapping();
    	
    	// do we want a clean database
    	if(clearAllDatabasesBetweenTests) {
	    	try {
	    		ApiSemossTestEngineUtils.deleteAllDataAndAddUser();
	    	} catch (Exception e) {
	    		System.out.println("Could not rebuild core semoss dbs");
	    		fail(e.toString());
	    	}
	    	ApiSemossTestUserUtils.setDefaultTestUser();
    	}
    	
    	// do we want a clean email server
    	if(clearAllEmailsBetweenTests) {
    		ApiSemossTestEmailUtils.deleteAllEmails();
    	}
    	
    	ApiSemossTestInsightUtils.clearInsight();
    }
    
}
