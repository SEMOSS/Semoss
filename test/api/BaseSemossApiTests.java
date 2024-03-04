package api;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class BaseSemossApiTests {
	
	protected static final Logger classLogger = LogManager.getLogger(BaseSemossApiTests.class);

    @BeforeClass
    public static void BaseSemossApiTestsSetup() throws Exception {
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
			} finally {
				es.shutdown();
			}
			
			ApiSemossTestEngineUtils.createUser("ater", "Native", true);
    	}
    	System.out.println("Semoss Before All Time: " + (System.nanoTime() - start) / 1000000000);
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
    public void BaseSemossApiTestsBefore() {
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
    
	
	
	
}
