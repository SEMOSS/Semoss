/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.testing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import prerna.auth.AuthProvider;

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
			ApiSemossTestUserUtils.clearUserDirectory();

			ApiSemossTestSetupUtils.setup(false);

			ApiSemossTestEngineUtils.createUser(ApiTestsSemossConstants.USER_NAME, ApiTestsSemossConstants.USER_EMAIL,
					AuthProvider.NATIVE.toString(), true);

			
			ApiRTestUtils.setup();
			ApiPyTestUtils.setupAndCheck();
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
	
	@AfterEach
	public void resetBeforeNextTest(TestInfo ti) {
		String testClass = "";
		if (ti.getTestClass().isPresent()) {
			testClass = ti.getTestClass().get().getCanonicalName();
		}
		
		String testMethod = "";
		if (ti.getTestMethod().isPresent()) {
			testMethod = ti.getTestMethod().get().toString();
		}
		classLogger.info("Test finished for: {} : {}", testClass, testMethod);
	}

}
