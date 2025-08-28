/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.testing.cloudstorage;

import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import prerna.auth.User;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestEngineUtils;
import prerna.testing.ApiSemossTestUserUtils;

@TestMethodOrder(OrderAnnotation.class)
public class CentralCloudStorageTests extends AbstractBaseSemossApiTests {

	@BeforeAll
	public static void initialSetup() throws Exception {
		AbstractBaseSemossApiTests.initialSetup();
		// unnecessary if running by itself, but necessary if running {@link
		// prerna.testing.AllTests}
		ApiSemossTestEngineUtils.deleteAllDataAndAddUser();
	}

	@Override
	@BeforeEach
	public void beforeEachTest() throws Exception {
		this.clearAllDatabasesBetweenTests = false;
		super.beforeEachTest();
	}

	@Test
	@Order(1)
	public void storeEngine() {
		User defaultTestAdminUser = ApiSemossTestUserUtils.getUser();

		try {

		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
}
