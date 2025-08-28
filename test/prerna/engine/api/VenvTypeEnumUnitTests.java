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
package prerna.engine.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import prerna.engine.impl.venv.PythonVenvEngine;

public class VenvTypeEnumUnitTests {

	@Test
	void testPython() {
		VenvTypeEnum testEnum = VenvTypeEnum.PYTHON;
		assertEquals("PYTHON", testEnum.getVenvName());
		assertEquals(PythonVenvEngine.class.getName(), testEnum.getVenvClass());
	}

	@Test
	void testBadVectorDatabaseName() {
		String badName = "NOT_A_REAL_VENV_TYPE";
		Exception thrown = assertThrows(IllegalArgumentException.class, () -> VenvTypeEnum.getEnumFromName(badName));
		assertEquals("Invalid input for name " + badName, thrown.getMessage());
	}
}
