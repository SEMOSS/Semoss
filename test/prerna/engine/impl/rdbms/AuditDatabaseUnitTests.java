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
package prerna.engine.impl.rdbms;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.SemossUnitTest;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineUtility;

public class AuditDatabaseUnitTests extends SemossUnitTest {

	@AfterEach
	void resetDefaultInsightsRdbmsProperty() {
		DIHelper.getInstance().putProperty(Constants.DEFAULT_INSIGHTS_RDBMS, "H2_DB");
	}

	@Test
	void testInitInvalidConfiguredRdbmsType() {
		DIHelper.getInstance().putProperty(Constants.DEFAULT_INSIGHTS_RDBMS, "NOT_A_REAL_RDBMS_TYPE");

		AuditDatabase auditDatabase = new AuditDatabase();
		IDatabaseEngine database = mock(IDatabaseEngine.class);

		try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class)) {
			eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.DATABASE, "did", "dname"))
					.thenReturn(tempDir.resolve("db").toString());

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> auditDatabase.init(database, "did", "dname"));

			assertTrue(e.getMessage().contains("NOT_A_REAL_RDBMS_TYPE"),
					"Error message should name the invalid configured value, was: " + e.getMessage());
			assertTrue(e.getMessage().contains("H2_DB"),
					"Error message should list the valid RdbmsTypeEnum constants, was: " + e.getMessage());
		}
	}

}
