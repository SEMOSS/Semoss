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
package prerna.auth.utils;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.owl.AbstractOWLEngine;
import prerna.util.SystemEngineRegistry;

public class SecurityOwlCreatorUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private IRDBMSEngine securityDb;
	private List<String> tables = new ArrayList<>();

	private SecurityOwlCreator creator;

	@BeforeEach
	void setup() {
		securityDb = SystemEngineRegistry.getSecurityDb();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		assertNotNull(this.securityDb);

		creator = new SecurityOwlCreator(securityDb.getQueryUtil());
	}

	@AfterEach
	void cleanup() throws SQLException {
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		// clear test database inside of temp directory
		// quicker than deleting and recreating
		tables = UnitTestSecurityAuthUtils.clearSecurityDB(securityDb, tables);
	}

	///
	/// needsRemake
	///

	@Test
	void testNeedsRemake_notForTheOwlWrittenAtStartUp() {
		assertFalse(creator.needsRemake(securityDb));
	}

	@Test
	void testNeedsRemake_anOwlWithNoTables() {
		IDatabaseEngine engine = mock(IDatabaseEngine.class);
		when(engine.getPhysicalConcepts()).thenReturn(List.of());

		assertTrue(creator.needsRemake(engine));
	}

	@Test
	void testNeedsRemake_anOwlMissingATable() {
		IDatabaseEngine engine = mock(IDatabaseEngine.class);
		when(engine.getPhysicalConcepts()).thenReturn(List.of(AbstractOWLEngine.BASE_NODE_URI + "/ENGINE"));

		assertTrue(creator.needsRemake(engine));
	}

	///
	/// remakeOwl
	///
	@Test
	void testRemakeOwl_successful() throws Exception {
		if (Files.exists(securityOwlFile)) {
			Files.delete(securityOwlFile);
		}

		creator.remakeOwl(securityDb);

//        assertTrue(Files.exists(securityOwlFile));
//        try (Stream<String> lines = Files.lines(securityOwlFile)) {
//            // this number will change with changes to security db schema
//            assertEquals(4404, lines.count());
//        }
	}

}
