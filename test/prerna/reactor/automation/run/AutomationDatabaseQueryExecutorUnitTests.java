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
package prerna.reactor.automation.run;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.reactor.automation.AutomationConstants;

public class AutomationDatabaseQueryExecutorUnitTests {

	@Test
	void supportsOnlyGeneratedDatabaseReads() {
		Map<String, Object> generated = new LinkedHashMap<>();
		generated.put(AutomationConstants.NODE_FIELD_TYPE, AutomationConstants.NODE_DATABASE_QUERY);
		assertTrue(AutomationDatabaseQueryExecutor.supports(generated));

		Map<String, Object> custom = new LinkedHashMap<>(generated);
		custom.put(AutomationConstants.NODE_FIELD_CODE_MODE, AutomationConstants.NODE_CODE_MODE_CUSTOM);
		assertFalse(AutomationDatabaseQueryExecutor.supports(custom));
		assertFalse(AutomationDatabaseQueryExecutor.supports(
				Map.of(AutomationConstants.NODE_FIELD_TYPE, AutomationConstants.NODE_DATABASE_INSERT)));
	}

	@Test
	void parsesTheResolvedInternalRequest() {
		Map<String, Object> request = Map.of(AutomationConstants.INTERNAL_DATABASE_QUERY,
				Map.of(AutomationConstants.CONFIG_ENGINE_ID, "engine-1", "query", "SELECT 1",
						AutomationConstants.CONFIG_LIMIT, 250));

		assertTrue(AutomationDatabaseQueryExecutor.isRequest(request));
		AutomationDatabaseQueryExecutor.QueryRequest parsed = AutomationDatabaseQueryExecutor.parseRequest(request);
		assertEquals("engine-1", parsed.databaseId());
		assertEquals("SELECT 1", parsed.query());
		assertEquals(250, parsed.limit());
	}

	@Test
	void rejectsMalformedOrUnboundedRequests() {
		Map<String, Object> missingQuery = Map.of(AutomationConstants.INTERNAL_DATABASE_QUERY,
				Map.of(AutomationConstants.CONFIG_ENGINE_ID, "engine-1", AutomationConstants.CONFIG_LIMIT, 50));
		assertThrows(IllegalStateException.class,
				() -> AutomationDatabaseQueryExecutor.parseRequest(missingQuery));

		Map<String, Object> excessiveLimit = Map.of(AutomationConstants.INTERNAL_DATABASE_QUERY,
				Map.of(AutomationConstants.CONFIG_ENGINE_ID, "engine-1", "query", "SELECT 1",
						AutomationConstants.CONFIG_LIMIT, AutomationConstants.DB_QUERY_MAX_LIMIT + 1));
		assertThrows(IllegalStateException.class,
				() -> AutomationDatabaseQueryExecutor.parseRequest(excessiveLimit));
	}
}
