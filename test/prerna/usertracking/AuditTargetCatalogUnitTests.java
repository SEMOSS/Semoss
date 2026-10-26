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
package prerna.usertracking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.util.JdbcTestDatabase;

class AuditTargetCatalogUnitTests {

	private static Map<String, Object> event(String targetType, String targetId) {
		Map<String, Object> event = new LinkedHashMap<>();
		event.put("TARGET_TYPE", targetType);
		event.put("TARGET_ID", targetId);
		return event;
	}

	@Test
	void addsTheCurrentNameOfEachEngineAndAppAndWhatPicksAnEnginesLogo() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			when(db.engine.isBasic()).thenReturn(true);
			db.execute(
					"CREATE TABLE ENGINE (ENGINEID VARCHAR, ENGINENAME VARCHAR, ENGINEDISPLAYNAME VARCHAR, ENGINETYPE VARCHAR, ENGINESUBTYPE VARCHAR)",
					"INSERT INTO ENGINE VALUES ('m', 'model_m', 'Model M', 'MODEL', 'OPEN_AI'), ('d', 'database_d', '', 'DATABASE', 'H2_DB')",
					"CREATE TABLE PROJECT (PROJECTID VARCHAR, PROJECTNAME VARCHAR, PROJECTDISPLAYNAME VARCHAR)",
					"INSERT INTO PROJECT VALUES ('p', 'app_p', 'App P')");
			List<Map<String, Object>> events = new ArrayList<>(List.of(event("ENGINE", "m"), event("DATABASE", "d"),
					event("PROJECT", "p"), event("USER", "u"), event("MODEL", "deleted"), event(null, null)));
			AuditTargetCatalog.addTo(events);
			assertEquals("Model M", events.get(0).get("TARGET_DISPLAY_NAME"));
			assertEquals("MODEL", events.get(0).get("TARGET_ENGINE_TYPE"));
			assertEquals("OPEN_AI", events.get(0).get("TARGET_ENGINE_SUBTYPE"));
			// a blank display name falls back to the name
			assertEquals("database_d", events.get(1).get("TARGET_DISPLAY_NAME"));
			assertEquals("App P", events.get(2).get("TARGET_DISPLAY_NAME"));
			assertNull(events.get(2).get("TARGET_ENGINE_TYPE"));
			// people, engines the catalog no longer has, and events with no target get none
			for (Map<String, Object> event : events.subList(3, 6)) {
				for (String column : AuditTargetCatalog.COLUMNS) {
					assertNull(event.get(column));
				}
			}
		}
	}

	@Test
	void aCatalogThatCannotBeReadLeavesTheEventsAsRecorded() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			when(db.engine.isBasic()).thenReturn(true);
			List<Map<String, Object>> events = new ArrayList<>(List.of(event("MODEL", "m")));
			// there is no ENGINE table to read
			AuditTargetCatalog.addTo(events);
			assertNull(events.get(0).get("TARGET_DISPLAY_NAME"));
			assertEquals("m", events.get(0).get("TARGET_ID"));
		}
	}
}
