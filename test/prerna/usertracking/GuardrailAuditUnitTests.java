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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockStatic;

import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.engine.api.IEngine;
import prerna.util.JdbcTestDatabase;
import prerna.util.Utility;

class GuardrailAuditUnitTests {

	private static final GuardrailAudit.Hit BLOCK = new GuardrailAudit.Hit("check-1", "toxicity-1", "Toxicity", "INPUT",
			GuardrailAudit.Action.BLOCK, Map.of("prompt", "something private"));

	@Test
	void aBlockIsRecordedOnTheGuardedEngineWithoutWhatItScreened() throws Exception {
		try (var db = new JdbcTestDatabase(); var utility = mockStatic(Utility.class)) {
			UserAuditTrailUtilsUnitTests.createAuditTable(db);
			utility.when(Utility::isUserTrackingEnabled).thenReturn(true);

			GuardrailAudit.recordHit(null, new GuardrailAudit.Call("model-1", "Assistant", IEngine.CATALOG_TYPE.MODEL,
					"askRoom", "room-1", false), BLOCK);

			assertEquals(1, db.count("USER_AUDIT_EVENTS"));
			assertEquals("GUARDRAIL_BLOCK", db.value("SELECT EVENT_TYPE FROM USER_AUDIT_EVENTS"));
			// DENIED is for access refusals, which the audit trail names Access Denied
			assertEquals("FAILURE", db.value("SELECT STATUS FROM USER_AUDIT_EVENTS"));
			assertEquals("MODEL", db.value("SELECT TARGET_TYPE FROM USER_AUDIT_EVENTS"));
			assertEquals("model-1", db.value("SELECT TARGET_ID FROM USER_AUDIT_EVENTS"));
			assertEquals("model-1", db.value("SELECT ENGINE_ID FROM USER_AUDIT_EVENTS"));
			assertEquals("room-1", db.value("SELECT ROOM_ID FROM USER_AUDIT_EVENTS"));
			String details = String.valueOf(db.value("SELECT DETAILS FROM USER_AUDIT_EVENTS"));
			assertTrue(details.contains("\"checkId\":\"check-1\""), details);
			assertTrue(details.contains("\"methodName\":\"askRoom\""), details);
			assertTrue(details.contains("\"action\":\"BLOCK\""), details);
			assertFalse(details.contains("something private"), details);
		}
	}

	@Test
	void aMaskKeepsWhatItScreenedWhenTheEngineKeepsInputAndOutput() throws Exception {
		try (var db = new JdbcTestDatabase(); var utility = mockStatic(Utility.class)) {
			UserAuditTrailUtilsUnitTests.createAuditTable(db);
			utility.when(Utility::isUserTrackingEnabled).thenReturn(true);

			GuardrailAudit.recordHit(null,
					new GuardrailAudit.Call("database-1", "Orders", IEngine.CATALOG_TYPE.DATABASE, "execQuery", null,
							true),
					new GuardrailAudit.Hit("check-2", "pii-1", "PII", "INPUT", GuardrailAudit.Action.MASK,
							Map.of("prompt", "SELECT * FROM people WHERE ssn = '123'")));

			assertEquals("GUARDRAIL_MASK", db.value("SELECT EVENT_TYPE FROM USER_AUDIT_EVENTS"));
			assertEquals("SUCCESS", db.value("SELECT STATUS FROM USER_AUDIT_EVENTS"));
			assertEquals("DATABASE", db.value("SELECT TARGET_TYPE FROM USER_AUDIT_EVENTS"));
			String details = String.valueOf(db.value("SELECT DETAILS FROM USER_AUDIT_EVENTS"));
			assertTrue(details.contains("ssn = '123'"), details);
		}
	}

	@Test
	void nothingIsRecordedWhileUserTrackingIsOff() throws Exception {
		try (var db = new JdbcTestDatabase(); var utility = mockStatic(Utility.class)) {
			UserAuditTrailUtilsUnitTests.createAuditTable(db);
			utility.when(Utility::isUserTrackingEnabled).thenReturn(false);

			GuardrailAudit.recordHit(null,
					new GuardrailAudit.Call("model-1", "Assistant", IEngine.CATALOG_TYPE.MODEL, "askRoom", null, true),
					BLOCK);

			assertEquals(0, db.count("USER_AUDIT_EVENTS"));
		}
	}
}
