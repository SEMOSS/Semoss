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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

import org.junit.jupiter.api.Test;

import prerna.auth.User;
import prerna.usertracking.AuditEventType;
import prerna.usertracking.AuditTargetType;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.util.JdbcTestDatabase;

class EngineSettingsAuditTest {

	@Test
	void missingAndRejectedEngineUpdatesNeverEmitSuccess() throws Exception {
		try (var db = new JdbcTestDatabase();
				var owners = mockStatic(SecurityUserEngineUtils.class);
				var audit = mockStatic(UserAuditTrailUtils.class)) {
			User user = mock(User.class);
			owners.when(() -> SecurityUserEngineUtils.userIsOwner(user, "engine")).thenReturn(true);
			db.execute("CREATE TABLE ENGINE (ENGINEID VARCHAR, GLOBAL BOOLEAN, DISCOVERABLE BOOLEAN)");
			assertThrows(IllegalStateException.class, () -> SecurityEngineUtils.setEngineGlobal(user, "engine", true));
			db.execute("INSERT INTO ENGINE VALUES ('engine',false,false)",
					"ALTER TABLE ENGINE ADD CHECK (DISCOVERABLE=false)");
			assertTrue(SecurityEngineUtils.setEngineGlobal(user, "engine", false));
			assertTrue(SecurityEngineUtils.setEngineDiscoverable(user, "engine", false));
			assertThrows(IllegalStateException.class,
					() -> SecurityEngineUtils.setEngineDiscoverable(user, "engine", true));
			audit.verify(() -> UserAuditTrailUtils.recordEngineLifecycle(any(), any(), any(), any(), any(), any()),
					never());
			audit.verify(() -> UserAuditTrailUtils.recordFailure(eq(user), eq(AuditEventType.ENGINE_GLOBAL_UPDATE),
					eq(AuditTargetType.ENGINE), eq("engine"), any(), isNull()), times(1));
			audit.verify(
					() -> UserAuditTrailUtils.recordFailure(eq(user), eq(AuditEventType.ENGINE_DISCOVERABLE_UPDATE),
							eq(AuditTargetType.ENGINE), eq("engine"), any(), isNull()),
					times(1));
		}
	}
}
