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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.util.JdbcTestDatabase;

class AbstractUserTrackingUtilsUnitTests {

	private final AbstractUserTrackingUtils tracker = new AbstractUserTrackingUtils() {
		@Override
		public void registerLogin(String session, String ip, User user, AuthProvider provider) {
		}
	};

	@Test
	void anonymousSessionBindsNullableLocationAndLogoutCommits() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			db.execute(
					"CREATE TABLE USER_TRACKING (SESSIONID VARCHAR, USERID VARCHAR, TYPE VARCHAR, CREATED_ON TIMESTAMP, ENDED_ON TIMESTAMP, IP_ADDR VARCHAR, IP_LAT VARCHAR, IP_LONG VARCHAR, IP_COUNTRY VARCHAR, IP_STATE VARCHAR, IP_CITY VARCHAR)");
			User user = mock(User.class);
			when(user.isAnonymous()).thenReturn(true);
			when(user.getAnonymousId()).thenReturn("anonymous");
			var details = new UserTrackingDetails(null, null, null, null, null, null);
			details.setIpAddr("  address  ");
			db.manual();
			AbstractUserTrackingUtils.saveSession("session", details, user, AuthProvider.NATIVE);
			db.connection.rollback();
			assertEquals("  address  ", db.value("SELECT IP_ADDR FROM USER_TRACKING"));
			assertNull(db.value("SELECT IP_LAT FROM USER_TRACKING"));
			assertEquals("ANONYMOUS", db.value("SELECT TYPE FROM USER_TRACKING"));
			tracker.registerLogout("session");
			assertNotNull(db.value("SELECT ENDED_ON FROM USER_TRACKING"));
		}
	}

	@Test
	void logoutFailureRollsBackWithoutChangingPublicFallback() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			db.manual();
			assertDoesNotThrow(() -> tracker.registerLogout("missing"));
			verify(db.connection).rollback();
			verify(db.connection, never()).commit();
		}
	}
}
