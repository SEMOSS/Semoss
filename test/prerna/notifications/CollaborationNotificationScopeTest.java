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
package prerna.notifications;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.collaboration.CollaborationUtils;

class CollaborationNotificationScopeTest {

	private final User user = mock(User.class);

	@Test
	void everySignedInUserCanReadTheCollaborationInbox() {
		try (MockedStatic<SecurityProjectUtils> projects = mockStatic(SecurityProjectUtils.class)) {
			assertTrue(NotificationDbUtils.canViewAppScope(user, CollaborationUtils.COLLABORATION_PROJECT_ID));
			assertEquals("APP", NotificationDbUtils.resolveReadScope(user, " app ", "SYSTEM__COLLABORATION"));
			projects.verify(() -> SecurityProjectUtils.userCanViewProject(any(), any()), never());
		}
	}

	@Test
	void otherAppScopesStillNeedProjectAccess() {
		try (MockedStatic<SecurityProjectUtils> projects = mockStatic(SecurityProjectUtils.class)) {
			projects.when(() -> SecurityProjectUtils.userCanViewProject(user, "shared-app")).thenReturn(true);

			assertEquals("APP", NotificationDbUtils.resolveReadScope(user, "APP", "shared-app"));
			assertFalse(NotificationDbUtils.canViewAppScope(user, "private-app"));
			IllegalArgumentException denied = assertThrows(IllegalArgumentException.class,
					() -> NotificationDbUtils.resolveReadScope(user, "APP", "private-app"));
			assertEquals("Project does not exist or user does not have access to the project", denied.getMessage());
		}
	}

	@Test
	void readScopesDefaultToAllAndRejectBadSelectors() {
		assertEquals("ALL", NotificationDbUtils.resolveReadScope(user, null, null));
		assertEquals("ALL", NotificationDbUtils.resolveReadScope(user, " ", null));
		assertEquals("SYSTEM", NotificationDbUtils.resolveReadScope(user, "system", null));
		assertThrows(IllegalArgumentException.class, () -> NotificationDbUtils.resolveReadScope(user, "APP", " "));
		assertThrows(IllegalArgumentException.class,
				() -> NotificationDbUtils.resolveReadScope(user, "EVERYTHING", null));
		assertFalse(NotificationDbUtils.canViewAppScope(user, null));
	}

	@Test
	void collaborationNotificationsLandInTheCollaborationInboxAndOpenTheirRoom() {
		try (MockedStatic<NotificationDbUtils> db = mockStatic(NotificationDbUtils.class)) {
			NotificationService.createCollaborationNotification(" n-1 ", "DELEGATION_RESPONSE", "jane-id", "NATIVE",
					"Ryan Weiler responded to your request", "Open the conversation to read their response.",
					"ryan-id", "room-1", "{\"actionId\":\"action-1\"}");

			db.verify(() -> NotificationDbUtils.insertNotificationEventIfAbsent("n-1", "DELEGATION_RESPONSE", "APP",
					"SYSTEM__COLLABORATION", "USER", "jane-id", "NATIVE", "Ryan Weiler responded to your request",
					"Open the conversation to read their response.", "NORMAL", "BELL", "USER", "ryan-id", "ROOM",
					"room-1", "{\"actionId\":\"action-1\"}", "ryan-id"));
		}
	}

	@Test
	void aCollaborationNotificationWithoutARoomOpensNothing() {
		try (MockedStatic<NotificationDbUtils> db = mockStatic(NotificationDbUtils.class)) {
			NotificationService.createCollaborationNotification("n-2", "DELEGATION_REQUEST", "ryan-id", "MS",
					"Jane Doe sent you a request", "Open the request to review it and respond.", "jane-id", " ",
					null);

			db.verify(() -> NotificationDbUtils.insertNotificationEventIfAbsent("n-2", "DELEGATION_REQUEST", "APP",
					"SYSTEM__COLLABORATION", "USER", "ryan-id", "MS", "Jane Doe sent you a request",
					"Open the request to review it and respond.", "NORMAL", "BELL", "USER", "jane-id", "NONE", null,
					null, "jane-id"));
		}
	}

	@Test
	void collaborationNotificationsNeedARecipientAndAShortTitle() {
		try (MockedStatic<NotificationDbUtils> db = mockStatic(NotificationDbUtils.class)) {
			assertThrows(IllegalArgumentException.class,
					() -> NotificationService.createCollaborationNotification("n-3", "DELEGATION_REQUEST", " ",
							"MS", "Title", "Message", "jane-id", "room-1", null));
			assertThrows(IllegalArgumentException.class,
					() -> NotificationService.createCollaborationNotification("n-4", "DELEGATION_REQUEST",
							"ryan-id", "MS", "x".repeat(256), "Message", "jane-id", "room-1", null));
			db.verifyNoInteractions();
		}
	}
}
