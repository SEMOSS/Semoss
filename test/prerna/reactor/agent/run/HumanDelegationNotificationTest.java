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
package prerna.reactor.agent.run;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import prerna.notifications.NotificationDbUtils;
import prerna.notifications.NotificationService;
import prerna.util.NotificationConstants;

class HumanDelegationNotificationTest {

	private static final String META = "{\"requester\":{\"userId\":\"jane-id\",\"provider\":\"NATIVE\","
			+ "\"name\":\"Jane Doe\",\"email\":\"jane@example.com\"},\"assignee\":{\"userId\":\"ryan-id\","
			+ "\"provider\":\"MS\",\"name\":\"Ryan Weiler\",\"email\":\"ryan@example.com\"}}";

	private MockedStatic<NotificationDbUtils> db;
	private MockedStatic<NotificationService> service;
	private MockedStatic<AgentRunActionStore> actions;

	@BeforeEach
	void mockStores() {
		db = mockStatic(NotificationDbUtils.class);
		service = mockStatic(NotificationService.class);
		actions = mockStatic(AgentRunActionStore.class);
		db.when(NotificationDbUtils::isInitalized).thenReturn(true);
	}

	@AfterEach
	void closeMocks() {
		actions.close();
		service.close();
		db.close();
	}

	@Test
	void anAnswerNotifiesTheRequesterInTheirOwnRoomAndClearsTheRequestNotice() {
		givenDelegation("RESPONDED", META);

		HumanDelegationService.notifySettled("child-1", "requester-room", "RESPONDED");

		ArgumentCaptor<String> metadata = ArgumentCaptor.forClass(String.class);
		service.verify(() -> NotificationService.createCollaborationNotification(
				eq(id("semoss:delegation-notification:RESPONDED:action-1")),
				eq(NotificationConstants.Type.DELEGATION_RESPONSE), eq("jane-id"), eq("NATIVE"),
				eq("Ryan Weiler responded to your request"), eq("Open the conversation to read their response."),
				eq("ryan-id"), eq("requester-room"), metadata.capture()));
		assertTrue(metadata.getValue().contains("\"actionId\":\"action-1\""), metadata.getValue());
		assertTrue(metadata.getValue().contains("\"roomId\":\"requester-room\""), metadata.getValue());
		service.verify(() -> NotificationService.dismissUserNotification(
				id("semoss:delegation-notification:action-1"), "ryan-id", "MS"));
	}

	@Test
	void aDeclineNotifiesTheRequester() {
		givenDelegation("DECLINED", META);

		HumanDelegationService.notifySettled("child-1", "requester-room", "DECLINED");

		service.verify(() -> NotificationService.createCollaborationNotification(
				eq(id("semoss:delegation-notification:DECLINED:action-1")),
				eq(NotificationConstants.Type.DELEGATION_DECLINED), eq("jane-id"), eq("NATIVE"),
				eq("Ryan Weiler declined your request"), eq("Open the conversation for details."), eq("ryan-id"),
				eq("requester-room"), anyString()));
	}

	@Test
	void aWithdrawalNotifiesTheAssigneeInTheRequestRoom() {
		givenDelegation("CANCELLED", META);

		HumanDelegationService.notifySettled("child-1", "requester-room", "CANCELLED");

		service.verify(() -> NotificationService.createCollaborationNotification(
				eq(id("semoss:delegation-notification:CANCELLED:action-1")),
				eq(NotificationConstants.Type.DELEGATION_WITHDRAWN), eq("ryan-id"), eq("MS"),
				eq("Jane Doe withdrew their request"), eq("You no longer need to respond to it."), eq("jane-id"),
				eq("assignee-room"), anyString()));
		service.verify(() -> NotificationService.dismissUserNotification(
				id("semoss:delegation-notification:action-1"), "ryan-id", "MS"));
	}

	@Test
	void anUnansweredRequestOnlyClearsTheRequestNotice() {
		givenDelegation("PENDING", META);

		HumanDelegationService.notifySettled("child-1", "requester-room", "UNANSWERED");

		service.verify(() -> NotificationService.createCollaborationNotification(any(), any(), any(), any(), any(),
				any(), any(), any(), any()), never());
		service.verify(() -> NotificationService.dismissUserNotification(anyString(), eq("ryan-id"), eq("MS")));
	}

	@Test
	void rowsWithoutPeopleCannotBeAddressed() {
		givenDelegation("RESPONDED", "{\"requesterName\":\"Jane Doe\",\"assigneeAuthType\":\"NATIVE\"}");

		HumanDelegationService.notifySettled("child-1", "requester-room", "RESPONDED");

		service.verify(() -> NotificationService.createCollaborationNotification(any(), any(), any(), any(), any(),
				any(), any(), any(), any()), never());
	}

	@Test
	void nothingIsSentWhenNotificationsAreOff() {
		db.when(NotificationDbUtils::isInitalized).thenReturn(false);
		givenDelegation("RESPONDED", META);

		HumanDelegationService.notifySettled("child-1", "requester-room", "RESPONDED");

		actions.verify(() -> AgentRunActionStore.getActionsForRun(anyString()), never());
		service.verifyNoInteractions();
	}

	@Test
	void aNotificationFailureNeverReachesDelivery() {
		givenDelegation("RESPONDED", META);
		service.when(() -> NotificationService.createCollaborationNotification(any(), any(), any(), any(), any(),
				any(), any(), any(), any())).thenThrow(new IllegalStateException("Unable to create notification"));
		actions.when(() -> AgentRunActionStore.getActionsForRun("child-2"))
				.thenThrow(new IllegalStateException("database down"));

		HumanDelegationService.notifySettled("child-1", "requester-room", "RESPONDED");
		HumanDelegationService.notifySettled("child-2", "requester-room", "RESPONDED");
	}

	@Test
	void retriesReuseOneNotificationPerOutcome() {
		givenDelegation("RESPONDED", META);

		HumanDelegationService.notifySettled("child-1", "requester-room", "RESPONDED");
		HumanDelegationService.notifySettled("child-1", "requester-room", "RESPONDED");

		ArgumentCaptor<String> ids = ArgumentCaptor.forClass(String.class);
		service.verify(() -> NotificationService.createCollaborationNotification(ids.capture(), any(), any(), any(),
				any(), any(), any(), any(), any()), org.mockito.Mockito.times(2));
		assertEquals(ids.getAllValues().get(0), ids.getAllValues().get(1));
	}

	private void givenDelegation(String status, String toolMeta) {
		Map<String, Object> approval = new HashMap<>();
		approval.put("actionId", "approval-1");
		approval.put("toolName", "SomeTool");
		approval.put("status", "APPROVED");
		Map<String, Object> delegation = new HashMap<>();
		delegation.put("actionId", "action-1");
		delegation.put("runId", "child-1");
		delegation.put("roomId", "assignee-room");
		delegation.put("toolName", "DelegationRequest");
		delegation.put("status", status);
		delegation.put("toolMeta", toolMeta);
		actions.when(() -> AgentRunActionStore.getActionsForRun("child-1")).thenReturn(List.of(approval, delegation));
	}

	private static String id(String seed) {
		return UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8)).toString();
	}
}
