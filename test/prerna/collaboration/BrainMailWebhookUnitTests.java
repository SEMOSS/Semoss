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
package prerna.collaboration;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.AccessToken;
import prerna.io.connector.ms.MicrosoftGraphCredentialStore;
import prerna.io.connector.ms.MicrosoftGraphSubscriptionClient;
import prerna.io.connector.ms.MicrosoftGraphSubscriptionRegistry;
import prerna.io.connector.ms.MicrosoftGraphSubscriptionRegistry.Subscription;

/** New-mail subscriptions read the mailbox with nobody watching, so only with background access and mail on. */
class BrainMailWebhookUnitTests {

	private static final String MAILBOX = "ms-owner";

	private MockedStatic<CollaborationDbUtils> db;
	private MockedStatic<CollaborationSourceUtils> sources;
	private MockedStatic<MicrosoftGraphCredentialStore> credentials;
	private MockedStatic<MicrosoftGraphSubscriptionRegistry> registry;
	private MockedStatic<MicrosoftGraphSubscriptionClient> graph;
	private MockedStatic<BrainMailHeaderSource> headers;
	private Subscription subscription;

	@BeforeEach
	void setUp() throws Exception {
		// one owner of the mailbox, as onboarding records it
		db = mockStatic(CollaborationDbUtils.class, invocation -> switch (invocation.getMethod().getName()) {
		case "isInitalized" -> true;
		case "query" -> ((String) invocation.getArgument(0)).contains("SELECT DISTINCT MS_USER_ID") ? List.of(MAILBOX)
				: List.<String[]>of(new String[] { "owner-one", "NATIVE" });
		default -> invocation.callRealMethod();
		});
		sources = mockStatic(CollaborationSourceUtils.class);
		credentials = mockStatic(MicrosoftGraphCredentialStore.class);
		registry = mockStatic(MicrosoftGraphSubscriptionRegistry.class);
		graph = mockStatic(MicrosoftGraphSubscriptionClient.class);
		headers = mockStatic(BrainMailHeaderSource.class);
		subscription = mock(Subscription.class);
		when(subscription.getId()).thenReturn("sub-1");
		when(subscription.getUserId()).thenReturn(MAILBOX);
		when(subscription.getResource()).thenReturn("me/messages");
		when(subscription.accessToken()).thenReturn("subscription-token");
		registry.when(() -> MicrosoftGraphSubscriptionRegistry.forUser(MAILBOX)).thenReturn(List.of(subscription));
	}

	@AfterEach
	void tearDown() {
		headers.close();
		graph.close();
		registry.close();
		credentials.close();
		sources.close();
		db.close();
	}

	private void mail(boolean on) {
		sources.when(() -> CollaborationSourceUtils.getSourcesEnabled("owner-one", "NATIVE"))
				.thenReturn(Map.of("email", on));
	}

	private void backgroundAccess(boolean saved) {
		credentials.when(() -> MicrosoftGraphCredentialStore.get(MAILBOX)).thenReturn(saved ? new AccessToken() : null);
	}

	@Test
	void newMailIsNotReadAfterBackgroundAccessIsTurnedOff() throws Exception {
		mail(true);
		backgroundAccess(false);
		BrainMailWebhook.onMail(subscription, "created", "message-1");
		credentials.verify(() -> MicrosoftGraphCredentialStore.get(MAILBOX));
		headers.verify(BrainMailHeaderSource::current, never());
		verify(subscription, never()).accessToken();
	}

	@Test
	void newMailIsNotReadWithMailTurnedOff() throws Exception {
		mail(false);
		backgroundAccess(true);
		BrainMailWebhook.onMail(subscription, "created", "message-1");
		sources.verify(() -> CollaborationSourceUtils.getSourcesEnabled("owner-one", "NATIVE"));
		headers.verify(BrainMailHeaderSource::current, never());
		verify(subscription, never()).accessToken();
	}

	@Test
	void aSubscriptionIsWantedOnlyWithBackgroundAccessAndMailOn() {
		mail(true);
		backgroundAccess(true);
		assertTrue(BrainMailWebhook.wanted(MAILBOX));
		mail(false);
		assertFalse(BrainMailWebhook.wanted(MAILBOX));
		mail(true);
		backgroundAccess(false);
		assertFalse(BrainMailWebhook.wanted(MAILBOX));
	}

	@Test
	void unsubscribingStopsTheSubscriptionAtGraphAndForgetsIt() {
		BrainMailWebhook.unsubscribe(MAILBOX);
		graph.verify(() -> MicrosoftGraphSubscriptionClient.delete("subscription-token", "sub-1"));
		registry.verify(() -> MicrosoftGraphSubscriptionRegistry.remove("sub-1"));
	}

	@Test
	void theHourlyRenewalStopsWhatIsNoLongerWantedInsteadOfRenewingIt() {
		mail(true);
		backgroundAccess(false);
		BrainMailWebhook.renewAll();
		graph.verify(() -> MicrosoftGraphSubscriptionClient.renew(anyString(), anyString(), anyString(), anyInt()),
				never());
		graph.verify(() -> MicrosoftGraphSubscriptionClient.delete("subscription-token", "sub-1"));
		registry.verify(() -> MicrosoftGraphSubscriptionRegistry.remove("sub-1"));
	}
}
