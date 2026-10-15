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
package prerna.reactor.collaboration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.User;
import prerna.collaboration.BrainOutgoingEmail;
import prerna.collaboration.email.MailProvider;
import prerna.collaboration.email.MailProviders;
import prerna.io.connector.mail.ComposedMail;
import prerna.io.connector.ms.outlook.MicrosoftOutlookSendDraftReactor;
import prerna.om.Insight;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.EmailUtility;

class WorkEmailTrackingUnitTests {

	private final Insight insight = mock(Insight.class);
	private final User user = mock(User.class);
	private final ComposedMail draft = new ComposedMail("draft-1", "conversation-1", null,
			List.of("recipient@test.com"), List.of(), List.of(), "Subject", "Body", false, List.of());

	private class EditorSend extends MicrosoftOutlookSendDraftReactor {
		boolean sent;
		final AtomicBoolean tracked;

		EditorSend(AtomicBoolean tracked) {
			this.tracked = tracked;
			setInsight(WorkEmailTrackingUnitTests.this.insight);
		}

		Map<String, Object> deliverDraft(boolean send) throws Exception {
			return finish(draft, send);
		}

		@Override
		protected ComposedMail sendDraft(User user, ComposedMail draft) {
			assertTrue(tracked.get(), "Topic tracking must commit before any send");
			sent = true;
			return draft;
		}
	}

	@Test
	void ordinaryEditorSendsOnlyAfterTrackingAndDoesNotTrackUnsentDrafts() throws Exception {
		when(insight.getUser()).thenReturn(user);
		AtomicBoolean tracked = new AtomicBoolean();
		try (MockedStatic<BrainOutgoingEmail> tracking = mockStatic(BrainOutgoingEmail.class);
				MockedStatic<EmailUtility> delivery = mockStatic(EmailUtility.class)) {
			tracking.when(() -> BrainOutgoingEmail.trackDraft(insight, "draft-1")).thenAnswer(call -> {
				tracked.set(true);
				return null;
			});
			delivery.when(() -> EmailUtility.sendEmail(any(), any())).thenAnswer(call -> ((Callable<?>) call.getArgument(0)).call());
			EditorSend editor = new EditorSend(tracked);
			assertEquals(true, editor.deliverDraft(false).get("draft"));
			tracking.verifyNoInteractions();
			assertEquals(true, editor.deliverDraft(true).get("sent"));
			assertTrue(editor.sent);
		}
	}

	@Test
	void ordinaryEditorNeverSendsAfterATrackingFailure() throws Exception {
		when(insight.getUser()).thenReturn(user);
		try (MockedStatic<BrainOutgoingEmail> tracking = mockStatic(BrainOutgoingEmail.class)) {
			tracking.when(() -> BrainOutgoingEmail.trackDraft(insight, "draft-1"))
					.thenThrow(new IllegalStateException("Tracking unavailable"));
			EditorSend editor = new EditorSend(new AtomicBoolean());
			assertThrows(IllegalStateException.class, () -> editor.deliverDraft(true));
			assertFalse(editor.sent);
		}
	}

	@Test
	void assistantSendUsesTheSameTrackingBoundary() throws Exception {
		MailProvider mail = mock(MailProvider.class);
		AtomicBoolean tracked = new AtomicBoolean();
		try (MockedStatic<MailProviders> providers = mockStatic(MailProviders.class);
				MockedStatic<BrainOutgoingEmail> tracking = mockStatic(BrainOutgoingEmail.class)) {
			providers.when(() -> MailProviders.forUser(user, null)).thenReturn(mail);
			tracking.when(() -> BrainOutgoingEmail.trackDraft(insight, "draft-1")).thenAnswer(call -> {
				tracked.set(true);
				return null;
			});
			when(mail.sendDraft("draft-1")).thenAnswer(call -> {
				assertTrue(tracked.get());
				return Map.of();
			});
			WorkSendEmailReactor reactor = sendReactor();
			assertEquals(true, ((Map<?, ?>) reactor.execute().getValue()).get("sent"));
			verify(mail).sendDraft("draft-1");
			reset(mail);
			tracking.when(() -> BrainOutgoingEmail.trackDraft(insight, "draft-1"))
					.thenThrow(new IllegalStateException("Tracking unavailable"));
			assertThrows(SemossPixelException.class, reactor::execute);
			verify(mail, never()).sendDraft(anyString());
		}
	}

	private WorkSendEmailReactor sendReactor() {
		WorkSendEmailReactor reactor = new WorkSendEmailReactor() {
			@Override
			protected User getUser() {
				return user;
			}
		};
		reactor.setInsight(insight);
		NounStore store = new NounStore("send");
		store.makeNoun("draftId").addLiteral("draft-1");
		reactor.setNounStore(store);
		return reactor;
	}
}
