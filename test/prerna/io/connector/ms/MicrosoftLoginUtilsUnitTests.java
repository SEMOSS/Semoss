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
package prerna.io.connector.ms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.io.connector.ms.calendar.MicrosoftCalendarListCalendarsReactor;
import prerna.io.connector.ms.onedrive.MicrosoftOneDriveListFilesReactor;
import prerna.io.connector.ms.outlook.MicrosoftOutlookListMailFoldersReactor;
import prerna.io.connector.ms.teams.MicrosoftTeamsListTeamsReactor;
import prerna.om.Insight;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.security.HttpHelperUtility;

class MicrosoftLoginUtilsUnitTests {

	@Test
	void returnsCurrentTokenWithoutRefreshing() throws Exception {
		User user = userWith(token("current-token", 3_600_000L));
		try (MockedConstruction<MicrosoftTokenFiller> fillers = mockConstruction(MicrosoftTokenFiller.class)) {
			assertEquals("current-token", MicrosoftLoginUtils.getValidAccessToken(user));
			assertTrue(fillers.constructed().isEmpty());
			verify(user, never()).setAccessToken(any());
		}
	}

	@ParameterizedTest
	@ValueSource(longs = { -1_000L, 30_000L })
	void refreshesExpiredOrSoonToExpireTokensAndStoresTheResult(long remainingMillis) throws Exception {
		AccessToken expired = token("old-token", remainingMillis);
		AccessToken refreshed = token("new-token", 3_600_000L);
		User user = userWith(expired);
		try (MockedConstruction<MicrosoftTokenFiller> fillers = mockConstruction(MicrosoftTokenFiller.class,
				(filler, context) -> when(filler.refreshAccessToken(same(expired), anyMap())).thenReturn(refreshed))) {
			assertEquals("new-token", MicrosoftLoginUtils.getValidAccessToken(user));
			verify(user).setAccessToken(refreshed);
			assertEquals(1, fillers.constructed().size());
		}
	}

	@Test
	void originalHelperAlsoRefreshesExpiredTokens() throws Exception {
		AccessToken expired = token("old-token", -1_000L);
		AccessToken refreshed = token("new-token", 3_600_000L);
		User user = userWith(expired);
		try (MockedConstruction<MicrosoftTokenFiller> fillers = mockConstruction(MicrosoftTokenFiller.class,
				(filler, context) -> when(filler.refreshAccessToken(same(expired), anyMap())).thenReturn(refreshed))) {
			assertEquals("new-token", MicrosoftLoginUtils.getMicrosoftAccessToken(user));
			verify(user).setAccessToken(refreshed);
		}
	}

	@Test
	void missingLoginRequiresAuthentication() {
		assertLoginRequired(
				assertThrows(SemossPixelException.class, () -> MicrosoftLoginUtils.getValidAccessToken(null)));
		assertLoginRequired(assertThrows(SemossPixelException.class,
				() -> MicrosoftLoginUtils.getValidAccessToken(userWith(null))));
	}

	@Test
	void failedRefreshRequiresAuthenticationWithoutSavingAnInvalidToken() {
		User user = userWith(token("old-token", -1_000L));
		try (MockedConstruction<MicrosoftTokenFiller> fillers = mockConstruction(MicrosoftTokenFiller.class)) {
			assertLoginRequired(
					assertThrows(SemossPixelException.class, () -> MicrosoftLoginUtils.getValidAccessToken(user)));
			verify(user, never()).setAccessToken(any());
		}
	}

	@ParameterizedTest
	@MethodSource("reactors")
	void reactorsSendRefreshedTokensToGraph(AbstractReactor reactor) throws Exception {
		AccessToken expired = token("old-token", -1_000L);
		AccessToken refreshed = token("new-token", 3_600_000L);
		User user = userWith(expired);
		Insight insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(user);
		reactor.setInsight(insight);

		try (MockedConstruction<MicrosoftTokenFiller> fillers = mockConstruction(MicrosoftTokenFiller.class,
				(filler, context) -> when(filler.refreshAccessToken(same(expired), anyMap())).thenReturn(refreshed));
				MockedStatic<HttpHelperUtility> http = mockStatic(HttpHelperUtility.class)) {
			http.when(() -> HttpHelperUtility.getRequest(anyString(), anyMap(), isNull(), isNull(), isNull()))
					.thenAnswer(invocation -> {
						Map<String, String> headers = invocation.getArgument(1);
						assertEquals("Bearer new-token", headers.get("Authorization"));
						return "{\"value\":[]}";
					});

			reactor.execute();

			http.verify(() -> HttpHelperUtility.getRequest(anyString(),
					argThat(headers -> "Bearer new-token".equals(headers.get("Authorization"))), isNull(), isNull(),
					isNull()), atLeastOnce());
			verify(user).setAccessToken(refreshed);
		}
	}

	private static Stream<AbstractReactor> reactors() {
		return Stream.of(new MicrosoftTeamsListTeamsReactor(), new MicrosoftCalendarListCalendarsReactor(),
				new MicrosoftOneDriveListFilesReactor(), new MicrosoftOutlookListMailFoldersReactor());
	}

	private static User userWith(AccessToken token) {
		User user = mock(User.class);
		when(user.getAccessToken(AuthProvider.MICROSOFT)).thenReturn(token);
		return user;
	}

	private static AccessToken token(String value, long remainingMillis) {
		AccessToken token = new AccessToken();
		token.setProvider(AuthProvider.MICROSOFT);
		token.setAccess_token(value);
		token.setExpires_in(3_600);
		token.setStartTime(System.currentTimeMillis() - 3_600_000L + remainingMillis);
		return token;
	}

	private static void assertLoginRequired(SemossPixelException exception) {
		assertFalse(exception.isContinueThreadOfExecution());
		assertTrue(exception.getNoun().getOpType().contains(PixelOperationType.LOGGIN_REQUIRED_ERROR));
	}
}
