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
package prerna.io.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.io.connector.google.gmail.GoogleGmailProfileByIdReactor;
import prerna.om.Insight;
import prerna.reactor.AbstractReactor;
import prerna.reactor.export.GoogleUploaderReactor;
import prerna.reactor.qs.source.GoogleFileRetrieverReactor;
import prerna.reactor.qs.source.GoogleListFilesReactor;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.security.HttpHelperUtility;

class ProviderLoginReactorUnitTests {

	@ParameterizedTest(name = "{0}: missing login is reported before inputs are validated")
	@MethodSource("reactors")
	void requiresProviderLoginBeforeDoingAnyWork(Class<? extends AbstractReactor> reactorClass, AuthProvider provider)
			throws Exception {
		AbstractReactor reactor = reactorClass.getDeclaredConstructor().newInstance();
		try (MockedStatic<HttpHelperUtility> http = mockStatic(HttpHelperUtility.class)) {
			// No insight or no user must produce the same structured login error.
			assertLoginRequired(assertThrows(SemossPixelException.class, reactor::execute), provider);
			Insight insight = mock(Insight.class);
			reactor.setInsight(insight);
			assertLoginRequired(assertThrows(SemossPixelException.class, reactor::execute), provider);

			// Being signed in to another provider does not authorize this connector.
			User user = mock(User.class);
			when(insight.getUser()).thenReturn(user);
			AuthProvider otherProvider = provider == AuthProvider.GOOGLE ? AuthProvider.MICROSOFT : AuthProvider.GOOGLE;
			when(user.isLoggedIn()).thenReturn(true);
			when(user.getLogins()).thenReturn(List.of(otherProvider));
			when(user.getAccessToken(otherProvider)).thenReturn(token(otherProvider, "other-provider-token"));
			assertLoginRequired(assertThrows(SemossPixelException.class, reactor::execute), provider);

			for (String value : new String[] { null, "", " \t\n" }) {
				when(user.getAccessToken(provider)).thenReturn(token(provider, value));
				assertLoginRequired(assertThrows(SemossPixelException.class, reactor::execute), provider);
			}
			http.verifyNoInteractions();
		}
	}

	@Test
	void googleQueryMergeRequiresLoginBeforeInitializingQueryState() {
		GoogleFileRetrieverReactor reactor = new GoogleFileRetrieverReactor();
		Insight insight = mock(Insight.class);
		reactor.setInsight(insight);
		assertLoginRequired(assertThrows(SemossPixelException.class, reactor::mergeUp), AuthProvider.GOOGLE);
	}

	@Test
	void signedInGoogleUserCanExecuteReactorAndThenMustLoginAgainAfterLogout() throws Exception {
		GoogleGmailProfileByIdReactor reactor = new GoogleGmailProfileByIdReactor();
		User user = mock(User.class);
		when(user.getAccessToken(AuthProvider.GOOGLE)).thenReturn(token(AuthProvider.GOOGLE, "google-token"));
		Insight insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(user);
		reactor.setInsight(insight);

		try (MockedStatic<HttpHelperUtility> http = mockStatic(HttpHelperUtility.class)) {
			http.when(() -> HttpHelperUtility.getRequest(anyString(), anyMap(), isNull(), isNull(), isNull()))
					.thenAnswer(invocation -> {
						Map<String, String> headers = invocation.getArgument(1);
						assertEquals("Bearer google-token", headers.get("Authorization"));
						return "{\"emailAddress\":\"user@example.com\",\"messagesTotal\":1,\"threadsTotal\":1,\"historyId\":\"1\"}";
					});
			NounMetadata result = reactor.execute();
			assertEquals("user@example.com", ((Map<?, ?>) result.getValue()).get("emailAddress"));
			http.clearInvocations();

			when(user.getAccessToken(AuthProvider.GOOGLE)).thenReturn(null);
			assertLoginRequired(assertThrows(SemossPixelException.class, reactor::execute), AuthProvider.GOOGLE);
			http.verifyNoInteractions();
		}
	}

	private static List<Arguments> reactors() {
		List<Arguments> reactors = new ArrayList<>();
		for (AuthProvider provider : List.of(AuthProvider.MICROSOFT, AuthProvider.GOOGLE)) {
			String providerPackage = "prerna.io.connector." + (provider == AuthProvider.MICROSOFT ? "ms" : "google");
			int before = reactors.size();
			try (ScanResult scan = new ClassGraph().enableClassInfo().acceptPackages(providerPackage).scan()) {
				for (ClassInfo info : scan.getSubclasses(AbstractReactor.class.getName())) {
					if (!info.isAbstract()) {
						reactors.add(Arguments.of(info.loadClass(AbstractReactor.class), provider));
					}
				}
			}
			assertTrue(reactors.size() > before, "No reactors found for " + provider);
		}
		reactors.add(Arguments.of(GoogleListFilesReactor.class, AuthProvider.GOOGLE));
		reactors.add(Arguments.of(GoogleFileRetrieverReactor.class, AuthProvider.GOOGLE));
		reactors.add(Arguments.of(GoogleUploaderReactor.class, AuthProvider.GOOGLE));
		return reactors;
	}

	private static AccessToken token(AuthProvider provider, String value) {
		AccessToken token = new AccessToken();
		token.setProvider(provider);
		token.setAccess_token(value);
		return token;
	}

	private static void assertLoginRequired(SemossPixelException exception, AuthProvider provider) {
		assertFalse(exception.isContinueThreadOfExecution());
		assertTrue(exception.getNoun().getOpType().contains(PixelOperationType.LOGGIN_REQUIRED_ERROR));
		assertEquals(
				Map.of("type", provider.getLabel(), "message",
						"Please login to your " + provider.getDisplayName() + " account"),
				exception.getNoun().getValue());
	}
}
