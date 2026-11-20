package prerna.io.connector.ms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.io.connector.AbstractOAuthTokenFiller;
import prerna.security.HttpHelperUtility;
import prerna.util.SocialPropertiesUtil;

class MicrosoftTokenFillerUnitTests {

	@Test
	void replacesRefreshTokenAndUsesItForNextRefresh() {
		AccessToken current = token("old-access", "old-refresh");
		current.addMetaValue("unrelated", "preserved");
		MicrosoftTokenFiller filler = new MicrosoftTokenFiller();
		try (MockedStatic<SocialPropertiesUtil> social = configuredMicrosoft();
				MockedStatic<HttpHelperUtility> http = mockStatic(HttpHelperUtility.class)) {
			AtomicInteger calls = new AtomicInteger();
			http.when(() -> HttpHelperUtility.getAccessToken(anyString(), anyMap(), eq(true), eq(true)))
					.thenAnswer(invocation -> {
						Map<String, String> request = invocation.getArgument(1);
						assertEquals(calls.getAndIncrement() == 0 ? "old-refresh" : "new-refresh",
								request.get("refresh_token"));
						return token("new-access", "new-refresh");
					});

			AccessToken refreshed = filler.refreshAccessToken(current, Map.of());
			assertEquals(List.of("new-refresh"), refreshed.getMetaValues(AbstractOAuthTokenFiller.REFRESH_TOKEN_KEY));
			assertEquals(List.of("preserved"), refreshed.getMetaValues("unrelated"));
			assertEquals(List.of("old-refresh"), current.getMetaValues(AbstractOAuthTokenFiller.REFRESH_TOKEN_KEY));
			filler.refreshAccessToken(refreshed, Map.of());
			assertEquals(2, calls.get());
		}
	}

	@ParameterizedTest
	@NullAndEmptySource
	@ValueSource(strings = "   ")
	void keepsExistingRefreshTokenWhenResponseOmitsIt(String returnedRefreshToken) {
		MicrosoftTokenFiller filler = new MicrosoftTokenFiller();
		try (MockedStatic<SocialPropertiesUtil> social = configuredMicrosoft();
				MockedStatic<HttpHelperUtility> http = mockStatic(HttpHelperUtility.class)) {
			http.when(() -> HttpHelperUtility.getAccessToken(anyString(), anyMap(), eq(true), eq(true)))
					.thenReturn(token("new-access", returnedRefreshToken));

			AccessToken refreshed = filler.refreshAccessToken(token("old-access", "old-refresh"), Map.of());
			assertEquals("new-access", refreshed.getAccess_token());
			assertEquals(List.of("old-refresh"), refreshed.getMetaValues(AbstractOAuthTokenFiller.REFRESH_TOKEN_KEY));
		}
	}

	private static MockedStatic<SocialPropertiesUtil> configuredMicrosoft() {
		SocialPropertiesUtil properties = mock(SocialPropertiesUtil.class);
		when(properties.getProperty("ms_client_id")).thenReturn("test-client");
		when(properties.getProperty("ms_tenant")).thenReturn("test-tenant");
		MockedStatic<SocialPropertiesUtil> social = mockStatic(SocialPropertiesUtil.class);
		social.when(SocialPropertiesUtil::getInstance).thenReturn(properties);
		return social;
	}

	private static AccessToken token(String access, String refresh) {
		AccessToken token = new AccessToken();
		token.setProvider(AuthProvider.MICROSOFT);
		token.setAccess_token(access);
		token.setExpires_in(3600);
		token.init();
		if (refresh != null) {
			token.addMetaValue(AbstractOAuthTokenFiller.REFRESH_TOKEN_KEY, refresh);
		}
		return token;
	}
}
