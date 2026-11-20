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

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityExternalConnectorsUtils;
import prerna.io.connector.AbstractOAuthTokenFiller;

/** Server-only delegated credentials for consented work that runs without a session. */
public final class MicrosoftGraphCredentialStore {

	private MicrosoftGraphCredentialStore() {
	}

	public static AccessToken get(String microsoftUserId) {
		if (microsoftUserId == null || microsoftUserId.isBlank()) {
			return null;
		}
		Map<String, Object> row = SecurityExternalConnectorsUtils.getMicrosoftGraphCredential(microsoftUserId);
		if (row == null) {
			return null;
		}
		AccessToken token = new AccessToken();
		token.setProvider(AuthProvider.MICROSOFT);
		token.setId((String) row.get("userId"));
		token.setEmail((String) row.get("userEmail"));
		token.setAccess_token((String) row.get("accessToken"));
		token.addMetaValue(AbstractOAuthTokenFiller.REFRESH_TOKEN_KEY, (String) row.get("refreshToken"));
		Timestamp expiration = (Timestamp) row.get("tokenExpiration");
		// Unknown expiry must refresh; MicrosoftLoginUtils otherwise treats it as current.
		token.setExpires_in(3600);
		token.setStartTime(expiration == null ? 1L
				: expiration.toLocalDateTime().toInstant(ZoneOffset.UTC).toEpochMilli() - 3_600_000L);
		return token;
	}

	public static boolean isRefreshable(AccessToken token) {
		if (token == null || token.getProvider() != AuthProvider.MICROSOFT || token.getId() == null
				|| token.getId().isBlank() || token.getAccess_token() == null || token.getAccess_token().isBlank()) {
			return false;
		}
		Collection<String> refresh = token.getMetaValues(AbstractOAuthTokenFiller.REFRESH_TOKEN_KEY);
		return refresh != null && refresh.stream().anyMatch(value -> value != null && !value.isBlank());
	}

	public static void save(AccessToken token) {
		if (!isRefreshable(token)) {
			throw new IllegalArgumentException("Reconnect Microsoft to enable background inbox access.");
		}
		String refresh = token.getMetaValues(AbstractOAuthTokenFiller.REFRESH_TOKEN_KEY).stream()
				.filter(value -> value != null && !value.isBlank()).findFirst().orElseThrow();
		Timestamp expiration = null;
		if (token.getStartTime() > 0 && token.getExpires_in() > 0) {
			Instant instant = Instant.ofEpochMilli(token.getStartTime() + token.getExpires_in() * 1000L);
			expiration = Timestamp.valueOf(LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
		}
		SecurityExternalConnectorsUtils.upsertMicrosoftGraphCredential(token.getId(), token.getEmail(),
				token.getAccess_token(), refresh, expiration);
	}

	public static String accessToken(String microsoftUserId) throws Exception {
		AccessToken token = get(microsoftUserId);
		if (!isRefreshable(token)) {
			throw new IllegalArgumentException("Reconnect Microsoft to enable background inbox access.");
		}
		User user = new User();
		try {
			user.setAccessToken(token);
			String accessToken = MicrosoftLoginUtils.getValidAccessToken(user);
			AccessToken refreshed = user.getAccessToken(AuthProvider.MICROSOFT);
			if (!isRefreshable(refreshed) || !microsoftUserId.equals(refreshed.getId())) {
				throw new IllegalStateException("Microsoft background credentials could not be renewed.");
			}
			save(refreshed);
			return accessToken;
		} finally {
			user.removeUserMemory();
		}
	}
}
