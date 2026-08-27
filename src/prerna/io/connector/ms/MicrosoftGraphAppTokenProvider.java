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

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.auth.AccessToken;
import prerna.security.HttpHelperUtility;

/**
 * Acquires app-only Microsoft Graph access tokens through the OAuth2 client
 * credentials flow.
 *
 * <p>
 * An instance is bound to one set of application credentials, so callers that
 * connect as a service identity rather than as the signed in user hold their own
 * provider and get their own cached token. The token is refreshed on demand and
 * reused until it is within {@link #EXPIRY_BUFFER_MILLIS} of expiring.
 * </p>
 *
 * <p>
 * Client credentials tokens carry the application permissions granted on the app
 * registration rather than individual scopes, which is why the requested scope
 * is {@code .default}. Note that {@code /me} endpoints cannot be used with these
 * tokens, since there is no signed in user for {@code /me} to resolve to.
 * </p>
 */
public class MicrosoftGraphAppTokenProvider {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftGraphAppTokenProvider.class);

	private static final Gson GSON = new Gson();

	private static final String LOGIN_BASE = "https://login.microsoftonline.com/";
	private static final String TOKEN_PATH = "/oauth2/v2.0/token";
	private static final String DEFAULT_SCOPE = MicrosoftTokenFiller.MS_GRAPH_BASE_API + "/.default";

	/**
	 * Renew this far ahead of the stated expiry so a token cannot lapse midway
	 * through a multi request operation such as a chunked upload.
	 */
	public static final long EXPIRY_BUFFER_MILLIS = 300_000L;

	/**
	 * Shortest window a token is ever cached for, so an absent or unreadable
	 * expires_in cannot turn every Graph call into a token request.
	 */
	private static final long MIN_CACHE_MILLIS = 30_000L;

	private final String tenantId;
	private final String clientId;
	private final String clientSecret;
	private final String scope;

	private String cachedToken = null;
	private long cachedTokenExpiryMillis = 0;

	/**
	 * @param tenantId     Azure AD tenant id or domain
	 * @param clientId     application (client) id of the app registration
	 * @param clientSecret client secret for the app registration
	 * @param scope        optional scope override; defaults to the Graph
	 *                     {@code .default} scope
	 * @throws IllegalArgumentException if any required credential is missing
	 */
	public MicrosoftGraphAppTokenProvider(String tenantId, String clientId, String clientSecret, String scope) {
		if (isBlank(tenantId)) {
			throw new IllegalArgumentException("A tenant is required to request an app-only Microsoft Graph token.");
		}
		if (isBlank(clientId)) {
			throw new IllegalArgumentException("A client id is required to request an app-only Microsoft Graph token.");
		}
		if (isBlank(clientSecret)) {
			throw new IllegalArgumentException(
					"A client secret is required to request an app-only Microsoft Graph token.");
		}
		this.tenantId = tenantId.trim();
		this.clientId = clientId.trim();
		this.clientSecret = clientSecret.trim();
		this.scope = isBlank(scope) ? DEFAULT_SCOPE : scope.trim();
	}

	/**
	 * Returns a valid app-only access token, refreshing it when the cached one is
	 * absent or close to expiring.
	 *
	 * @return the bearer token value
	 * @throws IllegalStateException if the token endpoint does not return a token
	 */
	public synchronized String getAccessToken() {
		if (this.cachedToken != null && System.currentTimeMillis() < this.cachedTokenExpiryMillis) {
			return this.cachedToken;
		}
		return refresh();
	}

	/**
	 * Discards the cached token so the next call acquires a fresh one. Used when
	 * Graph rejects a token that has not yet reached its stated expiry.
	 */
	public synchronized void invalidate() {
		this.cachedToken = null;
		this.cachedTokenExpiryMillis = 0;
	}

	/**
	 * @return the tenant this provider authenticates against
	 */
	public String getTenantId() {
		return this.tenantId;
	}

	/**
	 * @return the application (client) id this provider authenticates as
	 */
	public String getClientId() {
		return this.clientId;
	}

	/**
	 * The application permissions actually present on the current token, read from
	 * its {@code roles} claim.
	 *
	 * <p>
	 * Used to explain an authorization failure: a token with no roles means admin
	 * consent was never granted, while a token carrying only {@code Sites.Selected}
	 * means the permission is right but the per-site grant is missing. The claim is
	 * read for diagnostics only and is never treated as a security decision, so the
	 * signature is deliberately not validated.
	 * </p>
	 *
	 * @return the granted roles, empty when the token carries none or cannot be read
	 */
	public Set<String> getGrantedRoles() {
		String token;
		try {
			token = getAccessToken();
		} catch (RuntimeException e) {
			classLogger.debug("Unable to acquire a token to read its roles", e);
			return Collections.emptySet();
		}
		return readRoles(token);
	}

	/**
	 * Decodes the {@code roles} claim out of a JWT payload.
	 */
	static Set<String> readRoles(String token) {
		if (isBlank(token)) {
			return Collections.emptySet();
		}
		try {
			String[] segments = token.split("\\.");
			if (segments.length < 2) {
				return Collections.emptySet();
			}
			// base64url without padding is what JWT uses, so pad it back out
			String payload = segments[1];
			int padding = (4 - (payload.length() % 4)) % 4;
			payload = payload + "=".repeat(padding);
			String json = new String(Base64.getUrlDecoder().decode(payload), StandardCharsets.UTF_8);

			Map<String, Object> claims = GSON.fromJson(json, new TypeToken<Map<String, Object>>() {
			}.getType());
			if (claims == null) {
				return Collections.emptySet();
			}
			Object roles = claims.get("roles");
			if (!(roles instanceof List)) {
				return Collections.emptySet();
			}
			Set<String> granted = new LinkedHashSet<>();
			for (Object role : (List<?>) roles) {
				if (role != null) {
					granted.add(role.toString());
				}
			}
			return granted;
		} catch (Exception e) {
			classLogger.debug("Unable to read the roles claim out of the Microsoft Graph token", e);
			return Collections.emptySet();
		}
	}

	private String refresh() {
		String tokenEndpoint = LOGIN_BASE + this.tenantId + TOKEN_PATH;

		Map<String, String> params = new HashMap<>();
		params.put("client_id", this.clientId);
		params.put("client_secret", this.clientSecret);
		params.put("scope", this.scope);
		params.put("grant_type", "client_credentials");

		AccessToken newToken = HttpHelperUtility.getAccessToken(tokenEndpoint, params, true, true);
		if (newToken == null || isBlank(newToken.getAccess_token())) {
			throw new IllegalStateException(
					"Unable to acquire an app-only Microsoft Graph token for tenant " + this.tenantId
							+ ". Verify the client id, client secret and tenant, and that admin consent has been granted.");
		}

		this.cachedToken = newToken.getAccess_token();
		long expiresInMillis = newToken.getExpires_in() * 1000L;
		// renew ahead of the stated expiry, but keep a floor: a missing or unparsed
		// expires_in would otherwise collapse the window to zero and send a token
		// request per Graph call
		long usableMillis = Math.max(MIN_CACHE_MILLIS, expiresInMillis - EXPIRY_BUFFER_MILLIS);
		this.cachedTokenExpiryMillis = System.currentTimeMillis() + usableMillis;
		classLogger.debug("Acquired app-only Microsoft Graph token for tenant {}, usable for {} ms", this.tenantId,
				usableMillis);
		return this.cachedToken;
	}

	private static boolean isBlank(String value) {
		return value == null || value.trim().isEmpty();
	}
}
