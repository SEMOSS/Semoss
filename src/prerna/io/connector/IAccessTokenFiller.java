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

import java.util.Map;

import prerna.auth.AccessToken;

public interface IAccessTokenFiller {

	/**
	 * Populate the access token's user-profile fields from the provider, using
	 * explicitly supplied parsing configuration.
	 * <p>
	 * Implementations call the {@code userInfoUrl}, then apply {@code jsonPattern}
	 * (a JMESPath query projecting an ordered array of values out of the JSON
	 * response) and {@code beanProps} (the {@link AccessToken} property names that
	 * receive those values, matched by position) to fill the token. Any argument
	 * left {@code null}/empty falls back to the implementation's own default.
	 * <p>
	 * This is the lower-level, caller-driven form; the unified login flow uses
	 * {@link #fillAccessToken(AccessToken, String)} instead, which reads all of
	 * this configuration (and its defaults) internally from the social properties.
	 *
	 * @param accessToken the token to populate (must already hold an access token)
	 * @param userInfoUrl the provider endpoint returning the user's profile JSON
	 * @param jsonPattern the JMESPath query run against that JSON (the "what to
	 *                    read")
	 * @param beanProps   the {@link AccessToken} properties each projected value
	 *                    maps to, by position (the "where to put it")
	 * @param params      optional extra query parameters for the userinfo call (may
	 *                    be {@code null})
	 */
	void fillAccessToken(AccessToken accessToken, String userInfoUrl, String jsonPattern, String[] beanProps,
			Map<String, Object> params);

	/**
	 * Same as {@link #fillAccessToken(AccessToken, String, String, String[], Map)}
	 * but allows escaping problematic characters in the raw provider response
	 * before it is parsed.
	 *
	 * @param accessToken      the token to populate (must already hold an access
	 *                         token)
	 * @param userInfoUrl      the provider endpoint returning the user's profile
	 *                         JSON
	 * @param jsonPattern      the JMESPath query run against that JSON (the "what
	 *                         to read")
	 * @param beanProps        the {@link AccessToken} properties each projected
	 *                         value maps to, by position (the "where to put it")
	 * @param params           optional extra query parameters for the userinfo call
	 *                         (may be {@code null})
	 * @param sanitizeResponse when {@code true}, escapes backslashes in the
	 *                         response so an otherwise-unparseable payload becomes
	 *                         valid JSON (rare; needed for some IdPs that emit
	 *                         unescaped chars)
	 */
	void fillAccessToken(AccessToken accessToken, String userInfoUrl, String jsonPattern, String[] beanProps,
			Map<String, Object> params, boolean sanitizeResponse);

	/**
	 * Attempt to refresh the provided access token.
	 * <p>
	 * Default behavior is unsupported and returns {@code null}. Provider-specific
	 * fillers can override to support delegated refresh token flows.
	 *
	 * @param accessToken token to refresh
	 * @param params      optional provider-specific parameters
	 * @return refreshed token (or null when refresh is not supported/possible)
	 */
	default AccessToken refreshAccessToken(AccessToken accessToken, Map<String, Object> params) {
		return null;
	}

	/**
	 * Build the provider's authorize (login) redirect URL for the OAuth2
	 * authorization-code flow.
	 * <p>
	 * The default implementation is unsupported; {@link AbstractOAuthTokenFiller}
	 * provides the generic behavior and provider fillers override the hooks they
	 * need. Only fillers routed through the unified {@code /login/{provider}}
	 * endpoint need to support this.
	 *
	 * @param prefix social.properties prefix for the provider (e.g.
	 *               {@code "google_"})
	 * @param state  opaque state value to round-trip through the provider
	 * @return fully built authorize redirect URL
	 */
	default String buildAuthorizeRedirect(String prefix, String state) {
		throw new UnsupportedOperationException(
				getClass().getSimpleName() + " does not support the unified login flow");
	}

	/**
	 * Exchange an OAuth2 authorization code for an access token.
	 * <p>
	 * The default implementation is unsupported; {@link AbstractOAuthTokenFiller}
	 * provides the generic token exchange and provider fillers override the hooks
	 * they need.
	 *
	 * @param prefix social.properties prefix for the provider
	 * @param code   authorization code returned on the OAuth callback
	 * @return the access token, or {@code null} when authentication failed
	 */
	default AccessToken exchangeCodeForToken(String prefix, String code) {
		throw new UnsupportedOperationException(
				getClass().getSimpleName() + " does not support the unified login flow");
	}

	/**
	 * Populate the access token with the user's profile details, reading all
	 * {@code userinfo_url}/{@code jsonPattern}/{@code beanProps} configuration (and
	 * their provider defaults) internally from the social properties.
	 * <p>
	 * Implementations that do not fetch a profile at login time (the token is
	 * enough, profile fetched lazily elsewhere) should no-op.
	 *
	 * @param accessToken token to populate
	 * @param prefix      social.properties prefix for the provider
	 */
	default void fillAccessToken(AccessToken accessToken, String prefix) {
		throw new UnsupportedOperationException(
				getClass().getSimpleName() + " does not support the unified login flow");
	}

	/**
	 * Whether this provider uses PKCE (Proof Key for Code Exchange). When
	 * {@code true}, the login endpoint generates a {@code code_verifier} before the
	 * authorize redirect (stashing it in the session), passes the derived
	 * {@code code_challenge} to
	 * {@link #buildAuthorizeRedirect(String, String, String)} and passes the
	 * verifier back to {@link #exchangeCodeForToken(String, String, String)}.
	 *
	 * @return {@code true} to enable the PKCE variants
	 */
	default boolean usesPKCE() {
		return false;
	}

	/**
	 * PKCE-aware variant of {@link #buildAuthorizeRedirect(String, String)}. The
	 * default ignores the challenge and delegates to the non-PKCE form; PKCE
	 * providers override to append
	 * {@code code_challenge}/{@code code_challenge_method}.
	 *
	 * @param prefix        social.properties prefix for the provider
	 * @param state         opaque state value to round-trip through the provider
	 * @param codeChallenge the S256 PKCE code challenge ({@code null} when not
	 *                      PKCE)
	 * @return fully built authorize redirect URL
	 */
	default String buildAuthorizeRedirect(String prefix, String state, String codeChallenge) {
		return buildAuthorizeRedirect(prefix, state);
	}

	/**
	 * PKCE-aware variant of {@link #exchangeCodeForToken(String, String)}. The
	 * default ignores the verifier and delegates to the non-PKCE form; PKCE
	 * providers override to send the {@code code_verifier} in the token exchange.
	 *
	 * @param prefix       social.properties prefix for the provider
	 * @param code         authorization code returned on the OAuth callback
	 * @param codeVerifier the PKCE code verifier generated at authorize time
	 *                     ({@code null} when not PKCE)
	 * @return the access token, or {@code null} when authentication failed
	 */
	default AccessToken exchangeCodeForToken(String prefix, String code, String codeVerifier) {
		return exchangeCodeForToken(prefix, code);
	}

}
