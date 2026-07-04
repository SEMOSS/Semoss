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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;
import prerna.util.SocialPropertiesUtil;

/**
 * Base implementation of {@link IAccessTokenFiller} that owns the full OAuth2
 * authorization-code flow for a provider:
 * <ol>
 * <li>{@link #buildAuthorizeRedirect(String, String)} - the authorize
 * redirect</li>
 * <li>{@link #exchangeCodeForToken(String, String)} - code -&gt; token
 * exchange</li>
 * <li>{@link #fillAccessToken(AccessToken, String)} - userinfo -&gt; access
 * token</li>
 * </ol>
 * All three read their configuration from the social properties under the given
 * prefix, falling back to provider-specific defaults exposed as the protected
 * hooks below. A new provider is therefore added by defining an
 * {@link prerna.auth.AuthProvider} entry plus a subclass that overrides only
 * the hooks it needs - no changes are required in the web (Monolith) layer.
 */
public abstract class AbstractOAuthTokenFiller implements IAccessTokenFiller {

	protected static final Logger classLogger = LogManager.getLogger(AbstractOAuthTokenFiller.class);
	protected static final SocialPropertiesUtil socialData = SocialPropertiesUtil.getInstance();
	private static final String UTF8 = StandardCharsets.UTF_8.name();

	public static final String REFRESH_TOKEN_KEY = "refresh_token";

	// ------------------------------------------------------------------
	// provider override hooks - defaults keep everything driven by config
	// ------------------------------------------------------------------

	/** Default authorize endpoint used when {@code {prefix}auth_url} is not set. */
	protected String getDefaultAuthorizeUrl(String prefix) {
		return null;
	}

	/** Default token endpoint used when {@code {prefix}token_url} is not set. */
	protected String getDefaultTokenUrl(String prefix) {
		return null;
	}

	/**
	 * Default userinfo endpoint used when {@code {prefix}userinfo_url} is not set.
	 * Returning {@code null}/empty means this provider does not fetch a profile at
	 * login time and {@link #fillAccessToken(AccessToken, String)} becomes a no-op.
	 */
	protected String getDefaultUserInfoUrl(String prefix) {
		return null;
	}

	/**
	 * Default JMESPath query run against the JSON the IdP returns (from the
	 * userinfo endpoint, or the decoded id_token in {@link #usesIdToken() id_token
	 * mode}). It projects an ordered array of values out of that response, e.g.
	 * {@code [name, email, sub]} pulls three top-level fields; dotted/nested paths
	 * such as {@code result.name} are supported.
	 * <p>
	 * This is the "what to read from the provider" half;
	 * {@link #getDefaultBeanProps()} is the "where to put it" half. The two are
	 * matched by position.
	 */
	protected String getDefaultJsonPattern() {
		return null;
	}

	/**
	 * Default {@link prerna.auth.AccessToken} property names that receive the
	 * {@link #getDefaultJsonPattern() jsonPattern} results, matched by position:
	 * the i-th value the JMESPath query projects is written to the i-th property
	 * here (via a bean setter, e.g. {@code "name"} -&gt; {@code setName}). So a
	 * pattern of {@code [displayName, mail]} with beanProps {@code {"name",
	 * "email"}} maps the provider's {@code displayName} to {@code AccessToken.name}
	 * and its {@code mail} to {@code AccessToken.email}.
	 */
	protected String[] getDefaultBeanProps() {
		return null;
	}

	/** Default OAuth scope used when {@code {prefix}scope} is not set. */
	protected String getDefaultScope(String prefix) {
		return null;
	}

	/** Extra provider-specific query params appended to the authorize redirect. */
	protected Map<String, String> getExtraAuthorizeParams(String prefix) {
		return new LinkedHashMap<>();
	}

	/**
	 * Whether {@code &response_mode=query} is appended to the authorize redirect.
	 */
	protected boolean includeResponseMode() {
		return true;
	}

	/** Whether {@code &state=...} is appended to the authorize redirect. */
	protected boolean includeState() {
		return true;
	}

	/** Whether {@code scope} is sent as part of the token exchange request. */
	protected boolean includeScopeInTokenRequest() {
		return false;
	}

	/**
	 * Whether this provider is OIDC-style and carries the user's claims inside the
	 * {@code id_token} itself. When {@code true} the token exchange requests an id
	 * token and {@link #fillAccessToken(AccessToken, String)} parses the JWT
	 * payload instead of calling a userinfo endpoint.
	 */
	protected boolean usesIdToken() {
		return false;
	}

	/**
	 * Whether the userinfo endpoint must be called with an HTTP POST rather than a
	 * GET (e.g. Dropbox's {@code /users/get_current_account}). Only consulted when
	 * {@link #usesIdToken()} is {@code false}.
	 */
	protected boolean userInfoUsesPost() {
		return false;
	}

	// ------------------------------------------------------------------
	// step 1: authorize redirect
	// ------------------------------------------------------------------

	@Override
	public String buildAuthorizeRedirect(String prefix, String state) {
		String clientId = socialData.getProperty(prefix + "client_id");
		String redirectUri = socialData.getProperty(prefix + "redirect_uri");
		String scope = resolve(socialData.getProperty(prefix + "scope"), getDefaultScope(prefix));
		String authUrl = resolve(socialData.getProperty(prefix + "auth_url"), getDefaultAuthorizeUrl(prefix));

		if (isBlank(authUrl)) {
			throw new IllegalArgumentException("Authorize URL can not be null or empty");
		}

		StringBuilder sb = new StringBuilder(authUrl);
		sb.append(authUrl.contains("?") ? "&" : "?");
		sb.append("client_id=").append(clientId);
		sb.append("&response_type=code");
		sb.append("&redirect_uri=").append(encode(redirectUri));
		if (includeResponseMode()) {
			sb.append("&response_mode=query");
		}
		if (!isBlank(scope)) {
			sb.append("&scope=").append(encode(scope));
		}
		if (includeState()) {
			sb.append("&state=").append(state);
		}
		for (Map.Entry<String, String> extra : getExtraAuthorizeParams(prefix).entrySet()) {
			sb.append("&").append(extra.getKey()).append("=").append(extra.getValue());
		}

		String redirectUrl = sb.toString();
		classLogger.debug("Sending redirect for prefix {}", prefix);
		return redirectUrl;
	}

	// ------------------------------------------------------------------
	// step 2: authorization code -> access token
	// ------------------------------------------------------------------

	@Override
	public AccessToken exchangeCodeForToken(String prefix, String code) {
		String clientId = socialData.getProperty(prefix + "client_id");
		String clientSecret = socialData.getProperty(prefix + "secret_key");
		String redirectUri = socialData.getProperty(prefix + "redirect_uri");
		String scope = resolve(socialData.getProperty(prefix + "scope"), getDefaultScope(prefix));
		String tokenUrl = resolve(socialData.getProperty(prefix + "token_url"), getDefaultTokenUrl(prefix));

		if (isBlank(tokenUrl)) {
			throw new IllegalArgumentException("Token URL can not be null or empty");
		}

		Map<String, String> params = new HashMap<>();
		params.put("client_id", clientId);
		params.put("redirect_uri", redirectUri);
		params.put("code", code);
		params.put("grant_type", "authorization_code");
		params.put("client_secret", clientSecret);
		if (includeScopeInTokenRequest() && !isBlank(scope)) {
			params.put("scope", scope);
		}

		if (usesIdToken()) {
			return HttpHelperUtility.getIdToken(tokenUrl, params, true, true);
		}
		return HttpHelperUtility.getAccessToken(tokenUrl, params, true, true);
	}

	// ------------------------------------------------------------------
	// step 3: fill user profile from userinfo endpoint
	// ------------------------------------------------------------------

	@Override
	public void fillAccessToken(AccessToken accessToken, String prefix) {
		String jsonPattern = resolve(socialData.getProperty(prefix + "jsonPattern"), getDefaultJsonPattern());
		String[] beanProps = parseBeanProps(socialData.getProperty(prefix + "beanProps"));
		if (beanProps == null || beanProps.length == 0) {
			beanProps = getDefaultBeanProps();
		}
		boolean sanitize = Boolean.parseBoolean(socialData.getProperty(prefix + "sanitizeUserResponse"));

		String output;
		if (usesIdToken()) {
			// OIDC: the claims live in the id_token JWT itself, no userinfo call
			output = decodeJwtPayload(accessToken.getAccess_token());
		} else {
			String userInfoUrl = resolve(socialData.getProperty(prefix + "userinfo_url"),
					getDefaultUserInfoUrl(prefix));
			// no userinfo endpoint => profile is fetched lazily elsewhere; nothing to fill
			if (isBlank(userInfoUrl)) {
				return;
			}
			output = userInfoUsesPost()
					? HttpHelperUtility.makePostCall(userInfoUrl, accessToken.getAccess_token(), null, true)
					: HttpHelperUtility.makeGetCall(userInfoUrl, accessToken.getAccess_token(), null, true);
		}

		if (sanitize) {
			output = output.replace("\\", "\\\\");
		}
		BeanFiller.fillFromJson(output, jsonPattern, beanProps, accessToken);
	}

	// ------------------------------------------------------------------
	// legacy signatures - still used by the /userinfo and /login2 endpoints
	// ------------------------------------------------------------------

	@Override
	public void fillAccessToken(AccessToken accessToken, String userInfoUrl, String jsonPattern, String[] beanProps,
			Map<String, Object> params) {
		fillAccessToken(accessToken, userInfoUrl, jsonPattern, beanProps, params, false);
	}

	@Override
	public void fillAccessToken(AccessToken accessToken, String userInfoUrl, String jsonPattern, String[] beanProps,
			Map<String, Object> params, boolean sanitizeResponse) {
		userInfoUrl = resolve(userInfoUrl, getDefaultUserInfoUrl(null));
		jsonPattern = resolve(jsonPattern, getDefaultJsonPattern());
		if (beanProps == null || beanProps.length == 0) {
			beanProps = getDefaultBeanProps();
		}
		fetchAndFill(accessToken, userInfoUrl, jsonPattern, beanProps, params, sanitizeResponse);
	}

	// ------------------------------------------------------------------
	// shared helpers
	// ------------------------------------------------------------------

	private void fetchAndFill(AccessToken accessToken, String userInfoUrl, String jsonPattern, String[] beanProps,
			Map<String, Object> params, boolean sanitize) {
		String output = HttpHelperUtility.makeGetCall(userInfoUrl, accessToken.getAccess_token(), params, true);
		if (sanitize) {
			output = output.replace("\\", "\\\\");
		}
		BeanFiller.fillFromJson(output, jsonPattern, beanProps, accessToken);
	}

	/**
	 * Returns {@code configured} when it holds a non-blank value, else
	 * {@code fallback}.
	 */
	protected String resolve(String configured, String fallback) {
		return isBlank(configured) ? fallback : configured;
	}

	/**
	 * Parse a comma-separated beanProps value, or {@code null} when not configured.
	 */
	protected static String[] parseBeanProps(String beanProps) {
		if (isBlank(beanProps)) {
			return null;
		}
		return beanProps.split(",", -1);
	}

	protected static boolean isBlank(String value) {
		return value == null || value.trim().isEmpty();
	}

	/**
	 * Decode the JSON payload (claims) segment of a JWT / id_token.
	 *
	 * @param token a compact JWS ({@code header.payload.signature})
	 * @return the decoded JSON payload
	 */
	protected static String decodeJwtPayload(String token) {
		String[] parts = token.split("\\.");
		byte[] bytes = Base64.getUrlDecoder().decode(parts[1]);
		return new String(bytes, StandardCharsets.UTF_8);
	}

	private static String encode(String value) {
		try {
			return URLEncoder.encode(value, UTF8);
		} catch (UnsupportedEncodingException e) {
			// UTF-8 is always supported
			throw new IllegalStateException(e);
		}
	}

}
