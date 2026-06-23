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
package prerna.io.connector.github;

import java.io.IOException;
import java.io.StringReader;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.security.Signature;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hc.core5.http.ContentType;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.auth.utils.SecurityExternalConnectorsUtils;
import prerna.security.HttpHelperUtility;
import prerna.util.Utility;

/**
 * Authenticates as the configured GitHub App and mints short-lived installation
 * access tokens for talking to the GitHub REST API and git transport.
 * <p>
 * An installation only knows an account (org/user), not a repository. To act on
 * an installation's repositories we authenticate as the app: 1. build a
 * short-lived RS256 JWT signed with the app private key, 2. trade it for an
 * installation access token, 3. use that token (REST API or git over HTTPS as
 * {@code x-access-token:<token>}).
 * <p>
 * App config (id + private key) comes from the {@code GITHUB_APP} row in the
 * security database, populated by the app-manifest flow. The web-facing pieces
 * of the integration (manifest conversion, webhook/redirect URLs) live in the
 * Monolith web layer; this class holds only the token-minting core the engine
 * needs.
 */
public class GitHubAppClient {

	private static final String API = "https://api.github.com";

	/**
	 * GitHub must reach public URLs, not localhost. Browser/redirect endpoints are
	 * tunneled via ngrok or pinggy; webhooks are relayed via smee.io. Do not
	 * provide these values in RDF_Map to be the real application URL.
	 */

	/**
	 * example localhost value:
	 * "https://aqmil-2600-4040-10db-5b00-646e-8fd-4ebc-cebf.run.pinggy-free.link";
	 * or "https://elm-uselessly-laurel.ngrok-free.dev";
	 */
	private static final String GH_PUBLIC_ORIGIN_OVERRIDE = "GH_PUBLIC_ORIGIN_OVERRIDE";

	// example localhost value: "https://smee.io/2ms8Idfw7SQgYI9V"
	private static final String GH_WEBHOOK_URL_OVERRIDE = "GH_WEBHOOK_URL_OVERRIDE";

	private GitHubAppClient() {

	}

	/**
	 * Base url GitHub redirects the browser back to (origin overridden for ngrok or
	 * pinggy).
	 */
	public static String publicBaseUrl() {
		String appUrl = Utility.getApplicationUrl(); // e.g. http://localhost:8080/Monolith
		if (Utility.getDIHelperProperty(GH_PUBLIC_ORIGIN_OVERRIDE) != null) {
			String link = Utility.getDIHelperProperty(GH_PUBLIC_ORIGIN_OVERRIDE);
			if (link != null && !(link = link.trim()).isEmpty()) {
				int schemeEnd = appUrl.indexOf("://");
				int pathStart = schemeEnd < 0 ? -1 : appUrl.indexOf('/', schemeEnd + 3);
				String path = pathStart < 0 ? "" : appUrl.substring(pathStart);
				return link + path;
			}
		}
		return appUrl;
	}

	/**
	 * Public url GitHub POSTs webhook events to (relayed via smee.io in local
	 * testing).
	 */
	public static String webhookUrl() {
		if (Utility.getDIHelperProperty(GH_WEBHOOK_URL_OVERRIDE) != null) {
			String link = Utility.getDIHelperProperty(GH_WEBHOOK_URL_OVERRIDE);
			if (link != null && !(link = link.trim()).isEmpty()) {
				return link;
			}
		}

		return publicBaseUrl() + "/github/webhook";
	}

	/**
	 * Builds an absolute URL into the frontend (client) app for a hash route, e.g.
	 * {@code frontendUrl("/settings/github-app?githubApp=created")}. The client is
	 * served as a sibling of this servlet's context root - the same pattern the
	 * install callback uses to return the user to the app.
	 *
	 * @param hashRoute the client hash route (and optional query string) to land
	 *                  on, with or without a leading slash
	 * @return an absolute, browser-navigable URL into the client app
	 */
	public static String frontendUrl(String hashRoute) {
		String base = publicBaseUrl(); // e.g. https://origin/Monolith
		int lastSlash = base.lastIndexOf('/');
		if (lastSlash > -1) {
			base = base.substring(0, lastSlash); // https://origin
		}
		String suffix = hashRoute.startsWith("/") ? hashRoute : "/" + hashRoute;
		return base + "/SemossWeb/packages/client/dist/#" + suffix;
	}

	/** A repository an installation can access. */
	public record Repo(long id, String fullName, String defaultBranch) {
	}

	/**
	 * An installation of the app on an account (user or org). An installation is
	 * the unit a project links to; it knows an account, not a repository.
	 *
	 * @param id                  the installation id used to mint tokens / list
	 *                            repos
	 * @param accountLogin        the org/user login the app is installed on
	 * @param accountType         {@code "User"} or {@code "Organization"}
	 * @param repositorySelection {@code "all"} or {@code "selected"}
	 * @param suspended           whether the account owner has suspended it (cannot
	 *                            mint tokens while suspended)
	 */
	public record Installation(long id, String accountLogin, String accountType, String repositorySelection,
			boolean suspended) {
	}

	/**
	 * Signals that a user-to-server call failed because the user token is missing,
	 * expired, or revoked (GitHub returned {@code 401}). The web layer maps this to
	 * a {@code needsAuth} response so the user is sent back through the GitHub
	 * authorization redirect; we deliberately do not persist or refresh user
	 * tokens.
	 */
	public static class UserAuthRequiredException extends RuntimeException {
		private static final long serialVersionUID = 1L;

		public UserAuthRequiredException(String message) {
			super(message);
		}
	}

	/**
	 * Loads the single configured GitHub App row, or fails if none is set up yet.
	 *
	 * @return the app config map keyed by the aliases from
	 *         {@link SecurityExternalConnectorsUtils#getGitHubApp()}
	 */
	private static Map<String, Object> appConfig() {
		Map<String, Object> app = SecurityExternalConnectorsUtils.getGitHubApp();
		if (app == null) {
			throw new IllegalStateException("No GitHub App is configured");
		}
		return app;
	}

	/** The standard GitHub REST headers (Accept + API version), unauthenticated. */
	private static Map<String, String> githubHeaders() {
		Map<String, String> headers = new HashMap<>();
		headers.put("Accept", "application/vnd.github+json");
		headers.put("X-GitHub-Api-Version", "2022-11-28");
		return headers;
	}

	/** The standard GitHub REST headers plus a {@code Bearer} authorization. */
	private static Map<String, String> githubHeaders(String bearer) {
		Map<String, String> headers = githubHeaders();
		headers.put("Authorization", "Bearer " + bearer);
		return headers;
	}

	/**
	 * Exchanges a one-time app-manifest code for the newly created app's full
	 * config (id, slug, pem, client/webhook secrets, ...). This call is
	 * unauthenticated - the short-lived code is the credential, and it expires ~1
	 * hour after GitHub issues it.
	 */
	public static JsonObject convertManifest(String code) throws Exception {
		String body = HttpHelperUtility.postRequestStringBody(API + "/app-manifests/" + code + "/conversions",
				githubHeaders(), null, null, null, null, null);
		return JsonParser.parseString(body).getAsJsonObject();
	}

	/**
	 * Exchanges the app JWT for a short-lived installation access token. The token
	 * expires ~1 hour after issue, so mint one per operation rather than persisting
	 * it.
	 *
	 * @param installationId the installation granting repository access
	 * @return a {@code ghs_...} installation access token
	 * @throws Exception if the JWT cannot be built or GitHub rejects the request
	 */
	public static String getInstallationToken(String installationId) throws Exception {
		String jwt = createJwt();

		String body = HttpHelperUtility.postRequestStringBody(
				API + "/app/installations/" + installationId + "/access_tokens", githubHeaders(jwt), null, null, null,
				null, null);
		return JsonParser.parseString(body).getAsJsonObject().get("token").getAsString();
	}

	/**
	 * Lists every repository the given installation has access to.
	 *
	 * @param installationId the installation to enumerate
	 * @return the repositories the installation can access (id + owner/name)
	 * @throws Exception if a token cannot be minted or GitHub rejects the request
	 */
	public static List<Repo> getInstallationRepositories(String installationId) throws Exception {
		String token = getInstallationToken(installationId);

		// Page through; an "all repositories" install on a large org can return
		// more than one page. Stop on the first short or empty page.
		Map<String, String> headers = githubHeaders(token);
		List<Repo> out = new ArrayList<>();
		int page = 1;
		while (true) {
			String body = HttpHelperUtility.getRequest(API + "/installation/repositories?per_page=100&page=" + page,
					headers, null, null, null);

			JsonArray repos = JsonParser.parseString(body).getAsJsonObject().getAsJsonArray("repositories");
			if (repos == null || repos.size() == 0) {
				break;
			}
			for (JsonElement el : repos) {
				JsonObject r = el.getAsJsonObject();
				out.add(new Repo(r.get("id").getAsLong(), r.get("full_name").getAsString(),
						asStringOrNull(r, "default_branch")));
			}
			if (repos.size() < 100) {
				break;
			}
			page++;
		}
		return out;
	}

	/**
	 * Lists the branch names of a repository the installation can access.
	 *
	 * @param installationId the installation granting repository access
	 * @param repoFullName   the {@code owner/repo} full name of the repository
	 * @return the repository's branch names, newest API page order
	 * @throws Exception if a token cannot be minted or GitHub rejects the request
	 */
	public static List<String> listRepositoryBranches(String installationId, String repoFullName) throws Exception {
		String token = getInstallationToken(installationId);

		Map<String, String> headers = githubHeaders(token);
		List<String> out = new ArrayList<>();
		int page = 1;
		while (true) {
			String body = HttpHelperUtility.getRequest(
					API + "/repos/" + repoFullName + "/branches?per_page=100&page=" + page, headers, null, null, null);

			JsonArray branches = JsonParser.parseString(body).getAsJsonArray();
			if (branches == null || branches.size() == 0) {
				break;
			}
			for (JsonElement el : branches) {
				out.add(el.getAsJsonObject().get("name").getAsString());
			}
			if (branches.size() < 100) {
				break;
			}
			page++;
		}
		return out;
	}

	/**
	 * Checks whether an installation is still usable by the app.
	 * <p>
	 * Calls {@code GET /app/installations/{id}} as the app. An installation the
	 * account owner has uninstalled returns {@code 404}; a suspended one returns
	 * {@code 200} with a non-null {@code suspended_at}. Both mean the installation
	 * can no longer mint tokens, so both are reported as invalid.
	 *
	 * @param installationId the installation to check
	 * @return {@code true} if the installation exists and is not suspended;
	 *         {@code false} if it was uninstalled (404) or is suspended
	 * @throws Exception if the JWT cannot be built or GitHub returns an unexpected
	 *                   status
	 */
	public static boolean isInstallationValid(String installationId) throws Exception {
		String jwt = createJwt();

		String body;
		try {
			body = HttpHelperUtility.getRequest(API + "/app/installations/" + installationId, githubHeaders(jwt), null,
					null, null);
		} catch (IllegalArgumentException e) {
			// a deleted/uninstalled installation returns 404 - that is a valid
			// "not usable" answer here, not an error
			if (e.getMessage() != null && e.getMessage().contains("returned HTTP 404")) {
				return false;
			}
			throw e;
		}
		JsonObject obj = JsonParser.parseString(body).getAsJsonObject();
		JsonElement suspendedAt = obj.get("suspended_at");
		boolean suspended = suspendedAt != null && !suspendedAt.isJsonNull();
		return !suspended;
	}

	/**
	 * Lists every installation of the app, across all accounts that have installed
	 * it. This lets the user pick which existing installation to link a project to,
	 * instead of depending on the single installation_id GitHub hands back on the
	 * install/setup redirect - so an app already installed on GitHub (out of band,
	 * or for a previous project) can still be connected.
	 * <p>
	 * Calls {@code GET /app/installations} as the app. Suspended installations are
	 * returned but flagged so the UI can disable them (a suspended installation
	 * cannot mint tokens).
	 *
	 * @return the app's installations (id + account + repository selection),
	 *         GitHub's page order
	 * @throws Exception if the JWT cannot be built or GitHub rejects the request
	 */
	public static List<Installation> listInstallations() throws Exception {
		String jwt = createJwt();

		// Page through; an app installed on many accounts can span pages. Stop on
		// the first short or empty page.
		Map<String, String> headers = githubHeaders(jwt);
		List<Installation> out = new ArrayList<>();
		int page = 1;
		while (true) {
			String body = HttpHelperUtility.getRequest(API + "/app/installations?per_page=100&page=" + page, headers,
					null, null, null);

			JsonArray installations = JsonParser.parseString(body).getAsJsonArray();
			if (installations == null || installations.size() == 0) {
				break;
			}
			for (JsonElement el : installations) {
				JsonObject obj = el.getAsJsonObject();
				JsonObject account = obj.getAsJsonObject("account");
				String accountLogin = account == null ? null : asStringOrNull(account, "login");
				String accountType = account == null ? null : asStringOrNull(account, "type");
				JsonElement suspendedAt = obj.get("suspended_at");
				boolean suspended = suspendedAt != null && !suspendedAt.isJsonNull();
				out.add(new Installation(obj.get("id").getAsLong(), accountLogin, accountType,
						asStringOrNull(obj, "repository_selection"), suspended));
			}
			if (installations.size() < 100) {
				break;
			}
			page++;
		}
		return out;
	}

	/**
	 * Returns the most recent deliveries of the app's webhook, newest first.
	 * <p>
	 * Calls {@code GET /app/hook/deliveries} as the app. GitHub retains only a
	 * limited recent window of deliveries (not full history), so treat this as a
	 * live diagnostics feed, not an audit log. Each entry summarizes one delivery;
	 * use its {@code id} with the delivery-detail endpoint for the full
	 * request/response payloads.
	 *
	 * @param perPage maximum deliveries to return; clamped to GitHub's 1-100 range
	 *                (a non-positive value defaults to 30)
	 * @return delivery summaries (id, guid, event, action, status, statusCode,
	 *         deliveredAt, duration, redelivery, installationId, repositoryId),
	 *         newest first
	 * @throws Exception if the JWT cannot be built or GitHub rejects the request
	 */
	public static List<Map<String, Object>> getRecentWebhookDeliveries(int perPage) throws Exception {
		String jwt = createJwt();
		int limit = (perPage <= 0) ? 30 : Math.min(perPage, 100);

		String body = HttpHelperUtility.getRequest(API + "/app/hook/deliveries?per_page=" + limit, githubHeaders(jwt),
				null, null, null);

		List<Map<String, Object>> out = new ArrayList<>();
		JsonArray deliveries = JsonParser.parseString(body).getAsJsonArray();
		for (JsonElement el : deliveries) {
			JsonObject d = el.getAsJsonObject();
			Map<String, Object> row = new LinkedHashMap<>();
			row.put("id", asLongOrNull(d, "id"));
			row.put("guid", asStringOrNull(d, "guid"));
			row.put("event", asStringOrNull(d, "event"));
			row.put("action", asStringOrNull(d, "action"));
			row.put("status", asStringOrNull(d, "status"));
			row.put("statusCode", asIntOrNull(d, "status_code"));
			row.put("deliveredAt", asStringOrNull(d, "delivered_at"));
			row.put("duration", asDoubleOrNull(d, "duration"));
			row.put("redelivery", asBoolOrNull(d, "redelivery"));
			row.put("installationId", asLongOrNull(d, "installation_id"));
			row.put("repositoryId", asLongOrNull(d, "repository_id"));
			out.add(row);
		}
		return out;
	}

	// ---------------------------------------------------------------------
	// User-to-server (per-user scoping). These use a short-lived user access
	// token, never the app JWT, so a user only sees the installations and repos
	// GitHub says THEY can access. The token is used immediately and not
	// persisted; a 401 surfaces as UserAuthRequiredException so the caller
	// re-runs the authorization redirect rather than failing hard.
	// ---------------------------------------------------------------------

	/**
	 * Exchanges a GitHub user-authorization code for a user access token
	 * ({@code ghu_...}). Posts to github.com (not the API host) with the app's
	 * OAuth client credentials. The token is meant to be used once for scoping
	 * reads and then discarded - do not persist it.
	 *
	 * @param code the one-time code from the authorize redirect
	 * @return a user-to-server access token
	 * @throws Exception if the app has no client credentials or GitHub rejects the
	 *                   exchange
	 */
	public static String exchangeUserCode(String code) throws Exception {
		Map<String, Object> app = appConfig();
		String clientId = (String) app.get("clientId");
		String clientSecret = (String) app.get("clientSecret");
		if (clientId == null || clientId.isEmpty() || clientSecret == null || clientSecret.isEmpty()) {
			throw new IllegalStateException("Configured GitHub App has no OAuth client credentials");
		}

		String form = "client_id=" + enc(clientId) + "&client_secret=" + enc(clientSecret) + "&code=" + enc(code);
		Map<String, String> headers = new HashMap<>();
		headers.put("Accept", "application/json");

		String body = HttpHelperUtility.postRequestStringBody("https://github.com/login/oauth/access_token", headers,
				form, ContentType.APPLICATION_FORM_URLENCODED, null, null, null);

		JsonObject obj = JsonParser.parseString(body).getAsJsonObject();
		// GitHub returns 200 with an {error,...} body for a bad or expired code
		if (obj.has("error")) {
			throw new IllegalStateException("GitHub user code exchange failed: " + asStringOrNull(obj, "error"));
		}
		JsonElement token = obj.get("access_token");
		if (token == null || token.isJsonNull()) {
			throw new IllegalStateException("GitHub user code exchange returned no access token");
		}
		return token.getAsString();
	}

	/**
	 * Lists the installations of THIS app that the given user can access, using
	 * their user token. Unlike {@link #listInstallations()} (app-wide, app JWT),
	 * this returns only accounts the user belongs to, so one tenant never sees
	 * another's installations.
	 *
	 * @param userToken a user-to-server access token (see
	 *                  {@link #exchangeUserCode})
	 * @return the installations the user can access, limited to this app
	 * @throws UserAuthRequiredException if the token is missing/expired/revoked
	 *                                   (401)
	 * @throws Exception                 if GitHub otherwise rejects the request
	 */
	public static List<Installation> listUserInstallations(String userToken) throws Exception {
		long appId = ((Number) appConfig().get("appId")).longValue();

		Map<String, String> headers = githubHeaders(userToken);
		List<Installation> out = new ArrayList<>();
		int page = 1;
		while (true) {
			JsonObject obj = getUserJson(API + "/user/installations?per_page=100&page=" + page, headers);
			JsonArray installations = obj.getAsJsonArray("installations");
			if (installations == null || installations.size() == 0) {
				break;
			}
			for (JsonElement el : installations) {
				JsonObject inst = el.getAsJsonObject();
				// /user/installations spans every app the user can access; keep only ours
				Long instAppId = asLongOrNull(inst, "app_id");
				if (instAppId == null || instAppId.longValue() != appId) {
					continue;
				}
				JsonObject account = inst.getAsJsonObject("account");
				String accountLogin = account == null ? null : asStringOrNull(account, "login");
				String accountType = account == null ? null : asStringOrNull(account, "type");
				JsonElement suspendedAt = inst.get("suspended_at");
				boolean suspended = suspendedAt != null && !suspendedAt.isJsonNull();
				out.add(new Installation(inst.get("id").getAsLong(), accountLogin, accountType,
						asStringOrNull(inst, "repository_selection"), suspended));
			}
			if (installations.size() < 100) {
				break;
			}
			page++;
		}
		return out;
	}

	/**
	 * Lists the repositories the given user can access within one installation,
	 * using their user token. The user-scoped counterpart of
	 * {@link #getInstallationRepositories(String)}; the repo picker and
	 * {@code /install/select} validate against this so a user cannot select a repo
	 * they cannot see.
	 *
	 * @param userToken      a user-to-server access token
	 * @param installationId the installation to enumerate
	 * @return the repositories the user can access in that installation
	 * @throws UserAuthRequiredException if the token is missing/expired/revoked
	 *                                   (401)
	 * @throws Exception                 if GitHub otherwise rejects the request
	 */
	public static List<Repo> listUserInstallationRepositories(String userToken, String installationId)
			throws Exception {
		Map<String, String> headers = githubHeaders(userToken);
		List<Repo> out = new ArrayList<>();
		int page = 1;
		while (true) {
			JsonObject obj = getUserJson(
					API + "/user/installations/" + installationId + "/repositories?per_page=100&page=" + page, headers);
			JsonArray repos = obj.getAsJsonArray("repositories");
			if (repos == null || repos.size() == 0) {
				break;
			}
			for (JsonElement el : repos) {
				JsonObject r = el.getAsJsonObject();
				out.add(new Repo(r.get("id").getAsLong(), r.get("full_name").getAsString(),
						asStringOrNull(r, "default_branch")));
			}
			if (repos.size() < 100) {
				break;
			}
			page++;
		}
		return out;
	}

	/**
	 * GETs a user-scoped endpoint, translating a 401 into
	 * {@link UserAuthRequiredException} so the web layer can prompt
	 * re-authorization instead of treating it as a hard failure.
	 */
	private static JsonObject getUserJson(String url, Map<String, String> headers) {
		try {
			return JsonParser.parseString(HttpHelperUtility.getRequest(url, headers, null, null, null))
					.getAsJsonObject();
		} catch (IllegalArgumentException e) {
			if (e.getMessage() != null && e.getMessage().contains("returned HTTP 401")) {
				throw new UserAuthRequiredException("GitHub user token is missing, expired, or revoked");
			}
			throw e;
		}
	}

	/** URL-encodes a form value (UTF-8). */
	private static String enc(String value) {
		return URLEncoder.encode(value, StandardCharsets.UTF_8);
	}

	private static String asStringOrNull(JsonObject o, String key) {
		JsonElement e = o.get(key);
		return (e == null || e.isJsonNull()) ? null : e.getAsString();
	}

	private static Long asLongOrNull(JsonObject o, String key) {
		JsonElement e = o.get(key);
		return (e == null || e.isJsonNull()) ? null : e.getAsLong();
	}

	private static Integer asIntOrNull(JsonObject o, String key) {
		JsonElement e = o.get(key);
		return (e == null || e.isJsonNull()) ? null : e.getAsInt();
	}

	private static Double asDoubleOrNull(JsonObject o, String key) {
		JsonElement e = o.get(key);
		return (e == null || e.isJsonNull()) ? null : e.getAsDouble();
	}

	private static Boolean asBoolOrNull(JsonObject o, String key) {
		JsonElement e = o.get(key);
		return (e == null || e.isJsonNull()) ? null : e.getAsBoolean();
	}

	/** Builds the RS256 JWT (max 10 min lifetime) that identifies the app. */
	private static String createJwt() throws Exception {
		Map<String, Object> app = appConfig();
		long appId = ((Number) app.get("appId")).longValue();
		String privateKeyPem = (String) app.get("privateKey");
		if (privateKeyPem == null || privateKeyPem.isEmpty()) {
			throw new IllegalStateException("Configured GitHub App has no private key");
		}

		long now = Instant.now().getEpochSecond();
		String headerJson = "{\"alg\":\"RS256\",\"typ\":\"JWT\"}";
		// iat backdated 60s for clock drift; exp 9 min out (GitHub caps at 10)
		String payloadJson = "{\"iat\":" + (now - 60) + ",\"exp\":" + (now + 540) + ",\"iss\":\"" + appId + "\"}";

		Base64.Encoder b64 = Base64.getUrlEncoder().withoutPadding();
		String header = b64.encodeToString(headerJson.getBytes(StandardCharsets.UTF_8));
		String payload = b64.encodeToString(payloadJson.getBytes(StandardCharsets.UTF_8));
		String signingInput = header + "." + payload;

		Signature signer = Signature.getInstance("SHA256withRSA");
		signer.initSign(loadPrivateKey(privateKeyPem));
		signer.update(signingInput.getBytes(StandardCharsets.UTF_8));
		String signature = b64.encodeToString(signer.sign());

		return signingInput + "." + signature;
	}

	/**
	 * Parses the PEM private key. GitHub issues PKCS#1 keys, so use BouncyCastle.
	 */
	private static PrivateKey loadPrivateKey(String privateKeyPem) throws IOException {
		try (PEMParser parser = new PEMParser(new StringReader(privateKeyPem))) {
			Object obj = parser.readObject();
			JcaPEMKeyConverter converter = new JcaPEMKeyConverter();
			if (obj instanceof PEMKeyPair) {
				return converter.getPrivateKey(((PEMKeyPair) obj).getPrivateKeyInfo());
			} else if (obj instanceof PrivateKeyInfo) {
				return converter.getPrivateKey((PrivateKeyInfo) obj);
			}
			throw new IOException("Unsupported GitHub App private key format");
		}
	}

}
