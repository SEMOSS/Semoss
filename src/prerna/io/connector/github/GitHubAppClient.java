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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.security.Signature;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.auth.utils.SecurityExternalConnectorsUtils;
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

	private static final HttpClient HTTP = HttpClient.newHttpClient();
	private static final String API = "https://api.github.com";

	/**
	 * GitHub must reach public URLs, not localhost. Browser/redirect endpoints are
	 * tunneled via ngrok or pinggy; webhooks are relayed via smee.io. Do not
	 * provide these values in RDF_Map to be the real application URL.
	 */

	/**
	 * // example localhost value: //
	 * "https://aqmil-2600-4040-10db-5b00-646e-8fd-4ebc-cebf.run.pinggy-free.link";
	 * // or // "https://elm-uselessly-laurel.ngrok-free.dev"; ///
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

	/**
	 * Exchanges a one-time app-manifest code for the newly created app's full
	 * config (id, slug, pem, client/webhook secrets, ...). This call is
	 * unauthenticated - the short-lived code is the credential, and it expires ~1
	 * hour after GitHub issues it.
	 */
	public static JsonObject convertManifest(String code) throws Exception {
		HttpRequest request = HttpRequest.newBuilder().uri(URI.create(API + "/app-manifests/" + code + "/conversions"))
				.header("Accept", "application/vnd.github+json").header("X-GitHub-Api-Version", "2022-11-28")
				.POST(HttpRequest.BodyPublishers.noBody()).build();

		HttpResponse<String> resp = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
		if (resp.statusCode() != 201) {
			throw new IOException("GitHub manifest conversion failed: " + resp.statusCode() + " " + resp.body());
		}
		return JsonParser.parseString(resp.body()).getAsJsonObject();
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

		HttpRequest request = HttpRequest.newBuilder()
				.uri(URI.create(API + "/app/installations/" + installationId + "/access_tokens"))
				.header("Authorization", "Bearer " + jwt).header("Accept", "application/vnd.github+json")
				.header("X-GitHub-Api-Version", "2022-11-28").POST(HttpRequest.BodyPublishers.noBody()).build();

		HttpResponse<String> resp = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
		if (resp.statusCode() != 201) {
			throw new IOException("GitHub installation-token failed: " + resp.statusCode() + " " + resp.body());
		}
		return JsonParser.parseString(resp.body()).getAsJsonObject().get("token").getAsString();
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
		List<Repo> out = new ArrayList<>();
		int page = 1;
		while (true) {
			HttpRequest request = HttpRequest.newBuilder()
					.uri(URI.create(API + "/installation/repositories?per_page=100&page=" + page))
					.header("Authorization", "Bearer " + token).header("Accept", "application/vnd.github+json")
					.header("X-GitHub-Api-Version", "2022-11-28").GET().build();

			HttpResponse<String> resp = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
			if (resp.statusCode() != 200) {
				throw new IOException("GitHub list-repositories failed: " + resp.statusCode() + " " + resp.body());
			}

			JsonArray repos = JsonParser.parseString(resp.body()).getAsJsonObject().getAsJsonArray("repositories");
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

		List<String> out = new ArrayList<>();
		int page = 1;
		while (true) {
			HttpRequest request = HttpRequest.newBuilder()
					.uri(URI.create(API + "/repos/" + repoFullName + "/branches?per_page=100&page=" + page))
					.header("Authorization", "Bearer " + token).header("Accept", "application/vnd.github+json")
					.header("X-GitHub-Api-Version", "2022-11-28").GET().build();

			HttpResponse<String> resp = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
			if (resp.statusCode() != 200) {
				throw new IOException("GitHub list-branches failed: " + resp.statusCode() + " " + resp.body());
			}

			JsonArray branches = JsonParser.parseString(resp.body()).getAsJsonArray();
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

		HttpRequest request = HttpRequest.newBuilder().uri(URI.create(API + "/app/installations/" + installationId))
				.header("Authorization", "Bearer " + jwt).header("Accept", "application/vnd.github+json")
				.header("X-GitHub-Api-Version", "2022-11-28").GET().build();

		HttpResponse<String> resp = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
		if (resp.statusCode() == 404) {
			return false;
		}
		if (resp.statusCode() != 200) {
			throw new IOException("GitHub get-installation failed: " + resp.statusCode() + " " + resp.body());
		}
		JsonObject obj = JsonParser.parseString(resp.body()).getAsJsonObject();
		JsonElement suspendedAt = obj.get("suspended_at");
		boolean suspended = suspendedAt != null && !suspendedAt.isJsonNull();
		return !suspended;
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

		HttpRequest request = HttpRequest.newBuilder().uri(URI.create(API + "/app/hook/deliveries?per_page=" + limit))
				.header("Authorization", "Bearer " + jwt).header("Accept", "application/vnd.github+json")
				.header("X-GitHub-Api-Version", "2022-11-28").GET().build();

		HttpResponse<String> resp = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
		if (resp.statusCode() != 200) {
			throw new IOException("GitHub list-webhook-deliveries failed: " + resp.statusCode() + " " + resp.body());
		}

		List<Map<String, Object>> out = new ArrayList<>();
		JsonArray deliveries = JsonParser.parseString(resp.body()).getAsJsonArray();
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
