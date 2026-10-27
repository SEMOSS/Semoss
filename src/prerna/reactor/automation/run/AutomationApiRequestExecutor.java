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
package prerna.reactor.automation.run;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.definition.AutomationDefinitionValidator;
import prerna.util.Utility;

/**
 * Sends the single HTTP request described by an {@code api.request} node.
 *
 * <p>
 * Every HTTP status code is returned as an ordinary response. Only a request
 * that never receives a response (blocked destination, timeout, unreachable
 * host) throws. Redirects are never followed; the {@code Location} header is
 * returned so the Automation can act on it.
 */
final class AutomationApiRequestExecutor {

	private static final Pattern EMBEDDED_REFERENCE = Pattern.compile("\\$\\{([^}]+)}");
	private static final Set<String> HIDDEN_RESPONSE_HEADERS = Set.of("set-cookie", "set-cookie2", "authorization",
			"proxy-authorization");
	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();
	private static final HttpClient CLIENT = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER)
			.connectTimeout(Duration.ofSeconds(AutomationConstants.API_MAX_TIMEOUT_SECONDS)).build();

	private AutomationApiRequestExecutor() {
	}

	/** A request after references are resolved and the destination is checked. */
	record PreparedRequest(String method, URI uri, Map<String, String> headers, String bodyType, byte[] body,
			Duration timeout) {
	}

	/**
	 * Resolves the node, sends the request, and builds the node output.
	 *
	 * @param user caller whose permission governs the optional Connection
	 */
	static Map<String, Object> execute(Map<String, Object> config, Map<String, Object> scope, User user,
			String nodeId) {
		PreparedRequest request = prepare(config, scope, user, nodeId, AutomationApiRequestExecutor::resolveHost);
		return send(request);
	}

	static PreparedRequest prepare(Map<String, Object> config, Map<String, Object> scope, User user, String nodeId,
			Function<String, InetAddress[]> hostResolver) {
		String label = "API request node '" + nodeId + "'";
		AutomationDefinitionValidator.validateApiRequestConfig(nodeId, config);

		String method = String.valueOf(config.get(AutomationConstants.CONFIG_API_METHOD)).trim()
				.toUpperCase(Locale.ROOT);
		String urlText = interpolate(config.get(AutomationConstants.CONFIG_API_URL), scope, label + " url");
		Map<String, String> query = stringMap(config.get(AutomationConstants.CONFIG_API_QUERY), scope,
				label + " query");
		Map<String, String> headers = stringMap(config.get(AutomationConstants.CONFIG_API_HEADERS), scope,
				label + " headers");

		Connection connection = connection(config, user, nodeId);
		URI uri = destination(urlText, query, connection, label);
		// The Connection owns credentials, so its headers win over the node's.
		if (connection != null) {
			connection.headers().forEach((name, value) -> removeIgnoreCase(headers, name));
			headers.putAll(connection.headers());
		}
		boolean hasConnection = connection != null;
		if (!hasConnection) {
			Utility.checkIfValidDomain(uri.toString());
		}
		checkAddresses(uri.getHost(), hostResolver, label);

		String bodyType = bodyType(config);
		byte[] body = body(config, bodyType, scope, headers, label);
		int timeout = ((Number) config.get(AutomationConstants.CONFIG_API_TIMEOUT_SECONDS)).intValue();
		return new PreparedRequest(method, uri, headers, bodyType, body, Duration.ofSeconds(timeout));
	}

	static Map<String, Object> send(PreparedRequest prepared) {
		HttpRequest.Builder builder = HttpRequest.newBuilder(prepared.uri()).timeout(prepared.timeout());
		try {
			for (Map.Entry<String, String> header : prepared.headers().entrySet()) {
				builder.header(header.getKey(), header.getValue());
			}
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("API request header is not allowed: " + e.getMessage());
		}
		HttpRequest.BodyPublisher publisher = prepared.body() == null ? HttpRequest.BodyPublishers.noBody()
				: HttpRequest.BodyPublishers.ofByteArray(prepared.body());
		builder.method(prepared.method(), publisher);

		long started = System.currentTimeMillis();
		HttpResponse<InputStream> response;
		try {
			response = CLIENT.send(builder.build(), HttpResponse.BodyHandlers.ofInputStream());
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("API request was interrupted.");
		} catch (IOException e) {
			throw new IllegalStateException("API request received no response: " + e.getMessage());
		}
		try (InputStream stream = response.body()) {
			Map<String, String> responseHeaders = responseHeaders(response);
			boolean headOnly = "HEAD".equals(prepared.method());
			byte[] bytes = new byte[0];
			boolean truncated = false;
			if (!headOnly) {
				InputStream decoded = "gzip".equalsIgnoreCase(responseHeaders.get("content-encoding"))
						? new GZIPInputStream(stream)
						: stream;
				ByteArrayOutputStream buffer = new ByteArrayOutputStream();
				byte[] chunk = new byte[8192];
				int read;
				while ((read = decoded.read(chunk)) != -1) {
					int room = AutomationConstants.API_RESPONSE_MAX_BYTES - buffer.size();
					if (read > room) {
						buffer.write(chunk, 0, room);
						truncated = true;
						break;
					}
					buffer.write(chunk, 0, read);
				}
				bytes = buffer.toByteArray();
			}
			Map<String, Object> output = new LinkedHashMap<>();
			output.put("status", response.statusCode());
			output.put("headers", responseHeaders);
			output.put("body", headOnly ? null : parseBody(bytes, responseHeaders.get("content-type"), truncated));
			output.put("truncated", truncated);
			output.put("durationMs", System.currentTimeMillis() - started);
			return output;
		} catch (IOException e) {
			throw new IllegalStateException("API request response could not be read: " + e.getMessage());
		}
	}

	// ---- reference resolution ------------------------------------------------

	/**
	 * Resolves {@code ${path}} references inside a string. An exact reference keeps
	 * the referenced value's type; an embedded one is replaced with its text.
	 */
	static Object resolve(Object configured, Map<String, Object> scope, String valueName) {
		if (configured instanceof String text) {
			String exact = AutomationScopeResolver.exactReferencePath(text);
			if (exact != null) {
				return AutomationScopeResolver.resolveRequired(exact, scope, valueName);
			}
			return interpolate(text, scope, valueName);
		}
		if (configured instanceof Map<?, ?> map) {
			Map<String, Object> resolved = new LinkedHashMap<>();
			for (Map.Entry<?, ?> entry : map.entrySet()) {
				String key = String.valueOf(entry.getKey());
				resolved.put(key, resolve(entry.getValue(), scope, valueName + "." + key));
			}
			return resolved;
		}
		if (configured instanceof List<?> list) {
			List<Object> resolved = new java.util.ArrayList<>(list.size());
			for (int index = 0; index < list.size(); index++) {
				resolved.add(resolve(list.get(index), scope, valueName + "[" + index + "]"));
			}
			return resolved;
		}
		return configured;
	}

	static String interpolate(Object configured, Map<String, Object> scope, String valueName) {
		if (configured == null) {
			return "";
		}
		String text = String.valueOf(configured);
		Matcher matcher = EMBEDDED_REFERENCE.matcher(text);
		StringBuilder result = new StringBuilder();
		while (matcher.find()) {
			Object value = AutomationScopeResolver.resolveRequired(matcher.group(1).trim(), scope, valueName);
			if (value instanceof Map<?, ?> || value instanceof Iterable<?>) {
				throw new IllegalArgumentException(
						valueName + " reference '" + matcher.group(1) + "' must be a single value, not a list or object.");
			}
			matcher.appendReplacement(result, Matcher.quoteReplacement(value == null ? "" : String.valueOf(value)));
		}
		matcher.appendTail(result);
		return result.toString();
	}

	private static Map<String, String> stringMap(Object configured, Map<String, Object> scope, String valueName) {
		Map<String, String> result = new LinkedHashMap<>();
		if (!(configured instanceof Map<?, ?> map)) {
			return result;
		}
		for (Map.Entry<?, ?> entry : map.entrySet()) {
			String key = String.valueOf(entry.getKey());
			if (key.isBlank()) {
				continue;
			}
			result.put(key, interpolate(entry.getValue(), scope, valueName + "." + key));
		}
		return result;
	}

	// ---- destination ---------------------------------------------------------

	/** Connection details copied from a REST function engine's settings. */
	record Connection(String baseUrl, Map<String, String> headers) {
	}

	private static Connection connection(Map<String, Object> config, User user, String nodeId) {
		Object raw = config.get(AutomationConstants.CONFIG_API_CONNECTION_ID);
		if (!(raw instanceof String connectionId) || connectionId.isBlank()) {
			return null;
		}
		if (!SecurityEngineUtils.userCanViewEngine(user, connectionId)
				|| SecurityEngineUtils.getEngineType(connectionId) != IEngine.CATALOG_TYPE.FUNCTION) {
			throw new IllegalArgumentException("API request node '" + nodeId + "' connection '" + connectionId
					+ "' is not an accessible function engine.");
		}
		IFunctionEngine engine = Utility.getFunctionEngine(connectionId);
		Properties smss = engine.getSmssProp();
		String url = smss == null ? null : smss.getProperty("URL");
		if (url == null || url.isBlank()) {
			throw new IllegalArgumentException(
					"API request node '" + nodeId + "' connection '" + connectionId + "' is not a REST function engine.");
		}
		Map<String, String> headers = new LinkedHashMap<>();
		String headersJson = smss.getProperty("HEADERS");
		if (headersJson != null && !headersJson.isBlank()) {
			@SuppressWarnings("unchecked")
			Map<String, Object> parsed = GSON.fromJson(headersJson, Map.class);
			if (parsed != null) {
				parsed.forEach((name, value) -> headers.put(name, String.valueOf(value)));
			}
		}
		return new Connection(url.trim(), headers);
	}

	private static URI destination(String urlText, Map<String, String> query, Connection connection, String label) {
		try {
			String text = urlText.trim();
			URI uri;
			if (connection != null) {
				URI base = new URI(connection.baseUrl());
				if (text.matches("(?i)^[a-z][a-z0-9+.-]*://.*")) {
					uri = new URI(text);
					if (!sameOrigin(base, uri)) {
						throw new IllegalArgumentException(label + " url must stay on the Connection's host.");
					}
				} else {
					String root = connection.baseUrl().replaceAll("/+$", "");
					uri = new URI(root + (text.startsWith("/") || text.isEmpty() ? "" : "/") + text);
				}
			} else {
				uri = new URI(text);
			}
			String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
			if (!scheme.equals("http") && !scheme.equals("https")) {
				throw new IllegalArgumentException(label + " url must start with http:// or https://.");
			}
			if (uri.getHost() == null || uri.getHost().isBlank()) {
				throw new IllegalArgumentException(label + " url must include a host.");
			}
			if (uri.getUserInfo() != null) {
				throw new IllegalArgumentException(label + " url must not contain credentials.");
			}
			if (query.isEmpty()) {
				return uri;
			}
			StringBuilder extra = new StringBuilder();
			for (Map.Entry<String, String> entry : query.entrySet()) {
				if (extra.length() > 0) {
					extra.append('&');
				}
				extra.append(encode(entry.getKey())).append('=').append(encode(entry.getValue()));
			}
			String withoutFragment = uri.toString();
			int fragmentStart = withoutFragment.indexOf('#');
			if (fragmentStart >= 0) {
				withoutFragment = withoutFragment.substring(0, fragmentStart);
			}
			String separator = uri.getRawQuery() == null ? "?" : "&";
			return new URI(withoutFragment + separator + extra);
		} catch (URISyntaxException e) {
			throw new IllegalArgumentException(label + " url is not valid: " + e.getMessage());
		}
	}

	private static boolean sameOrigin(URI base, URI other) {
		return base.getScheme() != null && base.getScheme().equalsIgnoreCase(other.getScheme())
				&& base.getHost() != null && base.getHost().equalsIgnoreCase(other.getHost())
				&& base.getPort() == other.getPort();
	}

	private static String encode(String value) {
		return java.net.URLEncoder.encode(value, StandardCharsets.UTF_8);
	}

	private static InetAddress[] resolveHost(String host) {
		try {
			return InetAddress.getAllByName(host);
		} catch (UnknownHostException e) {
			throw new IllegalStateException("API request host could not be resolved: " + host);
		}
	}

	/** Rejects any destination that resolves to a non-public address. */
	static void checkAddresses(String host, Function<String, InetAddress[]> hostResolver, String label) {
		for (InetAddress address : hostResolver.apply(host)) {
			if (!isPublic(address)) {
				throw new IllegalArgumentException(label + " is not allowed to call a private or internal address.");
			}
		}
	}

	static boolean isPublic(InetAddress address) {
		if (address.isAnyLocalAddress() || address.isLoopbackAddress() || address.isLinkLocalAddress()
				|| address.isSiteLocalAddress() || address.isMulticastAddress()) {
			return false;
		}
		byte[] bytes = address.getAddress();
		if (bytes.length == 4) {
			int first = bytes[0] & 0xff;
			int second = bytes[1] & 0xff;
			// 0.0.0.0/8 and carrier-grade NAT 100.64.0.0/10
			return first != 0 && !(first == 100 && second >= 64 && second <= 127);
		}
		// IPv6 unique-local fc00::/7
		return (bytes[0] & 0xfe) != 0xfc;
	}

	// ---- body and response ---------------------------------------------------

	private static String bodyType(Map<String, Object> config) {
		Object type = config.get(AutomationConstants.CONFIG_API_BODY_TYPE);
		return type == null || String.valueOf(type).isBlank() ? AutomationConstants.API_BODY_NONE
				: String.valueOf(type).trim().toLowerCase(Locale.ROOT);
	}

	private static byte[] body(Map<String, Object> config, String bodyType, Map<String, Object> scope,
			Map<String, String> headers, String label) {
		if (AutomationConstants.API_BODY_NONE.equals(bodyType)) {
			return null;
		}
		Object resolved = resolve(config.get(AutomationConstants.CONFIG_API_BODY), scope, label + " body");
		String text;
		String contentType;
		switch (bodyType) {
		case AutomationConstants.API_BODY_JSON -> {
			text = resolved instanceof String string ? string : GSON.toJson(resolved);
			contentType = "application/json";
		}
		case AutomationConstants.API_BODY_FORM -> {
			if (!(resolved instanceof Map<?, ?> fields)) {
				throw new IllegalArgumentException(label + " form body must be an object of field names and values.");
			}
			StringBuilder form = new StringBuilder();
			for (Map.Entry<?, ?> entry : fields.entrySet()) {
				if (form.length() > 0) {
					form.append('&');
				}
				form.append(encode(String.valueOf(entry.getKey()))).append('=')
						.append(encode(String.valueOf(entry.getValue())));
			}
			text = form.toString();
			contentType = "application/x-www-form-urlencoded";
		}
		default -> {
			text = resolved == null ? "" : String.valueOf(resolved);
			contentType = "text/plain";
		}
		}
		boolean hasContentType = headers.keySet().stream().anyMatch(name -> name.equalsIgnoreCase("Content-Type"));
		if (!hasContentType) {
			headers.put("Content-Type", contentType);
		}
		return text.getBytes(StandardCharsets.UTF_8);
	}

	private static Map<String, String> responseHeaders(HttpResponse<?> response) {
		Map<String, String> headers = new LinkedHashMap<>();
		response.headers().map().forEach((name, values) -> {
			String lower = name.toLowerCase(Locale.ROOT);
			if (!HIDDEN_RESPONSE_HEADERS.contains(lower)) {
				headers.put(lower, String.join(", ", values));
			}
		});
		return headers;
	}

	static Object parseBody(byte[] bytes, String contentType, boolean truncated) {
		Charset charset = StandardCharsets.UTF_8;
		String type = contentType == null ? "" : contentType.toLowerCase(Locale.ROOT);
		Matcher charsetMatch = Pattern.compile("charset=([\\w-]+)").matcher(type);
		if (charsetMatch.find()) {
			try {
				charset = Charset.forName(charsetMatch.group(1));
			} catch (RuntimeException ignored) {
				// Unknown charset: fall back to UTF-8.
			}
		}
		String text = new String(bytes, charset);
		boolean json = type.contains("json");
		if (json && !truncated && !text.isBlank()) {
			try {
				return GSON.fromJson(text, Object.class);
			} catch (RuntimeException ignored) {
				// Malformed JSON is still a response: return it as text.
			}
		}
		return text;
	}

	private static void removeIgnoreCase(Map<String, String> headers, String name) {
		headers.keySet().removeIf(existing -> existing.equalsIgnoreCase(name));
	}
}
