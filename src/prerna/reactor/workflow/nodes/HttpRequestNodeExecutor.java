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
package prerna.reactor.workflow.nodes;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hc.core5.http.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.reflect.TypeToken;

import prerna.reactor.workflow.WorkflowExecutionUtils;
import prerna.security.HttpHelperUtility;

/**
 * Executes an "http-request" node: calls {@link HttpHelperUtility} directly - no Pixel
 * string-building at all, the same approach this node type already used before this refactor.
 */
public final class HttpRequestNodeExecutor implements IWorkflowNodeExecutor {

	private static final Logger classLogger = LogManager.getLogger(HttpRequestNodeExecutor.class);

	/**
	 * SSRF guard for the HTTP Request node. Rejects non-http(s) schemes and any host that
	 * resolves to a loopback, link-local (which includes the cloud instance-metadata address
	 * 169.254.169.254), wildcard, or multicast address. This blocks the classic
	 * server-side-request-forgery targets while still allowing ordinary external and internal
	 * corporate hosts.
	 *
	 * <p>Note: this validates at resolution time; it does not defend against DNS rebinding
	 * (a host that resolves to an allowed address here but a blocked one when the request is
	 * actually made). Pinning the resolved address into the request would be a follow-up.
	 */
	private static void assertHttpTargetAllowed(String url) {
		final URI uri;
		try {
			uri = new URI(url.trim());
		} catch (URISyntaxException e) {
			throw new IllegalArgumentException("HTTP Request node: malformed url: " + url);
		}
		String scheme = uri.getScheme();
		if (scheme == null || !(scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"))) {
			throw new IllegalArgumentException("HTTP Request node: only http/https urls are allowed");
		}
		String host = uri.getHost();
		if (host == null || host.isBlank()) {
			throw new IllegalArgumentException("HTTP Request node: url has no host: " + url);
		}
		try {
			for (InetAddress addr : InetAddress.getAllByName(host)) {
				if (addr.isLoopbackAddress() || addr.isLinkLocalAddress()
						|| addr.isAnyLocalAddress() || addr.isMulticastAddress()) {
					throw new IllegalStateException("HTTP Request node: target host '" + host
							+ "' resolves to a blocked address (" + addr.getHostAddress()
							+ "). Requests to loopback, link-local (including cloud metadata), "
							+ "wildcard, and multicast addresses are not permitted.");
				}
			}
		} catch (UnknownHostException e) {
			throw new IllegalStateException("HTTP Request node: could not resolve host '" + host + "'");
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public Object execute(WorkflowNodeContext ctx) {
		Map<String, Object> node = ctx.node();
		Map<String, String> scope = ctx.scope();
		Map<String, String> configMap = ctx.configMap();
		Map<String, Object> config = (Map<String, Object>) node.get("config");

		String method = WorkflowExecutionUtils.strCfg(config.getOrDefault("method", "GET")).toUpperCase();
		String url = WorkflowExecutionUtils.resolve(WorkflowExecutionUtils.strCfg(config.get("url")), scope, configMap);
		if (url == null || url.isBlank()) throw new IllegalArgumentException("HTTP Request node: 'url' is required");
		assertHttpTargetAllowed(url);

		// Parse headers JSON
		Map<String, String> headers = new LinkedHashMap<>();
		String headersJson = WorkflowExecutionUtils.strCfg(config.get("headers"));
		if (headersJson != null && !headersJson.isBlank()) {
			try {
				Map<String, Object> parsed = WorkflowExecutionUtils.GSON.fromJson(headersJson, new TypeToken<Map<String, Object>>(){}.getType());
				if (parsed != null) parsed.forEach((k, v) -> { if (v != null) headers.put(k, v.toString()); });
			} catch (Exception e) {
				classLogger.warn("HTTP node: could not parse headers JSON: {}", e.getMessage());
			}
		}

		// Basic auth
		String username = WorkflowExecutionUtils.strCfg(config.get("username"));
		String password = WorkflowExecutionUtils.strCfg(config.get("password"));
		if (username != null && !username.isBlank() && password != null) {
			String creds = Base64.getEncoder().encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
			headers.put("Authorization", "Basic " + creds);
		}

		String body = WorkflowExecutionUtils.strCfg(config.get("body"));
		if (body != null) body = WorkflowExecutionUtils.resolve(body, scope, configMap);
		final String resolvedBody = body != null ? body : "";

		String response;
		try {
			switch (method) {
				case "GET":
					response = HttpHelperUtility.getRequest(url, headers, null, null, null);
					break;
				case "POST":
					response = HttpHelperUtility.postRequestStringBody(url, headers, resolvedBody, ContentType.APPLICATION_JSON, null, null, null);
					break;
				case "PUT":
					response = HttpHelperUtility.putRequestStringBody(url, headers, resolvedBody, ContentType.APPLICATION_JSON, null, null, null);
					break;
				case "PATCH":
					response = HttpHelperUtility.patchRequestStringBody(url, headers, resolvedBody, ContentType.APPLICATION_JSON, null, null, null);
					break;
				case "DELETE":
					response = HttpHelperUtility.deleteRequestStringBody(url, headers, null, null, null);
					break;
				default:
					throw new IllegalArgumentException("Unsupported HTTP method: " + method);
			}
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			throw new IllegalStateException("HTTP request failed [" + method + " " + url + "]: " + e.getMessage(), e);
		}

		if (response == null) return "{\"response\": null}";
		// Return as-is if valid JSON, otherwise wrap it
		try {
			WorkflowExecutionUtils.GSON.fromJson(response, Object.class);
			return response;
		} catch (Exception e) {
			Map<String, Object> r = new LinkedHashMap<>();
			r.put("response", response);
			return WorkflowExecutionUtils.GSON.toJson(r);
		}
	}
}
