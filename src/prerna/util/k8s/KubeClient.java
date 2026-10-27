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
package prerna.util.k8s;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPatch;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.hc.core5.util.Timeout;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * A small Kubernetes REST client over the HttpClient 5 already on SEMOSS's
 * classpath: GET (JSON or text), JSON merge patch, and the error mapping of the
 * API's {@code Status} replies. Requests are built from path segments, which
 * are encoded, so names never alter the path.
 *
 * The client does not read the JVM's or the environment's proxy settings: the
 * API server is reached directly, as kubectl does for in-cluster and loopback
 * addresses.
 */
public final class KubeClient implements Closeable {

	private static final Timeout CONNECT_TIMEOUT = Timeout.ofSeconds(5);
	private static final Timeout RESPONSE_TIMEOUT = Timeout.ofSeconds(30);
	private static final ContentType MERGE_PATCH = ContentType.create("application/merge-patch+json",
			StandardCharsets.UTF_8);
	private static final ContentType JSON = ContentType.create("application/json", StandardCharsets.UTF_8);

	private final KubeConnection connection;
	private final CloseableHttpClient http;

	public KubeClient(KubeConnection connection) {
		this.connection = connection;
		PoolingHttpClientConnectionManagerBuilder connections = PoolingHttpClientConnectionManagerBuilder.create()
				.setDefaultConnectionConfig(ConnectionConfig.custom().setConnectTimeout(CONNECT_TIMEOUT)
						.setSocketTimeout(RESPONSE_TIMEOUT).build());
		if (connection.getSslContext() != null) {
			connections.setTlsSocketStrategy(new DefaultClientTlsStrategy(connection.getSslContext()));
		}
		this.http = HttpClients.custom().setConnectionManager(connections.build())
				.setDefaultRequestConfig(RequestConfig.custom().setResponseTimeout(RESPONSE_TIMEOUT).build())
				.disableCookieManagement().disableRedirectHandling().build();
	}

	/**
	 * Start a request for the API path made of these segments, for example
	 * {@code path("api", "v1", "namespaces", ns, "pods")}.
	 */
	public static Request path(String... segments) {
		return new Request(Arrays.asList(segments));
	}

	public KubeConnection getConnection() {
		return this.connection;
	}

	public JsonObject get(Request request) {
		return execute(new HttpGet(uri(request)), "application/json", KubeClient::parseObject);
	}

	/** GET a text resource, such as {@code pods/{name}/log}. */
	public String getText(Request request) {
		return execute(new HttpGet(uri(request)), "*/*", Function.identity());
	}

	/** Apply a JSON merge patch (RFC 7386) and return the updated object. */
	public JsonObject mergePatch(Request request, JsonObject patch) {
		HttpPatch method = new HttpPatch(uri(request));
		method.setEntity(new StringEntity(patch.toString(), MERGE_PATCH));
		return execute(method, "application/json", KubeClient::parseObject);
	}

	/** Create an object (POST to its collection) and return it as stored, with its uid. */
	public JsonObject create(Request collection, JsonObject object) {
		HttpPost method = new HttpPost(uri(collection));
		method.setEntity(new StringEntity(object.toString(), JSON));
		return execute(method, "application/json", KubeClient::parseObject);
	}

	/**
	 * Delete an object. Kubernetes answers with the object itself while finalizers
	 * hold it, or with a Status once it is gone.
	 */
	public JsonObject delete(Request object) {
		return execute(new HttpDelete(uri(object)), "application/json", KubeClient::parseObject);
	}

	private <T> T execute(HttpUriRequestBase method, String accept, Function<String, T> parser) {
		method.setHeader("Accept", accept);
		String token = this.connection.bearerToken();
		if (token != null && !token.isEmpty()) {
			method.setHeader("Authorization", "Bearer " + token);
		}
		try {
			return this.http.execute(method, response -> {
				HttpEntity entity = response.getEntity();
				String body = entity == null ? "" : EntityUtils.toString(entity, StandardCharsets.UTF_8);
				int code = response.getCode();
				if (code < 200 || code >= 300) {
					throw KubeApiException.fromResponse(code, body);
				}
				return parser.apply(body);
			});
		} catch (IOException e) {
			throw new KubeApiException("Cannot reach the Kubernetes API at " + this.connection.getServer() + " ("
					+ this.connection.describe() + "): " + e.getMessage(), e);
		}
	}

	private URI uri(Request request) {
		try {
			URIBuilder builder = new URIBuilder(this.connection.getServer()).appendPathSegments(request.segments);
			for (Map.Entry<String, String> parameter : request.query.entrySet()) {
				builder.addParameter(parameter.getKey(), parameter.getValue());
			}
			return builder.build();
		} catch (URISyntaxException e) {
			throw new IllegalArgumentException("Invalid Kubernetes API path " + request.segments, e);
		}
	}

	private static JsonObject parseObject(String body) {
		JsonElement parsed = JsonParser.parseString(body);
		if (!parsed.isJsonObject()) {
			throw new KubeApiException(200, null, "Expected a JSON object from the Kubernetes API");
		}
		return parsed.getAsJsonObject();
	}

	@Override
	public void close() throws IOException {
		this.http.close();
	}

	/**
	 * An API path and its query parameters. Parameters with a null value are left
	 * out, so optional ones can be passed through unconditionally.
	 */
	public static final class Request {

		private final List<String> segments;
		private final Map<String, String> query = new LinkedHashMap<>();

		private Request(List<String> segments) {
			for (String segment : segments) {
				if (segment == null || segment.isEmpty()) {
					throw new IllegalArgumentException("Empty segment in Kubernetes API path " + segments);
				}
			}
			this.segments = List.copyOf(segments);
		}

		public Request query(String name, Object value) {
			if (value != null) {
				this.query.put(name, value.toString());
			}
			return this;
		}

		@Override
		public String toString() {
			return "/" + String.join("/", this.segments) + (this.query.isEmpty() ? "" : "?" + this.query);
		}
	}
}
