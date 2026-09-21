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

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.net.URIAuthority;
import org.apache.hc.core5.util.Timeout;

/** Graph metadata requests use a fixed origin and do not follow redirects. */
public final class MicrosoftGraphJsonClient {

	private MicrosoftGraphJsonClient() {
	}

	public static String get(String url, Map<String, String> headers) {
		URI page = URI.create(url);
		if (!"https".equalsIgnoreCase(page.getScheme())
				|| !"graph.microsoft.com".equalsIgnoreCase(page.getHost())
				|| (page.getPort() != -1 && page.getPort() != 443)
				|| page.getRawUserInfo() != null || page.getRawFragment() != null) {
			throw new IllegalArgumentException("Microsoft Graph metadata URL must use https://graph.microsoft.com");
		}

		// Keep opaque skip tokens and encoded paths unchanged. Only the path/query
		// comes from the response; the connection target is independently fixed.
		String path = page.getRawPath();
		if (path == null || path.isEmpty()) {
			path = "/";
		}
		if (page.getRawQuery() != null) {
			path += "?" + page.getRawQuery();
		}
		HttpGet request = new HttpGet("/");
		request.setPath(path);
		request.setScheme("https");
		request.setAuthority(new URIAuthority("graph.microsoft.com"));
		if (headers != null) {
			headers.forEach((name, value) -> {
				if (name == null || value == null || name.indexOf('\r') >= 0 || name.indexOf('\n') >= 0
						|| name.indexOf('\0') >= 0 || value.indexOf('\r') >= 0 || value.indexOf('\n') >= 0
						|| value.indexOf('\0') >= 0) {
					throw new IllegalArgumentException("Invalid Microsoft Graph request header");
				}
				request.addHeader(name, value);
			});
		}

		// Default TLS verifies the Microsoft certificate and hostname. These
		// metadata requests have no cross-host download redirect to follow.
		try (CloseableHttpClient client = HttpClients.custom().disableRedirectHandling()
				.setConnectionManager(PoolingHttpClientConnectionManagerBuilder.create()
						.setDefaultConnectionConfig(ConnectionConfig.custom()
								.setConnectTimeout(Timeout.DISABLED).build()).build())
				.setDefaultRequestConfig(RequestConfig.custom()
						.setConnectionRequestTimeout(Timeout.DISABLED).setResponseTimeout(Timeout.DISABLED).build())
				.build()) {
			return client.execute(request, new HttpClientResponseHandler<String>() {
				@Override
				public String handleResponse(ClassicHttpResponse response) throws IOException {
					String body;
					try {
						HttpEntity entity = response.getEntity();
						body = entity == null ? null : EntityUtils.toString(entity, StandardCharsets.UTF_8);
					} catch (ParseException e) {
						throw new IOException("Failed to parse Microsoft Graph response", e);
					}
					int status = response.getCode();
					if (status >= 200 && status < 300) {
						return body;
					}
					String message = "GET request to " + url + " returned HTTP " + status;
					if (body != null && !body.isBlank()) {
						String detail = body.replaceAll("[\\r\\n\\t]+", " ").trim();
						message += ". Response body: " + detail.substring(0, Math.min(detail.length(), 500));
					}
					throw new IllegalArgumentException(message);
				}
			});
		} catch (IOException e) {
			throw new IllegalArgumentException("Failed to execute GET request to " + url + ". Cause: " + e.getMessage(), e);
		}
	}
}
