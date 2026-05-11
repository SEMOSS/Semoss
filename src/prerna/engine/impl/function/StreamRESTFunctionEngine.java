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
package prerna.engine.impl.function;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpHead;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.entity.UrlEncodedFormEntity;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.sablecc2.comm.PixelJobManager;
import prerna.sablecc2.comm.PixelJobRunner;
import prerna.security.HttpHelperUtility;
import prerna.util.Utility;

public class StreamRESTFunctionEngine extends AbstractFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(StreamRESTFunctionEngine.class);

	private String httpMethod;
	private String url;
	private Map<String, String> headers;

	private String contentType = "JSON";

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.httpMethod = smssProp.getProperty("HTTP_METHOD");
		if (this.httpMethod == null || (this.httpMethod = this.httpMethod.trim().toUpperCase()).isEmpty()
				|| (!this.httpMethod.equals("GET") && !this.httpMethod.equals("POST") && !this.httpMethod.equals("PUT")
						&& !this.httpMethod.equals("HEAD"))) {
			throw new IllegalArgumentException("RESTFunctionEngine only supports GET, HEAD, POST, or PUT requests");
		}

		this.url = smssProp.getProperty("URL");
		if (this.url == null || (this.url = this.url.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must provide a URL");
		}
		Utility.checkIfValidDomain(url);

		String headersStr = smssProp.getProperty("HEADERS");
		if (headersStr != null && !(headersStr = headersStr.trim()).isEmpty()) {
			this.headers = new Gson().fromJson(headersStr, new TypeToken<Map<String, String>>() {
			}.getType());
		}

		if (smssProp.containsKey("CONTENT_TYPE")) {
			this.contentType = smssProp.getProperty("CONTENT_TYPE");
		}
	}

	@Override
	public void close() throws IOException {
		// nothing to close
	}

	@Override
	public Object execute(Map<String, Object> parameterValues) {
		String jobId = (String) parameterValues.remove(PixelJobRunner.JOB_KEY);
		if (jobId == null) {
			throw new IllegalArgumentException("Must provide the job id for streaming output");
		}

		if (this.requiredParameters != null && !this.requiredParameters.isEmpty()) {
			Set<String> missingPs = new HashSet<>();
			for (String requiredP : this.requiredParameters) {
				if (!parameterValues.containsKey(requiredP)) {
					missingPs.add(requiredP);
				}
			}
			if (!missingPs.isEmpty()) {
				throw new IllegalArgumentException("Must define required keys = " + missingPs);
			}
		}

		try (CloseableHttpClient httpClient = HttpHelperUtility.getCustomClient(null, null, null, null)) {
			return httpClient.execute(buildRequest(parameterValues), new HttpClientResponseHandler<String>() {
				@Override
				public String handleResponse(ClassicHttpResponse response) throws IOException {
					int statusCode = response.getCode();
					HttpEntity entity = response.getEntity();
					if (statusCode >= 200 && statusCode < 300) {
						if (entity == null) {
							return null;
						}
						StringBuilder responseAssimilator = new StringBuilder();
						try (BufferedReader reader = new BufferedReader(
								new InputStreamReader(entity.getContent(), StandardCharsets.UTF_8))) {
							String line;
							while ((line = reader.readLine()) != null) {
								responseAssimilator.append(line);
								Map<String, Object> chunk = new LinkedHashMap<>();
								chunk.put("stream_type", "content");
								chunk.put("data", line);
								PixelJobManager.getManager().addStreamOut(jobId, chunk);
							}
						} catch (Exception e) {
							classLogger.error("Error reading streaming response from '{}'", url, e);
							throw new IllegalArgumentException(
									"There was an error processing the response from " + url);
						}
						return responseAssimilator.toString();
					}
					String responseData = "";
					if (entity != null) {
						try {
							responseData = EntityUtils.toString(entity, StandardCharsets.UTF_8);
						} catch (ParseException e) {
							throw new IOException("Failed to parse error response body from '" + url + "'", e);
						}
					}
					throw new IllegalArgumentException("Connected to " + url + " but received error = " + responseData);
				}
			});
		} catch (IOException e) {
			classLogger.error("Failed to execute {} request to '{}'", this.httpMethod, this.url, e);
			throw new IllegalArgumentException("Could not connect to URL at " + url);
		}
	}

	private HttpUriRequestBase buildRequest(Map<String, Object> parameterValues) {
		if (httpMethod.equalsIgnoreCase("GET") || httpMethod.equalsIgnoreCase("HEAD")) {
			StringBuilder queryString = new StringBuilder();
			boolean first = true;
			for (String k : parameterValues.keySet()) {
				if (!first) {
					queryString.append("&");
				}
				queryString.append(k).append("=").append(parameterValues.get(k));
				first = false;
			}
			String runTimeUrl = url + "?" + queryString;
			if (httpMethod.equalsIgnoreCase("GET")) {
				HttpGet httpGet = new HttpGet(runTimeUrl);
				addHeaders(httpGet);
				return httpGet;
			} else {
				HttpHead httpHead = new HttpHead(runTimeUrl);
				addHeaders(httpHead);
				return httpHead;
			}
		} else if (httpMethod.equalsIgnoreCase("PUT")) {
			HttpPut httpPut = new HttpPut(url);
			addHeaders(httpPut);
			if (parameterValues != null && !parameterValues.isEmpty()) {
				if (this.contentType.equalsIgnoreCase("JSON")) {
					httpPut.setEntity(
							new StringEntity(new Gson().toJson(parameterValues), ContentType.APPLICATION_JSON));
				} else {
					List<NameValuePair> params = new ArrayList<>();
					for (String key : parameterValues.keySet()) {
						params.add(new BasicNameValuePair(key, parameterValues.get(key) + ""));
					}
					httpPut.setEntity(new UrlEncodedFormEntity(params, StandardCharsets.UTF_8));
				}
			}
			return httpPut;
		} else {
			HttpPost httpPost = new HttpPost(url);
			addHeaders(httpPost);
			if (parameterValues != null && !parameterValues.isEmpty()) {
				if (this.contentType.equalsIgnoreCase("JSON")) {
					httpPost.setEntity(
							new StringEntity(new Gson().toJson(parameterValues), ContentType.APPLICATION_JSON));
				} else {
					List<NameValuePair> params = new ArrayList<>();
					for (String key : parameterValues.keySet()) {
						params.add(new BasicNameValuePair(key, parameterValues.get(key) + ""));
					}
					httpPost.setEntity(new UrlEncodedFormEntity(params, StandardCharsets.UTF_8));
				}
			}
			return httpPost;
		}
	}

	private void addHeaders(HttpUriRequestBase requestMethod) {
		if (this.headers != null && !this.headers.isEmpty()) {
			for (String key : this.headers.keySet()) {
				requestMethod.addHeader(key, this.headers.get(key));
			}
		}
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return "REST";
	}
}
