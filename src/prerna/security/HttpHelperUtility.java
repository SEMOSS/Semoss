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
package prerna.security;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.HostnameVerifier;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hc.client5.http.ClientProtocolException;
import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpHead;
import org.apache.hc.client5.http.classic.methods.HttpPatch;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.cookie.CookieStore;
import org.apache.hc.client5.http.entity.UrlEncodedFormEntity;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.FileEntity;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.hc.core5.http.ssl.TLS;
import org.apache.hc.core5.reactor.ssl.SSLBufferMode;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.hc.core5.ssl.TrustStrategy;
import org.apache.hc.core5.util.Timeout;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.burt.jmespath.Expression;
import io.burt.jmespath.JmesPath;
import io.burt.jmespath.jackson.JacksonRuntime;
import net.snowflake.client.jdbc.internal.com.nimbusds.jose.shaded.gson.GsonBuilder;
import prerna.auth.AccessToken;
import prerna.io.connector.antivirus.VirusScannerUtils;
import prerna.util.Utility;

public final class HttpHelperUtility {

	private static final Logger classLogger = LogManager.getLogger(HttpHelperUtility.class.getName());
	private static ObjectMapper mapper = new ObjectMapper();

	/**
	 * Builds a custom Apache HttpClient instance for connector requests.
	 * <p>
	 * The client is configured to trust all certificates and can optionally load
	 * client key material from a keystore for mutual TLS scenarios.
	 * </p>
	 *
	 * @param cookieStore  optional cookie store to attach to the client
	 * @param keyStore     optional path to a keystore file that contains client
	 *                     certificates
	 * @param keyStorePass password for the keystore
	 * @param keyPass      optional key password; when omitted, {@code keyStorePass}
	 *                     is used
	 * @return configured {@link CloseableHttpClient}
	 */
	public static CloseableHttpClient getCustomClient(CookieStore cookieStore, String keyStore, String keyStorePass,
			String keyPass) {
		TrustStrategy trustStrategy = new TrustStrategy() {
			@Override
			public boolean isTrusted(X509Certificate[] chain, String authType) throws CertificateException {
				return true;
			}
		};

		HostnameVerifier verifier = NoopHostnameVerifier.INSTANCE;
		DefaultClientTlsStrategy tlsStrategy = null;

		try {
			SSLContextBuilder sslContextBuilder = SSLContextBuilder.create().loadTrustMaterial(trustStrategy);

			// add the cert if required
			if (keyStore != null && !keyStore.isEmpty() && keyStorePass != null && !keyStorePass.isEmpty()) {
				File keyStoreF = new File(keyStore);
				if (!keyStoreF.exists() && !keyStoreF.isFile()) {
					classLogger.warn("Defined a keystore to use in the request but the file "
							+ keyStoreF.getAbsolutePath() + " does not exist");
				} else {
					if (keyPass == null || keyPass.isEmpty()) {
						sslContextBuilder.loadKeyMaterial(keyStoreF, keyStorePass.toCharArray(),
								keyStorePass.toCharArray());
					} else {
						sslContextBuilder.loadKeyMaterial(keyStoreF, keyStorePass.toCharArray(), keyPass.toCharArray());
					}
				}
			}

			tlsStrategy = new DefaultClientTlsStrategy(sslContextBuilder.build(),
					new String[] { TLS.V_1_2.getId(), TLS.V_1_3.getId() }, // Removed deprecated TLS versions
					null, // Use default cipher suites
					SSLBufferMode.DYNAMIC, verifier);

		} catch (Exception e) {
			classLogger.error("getCustomClient: failed to configure TLS strategy or keystore for custom HTTP client",
					e);
		}

		PoolingHttpClientConnectionManager connectionManager = PoolingHttpClientConnectionManagerBuilder.create()
				.setTlsSocketStrategy(tlsStrategy).build();

		connectionManager
				.setDefaultConnectionConfig(ConnectionConfig.custom().setConnectTimeout(Timeout.DISABLED).build());

		HttpClientBuilder builder = HttpClients.custom();
		if (cookieStore != null) {
			builder.setDefaultCookieStore(cookieStore);
		}
		builder.setConnectionManager(connectionManager);
		builder.setDefaultRequestConfig(RequestConfig.custom().setConnectionRequestTimeout(Timeout.DISABLED)
				.setResponseTimeout(Timeout.DISABLED).build());
		return builder.build();
	}

	/**
	 * Executes an HTTP GET request and returns the response body as UTF-8 text.
	 *
	 * @param url          target URL
	 * @param headerMap    optional request headers
	 * @param keyStore     optional path to a keystore file for mutual TLS
	 * @param keyStorePass password for the keystore
	 * @param keyPass      optional key password
	 * @return response payload as a string, or {@code null} when the response has
	 *         no entity
	 * @throws IllegalArgumentException if the URL cannot be reached or the endpoint
	 *                                  returns a non-2xx status
	 */
	public static String getRequest(String url, Map<String, String> headerMap, String keyStore, String keyStorePass,
			String keyPass) {
		try (CloseableHttpClient httpClient = HttpHelperUtility.getCustomClient(null, keyStore, keyStorePass,
				keyPass)) {
			HttpGet httpGet = new HttpGet(url);
			if (headerMap != null && !headerMap.isEmpty()) {
				for (String key : headerMap.keySet()) {
					httpGet.addHeader(key, headerMap.get(key));
				}
			}

			return httpClient.execute(httpGet, new HttpClientResponseHandler<String>() {
				@Override
				public String handleResponse(ClassicHttpResponse response) throws IOException {
					int statusCode = response.getCode();
					HttpEntity entity = response.getEntity();
					if (statusCode >= 200 && statusCode < 300) {
						return readEntityAsString(entity);
					}
					String responseData = readEntityAsStringOrEmpty(entity);
					throw new IllegalArgumentException("Connected to " + url + " but received error = " + responseData);
				}
			});
		} catch (IOException e) {
			classLogger.error("getRequest: failed to execute GET request for URL: " + url, e);
			throw new IllegalArgumentException("Could not connect to URL at " + url);
		}
	}

	/**
	 * Executes an HTTP GET request and returns the raw response bytes.
	 *
	 * @param url          target URL
	 * @param headerMap    optional request headers
	 * @param keyStore     optional path to a keystore file for mutual TLS
	 * @param keyStorePass password for the keystore
	 * @param keyPass      optional key password
	 * @return response payload as bytes, or {@code null} when the response has no
	 *         entity
	 * @throws IllegalArgumentException if the URL cannot be reached or the endpoint
	 *                                  returns a non-2xx status
	 */
	public static byte[] getRequestBytes(String url, Map<String, String> headerMap, String keyStore,
			String keyStorePass, String keyPass) {
		try (CloseableHttpClient httpClient = HttpHelperUtility.getCustomClient(null, keyStore, keyStorePass,
				keyPass)) {
			HttpGet httpGet = new HttpGet(url);

			if (headerMap != null && !headerMap.isEmpty()) {
				for (String key : headerMap.keySet()) {
					httpGet.addHeader(key, headerMap.get(key));
				}
			}

			return httpClient.execute(httpGet, new HttpClientResponseHandler<byte[]>() {
				@Override
				public byte[] handleResponse(ClassicHttpResponse response) throws IOException {
					int statusCode = response.getCode();
					HttpEntity entity = response.getEntity();
					if (statusCode >= 200 && statusCode < 300) {
						return entity != null ? EntityUtils.toByteArray(entity) : null;
					}
					String errorMsg = readEntityAsStringOrEmpty(entity);
					throw new IllegalArgumentException("Connected to " + url + " but received error = " + errorMsg);
				}
			});
		} catch (IOException e) {
			classLogger.error("getRequestBytes: failed to execute GET request for URL: " + url, e);
			throw new IllegalArgumentException("Could not connect to URL at " + url);
		}
	}

	/**
	 * Downloads content from a URL and saves it to disk.
	 * <p>
	 * If {@code saveFileName} is {@code null}, the file name is inferred from the
	 * URL path. The final output path is made unique to avoid overwriting an
	 * existing file. When virus scanning is enabled, the downloaded bytes are
	 * scanned before writing to disk.
	 * </p>
	 *
	 * @param url          source URL
	 * @param headerMap    optional request headers
	 * @param keyStore     optional path to a keystore file for mutual TLS
	 * @param keyStorePass password for the keystore
	 * @param keyPass      optional key password
	 * @param saveFilePath destination directory path
	 * @param saveFileName optional destination file name
	 * @return saved file reference
	 * @throws IllegalArgumentException if the destination directory cannot be
	 *                                  created, the URL cannot be reached, the URL
	 *                                  does not include a file name when none is
	 *                                  provided, or malware is detected
	 */
	public static File getRequestFileDownload(String url, Map<String, String> headerMap, String keyStore,
			String keyStorePass, String keyPass, String saveFilePath, String saveFileName) {
		String fileName = saveFileName;
		if (fileName == null) {
			// if not passed in, see if we can grab it from the URL
			String[] pathSeparated = url.split("/");
			fileName = pathSeparated[pathSeparated.length - 1];
			if (fileName == null) {
				throw new IllegalArgumentException("Url path does not end in a file name");
			}
		}
		final String resolvedFileName = fileName;

		try (CloseableHttpClient httpClient = HttpHelperUtility.getCustomClient(null, keyStore, keyStorePass,
				keyPass)) {
			HttpGet httpGet = new HttpGet(url);
			if (headerMap != null && !headerMap.isEmpty()) {
				for (String key : headerMap.keySet()) {
					httpGet.addHeader(key, headerMap.get(key));
				}
			}
			return httpClient.execute(httpGet, new HttpClientResponseHandler<File>() {
				@Override
				public File handleResponse(ClassicHttpResponse response) throws IOException {
					HttpEntity entity = response.getEntity();

					File fileDir = new File(saveFilePath);
					if (!fileDir.exists()) {
						Boolean success = fileDir.mkdirs();
						if (!success) {
							classLogger.warn("Unable to make the directory to save the file at location: "
									+ Utility.cleanLogString(saveFilePath));
							throw new IllegalArgumentException(
									"Directory to save the file download does not exist and could not be created");
						}
					}

					String fileLocation = Utility.getUniqueFilePath(saveFilePath, resolvedFileName);
					File savedFile = new File(fileLocation);
					if (entity == null) {
						throw new IllegalArgumentException("Connected to " + url + " but no file payload was returned");
					}

					try (InputStream is = entity.getContent()) {
						if (Utility.isVirusScanningEnabled()) {
							try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
								IOUtils.copy(is, baos);
								try (ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray())) {
									Map<String, Collection<String>> viruses = VirusScannerUtils
											.getViruses(resolvedFileName, bais);
									if (!viruses.isEmpty()) {
										String error = "File contained " + viruses.size() + " virus";
										if (viruses.size() > 1) {
											error = error + "es";
										}

										throw new IllegalArgumentException(error);
									}

									bais.reset();
									FileUtils.copyInputStreamToFile(bais, savedFile);
								}
							}
						} else {
							FileUtils.copyInputStreamToFile(is, savedFile);
						}
					} catch (IOException e) {
						classLogger.error(
								"getRequestFileDownload: failed while reading or scanning downloaded file from URL: "
										+ url,
								e);
						throw new IllegalArgumentException("Could not read file item.");
					}

					return savedFile;
				}
			});
		} catch (IOException e) {
			classLogger.error("getRequestFileDownload: failed to download file from URL: " + url, e);
			throw new IllegalArgumentException("Could not connect to URL at " + url);
		}
	}

	/**
	 * Executes an HTTP POST request with a URL-encoded form body.
	 *
	 * @param url          target URL
	 * @param headersMap   optional request headers
	 * @param bodyMap      optional form parameters encoded as
	 *                     {@code application/x-www-form-urlencoded}
	 * @param keyStore     optional path to a keystore file for mutual TLS
	 * @param keyStorePass password for the keystore
	 * @param keyPass      optional key password
	 * @return response payload as a string, or {@code null} when the response has
	 *         no entity
	 * @throws IllegalArgumentException if the URL cannot be reached or the endpoint
	 *                                  returns a non-2xx status
	 */
	public static String postRequestUrlEncodedBody(String url, Map<String, String> headersMap,
			Map<String, String> bodyMap, String keyStore, String keyStorePass, String keyPass) {
		try (CloseableHttpClient httpClient = HttpHelperUtility.getCustomClient(null, keyStore, keyStorePass,
				keyPass)) {
			HttpPost httpPost = new HttpPost(url);
			if (headersMap != null && !headersMap.isEmpty()) {
				for (String key : headersMap.keySet()) {
					httpPost.addHeader(key, headersMap.get(key));
				}
			}
			if (bodyMap != null && !bodyMap.isEmpty()) {
				List<NameValuePair> params = new ArrayList<NameValuePair>();
				for (String key : bodyMap.keySet()) {
					params.add(new BasicNameValuePair(key, bodyMap.get(key)));
				}
				httpPost.setEntity(new UrlEncodedFormEntity(params, StandardCharsets.UTF_8));
			}
			return httpClient.execute(httpPost, new HttpClientResponseHandler<String>() {
				@Override
				public String handleResponse(ClassicHttpResponse response) throws IOException {
					int statusCode = response.getCode();
					HttpEntity entity = response.getEntity();
					if (statusCode >= 200 && statusCode < 300) {
						return readEntityAsString(entity);
					}
					String responseData = readEntityAsStringOrEmpty(entity);
					throw new IllegalArgumentException("Connected to " + url + " but received error = " + responseData);
				}
			});
		} catch (IOException e) {
			classLogger.error("postRequestUrlEncodedBody: failed to execute POST request for URL: " + url, e);
			throw new IllegalArgumentException(
					"Could not connect to URL at " + url + " and received error = " + e.getMessage());
		}
	}

	/**
	 * Executes an HTTP POST request with a string payload.
	 *
	 * @param url          target URL
	 * @param headersMap   optional request headers
	 * @param body         request payload; ignored when {@code null} or empty
	 * @param contentType  content type used for the request entity
	 * @param keyStore     optional path to a keystore file for mutual TLS
	 * @param keyStorePass password for the keystore
	 * @param keyPass      optional key password
	 * @return response payload as a string, or {@code null} when the response has
	 *         no entity
	 * @throws IllegalArgumentException if the URL cannot be reached or the endpoint
	 *                                  returns a non-2xx status
	 */
	public static String postRequestStringBody(String url, Map<String, String> headersMap, String body,
			ContentType contentType, String keyStore, String keyStorePass, String keyPass) {
		try (CloseableHttpClient httpClient = HttpHelperUtility.getCustomClient(null, keyStore, keyStorePass,
				keyPass)) {
			HttpPost httpPost = new HttpPost(url);
			if (headersMap != null && !headersMap.isEmpty()) {
				for (String key : headersMap.keySet()) {
					httpPost.addHeader(key, headersMap.get(key));
				}
			}
			if (body != null && !body.isEmpty()) {
				httpPost.setEntity(new StringEntity(body, contentType));
			}
			return httpClient.execute(httpPost, new HttpClientResponseHandler<String>() {
				@Override
				public String handleResponse(ClassicHttpResponse response) throws IOException {
					int statusCode = response.getCode();
					HttpEntity entity = response.getEntity();
					if (statusCode >= 200 && statusCode < 300) {
						return readEntityAsString(entity);
					}
					String responseData = readEntityAsStringOrEmpty(entity);
					throw new IllegalArgumentException("Connected to " + url + " but received error = " + responseData);
				}
			});
		} catch (IOException e) {
			classLogger.error("postRequestStringBody: failed to execute POST request for URL: " + url, e);
			throw new IllegalArgumentException("Could not connect to URL at " + url);
		}
	}

	/**
	 * Executes an HTTP POST request with a byte-array payload.
	 *
	 * @param url          target URL
	 * @param headersMap   optional request headers
	 * @param bodyBytes    request payload; ignored when {@code null} or empty
	 * @param contentType  content type used for the request entity
	 * @param keyStore     optional path to a keystore file for mutual TLS
	 * @param keyStorePass password for the keystore
	 * @param keyPass      optional key password
	 * @return response payload as a string, or {@code null} when the response has
	 *         no entity
	 * @throws IllegalArgumentException if the URL cannot be reached or the endpoint
	 *                                  returns a non-2xx status
	 */
	public static String postRequestBytesBody(String url, Map<String, String> headersMap, byte[] bodyBytes,
			ContentType contentType, String keyStore, String keyStorePass, String keyPass) {
		try (CloseableHttpClient httpClient = HttpHelperUtility.getCustomClient(null, keyStore, keyStorePass,
				keyPass)) {

			HttpPost httpPost = new HttpPost(url);
			if (headersMap != null && !headersMap.isEmpty()) {
				for (Map.Entry<String, String> entry : headersMap.entrySet()) {
					httpPost.addHeader(entry.getKey(), entry.getValue());
				}
			}
			if (bodyBytes != null && bodyBytes.length > 0) {
				httpPost.setEntity(new ByteArrayEntity(bodyBytes, contentType));
			}

			return httpClient.execute(httpPost, new HttpClientResponseHandler<String>() {
				@Override
				public String handleResponse(ClassicHttpResponse response) throws IOException {
					int statusCode = response.getCode();
					HttpEntity entity = response.getEntity();
					if (statusCode >= 200 && statusCode < 300) {
						return readEntityAsString(entity);
					}
					String responseData = readEntityAsStringOrEmpty(entity);
					throw new IllegalArgumentException("Connected to " + url + " but received error = " + responseData);
				}
			});
		} catch (IOException e) {
			classLogger.error(
					"postRequestBytesBody: failed to execute POST request with byte[] payload for URL: " + url, e);
			throw new IllegalArgumentException("Could not connect to URL at " + url);
		}
	}

	/**
	 * Executes an HTTP PUT request with a string payload.
	 *
	 * @param url          target URL
	 * @param headersMap   optional request headers
	 * @param body         request payload; ignored when {@code null} or empty
	 * @param contentType  content type used for the request entity
	 * @param keyStore     optional path to a keystore file for mutual TLS
	 * @param keyStorePass password for the keystore
	 * @param keyPass      optional key password
	 * @return response payload as a string, or {@code null} when the response has
	 *         no entity
	 * @throws IllegalArgumentException if the URL cannot be reached or the endpoint
	 *                                  returns a non-2xx status
	 */
	public static String putRequestStringBody(String url, Map<String, String> headersMap, String body,
			ContentType contentType, String keyStore, String keyStorePass, String keyPass) {
		try (CloseableHttpClient httpClient = HttpHelperUtility.getCustomClient(null, keyStore, keyStorePass,
				keyPass)) {
			HttpPut httpPut = new HttpPut(url);
			if (headersMap != null && !headersMap.isEmpty()) {
				for (String key : headersMap.keySet()) {
					httpPut.addHeader(key, headersMap.get(key));
				}
			}
			if (body != null && !body.isEmpty()) {
				httpPut.setEntity(new StringEntity(body, contentType));
			}
			return httpClient.execute(httpPut, new HttpClientResponseHandler<String>() {
				@Override
				public String handleResponse(ClassicHttpResponse response) throws IOException {
					int statusCode = response.getCode();
					HttpEntity entity = response.getEntity();
					if (statusCode >= 200 && statusCode < 300) {
						return readEntityAsString(entity);
					}
					String responseData = readEntityAsStringOrEmpty(entity);
					throw new IllegalArgumentException("Connected to " + url + " but received error = " + responseData);
				}
			});
		} catch (IOException e) {
			classLogger.error("putRequestStringBody: failed to execute PUT request for URL: " + url, e);
			throw new IllegalArgumentException("Could not connect to URL at " + url);
		}
	}

	/**
	 * Executes an HTTP PUT request with a URL-encoded form body.
	 *
	 * @param url          target URL
	 * @param headersMap   optional request headers
	 * @param bodyMap      optional form parameters encoded as
	 *                     {@code application/x-www-form-urlencoded}
	 * @param keyStore     optional path to a keystore file for mutual TLS
	 * @param keyStorePass password for the keystore
	 * @param keyPass      optional key password
	 * @return response payload as a string, or {@code null} when the response has
	 *         no entity
	 * @throws IllegalArgumentException if the URL cannot be reached or the endpoint
	 *                                  returns a non-2xx status
	 */
	public static String putRequestUrlEncodedBody(String url, Map<String, String> headersMap,
			Map<String, String> bodyMap, String keyStore, String keyStorePass, String keyPass) {
		try (CloseableHttpClient httpClient = HttpHelperUtility.getCustomClient(null, keyStore, keyStorePass,
				keyPass)) {
			HttpPut httpPost = new HttpPut(url);
			if (headersMap != null && !headersMap.isEmpty()) {
				for (String key : headersMap.keySet()) {
					httpPost.addHeader(key, headersMap.get(key));
				}
			}
			if (bodyMap != null && !bodyMap.isEmpty()) {
				List<NameValuePair> params = new ArrayList<NameValuePair>();
				for (String key : bodyMap.keySet()) {
					params.add(new BasicNameValuePair(key, bodyMap.get(key)));
				}
				httpPost.setEntity(new UrlEncodedFormEntity(params, StandardCharsets.UTF_8));
			}
			return httpClient.execute(httpPost, new HttpClientResponseHandler<String>() {
				@Override
				public String handleResponse(ClassicHttpResponse response) throws IOException {
					int statusCode = response.getCode();
					HttpEntity entity = response.getEntity();
					if (statusCode >= 200 && statusCode < 300) {
						return readEntityAsString(entity);
					}
					String responseData = readEntityAsStringOrEmpty(entity);
					throw new IllegalArgumentException("Connected to " + url + " but received error = " + responseData);
				}
			});
		} catch (IOException e) {
			classLogger.error("putRequestUrlEncodedBody: failed to execute PUT request for URL: " + url, e);
			throw new IllegalArgumentException(
					"Could not connect to URL at " + url + " and received error = " + e.getMessage());
		}
	}
	
	/**
	 * 
	 * @param url
	 * @param headersMap
	 * @param body
	 * @param contentType
	 * @param keyStore
	 * @param keyStorePass
	 * @param keyPass
	 * @return
	 */
	public static String patchRequestStringBody(String url, Map<String, String> headersMap, String body, ContentType contentType, String keyStore, String keyStorePass, String keyPass) {
		String responseData = null;
		CloseableHttpClient httpClient = null;
		CloseableHttpResponse response = null;
		HttpEntity entity = null;
		try {
			httpClient = HttpHelperUtility.getCustomClient(null, keyStore, keyStorePass, keyPass);
			HttpPatch httpPatch = new HttpPatch(url);
			if(headersMap != null && !headersMap.isEmpty()) {
				for(String key : headersMap.keySet()) {
					httpPatch.addHeader(key, headersMap.get(key));
				}
			}
			if(body != null && !body.isEmpty()) {
				httpPatch.setEntity(new StringEntity(body, contentType));
			}
			response = httpClient.execute(httpPatch);
			
			int statusCode = response.getCode();
			entity = response.getEntity();
			if (statusCode >= 200 && statusCode < 300) {
				responseData = entity != null ? EntityUtils.toString(entity, "UTF-8") : null;
			} else {
				responseData = entity != null ? EntityUtils.toString(entity, "UTF-8") : "";
				throw new IllegalArgumentException("Connected to " + url + " but received error = " + responseData);
			}

			return responseData;
		} catch (IOException | ParseException e) {
			classLogger.error("HttpHelperUtility operation failed while handling an HTTP/token request", e);
			throw new IllegalArgumentException("Could not connect to URL at " + url + " and received error = " + e.getMessage());
		}
	}

	/**
	 * Executes an HTTP HEAD request and returns response headers as JSON.
	 *
	 * @param url          target URL
	 * @param headersMap   optional request headers
	 * @param keyStore     optional path to a keystore file for mutual TLS
	 * @param keyStorePass password for the keystore
	 * @param keyPass      optional key password
	 * @return JSON array of header name/value maps
	 * @throws IllegalArgumentException if the URL cannot be reached or the endpoint
	 *                                  returns a non-2xx status
	 */
	public static String headRequest(String url, Map<String, String> headersMap, String keyStore, String keyStorePass,
			String keyPass) {
		try (CloseableHttpClient httpClient = HttpHelperUtility.getCustomClient(null, keyStore, keyStorePass,
				keyPass)) {
			HttpHead httpHead = new HttpHead(url);
			if (headersMap != null && !headersMap.isEmpty()) {
				for (String key : headersMap.keySet()) {
					httpHead.addHeader(key, headersMap.get(key));
				}
			}
			return httpClient.execute(httpHead, new HttpClientResponseHandler<String>() {
				@Override
				public String handleResponse(ClassicHttpResponse response) throws IOException {
					int statusCode = response.getCode();
					HttpEntity entity = response.getEntity();
					if (statusCode >= 200 && statusCode < 300) {
						List<Map<String, String>> headersArray = new ArrayList<>();
						Header[] allHeaders = response.getHeaders();
						for (Header h : allHeaders) {
							Map<String, String> headerMap = new HashMap<>();
							headerMap.put(h.getName(), h.getValue());
							headersArray.add(headerMap);
						}
						return new GsonBuilder().disableHtmlEscaping().create().toJson(headersArray);
					}
					String responseData = readEntityAsStringOrEmpty(entity);
					throw new IllegalArgumentException("Connected to " + url + " but received error = " + responseData);
				}
			});
		} catch (IOException e) {
			classLogger.error("headRequest: failed to execute HEAD request for URL: " + url, e);
			throw new IllegalArgumentException("Could not connect to URL at " + url);
		}
	}

	/**
	 * Executes an HTTP HEAD request and returns only the HTTP status code.
	 *
	 * @param url          target URL
	 * @param headersMap   optional request headers
	 * @param keyStore     optional path to a keystore file for mutual TLS
	 * @param keyStorePass password for the keystore
	 * @param keyPass      optional key password
	 * @return HTTP status code from the response
	 * @throws IllegalArgumentException if the URL cannot be reached
	 */
	public static int headRequestStatus(String url, Map<String, String> headersMap, String keyStore,
			String keyStorePass, String keyPass) {
		try (CloseableHttpClient httpClient = HttpHelperUtility.getCustomClient(null, keyStore, keyStorePass,
				keyPass)) {
			HttpHead httpHead = new HttpHead(url);
			if (headersMap != null && !headersMap.isEmpty()) {
				for (String key : headersMap.keySet()) {
					httpHead.addHeader(key, headersMap.get(key));
				}
			}
			return httpClient.execute(httpHead, new HttpClientResponseHandler<Integer>() {
				@Override
				public Integer handleResponse(ClassicHttpResponse response) throws IOException {
					return response.getCode();
				}
			});
		} catch (IOException e) {
			classLogger.error("headRequestStatus: failed to execute HEAD status request for URL: " + url, e);
			throw new IllegalArgumentException("Could not connect to URL at " + url, e);
		}
	}

	/**
	 * Executes an HTTP DELETE request and returns the response body as UTF-8 text.
	 *
	 * @param url          target URL
	 * @param headersMap   optional request headers
	 * @param keyStore     optional path to a keystore file for mutual TLS
	 * @param keyStorePass password for the keystore
	 * @param keyPass      optional key password
	 * @return response payload as a string, or {@code null} when the response has
	 *         no entity
	 * @throws IllegalArgumentException if the URL cannot be reached or the endpoint
	 *                                  returns a non-2xx status
	 */
	public static String deleteRequestStringBody(String url, Map<String, String> headersMap, String keyStore,
			String keyStorePass, String keyPass) {
		try (CloseableHttpClient httpClient = HttpHelperUtility.getCustomClient(null, keyStore, keyStorePass,
				keyPass)) {
			HttpDelete httpHead = new HttpDelete(url);
			if (headersMap != null && !headersMap.isEmpty()) {
				for (String key : headersMap.keySet()) {
					httpHead.addHeader(key, headersMap.get(key));
				}
			}
			return httpClient.execute(httpHead, new HttpClientResponseHandler<String>() {
				@Override
				public String handleResponse(ClassicHttpResponse response) throws IOException {
					int statusCode = response.getCode();
					HttpEntity entity = response.getEntity();
					if (statusCode >= 200 && statusCode < 300) {
						return readEntityAsString(entity);
					}
					String responseData = readEntityAsStringOrEmpty(entity);
					throw new IllegalArgumentException("Connected to " + url + " but received error = " + responseData);
				}
			});
		} catch (IOException e) {
			classLogger.error("deleteRequestStringBody: failed to execute DELETE request for URL: " + url, e);
			throw new IllegalArgumentException("Could not connect to URL at " + url);
		}
	}

	/////////////////////////////////////////////////////
	/////////////////////////////////////////////////////
	/////////////////////////////////////////////////////

	/*
	 * Methods for generating an access token
	 */

	/**
	 * Requests an OAuth token endpoint and optionally parses the response into an
	 * {@link AccessToken}.
	 *
	 * @param url     token endpoint URL
	 * @param params  form parameters sent as URL-encoded body values
	 * @param json    {@code true} when the response is JSON; {@code false} for
	 *                key-value form responses
	 * @param extract {@code true} to parse and return a token object; {@code false}
	 *                to skip parsing
	 * @return parsed {@link AccessToken} when {@code extract} is {@code true} and
	 *         parsing succeeds; otherwise {@code null}
	 */
	public static AccessToken getAccessToken(String url, Map<String, String> params, boolean json, boolean extract) {
		AccessToken tok = null;
		CloseableHttpClient httpclient = null;
		String result = null;
		try {
			httpclient = HttpClientBuilder.create().useSystemProperties().build();
			;
			// this is a post
			HttpPost httppost = new HttpPost(url);
			// loop through all keys and add as basic name value pair
			List<NameValuePair> paramList = new ArrayList<NameValuePair>();
			params.keySet().stream().forEach(param -> paramList.add(new BasicNameValuePair(param, params.get(param))));
			// set within post
			httppost.setEntity(new UrlEncodedFormEntity(paramList));

			final String[] resultHolder = new String[1];
			tok = httpclient.execute(httppost, new HttpClientResponseHandler<AccessToken>() {
				@Override
				public AccessToken handleResponse(ClassicHttpResponse response) throws IOException {
					int status = response.getCode();
					classLogger.info("Request for access token at " + url + " returned status code = " + status);

					HttpEntity entity = response.getEntity();
					if (entity != null) {
						try (InputStream content = entity.getContent()) {
							resultHolder[0] = IOUtils.toString(content, StandardCharsets.UTF_8);
						}
					} else {
						resultHolder[0] = null;
					}
					classLogger.info("Request response = " + Utility.cleanLogString(resultHolder[0]));

					if (status == 200 && extract) {
						if (json) {
							return getJAccessToken(resultHolder[0]);
						}
						return getAccessToken(resultHolder[0]);
					}
					return null;
				}
			});
			result = resultHolder[0];
		} catch (UnsupportedEncodingException e) {
			classLogger.error("getAccessToken: unsupported encoding while processing token response from URL: " + url,
					e);
		} catch (ClientProtocolException e) {
			classLogger.error("getAccessToken: HTTP protocol error while requesting token from URL: " + url, e);
		} catch (UnsupportedOperationException e) {
			classLogger.error("getAccessToken: unsupported operation while requesting token from URL: " + url, e);
		} catch (IOException e) {
			classLogger.error("getAccessToken: I/O error while requesting token from URL: " + url, e);
		} finally {
			if (httpclient != null) {
				try {
					httpclient.close();
				} catch (IOException e) {
					classLogger.error("getAccessToken: failed to close HTTP client for URL: " + url, e);
				}
			}
		}

		if (tok != null && tok.getAccess_token() == null) {
			classLogger.warn("Error occurred grabbing the access token: " + Utility.cleanLogString(result));
		}

		// send back the token
		return tok;
	}

	/**
	 * Requests an OAuth token endpoint and optionally parses the response into an
	 * ID-token-oriented {@link AccessToken}.
	 *
	 * @param url     token endpoint URL
	 * @param params  form parameters sent as URL-encoded body values
	 * @param json    {@code true} when the response is JSON; {@code false} for
	 *                key-value form responses
	 * @param extract {@code true} to parse and return a token object; {@code false}
	 *                to skip parsing
	 * @return parsed {@link AccessToken} when {@code extract} is {@code true} and
	 *         parsing succeeds; otherwise {@code null}
	 */
	public static AccessToken getIdToken(String url, Map<String, String> params, boolean json, boolean extract) {
		// still using accessToken object
		AccessToken tok = null;
		CloseableHttpClient httpclient = null;
		String result = null;
		try {
			httpclient = HttpClientBuilder.create().useSystemProperties().build();
			// this is a post
			HttpPost httppost = new HttpPost(url);
			// loop through all keys and add as basic name value pair
			List<NameValuePair> paramList = new ArrayList<NameValuePair>();
			params.keySet().stream().forEach(param -> paramList.add(new BasicNameValuePair(param, params.get(param))));
			// set within post
			httppost.setEntity(new UrlEncodedFormEntity(paramList, StandardCharsets.UTF_8));

			final String[] resultHolder = new String[1];
			tok = httpclient.execute(httppost, new HttpClientResponseHandler<AccessToken>() {
				@Override
				public AccessToken handleResponse(ClassicHttpResponse response) throws IOException {
					int status = response.getCode();
					classLogger.info("Request for access token at " + url + " returned status code = " + status);

					HttpEntity entity = response.getEntity();
					if (entity != null) {
						try (InputStream content = entity.getContent()) {
							resultHolder[0] = IOUtils.toString(content, StandardCharsets.UTF_8);
						}
					} else {
						resultHolder[0] = null;
					}
					classLogger.info("Request response = " + Utility.cleanLogString(resultHolder[0]));

					if (status == 200 && extract) {
						if (json) {
							return getJIDToken(resultHolder[0]);
						}
						return getIDToken(resultHolder[0]);
					}
					return null;
				}
			});
			result = resultHolder[0];
		} catch (Exception e) {
			classLogger.error("getIdToken: failed to request or parse ID token from URL: " + url, e);
		} finally {
			if (httpclient != null) {
				try {
					httpclient.close();
				} catch (IOException e) {
					classLogger.error("getIdToken: failed to close HTTP client for URL: " + url, e);
				}
			}
		}

		if (tok != null && tok.getAccess_token() == null) {
			classLogger.warn("Error occurred grabbing the id token: " + Utility.cleanLogString(result));
		}

		// send back the token
		return tok;
	}

	/**
	 * Parses an access token from a URL-encoded key-value response body.
	 *
	 * @param input response body containing an {@code access_token} entry
	 * @return parsed {@link AccessToken}
	 */
	public static AccessToken getAccessToken(String input) {
		return getAccessToken(input, "access_token");
	}

	/**
	 * Parses an ID token from a URL-encoded key-value response body.
	 *
	 * @param input response body containing an {@code id_token} entry
	 * @return parsed {@link AccessToken}
	 */
	public static AccessToken getIDToken(String input) {
		return getAccessToken(input, "id_token");
	}

	/**
	 * Parses token values from a URL-encoded key-value response body.
	 * <p>
	 * Example input: {@code access_token=...&scope=...&token_type=bearer}.
	 * </p>
	 *
	 * @param input       response body in key-value pair format
	 * @param nameOfToken token key to read (for example {@code access_token} or
	 *                    {@code id_token})
	 * @return populated {@link AccessToken} including optional refresh-token
	 *         metadata
	 */
	public static AccessToken getAccessToken(String input, String nameOfToken) {
		String accessToken = null;
		String refreshToken = null;
		String[] tokens = input.split("&");
		for (int tokenIndex = 0; tokenIndex < tokens.length; tokenIndex++) {
			String thisToken = tokens[tokenIndex];
			if (thisToken.startsWith(nameOfToken)) {
				accessToken = thisToken.replaceAll(nameOfToken + "=", "");
			} else if (thisToken.startsWith("refresh_token=")) {
				refreshToken = thisToken.replaceAll("refresh_token=", "");
			}
		}
		AccessToken tok = new AccessToken();
		tok.setAccess_token(accessToken);
		if (refreshToken != null && !refreshToken.isEmpty()) {
			try {
				tok.addMetaValue("refresh_token", URLDecoder.decode(refreshToken, StandardCharsets.UTF_8.toString()));
			} catch (UnsupportedEncodingException e) {
				classLogger.error("getAccessToken(input, nameOfToken): failed to decode refresh token value", e);
				tok.addMetaValue("refresh_token", refreshToken);
			}
		}
		tok.init();

		return tok;
	}

	/**
	 * Parses an access token from a JSON token response.
	 *
	 * @param input token response JSON
	 * @return parsed {@link AccessToken}
	 */
	public static AccessToken getJAccessToken(String input) {
		return getJAccessToken(input, "[access_token, token_type, expires_in, instance_url]");
	}

	/**
	 * Parses an ID token from a JSON token response.
	 *
	 * @param input token response JSON
	 * @return parsed {@link AccessToken}
	 */
	public static AccessToken getJIDToken(String input) {
		return getJAccessToken(input, "[id_token, token_type, expires_in]");
	}

	/**
	 * Parses token values from JSON using a JMESPath expression.
	 *
	 * @param json        token response JSON
	 * @param nameOfToken JMESPath expression that resolves to an array containing
	 *                    token fields in order: token value, token type, and expiry
	 *                    seconds
	 * @return populated {@link AccessToken}; an empty token object is returned when
	 *         parsing fails
	 */
	public static AccessToken getJAccessToken(String json, String nameOfToken) {
		AccessToken tok = new AccessToken();
		try {
			JmesPath<JsonNode> jmespath = new JacksonRuntime();
			// Expressions need to be compiled before you can search. Compiled expressions
			// are reusable and thread safe
			// Compile your expressions once, just like database prepared statements.
			Expression<JsonNode> expression = jmespath.compile(nameOfToken);

			JsonNode input = mapper.readTree(json);
			JsonNode result = expression.search(input);
			if (result.size() >= 0) {
				tok.setAccess_token(result.get(0).asText());
			}
			if (result.size() >= 1) {
				tok.setToken_type(result.get(1).asText());
			}
			if (result.size() >= 2) {
				tok.setExpires_in(result.get(2).asInt());
			}
			if(result.size() >= 3) {
				tok.setInstance_url(result.get(3).asText());
      }
			JsonNode refreshTokenNode = input.get("refresh_token");
			if (refreshTokenNode != null && !refreshTokenNode.isNull()) {
				String refreshToken = refreshTokenNode.asText();
				if (refreshToken != null && !refreshToken.isEmpty()) {
					tok.addMetaValue("refresh_token", refreshToken);
				}
			}
			tok.init();
		} catch (IOException e) {
			classLogger.error("getJAccessToken: failed to parse token JSON using expression: " + nameOfToken, e);
		}
		return tok;
	}

	/////////////////////////////////////////////////////
	/////////////////////////////////////////////////////
	/////////////////////////////////////////////////////

	/*
	 * Methods for making requests using the access token
	 */

	/**
	 * Executes an authenticated GET request without query parameters.
	 *
	 * @param urlStr      target URL
	 * @param accessToken bearer token value
	 * @return response body
	 */
	public static String makeGetCall(String urlStr, String accessToken) {
		return makeGetCall(urlStr, accessToken, null, true);
	}

	/**
	 * Executes a GET request with optional query parameters and optional bearer
	 * authentication.
	 *
	 * @param urlStr      target URL
	 * @param accessToken bearer token used when {@code auth} is {@code true}
	 * @param params      optional query-string parameters
	 * @param auth        {@code true} to add an Authorization header; {@code false}
	 *                    for an unauthenticated request
	 * @return response body
	 * @throws NullPointerException     if {@code urlStr} is {@code null}
	 * @throws IllegalArgumentException if the response status is 4xx/5xx
	 */
	public static String makeGetCall(String urlStr, String accessToken, Map<String, Object> params, boolean auth) {
		if (urlStr == null) {
			throw new NullPointerException("Must provide the URL");
		}
		// fill the params on the get since it is not null
		if (params != null) {
			StringBuffer urlBuf = new StringBuffer(urlStr);
			urlBuf.append("?");
			boolean first = true;
			Set<String> keys = params.keySet();
			for (String key : keys) {
				Object value = params.get(key);
				if (!first) {
					urlBuf.append("&");
				}

				urlBuf.append(key).append("=").append(URLEncoder.encode(value + "", StandardCharsets.UTF_8));
				first = false;
			}
			urlStr = urlBuf.toString();
		}

		String retString = null;
		String responseCode = null;
		BufferedReader br = null;
		InputStreamReader isr = null;
		try {
			HttpURLConnection con = null;
			URL url = new URI(urlStr).toURL();
			con = (HttpURLConnection) url.openConnection();
			con.setDoInput(true);
			con.setDoOutput(true);
			con.setUseCaches(false);
			con.setRequestMethod("GET");
			con.setRequestProperty("User-Agent", "SEMOSS");
			if (auth) {
				con.setRequestProperty("Authorization", "Bearer " + accessToken);
			}
			con.setRequestProperty("Accept", "application/json"); // I added this line.
			con.connect();

			isr = new InputStreamReader(con.getInputStream(), StandardCharsets.UTF_8);
			br = new BufferedReader(isr);
			StringBuilder str = new StringBuilder();
			String line;
			while ((line = br.readLine()) != null) {
				str.append(line);
			}
			retString = str.toString();

			responseCode = String.valueOf(con.getResponseCode());
		} catch (Exception e) {
			classLogger.error("makeGetCall: failed to execute GET request for URL: " + urlStr, e);
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					classLogger.error("makeGetCall: failed to close response BufferedReader for URL: " + urlStr, e);
				}
			}
			if (isr != null) {
				try {
					isr.close();
				} catch (IOException e) {
					classLogger.error("makeGetCall: failed to close response InputStreamReader for URL: " + urlStr, e);
				}
			}
		}

		classLogger.info("Return from " + urlStr + " with response " + responseCode + " = " + retString);
		if (responseCode.startsWith("4") || responseCode.startsWith("5")) {
			throw new IllegalArgumentException(retString);
		}

		return retString;
	}

	/**
	 * Opens a GET request and returns a reader for streaming the response body.
	 * <p>
	 * The caller is responsible for closing the returned {@link BufferedReader}.
	 * </p>
	 *
	 * @param urlStr      target URL
	 * @param accessToken bearer token used when {@code auth} is {@code true}
	 * @param params      optional query-string parameters
	 * @param auth        {@code true} to add an Authorization header; {@code false}
	 *                    for an unauthenticated request
	 * @return open {@link BufferedReader} for the response body, or {@code null}
	 *         when the request fails
	 */
	public static BufferedReader getHttpStream(String urlStr, String accessToken, Map<String, Object> params,
			boolean auth) {
		// fill the params on the get since it is not null
		// fill the params on the get since it is not null
		if (params != null) {
			StringBuffer urlBuf = new StringBuffer(urlStr);
			urlBuf.append("?");
			boolean first = true;
			Set<String> keys = params.keySet();
			for (String key : keys) {
				Object value = params.get(key);
				if (!first) {
					urlBuf.append("&");
				}

				urlBuf.append(key).append("=").append(URLEncoder.encode(value + "", StandardCharsets.UTF_8));
				first = false;
			}
			urlStr = urlBuf.toString();
		}

		try {
			HttpURLConnection con = null;
			URL url = new URI(urlStr).toURL();
			con = (HttpURLConnection) url.openConnection();
			con.setDoInput(true);
			con.setDoOutput(true);
			con.setUseCaches(false);
			con.setRequestMethod("GET");
			con.setRequestProperty("User-Agent", "SEMOSS");
			if (auth) {
				con.setRequestProperty("Authorization", "Bearer " + accessToken);
			}
			con.setRequestProperty("Accept", "application/json"); // I added this line.
			con.connect();

			BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream(), StandardCharsets.UTF_8));
			return br;
		} catch (Exception e) {
			classLogger.error("getHttpStream: failed to open HTTP stream for URL: " + urlStr, e);
		}

		return null;
	}

	/**
	 * Executes an authenticated POST request with either URL-encoded form values or
	 * a JSON payload.
	 *
	 * @param url         target URL
	 * @param accessToken bearer token value
	 * @param input       request body object; expected to be a {@link Hashtable}
	 *                    when {@code json} is {@code false}
	 * @param json        {@code true} to serialize {@code input} as JSON;
	 *                    {@code false} to send URL-encoded form fields
	 * @return response body, or {@code null} when the request fails
	 */
	public static String makePostCall(String url, String accessToken, Object input, boolean json) {
		CloseableHttpClient httpclient = null;
		try {
			httpclient = HttpClientBuilder.create().useSystemProperties().build();
			HttpPost httppost = new HttpPost(url);
			httppost.addHeader("Authorization", "Bearer " + accessToken);
			httppost.addHeader("Content-Type", "application/json; charset=utf-8");
			Hashtable params = null;
			List<NameValuePair> paramList = new ArrayList<NameValuePair>();
			if (!json) {
				params = (Hashtable) input;
				Enumeration<String> keys = params.keys();
				while (keys.hasMoreElements()) {
					String key = keys.nextElement();
					String value = (String) params.get(key);
					paramList.add(new BasicNameValuePair(key, value));
				}
				httppost.setEntity(new UrlEncodedFormEntity(paramList, StandardCharsets.UTF_8));
			}
			// this is a json input
			else {
				String inputJson = mapper.writeValueAsString(input);
				httppost.setEntity(new StringEntity(inputJson));
			}

			return httpclient.execute(httppost, new HttpClientResponseHandler<String>() {
				@Override
				public String handleResponse(ClassicHttpResponse response) throws IOException {
					HttpEntity entity = response.getEntity();
					if (entity == null) {
						return null;
					}

					try (BufferedReader rd = new BufferedReader(
							new InputStreamReader(entity.getContent(), StandardCharsets.UTF_8))) {
						StringBuffer result = new StringBuffer();
						String line = "";
						while ((line = rd.readLine()) != null) {
							result.append(line);
						}
						return result.toString();
					}
				}
			});
		} catch (Exception ex) {
			classLogger.error("makePostCall: failed to execute POST request for URL: " + url, ex);
		} finally {
			if (httpclient != null) {
				try {
					httpclient.close();
				} catch (IOException e) {
					classLogger.error("makePostCall: failed to close HTTP client for URL: " + url, e);
				}
			}
		}

		return null;
	}

	/**
	 * Uploads a local file using an authenticated HTTP PUT request.
	 *
	 * @param url         target URL
	 * @param accessToken bearer token value
	 * @param fileName    file name label used by callers (not used directly in the
	 *                    request payload)
	 * @param localPath   path to the local file to upload
	 * @return response body, or {@code null} when the request fails
	 */
	public static String makeBinaryFilePutCall(String url, String accessToken, String fileName, String localPath) {
		CloseableHttpClient httpclient = null;
		try {
			httpclient = HttpClientBuilder.create().useSystemProperties().build();
			HttpPut httpput = new HttpPut(url);
			httpput.addHeader("Authorization", "Bearer " + accessToken);
			httpput.addHeader("Content-Type", "application/json; charset=utf-8");
			File fileupload = new File(localPath);
			httpput.setEntity(new FileEntity(fileupload, ContentType.APPLICATION_OCTET_STREAM));
			return httpclient.execute(httpput, new HttpClientResponseHandler<String>() {
				@Override
				public String handleResponse(ClassicHttpResponse response) throws IOException {
					HttpEntity entity = response.getEntity();
					if (entity == null) {
						return null;
					}

					try (BufferedReader rd = new BufferedReader(
							new InputStreamReader(entity.getContent(), StandardCharsets.UTF_8))) {
						StringBuffer result = new StringBuffer();
						String line = "";
						while ((line = rd.readLine()) != null) {
							result.append(line);
						}
						return result.toString();
					}
				}
			});
		} catch (Exception ex) {
			classLogger.error("makeBinaryFilePutCall: failed to upload file via PUT for URL: " + url, ex);
		} finally {
			if (httpclient != null) {
				try {
					httpclient.close();
				} catch (IOException e) {
					classLogger.error("makeBinaryFilePutCall: failed to close HTTP client for URL: " + url, e);
				}
			}
		}

		return null;

	}

	/**
	 * Uploads a local file using an authenticated HTTP POST request.
	 *
	 * @param url         target URL
	 * @param accessToken bearer token value
	 * @param filename    remote file name used in provider-specific headers
	 * @param filepath    path to the local file to upload
	 * @return response body, or {@code null} when the request fails
	 */
	public static String makeBinaryFilePostCall(String url, String accessToken, String filename, String filepath) {
		CloseableHttpClient httpclient = null;
		try {
			httpclient = HttpClientBuilder.create().useSystemProperties().build();
			HttpPost httppost = new HttpPost(url);
			httppost.addHeader("Authorization", "Bearer " + accessToken);
			httppost.addHeader("Content-Type", "application/octet-stream");
			httppost.addHeader("Dropbox-API-Arg",
					"{\"path\": \"/" + filename + "\",\"mode\": \"add\",\"autorename\": true,\"mute\": false}");
			File fileupload = new File(filepath);
			httppost.setEntity(new FileEntity(fileupload, ContentType.APPLICATION_OCTET_STREAM));
			return httpclient.execute(httppost, new HttpClientResponseHandler<String>() {
				@Override
				public String handleResponse(ClassicHttpResponse response) throws IOException {
					HttpEntity entity = response.getEntity();
					if (entity == null) {
						return null;
					}

					try (BufferedReader rd = new BufferedReader(
							new InputStreamReader(entity.getContent(), StandardCharsets.UTF_8))) {
						StringBuffer result = new StringBuffer();
						String line = "";
						while ((line = rd.readLine()) != null) {
							result.append(line);
						}
						return result.toString();
					}
				}
			});
		} catch (Exception ex) {
			classLogger.error("makeBinaryFilePostCall: failed to upload file via POST for URL: " + url, ex);
		} finally {
			if (httpclient != null) {
				try {
					httpclient.close();
				} catch (IOException e) {
					classLogger.error("makeBinaryFilePostCall: failed to close HTTP client for URL: " + url, e);
				}
			}
		}
		return null;
	}

	/**
	 * Uploads a local file using an authenticated HTTP PATCH request.
	 *
	 * @param url         target URL
	 * @param accessToken bearer token value
	 * @param filepath    path to the local file to upload
	 * @return response body, or {@code null} when the request fails
	 */
	public static String makeBinaryFilePatchCall(String url, String accessToken, String filepath) {
		CloseableHttpClient httpclient = null;
		try {
			httpclient = HttpClientBuilder.create().useSystemProperties().build();
			HttpPatch httppatch = new HttpPatch(url);
			httppatch.addHeader("Authorization", "Bearer " + accessToken);

			File fileupload = new File(filepath);
			httppatch.setEntity(new FileEntity(fileupload, ContentType.APPLICATION_OCTET_STREAM));
			return httpclient.execute(httppatch, new HttpClientResponseHandler<String>() {
				@Override
				public String handleResponse(ClassicHttpResponse response) throws IOException {
					HttpEntity entity = response.getEntity();
					if (entity == null) {
						return null;
					}

					try (BufferedReader rd = new BufferedReader(
							new InputStreamReader(entity.getContent(), StandardCharsets.UTF_8))) {
						StringBuffer result = new StringBuffer();
						String line = "";
						while ((line = rd.readLine()) != null) {
							result.append(line);
						}
						return result.toString();
					}
				}
			});
		} catch (Exception ex) {
			classLogger.error("makeBinaryFilePatchCall: failed to upload file via PATCH for URL: " + url, ex);
		} finally {
			if (httpclient != null) {
				try {
					httpclient.close();
				} catch (IOException e) {
					classLogger.error("makeBinaryFilePatchCall: failed to close HTTP client for URL: " + url, e);
				}
			}
		}
		return null;
	}

	/**
	 * Reads an HTTP entity as a UTF-8 string.
	 *
	 * @param entity response entity
	 * @return string representation of the entity, or {@code null} when no entity
	 *         is present
	 * @throws IOException if the entity cannot be read or parsed
	 */
	private static String readEntityAsString(HttpEntity entity) throws IOException {
		if (entity == null) {
			return null;
		}
		try {
			return EntityUtils.toString(entity, StandardCharsets.UTF_8);
		} catch (ParseException e) {
			throw new IOException("Failed to parse HTTP response body", e);
		}
	}

	/**
	 * Reads an HTTP entity as a UTF-8 string and normalizes {@code null} to an
	 * empty string.
	 *
	 * @param entity response entity
	 * @return entity contents as a string, or an empty string when no entity is
	 *         present
	 * @throws IOException if the entity cannot be read or parsed
	 */
	private static String readEntityAsStringOrEmpty(HttpEntity entity) throws IOException {
		String body = readEntityAsString(entity);
		return body == null ? "" : body;
	}

	/**
	 * Parses OAuth callback query parameters and extracts authorization code and
	 * state.
	 *
	 * @param queryStr query string from a callback URL
	 * @return array with two elements: index {@code 0} is {@code code}, index
	 *         {@code 1} is {@code state}
	 */
	public static String[] getCodes(String queryStr) {
		String[] retString = new String[2];
		String[] inputCodes = URLDecoder.decode(queryStr, StandardCharsets.UTF_8).split("&");
		for (int inputIndex = 0; inputIndex < inputCodes.length; inputIndex++) {
			String thisToken = Utility.inputSQLSanitizer(inputCodes[inputIndex]);
			if (thisToken.startsWith("state")) {
				retString[1] = thisToken.replaceAll("state=", "");
			}
			if (thisToken.startsWith("code")) {
				retString[0] = thisToken.replaceAll("code=", "");
			}
		}

		return retString;
	}

	/**
	 * Utility class constructor intentionally hidden.
	 */
	private HttpHelperUtility() {

	}
}
