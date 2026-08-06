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
package prerna.engine.impl.model;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import prerna.engine.impl.model.responses.IModelEngineResponseHandler;
import prerna.engine.impl.model.responses.IModelEngineResponseStreamHandler;
import prerna.om.ThreadStore;
import prerna.sablecc2.comm.PixelJobManager;
import prerna.sablecc2.comm.PixelJobRunner;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;

public abstract class AbstractRESTModelEngine extends AbstractModelEngine {

	private static final Logger classLogger = LogManager.getLogger(AbstractRESTModelEngine.class);

	protected static final String ENDPOINT = "ENDPOINT";

	protected ScheduledExecutorService scheduler;
	private ScheduledFuture<?> scheduledFuture = null; // Holds the future of the scheduled task
	protected Runnable timeoutAction = this::resetAfterTimeout;
	private long timeoutDelay; // Delay after which the timeoutMethod is called

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		String timeout = this.smssProp.getProperty(Constants.IDLE_TIMEOUT, "30");
		this.timeoutDelay = Long.parseLong(timeout);
		this.scheduler = Executors.newScheduledThreadPool(1);
	}

	/**
	 * This method is responsible for resetting the timeout window between REST
	 * calls.
	 */
	protected synchronized void resetTimer() {
		if (scheduledFuture != null && !scheduledFuture.isDone()) {
			scheduledFuture.cancel(false);
		}

		scheduledFuture = scheduler.schedule(timeoutAction, timeoutDelay, TimeUnit.MINUTES);
	}

	/**
	 * This method defined what should happen when the timeout is reached. Currently
	 * this is an abstract method until conversation history / chains are
	 * standardized.
	 */
	protected abstract void resetAfterTimeout();

	@Override
	public void close() throws IOException {
		this.scheduler.shutdown();
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
	 * @param isStream
	 * @param responseType
	 * @param insightId
	 * @return
	 */
	protected IModelEngineResponseHandler postRequestStringBody(String url, Map<String, String> headersMap, String body,
			ContentType contentType, String keyStore, String keyStorePass, String keyPass, boolean isStream,
			Class<? extends IModelEngineResponseHandler> responseType, String insightId) {
		CloseableHttpClient httpClient = null;
		CloseableHttpResponse response = null;
		try {
			httpClient = HttpHelperUtility.getCustomClient(null, keyStore, keyStorePass, keyPass);
			HttpPost httpPost = new HttpPost(url);
			if (headersMap != null && !headersMap.isEmpty()) {
				for (String key : headersMap.keySet()) {
					httpPost.addHeader(key, headersMap.get(key));
				}
			}
			if (body != null && !body.isEmpty()) {
				httpPost.setEntity(new StringEntity(body, contentType));
			}
			response = httpClient.execute(httpPost);

			int statusCode = response.getCode();
			if (statusCode >= 200 && statusCode < 300) {
				HttpEntity entity = response.getEntity();
				if (!isStream) {
					// Handle regular response
					String responseData = entity != null ? EntityUtils.toString(entity, StandardCharsets.UTF_8) : null;
					return handleDeserialization(responseData, responseType);
				} else {
					// Handle streaming response
					if (entity != null) {
						// Closing the response on cancel unblocks reader.readLine() with an IOException below.
						final CloseableHttpResponse responseRef = response;
						String threadJobId = ThreadStore.getJobId();
						PixelJobRunner jobRunner = threadJobId != null
								? PixelJobManager.getManager().getJob(threadJobId)
								: null;
						if (jobRunner != null) {
							jobRunner.setCancelHook(() -> {
								try {
									responseRef.close();
								} catch (Exception ignored) {
									// ignore - we're cancelling anyway
								}
							});
							// Cancel may have fired before the hook was registered; honor it now so the
							// pending readLine() unblocks instead of waiting for the first streamed line.
							if (jobRunner.isCancelRequested()) {
								try {
									responseRef.close();
								} catch (Exception ignored) {
									// ignore - we're cancelling anyway
								}
							}
						}
						try (BufferedReader reader = new BufferedReader(
								new InputStreamReader(entity.getContent(), StandardCharsets.UTF_8))) {
							String line;
							StringBuilder responseAssimilator = new StringBuilder();
							IModelEngineResponseHandler responseObject = responseType.newInstance();

							while ((line = reader.readLine()) != null) {
//	                        	System.out.println(line);
								// fast-exit if cancellation was requested between lines
								if (Thread.currentThread().isInterrupted()) {
									break;
								}
								if (line.contains("data: [DONE]") || line.contains("data:[DONE]")) {
									break;
								}

								if (line.startsWith("data:")) {
									// Extract JSON part
									String jsonPart = line.substring("data:".length()).trim();
									IModelEngineResponseStreamHandler partialObject = new Gson().fromJson(jsonPart,
											responseObject.getStreamHandlerClass());
									Object partial = partialObject.getPartialResponse();

									if (partial != null) {
										responseObject.appendStream(partialObject);
										PixelJobManager.getManager().addPartialOut(insightId, partial + "");
										responseAssimilator.append(partial);
									}
								} else if (!line.isEmpty()) {
									classLogger.debug("Found unknown rest model response = " + line);
								}
							}
							responseObject.setResponse(responseAssimilator.toString());
							return responseObject;
						} catch (Exception e) {
							// Cancel-triggered close surfaces as IOException here; convert to a clean cancellation.
							if (Thread.currentThread().isInterrupted()
									|| (jobRunner != null && jobRunner.isCancelRequested())) {
								throw new IllegalStateException("LLM stream cancelled by user");
							}
							classLogger.error(Constants.STACKTRACE, e);
							throw new IllegalArgumentException(
									"There was an error processing the response from " + url);
						} finally {
							// always clear the hook so it can't be fired after the call ends
							if (jobRunner != null) {
								jobRunner.setCancelHook(null);
							}
						}
					}
				}
			} else {
				// try to send back the error from the server
				String errorResponse = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
				throw new IllegalArgumentException("Connected to " + url + " but received error = " + errorResponse);
			}
		} catch (IOException | ParseException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Could not connect to URL at " + url);
		} finally {
			try {
				if (response != null) {
					response.close();
				}
				if (httpClient != null) {
					httpClient.close();
				}
			} catch (IOException e) {
				classLogger.error("Error while closing resources", e);
			}
		}
		return null; // In case of unexpected flow
	}

	/**
	 * This method is intended to be overridden in an implementing class if the
	 * responseData requires more unique deserialization than gson.fromJson() can
	 * provide
	 * 
	 * @param responseData
	 * @param responseType
	 * @return
	 */
	protected IModelEngineResponseHandler handleDeserialization(String responseData,
			Class<? extends IModelEngineResponseHandler> responseType) {
		IModelEngineResponseHandler responseObject = new Gson().fromJson(responseData, responseType);
		return responseObject;
	}

}
