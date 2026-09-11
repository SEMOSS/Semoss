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
package prerna.rpa.quartz.jobs.insight;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.cookie.BasicCookieStore;
import org.apache.hc.client5.http.cookie.CookieStore;
import org.apache.hc.client5.http.entity.UrlEncodedFormEntity;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.InterruptableJob;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

import prerna.reactor.scheduler.SchedulerDatabaseUtility;
import prerna.rpa.config.JobConfigKeys;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class RunPixelJobFromDB implements InterruptableJob {

	private static final Logger classLogger = LogManager.getLogger(RunPixelJobFromDB.class);

	public static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	private static final String CSRF_TOKEN_HEADER = "X-CSRF-Token";
	private static final String CSRF_FETCH_PATH = "/api/config/fetchCsrf";
	private static final String EXECUTE_PIXEL_PATH = "/api/schedule/executePixel";

	private static boolean FETCH_CSRF = false;

	/**
	 * Status code and payload returned by the scheduler endpoint.
	 *
	 * @param statusCode   HTTP status code returned by the endpoint
	 * @param responseBody response payload, or {@code null} when the response has
	 *                     no entity
	 */
	private record SchedulerResponse(int statusCode, String responseBody) {
	}

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		String jobId = context.getJobDetail().getKey().getName();
		String jobGroup = context.getJobDetail().getKey().getGroup();

		JobDataMap dataMap = context.getMergedJobDataMap();
		String pixel = dataMap.getString(JobConfigKeys.PIXEL);
		String pixelParameters = dataMap.getString(JobConfigKeys.PIXEL_PARAMETERS);
		String userAccess = dataMap.getString(JobConfigKeys.USER_ACCESS);

		String execId = UUID.randomUUID().toString();
		// insert the exec id so we allow the execution
		SchedulerDatabaseUtility.insertIntoExecutionTable(execId, jobId, jobGroup);

		// add the scheduler cert if required
		String keyStore = Utility.getDIHelperProperty(Constants.SCHEDULER_KEYSTORE);
		String keyStorePass = Utility.getDIHelperProperty(Constants.SCHEDULER_KEYSTORE_PASSWORD);
		String keyPass = Utility.getDIHelperProperty(Constants.SCHEDULER_CERTIFICATE_PASSWORD);

		try {
			// run the pixel endpoint
			String url = Utility.getDIHelperProperty(Constants.SCHEDULER_ENDPOINT);
			if (url == null) {
				throw new IllegalArgumentException("Must define the scheduler endpoint to run scheduled jobs");
			}
			url = url.trim();
			if (url.endsWith("/")) {
				url = url.substring(0, url.length() - 1);
			}
			String executeUrl = url + EXECUTE_PIXEL_PATH;

			boolean success = false;
			int status = -1;
			String schedulerOutput = null;
			long start = System.currentTimeMillis();

			// the csrf token is bound to the session cookie, so the token fetch and the
			// execution have to share a single client and cookie store
			CookieStore httpCookieStore = new BasicCookieStore();
			try (CloseableHttpClient httpClient = HttpHelperUtility.getCustomClient(httpCookieStore, keyStore,
					keyStorePass, keyPass)) {
				String csrfToken = null;
				if (FETCH_CSRF) {
					csrfToken = fetchCsrfToken(httpClient, url + CSRF_FETCH_PATH);
				}

				HttpPost httpPost = new HttpPost(executeUrl);
				if (csrfToken != null) {
					httpPost.addHeader(CSRF_TOKEN_HEADER, csrfToken);
				}
				httpPost.setEntity(new UrlEncodedFormEntity(
						buildExecuteParams(jobId, jobGroup, execId, userAccess, pixel, pixelParameters),
						StandardCharsets.UTF_8));

				SchedulerResponse schedulerResponse = httpClient.execute(httpPost,
						new HttpClientResponseHandler<SchedulerResponse>() {
							@Override
							public SchedulerResponse handleResponse(ClassicHttpResponse response) throws IOException {
								return new SchedulerResponse(response.getCode(), readEntityAsString(response));
							}
						});
				status = schedulerResponse.statusCode();
				schedulerOutput = schedulerResponse.responseBody();
				success = status >= 200 && status < 300;
			} catch (IOException e) {
				classLogger.error("SCHEDULED JOB: {} could not reach the scheduler endpoint {}",
						Utility.cleanLogString(jobId), executeUrl, e);
			}

			if (success) {
				classLogger.info("SCHEDULED JOB: {} returned response code {}", Utility.cleanLogString(jobId), status);
			} else {
				classLogger.error("SCHEDULED JOB: {} failed with response code {} and output {}",
						Utility.cleanLogString(jobId), status, Utility.cleanLogString(schedulerOutput));
			}

			// store execution time and date in SMSS_AUDIT_TRAIL table
			long end = System.currentTimeMillis();
			SchedulerDatabaseUtility.insertIntoAuditTrailTable(jobId, jobGroup, start, end, success, schedulerOutput);
			classLogger.info("SCHEDULED JOB: {} execution time: {} seconds.", Utility.cleanLogString(jobId),
					(end - start) / 1000);
		} finally {
			// always delete the UUID
			SchedulerDatabaseUtility.removeExecutionId(execId);
		}
	}

	/**
	 * Requests a csrf token so the scheduled execution can pass the csrf filter.
	 * <p>
	 * A missing token is not fatal, the execution is still attempted without one.
	 * </p>
	 *
	 * @param httpClient client holding the cookie store that the token is bound to
	 * @param fetchUrl   fully qualified csrf fetch endpoint
	 * @return the csrf token, or {@code null} when the endpoint did not return one
	 */
	private static String fetchCsrfToken(CloseableHttpClient httpClient, String fetchUrl) {
		HttpGet httpGet = new HttpGet(fetchUrl);
		httpGet.addHeader(CSRF_TOKEN_HEADER, "fetch");
		try {
			return httpClient.execute(httpGet, new HttpClientResponseHandler<String>() {
				@Override
				public String handleResponse(ClassicHttpResponse response) throws IOException {
					Header csrfHeader = response.getFirstHeader(CSRF_TOKEN_HEADER);
					if (csrfHeader == null) {
						classLogger.warn("No csrf token was returned from {}, the request came back with HTTP {}",
								fetchUrl, response.getCode());
						return null;
					}
					return csrfHeader.getValue();
				}
			});
		} catch (IOException e) {
			classLogger.error("Failed to fetch a csrf token from {}", fetchUrl, e);
			return null;
		}
	}

	/**
	 * Builds the url encoded form body for the scheduler execution endpoint.
	 *
	 * @param jobId           id of the job being run
	 * @param jobGroup        group of the job being run
	 * @param execId          one time execution id used to authorize the request
	 * @param userAccess      provider and id pairs for the user running the job
	 * @param pixel           the recipe to run
	 * @param pixelParameters optional recipe parameters prepended to the recipe
	 * @return the form parameters to post
	 */
	private static List<NameValuePair> buildExecuteParams(String jobId, String jobGroup, String execId,
			String userAccess, String pixel, String pixelParameters) {
		List<NameValuePair> paramList = new ArrayList<>();
		paramList.add(new BasicNameValuePair(JobConfigKeys.EXEC_ID, execId));
		paramList.add(new BasicNameValuePair(JobConfigKeys.JOB_ID, jobId));
		paramList.add(new BasicNameValuePair(JobConfigKeys.JOB_GROUP, jobGroup));
		paramList.add(new BasicNameValuePair(JobConfigKeys.USER_ACCESS, userAccess));

		String recipe = pixel;
		if (pixelParameters != null) {
			String params = pixelParameters.trim();
			if (params.endsWith(";")) {
				params = params.substring(0, params.length() - 1);
			}
			// account for just a ";" being sent as the pixel parameter
			if (!params.isEmpty()) {
				recipe = params + " | " + pixel;
			}
		}
		paramList.add(new BasicNameValuePair(JobConfigKeys.PIXEL, recipe));
		return paramList;
	}

	/**
	 * Reads the response payload as a UTF-8 string.
	 *
	 * @param response the response to read
	 * @return the payload, or {@code null} when the response has no entity
	 * @throws IOException if the payload cannot be read or parsed
	 */
	private static String readEntityAsString(ClassicHttpResponse response) throws IOException {
		HttpEntity entity = response.getEntity();
		if (entity == null) {
			return null;
		}
		try {
			return EntityUtils.toString(entity, StandardCharsets.UTF_8);
		} catch (ParseException e) {
			throw new IOException("Failed to parse the scheduler response body", e);
		}
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		classLogger.warn(
				"Received request to interrupt a scheduled pixel job. However, ability to interrupt has not been implemented yet.");
	}

	public static void setFetchCsrf(boolean fetchCsrf) {
		RunPixelJobFromDB.FETCH_CSRF = fetchCsrf;
	}

}
