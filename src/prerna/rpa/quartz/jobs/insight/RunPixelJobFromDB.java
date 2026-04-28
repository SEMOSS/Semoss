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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.hc.client5.http.ClientProtocolException;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.cookie.BasicCookieStore;
import org.apache.hc.client5.http.cookie.CookieStore;
import org.apache.hc.client5.http.entity.UrlEncodedFormEntity;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.ParseException;
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
import prerna.rpa.RPAProps;
import prerna.rpa.config.JobConfigKeys;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class RunPixelJobFromDB implements InterruptableJob {

	private static final Logger classLogger = LogManager.getLogger(RunPixelJobFromDB.class);

	public static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	private static boolean FETCH_CSRF = false;
	
	private String jobId;
	private String jobGroup;
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		jobId = context.getJobDetail().getKey().getName();
		jobGroup = context.getJobDetail().getKey().getGroup();
		
		JobDataMap dataMap = context.getMergedJobDataMap();
		String pixel = dataMap.getString(JobConfigKeys.PIXEL);
		String pixelParameters = dataMap.getString(JobConfigKeys.PIXEL_PARAMETERS);
		String userAccess = RPAProps.getInstance().decrypt(dataMap.getString(JobConfigKeys.USER_ACCESS));

		String execId = UUID.randomUUID().toString();
		// Set execution guard (IS_RUNNING = true)
		try {
			boolean marked = SchedulerDatabaseUtility.markJobAsRunning(jobId);
			if (!marked) {
				classLogger.warn("Failed to set execution guard for Quartz job {} - job may already be running", jobId);
			} else {
				classLogger.debug("Set execution guard for Quartz job: {}", jobId);
			}
		} catch (Exception guardEx) {
			classLogger.error("Failed to update IS_RUNNING flag for Quartz job: {}", jobId, guardEx);
		}
		
		// insert the exec id so we allow the execution
		SchedulerDatabaseUtility.insertIntoExecutionTable(execId, jobId, jobGroup);
		
		// add the scheduler cert if required
		String keyStore = Utility.getDIHelperProperty(Constants.SCHEDULER_KEYSTORE);
		String keyStorePass = Utility.getDIHelperProperty(Constants.SCHEDULER_KEYSTORE_PASSWORD);
		String keyPass = Utility.getDIHelperProperty(Constants.SCHEDULER_CERTIFICATE_PASSWORD);
		
		try {
			// run the pixel endpoint
			boolean success = false;
			String url = Utility.getDIHelperProperty(Constants.SCHEDULER_ENDPOINT);
			if(url == null) {
				throw new IllegalArgumentException("Must define the scheduler endpoint to run scheduled jobs");
			}
			url = url.trim();
			
			String csrfToken = null;
			CookieStore httpCookieStore = new BasicCookieStore();
			CloseableHttpClient httpclient = HttpHelperUtility.getCustomClient(httpCookieStore, keyStore, keyStorePass, keyPass);
			if(FETCH_CSRF){
				String fetchUrl = url;
				if(fetchUrl.endsWith("/")) {
					fetchUrl += "api/config/fetchCsrf";
				} else {
					fetchUrl += "/api/config/fetchCsrf";
				}
				HttpGet httpget = new HttpGet(url);
				httpget.addHeader("Content-Type","application/x-www-form-urlencoded; charset=utf-8");
				httpget.addHeader("X-CSRF-Token","fetch");
				CloseableHttpResponse response = null;
				try {
					response = httpclient.execute(httpget);
					Header[] allheaders = response.getHeaders();
					for(Header h : allheaders) {
						if(h.getName().equals("X-CSRF-Token")) {
							csrfToken = h.getValue();
							break;
						}
					}
				} catch (ClientProtocolException e) {
					classLogger.error(Constants.STACKTRACE, e);
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				} finally {
					if(response != null) {
						try {
							response.close();
						} catch (IOException e) {
							classLogger.error(Constants.STACKTRACE, e);
						}
					}
				}
			}
			
			if(url.endsWith("/")) {
				url += "api/schedule/executePixel";
			} else {
				url += "/api/schedule/executePixel";
			}
			
			// use the same cookie store from above if values are set
			HttpPost httppost = new HttpPost(url);
			httppost.addHeader("Content-Type","application/x-www-form-urlencoded; charset=utf-8");
			if(csrfToken != null) {
				httppost.addHeader("X-CSRF-Token",csrfToken);
			}
			
			// add the body
			List<NameValuePair> paramList = new ArrayList<NameValuePair>();
			paramList.add(new BasicNameValuePair(JobConfigKeys.EXEC_ID, execId));
			paramList.add(new BasicNameValuePair(JobConfigKeys.JOB_ID, jobId));
			paramList.add(new BasicNameValuePair(JobConfigKeys.JOB_GROUP, jobGroup));
			paramList.add(new BasicNameValuePair(JobConfigKeys.USER_ACCESS, userAccess));
			boolean hasParam = false;
			if(pixelParameters != null && !(pixelParameters = pixelParameters.trim()).isEmpty()) {
				if(pixelParameters.endsWith(";")) {
					pixelParameters = pixelParameters.substring(0, pixelParameters.length()-1);
				}
				// account for just a ";" being sent as the pixel parameter
				if(!pixelParameters.isEmpty()) {
					hasParam = true;
					paramList.add(new BasicNameValuePair(JobConfigKeys.PIXEL, pixelParameters + " | " + pixel));
				}
			}
			if(!hasParam) {
				paramList.add(new BasicNameValuePair(JobConfigKeys.PIXEL, pixel));
			}
			
			long start = System.currentTimeMillis();
			
			int status = -1;
			CloseableHttpResponse response = null;
			HttpEntity entity = null;
			String schedulerOutput = null;
			try {
				httppost.setEntity(new UrlEncodedFormEntity(paramList));
				response = httpclient.execute(httppost);
				status = response.getCode();
				if (status == 200 ) {
					success = true;
				}
				
				entity = response.getEntity();
				schedulerOutput = EntityUtils.toString(entity);
			} catch (ClientProtocolException e) {
				classLogger.error(Constants.STACKTRACE, e);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			} catch (ParseException e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				// consume will release the entity
				if(entity != null) {
					try {
						EntityUtils.consume(entity);
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
				if(response != null) {
					try {
						response.close();
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
			
			classLogger.info("##SCHEDULED JOB: Response Code " + status);
//			try {
//				logger.info("##SCHEDULED JOB: Json return = " + EntityUtils.toString(response.getEntity()));
//			} catch (ParseException e) {
//				logger.error(Constants.STACKTRACE, e);
//			} catch (IOException e) {
//				logger.error(Constants.STACKTRACE, e);
//			}
			
			// store execution time and date in SMSS_AUDIT_TRAIL table
			long end = System.currentTimeMillis();
			SchedulerDatabaseUtility.insertIntoAuditTrailTable(jobId, jobGroup, start, end, success, schedulerOutput);
			classLogger.info("##SCHEDULED JOB: Execution time: " + (end - start) / 1000 + " seconds.");
			
			// Record execution success/failure in SMSS_JOB_RECIPES
			try {
				if (success) {
					SchedulerDatabaseUtility.recordJobSuccess(jobId, new java.sql.Timestamp(end));
					classLogger.info("Recorded successful execution for Quartz job: {}", jobId);
				} else {
					String errorMsg = "HTTP Status: " + status + (schedulerOutput != null ? " - " + schedulerOutput : "");
					SchedulerDatabaseUtility.recordJobFailure(jobId, errorMsg, new java.sql.Timestamp(end));
					classLogger.info("Recorded failed execution for Quartz job: {}", jobId);
				}
			} catch (Exception metadataEx) {
				classLogger.error("Failed to record execution metadata for Quartz job: {}", jobId, metadataEx);
			}
		} finally {
			// always delete the UUID
			SchedulerDatabaseUtility.removeExecutionId(execId);
		}
		
//		// Execute job
//		Insight insight = new Insight();
//
//		// Add user info to the insight
//		User user = new User();
//		String[] accessPairs = userAccess.split(",");
//
//		for (String accessPair : accessPairs) {
//			String[] providerAndId = accessPair.split(":");
//
//			// Get the auth provider
//			AuthProvider provider = AuthProvider.valueOf(providerAndId[0]);
//
//			// Get the id
//			String id = providerAndId[1];
//
//			// Create the access token
//			AccessToken token = new AccessToken();
//			token.setProvider(provider);
//			token.setId(id);
//
//			user.setAccessToken(token);
//		}
//		insight.setUser(user);
//
//		String insightId = InsightStore.getInstance().put(insight);
//		if (!pixel.endsWith(";")) {
//			pixel = pixel + ";";
//		}
//
//		// make a random session id
//		ThreadStore.setInsightId(insightId);
//		ThreadStore.setSessionId("scheduledJob_" + UUID.randomUUID().toString());
//		ThreadStore.setJobId(insightId);
//		ThreadStore.setUser(user);
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		classLogger.warn("Received request to interrupt the " + jobId + " job. However, there is nothing to interrupt for this job.");
	}
	
	public static void setFetchCsrf(boolean fetchCsrf) {
		RunPixelJobFromDB.FETCH_CSRF = fetchCsrf;
	}

}
