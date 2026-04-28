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
package prerna.rpa.jobrunr.jobs;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jobrunr.jobs.annotations.Job;
import org.jobrunr.jobs.lambdas.JobRequestHandler;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IEngine;
import prerna.engine.impl.InsightAdministrator;
import prerna.om.Insight;
import prerna.om.Pixel;
import prerna.reactor.scheduler.SchedulerDatabaseUtility;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * JobRunr handler for executing Pixel scripts as background jobs.
 * This replaces the Quartz-based PixelJob implementation with a more efficient
 * direct execution approach.
 */
public class JobRunrPixelExecutionJobHandler implements JobRequestHandler<JobRunrPixelExecutionJobRequest> {

	private static final Logger LOGGER = LogManager.getLogger(JobRunrPixelExecutionJobHandler.class);

	/**
	 * Execute the Pixel script job
	 * 
	 * @param jobRequest The job request containing execution details
	 * @throws Exception if execution fails
	 */
	@Override
	@Job(name = "Execute Pixel Script: %0", retries = 3)
	public void run(JobRunrPixelExecutionJobRequest jobRequest) throws Exception {
		LOGGER.info("Starting execution of Pixel job: {} in group: {}", 
			jobRequest.getJobId(), jobRequest.getJobGroup());

		// Generate unique execution ID if not provided
		String execId = jobRequest.getExecId();
		if (execId == null || execId.trim().isEmpty()) {
			execId = UUID.randomUUID().toString();
		}

		final String finalExecId = execId;
		long startTime = System.currentTimeMillis();
		boolean success = false;
		String errorMessage = null;
		String schedulerOutput = null;

		try {
			// Insert execution record
			SchedulerDatabaseUtility.insertIntoExecutionTable(
				finalExecId, 
				jobRequest.getJobId(),
				jobRequest.getJobGroup()
			);

			// Execute Pixel script directly and capture output
			schedulerOutput = executePixelDirectly(
				jobRequest.getPixelScript(), 
				jobRequest.getPixelParameters(),
				jobRequest.getUserAccess(), 
				finalExecId, 
				jobRequest.getJobId(), 
				jobRequest.getJobGroup()
			);

			success = true;
			LOGGER.info("Successfully completed Pixel job: {}", jobRequest.getJobId());

		} catch (Exception e) {
			success = false;
			errorMessage = e.getMessage();
			schedulerOutput = e.getMessage();
			LOGGER.error("Failed to execute Pixel job: {} - Error: {}", 
				jobRequest.getJobId(), e.getMessage(), e);

			// Re-throw to let JobRunr handle retry logic
			throw e;

		} finally {
			long endTime = System.currentTimeMillis();
			
			// Record audit trail
			try {
				SchedulerDatabaseUtility.insertIntoAuditTrailTable(
					jobRequest.getJobId(), 
					jobRequest.getJobGroup(), 
					startTime, 
					endTime, 
					success,
					schedulerOutput != null ? schedulerOutput : 
						(success ? "Execution completed successfully" : "Execution failed: " + errorMessage)
				);
			} catch (Exception auditEx) {
				LOGGER.error("Failed to insert audit trail for job: {}", jobRequest.getJobId(), auditEx);
			}
			
			// ENHANCED METADATA: Record execution success/failure in SMSS_JOB_RECIPES
			// This updates EXECUTION_COUNT, RETRY_COUNT, LAST_EXECUTION_STATUS, etc.
			try {
				if (success) {
					SchedulerDatabaseUtility.recordJobSuccess(
						jobRequest.getJobId(),
						new java.sql.Timestamp(endTime)
					);
					LOGGER.info("Recorded successful execution for job: {}", jobRequest.getJobId());
				} else {
					SchedulerDatabaseUtility.recordJobFailure(
						jobRequest.getJobId(),
						errorMessage,
						new java.sql.Timestamp(endTime)
					);
					LOGGER.info("Recorded failed execution for job: {} (Error: {})", 
						jobRequest.getJobId(), errorMessage);
				}
			} catch (Exception metadataEx) {
				// Don't fail the job if metadata recording fails
				LOGGER.error("Failed to record execution metadata for job: {}", jobRequest.getJobId(), metadataEx);
			}
			
			// Clean up execution ID from database
			try {
				SchedulerDatabaseUtility.removeExecutionId(finalExecId);
				LOGGER.debug("Removed execution ID: {} from database", finalExecId);
			} catch (Exception cleanupEx) {
				LOGGER.warn("Failed to remove execution ID: {}", finalExecId, cleanupEx);
			}

			LOGGER.info("Pixel job execution finished: {} (Duration: {}ms, Success: {})", 
				jobRequest.getJobId(), (endTime - startTime), success);
		}
	}

	/**
	 * Execute Pixel script directly using SEMOSS Insight API
	 * This avoids HTTP overhead from the Quartz implementation
	 * 
	 * @param pixelScript     The Pixel code to execute
	 * @param pixelParameters Parameters for the Pixel script (JSON string)
	 * @param userAccess      User access credentials (format: "provider:userId")
	 * @param execId          Execution ID for tracking
	 * @param jobId           Job ID
	 * @param jobGroup        Job group
	 * @return Execution output/result string
	 * @throws Exception if execution fails
	 */
	private String executePixelDirectly(String pixelScript, String pixelParameters, String userAccess, 
			String execId, String jobId, String jobGroup) throws Exception {
		
		LOGGER.info("Executing Pixel script directly for job: {}", jobId);
		LOGGER.debug("Pixel script: {}", pixelScript);
		LOGGER.debug("Parameters: {}", pixelParameters);

		Insight insight = null;
		String executionOutput = null;
		
		try {
			// Parse user access information
			User user = reconstructUser(userAccess);
			if (user == null) {
				throw new IllegalStateException("Unable to reconstruct user from access info: " + userAccess);
			}

			// Create a new Insight instance for this job execution
			insight = new Insight();
			insight.setUser(user);

			// Set execution context
			Map<String, Object> contextMap = new HashMap<>();
			contextMap.put("EXEC_ID", execId);
			contextMap.put("JOB_ID", jobId);
			contextMap.put("JOB_GROUP", jobGroup);
			
			// Parse and apply pixel parameters if provided
			if (pixelParameters != null && !pixelParameters.trim().isEmpty()) {
				try {
					Map<String, Object> params = parsePixelParameters(pixelParameters);
					contextMap.putAll(params);
				} catch (Exception e) {
					LOGGER.warn("Failed to parse pixel parameters, continuing without them: {}", e.getMessage());
				}
			}

			// Set context variables in insight
			for (Map.Entry<String, Object> entry : contextMap.entrySet()) {
				insight.getVarStore().put(entry.getKey(), new NounMetadata(entry.getValue(), PixelDataType.CONST_STRING));
			}

			// Ensure pixel script ends with semicolon
			if (!pixelScript.trim().endsWith(";")) {
				pixelScript = pixelScript.trim() + ";";
			}

			// Execute the Pixel script
			PixelRunner runner = insight.runPixel(pixelScript);
			
			// Capture execution output from results
			if (runner != null && runner.getResults() != null && !runner.getResults().isEmpty()) {
				NounMetadata lastResult = runner.getResults().get(runner.getResults().size() - 1);
				if (lastResult != null && lastResult.getValue() != null) {
					executionOutput = lastResult.getValue().toString();
					LOGGER.debug("Pixel execution output captured for job: {}", jobId);
				}
			}

			LOGGER.info("Pixel script execution completed successfully for job: {}", jobId);
			return executionOutput;

		} catch (Exception e) {
			LOGGER.error("Error executing Pixel script for job: {}", jobId, e);
			executionOutput = "Pixel execution failed: " + e.getMessage();
			throw new Exception("Pixel execution failed: " + e.getMessage(), e);
			
		} finally {
			// Clean up insight resources
			if (insight != null) {
				try {
					// Close any open engines and resources
					cleanupInsight(insight);
				} catch (Exception cleanupEx) {
					LOGGER.warn("Error during insight cleanup for job: {}", jobId, cleanupEx);
				}
			}
		}
	}

	/**
	 * Reconstruct User object from access string
	 * Format: "provider1:userId1,provider2:userId2"
	 * @param userAccess The user access string
	 * @return Reconstructed User object
	 */
	private User reconstructUser(String userAccess) {
		if (userAccess == null || userAccess.trim().isEmpty()) {
			LOGGER.error("User access information is null or empty");
			return null;
		}

		try {
			User user = new User();
			// Regular user access parsing
			String[] accessPairs = userAccess.split(",");
			
			for (String accessPair : accessPairs) {
				String[] parts = accessPair.split(":");
				if (parts.length != 2) {
					LOGGER.warn("Invalid access pair format: {}", accessPair);
					continue;
				}

				String providerName = parts[0].trim();
				String userId = parts[1].trim();

				try {
					AuthProvider provider = AuthProvider.valueOf(providerName);
					AccessToken token = new AccessToken();
					token.setId(userId);
					token.setProvider(provider);
					
					user.setAccessToken(token);
					
					LOGGER.debug("Added access token for provider: {} with userId: {}", providerName, userId);
					
				} catch (IllegalArgumentException e) {
					LOGGER.warn("Unknown auth provider: {}", providerName);
				}
			}

			if (user.getLogins().isEmpty()) {
				LOGGER.error("No valid login providers found in user access string");
				return null;
			}

			return user;

		} catch (Exception e) {
			LOGGER.error("Failed to reconstruct user from access string: {}", userAccess, e);
			return null;
		}
	}
	
	/**
	 * Parse pixel parameters from JSON string
	 * 
	 * @param pixelParameters JSON string of parameters
	 * @return Map of parameter key-value pairs
	 */
	private Map<String, Object> parsePixelParameters(String pixelParameters) {
		Map<String, Object> params = new HashMap<>();
		
		if (pixelParameters == null || pixelParameters.trim().isEmpty()) {
			return params;
		}

		try {
			// Try to parse as JSON
			com.google.gson.Gson gson = new com.google.gson.Gson();
			@SuppressWarnings("unchecked")
			Map<String, Object> parsedParams = gson.fromJson(pixelParameters, Map.class);
			
			if (parsedParams != null) {
				params.putAll(parsedParams);
			}
			
		} catch (Exception e) {
			LOGGER.warn("Failed to parse pixel parameters as JSON: {}", e.getMessage());
			
			// Fallback: try simple key=value parsing
			try {
				String[] pairs = pixelParameters.split("&");
				for (String pair : pairs) {
					String[] kv = pair.split("=");
					if (kv.length == 2) {
						params.put(kv[0].trim(), kv[1].trim());
					}
				}
			} catch (Exception fallbackEx) {
				LOGGER.warn("Failed to parse pixel parameters with fallback method", fallbackEx);
			}
		}

		return params;
	}

	/**
	 * Clean up Insight resources after execution
	 * 
	 * @param insight The insight instance to clean up
	 */
	private void cleanupInsight(Insight insight) {
		if (insight == null) {
			return;
		}

		try {
			// Clear variable store to release memory
			if (insight.getVarStore() != null) {
				insight.getVarStore().clear();
			}

			LOGGER.debug("Insight cleanup completed");

		} catch (Exception e) {
			LOGGER.warn("Error during insight cleanup", e);
		}
	}
}

