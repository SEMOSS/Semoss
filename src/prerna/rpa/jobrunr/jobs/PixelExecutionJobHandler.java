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
import prerna.reactor.scheduler.SchedulerDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * JobRunr handler for executing Pixel scripts as background jobs.
 * This replaces the Quartz-based PixelJob implementation with a more efficient
 * direct execution approach.
 */
public class PixelExecutionJobHandler implements JobRequestHandler<PixelExecutionJobRequest> {

	private static final Logger LOGGER = LogManager.getLogger(PixelExecutionJobHandler.class);

	/**
	 * Execute the Pixel script job
	 * 
	 * @param jobRequest The job request containing execution details
	 * @throws Exception if execution fails
	 */
	@Override
	@Job(name = "Execute Pixel Script: %0", retries = 3)
	public void run(PixelExecutionJobRequest jobRequest) throws Exception {
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

		try {
			// Insert execution record
			SchedulerDatabaseUtility.insertIntoExecutionTable(
				finalExecId, 
				jobRequest.getJobId(),
				jobRequest.getJobGroup()
			);

			// Execute Pixel script directly
			executePixelDirectly(
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
					success ? "Execution completed successfully" : "Execution failed: " + errorMessage
				);
			} catch (Exception auditEx) {
				LOGGER.error("Failed to insert audit trail for job: {}", jobRequest.getJobId(), auditEx);
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
	 * @throws Exception if execution fails
	 */
	private void executePixelDirectly(String pixelScript, String pixelParameters, String userAccess, 
			String execId, String jobId, String jobGroup) throws Exception {
		
		LOGGER.info("Executing Pixel script directly for job: {}", jobId);
		LOGGER.debug("Pixel script: {}", pixelScript);
		LOGGER.debug("Parameters: {}", pixelParameters);

		Insight insight = null;
		
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

			LOGGER.info("Executing Pixel script for job: {} with user: {}", jobId, user.getPrimaryLogin());

			// Execute the Pixel script
			insight.runPixel(pixelScript);

			LOGGER.info("Pixel script execution completed successfully for job: {}", jobId);

		} catch (Exception e) {
			LOGGER.error("Error executing Pixel script for job: {}", jobId, e);
			throw new Exception("Pixel execution failed: " + e.getMessage(), e);
			
		} finally {
			// Clean up insight resources
			if (insight != null) {
				try {
					// Close any open engines or resources
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
	 * 
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
			// Close any open engines
//			Map<String, IEngine> engineMap = insight.);
//			if (engineMap != null && !engineMap.isEmpty()) {
//				for (IEngine engine : engineMap.values()) {
//					try {
//						if (engine != null) {
//							engine.close();
//						}
//					} catch (Exception e) {
//						LOGGER.warn("Error closing engine: {}", e.getMessage());
//					}
//				}
//			}

			// Clear variable store
			if (insight.getVarStore() != null) {
				insight.getVarStore().clear();
			}

			LOGGER.debug("Insight cleanup completed");

		} catch (Exception e) {
			LOGGER.warn("Error during insight cleanup", e);
		}
	}
}

