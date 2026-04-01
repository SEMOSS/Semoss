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
package prerna.reactor.scheduler;

import org.jobrunr.jobs.JobId;

import prerna.rpa.jobrunr.jobs.PixelExecutionJobRequest;
import prerna.util.Utility;
import prerna.util.jobrunr.JobRunrService;

/**
 * Quartz to JobRunr Migration Utility for Scheduler Reactors
 * 
 * <p>
 * This utility class provides common methods for scheduler reactors to support
 * both Quartz and JobRunr via feature flag.
 * </p>
 * 
 * <p>
 * <strong>SEMOSS is NOT a Spring Boot application</strong> - this utility uses
 * DIHelper for configuration management.
 * </p>
 * 
 * <h3>Usage Pattern:</h3>
 * 
 * <pre>
 * {@code
 * public class ScheduleJobReactor extends AbstractReactor {
 *     
 *     &#64;Override
 *     public NounMetadata execute() {
 *         organizeKeys();
 *         
 *         // Check feature flag
 *         if (SchedulerMigrationUtil.isJobRunrEnabled()) {
 *             return scheduleWithJobRunr(...);
 *         } else {
 *             return scheduleWithQuartz(...);  // Existing code
 *         }
 *     }
 * }
 * }
 * </pre>
 * 
 * @author SEMOSS Development Team
 * @version 1.0
 */
public final class SchedulerMigrationUtil {

	/**
	 * Feature flag property name for enabling JobRunr
	 */
	private static final String JOBRUNR_ENABLED_FLAG = "scheduler.use-jobrunr";

	/**
	 * Default value for feature flag (disabled by default for safety)
	 */
	private static final String DEFAULT_ENABLED_VALUE = "false";

	/**
	 * Private constructor - utility class with only static methods
	 */
	private SchedulerMigrationUtil() {
		// Prevent instantiation
	}

	/**
	 * Check if JobRunr is enabled via feature flag.
	 * 
	 * <p>
	 * Reads the property from DIHelper (SEMOSS configuration).
	 * </p>
	 * 
	 * @return true if JobRunr is enabled, false otherwise
	 */
	public static boolean isJobRunrEnabled() {
		try {
			String flag = Utility.getDIHelperProperty(JOBRUNR_ENABLED_FLAG);
			if (flag == null) {
				flag = DEFAULT_ENABLED_VALUE; // Default to false for safety
			}
			return Boolean.parseBoolean(flag);
		} catch (Exception e) {
			// Log warning but don't fail
			System.out.println("Warning: Could not read " + JOBRUNR_ENABLED_FLAG + ", defaulting to false");
			return false;
		}
	}

	/**
	 * Get JobRunr service instance for scheduling jobs.
	 * 
	 * <p>
	 * Returns the singleton JobRunrService instance.
	 * </p>
	 * 
	 * @return JobRunrService instance
	 * @throws IllegalStateException if JobRunr is not enabled
	 */
	public static JobRunrService getJobRunrService() {
		if (!isJobRunrEnabled()) {
			throw new IllegalStateException("JobRunr is disabled. Set " + JOBRUNR_ENABLED_FLAG + "=true to enable.");
		}

		try {
			return JobRunrService.getInstance();
		} catch (Exception e) {
			throw new RuntimeException("Failed to initialize JobRunrService", e);
		}
	}

	/**
	 * Schedule a recurring job with JobRunr.
	 * 
	 * <p>
	 * Creates a PixelExecutionJobRequest and schedules it as a recurring job using
	 * JobRunr's Cron scheduling.
	 * </p>
	 * 
	 * @param jobId           Unique job identifier
	 * @param jobGroup        Job group/namespace
	 * @param cronExpression  Cron expression for scheduling (e.g., "0 0 9 * * ?")
	 * @param pixelScript     The Pixel script to execute
	 * @param pixelParameters JSON parameters for the Pixel script
	 * @param userAccess      Encrypted user access token
	 * @param execId          Execution ID for tracking
	 * @return JobId if successful, null if failed
	 */
	public static String scheduleRecurringJobWithJobRunr(String jobId, String jobGroup, String cronExpression,
			String pixelScript, String pixelParameters, String userAccess, String execId) {

		try {
			JobRunrService service = getJobRunrService();

			// Create job request
			PixelExecutionJobRequest jobRequest = new PixelExecutionJobRequest(pixelScript, pixelParameters, userAccess,
					execId, jobId, jobGroup);

			// Schedule as recurring job with Cron expression
			return service.scheduleRecurring(jobId, cronExpression, jobRequest);

		} catch (Exception e) {
			System.err.println("Failed to schedule job with JobRunr: " + e.getMessage());
			return null;
		}
	}

	/**
	 * Pause a recurring job in JobRunr.
	 * 
	 * @param jobId Job identifier to pause
	 * @return true if paused successfully, false otherwise
	 */
	public static boolean pauseJobWithJobRunr(String jobId) {
		try {
			JobRunrService service = getJobRunrService();
			service.pauseRecurringJob(jobId);
			return true;
		} catch (Exception e) {
			System.err.println("Failed to pause job with JobRunr: " + e.getMessage());
			return false;
		}
	}

	/**
	 * Resume a paused job in JobRunr.
	 * 
	 * @param jobId Job identifier to resume
	 * @return true if resumed successfully, false otherwise
	 */
//	public static boolean resumeJobWithJobRunr(String jobId) {
//		try {
//			JobRunrService service = getJobRunrService();
//			service.resumeRecurringJob(jobId);
//			return true;
//		} catch (Exception e) {
//			System.err.println("Failed to resume job with JobRunr: " + e.getMessage());
//			return false;
//		}
//	}

	/**
	 * Delete a job from JobRunr.
	 * 
	 * @param jobId Job identifier to delete
	 * @return true if deleted successfully, false otherwise
	 */
	public static boolean deleteJobWithJobRunr(String jobId) {
		try {
			JobRunrService service = getJobRunrService();
			service.deleteRecurringJob(jobId);
			return true;
		} catch (Exception e) {
			System.err.println("Failed to delete job with JobRunr: " + e.getMessage());
			return false;
		}
	}

	/**
	 * Execute a job immediately with JobRunr.
	 * 
	 * @param pixelScript     The Pixel script to execute
	 * @param pixelParameters JSON parameters for the Pixel script
	 * @param userAccess      Encrypted user access token
	 * @param jobId           Job identifier
	 * @param jobGroup        Job group
	 * @return JobId if enqueued successfully, null otherwise
	 */
	public static JobId executeImmediateJobWithJobRunr(String pixelScript, String pixelParameters, String userAccess,
			String jobId, String jobGroup) {

		try {
			JobRunrService service = getJobRunrService();

			PixelExecutionJobRequest jobRequest = new PixelExecutionJobRequest(pixelScript, pixelParameters, userAccess,
					java.util.UUID.randomUUID().toString(), jobId, jobGroup);

			return service.enqueue(jobRequest);

		} catch (Exception e) {
			System.err.println("Failed to execute immediate job with JobRunr: " + e.getMessage());
			return null;
		}
	}

	/**
	 * Edit/update a recurring job in JobRunr.
	 * 
	 * <p>
	 * This deletes the old job and creates a new one with updated parameters.
	 * </p>
	 * 
	 * @param jobId             Job identifier (will be reused)
	 * @param jobGroup          Job group
	 * @param newCronExpression New Cron expression
	 * @param newPixelScript    New Pixel script
	 * @param newParameters     New parameters
	 * @param userAccess        User access token
	 * @return true if updated successfully, false otherwise
	 */
	public static boolean updateJobWithJobRunr(String jobId, String jobGroup, String newCronExpression,
			String newPixelScript, String newParameters, String userAccess) {

		try {
			// Delete old job
			deleteJobWithJobRunr(jobId);

			// Small delay to ensure deletion completes
			Thread.sleep(100);

			// Create new job with same ID
			String execId = java.util.UUID.randomUUID().toString();
			scheduleRecurringJobWithJobRunr(jobId, jobGroup, newCronExpression, newPixelScript, newParameters,
					userAccess, execId);

			return true;

		} catch (Exception e) {
			System.err.println("Failed to update job with JobRunr: " + e.getMessage());
			return false;
		}
	}
}
