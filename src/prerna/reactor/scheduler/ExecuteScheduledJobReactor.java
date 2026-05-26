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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.jobrunr.JobRunrService;

public class ExecuteScheduledJobReactor extends AbstractReactor {

	private static final Logger logger = LogManager.getLogger(ExecuteScheduledJobReactor.class);

	public ExecuteScheduledJobReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.JOB_ID.getKey(), ReactorKeysEnum.JOB_GROUP.getKey() };
	}

	@Override
	public NounMetadata execute() {
		if (Utility.schedulerForceDisable()) {
			throw new IllegalArgumentException("Scheduler is not enabled");
		}

		organizeKeys();

		// Get inputs
		String jobId = this.keyValue.get(this.keysToGet[0]);
		String jobGroup = this.keyValue.get(this.keysToGet[1]);

		// the job group is the app the user is in
		// user must be an admin or editor of the app
		// to add a scheduled job
		User user = this.insight.getUser();
		if (!SecurityAdminUtils.userIsAdmin(user) && !SecurityProjectUtils.userCanEditProject(user, jobGroup)) {
			throw new IllegalArgumentException("User does not have proper permissions to schedule jobs");
		}

		// Check if JobRunr is enabled
		boolean isJobRunrJob = JobRunrService.isJobRunrEnabled();

		if (isJobRunrJob) {
			return executeWithJobRunr(jobId, jobGroup);
		} else {
			return executeWithQuartz(jobId, jobGroup);
		}
	}

	/**
	 * Execute job immediately using JobRunr
	 */
	private NounMetadata executeWithJobRunr(String jobId, String jobGroup) {
		try {
			JobRunrService jobRunrService = JobRunrService.getJobRunrService();

			// Trigger the job to run immediately
			jobRunrService.triggerRecurringJobNow(jobId);

			logger.info("Triggered JobRunr job: {} to execute immediately", jobId);

			return new NounMetadata(true, PixelDataType.BOOLEAN);
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to execute job with JobRunr: " + e.getMessage(), e);
		}
	}

	/**
	 * Execute job immediately using Quartz (existing implementation)
	 */
	private NounMetadata executeWithQuartz(String jobId, String jobGroup) {
		JobKey jobKey = JobKey.jobKey(jobId, jobGroup);
		Scheduler scheduler = SchedulerFactorySingleton.getInstance().getScheduler();
		try {
			if (scheduler.checkExists(jobKey)) {
				scheduler.triggerJob(jobKey);
			} else {
				throw new IllegalArgumentException(
						"Could not find job with name = " + jobId + " and group = " + jobGroup);
			}
		} catch (SchedulerException se) {
			logger.error("Failed to trigger Quartz job for jobId '{}', jobGroup '{}': {}", jobId, jobGroup,
					se.getMessage(), se);
		}

		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

}
