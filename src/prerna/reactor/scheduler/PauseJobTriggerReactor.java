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
import org.quartz.TriggerKey;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class PauseJobTriggerReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(PauseJobTriggerReactor.class);

	public PauseJobTriggerReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.JOB_ID.getKey(), ReactorKeysEnum.JOB_GROUP.getKey() };
	}

	@Override
	public NounMetadata execute() {
		if (Utility.schedulerForceDisable()) {
			throw new IllegalArgumentException("Scheduler is not enabled");
		}

		/**
		 * PauseJobTrigger(jobName = ["sample_job_name"],
		 * jobGroup=["sample_job_group"]);
		 * 
		 * This reactor will pause the job in Quartz but keep the job stored in the
		 * database. The jobs that are paused can be resumed in the future.
		 */

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

		try {
			String triggerName = jobId.concat("Trigger");
			String triggerGroup = jobGroup.concat("TriggerGroup");
			TriggerKey triggerKey = TriggerKey.triggerKey(triggerName, triggerGroup);
			JobKey jobKey = JobKey.jobKey(jobId, jobGroup);

			Scheduler scheduler = SchedulerFactorySingleton.getInstance().getScheduler();

			// start up scheduler
			SchedulerDatabaseUtility.startScheduler(scheduler);

			if (scheduler.checkExists(jobKey)) {
				scheduler.pauseTrigger(triggerKey);
				return new NounMetadata(false, PixelDataType.BOOLEAN, PixelOperationType.UNSCHEDULE_JOB);
			}
		} catch (SchedulerException se) {
			classLogger.error("Failed to pause job trigger for jobId '{}', jobGroup '{}': {}", jobId, jobGroup,
					se.getMessage(), se);
		}

		return new NounMetadata(false, PixelDataType.BOOLEAN, PixelOperationType.UNSCHEDULE_JOB);
	}
}
