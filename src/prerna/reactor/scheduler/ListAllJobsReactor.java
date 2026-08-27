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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ListAllJobsReactor extends AbstractReactor {

	// inputs
	private static final String MY_JOBS = "myJobs";

	public ListAllJobsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.USERNAME.getKey(), MY_JOBS,
				ReactorKeysEnum.JOB_TAGS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		if (Utility.schedulerForceDisable()) {
			throw new IllegalArgumentException("Scheduler is not enabled");
		}

		/**
		 * 4 POSSIBLE CASES ListAllJobs(); ListAllJobs(app=["sample_app_id"]);
		 * ListAllJobs(username=["sample_username"]); ListAllJobs(app=["sample_app_id"],
		 * username=["sample_username"]);
		 * 
		 * This reactor will return all jobs based on app and user, if no parameters are
		 * passed it will check if user has admin permissions, if so it will return all
		 * jobs, if not it will throw error.
		 */

		Map<String, Map<String, String>> jobMap = null;
		organizeKeys();

		String projectId = this.keyValue.get(this.keysToGet[0]);
		String userId = this.keyValue.get(this.keysToGet[1]);
		List<String> jobTags = getJobTags();
		User user = this.insight.getUser();
		if (projectId != null && !SecurityAdminUtils.userIsAdmin(user)
				&& !SecurityProjectUtils.userCanViewProject(user, projectId)) {
			throw new IllegalArgumentException(
					"Project does not exist or user does not have permission to view its scheduled jobs");
		}

		if (projectId == null && userId == null) {
			// TODO: check if admin if not admin throw permissions error
			// security utils. isAdmin() to check if user is admin *******
			// return all jobs
			jobMap = SchedulerDatabaseUtility.retrieveAllJobs(jobTags);
		} else if (projectId != null && userId == null) {
			jobMap = SchedulerDatabaseUtility.retrieveJobsForProject(projectId, jobTags);
		} else if (projectId == null) {
			jobMap = SchedulerDatabaseUtility.retrieveUsersJobs(userId, jobTags);
		} else {
			jobMap = SchedulerDatabaseUtility.retrieveUsersJobsForProject(projectId, userId, jobTags);
		}

		return new NounMetadata(jobMap, PixelDataType.MAP, PixelOperationType.LIST_JOB);
	}

	private List<String> getJobTags() {
		List<String> jobTags = null;
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.JOB_TAGS.getKey());
		if (grs != null && !grs.isEmpty()) {
			jobTags = new ArrayList<>();
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				jobTags.add(grs.get(i) + "");
			}
		}
		return jobTags;
	}
}
