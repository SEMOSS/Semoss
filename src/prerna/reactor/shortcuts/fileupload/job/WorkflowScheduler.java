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
package prerna.reactor.shortcuts.fileupload.job;

import java.io.File;

import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

public class WorkflowScheduler {
	static Scheduler scheduler;

	static {
		try {
			scheduler = StdSchedulerFactory.getDefaultScheduler();
			scheduler.start();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	static void runNow(WorkflowDefinition wf, File file, String execId) throws Exception {

		JobDataMap map = new JobDataMap();
		map.put("workflow", wf);
		map.put("file", file);

		JobDetail job = JobBuilder.newJob(WorkflowQuartzJob.class).withIdentity("job-" + execId).usingJobData(map)
				.build();

		Trigger trigger = TriggerBuilder.newTrigger().withIdentity("trigger-" + execId).startNow().build();

		scheduler.scheduleJob(job, trigger);
	}

	static void schedule(WorkflowDefinition wf, File file, String cron, String execId) throws Exception {

		JobDataMap map = new JobDataMap();
		map.put("workflow", wf);
		map.put("file", file);

		JobDetail job = JobBuilder.newJob(WorkflowQuartzJob.class).withIdentity("job-" + execId).usingJobData(map)
				.build();

		Trigger trigger = TriggerBuilder.newTrigger().withIdentity("trigger-" + execId)
				.withSchedule(CronScheduleBuilder.cronSchedule(cron)).build();

		scheduler.scheduleJob(job, trigger);
	}

	static void pause(String execId) throws Exception {
		scheduler.pauseJob(JobKey.jobKey("job-" + execId));
	}

	static void resume(String execId) throws Exception {
		scheduler.resumeJob(JobKey.jobKey("job-" + execId));
	}

	static void cancel(String workflowId, String executionId) throws Exception {

		JobKey jobKey = JobKey.jobKey("job-" + workflowId + "-" + executionId, "workflow-jobs");

		scheduler.interrupt(jobKey);
	}
}
