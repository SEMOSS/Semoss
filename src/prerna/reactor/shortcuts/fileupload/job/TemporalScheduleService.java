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

import java.util.Collections;

import io.temporal.client.WorkflowOptions;
import io.temporal.client.schedules.Schedule;
import io.temporal.client.schedules.ScheduleActionStartWorkflow;
import io.temporal.client.schedules.ScheduleClient;
import io.temporal.client.schedules.ScheduleHandle;
import io.temporal.client.schedules.ScheduleOptions;
import io.temporal.client.schedules.ScheduleSpec;
import io.temporal.client.schedules.ScheduleUpdate;
import prerna.reactor.scheduler.SchedulerDatabaseUtility;
import prerna.reactor.shortcuts.temporal.TemporalClientProvider;
import prerna.reactor.shortcuts.temporal.WorkflowEntity;

public class TemporalScheduleService {

	public void upsertScheduleForWorkflow(String workflowKey) throws Exception {
		WorkflowEntity workflow = SchedulerDatabaseUtility.getWorkflowByKey(workflowKey);
		if (workflow == null) {
			throw new IllegalArgumentException("Workflow not found: " + workflowKey);
		}

		if (!"SCHEDULE".equalsIgnoreCase(workflow.triggerType)) {
			throw new IllegalArgumentException("Workflow triggerType must be SCHEDULE");
		}

		if (workflow.cronExpression == null || workflow.cronExpression.isBlank()) {
			throw new IllegalArgumentException("cronExpression is required for schedule workflow");
		}

		ScheduleClient scheduleClient = TemporalClientProvider.getScheduleClient();

		String scheduleId = workflow.temporalScheduleId;
		if (scheduleId == null || scheduleId.isBlank()) {
			scheduleId = "SCH_" + workflow.workflowKey;
		}

		Schedule schedule = Schedule.newBuilder()
				.setSpec(ScheduleSpec.newBuilder()
						.setCronExpressions(Collections.singletonList(workflow.cronExpression)).build())
				.setAction(ScheduleActionStartWorkflow.newBuilder().setWorkflowType(WorkflowEngine.class)
						.setOptions(WorkflowOptions.newBuilder().setTaskQueue("QUEUE").build())
						.setArguments(workflow.workflowKey, "SCHEDULE", "TEMPORAL_SCHEDULE").build())
				.build();

		try {
			ScheduleHandle handle = scheduleClient.getHandle(scheduleId);

			// describe() helps confirm whether schedule exists
			handle.describe();

			handle.update(input -> new ScheduleUpdate(schedule));

			System.out.println("Updated Temporal schedule: " + scheduleId);

		} catch (Exception ex) {
			scheduleClient.createSchedule(scheduleId, schedule, ScheduleOptions.newBuilder().build());
			System.out.println("Created Temporal schedule: " + scheduleId);
		}

		SchedulerDatabaseUtility.updateWorkflowScheduleId(workflow.workflowKey, scheduleId);
	}

	public void pauseSchedule(String workflowKey) throws Exception {
		WorkflowEntity workflow = SchedulerDatabaseUtility.getWorkflowByKey(workflowKey);

		if (workflow == null) {
			throw new IllegalArgumentException("Workflow not found: " + workflowKey);
		}

		if (workflow.temporalScheduleId == null || workflow.temporalScheduleId.isBlank()) {
			throw new IllegalArgumentException("Temporal schedule id not found for workflow: " + workflowKey);
		}

		ScheduleClient scheduleClient = TemporalClientProvider.getScheduleClient();
		ScheduleHandle handle = scheduleClient.getHandle(workflow.temporalScheduleId);

		handle.pause();
		System.out.println("Paused schedule: " + workflow.temporalScheduleId);
	}

	public void resumeSchedule(String workflowKey) throws Exception {
		WorkflowEntity workflow = SchedulerDatabaseUtility.getWorkflowByKey(workflowKey);

		if (workflow == null) {
			throw new IllegalArgumentException("Workflow not found: " + workflowKey);
		}

		if (workflow.temporalScheduleId == null || workflow.temporalScheduleId.isBlank()) {
			throw new IllegalArgumentException("Temporal schedule id not found for workflow: " + workflowKey);
		}

		ScheduleClient scheduleClient = TemporalClientProvider.getScheduleClient();
		ScheduleHandle handle = scheduleClient.getHandle(workflow.temporalScheduleId);

		handle.unpause();
		System.out.println("Resumed schedule: " + workflow.temporalScheduleId);
	}

	public void triggerNow(String workflowKey) throws Exception {
		WorkflowEntity workflow = SchedulerDatabaseUtility.getWorkflowByKey(workflowKey);

		if (workflow == null) {
			throw new IllegalArgumentException("Workflow not found: " + workflowKey);
		}

		if (workflow.temporalScheduleId == null || workflow.temporalScheduleId.isBlank()) {
			throw new IllegalArgumentException("Temporal schedule id not found for workflow: " + workflowKey);
		}

		ScheduleClient scheduleClient = TemporalClientProvider.getScheduleClient();
		ScheduleHandle handle = scheduleClient.getHandle(workflow.temporalScheduleId);

		handle.trigger();
		System.out.println("Triggered schedule immediately: " + workflow.temporalScheduleId);
	}

	public void deleteSchedule(String workflowKey) throws Exception {
		WorkflowEntity workflow = SchedulerDatabaseUtility.getWorkflowByKey(workflowKey);

		if (workflow == null) {
			throw new IllegalArgumentException("Workflow not found: " + workflowKey);
		}

		if (workflow.temporalScheduleId == null || workflow.temporalScheduleId.isBlank()) {
			System.out.println("No Temporal schedule id found, nothing to delete for workflow: " + workflowKey);
			return;
		}

		ScheduleClient scheduleClient = TemporalClientProvider.getScheduleClient();
		ScheduleHandle handle = scheduleClient.getHandle(workflow.temporalScheduleId);

		handle.delete();
		System.out.println("Deleted schedule: " + workflow.temporalScheduleId);

		workflow.temporalScheduleId = null;
		SchedulerDatabaseUtility.updateWorkflow(workflow);
	}
}
