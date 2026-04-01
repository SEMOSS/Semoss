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
package prerna.reactor.shortcuts.temporal;

import prerna.reactor.scheduler.SchedulerDatabaseUtility;
import prerna.reactor.shortcuts.fileupload.job.TemporalScheduleService;

public class WorkflowService {

	private final TemporalScheduleService temporalScheduleService = new TemporalScheduleService();

	public void insertWorkflowWorkflowTemplate(WorkflowTemplate e) throws Exception {
		SchedulerDatabaseUtility.insertWorkflowTemplate(e);
	}

	public void updateTemplate(WorkflowTemplate e) throws Exception {
		SchedulerDatabaseUtility.updateWorkflowTemplate(e);
	}

	public WorkflowTemplate getTemplate(String key) throws Exception {
		return SchedulerDatabaseUtility.getWorkflowTemplateByKey(key);
	}

	public void deleteTemplate(String key) throws Exception {
		SchedulerDatabaseUtility.deleteWorkflowTemplateByKey(key);
	}

	public void createWorkflow(WorkflowEntity e) throws Exception {
		SchedulerDatabaseUtility.insertWorkflow(e);
		if ("SCHEDULE".equals(e.triggerType) && "ACTIVE".equals(e.status)) {
			temporalScheduleService.upsertScheduleForWorkflow(e.workflowKey);
		}
	}

	public void updateWorkflow(WorkflowEntity e) throws Exception {
		SchedulerDatabaseUtility.updateWorkflow(e);
		if ("SCHEDULE".equals(e.triggerType) && "ACTIVE".equals(e.status)) {
			temporalScheduleService.upsertScheduleForWorkflow(e.workflowKey);
		}
	}

	public WorkflowEntity getWorkflow(String workflowKey) throws Exception {
		return SchedulerDatabaseUtility.getWorkflowByKey(workflowKey);
	}

	public void deleteWorkflow(String workflowKey) throws Exception {
		WorkflowEntity existing = SchedulerDatabaseUtility.getWorkflowByKey(workflowKey);
		if (existing != null && existing.temporalScheduleId != null) {
			temporalScheduleService.deleteSchedule(workflowKey);
		}
		SchedulerDatabaseUtility.deleteWorkflowByKey(workflowKey);
	}

	public void createWorkflowFromTemplate(String workflowTemplateKey, WorkflowEntity workflow) throws Exception {
		WorkflowTemplate template = SchedulerDatabaseUtility.getWorkflowTemplateByKey(workflowTemplateKey);
		if (template == null) {
			throw new IllegalArgumentException("Template not found: " + workflowTemplateKey);
		}

		workflow.workflowTemplateKey = template.workflowTemplateKey;
		workflow.workflowJson = template.workflowJson;
		SchedulerDatabaseUtility.insertWorkflow(workflow);

		if ("SCHEDULE".equals(workflow.triggerType) && "ACTIVE".equals(workflow.status)) {
			temporalScheduleService.upsertScheduleForWorkflow(workflow.workflowKey);
		}
	}

	public void pauseWorkflowSchedule(String workflowKey) throws Exception {
		temporalScheduleService.pauseSchedule(workflowKey);
	}

	public void resumeWorkflowSchedule(String workflowKey) throws Exception {
		temporalScheduleService.resumeSchedule(workflowKey);
	}

	public void triggerWorkflowScheduleNow(String workflowKey) throws Exception {
		temporalScheduleService.triggerNow(workflowKey);
	}
}
