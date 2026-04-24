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
package prerna.reactor.shortcuts.conductor.oss.workers;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.client.worker.Worker;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskResult;

import prerna.om.Insight;
import prerna.reactor.scheduler.SchedulerDatabaseUtility;
import prerna.reactor.shortcuts.conductor.oss.WorkflowDefinition;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class PixelWorker implements Worker {

	private final String taskName;

	private Insight insight;

	private WorkflowDefinition workflowDefinition;

	ObjectMapper mapper = new ObjectMapper();

	public PixelWorker(String taskName) {
		this.taskName = taskName;

	}

	public PixelWorker(String taskName, Insight insight, WorkflowDefinition workflowDefinition) {
		this.taskName = taskName;
		this.insight = insight;
		this.workflowDefinition = workflowDefinition;
	}

	@Override
	public String getTaskDefName() {
		return taskName;
	}

	@Override
	public TaskResult execute(Task task) {

		TaskResult result = new TaskResult(task);
		String workflowInstanceId = task.getWorkflowInstanceId();
		String taskRef = task.getReferenceTaskName();
		int retryCount = task.getRetryCount();
		String inputJson = "";
		Map<String, Object> output = new HashMap<>();
		try {

			/*
			 * TaskConfig taskConfig = SchedulerDatabaseUtility
			 * .findByWorkflowAndRef(Long.valueOf(workflowDefinition.getId()), taskRef);
			 */

			// String pixel = taskConfig.getPixel();

			inputJson = mapper.writeValueAsString(task.getInputData());

			Map<String, Object> inputData = task.getInputData();

			String pixel = (String) inputData.get("pixel");

			System.out.println("Pixel: " + pixel);

			SchedulerDatabaseUtility.insertTaskExecutionLog(workflowInstanceId, workflowDefinition.getName(),
					workflowDefinition.getVersion(), taskRef, task.getTaskDefName(), "STARTED", retryCount, inputJson,
					"", "");

			// String pixel = (String) task.getInputData().get("pixel");

			if (pixel == null) {
				throw new RuntimeException("Pixel is missing for task: " + taskRef);
			}

			PixelRunner innerRunner = insight.runPixel(pixel);

			NounMetadata nounMetadata = innerRunner.getResults().get(0);

			Map<String, Object> resultMap = (Map<String, Object>) nounMetadata.getValue();

			String key = resultMap.keySet().stream().findFirst().orElse(null);

			if (key != null) {
				insight.getVarStore().put(key, nounMetadata);
			}

			// Important: expose fields for DECISION task
			output.put("result", resultMap);

			result.setOutputData(output);
			result.setStatus(TaskResult.Status.COMPLETED);

			SchedulerDatabaseUtility.insertTaskExecutionLog(workflowInstanceId, workflowDefinition.getName(),
					workflowDefinition.getVersion(), taskRef, task.getTaskDefName(), "COMPLETED", retryCount, inputJson,
					output.toString(), "");

		} catch (Exception e) {

			result.setStatus(TaskResult.Status.FAILED);
			result.setReasonForIncompletion(e.getMessage());

			try {
				SchedulerDatabaseUtility.insertTaskExecutionLog(workflowInstanceId, workflowDefinition.getName(),
						workflowDefinition.getVersion(), taskRef, task.getTaskDefName(), "COMPLETED", retryCount,
						inputJson, output.toString(), e.getMessage());
				SchedulerDatabaseUtility.insertWorkflowDlq(task.getWorkflowInstanceId(), workflowDefinition.getName(),
						workflowDefinition.getVersion(), task.getReferenceTaskName(), e.getMessage(),
						task.getInputData().toString());

			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}

		return result;
	}
}
