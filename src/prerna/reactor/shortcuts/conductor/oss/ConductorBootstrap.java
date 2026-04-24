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
package prerna.reactor.shortcuts.conductor.oss;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.client.http.ConductorClient;
import com.netflix.conductor.client.http.MetadataClient;
import com.netflix.conductor.client.worker.Worker;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;

import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.reactor.scheduler.SchedulerDatabaseUtility;
import prerna.reactor.shortcuts.conductor.oss.workers.PixelWorker;

public class ConductorBootstrap {
	public static void init(String workflowDefinitionName, int version) throws Exception {

		WorkflowDefinition workflowDefinition = SchedulerDatabaseUtility
				.findWorkflowDefinitionByName(workflowDefinitionName, version);

		ObjectMapper mapper = new ObjectMapper();
		ConductorClient client = ConductorClientFactory.getClient();
		MetadataClient mc = new MetadataClient(client);

		Set<String> uniqueTaskNames = new HashSet<>();

		// for (WorkflowDefinition w : workflows) {

		// 2. Convert JSON - WorkflowDef
		WorkflowDef def = mapper.readValue(workflowDefinition.getJson(), WorkflowDef.class);

		// 3. Register workflow if not exists
		try {
			mc.getWorkflowDef(def.getName(), def.getVersion());
		} catch (Exception e) {
			mc.registerWorkflowDef(def);
		}

		// 4. Parse JSON - extract task names
		Map<String, Object> map = mapper.readValue(workflowDefinition.getJson(), Map.class);
		List<Map<String, Object>> tasks = (List<Map<String, Object>>) map.get("tasks");

		extractTaskNames(tasks, uniqueTaskNames);
		// }

		// 5. Create workers dynamically
		List<Worker> workers = new ArrayList<>();

		Insight insight = new Insight();

		String insightId = "TempWorkflowInsight_" + UUID.randomUUID().toString();
		ThreadStore.setInsightId(insightId);
		insight.setInsightId(insightId);

		for (String taskName : uniqueTaskNames) {
			workers.add(new PixelWorker(taskName, insight, workflowDefinition));
		}

		// 6. Start workers
		new WorkerInitializer(10).startWorkers(workers);

		System.out.println("Workers initialized for tasks: " + uniqueTaskNames);
	}

	private static void extractTaskNames(List<Map<String, Object>> tasks, Set<String> taskNames) {

		if (tasks == null) {
			return;
		}

		for (Map<String, Object> task : tasks) {

			String type = (String) task.get("type");
			String taskName = (String) task.get("name");

//  Only SIMPLE tasks - actual workers
			if ("SIMPLE".equalsIgnoreCase(type)) {
				taskNames.add(taskName);
			}

//  Handle DECISION tasks
			if ("DECISION".equalsIgnoreCase(type)) {

				Map<String, List<Map<String, Object>>> decisionCases = (Map<String, List<Map<String, Object>>>) task
						.get("decisionCases");

				if (decisionCases != null) {
					for (List<Map<String, Object>> caseTasks : decisionCases.values()) {
						extractTaskNames(caseTasks, taskNames);
					}
				}

				List<Map<String, Object>> defaultCase = (List<Map<String, Object>>) task.get("defaultCase");

				if (defaultCase != null) {
					extractTaskNames(defaultCase, taskNames);
				}
			}
		}
	}
}
