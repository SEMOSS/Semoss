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
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.client.worker.Worker;

import prerna.om.Insight;
import prerna.om.ThreadStore;

public class WorkerFactory {

	public static List<Worker> createWorkers(String json) throws Exception {

		Insight insight = new Insight();

		String insightId = "TempWorkflowInsight_" + UUID.randomUUID();
		ThreadStore.setInsightId(insightId);
		insight.setInsightId(insightId);

		ObjectMapper mapper = new ObjectMapper();

		Map<String, Object> map = mapper.readValue(json, Map.class);

		List<Map<String, Object>> tasks = (List<Map<String, Object>>) map.get("tasks");

		List<Worker> workers = new ArrayList<>();

		// Recursive extraction
		extractWorkers(tasks, workers, insight);

		return workers;
	}

	private static void extractWorkers(List<Map<String, Object>> tasks, List<Worker> workers, Insight insight) {

		if (tasks == null) {
			return;
		}

		for (Map<String, Object> task : tasks) {

			String taskName = (String) task.get("name");
			String taskRef = (String) task.get("taskReferenceName");
			String type = (String) task.get("type");

			// Create worker for SIMPLE tasks only
			if ("SIMPLE".equalsIgnoreCase(type)) {
				// workers.add(new PixelWorker(taskName, insight));
			}

			// Handle DECISION tasks
			if ("DECISION".equalsIgnoreCase(type)) {

				// Handle decisionCases
				Map<String, List<Map<String, Object>>> decisionCases = (Map<String, List<Map<String, Object>>>) task
						.get("decisionCases");

				if (decisionCases != null) {
					for (List<Map<String, Object>> caseTasks : decisionCases.values()) {
						extractWorkers(caseTasks, workers, insight);
					}
				}

				// Handle defaultCase
				List<Map<String, Object>> defaultCase = (List<Map<String, Object>>) task.get("defaultCase");

				if (defaultCase != null) {
					extractWorkers(defaultCase, workers, insight);
				}
			}
		}
	}
}
