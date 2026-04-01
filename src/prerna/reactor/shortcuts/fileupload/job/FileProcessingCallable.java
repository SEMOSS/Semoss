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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.quartz.Scheduler;

import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.reactor.scheduler.SchedulerDatabaseUtility;
import prerna.reactor.scheduler.SchedulerFactorySingleton;
import prerna.util.Utility;

public class FileProcessingCallable implements Callable<FileProcessResult> {

	private final Path file;

	protected Scheduler scheduler = null;

	public FileProcessingCallable(Path file) {
		this.file = file;
	}

	// Map<String, String> replacements = new HashMap<String, String>();

	@Override
	public FileProcessResult call() throws Exception {

		System.out.println(" Processing file: " + file + " | Thread: " + Thread.currentThread());
		try {
			// Actual logic happens here
			String basePath = Utility.normalizePath(file.getParent().toString());
			// replacements.put("{filename}", file.getFileName().toString());
			String workflowJson = "";// SchedulerDatabaseUtility.fetchWorkflowFromWatchDirectory(basePath);
			workflowJson = workflowJson.replace("{filename}", file.getFileName().toString());
			// resolvePlaceholders(workflowJson, replacements);

			Insight insight = new Insight();

			String insightId = "TempWorkflowInsight_" + UUID.randomUUID().toString();
			ThreadStore.setInsightId(insightId);
			insight.setInsightId(insightId);

			ObjectMapper mapper = new ObjectMapper();

			Workflow workflow = null;
			workflow = mapper.readValue(workflowJson, Workflow.class);

			WorkflowDefinition workflowDefinition = WorkflowParser.parse(workflow);

			ExecutionContext ctx = new ExecutionContext(UUID.randomUUID().toString(),
					workflowDefinition.getWorkflowId());

			// WorkflowOrchestrator orchestrator = new WorkflowOrchestrator(new
			// ActionService(), new DlqRepository());

			scheduler = SchedulerFactorySingleton.getInstance().getScheduler();

			// start up scheduler
			SchedulerDatabaseUtility.startScheduler(scheduler);

			if (workflow.getExecution().getMode().equals("SCHEDULED")) {

			}

			else if (workflow.getExecution().isTriggerNow()) {

				NodeExecutor.executeNode(workflowDefinition, workflowDefinition.startNodeId, ctx, insight).join();

				System.out.println("Workflow Completed");
			}

			else {
				NodeExecutor.executeNode(workflowDefinition, workflowDefinition.startNodeId, ctx, insight).join();

				System.out.println("Workflow Completed");

				System.out.println("Workflow Completed");

			}

			long size = Files.size(file);
			return new FileProcessResult(file.toString(), true, "Processed successfully (size=" + size + ")");
			// WorkflowScheduler.runNow(wf, file, "exec-001");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;

	}

	/*
	 * public static Path resolveFilePath(String watchDirectoryPath, String
	 * fileName) {
	 * 
	 * String resolved = watchDirectoryPath.replace("{filename}", fileName);
	 * 
	 * return Path.of(resolved).normalize(); }
	 */

	/*
	 * public static String resolvePlaceholders(String json, Map<String, String>
	 * values) throws Exception {
	 * 
	 * ObjectMapper mapper = new ObjectMapper();
	 * 
	 * JsonNode root = mapper.readTree(json);
	 * 
	 * JsonNode updated = replaceRecursive(root, values);
	 * 
	 * return mapper.writeValueAsString(updated); }
	 * 
	 * private static JsonNode replaceRecursive(JsonNode node, Map<String, String>
	 * values) {
	 * 
	 * if (node.isObject()) {
	 * 
	 * ObjectNode objNode = (ObjectNode) node;
	 * 
	 * Iterator<Map.Entry<String, JsonNode>> fields = objNode.fields();
	 * 
	 * while (fields.hasNext()) {
	 * 
	 * Map.Entry<String, JsonNode> entry = fields.next();
	 * 
	 * objNode.set(entry.getKey(), replaceRecursive(entry.getValue(), values)); }
	 * 
	 * return objNode; }
	 * 
	 * if (node.isArray()) {
	 * 
	 * ArrayNode arrayNode = (ArrayNode) node;
	 * 
	 * for (int i = 0; i < arrayNode.size(); i++) { arrayNode.set(i,
	 * replaceRecursive(arrayNode.get(i), values)); }
	 * 
	 * return arrayNode; }
	 * 
	 * if (node.isTextual()) {
	 * 
	 * String text = node.asText();
	 * 
	 * for (Map.Entry<String, String> entry : values.entrySet()) {
	 * 
	 * String placeholder = "{" + entry.getKey() + "}";
	 * 
	 * if (text.contains(placeholder)) { text = text.replace(placeholder,
	 * entry.getValue()); } }
	 * 
	 * return new TextNode(text); }
	 * 
	 * return node; }
	 */

}
