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

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.scheduler.SchedulerDatabaseUtility;
import prerna.reactor.shortcuts.conductor.oss.ConductorBootstrap;
import prerna.reactor.shortcuts.conductor.oss.TaskConfig;
import prerna.reactor.shortcuts.conductor.oss.WorkflowDefinition;
import prerna.reactor.shortcuts.conductor.oss.WorkflowMapping;
import prerna.reactor.shortcuts.fileupload.job.FileWatchServiceFactory;
import prerna.reactor.shortcuts.fileupload.job.FileWatcherManager;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class WorkflowReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(WorkflowReactor.class);

	public WorkflowReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.WATCH_DIRECTORY.getKey(),
				ReactorKeysEnum.TEMPLATE.getKey(), ReactorKeysEnum.JSON.getKey() };
		this.keyRequired = new int[] { 1, 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		String dir = this.keyValue.get(ReactorKeysEnum.WATCH_DIRECTORY.getKey());
		String isTemplate = this.keyValue.get(ReactorKeysEnum.TEMPLATE.getKey());
		if (projectId == null || projectId.isEmpty()) {
			throw new IllegalArgumentException("Must input an project id");
		}

		User user = this.insight.getUser();
		// String workflowId = UUID.randomUUID().toString();
		// make sure valid id for user
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(user, projectId);
		if (!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
			// you dont have access
			throw new IllegalArgumentException("Project does not exist or user does not have access to the project");
		}

		IProject project = Utility.getProject(projectId);
		if (project.requirePublish(true)) {
			classLogger.info(project.getProjectId() + " had to pull from cloud");
		}
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

		Map<String, Object> workflowMap = getBlocksJSON();
		String workflowJson = null;

		try {
			workflowJson = mapper.writeValueAsString(workflowMap);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		/*
		 * String workflowJson = null; try { workflowJson =
		 * Files.readString(Path.of("workflow.json")); } catch (IOException e) { // TODO
		 * Auto-generated catch block e.printStackTrace(); }
		 */

		// Workflow workflow = null;
		/*
		 * workflowJsonObj = mapper.readValue( workflowJson, WorkflowJson.class);
		 */

		try {
			WorkflowDef workflowDef = mapper.readValue(workflowJson, WorkflowDef.class);
			System.out.print(workflowDef.getName());
			// workflowDefinition = mapper.readValue(workflowJson,
			// WorkflowDefinition.class);
			// String watchDir = null;
			// Start trigger based on workflow
			// if
			// ("FILE_WATCHER".equalsIgnoreCase(workflowDefinition.getTrigger().getType()))
			// {

			// watchDir = //
			// workflowDefinition.getTrigger().getFileWatcher().getWatchDirectory();

			// System.out.println("Watching directory: " + watchDir);

			try {

				FileWatchServiceFactory factory = FileWatchServiceFactory.getInstance();

				factory.start();

				FileWatcherManager manager = factory.getManager();

				String watchDir = Utility.normalizePath(dir);

				manager.addDirectory(Path.of(dir).toAbsolutePath().normalize());
				// manager.removeDirectory(Path.of(watchDir));
				/*
				 * Thread.sleep(5000); manager.pauseDirectory(Path.of(watchDir));
				 * 
				 * Thread.sleep(5000); manager.resumeDirectory(Path.of(watchDir));
				 * 
				 * Thread.sleep(5000); manager.removeDirectory(Path.of(watchDir));
				 * 
				 * Thread.sleep(5000); factory.restart();
				 * 
				 * Thread.sleep(5000); factory.shutdown();
				 */
				// Save runtime workflow (trigger + json)
				// SchedulerDatabaseUtility.saveWorkflow(workflowDef.getName(), 1, true,
				// watchDir, "", workflowJson);

				// Optional: save template
				/*
				 * if (Boolean.parseBoolean(isTemplate)) {
				 * SchedulerDatabaseUtility.saveWorkflowTemplate(workflowDef.getName(), 1,
				 * workflowJson); }
				 */
				Map<String, Object> map = mapper.readValue(workflowJson, Map.class);

				// ===== 1. SAVE WORKFLOW =====
				WorkflowDefinition workflowDefinition = new WorkflowDefinition();
				workflowDefinition.name = (String) map.get("name");
				workflowDefinition.version = (Integer) map.get("version");
				workflowDefinition.json = workflowJson;
				workflowDefinition.status = "ACTIVE";

				// save and get workflow_id
				Long workflowId = SchedulerDatabaseUtility.saveWorkflowDefinition(workflowDefinition.name,
						workflowDefinition.version, workflowDefinition.json, workflowDefinition.status,
						user.getPrimaryLoginToken().getId());

				// ===== 2. PARSE TASKS FROM JSON =====
				List<Map<String, Object>> tasks = (List<Map<String, Object>>) map.get("tasks");

				saveTasksRecursively(tasks, workflowId);

				// ===== 3. SAVE WORKFLOW MAPPING =====
				WorkflowMapping mapping = new WorkflowMapping();
				mapping.directory = watchDir; // input param
				mapping.workflowName = workflowDefinition.name;
				mapping.version = workflowDefinition.version;
				mapping.setProjectId(projectId);
				SchedulerDatabaseUtility.saveWorkflowMapping(mapping.directory, mapping.projectId, mapping.workflowName,
						mapping.version, user.getPrimaryLoginToken().getId());

				ConductorBootstrap.init(workflowDefinition.name, workflowDefinition.version, this.insight);
				// SchedulerDatabaseUtility.createWorkflowTriggerMapping(watchDir, "*.txt",
				// "file_import_workflow");

				// SchedulerDatabaseUtility.createWorkflowTemplate("file_import_workflow", 1,
				// workflowJson, true);

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// }
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// workflow = mapper.convertValue(workflowMap, Workflow.class);

		/*
		 * WorkflowDefinition wf = null; try { wf =
		 * WorkflowParser.parse(workflowJsonObj); } catch (Exception e) { // TODO
		 * Auto-generated catch block e.printStackTrace(); }
		 */

		/*
		 * List<File> files = List.of(new File("data.csv"), new File("report.pdf"));
		 * 
		 * for (File file : files) { ExecutionContext ctx = new ExecutionContext();
		 * ctx.input.put("file", file);
		 * 
		 * try { // WorkflowScheduler.runNow(wf, file, "exec-001"); } catch (Exception
		 * e) { // TODO Auto-generated catch block e.printStackTrace(); } new
		 * WorkflowEngine().run(wf, ctx); }
		 */

		/*
		 * try { WorkflowEntity workflowEntity = mapToEntity(workflowDefinition,
		 * workflowJson); SchedulerDatabaseUtility.insertWorkflow(workflowEntity); }
		 * catch (Exception e) { // TODO Auto-generated catch block e.printStackTrace();
		 * }
		 */
		return null;

	}

	private void saveTasksRecursively(List<Map<String, Object>> tasks, Long workflowId) throws Exception {

		if (tasks == null) {
			return;
		}

		for (Map<String, Object> task : tasks) {

			String taskName = (String) task.get("name");
			String taskRef = (String) task.get("taskReferenceName");
			String type = (String) task.get("type");
			Map<String, Object> inputParams = (Map<String, Object>) task.get("inputParameters");

			String pixel = (String) inputParams.get("pixel");

//  ONLY SAVE SIMPLE TASKS
			if ("SIMPLE".equalsIgnoreCase(type)) {

				TaskConfig t = new TaskConfig();
				t.workflowId = workflowId;
				t.taskName = taskName;
				t.taskReferenceName = taskRef;

//  You must define pixel mapping logic
				t.pixel = pixel;

				t.retryCount = 3;
				t.retryDelaySeconds = 5;
				t.timeoutSeconds = 60;
				t.async = false;

				SchedulerDatabaseUtility.saveTaskConfig(t.workflowId, t.taskName, t.taskReferenceName, t.pixel,
						t.retryCount, t.retryDelaySeconds, t.timeoutSeconds, t.async);
			}

//  HANDLE DECISION TASK
			if ("DECISION".equalsIgnoreCase(type)) {

				Map<String, List<Map<String, Object>>> decisionCases = (Map<String, List<Map<String, Object>>>) task
						.get("decisionCases");

				if (decisionCases != null) {
					for (List<Map<String, Object>> caseTasks : decisionCases.values()) {
						saveTasksRecursively(caseTasks, workflowId);
					}
				}

				List<Map<String, Object>> defaultCase = (List<Map<String, Object>>) task.get("defaultCase");

				if (defaultCase != null) {
					saveTasksRecursively(defaultCase, workflowId);
				}
			}
		}
	}

	private Map<String, Object> getBlocksJSON() {
		GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.JSON.getKey());
		if (mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if (mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Object>) mapInputs.get(0).getValue();
			}

			List<NounMetadata> encodedStrGrs = mapGrs.getNounsOfType(PixelDataType.CONST_STRING);
			if (encodedStrGrs != null && !encodedStrGrs.isEmpty()) {
				String encodedStr = (String) encodedStrGrs.get(0).getValue();
				String mapStr = Utility.decodeURIComponent(encodedStr);
				return new Gson().fromJson(mapStr, Map.class);
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, Object>) mapInputs.get(0).getValue();
		}

		return null;
	}

	/*
	 * public WorkflowEntity mapToEntity(WorkflowDefinitionCopy wf, String rawJson)
	 * {
	 * 
	 * WorkflowEntity entity = new WorkflowEntity();
	 * 
	 * entity.workflowKey = wf.getWorkflowKey(); entity.workflowName =
	 * wf.getWorkflowName(); entity.workflowTemplateKey =
	 * wf.getWorkflowTemplateKey(); entity.version = wf.getVersion(); entity.status
	 * = wf.getStatus();
	 * 
	 * // Trigger mapping if (wf.getTrigger() != null) { entity.triggerType =
	 * wf.getTrigger().getType();
	 * 
	 * if ("FILE_WATCHER".equals(wf.getTrigger().getType())) { entity.watchDirectory
	 * = wf.getTrigger().getFileWatcher().getWatchDirectory(); entity.filePattern =
	 * wf.getTrigger().getFileWatcher().getFilePattern(); entity.recursiveWatch =
	 * wf.getTrigger().getFileWatcher().getRecursive(); } }
	 * 
	 * // Save full JSON entity.workflowJson = rawJson;
	 * 
	 * return entity; }
	 */
}
