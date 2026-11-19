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
package prerna.reactor.security;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.google.gson.Gson;

import prerna.auth.User;
import prerna.auth.utils.CatalogDependencyDerivation;
import prerna.auth.utils.CatalogDependencyRole;
import prerna.auth.utils.CatalogTypes;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityCatalogDependencyUtils;
import prerna.auth.utils.SecurityCatalogDependencyUtils.Dependency;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.model.ModelRouterEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.pipeline.EngineProxyFactory;
import prerna.project.api.IProject;
import prerna.project.impl.ProjectHelper;
import prerna.project.impl.notebook.INotebookHelper;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.reactor.automation.definition.AutomationDefinitionService;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineUtility;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

/**
 * Works out the automatic dependencies of every engine, agent, automation and app
 * from what each has saved, for items saved before dependencies were derived:
 * engines' guardrails and smss references, model routers' routes, agents' tools,
 * knowledge, skills, model and subagents, automations' nodes, blocks apps'
 * engines, and the ids in code apps' files. Picked
 * dependencies are kept; a row from before IS_AUTOMATIC that a derived one
 * stands for is replaced. Safe to run again.
 */
public class AdminBackfillCatalogDependenciesReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AdminBackfillCatalogDependenciesReactor.class);

	public AdminBackfillCatalogDependenciesReactor() {
		this.keysToGet = new String[0];
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		if (!SecurityAdminUtils.userIsAdmin(user)) {
			throw new IllegalArgumentException("User is not an admin and does not have access. Please login as an admin");
		}

		int engines = 0;
		List<String> failed = new ArrayList<>();
		for (String engineId : SecurityEngineUtils.getAllEngineIds()) {
			try {
				backfillEngine(user, engineId);
				engines++;
			} catch (Exception e) {
				classLogger.warn("Could not backfill the dependencies of engine {}", engineId, e);
				failed.add(engineId);
			}
		}

		int workspaces = 0;
		for (String projectId : projectIdsOfType(IProject.PROJECT_TYPE.WORKSPACE.name())) {
			try {
				backfillWorkspace(user, projectId);
				workspaces++;
			} catch (Exception e) {
				classLogger.warn("Could not backfill the dependencies of agent {}", projectId, e);
				failed.add(projectId);
			}
		}

		int automations = 0;
		for (String projectId : projectIdsOfType(IProject.PROJECT_TYPE.AUTOMATION.name())) {
			try {
				backfillAutomation(user, projectId);
				automations++;
			} catch (Exception e) {
				classLogger.warn("Could not backfill the dependencies of automation {}", projectId, e);
				failed.add(projectId);
			}
		}

		int blocks = 0;
		for (String projectId : projectIdsOfType(IProject.PROJECT_TYPE.BLOCKS.name())) {
			try {
				backfillBlocks(user, projectId);
				blocks++;
			} catch (Exception e) {
				classLogger.warn("Could not backfill the dependencies of blocks app {}", projectId, e);
				failed.add(projectId);
			}
		}

		int code = 0;
		for (String projectId : projectIdsOfType(IProject.PROJECT_TYPE.CODE.name())) {
			try {
				backfillCode(user, projectId);
				code++;
			} catch (Exception e) {
				classLogger.warn("Could not backfill the dependencies of code app {}", projectId, e);
				failed.add(projectId);
			}
		}

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("engines", engines);
		result.put("agents", workspaces);
		result.put("automations", automations);
		result.put("blocks", blocks);
		result.put("code", code);
		result.put("failed", failed);
		return new NounMetadata(result, PixelDataType.MAP);
	}

	@SuppressWarnings("unchecked")
	private static void backfillEngine(User user, String engineId) throws Exception {
		IEngine engine = Utility.getEngine(engineId);
		if (engine == null) {
			throw new IllegalStateException("Engine " + engineId + " could not be loaded");
		}
		String engineType = engine.getCatalogType().name();
		CatalogDependencyDerivation.syncSmss(user, engineId, engineType, engine.getSmssProp());

		if (engine.getCatalogType() != IEngine.CATALOG_TYPE.GUARDRAIL) {
			String pipelineName = engine.getSmssProp().getProperty(IEngine.PIPELINE);
			Map<String, Object> pipelines = Map.of();
			if (pipelineName != null && !pipelineName.isBlank()) {
				File pipelineFile = EngineProxyFactory.pipelineFile(engine, pipelineName.trim());
				if (pipelineFile.isFile()) {
					Map<String, Object> config = new Gson().fromJson(
							Files.readString(pipelineFile.toPath(), StandardCharsets.UTF_8), Map.class);
					if (config != null && config.get("pipelines") instanceof Map<?, ?> read) {
						pipelines = (Map<String, Object>) read;
					}
				}
			}
			CatalogDependencyDerivation.syncGuardrails(user, engineId, engineType, pipelines);
		}

		if (engine instanceof ModelRouterEngine router) {
			File configFile = router.resolveConfigFile();
			if (configFile.isFile()) {
				CatalogDependencyDerivation.syncRouterJson(user, engineId,
						Files.readString(configFile.toPath(), StandardCharsets.UTF_8));
			}
		}
	}

	private static void backfillWorkspace(User user, String workspaceId) {
		List<Map<String, Object>> resources = ModelInferenceLogsUtils.getWorkspaceResourcesByType(workspaceId, null);
		JSONObject config = ModelInferenceLogsUtils.getWorkspaceConfigJson(workspaceId);
		String modelId = config == null ? null : config.optString("model_id", null);
		List<String> subagentIds = new ArrayList<>();
		JSONArray subagents = config == null ? null : config.optJSONArray("subagents");
		if (subagents != null) {
			for (int i = 0; i < subagents.length(); i++) {
				JSONObject subagent = subagents.optJSONObject(i);
				if (subagent != null) {
					subagentIds.add(subagent.optString("workspaceId", null));
				}
			}
		}
		List<Dependency> dependencies = CatalogDependencyDerivation.fromWorkspace(resources, modelId, subagentIds,
				CatalogTypes::resolve);
		SecurityCatalogDependencyUtils.syncAutomaticDependencies(user, workspaceId,
				IProject.PROJECT_TYPE.WORKSPACE.name(), CatalogDependencyDerivation.WORKSPACE_ROLES, dependencies);
	}

	@SuppressWarnings("unchecked")
	private static void backfillAutomation(User user, String projectId) {
		String definition = AutomationDefinitionService.load(projectId).definition();
		List<Map<String, Object>> nodes = new ArrayList<>();
		if (definition != null && !definition.isBlank()) {
			Map<String, Object> parsed = new Gson().fromJson(definition, Map.class);
			if (parsed != null && parsed.get("nodes") instanceof List<?> list) {
				for (Object node : list) {
					if (node instanceof Map<?, ?> nodeMap) {
						nodes.add((Map<String, Object>) nodeMap);
					}
				}
			}
		}
		SecurityCatalogDependencyUtils.syncAutomaticDependencies(user, projectId,
				IProject.PROJECT_TYPE.AUTOMATION.name(), List.of(CatalogDependencyRole.AUTOMATION_NODE),
				CatalogDependencyDerivation.fromAutomationNodes(nodes, CatalogTypes::resolve));
	}

	/**
	 * A blocks app's BLOCK_ENGINE dependencies, read from its saved blocks json as
	 * SaveAppBlocksJson records them
	 */
	private static void backfillBlocks(User user, String projectId) {
		IProject project = Utility.getProject(projectId);
		if (project == null) {
			throw new IllegalStateException("Blocks app " + projectId + " could not be loaded");
		}
		Set<String> engineIds = new HashSet<>(project.getEngineDependencies().values());
		engineIds.remove(INotebookHelper.UNDEFINED_VALUE);
		SecurityCatalogDependencyUtils.syncAutomaticDependencyIds(user, projectId, IProject.PROJECT_TYPE.BLOCKS.name(),
				CatalogDependencyRole.BLOCK_ENGINE, "Engine a block of this app uses", engineIds);
	}

	/**
	 * A code app's CODE_REFERENCE dependencies: the engine and project ids found in
	 * its files, as ExtractAndSetDependencies records them for the whole app
	 */
	@SuppressWarnings("unchecked")
	private static void backfillCode(User user, String projectId) {
		IProject project = Utility.getProject(projectId);
		if (project == null) {
			throw new IllegalStateException("Code app " + projectId + " could not be loaded");
		}
		File assets = new File(EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.PROJECT,
				project.getProjectId(), project.getProjectName()));
		Map<String, Object> found = ProjectHelper.extractEngineIdsFromProjectFolder(assets);
		Set<String> ids = found.get("success") instanceof Map<?, ?> success
				? ((Map<String, Object>) success).keySet()
				: Set.of();
		SecurityCatalogDependencyUtils.syncAutomaticDependencyIds(user, projectId, IProject.PROJECT_TYPE.CODE.name(),
				CatalogDependencyRole.CODE_REFERENCE, "Engine referenced in this app's files", ids);
	}

	private static List<String> projectIdsOfType(String type) {
		try {
			return QueryExecutionUtility.queryList(SystemEngineRegistry.getSecurityDb(),
					"SELECT PROJECTID FROM PROJECT WHERE TYPE=?", ps -> ps.setString(1, type), rs -> rs.getString(1));
		} catch (Exception e) {
			classLogger.error("Could not list the {} projects", type, e);
			return List.of();
		}
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().noKeys()
				.description("Admin Only: Works out the automatic dependencies of every engine, agent, automation and app "
						+ "from its saved configuration: guardrails, judge models, vector databases, embedders, router "
						+ "models, agent tools, knowledge, skills, models and subagents, automation steps, blocks app engines "
						+ "and the ids in code apps' files. "
						+ "Picked dependencies are kept. Safe to run again.")
				.output(Map.class, "How many engines, agents, automations, blocks apps and code apps were read, and the ids that failed");
	}
}
