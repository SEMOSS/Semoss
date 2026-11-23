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
package prerna.auth.utils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;

import prerna.auth.User;
import prerna.auth.utils.SecurityCatalogDependencyUtils.Dependency;
import prerna.engine.api.IEngine;
import prerna.engine.impl.model.ModelRouterEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.pipeline.EngineProxyFactory;
import prerna.project.api.IProject;
import prerna.project.impl.ProjectHelper;
import prerna.project.impl.notebook.INotebookHelper;
import prerna.project.impl.notebook.NotebookHelperFactory;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.definition.AutomationDefinitionService;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Utility;

/**
 * Works out the automatic dependencies of one catalog item again from what it
 * has saved, and records them: an engine's smss references, guardrails and model
 * router routes; an agent's tools, knowledge, skills, model and subagents; an
 * automation's nodes; a blocks app's engines; and the ids in a code app's files.
 * Picked dependencies are kept.
 *
 * <p>
 * Unlike the syncs a save runs in {@link CatalogDependencyDerivation}, which only
 * log, a resync throws when the item cannot be read or its dependencies cannot be
 * recorded, so the caller can report it. A resync never clears an item's
 * dependencies because its configuration could not be read: an item that is
 * missing, or whose saved configuration is present but cannot be parsed (an
 * agent's config json, an automation's definition, a blocks app's blocks json,
 * an engine's pipeline.json or router.json), throws before anything is written.
 * Only an item that has genuinely saved nothing, such as an agent with no config
 * json or a blocks app with no blocks json, has the dependencies it derives
 * cleared.
 * </p>
 *
 * <p>
 * An engine's smss, guardrail and router dependencies are worked out first and
 * then recorded in one transaction, so a malformed pipeline.json or router.json
 * leaves all of them as they were.
 * </p>
 */
public final class CatalogDependencyResync {

	private CatalogDependencyResync() {

	}

	/**
	 * Works out and records the automatic dependencies of a catalog item. A project
	 * type with nothing to derive, such as SKILL or INSIGHTS, is left as it is.
	 *
	 * @param user        who asks, recorded on each row
	 * @param catalogId   the engine id or project id
	 * @param catalogType its engine type or project type, or null to read it from
	 *                    the security database
	 * @throws IllegalArgumentException when no engine or project has the id
	 * @throws IllegalStateException    when the item, or its saved configuration,
	 *                                  cannot be read; nothing is written then
	 * @throws Exception                when the item cannot be read or its
	 *                                  dependencies cannot be recorded
	 */
	public static void resync(User user, String catalogId, String catalogType) throws Exception {
		String type = catalogType == null || catalogType.isBlank() ? CatalogTypes.resolve(catalogId)
				: CatalogTypes.normalize(catalogType);
		if (type == null) {
			throw new IllegalArgumentException("No engine or project has the id " + catalogId);
		}
		if (!CatalogTypes.isProjectType(type)) {
			resyncEngine(user, catalogId);
			return;
		}
		IProject.PROJECT_TYPE projectType = IProject.PROJECT_TYPE.valueOf(type);
		switch (projectType) {
		case WORKSPACE -> resyncWorkspace(user, catalogId);
		case AUTOMATION -> resyncAutomation(user, catalogId);
		case BLOCKS -> resyncBlocks(user, catalogId);
		case CODE -> resyncCode(user, catalogId);
		default -> {
			// nothing is derived from its configuration
		}
		}
	}

	/**
	 * An engine's smss references, the guardrails its pipeline.json runs (but for
	 * a guardrail's own), and a model router's routes, recorded in one transaction
	 */
	public static void resyncEngine(User user, String engineId) throws Exception {
		IEngine engine = Utility.getEngine(engineId);
		if (engine == null) {
			throw new IllegalStateException("Engine " + engineId + " could not be loaded");
		}
		resyncEngine(user, engine);
	}

	/** {@link #resyncEngine(User, String)} for an engine already loaded */
	static void resyncEngine(User user, IEngine engine) throws IOException {
		Properties smss = engine.getSmssProp();
		String pipelineName = pipelineFileName(smss);
		File pipelineFile = pipelineName == null ? null : EngineProxyFactory.pipelineFile(engine, pipelineName);
		File routerConfigFile = engine instanceof ModelRouterEngine router ? router.resolveConfigFile() : null;
		record(user, engine.getEngineId(), engine.getCatalogType(), smss, pipelineFile, routerConfigFile);
	}

	/**
	 * Records an engine's automatic dependencies from its smss and the files in its
	 * folder, without loading the engine, as an upload has them: its smss
	 * references, the guardrails its pipeline.json runs and, for a model router,
	 * the routes in its router.json.
	 *
	 * @param user       who uploads, recorded on each row
	 * @param engineId   the engine id
	 * @param engineName the engine name, which names its folder
	 * @param engineType its catalog type
	 * @param smss       its smss properties
	 * @throws IOException           when its pipeline.json or router.json cannot
	 *                               be read
	 * @throws IllegalStateException when its pipeline.json or router.json cannot be
	 *                               parsed; nothing is written then
	 */
	public static void resyncEngineFromFiles(User user, String engineId, String engineName,
			IEngine.CATALOG_TYPE engineType, Properties smss) throws IOException {
		if (engineType == null || smss == null) {
			return;
		}
		// the same files EngineProxyFactory.pipelineFile and
		// ModelRouterEngine.resolveConfigFile find for the loaded engine
		String assetsFolder = EngineUtility.getSpecificEngineAssetsFolder(engineType, engineId, engineName);
		String pipelineName = pipelineFileName(smss);
		File pipelineFile = pipelineName == null ? null
				: new File((assetsFolder + "/" + pipelineName).replace("\\", "/"));
		File routerConfigFile = null;
		if (isModelRouter(smss)) {
			String configName = smss.getProperty(ModelRouterEngine.ROUTER_CONFIG);
			if (configName == null || configName.isBlank()) {
				configName = ModelRouterEngine.DEFAULT_CONFIG_FILE;
			}
			String routerAssetsFolder = EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.MODEL,
					engineId, engineName);
			routerConfigFile = new File((routerAssetsFolder + "/" + configName.trim()).replace("\\", "/"));
		}
		record(user, engineId, engineType, smss, pipelineFile, routerConfigFile);
	}

	/**
	 * Works out every automatic dependency of an engine, then writes them once. The
	 * guardrail role applies to every engine but a guardrail; the router roles
	 * apply only to a model router whose router.json exists.
	 */
	private static void record(User user, String engineId, IEngine.CATALOG_TYPE catalogType, Properties smss,
			File pipelineFile, File routerConfigFile) throws IOException {
		String engineType = catalogType.name();
		Set<CatalogDependencyRole> roles = new LinkedHashSet<>(CatalogDependencyDerivation.SMSS_ROLES);
		List<Dependency> dependencies = new ArrayList<>(
				CatalogDependencyDerivation.fromSmss(engineType, smss, CatalogTypes::resolve));

		if (catalogType != IEngine.CATALOG_TYPE.GUARDRAIL) {
			roles.add(CatalogDependencyRole.GUARDRAIL);
			dependencies.addAll(CatalogDependencyDerivation.fromGuardrailPipelines(readPipelines(engineId, pipelineFile),
					CatalogTypes::resolve));
		}

		if (routerConfigFile != null && routerConfigFile.isFile()) {
			roles.addAll(CatalogDependencyDerivation.ROUTER_ROLES);
			dependencies.addAll(CatalogDependencyDerivation.fromRouterConfig(readRouterConfig(engineId, routerConfigFile),
					CatalogTypes::resolve));
		}

		SecurityCatalogDependencyUtils.syncAutomaticDependencies(user, engineId, engineType, roles, dependencies);
	}

	/** The pipeline.json an engine's smss names, or null for none */
	private static String pipelineFileName(Properties smss) {
		String name = smss == null ? null : smss.getProperty(IEngine.PIPELINE);
		return name == null || name.isBlank() ? null : name.trim();
	}

	/** Whether an engine's smss names a model router class, without loading the engine */
	private static boolean isModelRouter(Properties smss) {
		String className = smss.getProperty(Constants.ENGINE_TYPE);
		if (className == null || className.isBlank()) {
			return false;
		}
		try {
			Class<?> engineClass = Class.forName(className.trim(), false, CatalogDependencyResync.class.getClassLoader());
			return ModelRouterEngine.class.isAssignableFrom(engineClass);
		} catch (ClassNotFoundException | LinkageError e) {
			return false;
		}
	}

	/**
	 * The pipelines a pipeline.json holds. An engine without one, or whose named
	 * file does not exist, runs no guardrails.
	 */
	@SuppressWarnings("unchecked")
	private static Map<String, Object> readPipelines(String engineId, File pipelineFile) throws IOException {
		if (pipelineFile == null || !pipelineFile.isFile()) {
			return Map.of();
		}
		Map<String, Object> config;
		try {
			config = new Gson().fromJson(Files.readString(pipelineFile.toPath(), StandardCharsets.UTF_8), Map.class);
		} catch (JsonParseException e) {
			throw new IllegalStateException(
					"The pipeline configuration of engine " + engineId + " could not be parsed: " + e.getMessage(), e);
		}
		if (config != null && config.get("pipelines") instanceof Map<?, ?> read) {
			return (Map<String, Object>) read;
		}
		return Map.of();
	}

	/** A model router's router.json, which the router cannot run without */
	@SuppressWarnings("unchecked")
	private static Map<String, Object> readRouterConfig(String engineId, File configFile) throws IOException {
		Map<String, Object> config;
		try {
			config = new Gson().fromJson(Files.readString(configFile.toPath(), StandardCharsets.UTF_8), Map.class);
		} catch (JsonParseException e) {
			throw new IllegalStateException(
					"The router configuration of engine " + engineId + " could not be parsed: " + e.getMessage(), e);
		}
		if (config == null) {
			throw new IllegalStateException("The router configuration of engine " + engineId + " is empty");
		}
		return config;
	}

	/**
	 * An agent's tools, knowledge, skills, model and subagents. An agent with no
	 * config json records no model or subagents.
	 *
	 * @throws IllegalStateException when the agent has no workspace row, or its
	 *                               config json cannot be parsed
	 */
	public static void resyncWorkspace(User user, String workspaceId) {
		JSONObject config = readWorkspaceConfig(workspaceId);
		List<Map<String, Object>> resources = ModelInferenceLogsUtils.getWorkspaceResourcesByType(workspaceId, null);
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

	/**
	 * An agent's saved config json, or null when it has none. Read from the row
	 * itself, since ModelInferenceLogsUtils.getWorkspaceConfigJson gives null for
	 * a missing row and an unparseable value too.
	 */
	private static JSONObject readWorkspaceConfig(String workspaceId) {
		Map<String, Object> row = ModelInferenceLogsUtils.getWorkspaceEntry(workspaceId);
		if (row == null) {
			throw new IllegalStateException("Agent " + workspaceId + " could not be read");
		}
		Object raw = row.get("config_json");
		String text = raw == null ? null : String.valueOf(raw).trim();
		if (text == null || text.isEmpty()) {
			return null;
		}
		try {
			return new JSONObject(text);
		} catch (JSONException e) {
			throw new IllegalStateException(
					"The configuration of agent " + workspaceId + " could not be parsed: " + e.getMessage(), e);
		}
	}

	/**
	 * The engines, apps and agents an automation's nodes use. A workflow with no
	 * nodes records none.
	 *
	 * @throws IllegalStateException when the automation, or its saved definition,
	 *                               cannot be found or parsed
	 */
	@SuppressWarnings("unchecked")
	public static void resyncAutomation(User user, String projectId) {
		IProject project = Utility.getProject(projectId);
		if (project == null) {
			throw new IllegalStateException("Automation " + projectId + " could not be loaded");
		}
		// AutomationDefinitionService.load gives an empty workflow when the file is
		// missing, which would clear the nodes
		File definitionFile = new File(AssetUtility.getProjectAssetsFolder(project.getProjectName(), projectId),
				AutomationConstants.AUTOMATION_PYTHON_DEFINITION_FILE_NAME);
		if (!definitionFile.isFile()) {
			throw new IllegalStateException("Automation " + projectId + " has no saved definition");
		}
		String definition;
		Map<String, Object> parsed;
		try {
			definition = AutomationDefinitionService.load(projectId).definition();
			parsed = definition == null || definition.isBlank() ? null : new Gson().fromJson(definition, Map.class);
		} catch (IllegalArgumentException | JsonParseException e) {
			throw new IllegalStateException(
					"The definition of automation " + projectId + " could not be parsed: " + e.getMessage(), e);
		}
		if (parsed == null) {
			throw new IllegalStateException("The definition of automation " + projectId + " is empty");
		}
		List<Map<String, Object>> nodes = new ArrayList<>();
		if (parsed.get("nodes") instanceof List<?> list) {
			for (Object node : list) {
				if (node instanceof Map<?, ?> nodeMap) {
					nodes.add((Map<String, Object>) nodeMap);
				}
			}
		}
		SecurityCatalogDependencyUtils.syncAutomaticDependencies(user, projectId,
				IProject.PROJECT_TYPE.AUTOMATION.name(), List.of(CatalogDependencyRole.AUTOMATION_NODE),
				CatalogDependencyDerivation.fromAutomationNodes(nodes, CatalogTypes::resolve));
	}

	/**
	 * A blocks app's BLOCK_ENGINE dependencies, read from its saved blocks json as
	 * SaveAppBlocksJson records them. An app with no blocks json records none.
	 *
	 * @throws IllegalStateException when the app cannot be loaded, or its blocks
	 *                               json cannot be read or parsed
	 */
	public static void resyncBlocks(User user, String projectId) {
		IProject project = Utility.getProject(projectId);
		if (project == null) {
			throw new IllegalStateException("Blocks app " + projectId + " could not be loaded");
		}
		// read the file here: Project.getEngineDependencies gives null both for no
		// blocks json and for one it cannot read
		File blocksFile = new File(AssetUtility.getProjectPortalsFolder(project.getProjectName(), projectId),
				IProject.BLOCK_FILE_NAME);
		Set<String> engineIds = new HashSet<>();
		if (blocksFile.isFile()) {
			Map<String, String> engines;
			try {
				engines = NotebookHelperFactory.getNotebookHelper(blocksFile).getBlocksEngineDependencies();
			} catch (IOException | RuntimeException e) {
				throw new IllegalStateException(
						"The blocks of app " + projectId + " could not be read: " + e.getMessage(), e);
			}
			if (engines != null) {
				engineIds.addAll(engines.values());
			}
		}
		engineIds.remove(INotebookHelper.UNDEFINED_VALUE);
		SecurityCatalogDependencyUtils.syncAutomaticDependencyIds(user, projectId, IProject.PROJECT_TYPE.BLOCKS.name(),
				CatalogDependencyRole.BLOCK_ENGINE, "Engine a block of this app uses", engineIds);
	}

	/**
	 * A code app's CODE_REFERENCE dependencies: the engine and project ids found in
	 * its files, as ExtractAndSetDependencies records them for the whole app
	 *
	 * @throws IllegalStateException when the app cannot be loaded or has no assets
	 *                               folder
	 */
	@SuppressWarnings("unchecked")
	public static void resyncCode(User user, String projectId) {
		IProject project = Utility.getProject(projectId);
		if (project == null) {
			throw new IllegalStateException("Code app " + projectId + " could not be loaded");
		}
		File assets = new File(EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.PROJECT,
				project.getProjectId(), project.getProjectName()));
		if (!assets.isDirectory()) {
			throw new IllegalStateException("The files of code app " + projectId + " could not be found");
		}
		Map<String, Object> found = ProjectHelper.extractEngineIdsFromProjectFolder(assets);
		Set<String> ids = found.get("success") instanceof Map<?, ?> success
				? ((Map<String, Object>) success).keySet()
				: Set.of();
		SecurityCatalogDependencyUtils.syncAutomaticDependencyIds(user, projectId, IProject.PROJECT_TYPE.CODE.name(),
				CatalogDependencyRole.CODE_REFERENCE, "Engine referenced in this app's files", ids);
	}
}
