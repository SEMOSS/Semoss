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
package prerna.util;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.project.api.IProject;

/**
 * Database-backed agent definitions carried alongside ordinary project files.
 */
public final class AgentProjectArchiveUtils {

	public static final String FILE_SUFFIX = "_agent.json";
	private static final Gson GSON = new GsonBuilder().serializeNulls().disableHtmlEscaping().setPrettyPrinting()
			.create();

	private AgentProjectArchiveUtils() {

	}

	/** Writes the portable workspace fields and all resource associations. */
	public static void exportAgent(ZipOutputStream zip, IProject project, String prefix) throws IOException {
		if (project.getProjectType() != IProject.PROJECT_TYPE.WORKSPACE) {
			return;
		}
		SystemEngineRegistry.requireDatabase(Constants.MODEL_INFERENCE_LOGS_DB, "agent export");
		String projectId = project.getProjectId();
		Map<String, Object> workspace = ModelInferenceLogsUtils.getWorkspaceEntry(projectId);
		List<Map<String, Object>> resources = ModelInferenceLogsUtils.getWorkspaceResourcesByType(projectId, null);
		if (workspace == null || resources == null) {
			throw new IllegalArgumentException("Unable to read the agent definition for project " + projectId);
		}
		JsonObject metadata = new JsonObject();
		metadata.addProperty("schema_version", 1);
		JsonObject definition = new JsonObject();
		definition.addProperty("workspace_id", projectId);
		for (String field : List.of("name", "description", "system_prompt", "is_active")) {
			definition.add(field, GSON.toJsonTree(workspace.get(field)));
		}
		Object config = workspace.get("config_json");
		definition.add("config_json",
				config == null || config.toString().isBlank() ? null : JsonParser.parseString(config.toString()));
		metadata.add("workspace", definition);
		JsonArray associations = new JsonArray();
		for (Map<String, Object> resource : resources) {
			JsonObject association = new JsonObject();
			for (String field : List.of("resource_id", "resource_type", "resource_subtype")) {
				association.add(field, GSON.toJsonTree(resource.get(field)));
			}
			associations.add(association);
		}
		metadata.add("resources", associations);
		validate(metadata, projectId);
		String name = project.getProjectName() + FILE_SUFFIX;
		zip.putNextEntry(new ZipEntry(prefix == null || prefix.isEmpty() ? name : prefix + "/" + name));
		zip.write(GSON.toJson(metadata).getBytes(StandardCharsets.UTF_8));
		zip.closeEntry();
	}

	/** Validates before upload moves files or replaces an existing project. */
	public static JsonObject readAgent(File directory, Properties properties) throws IOException {
		File file = new File(directory, properties.getProperty(Constants.PROJECT_ALIAS) + FILE_SUFFIX);
		if (!file.exists()) {
			// Archives created before agent export support retain their original behavior.
			return null;
		}
		if (!IProject.PROJECT_TYPE.WORKSPACE.name()
				.equals(properties.getProperty(Constants.PROJECT_ENUM_TYPE, "").trim())) {
			throw new IllegalArgumentException("Agent metadata requires a WORKSPACE project");
		}
		SystemEngineRegistry.requireDatabase(Constants.MODEL_INFERENCE_LOGS_DB, "agent import");
		try (Reader reader = Files.newBufferedReader(file.toPath(), StandardCharsets.UTF_8)) {
			JsonObject metadata = JsonParser.parseReader(reader).getAsJsonObject();
			validate(metadata, properties.getProperty(Constants.PROJECT));
			return metadata;
		} catch (RuntimeException e) {
			throw new IllegalArgumentException("Invalid agent metadata: " + e.getMessage(), e);
		}
	}

	/**
	 * Restores the definition using the destination project id and importing owner.
	 */
	public static void importAgent(String projectId, User user, JsonObject metadata, boolean replace) throws Exception {
		if (metadata == null) {
			return;
		}
		validate(metadata, projectId);
		JsonObject workspace = metadata.getAsJsonObject("workspace");
		List<Map<String, String>> resources = new ArrayList<>();
		for (JsonElement element : metadata.getAsJsonArray("resources")) {
			JsonObject resource = element.getAsJsonObject();
			Map<String, String> row = new HashMap<>();
			row.put("workspace_resource_id", UUID.randomUUID().toString());
			for (String field : List.of("resource_id", "resource_type", "resource_subtype")) {
				row.put(field, string(resource, field, false));
			}
			resources.add(row);
		}
		JsonElement config = workspace.get("config_json");
		ModelInferenceLogsUtils.importWorkspaceEntry(projectId, user.getPrimaryLoginToken().getId(),
				string(workspace, "name", true), string(workspace, "description", false),
				string(workspace, "system_prompt", false), workspace.get("is_active").getAsBoolean(),
				config == null || config.isJsonNull() ? null : config.toString(), resources, replace);
	}

	/**
	 * Restore the exported dependency types, including non-UUID platform projects.
	 */
	public static void importDependencies(File directory, String projectName, String projectId, User user)
			throws IOException {
		File file = new File(directory, projectName + IProject.DEPENDENCIES_FILE_SUFFIX);
		if (!file.isFile()) {
			return;
		}
		List<Map<String, Object>> dependencies = new ArrayList<>();
		try (Reader reader = Files.newBufferedReader(file.toPath(), StandardCharsets.UTF_8)) {
			for (JsonElement element : JsonParser.parseReader(reader).getAsJsonArray()) {
				JsonObject dependency = element.getAsJsonObject();
				dependencies.add(Map.of("ENGINEID", string(dependency, "engine_id", true), "ENGINETYPE",
						IProject.CATALOG_TYPE.valueOf(string(dependency, "engine_type", true)).name()));
			}
		}
		SecurityProjectUtils.updateProjectDependencies(user, projectId, dependencies);
		Files.delete(file.toPath());
	}

	private static void validate(JsonObject metadata, String projectId) {
		JsonElement version = metadata.get("schema_version");
		if (version == null || !version.isJsonPrimitive() || !version.getAsJsonPrimitive().isNumber()
				|| !"1".equals(version.getAsString())) {
			throw new IllegalArgumentException("Unsupported agent archive schema_version");
		}
		JsonObject workspace = metadata.getAsJsonObject("workspace");
		if (workspace == null || !string(workspace, "workspace_id", true).equals(projectId)) {
			throw new IllegalArgumentException("Agent workspace_id must match the project id");
		}
		string(workspace, "name", true);
		string(workspace, "description", false);
		string(workspace, "system_prompt", false);
		JsonElement active = workspace.get("is_active");
		if (active == null || !active.isJsonPrimitive() || !active.getAsJsonPrimitive().isBoolean()) {
			throw new IllegalArgumentException("Agent is_active must be a boolean");
		}
		JsonElement config = workspace.get("config_json");
		if (config != null && !config.isJsonNull() && !config.isJsonObject()) {
			throw new IllegalArgumentException("Agent config_json must be an object or null");
		}
		JsonArray resources = metadata.getAsJsonArray("resources");
		if (resources == null) {
			throw new IllegalArgumentException("Agent resources must be an array");
		}
		for (JsonElement element : resources) {
			JsonObject resource = element.getAsJsonObject();
			string(resource, "resource_id", true);
			string(resource, "resource_type", true);
			string(resource, "resource_subtype", false);
		}
	}

	private static String string(JsonObject object, String field, boolean required) {
		JsonElement value = object.get(field);
		if (value == null || value.isJsonNull()) {
			if (!required) {
				return null;
			}
		} else if (value.isJsonPrimitive() && value.getAsJsonPrimitive().isString()
				&& (!required || !value.getAsString().isBlank())) {
			return value.getAsString();
		}
		throw new IllegalArgumentException(
				"Agent " + field + " must be " + (required ? "a nonempty string" : "a string or null"));
	}

}
