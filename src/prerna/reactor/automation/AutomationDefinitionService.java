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
package prerna.reactor.automation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;

import prerna.reactor.automation.utils.AutomationRuntimeUtils;
import prerna.util.AssetUtility;

/**
 * Persists and loads the canonical automation graph and its per-node Python implementations.
 *
 * <p>The graph remains canonical metadata. Absent developer source is replaced with deterministic
 * generated {@code run(scope)} source. Trigger code is stored in
 * {@code trigger.start.config.pythonSource}; Java owns graph control flow and invokes one
 * persisted source file for each non-start node.
 */
public final class AutomationDefinitionService {

	private static final Gson PRETTY_JSON = new GsonBuilder().setPrettyPrinting().create();

	private AutomationDefinitionService() {
	}

	/**
	 * Loads the complete definition. Missing files produce an empty graph rather than
	 * interpreting a legacy definition.
	 *
	 * @param projectId project ID
	 * @return graph and Python source
	 */
	public static DefinitionFiles load(String projectId) {
		Path assetsFolder = getAssetsFolder(projectId);
		Path portalsFolder = getPortalsFolder(projectId);
		Path definitionFile = definitionPath(assetsFolder);
		Path legacyDefinitionFile = definitionPath(portalsFolder);
		try {
			if (!Files.isRegularFile(definitionFile)) {
				if (Files.isRegularFile(legacyDefinitionFile)) {
					definitionFile = legacyDefinitionFile;
				} else {
					AutomationDefinitionValidator.ValidatedDefinition starter =
							AutomationDefinitionValidator.parseAndValidate(emptyDefinition());
					return new DefinitionFiles(emptyDefinition(), withoutTriggerSources(defaultNodeSources(starter),
							starter));
				}
			}
			String definition = Files.readString(definitionFile, StandardCharsets.UTF_8);
			AutomationDefinitionValidator.ValidatedDefinition validated =
					AutomationDefinitionValidator.parseAndValidate(definition);
			Map<String, String> sources = new LinkedHashMap<>();
			for (Map<String, Object> node : validated.nodes()) {
				String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
				Path sourceFile = findNodeSourceFile(assetsFolder, portalsFolder, node);
				String source = Files.isRegularFile(sourceFile)
						? Files.readString(sourceFile, StandardCharsets.UTF_8)
						: AutomationSourceRenderer.renderNode(node);
				boolean generated = AutomationConstants.NODE_CODE_MODE_GENERATED.equals(
						node.get(AutomationConstants.NODE_FIELD_CODE_MODE))
						&& !AutomationConstants.NODE_START.equals(
								node.get(AutomationConstants.NODE_FIELD_TYPE));
				sources.put(nodeId, generated ? AutomationSourceRenderer.renderNode(node) : source);
			}
			String normalized = normalizeGeneratedCodeModes(validated, sources, definition);
			return new DefinitionFiles(normalized, withoutTriggerSources(sources,
					AutomationDefinitionValidator.parseAndValidate(normalized)));
		} catch (IOException e) {
			throw new IllegalArgumentException("Unable to read Python automation definition: " + e.getMessage(), e);
		}
	}

	/**
	 * Validates and replaces the automation definition artifacts.
	 *
	 * @param projectId project ID
	 * @param definitionJson canonical graph JSON
	 * @param nodeSources source by non-start node ID; a trigger entry is accepted and migrated
	 *        to {@code trigger.start.config.pythonSource} for compatibility
	 * @return persisted graph and node sources
	 */
	public static DefinitionFiles save(String projectId, String definitionJson, Map<String, String> nodeSources) {
		AutomationDefinitionValidator.ValidatedDefinition definition =
				AutomationDefinitionValidator.parseAndValidate(definitionJson);
		Path assetsFolder = getAssetsFolder(projectId);
		Path definitionFile = definitionPath(assetsFolder);
		Map<String, String> sourcesToPersist = validateAndCompleteNodeSources(definition, nodeSources);
		String persistedDefinition = normalizeGeneratedCodeModes(definition, sourcesToPersist, definitionJson);

		try {
			Files.createDirectories(assetsFolder);
			writeReplace(definitionFile, prettyJson(persistedDefinition));
			Path nodesFolder = nodesFolder(assetsFolder);
			Files.createDirectories(nodesFolder);
			for (Map<String, Object> node : definition.nodes()) {
				if (AutomationConstants.NODE_START.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))) {
					continue;
				}
				String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
				writeReplace(nodeSourcePath(assetsFolder, node), sourcesToPersist.get(nodeId));
			}
			removeDeletedNodeSources(nodesFolder, definition);
			Files.deleteIfExists(assetsFolder.resolve("automation-workflow.py"));
			return new DefinitionFiles(persistedDefinition, withoutTriggerSources(sourcesToPersist,
					AutomationDefinitionValidator.parseAndValidate(persistedDefinition)));
		} catch (IOException e) {
			throw new IllegalArgumentException("Unable to save Python automation definition: " + e.getMessage(), e);
		}
	}

	/**
	 * Creates the initial definition for a newly created automation project.
	 *
	 * @param projectId project ID
	 */
	public static void createStarter(String projectId) {
		save(projectId, emptyDefinition(), Map.of());
	}

	/**
	 * Returns the definition artifacts for versioning and cluster synchronization.
	 *
	 * @param projectId project ID
	 * @return canonical artifact paths
	 */
	public static List<Path> getArtifactPaths(String projectId) {
		Path assetsFolder = getAssetsFolder(projectId);
		Path portalsFolder = getPortalsFolder(projectId);
		Path folder = Files.isRegularFile(definitionPath(assetsFolder)) ? assetsFolder : portalsFolder;
		DefinitionFiles definition = load(projectId);
		AutomationDefinitionValidator.ValidatedDefinition validated =
				AutomationDefinitionValidator.parseAndValidate(definition.definition());
		List<Path> paths = new java.util.ArrayList<>();
		paths.add(definitionPath(folder));
		for (Map<String, Object> node : validated.nodes()) {
			if (!AutomationConstants.NODE_START.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))) {
				paths.add(findNodeSourceFile(assetsFolder, portalsFolder, node));
			}
		}
		return paths;
	}

	private static Path definitionPath(Path folder) {
		return folder.resolve(AutomationConstants.AUTOMATION_PYTHON_DEFINITION_FILE_NAME);
	}

	private static Path nodesFolder(Path folder) {
		return folder.resolve(AutomationConstants.AUTOMATION_NODE_SOURCES_FOLDER_NAME).normalize();
	}

	private static Path findNodeSourceFile(Path assetsFolder, Path portalsFolder, Map<String, Object> node) {
		Path current = nodeSourcePath(assetsFolder, node);
		if (Files.isRegularFile(current)) {
			return current;
		}
		String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
		Path assetsLegacy = legacyNodeSourcePath(assetsFolder, nodeId);
		if (Files.isRegularFile(assetsLegacy)) {
			return assetsLegacy;
		}
		Path portalsCurrent = nodeSourcePath(portalsFolder, node);
		if (Files.isRegularFile(portalsCurrent)) {
			return portalsCurrent;
		}
		return legacyNodeSourcePath(portalsFolder, nodeId);
	}

	private static Map<String, String> defaultNodeSources(AutomationDefinitionValidator.ValidatedDefinition definition) {
		return validateAndCompleteNodeSources(definition, Map.of());
	}

	private static Map<String, String> withoutTriggerSources(Map<String, String> sources,
			AutomationDefinitionValidator.ValidatedDefinition definition) {
		Map<String, String> result = new LinkedHashMap<>(sources);
		for (Map<String, Object> node : definition.nodes()) {
			if (AutomationConstants.NODE_START.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))) {
				result.remove((String) node.get(AutomationConstants.NODE_FIELD_ID));
			}
		}
		return result;
	}

	private static Map<String, String> validateAndCompleteNodeSources(
			AutomationDefinitionValidator.ValidatedDefinition definition, Map<String, String> nodeSources) {
		Map<String, String> supplied = nodeSources == null ? Map.of() : nodeSources;
		Map<String, Map<String, Object>> nodesById = new LinkedHashMap<>();
		for (Map<String, Object> node : definition.nodes()) {
			String id = (String) node.get(AutomationConstants.NODE_FIELD_ID);
			nodesById.put(id, node);
		}
		for (Map.Entry<String, String> entry : supplied.entrySet()) {
			if (!nodesById.containsKey(entry.getKey())) {
				throw new IllegalArgumentException("Python source was supplied for an unknown node: " + entry.getKey());
			}
			if (entry.getValue() == null || entry.getValue().isBlank()) {
				throw new IllegalArgumentException("Python source for node '" + entry.getKey() + "' must be nonblank.");
			}
		}
		Map<String, String> result = new LinkedHashMap<>();
		for (Map.Entry<String, Map<String, Object>> entry : nodesById.entrySet()) {
			String source = supplied.get(entry.getKey());
			result.put(entry.getKey(), source == null
					|| AutomationSourceRenderer.isLegacyDefaultSource(source)
					? AutomationSourceRenderer.renderNode(entry.getValue())
					: source);
		}
		return result;
	}

	/**
	 * A generated source is viewable/editable in the inspector, where an actual
	 * edit intentionally changes its mode to custom. Monaco can also emit its
	 * initial value while hydrating, so recover the generated mode whenever the
	 * persisted source remains byte-for-byte identical to its renderer output.
	 */
	private static String normalizeGeneratedCodeModes(
			AutomationDefinitionValidator.ValidatedDefinition definition, Map<String, String> nodeSources,
			String unchangedDefinition) {
		boolean changed = false;
		changed |= definition.definition().remove(AutomationConstants.DOC_NODE_SOURCES) != null;
		changed |= definition.definition().remove(AutomationConstants.DOC_LEGACY_VARIABLES) != null;
		changed |= definition.definition().remove(AutomationConstants.DOC_GLOBALS) != null;
		for (Map<String, Object> node : definition.nodes()) {
			String nodeType = (String) node.get(AutomationConstants.NODE_FIELD_TYPE);
			if (AutomationConstants.NODE_START.equals(nodeType)) {
				changed |= normalizeTriggerConfig(node, nodeSources);
			}
			if (AutomationConstants.NODE_DEVELOPER_PYTHON.equals(nodeType)
					|| !AutomationConstants.NODE_CODE_MODE_CUSTOM.equals(
							node.get(AutomationConstants.NODE_FIELD_CODE_MODE))) {
				if (!AutomationConstants.NODE_START.equals(nodeType)) {
					continue;
				}
			}
			String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
			boolean generated = AutomationSourceRenderer.renderNode(node).equals(
					AutomationRuntime.triggerSource(node, nodeSources.get(nodeId)));
			if (AutomationConstants.NODE_START.equals(nodeType)) {
				String codeMode = generated
						? AutomationConstants.NODE_CODE_MODE_GENERATED
						: AutomationConstants.NODE_CODE_MODE_CUSTOM;
				if (!codeMode.equals(node.get(AutomationConstants.NODE_FIELD_CODE_MODE))) {
					node.put(AutomationConstants.NODE_FIELD_CODE_MODE, codeMode);
					changed = true;
				}
			} else if (generated) {
				node.put(AutomationConstants.NODE_FIELD_CODE_MODE, AutomationConstants.NODE_CODE_MODE_GENERATED);
				changed = true;
			}
		}
		return changed
				? AutomationRuntimeUtils.GSON.toJson(definition.definition())
				: unchangedDefinition;
	}

	@SuppressWarnings("unchecked")
	private static boolean normalizeTriggerConfig(Map<String, Object> node, Map<String, String> nodeSources) {
		Object rawConfig = node.get(AutomationConstants.NODE_FIELD_CONFIG);
		Map<String, Object> config;
		if (rawConfig instanceof Map<?, ?> map) {
			config = new LinkedHashMap<>((Map<String, Object>) map);
		} else {
			config = new LinkedHashMap<>();
		}
		boolean changed = false;
		Object legacy = config.remove(AutomationConstants.CONFIG_PYTHON);
		if (legacy != null) {
			changed = true;
			if (!config.containsKey(AutomationConstants.CONFIG_PYTHON_SOURCE)) {
				config.put(AutomationConstants.CONFIG_PYTHON_SOURCE, legacy);
			}
		}
		String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
		String legacySource = nodeSources.get(nodeId);
		if (!config.containsKey(AutomationConstants.CONFIG_PYTHON_SOURCE)
				&& legacySource != null
				&& !AutomationSourceRenderer.renderNode(node).equals(legacySource)) {
			config.put(AutomationConstants.CONFIG_PYTHON_SOURCE, legacySource);
			changed = true;
		}
		if (changed) {
			node.put(AutomationConstants.NODE_FIELD_CONFIG, config);
			return true;
		}
		return false;
	}

	private static Path getAssetsFolder(String projectId) {
		String assetsFolder = AssetUtility.getProjectAssetsFolder(projectId);
		Path path = Path.of(assetsFolder).toAbsolutePath().normalize();
		if (!path.startsWith(Path.of(assetsFolder).toAbsolutePath().normalize())) {
			throw new IllegalArgumentException("Invalid automation definition path.");
		}
		return path;
	}

	private static Path getPortalsFolder(String projectId) {
		String portalsFolder = AssetUtility.getProjectPortalsFolder(projectId);
		Path path = Path.of(portalsFolder).toAbsolutePath().normalize();
		if (!path.startsWith(Path.of(portalsFolder).toAbsolutePath().normalize())) {
			throw new IllegalArgumentException("Invalid automation definition path.");
		}
		return path;
	}

	private static void writeReplace(Path target, String content) throws IOException {
		Path temporary = Files.createTempFile(target.getParent(), target.getFileName().toString(), ".tmp");
		try {
			Files.writeString(temporary, content, StandardCharsets.UTF_8);
			Files.move(temporary, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
					java.nio.file.StandardCopyOption.ATOMIC_MOVE);
		} catch (java.nio.file.AtomicMoveNotSupportedException e) {
			Files.move(temporary, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
		} finally {
			Files.deleteIfExists(temporary);
		}
	}

	private static String prettyJson(String definition) {
		return PRETTY_JSON.toJson(JsonParser.parseString(definition)) + System.lineSeparator();
	}

	private static Path nodeSourcePath(Path folder, Map<String, Object> node) {
		Path nodesFolder = nodesFolder(folder);
		Path result = nodesFolder.resolve(safeNodeFileName(node) + ".py").normalize();
		if (!result.startsWith(nodesFolder)) {
			throw new IllegalArgumentException("Invalid automation node source path.");
		}
		return result;
	}

	private static Path legacyNodeSourcePath(Path folder, String nodeId) {
		Path nodesFolder = nodesFolder(folder);
		return nodesFolder.resolve(legacySafeNodeFileName(nodeId) + ".py").normalize();
	}

	static String safeNodeFileName(Map<String, Object> node) {
		String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
		String label = (String) node.get(AutomationConstants.NODE_FIELD_LABEL);
		String type = (String) node.get(AutomationConstants.NODE_FIELD_TYPE);
		String slug = slugify(label);
		if (slug.isBlank()) {
			slug = slugify(type);
		}
		if (slug.isBlank()) {
			slug = "automation_node";
		}
		String uuidPrefix = nodeId != null && nodeId.matches(".*[0-9a-fA-F]{8}-[0-9a-fA-F-]{27}$")
				? nodeId.substring(nodeId.length() - 36, nodeId.length() - 28).toLowerCase()
				: "node";
		return slug + "_" + uuidPrefix;
	}

	private static String slugify(String value) {
		if (value == null) {
			return "";
		}
		return value.toLowerCase(java.util.Locale.ROOT)
				.replaceAll("[^a-z0-9]+", "_")
				.replaceAll("^_+|_+$", "");
	}

	private static String legacySafeNodeFileName(String nodeId) {
		return java.util.Base64.getUrlEncoder().withoutPadding()
				.encodeToString(nodeId.getBytes(StandardCharsets.UTF_8));
	}

	private static void removeDeletedNodeSources(Path nodesFolder,
			AutomationDefinitionValidator.ValidatedDefinition definition) throws IOException {
		java.util.Set<String> currentFiles = new java.util.HashSet<>();
		for (Map<String, Object> node : definition.nodes()) {
			if (!AutomationConstants.NODE_START.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))) {
				currentFiles.add(safeNodeFileName(node) + ".py");
			}
		}
		try (java.util.stream.Stream<Path> paths = Files.list(nodesFolder)) {
			for (Path path : paths.filter(Files::isRegularFile).toList()) {
				if (path.getFileName().toString().endsWith(".py")
						&& !currentFiles.contains(path.getFileName().toString())) {
					Files.delete(path);
				}
			}
		}
	}

	private static String emptyDefinition() {
		Map<String, Object> start = new java.util.LinkedHashMap<>();
		start.put(AutomationConstants.NODE_FIELD_ID, "start");
		start.put(AutomationConstants.NODE_FIELD_TYPE, AutomationConstants.NODE_START);
		start.put(AutomationConstants.NODE_FIELD_LABEL, "Start");
		start.put("position", Map.of("x", 240, "y", 80));
		start.put(AutomationConstants.NODE_FIELD_CODE_MODE, AutomationConstants.NODE_CODE_MODE_GENERATED);
		start.put(AutomationConstants.NODE_FIELD_CONFIG, Map.of());
		Map<String, Object> manualTrigger = new java.util.LinkedHashMap<>();
		manualTrigger.put(AutomationConstants.NODE_FIELD_ID, "manual");
		manualTrigger.put(AutomationConstants.NODE_FIELD_TYPE, "manual");

		Map<String, Object> graph = new java.util.LinkedHashMap<>();
		graph.put(AutomationConstants.DOC_NODES, List.of(start));
		graph.put(AutomationConstants.DOC_EDGES, List.of());

		Map<String, Object> definition = new java.util.LinkedHashMap<>();
		definition.put(AutomationConstants.DOC_FORMAT_VERSION, AutomationConstants.PYTHON_DOC_CURRENT_VERSION);
		definition.put(AutomationConstants.DOC_DESCRIPTION, "");
		definition.put(AutomationConstants.DOC_TRIGGER_BINDINGS, List.of(manualTrigger));
		definition.put(AutomationConstants.DOC_GRAPH, graph);
		return AutomationRuntimeUtils.GSON.toJson(definition);
	}

	/** Definition artifact contents. */
	public record DefinitionFiles(String definition, Map<String, String> nodeSources) {
		public DefinitionFiles {
			nodeSources = Map.copyOf(nodeSources);
		}
	}
}
