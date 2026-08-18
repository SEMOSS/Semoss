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

import prerna.reactor.automation.utils.AutomationRuntimeUtils;
import prerna.util.AssetUtility;

/**
 * Persists and loads the canonical automation graph and its per-node Python implementations.
 *
 * <p>The graph remains canonical metadata. Absent developer source is replaced with deterministic
 * generated {@code run(scope)} source. Java owns graph control flow and invokes only one
 * persisted source file for each non-start node.
 */
public final class AutomationDefinitionService {

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
					return new DefinitionFiles(emptyDefinition(), defaultNodeSources(starter));
				}
			}
			String definition = Files.readString(definitionFile, StandardCharsets.UTF_8);
			AutomationDefinitionValidator.ValidatedDefinition validated =
					AutomationDefinitionValidator.parseAndValidate(definition);
			Map<String, String> sources = new LinkedHashMap<>();
			for (Map<String, Object> node : validated.nodes()) {
				if (!AutomationConstants.NODE_START.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))) {
					String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
					Path sourceFile = findNodeSourceFile(assetsFolder, portalsFolder, node);
					sources.put(nodeId, Files.isRegularFile(sourceFile)
							? Files.readString(sourceFile, StandardCharsets.UTF_8)
							: AutomationSourceRenderer.renderNode(node));
				}
			}
			return new DefinitionFiles(definition, sources);
		} catch (IOException e) {
			throw new IllegalArgumentException("Unable to read Python automation definition: " + e.getMessage(), e);
		}
	}

	/**
	 * Validates and replaces the automation definition artifacts.
	 *
	 * @param projectId project ID
	 * @param definitionJson canonical graph JSON
	 * @param nodeSources source by non-start node ID, or absent entries to render defaults
	 * @return persisted graph and node sources
	 */
	public static DefinitionFiles save(String projectId, String definitionJson, Map<String, String> nodeSources) {
		AutomationDefinitionValidator.ValidatedDefinition definition =
				AutomationDefinitionValidator.parseAndValidate(definitionJson);
		Path assetsFolder = getAssetsFolder(projectId);
		Path definitionFile = definitionPath(assetsFolder);
		Map<String, String> sourcesToPersist = validateAndCompleteNodeSources(definition, nodeSources);

		try {
			Files.createDirectories(assetsFolder);
			writeReplace(definitionFile, definitionJson);
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
			return new DefinitionFiles(definitionJson, sourcesToPersist);
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

	private static Map<String, String> validateAndCompleteNodeSources(
			AutomationDefinitionValidator.ValidatedDefinition definition, Map<String, String> nodeSources) {
		Map<String, String> supplied = nodeSources == null ? Map.of() : nodeSources;
		Map<String, Map<String, Object>> nodesById = new LinkedHashMap<>();
		for (Map<String, Object> node : definition.nodes()) {
			String id = (String) node.get(AutomationConstants.NODE_FIELD_ID);
			if (!AutomationConstants.NODE_START.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))) {
				nodesById.put(id, node);
			}
		}
		for (Map.Entry<String, String> entry : supplied.entrySet()) {
			if (!nodesById.containsKey(entry.getKey())) {
				throw new IllegalArgumentException("Python source was supplied for an unknown or start node: "
						+ entry.getKey());
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
