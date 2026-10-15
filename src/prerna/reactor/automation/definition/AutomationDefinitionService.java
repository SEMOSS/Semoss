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
package prerna.reactor.automation.definition;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.AutomationRuntime;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;
import prerna.util.AssetUtility;

/**
 * Persists and loads the canonical automation graph and explicit Python-node
 * implementations.
 *
 * <p>
 * Only {@code developer.python} owns source. Trigger and typed nodes reject
 * source rather than entering a compatibility or migration path.
 */
public final class AutomationDefinitionService {

	private static final Gson PRETTY_JSON = new GsonBuilder().setPrettyPrinting().create();
	private static final Logger classLogger = LogManager.getLogger(AutomationDefinitionService.class);
	private static final String TRANSACTION_FOLDER = ".automation-save";
	private static final String TRANSACTION_MARKER_FILE = "publishing";
	private static final String TRANSACTION_STAGED_FOLDER = "staged";
	private static final String TRANSACTION_BACKUP_FOLDER = "backup";
	private static final String TRANSACTION_RECOVERY_FOLDER = "recovery";
	/** GetAutomation response keys that are derived, not part of the graph. */
	private static final List<String> INLINE_DERIVED_KEYS = List.of(AutomationConstants.DOC_NODE_SOURCES,
			"sourceHashes", AutomationConstants.RESULT_REVISION, AutomationConstants.RESULT_SCOPE_VARIABLES,
			AutomationConstants.DOC_GLOBALS);

	private AutomationDefinitionService() {
	}

	/**
	 * Loads the complete definition. Missing files produce an empty graph.
	 *
	 * @param projectId project ID
	 * @return graph and Python source
	 */
	public static DefinitionFiles load(String projectId) {
		Path assetsFolder = getAssetsFolder(projectId);
		Path transaction = transactionFolder(assetsFolder);
		Path readableAssetsFolder = Files.isRegularFile(transaction.resolve(TRANSACTION_MARKER_FILE))
				&& Files.isRegularFile(definitionPath(transaction.resolve(TRANSACTION_BACKUP_FOLDER)))
						? transaction.resolve(TRANSACTION_BACKUP_FOLDER)
						: assetsFolder;
		return loadFromFolder(readableAssetsFolder);
	}

	private static DefinitionFiles loadFromFolder(Path folder) {
		Path definitionFile = definitionPath(folder);
		try {
			if (!Files.isRegularFile(definitionFile)) {
				AutomationDefinitionValidator.ValidatedDefinition starter = AutomationDefinitionValidator
						.parseAndValidate(emptyDefinition());
				return new DefinitionFiles(emptyDefinition(),
						withoutTriggerSources(defaultNodeSources(starter), starter));
			}
			String definition = Files.readString(definitionFile, StandardCharsets.UTF_8);
			AutomationDefinitionValidator.ValidatedDefinition validated = AutomationDefinitionValidator
					.parseAndValidateForAuthoring(definition);
			validateUniqueNodeSourceFileNames(validated);
			Map<String, String> sources = new LinkedHashMap<>();
			for (Map<String, Object> node : AutomationRuntime.allNodes(validated)) {
				if (!requiresPythonSource(node)) {
					continue;
				}
				String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
				Path sourceFile = nodeSourcePath(folder, node);
				String source = Files.isRegularFile(sourceFile)
						? Files.readString(sourceFile, StandardCharsets.UTF_8)
						: null;
				validateNodeSource(nodeId, node, source);
				sources.put(nodeId, source);
			}
			return new DefinitionFiles(definition, sources);
		} catch (IOException e) {
			throw new IllegalArgumentException("Unable to read Automation definition: " + e.getMessage(), e);
		}
	}

	/**
	 * Builds a runnable definition from inline JSON without a project or files. A
	 * pasted {@code GetAutomation} export runs as is: its derived keys are dropped
	 * and, when {@code nodeSources} is null, its embedded source map is used.
	 *
	 * <p>
	 * Only {@code developer.python} keeps supplied source. Every generated node is
	 * executed by its Java handler and rejects source.
	 *
	 * @param definitionJson graph document JSON, optionally a GetAutomation export
	 * @param nodeSources    source by node ID, or null to use the embedded map
	 * @return strictly validated graph and the source of each Python-backed node
	 */
	public static DefinitionFiles fromInline(String definitionJson, Map<String, String> nodeSources) {
		if (definitionJson == null || definitionJson.isBlank()) {
			throw new IllegalArgumentException("Automation definition must be a nonblank JSON object.");
		}
		Map<String, Object> document;
		try {
			document = AutomationRuntimeUtils.parseJsonObject(definitionJson, "Python automation definition");
		} catch (JsonParseException e) {
			throw new IllegalArgumentException("Automation definition must be valid JSON.", e);
		}
		Map<String, String> supplied = nodeSources != null ? nodeSources
				: embeddedNodeSources(document.get(AutomationConstants.DOC_NODE_SOURCES));
		for (String derivedKey : INLINE_DERIVED_KEYS) {
			document.remove(derivedKey);
		}
		String definition = AutomationRuntimeUtils.GSON.toJson(document);
		AutomationDefinitionValidator.ValidatedDefinition validated = AutomationDefinitionValidator
				.parseAndValidate(definition);
		return new DefinitionFiles(definition, validateAndCompleteNodeSources(validated, supplied));
	}

	/**
	 * Reports whether a definition runs Python written by its caller.
	 *
	 * @param files definition built by {@link #fromInline(String, Map)}
	 * @return {@code true} when caller-written Python would run
	 */
	public static boolean containsCallerCode(DefinitionFiles files) {
		return !files.nodeSources().isEmpty();
	}

	private static Map<String, String> embeddedNodeSources(Object raw) {
		if (raw == null) {
			return Map.of();
		}
		if (!(raw instanceof Map<?, ?> map)) {
			throw new IllegalArgumentException("nodeSources must be a JSON object keyed by node id.");
		}
		Map<String, String> sources = new LinkedHashMap<>();
		for (Map.Entry<?, ?> entry : map.entrySet()) {
			if (!(entry.getKey() instanceof String nodeId) || !(entry.getValue() instanceof String source)) {
				throw new IllegalArgumentException("nodeSources must map node ids to Python source strings.");
			}
			sources.put(nodeId, source);
		}
		return sources;
	}

	/**
	 * Calculates a deterministic revision for the complete persisted automation
	 * aggregate.
	 *
	 * <p>
	 * The graph is canonicalized by the definition validator and node sources are
	 * ordered by node ID. Length-prefixing prevents different graph/source
	 * boundaries from producing the same input before hashing.
	 *
	 * @param definitionJson graph document JSON
	 * @param nodeSources    source by non-start node ID
	 * @return lowercase SHA-256 revision
	 */
	public static String calculateRevision(String definitionJson, Map<String, String> nodeSources) {
		AutomationDefinitionValidator.ValidatedDefinition definition = AutomationDefinitionValidator
				.parseAndValidateForAuthoring(definitionJson);
		StringBuilder canonical = new StringBuilder();
		appendRevisionValue(canonical, definition.snapshot());
		for (Map.Entry<String, String> entry : new TreeMap<>(nodeSources).entrySet()) {
			appendRevisionValue(canonical, entry.getKey());
			appendRevisionValue(canonical, entry.getValue());
		}
		return sha256(canonical.toString());
	}

	/**
	 * Calculates the source hash used by optimistic custom-node updates.
	 *
	 * @param source persisted node source
	 * @return lowercase SHA-256 hash
	 */
	public static String calculateSourceHash(String source) {
		if (source == null) {
			throw new IllegalArgumentException("Automation node source must not be null.");
		}
		return sha256(source);
	}

	/**
	 * Validates and replaces the automation definition artifacts.
	 *
	 * @param projectId      project ID
	 * @param definitionJson canonical graph JSON
	 * @param nodeSources    source by non-start node ID
	 * @return persisted graph and node sources
	 */
	public static DefinitionFiles save(String projectId, String definitionJson, Map<String, String> nodeSources) {
		AutomationDefinitionValidator.ValidatedDefinition definition = AutomationDefinitionValidator
				.parseAndValidateForAuthoring(definitionJson);
		validateUniqueNodeSourceFileNames(definition);
		Path assetsFolder = getAssetsFolder(projectId);
		Map<String, String> sourcesToPersist = validateAndCompleteNodeSources(definition, nodeSources);
		DefinitionFiles candidate = new DefinitionFiles(definitionJson, sourcesToPersist);

		try {
			Files.createDirectories(assetsFolder);
			recoverTransaction(assetsFolder);
			DefinitionFiles current = load(projectId);
			Path transaction = transactionFolder(assetsFolder);
			Path stagedFolder = transaction.resolve(TRANSACTION_STAGED_FOLDER);
			Path backupFolder = transaction.resolve(TRANSACTION_BACKUP_FOLDER);
			writeAggregate(stagedFolder, candidate);
			writeAggregate(backupFolder, current);
			writeReplace(transaction.resolve(TRANSACTION_MARKER_FILE), "publishing" + System.lineSeparator());
			try {
				replaceAggregate(stagedFolder, assetsFolder);
				Files.delete(transaction.resolve(TRANSACTION_MARKER_FILE));
			} catch (IOException publicationFailure) {
				try {
					restoreBackup(transaction, assetsFolder);
					deleteTree(transaction);
				} catch (IOException recoveryFailure) {
					publicationFailure.addSuppressed(recoveryFailure);
				}
				throw publicationFailure;
			}
			try {
				deleteTree(transaction);
			} catch (IOException cleanupFailure) {
				classLogger.warn("Unable to clean completed automation save transaction for project {}.", projectId,
						cleanupFailure);
			}
			return candidate;
		} catch (IOException e) {
			throw new IllegalArgumentException("Unable to save Automation definition: " + e.getMessage(), e);
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
		DefinitionFiles definition = load(projectId);
		AutomationDefinitionValidator.ValidatedDefinition validated = AutomationDefinitionValidator
				.parseAndValidateForAuthoring(definition.definition());
		List<Path> paths = new ArrayList<>();
		paths.add(definitionPath(assetsFolder));
		for (Map<String, Object> node : AutomationRuntime.allNodes(validated)) {
			if (requiresPythonSource(node)) {
				paths.add(nodeSourcePath(assetsFolder, node));
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

	private static Map<String, String> defaultNodeSources(
			AutomationDefinitionValidator.ValidatedDefinition definition) {
		return validateAndCompleteNodeSources(definition, Map.of());
	}

	private static Map<String, String> withoutTriggerSources(Map<String, String> sources,
			AutomationDefinitionValidator.ValidatedDefinition definition) {
		Map<String, String> result = new LinkedHashMap<>(sources);
		for (Map<String, Object> node : AutomationRuntime.allNodes(definition)) {
			if (!requiresPythonSource(node)) {
				result.remove(node.get(AutomationConstants.NODE_FIELD_ID));
			}
		}
		return result;
	}

	private static Map<String, String> validateAndCompleteNodeSources(
			AutomationDefinitionValidator.ValidatedDefinition definition, Map<String, String> nodeSources) {
		Map<String, String> supplied = nodeSources == null ? Map.of() : nodeSources;
		Map<String, Map<String, Object>> nodesById = new LinkedHashMap<>();
		for (Map<String, Object> node : AutomationRuntime.allNodes(definition)) {
			String id = (String) node.get(AutomationConstants.NODE_FIELD_ID);
			nodesById.put(id, node);
		}
		for (Map.Entry<String, String> entry : supplied.entrySet()) {
			if (!nodesById.containsKey(entry.getKey())) {
				throw new IllegalArgumentException("Python source was supplied for an unknown node: " + entry.getKey());
			}
			if (!requiresPythonSource(nodesById.get(entry.getKey()))) {
				throw new IllegalArgumentException("Only developer.python may own Python source; node '"
						+ entry.getKey() + "' is type '"
						+ nodesById.get(entry.getKey()).get(AutomationConstants.NODE_FIELD_TYPE) + "'.");
			}
			validateNodeSource(entry.getKey(), nodesById.get(entry.getKey()), entry.getValue());
		}
		Map<String, String> result = new LinkedHashMap<>();
		for (Map.Entry<String, Map<String, Object>> entry : nodesById.entrySet()) {
			if (!requiresPythonSource(entry.getValue())) {
				continue;
			}
			String source = supplied.get(entry.getKey());
			validateNodeSource(entry.getKey(), entry.getValue(), source);
			result.put(entry.getKey(), source);
		}
		return result;
	}

	private static void validateNodeSource(String nodeId, Map<String, Object> node, String source) {
		if (source == null || source.isBlank()) {
			throw new IllegalArgumentException("Python source for node '" + nodeId + "' must be nonblank.");
		}
		int sourceBytes = source.getBytes(StandardCharsets.UTF_8).length;
		if (sourceBytes > AutomationConstants.NODE_SOURCE_MAX_BYTES) {
			throw new IllegalArgumentException("Python source for node '" + nodeId + "' exceeds the maximum of "
					+ AutomationConstants.NODE_SOURCE_MAX_BYTES + " UTF-8 bytes.");
		}
		if (!AutomationConstants.NODE_DEVELOPER_PYTHON.equals(
				node.get(AutomationConstants.NODE_FIELD_TYPE))) {
			throw new IllegalArgumentException(
					"Only developer.python may own Python source; node '" + nodeId + "' is not explicit Python.");
		}
		if (!definesRunEntryPoint(source)) {
			throw new IllegalArgumentException("Python source for node '" + nodeId
					+ "' must define run(scope) at the top level. The runtime calls run(scope) with the run's"
					+ " variables and stores what it returns as this node's output.");
		}
	}

	/**
	 * Reports whether Python source binds a module-level {@code run}, which is the entry point
	 * {@code execute_node} looks up. Only an unindented definition or assignment counts, so a
	 * {@code run} nested inside a class or another function is not mistaken for the entry point.
	 *
	 * @param source persisted node source
	 * @return {@code true} when the source binds a top-level {@code run}
	 */
	public static boolean definesRunEntryPoint(String source) {
		return source.lines().anyMatch(line -> {
			if (line.isEmpty() || Character.isWhitespace(line.charAt(0))) {
				return false;
			}
			return line.startsWith("def run(") || line.startsWith("def run (")
					|| line.startsWith("async def run(") || line.startsWith("async def run (")
					|| line.startsWith("run=") || line.startsWith("run =");
		});
	}

	private static Path getAssetsFolder(String projectId) {
		String assetsFolder = AssetUtility.getProjectAssetsFolder(projectId);
		Path path = Path.of(assetsFolder).toAbsolutePath().normalize();
		if (!path.startsWith(Path.of(assetsFolder).toAbsolutePath().normalize())) {
			throw new IllegalArgumentException("Invalid automation definition path.");
		}
		return path;
	}

	private static Path transactionFolder(Path assetsFolder) {
		return assetsFolder.resolve(TRANSACTION_FOLDER).normalize();
	}

	private static void recoverTransaction(Path assetsFolder) throws IOException {
		Path transaction = transactionFolder(assetsFolder);
		if (!Files.exists(transaction)) {
			return;
		}
		if (Files.isRegularFile(transaction.resolve(TRANSACTION_MARKER_FILE))) {
			restoreBackup(transaction, assetsFolder);
		}
		deleteTree(transaction);
	}

	private static void writeAggregate(Path folder, DefinitionFiles files) throws IOException {
		AutomationDefinitionValidator.ValidatedDefinition definition = AutomationDefinitionValidator
				.parseAndValidateForAuthoring(files.definition());
		validateUniqueNodeSourceFileNames(definition);
		Files.createDirectories(nodesFolder(folder));
		writeReplace(definitionPath(folder), prettyJson(files.definition()));
		for (Map<String, Object> node : AutomationRuntime.allNodes(definition)) {
			if (!requiresPythonSource(node)) {
				continue;
			}
			String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
			writeReplace(nodeSourcePath(folder, node), files.nodeSources().get(nodeId));
		}
	}

	private static void replaceAggregate(Path sourceFolder, Path assetsFolder) throws IOException {
		deleteTree(nodesFolder(assetsFolder));
		moveReplace(nodesFolder(sourceFolder), nodesFolder(assetsFolder));
		moveReplace(definitionPath(sourceFolder), definitionPath(assetsFolder));
	}

	private static void restoreBackup(Path transaction, Path assetsFolder) throws IOException {
		Path backupFolder = transaction.resolve(TRANSACTION_BACKUP_FOLDER);
		if (!Files.isRegularFile(definitionPath(backupFolder))) {
			throw new IOException("Automation save backup is unavailable.");
		}
		Path recoveryFolder = transaction.resolve(TRANSACTION_RECOVERY_FOLDER);
		deleteTree(recoveryFolder);
		copyTree(backupFolder, recoveryFolder);
		replaceAggregate(recoveryFolder, assetsFolder);
	}

	private static void copyTree(Path source, Path target) throws IOException {
		try (Stream<Path> paths = Files.walk(source)) {
			for (Path path : paths.toList()) {
				Path destination = target.resolve(source.relativize(path));
				if (Files.isDirectory(path)) {
					Files.createDirectories(destination);
				} else {
					Files.copy(path, destination, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
				}
			}
		}
	}

	private static void moveReplace(Path source, Path target) throws IOException {
		try {
			Files.move(source, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
					java.nio.file.StandardCopyOption.ATOMIC_MOVE);
		} catch (java.nio.file.AtomicMoveNotSupportedException e) {
			Files.move(source, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
		}
	}

	private static void deleteTree(Path folder) throws IOException {
		if (!Files.exists(folder)) {
			return;
		}
		try (Stream<Path> paths = Files.walk(folder)) {
			for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
				Files.deleteIfExists(path);
			}
		}
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

	static String safeNodeFileName(Map<String, Object> node) {
		String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
		if (nodeId == null || nodeId.isBlank()) {
			throw new IllegalArgumentException("Automation node id must be nonblank for source persistence.");
		}
		String label = (String) node.get(AutomationConstants.NODE_FIELD_LABEL);
		String type = (String) node.get(AutomationConstants.NODE_FIELD_TYPE);
		String slug = slugify(label);
		if (slug.isBlank()) {
			slug = slugify(type);
		}
		if (slug.isBlank()) {
			slug = "automation_node";
		}
		if (slug.length() > 64) {
			slug = slug.substring(0, 64);
		}
		return slug + "__" + stableNodeIdSuffix(nodeId);
	}

	private static String stableNodeIdSuffix(String nodeId) {
		if (nodeId.matches(".*[0-9a-fA-F]{8}-[0-9a-fA-F-]{27}$")) {
			return nodeId.substring(nodeId.length() - 36).toLowerCase(Locale.ROOT);
		}
		return sha256(nodeId);
	}

	private static String sha256(String value) {
		try {
			byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
			StringBuilder result = new StringBuilder(digest.length * 2);
			for (byte digestByte : digest) {
				result.append(String.format(Locale.ROOT, "%02x", digestByte & 0xff));
			}
			return result.toString();
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException("SHA-256 is unavailable.", e);
		}
	}

	private static void appendRevisionValue(StringBuilder target, String value) {
		target.append(value.length()).append(':').append(value);
	}

	private static String slugify(String value) {
		if (value == null) {
			return "";
		}
		return value.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "_").replaceAll("^_+|_+$", "");
	}

	private static void validateUniqueNodeSourceFileNames(
			AutomationDefinitionValidator.ValidatedDefinition definition) {
		Map<String, String> nodeIdsByFileName = new LinkedHashMap<>();
		for (Map<String, Object> node : AutomationRuntime.allNodes(definition)) {
			if (!requiresPythonSource(node)) {
				continue;
			}
			String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
			String fileName = safeNodeFileName(node) + ".py";
			String existingNodeId = nodeIdsByFileName.putIfAbsent(fileName, nodeId);
			if (existingNodeId != null && !existingNodeId.equals(nodeId)) {
				throw new IllegalArgumentException("Automation nodes '" + existingNodeId + "' and '" + nodeId
						+ "' resolve to the same source file: " + fileName + ".");
			}
		}
	}

	private static boolean requiresPythonSource(Map<String, Object> node) {
		return AutomationConstants.NODE_DEVELOPER_PYTHON.equals(
				node.get(AutomationConstants.NODE_FIELD_TYPE));
	}

	private static String emptyDefinition() {
		Map<String, Object> start = new LinkedHashMap<>();
		start.put(AutomationConstants.NODE_FIELD_ID, "start");
		start.put(AutomationConstants.NODE_FIELD_TYPE, AutomationConstants.NODE_START);
		start.put(AutomationConstants.NODE_FIELD_LABEL, "Start");
		start.put("position", Map.of("x", 240, "y", 80));
		start.put(AutomationConstants.NODE_FIELD_CODE_MODE, AutomationConstants.NODE_CODE_MODE_GENERATED);
		start.put(AutomationConstants.NODE_FIELD_CONFIG, Map.of());
		Map<String, Object> manualTrigger = new LinkedHashMap<>();
		manualTrigger.put(AutomationConstants.NODE_FIELD_ID, "manual");
		manualTrigger.put(AutomationConstants.NODE_FIELD_TYPE, "manual");

		Map<String, Object> graph = new LinkedHashMap<>();
		graph.put(AutomationConstants.DOC_NODES, List.of(start));
		graph.put(AutomationConstants.DOC_EDGES, List.of());

		Map<String, Object> definition = new LinkedHashMap<>();
		definition.put(AutomationConstants.DOC_FORMAT_VERSION, AutomationConstants.PYTHON_DOC_CURRENT_VERSION);
		definition.put(AutomationConstants.DOC_DESCRIPTION, "");
		definition.put(AutomationConstants.DOC_TRIGGER_BINDINGS, List.of(manualTrigger));
		definition.put(AutomationConstants.DOC_GRAPH, graph);
		return AutomationRuntimeUtils.GSON.toJson(definition);
	}

	/**
	 * Complete persisted Automation aggregate.
	 *
	 * @param definition  canonical graph JSON
	 * @param nodeSources immutable source map keyed by non-start node ID
	 */
	public record DefinitionFiles(String definition, Map<String, String> nodeSources) {
		public DefinitionFiles {
			nodeSources = Map.copyOf(nodeSources);
		}
	}
}
