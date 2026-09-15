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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
	private static final Logger classLogger = LogManager.getLogger(AutomationDefinitionService.class);
	private static final String TRANSACTION_FOLDER = ".automation-save";
	private static final String TRANSACTION_MARKER_FILE = "publishing";
	private static final String TRANSACTION_STAGED_FOLDER = "staged";
	private static final String TRANSACTION_BACKUP_FOLDER = "backup";
	private static final String TRANSACTION_RECOVERY_FOLDER = "recovery";

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
		Path transaction = transactionFolder(assetsFolder);
		Path readableAssetsFolder = Files.isRegularFile(transaction.resolve(TRANSACTION_MARKER_FILE))
				&& Files.isRegularFile(definitionPath(transaction.resolve(TRANSACTION_BACKUP_FOLDER)))
				? transaction.resolve(TRANSACTION_BACKUP_FOLDER)
				: assetsFolder;
		return loadFromFolders(readableAssetsFolder, portalsFolder);
	}

	private static DefinitionFiles loadFromFolders(Path primaryFolder, Path fallbackFolder) {
		Path definitionFile = definitionPath(primaryFolder);
		Path fallbackDefinitionFile = definitionPath(fallbackFolder);
		try {
			if (!Files.isRegularFile(definitionFile)) {
				if (Files.isRegularFile(fallbackDefinitionFile)) {
					definitionFile = fallbackDefinitionFile;
				} else {
					AutomationDefinitionValidator.ValidatedDefinition starter =
							AutomationDefinitionValidator.parseAndValidate(emptyDefinition());
					return new DefinitionFiles(emptyDefinition(), withoutTriggerSources(defaultNodeSources(starter),
							starter));
				}
			}
			String definition = Files.readString(definitionFile, StandardCharsets.UTF_8);
			AutomationDefinitionValidator.ValidatedDefinition validated =
					AutomationDefinitionValidator.parseAndValidateForAuthoring(definition);
			validateUniqueNodeSourceFileNames(validated);
			Map<String, String> sources = new LinkedHashMap<>();
			for (Map<String, Object> node : validated.nodes()) {
				if (AutomationConstants.NODE_CONTROL_IF.equals(
						node.get(AutomationConstants.NODE_FIELD_TYPE))) {
					continue;
				}
				String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
				Path sourceFile = findNodeSourceFile(primaryFolder, fallbackFolder, node);
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
					AutomationDefinitionValidator.parseAndValidateForAuthoring(normalized)));
		} catch (IOException e) {
			throw new IllegalArgumentException("Unable to read Python automation definition: " + e.getMessage(), e);
		}
	}

	/**
	 * Calculates a deterministic revision for the complete persisted automation aggregate.
	 *
	 * <p>The graph is canonicalized by the definition validator and node sources are ordered by node
	 * ID. Length-prefixing prevents different graph/source boundaries from producing the same input
	 * before hashing.
	 *
	 * @param definitionJson graph document JSON
	 * @param nodeSources source by non-start node ID
	 * @return lowercase SHA-256 revision
	 */
	public static String calculateRevision(String definitionJson, Map<String, String> nodeSources) {
		AutomationDefinitionValidator.ValidatedDefinition definition =
				AutomationDefinitionValidator.parseAndValidateForAuthoring(definitionJson);
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
	 * @param projectId project ID
	 * @param definitionJson canonical graph JSON
	 * @param nodeSources source by non-start node ID; a trigger entry is accepted and migrated
	 *        to {@code trigger.start.config.pythonSource} for compatibility
	 * @return persisted graph and node sources
	 */
	public static DefinitionFiles save(String projectId, String definitionJson, Map<String, String> nodeSources) {
		AutomationDefinitionValidator.ValidatedDefinition definition =
				AutomationDefinitionValidator.parseAndValidateForAuthoring(definitionJson);
		validateUniqueNodeSourceFileNames(definition);
		Path assetsFolder = getAssetsFolder(projectId);
		Map<String, String> sourcesToPersist = validateAndCompleteNodeSources(definition, nodeSources);
		String persistedDefinition = normalizeGeneratedCodeModes(definition, sourcesToPersist, definitionJson);
		DefinitionFiles candidate = new DefinitionFiles(persistedDefinition,
				withoutTriggerSources(sourcesToPersist,
						AutomationDefinitionValidator.parseAndValidateForAuthoring(persistedDefinition)));

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
				Files.deleteIfExists(assetsFolder.resolve("automation-workflow.py"));
			} catch (IOException cleanupFailure) {
				classLogger.warn("Unable to remove the legacy automation workflow for project {}.",
						projectId, cleanupFailure);
			}
			try {
				deleteTree(transaction);
			} catch (IOException cleanupFailure) {
				classLogger.warn("Unable to clean completed automation save transaction for project {}.",
						projectId, cleanupFailure);
			}
			return candidate;
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
				AutomationDefinitionValidator.parseAndValidateForAuthoring(definition.definition());
		List<Path> paths = new ArrayList<>();
		paths.add(definitionPath(folder));
		for (Map<String, Object> node : validated.nodes()) {
			if (requiresPythonSource(node)) {
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
		Path assetsPriorReadable = priorReadableNodeSourcePath(assetsFolder, node);
		if (Files.isRegularFile(assetsPriorReadable)) {
			return assetsPriorReadable;
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
		Path portalsPriorReadable = priorReadableNodeSourcePath(portalsFolder, node);
		if (Files.isRegularFile(portalsPriorReadable)) {
			return portalsPriorReadable;
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
			if (!requiresPythonSource(node)) {
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
			if (AutomationConstants.NODE_CONTROL_IF.equals(
					nodesById.get(entry.getKey()).get(AutomationConstants.NODE_FIELD_TYPE))) {
				throw new IllegalArgumentException("If node '" + entry.getKey()
						+ "' is evaluated by Java and cannot have Python source.");
			}
			validateNodeSource(entry.getKey(), entry.getValue());
		}
		Map<String, String> result = new LinkedHashMap<>();
		for (Map.Entry<String, Map<String, Object>> entry : nodesById.entrySet()) {
			if (AutomationConstants.NODE_CONTROL_IF.equals(
					entry.getValue().get(AutomationConstants.NODE_FIELD_TYPE))) {
				continue;
			}
			String source = supplied.get(entry.getKey());
			String completedSource = source == null
					|| AutomationSourceRenderer.isLegacyDefaultSource(source)
					? AutomationSourceRenderer.renderNode(entry.getValue())
					: source;
			validateNodeSource(entry.getKey(), completedSource);
			result.put(entry.getKey(), completedSource);
		}
		return result;
	}

	private static void validateNodeSource(String nodeId, String source) {
		if (source == null || source.isBlank()) {
			throw new IllegalArgumentException("Python source for node '" + nodeId + "' must be nonblank.");
		}
		int sourceBytes = source.getBytes(StandardCharsets.UTF_8).length;
		if (sourceBytes > AutomationConstants.NODE_SOURCE_MAX_BYTES) {
			throw new IllegalArgumentException("Python source for node '" + nodeId + "' exceeds the maximum of "
					+ AutomationConstants.NODE_SOURCE_MAX_BYTES + " UTF-8 bytes.");
		}
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
			if (AutomationConstants.NODE_CONTROL_IF.equals(nodeType)) {
				if (!AutomationConstants.NODE_CODE_MODE_GENERATED.equals(
						node.get(AutomationConstants.NODE_FIELD_CODE_MODE))) {
					node.put(AutomationConstants.NODE_FIELD_CODE_MODE,
							AutomationConstants.NODE_CODE_MODE_GENERATED);
					changed = true;
				}
				continue;
			}
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
		AutomationDefinitionValidator.ValidatedDefinition definition =
				AutomationDefinitionValidator.parseAndValidateForAuthoring(files.definition());
		validateUniqueNodeSourceFileNames(definition);
		Files.createDirectories(nodesFolder(folder));
		writeReplace(definitionPath(folder), prettyJson(files.definition()));
		for (Map<String, Object> node : definition.nodes()) {
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

	private static Path legacyNodeSourcePath(Path folder, String nodeId) {
		Path nodesFolder = nodesFolder(folder);
		return nodesFolder.resolve(legacySafeNodeFileName(nodeId) + ".py").normalize();
	}

	private static Path priorReadableNodeSourcePath(Path folder, Map<String, Object> node) {
		Path nodesFolder = nodesFolder(folder);
		return nodesFolder.resolve(priorReadableNodeFileName(node) + ".py").normalize();
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

	private static String priorReadableNodeFileName(Map<String, Object> node) {
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
		return value.toLowerCase(Locale.ROOT)
				.replaceAll("[^a-z0-9]+", "_")
				.replaceAll("^_+|_+$", "");
	}

	private static String legacySafeNodeFileName(String nodeId) {
		return Base64.getUrlEncoder().withoutPadding()
				.encodeToString(nodeId.getBytes(StandardCharsets.UTF_8));
	}

	private static void validateUniqueNodeSourceFileNames(
			AutomationDefinitionValidator.ValidatedDefinition definition) {
		Map<String, String> nodeIdsByFileName = new LinkedHashMap<>();
		for (Map<String, Object> node : definition.nodes()) {
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
		Object nodeType = node.get(AutomationConstants.NODE_FIELD_TYPE);
		return !AutomationConstants.NODE_START.equals(nodeType)
				&& !AutomationConstants.NODE_CONTROL_IF.equals(nodeType);
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
	 * @param definition canonical graph JSON
	 * @param nodeSources immutable source map keyed by non-start node ID
	 */
	public record DefinitionFiles(String definition, Map<String, String> nodeSources) {
		public DefinitionFiles {
			nodeSources = Map.copyOf(nodeSources);
		}
	}
}
