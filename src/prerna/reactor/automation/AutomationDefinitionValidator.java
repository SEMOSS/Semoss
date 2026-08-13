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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;

import com.google.gson.JsonParseException;

import prerna.reactor.automation.nodes.IAutomationNodeExecutor;
import prerna.reactor.automation.utils.AutomationExecutionUtils;

/**
 * Validates automation definition documents and produces a stable run provenance snapshot.
 */
public final class AutomationDefinitionValidator {

	private AutomationDefinitionValidator() {}

	/**
	 * Parses and validates an automation JSON document.
	 *
	 * @param json automation definition JSON
	 * @return validated document metadata
	 */
	public static ValidatedDefinition parseAndValidate(String json) {
		if (json == null || json.isBlank()) {
			throw new IllegalArgumentException("Automation definition must be a nonblank JSON object.");
		}
		try {
			Map<String, Object> document = AutomationExecutionUtils.GSON.fromJson(json, AutomationExecutionUtils.MAP_TYPE);
			return validate(document);
		} catch (JsonParseException e) {
			throw new IllegalArgumentException("Automation definition must be valid JSON.", e);
		}
	}

	/**
	 * Validates a parsed automation document. The returned nodes are the document's node maps,
	 * allowing permitted runtime overrides to be revalidated and snapshotted without reparsing.
	 *
	 * @param document parsed automation definition
	 * @return validated document metadata
	 */
	public static ValidatedDefinition validate(Map<String, Object> document) {
		if (document == null) {
			throw new IllegalArgumentException("Automation definition must be a JSON object.");
		}

		int version = validateVersion(document.get(AutomationConstants.DOC_VERSION));
		Map<String, Object> graph = requireMap(document.get(AutomationConstants.DOC_GRAPH), "graph");
		List<Map<String, Object>> nodes = requireMapList(graph.get(AutomationConstants.DOC_NODES), "graph.nodes");
		List<Map<String, Object>> edges = requireMapList(graph.get(AutomationConstants.DOC_EDGES), "graph.edges");

		Set<String> nodeIds = validateNodes(nodes);
		validateEdgesAndDag(edges, nodeIds);

		String snapshot = toCanonicalJson(document);
		return new ValidatedDefinition(document, nodes, edges, version, snapshot, sha256(snapshot));
	}

	private static int validateVersion(Object value) {
		if (!(value instanceof Number)) {
			throw new IllegalArgumentException("Automation definition version must be " + AutomationConstants.DOC_CURRENT_VERSION + ".");
		}
		double number = ((Number) value).doubleValue();
		if (!Double.isFinite(number) || number != Math.rint(number)
				|| number != AutomationConstants.DOC_CURRENT_VERSION) {
			throw new IllegalArgumentException("Unsupported automation definition version: " + value + ".");
		}
		return (int) number;
	}

	private static Set<String> validateNodes(List<Map<String, Object>> nodes) {
		Set<String> nodeIds = new HashSet<>();
		int triggerCount = 0;
		for (int i = 0; i < nodes.size(); i++) {
			Map<String, Object> node = nodes.get(i);
			String nodeId = requireNonblankString(node.get(AutomationConstants.NODE_FIELD_ID),
					"graph.nodes[" + i + "].id");
			if (!nodeIds.add(nodeId)) {
				throw new IllegalArgumentException("Automation definition has duplicate node id: " + nodeId + ".");
			}

			String type = requireNonblankString(node.get(AutomationConstants.NODE_FIELD_TYPE),
					"graph.nodes[" + i + "].type");
			if (AutomationConstants.NODE_TRIGGER.equals(type)) {
				triggerCount++;
			} else if (!IAutomationNodeExecutor.EXECUTORS.containsKey(type)) {
				throw new IllegalArgumentException("Unsupported automation node type: " + type + ".");
			}
		}
		if (triggerCount != 1) {
			throw new IllegalArgumentException("Automation definition must contain exactly one trigger node.");
		}
		return nodeIds;
	}

	private static void validateEdgesAndDag(List<Map<String, Object>> edges, Set<String> nodeIds) {
		Map<String, List<String>> adjacency = new HashMap<>();
		Map<String, Integer> indegrees = new HashMap<>();
		for (String nodeId : nodeIds) {
			adjacency.put(nodeId, new ArrayList<>());
			indegrees.put(nodeId, 0);
		}

		for (int i = 0; i < edges.size(); i++) {
			Map<String, Object> edge = edges.get(i);
			String source = requireNonblankString(edge.get(AutomationConstants.EDGE_FIELD_SOURCE),
					"graph.edges[" + i + "].source");
			String target = requireNonblankString(edge.get(AutomationConstants.EDGE_FIELD_TARGET),
					"graph.edges[" + i + "].target");
			if (!nodeIds.contains(source) || !nodeIds.contains(target)) {
				throw new IllegalArgumentException("Automation edge references an unknown node: " + source + " -> " + target + ".");
			}
			if (source.equals(target)) {
				throw new IllegalArgumentException("Automation edge cannot reference the same source and target node: " + source + ".");
			}
			adjacency.get(source).add(target);
			indegrees.put(target, indegrees.get(target) + 1);
		}

		ArrayDeque<String> ready = new ArrayDeque<>();
		for (Map.Entry<String, Integer> entry : indegrees.entrySet()) {
			if (entry.getValue() == 0) {
				ready.add(entry.getKey());
			}
		}
		int visited = 0;
		while (!ready.isEmpty()) {
			String nodeId = ready.remove();
			visited++;
			for (String target : adjacency.get(nodeId)) {
				int remaining = indegrees.get(target) - 1;
				indegrees.put(target, remaining);
				if (remaining == 0) {
					ready.add(target);
				}
			}
		}
		if (visited != nodeIds.size()) {
			throw new IllegalArgumentException("Automation definition graph must be acyclic.");
		}
	}

	private static List<Map<String, Object>> topologicallyOrderNodes(List<Map<String, Object>> nodes,
			List<Map<String, Object>> edges) {
		Map<String, Map<String, Object>> nodesById = new LinkedHashMap<>();
		Map<String, List<String>> adjacency = new LinkedHashMap<>();
		Map<String, Integer> indegrees = new LinkedHashMap<>();
		Map<String, Integer> nodeIndexes = new HashMap<>();
		for (int index = 0; index < nodes.size(); index++) {
			Map<String, Object> node = nodes.get(index);
			String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
			nodesById.put(nodeId, node);
			adjacency.put(nodeId, new ArrayList<>());
			indegrees.put(nodeId, 0);
			nodeIndexes.put(nodeId, index);
		}
		for (Map<String, Object> edge : edges) {
			String source = (String) edge.get(AutomationConstants.EDGE_FIELD_SOURCE);
			String target = (String) edge.get(AutomationConstants.EDGE_FIELD_TARGET);
			adjacency.get(source).add(target);
			indegrees.put(target, indegrees.get(target) + 1);
		}

		PriorityQueue<String> ready = new PriorityQueue<>((left, right) ->
				Integer.compare(nodeIndexes.get(left), nodeIndexes.get(right)));
		for (String nodeId : nodesById.keySet()) {
			if (indegrees.get(nodeId) == 0) {
				ready.add(nodeId);
			}
		}

		List<Map<String, Object>> ordered = new ArrayList<>();
		while (!ready.isEmpty()) {
			String nodeId = ready.remove();
			ordered.add(nodesById.get(nodeId));
			for (String target : adjacency.get(nodeId)) {
				int remaining = indegrees.get(target) - 1;
				indegrees.put(target, remaining);
				if (remaining == 0) {
					ready.add(target);
				}
			}
		}
		return ordered;
	}

	private static Map<String, Object> requireMap(Object value, String field) {
		if (!(value instanceof Map<?, ?>)) {
			throw new IllegalArgumentException("Automation definition field '" + field + "' must be an object.");
		}
		for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
			if (!(entry.getKey() instanceof String)) {
				throw new IllegalArgumentException("Automation definition field '" + field + "' has a non-string key.");
			}
		}
		@SuppressWarnings("unchecked")
		Map<String, Object> source = (Map<String, Object>) value;
		return source;
	}

	private static List<Map<String, Object>> requireMapList(Object value, String field) {
		if (!(value instanceof List<?>)) {
			throw new IllegalArgumentException("Automation definition field '" + field + "' must be an array.");
		}
		List<Map<String, Object>> maps = new ArrayList<>();
		List<?> values = (List<?>) value;
		for (int i = 0; i < values.size(); i++) {
			maps.add(requireMap(values.get(i), field + "[" + i + "]"));
		}
		return maps;
	}

	private static String requireNonblankString(Object value, String field) {
		if (!(value instanceof String) || ((String) value).isBlank()) {
			throw new IllegalArgumentException("Automation definition field '" + field + "' must be a nonblank string.");
		}
		return (String) value;
	}

	private static String toCanonicalJson(Map<String, Object> document) {
		return AutomationExecutionUtils.GSON.toJson(canonicalize(document));
	}

	private static Object canonicalize(Object value) {
		if (value instanceof Map<?, ?>) {
			Map<String, Object> sorted = new TreeMap<>();
			for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
				if (!(entry.getKey() instanceof String)) {
					throw new IllegalArgumentException("Automation definition contains a non-string object key.");
				}
				sorted.put((String) entry.getKey(), canonicalize(entry.getValue()));
			}
			return sorted;
		}
		if (value instanceof List<?>) {
			List<Object> values = new ArrayList<>();
			for (Object element : (List<?>) value) {
				values.add(canonicalize(element));
			}
			return values;
		}
		return value;
	}

	private static String sha256(String value) {
		try {
			byte[] hash = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
			return java.util.HexFormat.of().formatHex(hash);
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException("SHA-256 is unavailable.", e);
		}
	}

	/**
	 * Immutable metadata for a validated document and its canonical execution snapshot.
	 */
	public static final class ValidatedDefinition {

		private final Map<String, Object> document;
		private final List<Map<String, Object>> nodes;
		private final List<Map<String, Object>> edges;
		private final int version;
		private final String snapshot;
		private final String hash;

		private ValidatedDefinition(Map<String, Object> document, List<Map<String, Object>> nodes,
				List<Map<String, Object>> edges,
				int version, String snapshot, String hash) {
			this.document = document;
			this.nodes = nodes;
			this.edges = edges;
			this.version = version;
			this.snapshot = snapshot;
			this.hash = hash;
		}

		public Map<String, Object> getDocument() {
			return document;
		}

		public List<Map<String, Object>> getNodes() {
			return nodes;
		}

		/**
		 * Returns nodes in dependency order. Nodes that become ready together retain their
		 * persisted document order, keeping shared-scope execution deterministic.
		 */
		public List<Map<String, Object>> getExecutionOrder() {
			return topologicallyOrderNodes(nodes, edges);
		}

		public int getVersion() {
			return version;
		}

		public String getSnapshot() {
			return snapshot;
		}

		public String getHash() {
			return hash;
		}
	}
}
