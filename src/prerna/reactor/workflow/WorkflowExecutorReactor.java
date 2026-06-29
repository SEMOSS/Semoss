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
package prerna.reactor.workflow;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

/**
 * WorkflowExecutor(project=["<appId>"])
 *
 * Loads workflow.json, executes each node's pixel in topological order.
 * Each node's output is stored as a named variable; downstream nodes can
 * reference upstream outputs as ${nodeId} in their pixel/config strings.
 *
 * Returns a run summary: per-node status, output snippet, timing, and errors.
 *
 * Critical rule: always use this.insight.runPixel(pixel).getResults() —
 * never a static PixelRunner.
 */
public class WorkflowExecutorReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(WorkflowExecutorReactor.class);
	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();
	private static final Pattern VAR_PATTERN = Pattern.compile("\\$\\{([^}]+)\\}");

	public WorkflowExecutorReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	@SuppressWarnings("unchecked")
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String projectId = this.keyValue.get(this.keysToGet[0]);

		if (!SecurityProjectUtils.userCanViewProject(user, projectId)) {
			NounMetadata noun = new NounMetadata(
				"User does not have permission to run workflow in project " + projectId,
				PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException ex = new SemossPixelException(noun);
			ex.setContinueThreadOfExecution(false);
			throw ex;
		}

		// Load workflow JSON
		String portalsFolder = AssetUtility.getProjectPortalsFolder(projectId);
		File workflowFile = new File(portalsFolder + "/workflow.json");

		if (!workflowFile.exists()) {
			NounMetadata noun = new NounMetadata(
				"No workflow.json found for project " + projectId,
				PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException ex = new SemossPixelException(noun);
			ex.setContinueThreadOfExecution(false);
			throw ex;
		}

		Map<String, Object> workflowDoc;
		try {
			String json = FileUtils.readFileToString(workflowFile, StandardCharsets.UTF_8);
			workflowDoc = GSON.fromJson(json, Map.class);
		} catch (IOException | com.google.gson.JsonSyntaxException e) {
			classLogger.error("Failed to parse workflow.json for project " + projectId, e);
			NounMetadata noun = new NounMetadata(
				"Failed to parse workflow: " + e.getMessage(),
				PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException ex = new SemossPixelException(noun);
			ex.setContinueThreadOfExecution(false);
			throw ex;
		}

		List<Map<String, Object>> nodes = (List<Map<String, Object>>) workflowDoc.getOrDefault("nodes", new ArrayList<>());
		List<Map<String, Object>> edges = (List<Map<String, Object>>) workflowDoc.getOrDefault("edges", new ArrayList<>());

		// Build adjacency maps for topological sort (Kahn's algorithm)
		Map<String, List<String>> downstream = new LinkedHashMap<>(); // nodeId -> list of downstream nodeIds
		Map<String, Integer> inDegree = new LinkedHashMap<>();

		for (Map<String, Object> node : nodes) {
			String id = (String) node.get("id");
			downstream.put(id, new ArrayList<>());
			inDegree.put(id, 0);
		}

		for (Map<String, Object> edge : edges) {
			String source = (String) edge.get("source");
			String target = (String) edge.get("target");
			if (source != null && target != null && downstream.containsKey(source)) {
				downstream.get(source).add(target);
				inDegree.merge(target, 1, Integer::sum);
			}
		}

		// Kahn's topological sort
		Queue<String> queue = new LinkedList<>();
		for (Map.Entry<String, Integer> entry : inDegree.entrySet()) {
			if (entry.getValue() == 0) {
				queue.add(entry.getKey());
			}
		}

		List<String> execOrder = new ArrayList<>();
		Set<String> visited = new HashSet<>();
		while (!queue.isEmpty()) {
			String cur = queue.poll();
			execOrder.add(cur);
			visited.add(cur);
			for (String next : downstream.getOrDefault(cur, new ArrayList<>())) {
				int deg = inDegree.merge(next, -1, Integer::sum);
				if (deg == 0 && !visited.contains(next)) {
					queue.add(next);
				}
			}
		}

		// Index nodes by id
		Map<String, Map<String, Object>> nodeById = new LinkedHashMap<>();
		for (Map<String, Object> node : nodes) {
			nodeById.put((String) node.get("id"), node);
		}

		// Execute nodes in order, collecting outputs
		Map<String, String> outputs = new LinkedHashMap<>(); // nodeId -> string output
		String runId = new SimpleDateFormat("yyyyMMdd-HHmmss-SSS").format(new Date());
		List<Map<String, Object>> nodeResults = new ArrayList<>();

		for (String nodeId : execOrder) {
			Map<String, Object> node = nodeById.get(nodeId);
			if (node == null) continue;

			String nodeType = (String) node.get("type");
			Map<String, Object> config = (Map<String, Object>) node.getOrDefault("config", new HashMap<>());

			// Skip trigger nodes — they just start the flow
			if ("trigger".equals(nodeType)) {
				outputs.put(nodeId, "triggered");
				recordResult(nodeResults, nodeId, nodeType, "skipped", "triggered", null, 0);
				continue;
			}

			String pixel = buildPixel(nodeType, config, outputs);
			if (pixel == null || pixel.trim().isEmpty()) {
				outputs.put(nodeId, "");
				recordResult(nodeResults, nodeId, nodeType, "skipped", "no pixel", null, 0);
				continue;
			}

			long start = System.currentTimeMillis();
			try {
				// THE correct pattern — never static PixelRunner
				List<NounMetadata> results = this.insight.runPixel(pixel).getResults();
				long elapsed = System.currentTimeMillis() - start;

				String outputStr = resultsToString(results);
				outputs.put(nodeId, outputStr);
				recordResult(nodeResults, nodeId, nodeType, "success", outputStr, null, elapsed);

			} catch (Exception e) {
				long elapsed = System.currentTimeMillis() - start;
				classLogger.warn("Workflow node " + nodeId + " failed: " + e.getMessage());
				outputs.put(nodeId, "");
				recordResult(nodeResults, nodeId, nodeType, "error", null, e.getMessage(), elapsed);
			}
		}

		// Build and persist run summary
		Map<String, Object> runSummary = new LinkedHashMap<>();
		runSummary.put("runId", runId);
		runSummary.put("projectId", projectId);
		runSummary.put("startedAt", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date()));
		runSummary.put("nodeResults", nodeResults);
		runSummary.put("status", nodeResults.stream().anyMatch(r -> "error".equals(r.get("status"))) ? "error" : "success");

		persistRunSummary(portalsFolder, runId, runSummary);

		return new NounMetadata(runSummary, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

	/**
	 * Build the executable pixel string for a node, substituting ${varName} references.
	 */
	private String buildPixel(String nodeType, Map<String, Object> config, Map<String, String> outputs) {
		String pixel = null;

		switch (nodeType) {
			case "custom-pixel":
				pixel = getString(config, "pixel");
				break;

			case "database-engine": {
				String engineId = getString(config, "engineId");
				String query = getString(config, "expression");
				if (engineId != null && query != null) {
					pixel = "DatabaseQuery(engine=[\"" + engineId + "\"], query=[\"" + escape(query) + "\"]);";
				}
				break;
			}

			case "model-engine": {
				String engineId = getString(config, "engineId");
				String prompt = getString(config, "promptTemplate");
				if (engineId != null && prompt != null) {
					pixel = "LLMChat(engine=[\"" + engineId + "\"], command=[\"" + escape(prompt) + "\"]);";
				}
				break;
			}

			case "storage-engine": {
				String engineId = getString(config, "engineId");
				String operation = getString(config, "operation");
				String path = getString(config, "path");
				if (engineId != null && operation != null) {
					if ("list".equals(operation)) {
						pixel = "StorageList(engine=[\"" + engineId + "\"], path=[\"" + (path != null ? path : "/") + "\"]);";
					} else if ("read".equals(operation) && path != null) {
						pixel = "StorageGet(engine=[\"" + engineId + "\"], path=[\"" + escape(path) + "\"]);";
					} else {
						pixel = "StorageList(engine=[\"" + engineId + "\"]);";
					}
				}
				break;
			}

			case "vector-engine": {
				String engineId = getString(config, "engineId");
				String operation = getString(config, "operation");
				String expression = getString(config, "expression");
				if (engineId != null && "query".equals(operation) && expression != null) {
					pixel = "VectorDatabaseQuery(engine=[\"" + engineId + "\"], command=[\"" + escape(expression) + "\"]);";
				} else if (engineId != null) {
					pixel = "VectorDatabaseQuery(engine=[\"" + engineId + "\"], command=[\"query\"]);";
				}
				break;
			}

			case "function-engine": {
				String engineId = getString(config, "engineId");
				String params = getString(config, "paramsExpression");
				if (engineId != null) {
					pixel = "FunctionEngine(engine=[\"" + engineId + "\"]"
						+ (params != null ? ", parameters=[" + params + "]" : "") + ");";
				}
				break;
			}

			case "transform": {
				String expression = getString(config, "expression");
				if (expression != null) {
					pixel = expression;
				}
				break;
			}

			default:
				return null;
		}

		if (pixel == null) return null;
		return substituteVars(pixel, outputs);
	}

	/** Replace ${nodeId} tokens with the actual output from that node. */
	private String substituteVars(String template, Map<String, String> outputs) {
		Matcher m = VAR_PATTERN.matcher(template);
		StringBuffer sb = new StringBuffer();
		while (m.find()) {
			String varName = m.group(1);
			String replacement = outputs.getOrDefault(varName, "");
			m.appendReplacement(sb, Matcher.quoteReplacement(replacement));
		}
		m.appendTail(sb);
		return sb.toString();
	}

	private String getString(Map<String, Object> map, String key) {
		Object val = map.get(key);
		return val != null ? val.toString() : null;
	}

	private String escape(String s) {
		return s.replace("\\", "\\\\").replace("\"", "\\\"");
	}

	private String resultsToString(List<NounMetadata> results) {
		if (results == null || results.isEmpty()) return "";
		Object val = results.get(0).getValue();
		if (val == null) return "";
		if (val instanceof String) return (String) val;
		return GSON.toJson(val);
	}

	private void recordResult(List<Map<String, Object>> nodeResults, String nodeId, String nodeType,
			String status, String output, String error, long elapsedMs) {
		Map<String, Object> r = new LinkedHashMap<>();
		r.put("nodeId", nodeId);
		r.put("nodeType", nodeType);
		r.put("status", status);
		if (output != null) {
			r.put("output", output.length() > 2000 ? output.substring(0, 2000) + "..." : output);
		}
		if (error != null) r.put("error", error);
		r.put("elapsedMs", elapsedMs);
		nodeResults.add(r);
	}

	private void persistRunSummary(String portalsFolder, String runId, Map<String, Object> summary) {
		File runsDir = new File(portalsFolder + "/runs");
		if (!runsDir.exists()) runsDir.mkdirs();
		File runFile = new File(runsDir, runId + ".json");
		try {
			FileUtils.writeStringToFile(runFile, GSON.toJson(summary), StandardCharsets.UTF_8);
		} catch (IOException e) {
			classLogger.warn("Could not persist run summary " + runId + ": " + e.getMessage());
		}
	}
}
