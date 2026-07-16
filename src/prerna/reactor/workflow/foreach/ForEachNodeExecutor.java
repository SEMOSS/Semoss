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
package prerna.reactor.workflow.foreach;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.om.Insight;
import prerna.reactor.workflow.PixelExecutionUtils;
import prerna.reactor.workflow.WorkflowConstants;
import prerna.reactor.workflow.WorkflowDatabaseUtility;
import prerna.reactor.workflow.WorkflowDatabaseUtility.ForEachRowResult;
import prerna.reactor.workflow.WorkflowExecutionUtils;
import prerna.reactor.workflow.nodes.IWorkflowNodeExecutor;
import prerna.reactor.workflow.nodes.WorkflowNodeContext;

/**
 * Executes a for-each node: iterates over rows of a prior node's output,
 * running inner nodes per row with row-level error handling.
 *
 * <p>Key behaviors:
 * <ul>
 *   <li>Row failures are logged but do NOT stop the batch</li>
 *   <li>Results are batch-inserted into WORKFLOW_FOREACH_ROWS every 100 rows</li>
 *   <li>Heartbeat is updated periodically during batch processing</li>
 *   <li>Supports row-level resume (skips already-processed rows from a prior interrupted run)</li>
 *   <li>Returns aggregate result: {processed, succeeded, failed}</li>
 * </ul>
 *
 * <p>Resolved via the {@code IWorkflowNodeExecutor} registry in
 * {@link prerna.reactor.workflow.TriggerWorkflowReactor} for nodes with
 * {@code "type": "for-each"}. Note: the aggregate result's {@code totalRows} entry is read by
 * {@code executeSingleNode} to populate the node checkpoint's {@code ROW_COUNT} column - this is
 * the one node type that populates it.
 */
public final class ForEachNodeExecutor implements IWorkflowNodeExecutor {

	private static final Logger classLogger = LogManager.getLogger(ForEachNodeExecutor.class);

	/**
	 * Execute a for-each node.
	 *
	 * @param ctx the node's execution context - {@code runId}, {@code node}, {@code scope},
	 *            {@code configMap}, {@code insight}, and {@code cancelFlag} are used;
	 *            {@code nodeDispatcher}/{@code childWorkflowRunner} are not needed by this node
	 *            type (inner nodes run their compiled Pixel directly, not through the top-level
	 *            dispatcher)
	 * @return aggregate result map with keys: processed, succeeded, failed, totalRows
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Map<String, Object> execute(WorkflowNodeContext ctx) {
		Insight insight = ctx.insight();
		Map<String, Object> node = ctx.node();
		Map<String, String> scope = ctx.scope();
		Map<String, String> configMap = ctx.configMap();
		String runId = ctx.runId();
		AtomicBoolean cancelled = ctx.cancelFlag();

		String nodeId = ctx.nodeId();
		Map<String, Object> config = (Map<String, Object>) node.get("config");
		if (config == null) {
			throw new IllegalStateException("For-each node \"" + node.get("label") + "\" has no config");
		}

		String sourceVar = (String) config.get("sourceVar");
		String iteratorVar = config.containsKey("iteratorVar") ? (String) config.get("iteratorVar") : "row";
		String rowKeyField = (String) config.get("rowKeyField");
		List<Map<String, Object>> innerNodes = (List<Map<String, Object>>) config.get("nodes");

		if (sourceVar == null || sourceVar.isEmpty()) {
			throw new IllegalStateException("For-each node must specify a sourceVar");
		}
		if (innerNodes == null || innerNodes.isEmpty()) {
			throw new IllegalStateException("For-each node must have at least one inner node");
		}

		// Parse source array from scope
		List<Map<String, Object>> rows = parseSourceArray(scope.get(sourceVar), sourceVar);
		int totalRows = rows.size();

		// Check for row-level resume (skip already-processed rows)
		int resumeFromIndex = getResumeStartIndex(runId, nodeId);

		int succeeded = 0;
		int failed = 0;
		int timeoutSeconds = WorkflowExecutionUtils.getNodeTimeout(node);
		List<ForEachRowResult> batchBuffer = new ArrayList<>();
		long lastHeartbeat = System.currentTimeMillis();

		classLogger.info("For-each node {} starting: {} total rows, resuming from index {}",
				nodeId, totalRows, resumeFromIndex);

		for (int i = 0; i < totalRows; i++) {
			// Check cancellation
			if (cancelled != null && cancelled.get()) {
				classLogger.info("For-each node {} cancelled at row {}", nodeId, i);
				break;
			}

			// Skip already-processed rows (resume support)
			if (i <= resumeFromIndex) {
				continue;
			}

			Map<String, Object> row = rows.get(i);
			String rowKey = extractRowKey(row, rowKeyField, i);
			long rowStart = System.currentTimeMillis();

			try {
				executeRowInnerNodes(insight, innerNodes, row, iteratorVar, scope, configMap, timeoutSeconds);
				succeeded++;
				batchBuffer.add(new ForEachRowResult(i, rowKey,
						WorkflowConstants.NODE_STATUS_SUCCESS, null, rowStart));
			} catch (Exception e) {
				failed++;
				String errorMsg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
				batchBuffer.add(new ForEachRowResult(i, rowKey,
						WorkflowConstants.NODE_STATUS_FAILED, errorMsg, rowStart));
				classLogger.warn("For-each row {} ({}) failed: {}", i, rowKey, errorMsg);
			}

			// Batch flush every FOREACH_BATCH_SIZE rows
			if (batchBuffer.size() >= WorkflowConstants.FOREACH_BATCH_SIZE) {
				WorkflowDatabaseUtility.insertForEachRowsBatch(runId, nodeId, batchBuffer);
				batchBuffer.clear();
			}

			// Update heartbeat every 30 seconds (timestamp only - node count is tracked at workflow level)
			if (System.currentTimeMillis() - lastHeartbeat > WorkflowConstants.HEARTBEAT_INTERVAL_SECONDS * 1000L) {
				WorkflowDatabaseUtility.touchHeartbeat(runId);
				lastHeartbeat = System.currentTimeMillis();
			}
		}

		// Flush remaining rows
		if (!batchBuffer.isEmpty()) {
			WorkflowDatabaseUtility.insertForEachRowsBatch(runId, nodeId, batchBuffer);
		}

		int processed = succeeded + failed;
		classLogger.info("For-each node {} complete: processed={}, succeeded={}, failed={}",
				nodeId, processed, succeeded, failed);

		Map<String, Object> result = new HashMap<>();
		result.put("processed", processed);
		result.put("succeeded", succeeded);
		result.put("failed", failed);
		result.put("totalRows", totalRows);
		return result;
	}

	// -- Inner Node Execution ------------------------------------------------------

	private static void executeRowInnerNodes(Insight insight, List<Map<String, Object>> innerNodes,
			Map<String, Object> row, String iteratorVar, Map<String, String> parentScope,
			Map<String, String> configMap, int timeoutSeconds) {

		// Build sub-scope: parent scope + ${iteratorVar.field} for each field in the row
		Map<String, String> subScope = new HashMap<>(parentScope);
		for (Map.Entry<String, Object> entry : row.entrySet()) {
			String key = iteratorVar + "." + entry.getKey();
			String value = entry.getValue() != null ? String.valueOf(entry.getValue()) : "";
			subScope.put(key, value);
		}
		// Also add the full row as JSON under the iterator variable name
		subScope.put(iteratorVar, WorkflowExecutionUtils.GSON.toJson(row));

		// Run inner nodes sequentially
		for (Map<String, Object> innerNode : innerNodes) {
			String builtPixel = (String) innerNode.get("builtPixel");
			if (builtPixel == null || builtPixel.isBlank()) {
				continue;
			}

			String resolved = WorkflowExecutionUtils.resolve(builtPixel, subScope, configMap);
			Object result = PixelExecutionUtils.runAndCollect(insight, resolved, timeoutSeconds);

			// Store inner node output in sub-scope for downstream inner nodes
			String outputVar = (String) innerNode.get("outputVar");
			if (outputVar != null && !outputVar.isEmpty()) {
				subScope.put(outputVar, PixelExecutionUtils.serializeResult(result));
			}
		}
	}

	// -- Helpers -------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> parseSourceArray(String sourceJson, String sourceVar) {
		if (sourceJson == null || sourceJson.isBlank()) {
			throw new IllegalStateException(
					"For-each source variable ${" + sourceVar + "} is empty. " +
					"Ensure the upstream node produces output before the for-each node runs.");
		}

		try {
			Object parsed = WorkflowExecutionUtils.GSON.fromJson(sourceJson, Object.class);
			if (parsed instanceof List) {
				List<?> list = (List<?>) parsed;
				List<Map<String, Object>> result = new ArrayList<>();
				for (Object item : list) {
					if (item instanceof Map) {
						result.add((Map<String, Object>) item);
					} else {
						// Wrap primitives in a single-key map
						Map<String, Object> wrapper = new HashMap<>();
						wrapper.put("value", item);
						result.add(wrapper);
					}
				}
				return result;
			}
			throw new IllegalStateException(
					"For-each source variable ${" + sourceVar + "} is not an array. " +
					"Use the 'rows-as-objects' output transform on the upstream node.");
		} catch (Exception e) {
			if (e instanceof IllegalStateException) throw (IllegalStateException) e;
			throw new IllegalStateException(
					"Failed to parse for-each source ${" + sourceVar + "}: " + e.getMessage(), e);
		}
	}

	private static String extractRowKey(Map<String, Object> row, String rowKeyField, int index) {
		if (rowKeyField != null && !rowKeyField.isEmpty() && row.containsKey(rowKeyField)) {
			Object val = row.get(rowKeyField);
			return val != null ? val.toString() : String.valueOf(index);
		}
		return String.valueOf(index);
	}

	private static int getResumeStartIndex(String runId, String nodeId) {
		int lastProcessed = WorkflowDatabaseUtility.getForEachLastProcessedIndex(runId, nodeId);
		return lastProcessed; // -1 means start from beginning
	}
}
