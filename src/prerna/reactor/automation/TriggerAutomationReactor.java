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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.om.ThreadStore;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Manually triggers an automation run for a project. Validates access, claims the single-run slot,
 * submits execution to a background thread pool, and returns the run ID immediately for polling.
 *
 * <p>Pixel: {@code TriggerAutomation(project=["appId"])}
 */
public class TriggerAutomationReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(TriggerAutomationReactor.class);

	private static final ExecutorService AUTOMATION_EXECUTOR = new ThreadPoolExecutor(
			2, 20, 60L, TimeUnit.SECONDS,
			new LinkedBlockingQueue<>(10),
			r -> {
				Thread t = new Thread(r, "automation-run-" + System.nanoTime());
				t.setDaemon(true);
				return t;
			},
			new ThreadPoolExecutor.AbortPolicy()
	);

	public TriggerAutomationReactor() {
		this.keysToGet = new String[] { "project" };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = getProjectId();
		String userId = getUserId();
		String runId = UUID.randomUUID().toString();

		if (!AutomationDatabaseUtility.claimActiveRun(projectId, runId)) {
			String activeRun = AutomationDatabaseUtility.getActiveRun(projectId);
			throw new IllegalArgumentException(
					"Automation already has an active run: " + activeRun +
					". Wait for it to complete or cancel it before starting a new run.");
		}

		try {
			Map<String, Object> doc = AutomationExecutionUtils.loadAutomationDoc(projectId);
			@SuppressWarnings("unchecked")
			Map<String, Object> graph = (Map<String, Object>) doc.get("graph");
			@SuppressWarnings("unchecked")
			List<Map<String, Object>> nodes = (List<Map<String, Object>>) graph.get("nodes");
			@SuppressWarnings("unchecked")
			List<Map<String, Object>> edges = (List<Map<String, Object>>) graph.get("edges");
			Map<String, String> configMap = AutomationExecutionUtils.loadConfig(projectId);

			// The form view always produces a sequential node list with no edges, so topoSort
			// degrades to node-list order. Edges + sort are kept for a future visual flow editor.
			List<Map<String, Object>> ordered = AutomationExecutionUtils.topoSort(nodes, edges);
			if (ordered.isEmpty()) {
				throw new IllegalArgumentException("Automation has no nodes to execute");
			}

			AutomationDatabaseUtility.insertRun(runId, projectId, AutomationConstants.DEFAULT_AUTOMATION_ID,
					AutomationConstants.TRIGGER_MANUAL, ordered.size(), userId);
			AutomationDatabaseUtility.insertAllNodeOutputs(runId, ordered);

			Map<String, Object> parentContext = ThreadStore.getTheadMapObject();
			final Map<String, Object> contextSnapshot =
					parentContext != null ? new HashMap<>(parentContext) : null;

			try {
				AUTOMATION_EXECUTOR.submit(() -> {
					installThreadContext(contextSnapshot);
					try {
						AutomationRunEngine.run(runId, projectId, ordered, configMap, this.insight);
					} catch (Exception e) {
						classLogger.error("Unhandled error in automation run {}: {}", runId, e.getMessage(), e);
						AutomationDatabaseUtility.updateRunStatus(runId,
								AutomationConstants.STATUS_FAILED, null, e.getMessage());
					} finally {
						ThreadStore.remove();
					}
				});
			} catch (RejectedExecutionException e) {
				AutomationDatabaseUtility.updateRunStatus(runId, AutomationConstants.STATUS_FAILED,
						null, "Server is at capacity - too many concurrent automation runs");
				throw new IllegalStateException("Too many concurrent automation runs. Please try again shortly.");
			}

			classLogger.info("Automation run {} submitted for project {}", runId, projectId);

			Map<String, Object> stored = AutomationDatabaseUtility.getRunDetail(runId);
			Map<String, Object> result = new HashMap<>();
			result.put(AutomationConstants.RUN_ID, runId);
			result.put(AutomationConstants.PROJECT_ID, projectId);
			result.put(AutomationConstants.STATUS, AutomationConstants.STATUS_RUNNING);
			result.put(AutomationConstants.TOTAL_NODES, ordered.size());
			result.put(AutomationConstants.COMPLETED_NODES, 0);
			if (stored != null) {
				result.put(AutomationConstants.STARTED_AT, stored.get(AutomationConstants.STARTED_AT));
			}
			result.put("nodeResults", new ArrayList<>());
			return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);

		} catch (RuntimeException e) {
			AutomationDatabaseUtility.releaseActiveRun(projectId, runId);
			throw e;
		}
	}

	// -- Helpers -------------------------------------------------------------------

	private static void installThreadContext(Map<String, Object> snapshot) {
		if (snapshot == null || snapshot.isEmpty()) return;
		ThreadStore.getInsightId();
		ThreadStore.setThreadMapObject(snapshot);
	}

	private String getProjectId() {
		String projectId = this.keyValue.get(this.keysToGet[0]);
		if (projectId == null || projectId.isEmpty()) {
			throw new IllegalArgumentException("Must provide a project id");
		}
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if (!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access");
		}
		return projectId;
	}

	private String getUserId() {
		if (this.insight.getUser() != null && this.insight.getUser().getPrimaryLoginToken() != null) {
			return this.insight.getUser().getPrimaryLoginToken().getId();
		}
		return "system";
	}

	@Override
	public String getReactorDescription() {
		return "Manually triggers an automation run for the given project and returns a run ID for polling.";
	}
}
