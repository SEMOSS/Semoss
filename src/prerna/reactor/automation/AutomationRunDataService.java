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
 * -----------------------------------------------------------------------------
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

import java.io.File;
import java.util.Map;

import prerna.auth.User;
import prerna.ds.py.PyTranslator;
import prerna.om.Insight;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;
import prerna.util.AssetUtility;

/**
 * Resolves client-safe pages from data owned by an Automation execution.
 *
 * <p>
 * This service owns the complete server-side resolution boundary: project
 * permission, run/project binding, node membership, immutable execution owner,
 * live Insight lookup, and bounded provider paging. Reactors must not resolve an
 * opaque reference directly.
 */
final class AutomationRunDataService {

	private AutomationRunDataService() {
	}

	/**
	 * Returns one bounded page from a node's retained run data.
	 *
	 * @param user authenticated requesting user
	 * @param projectId requested Automation project
	 * @param runId durable run identifier
	 * @param nodeId node identifier inside the immutable run snapshot
	 * @param offset zero-based page offset
	 * @param limit maximum entries to return
	 * @return provider-neutral page map
	 */
	static Map<String, Object> getNodeDataPage(User user, String projectId, String runId, String nodeId, int offset,
			int limit) {
		String authorizedProjectId = AutomationProjectUtils.getViewableAutomationProject(user, projectId).getProjectId();
		Map<String, Object> run = AutomationDatabaseUtility.getRunDetail(runId);
		if (run == null || !authorizedProjectId.equals(run.get(AutomationConstants.PROJECT_ID))) {
			throw new IllegalArgumentException("Automation run was not found for this project.");
		}

		AutomationDataReference reference = referenceFrom(
				AutomationDatabaseUtility.getNodeOutputForRun(runId, nodeId));
		if (reference == null) {
			throw new IllegalArgumentException("This Automation node has no retained data to view.");
		}

		Insight executionInsight = AutomationRunExecutionService.getAvailableExecutionInsight(runId);
		if (executionInsight == null || !authorizedProjectId.equals(executionInsight.getProjectId())) {
			throw new IllegalStateException("Run data is no longer available. Run the automation again.");
		}
		AutomationDataOwner owner = AutomationRunExecutionService.dataOwner(authorizedProjectId, runId);
		AutomationRunDataRegistry.Entry entry = AutomationRunDataRegistry.require(executionInsight, reference, owner);
		if (entry.backing() == AutomationRunDataRegistry.Backing.TASK) {
			return AutomationTaskDataService.readPage(executionInsight, owner, reference, offset, limit).toMap();
		}
		if (entry.backing() != AutomationRunDataRegistry.Backing.RUN_MEMORY) {
			throw new IllegalStateException("Automation run data uses an unsupported backing.");
		}
		PyTranslator translator = executionInsight.getPyTranslator();
		if (translator == null) {
			throw new IllegalStateException("Python runtime is not available for this run.");
		}
		String assetsFolder = AssetUtility.getProjectAssetsFolder(authorizedProjectId);
		Object raw = translator.runScriptWithExplicitAssetPaths(executionInsight,
				AutomationRuntime.buildDataPageInvocationScript(reference, owner, offset, limit), assetsFolder,
				new String[] { assetsFolder + File.separator + "py" });
		Object result = AutomationRuntime.normalizeNodeResult(raw);
		return AutomationDataPage.fromValue(result).toMap();
	}

	static AutomationDataReference referenceFrom(Map<String, Object> nodeOutput) {
		if (nodeOutput == null || !(nodeOutput.get(AutomationConstants.OUTPUT_VALUE) instanceof String value)
				|| value.isBlank()) {
			return null;
		}
		try {
			return AutomationDataReference.fromValue(AutomationRuntimeUtils.GSON.fromJson(value, Object.class));
		} catch (RuntimeException ignored) {
			return null;
		}
	}

}
