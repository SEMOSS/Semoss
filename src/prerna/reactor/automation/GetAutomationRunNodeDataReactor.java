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
 ******************************************************************************/
package prerna.reactor.automation;

import java.io.File;
import java.util.Map;

import prerna.ds.py.PyTranslator;
import prerna.om.Insight;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

/**
 * Returns one bounded page from a node value retained by an Automation run's
 * execution Insight.
 *
 * <p>
 * Pixel: {@code GetAutomationRunNodeData(project=["appId"], runId=["uuid"],
 * nodeId=["node"], offset=["0"], limit=["50"])}
 *
 * <p>
 * The opaque reference never crosses the client boundary. Project permission,
 * run ownership, node membership, and execution-Insight ownership are all
 * re-established for every page request.
 */
public class GetAutomationRunNodeDataReactor extends AbstractReactor {

	private static final String RUN_ID_KEY = "runId";
	private static final String NODE_ID_KEY = "nodeId";

	public GetAutomationRunNodeDataReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), RUN_ID_KEY, NODE_ID_KEY,
				ReactorKeysEnum.OFFSET.getKey(), ReactorKeysEnum.LIMIT.getKey() };
		this.keyRequired = new int[] { 1, 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = requiredValue(ReactorKeysEnum.PROJECT.getKey(), "project id");
		String runId = requiredValue(RUN_ID_KEY, "run id");
		String nodeId = requiredValue(NODE_ID_KEY, "node id");
		int offset = integerValue(ReactorKeysEnum.OFFSET.getKey(), 0, 0, Integer.MAX_VALUE);
		int limit = integerValue(ReactorKeysEnum.LIMIT.getKey(), AutomationConstants.DEFAULT_DATA_PAGE_LIMIT, 1,
				AutomationConstants.MAX_DATA_PAGE_LIMIT);

		projectId = AutomationProjectUtils.getViewableAutomationProject(this.insight.getUser(), projectId).getProjectId();
		Map<String, Object> run = AutomationDatabaseUtility.getRunDetail(runId);
		if (run == null || !projectId.equals(run.get(AutomationConstants.PROJECT_ID))) {
			throw new IllegalArgumentException("Automation run was not found for this project.");
		}

		Map<String, Object> nodeOutput = AutomationDatabaseUtility.getNodeOutputForRun(runId, nodeId);
		AutomationDataReference reference = referenceFrom(nodeOutput);
		if (reference == null) {
			throw new IllegalArgumentException("This Automation node has no retained data to view.");
		}

		Insight executionInsight = AutomationRunExecutionService.getAvailableExecutionInsight(runId);
		if (executionInsight == null || !projectId.equals(executionInsight.getProjectId())) {
			throw new IllegalStateException("Run data is no longer available. Run the automation again.");
		}
		PyTranslator translator = executionInsight.getPyTranslator();
		if (translator == null) {
			throw new IllegalStateException("Python runtime is not available for this run.");
		}

		Object createdByValue = run.get(AutomationConstants.CREATED_BY);
		String createdBy = createdByValue == null || createdByValue.toString().isBlank()
				? AutomationConstants.SYSTEM_USER_ID
				: createdByValue.toString();
		Map<String, String> owner = Map.of("projectId", projectId, "runId", runId, "userId", createdBy);
		String assetsFolder = AssetUtility.getProjectAssetsFolder(projectId);
		Object raw = translator.runScriptWithExplicitAssetPaths(executionInsight,
				AutomationRuntime.buildDataPageInvocationScript(reference, owner, offset, limit), assetsFolder,
				new String[] { assetsFolder + File.separator + "py" });
		Object result = AutomationRuntime.normalizeNodeResult(raw);
		if (!(result instanceof Map<?, ?>)) {
			throw new IllegalStateException("Automation run data returned an invalid page.");
		}
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private static AutomationDataReference referenceFrom(Map<String, Object> nodeOutput) {
		if (nodeOutput == null || !(nodeOutput.get(AutomationConstants.OUTPUT_VALUE) instanceof String value)
				|| value.isBlank()) {
			return null;
		}
		try {
			return AutomationDataReference.fromValue(
					prerna.reactor.automation.utils.AutomationRuntimeUtils.GSON.fromJson(value, Object.class));
		} catch (RuntimeException ignored) {
			return null;
		}
	}

	private String requiredValue(String key, String label) {
		String value = this.keyValue.get(key);
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException("Must provide a " + label);
		}
		return value;
	}

	private int integerValue(String key, int defaultValue, int minimum, int maximum) {
		String value = this.keyValue.get(key);
		if (value == null || value.isBlank()) {
			return defaultValue;
		}
		try {
			int parsed = Integer.parseInt(value);
			if (parsed < minimum || parsed > maximum) {
				throw new IllegalArgumentException(key + " must be between " + minimum + " and " + maximum + ".");
			}
			return parsed;
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException(key + " must be an integer.", e);
		}
	}

	@Override
	public String getReactorDescription() {
		return "Returns a bounded page from data retained by one Automation run node.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (RUN_ID_KEY.equals(key)) {
			return "Run identifier returned when the automation was triggered.";
		}
		if (NODE_ID_KEY.equals(key)) {
			return "Node identifier from the immutable run definition.";
		}
		return super.getDescriptionForKey(key);
	}
}
