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
 ******************************************************************************/
package prerna.reactor.automation;

import java.io.File;
import java.util.Map;

import prerna.ds.py.PyTranslator;
import prerna.om.Insight;
import prerna.reactor.AbstractReactor;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

/**
 * Returns one bounded page from data retained by an Automation execution
 * Insight.
 *
 * <p>
 * Run Details supplies project, run, and node identifiers. A Python node in the
 * owning execution Insight may instead supply the opaque reference ID directly.
 * The latter path is rejected from every other Insight.
 */
public class GetAutomationRunNodeDataReactor extends AbstractReactor {

	private static final String RUN_ID_KEY = "runId";
	private static final String NODE_ID_KEY = "nodeId";
	private static final String REFERENCE_ID_KEY = "referenceId";

	public GetAutomationRunNodeDataReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), RUN_ID_KEY, NODE_ID_KEY, REFERENCE_ID_KEY,
				ReactorKeysEnum.OFFSET.getKey(), ReactorKeysEnum.LIMIT.getKey() };
		this.keyRequired = new int[] { 0, 1, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String runId = required(RUN_ID_KEY, "run id");
		boolean internalReference = isInternalReference(runId);
		int offset = integerValue(ReactorKeysEnum.OFFSET.getKey(), 0, 0, Integer.MAX_VALUE);
		int maximumLimit = internalReference ? AutomationConstants.INTERNAL_DATA_PAGE_LIMIT
				: AutomationConstants.MAX_DATA_PAGE_LIMIT;
		int limit = integerValue(ReactorKeysEnum.LIMIT.getKey(), AutomationConstants.DEFAULT_DATA_PAGE_LIMIT, 1,
				maximumLimit);

		ResolvedData resolved = resolve(runId, internalReference);
		Map<String, Object> page;
		if (AutomationRunData.isTaskBacked(resolved.insight(), runId, resolved.reference())) {
			page = AutomationRunData.readTaskPage(resolved.insight(), runId, resolved.reference(), offset, limit);
		} else {
			page = readPythonPage(resolved.insight(), resolved.reference(), offset, limit);
		}
		return new NounMetadata(page, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private boolean isInternalReference(String runId) {
		String referenceId = this.keyValue.get(REFERENCE_ID_KEY);
		return referenceId != null && !referenceId.isBlank()
				&& AutomationRunExecutionService.isExecutionInsight(this.insight, runId);
	}

	private ResolvedData resolve(String runId, boolean internalReference) {
		if (internalReference) {
			String referenceId = this.keyValue.get(REFERENCE_ID_KEY);
			return new ResolvedData(this.insight,
					new AutomationDataReference(AutomationDataReference.CURRENT_SCHEMA_VERSION, referenceId));
		}

		String projectId = required(ReactorKeysEnum.PROJECT.getKey(), "project id");
		String nodeId = required(NODE_ID_KEY, "node id");
		String authorizedProjectId = AutomationProjectUtils.getViewableAutomationProject(this.insight.getUser(), projectId)
				.getProjectId();
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
		return new ResolvedData(executionInsight, reference);
	}

	private Map<String, Object> readPythonPage(Insight executionInsight, AutomationDataReference reference, int offset,
			int limit) {
		PyTranslator translator = executionInsight.getPyTranslator();
		if (translator == null) {
			throw new IllegalStateException("Python runtime is not available for this run.");
		}
		String assetsFolder = AssetUtility.getProjectAssetsFolder(executionInsight.getProjectId());
		Object raw = translator.runScriptWithExplicitAssetPaths(executionInsight,
				AutomationRuntime.buildDataPageInvocationScript(reference, offset, limit), assetsFolder,
				new String[] { assetsFolder + File.separator + "py" });
		Object normalized = AutomationRuntime.normalizeNodeResult(raw);
		if (!(normalized instanceof Map<?, ?> rawPage)) {
			throw new IllegalStateException("Automation run data returned an invalid page.");
		}
		@SuppressWarnings("unchecked")
		Map<String, Object> page = (Map<String, Object>) rawPage;
		return page;
	}

	private AutomationDataReference referenceFrom(Map<String, Object> nodeOutput) {
		if (nodeOutput == null || !(nodeOutput.get(AutomationConstants.OUTPUT_VALUE) instanceof String value)
				|| value.isBlank()) {
			return null;
		}
		try {
			return AutomationDataReference.find(AutomationRuntimeUtils.GSON.fromJson(value, Object.class));
		} catch (RuntimeException ignored) {
			return null;
		}
	}

	private String required(String key, String label) {
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
			return "Automation run identifier that owns the data.";
		}
		if (NODE_ID_KEY.equals(key)) {
			return "Node identifier whose retained output should be read.";
		}
		if (REFERENCE_ID_KEY.equals(key)) {
			return "Internal retained-data identifier used only inside the owning execution workspace.";
		}
		return super.getDescriptionForKey(key);
	}

	private record ResolvedData(Insight insight, AutomationDataReference reference) {
	}
}
