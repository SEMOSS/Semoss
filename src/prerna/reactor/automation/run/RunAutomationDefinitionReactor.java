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
package prerna.reactor.automation.run;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import prerna.ds.py.PyUtils;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.reactor.AbstractReactor;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.definition.AutomationDefinitionService;
import prerna.reactor.automation.project.AutomationProjectService;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Runs inline definition JSON as the caller, without an Automation project. The
 * run is stored under the caller's own inline run scope, so only that login can
 * read, list, cancel, or resume it.
 *
 * <p>
 * Pixel: {@code RunAutomationDefinition(json=["<json>"], nodeSources=["<json-map>"], inputs=[{...}])}
 */
public class RunAutomationDefinitionReactor extends AbstractReactor {

	private static final String NODE_SOURCES_KEY = AutomationConstants.DOC_NODE_SOURCES;

	public RunAutomationDefinitionReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.JSON.getKey(), NODE_SOURCES_KEY,
				AutomationConstants.AUTOMATION_INPUTS_KEY, AutomationConstants.AUTOMATION_TRIGGER_TYPE_KEY };
		this.keyRequired = new int[] { 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String json = this.keyValue.get(ReactorKeysEnum.JSON.getKey());
		if (json == null || json.isBlank()) {
			throw new IllegalArgumentException("Must provide a nonblank automation definition.");
		}
		AutomationDefinitionService.DefinitionFiles files = AutomationDefinitionService.fromInline(
				AutomationRuntimeUtils.decodeBase64OrRaw(json), decodeNodeSources(this.keyValue.get(NODE_SOURCES_KEY)));
		if (AutomationDefinitionService.containsCallerCode(files)) {
			requireUserPythonEnabled();
		}
		@SuppressWarnings("unchecked")
		Map<String, Object> inputs = this.getMap(AutomationConstants.AUTOMATION_INPUTS_KEY);
		Map<String, Object> result = runDefinition(this.insight, UUID.randomUUID().toString(),
				AutomationConstants.DEFAULT_AUTOMATION_ID, files, inputs,
				this.keyValue.get(AutomationConstants.AUTOMATION_TRIGGER_TYPE_KEY), ThreadStore.getJobId());
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	/**
	 * Runs a definition synchronously as the Insight's user, under that user's
	 * inline run scope.
	 *
	 * <p>
	 * This does not apply the {@code Py()} switches that {@link #execute()} applies
	 * to caller-written Python. Call it only with a definition the server holds,
	 * never with one a caller supplied.
	 *
	 * @param insight      authenticated Insight whose user owns the run
	 * @param runId        new durable run identifier
	 * @param automationId logical automation stored in {@code AUTOMATION_ID}
	 * @param files        definition from {@link AutomationDefinitionService#fromInline}
	 * @param inputs       optional values overriding trigger globals
	 * @param triggerType  MANUAL (default), PLAYGROUND, or SCHEDULED
	 * @param streamJobId  live Pixel job that receives progress, or {@code null}
	 * @return persisted run detail and node results
	 */
	public static Map<String, Object> runDefinition(Insight insight, String runId, String automationId,
			AutomationDefinitionService.DefinitionFiles files, Map<String, Object> inputs, String triggerType,
			String streamJobId) {
		return AutomationRunExecutionService.start(insight, streamJobId, runId,
				AutomationProjectService.inlineRunScope(insight.getUser()), automationId, files, inputs, triggerType);
	}

	/**
	 * Requests cancellation of a run: the durable flag every pod polls, plus the
	 * same-pod fast path. The caller owns the access check.
	 *
	 * @param runId run to cancel
	 */
	public static void cancelRun(String runId) {
		AutomationRunStore.setCancelRequested(runId);
		AutomationRunRegistry.requestCancellation(runId);
	}

	/** Applies the same global switches as {@code Py()} to caller-written Python. */
	private static void requireUserPythonEnabled() {
		if (Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.DISABLE_TERMINAL))) {
			throw new IllegalArgumentException("Terminal and user code execution has been disabled.");
		}
		if (Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.DISABLE_PY_TERMINAL))) {
			throw new IllegalArgumentException("Python terminal has been disabled.");
		}
		if (!PyUtils.pyEnabled()) {
			throw new IllegalArgumentException("Python is not enabled to use the following command");
		}
	}

	private static Map<String, String> decodeNodeSources(String value) {
		if (value == null || value.isBlank()) {
			return null;
		}
		Map<String, String> sources = new LinkedHashMap<>();
		for (Map.Entry<String, Object> entry : AutomationRuntimeUtils.parseJsonObject(value, NODE_SOURCES_KEY)
				.entrySet()) {
			if (!(entry.getValue() instanceof String source)) {
				throw new IllegalArgumentException("nodeSources must map node ids to Python source strings.");
			}
			sources.put(entry.getKey(), source);
		}
		return sources;
	}

	@Override
	public String getReactorDescription() {
		return "Runs inline automation definition JSON as the caller, without an Automation project.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.JSON.getKey().equals(key)) {
			return "Raw or Base64-encoded workflow graph JSON, such as a GetAutomation export.";
		}
		if (NODE_SOURCES_KEY.equals(key)) {
			return "Optional raw or Base64-encoded JSON object: { nodeId: Python source }. Only custom nodes keep "
					+ "supplied source; when omitted, the nodeSources embedded in json are used.";
		}
		if (AutomationConstants.AUTOMATION_INPUTS_KEY.equals(key)) {
			return "Optional values overriding trigger globals; date, triggered_at, and run_id are runtime-owned.";
		}
		if (AutomationConstants.AUTOMATION_TRIGGER_TYPE_KEY.equals(key)) {
			return "Optional trigger source: MANUAL (default), PLAYGROUND, or SCHEDULED.";
		}
		return super.getDescriptionForKey(key);
	}
}
