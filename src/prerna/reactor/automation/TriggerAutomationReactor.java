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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import prerna.om.ThreadStore;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Authenticates, validates, and initializes an Automation run before delegating its lifecycle to
 * {@link AutomationRunExecutionService}.
 *
 * <p>Pixel: {@code TriggerAutomation(project=["appId"])}
 */
public class TriggerAutomationReactor extends AbstractReactor {

	public TriggerAutomationReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.PROJECT.getKey(),
				AutomationConstants.AUTOMATION_INPUTS_KEY,
				AutomationConstants.AUTOMATION_TRIGGER_TYPE_KEY };
		this.keyRequired = new int[] { 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = getProjectId();
		String runId = UUID.randomUUID().toString();
		AutomationDefinitionService.DefinitionFiles files = AutomationDefinitionService.load(projectId);
		AutomationDefinitionValidator.ValidatedDefinition definition =
				AutomationDefinitionValidator.parseAndValidate(files.definition());
		AutomationProjectUtils.validateDefinitionReferences(definition, this.insight.getUser());
		List<Map<String, Object>> runNodes = AutomationRuntime.nodesForRun(definition);
		@SuppressWarnings("unchecked")
		Map<String, Object> inputs = this.getMap(AutomationConstants.AUTOMATION_INPUTS_KEY);
		validateInputs(inputs);
		Map<String, Object> effectiveInputs = new LinkedHashMap<>(
				AutomationRuntime.declaredGlobals(definition, files.nodeSources()));
		if (inputs != null) {
			effectiveInputs.putAll(inputs);
		}
		Map<String, String> traceRoomIds = AutomationRunExecutionService.allocateTraceRoomIds(runNodes);

		initializeRun(runId, projectId, definition, effectiveInputs, runNodes, traceRoomIds,
				files.nodeSources());

		Map<String, Object> result = new AutomationRunExecutionService(this.insight, ThreadStore.getJobId())
				.executeInitializedRun(runId, projectId, definition, runNodes, traceRoomIds);
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private void initializeRun(String runId, String projectId,
			AutomationDefinitionValidator.ValidatedDefinition definition,
			Map<String, Object> inputs, List<Map<String, Object>> runNodes,
			Map<String, String> traceRoomIds,
			Map<String, String> nodeSources) {
		AutomationDatabaseUtility.initializeRun(runId, projectId,
				AutomationConstants.DEFAULT_AUTOMATION_ID, AutomationConstants.PYTHON_DOC_CURRENT_VERSION,
				definition.hash(), definition.snapshot(), inputs, getTriggerType(), getUserId(), runNodes,
				traceRoomIds, nodeSources);
	}

	private String getProjectId() {
		return AutomationProjectUtils.getEditableAutomationProject(this.insight.getUser(),
				this.keyValue.get(ReactorKeysEnum.PROJECT.getKey())).getProjectId();
	}

	private String getTriggerType() {
		String triggerType = this.keyValue.get(AutomationConstants.AUTOMATION_TRIGGER_TYPE_KEY);
		if (triggerType == null || triggerType.isBlank()) {
			return AutomationConstants.TRIGGER_MANUAL;
		}
		if (!AutomationConstants.TRIGGER_MANUAL.equals(triggerType)
				&& !AutomationConstants.TRIGGER_PLAYGROUND.equals(triggerType)
				&& !AutomationConstants.TRIGGER_SCHEDULED.equals(triggerType)) {
			throw new IllegalArgumentException("triggerType must be MANUAL, PLAYGROUND, or SCHEDULED.");
		}
		return triggerType;
	}

	private String getUserId() {
		if (this.insight.getUser() != null && this.insight.getUser().getPrimaryLoginToken() != null) {
			return this.insight.getUser().getPrimaryLoginToken().getId();
		}
		return AutomationConstants.SYSTEM_USER_ID;
	}

	private static void validateInputs(Map<String, Object> inputs) {
		if (inputs == null) {
			return;
		}
		for (String key : inputs.keySet()) {
			if (AutomationConstants.RESERVED_SCOPE_KEYS.contains(key)) {
				throw new IllegalArgumentException("Automation input '" + key
						+ "' is reserved for runtime metadata and cannot be overridden.");
			}
		}
	}

	@Override
	public String getReactorDescription() {
		return "Runs automation through the authenticated Python workflow runtime.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.PROJECT.getKey().equals(key)) {
			return "The project ID or alias containing the automation workflow.";
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
