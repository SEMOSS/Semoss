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

import java.util.Map;
import java.util.UUID;

import prerna.om.ThreadStore;
import prerna.reactor.AbstractReactor;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.definition.AutomationDefinitionService;
import prerna.reactor.automation.project.AutomationProjectService;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Checks edit access to an Automation project, then starts its saved definition through
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
		String projectId = AutomationProjectService.getEditableAutomationProject(this.insight.getUser(),
				this.keyValue.get(ReactorKeysEnum.PROJECT.getKey())).getProjectId();
		@SuppressWarnings("unchecked")
		Map<String, Object> inputs = this.getMap(AutomationConstants.AUTOMATION_INPUTS_KEY);
		Map<String, Object> result = AutomationRunExecutionService.start(this.insight, ThreadStore.getJobId(),
				UUID.randomUUID().toString(), projectId, AutomationConstants.DEFAULT_AUTOMATION_ID,
				AutomationDefinitionService.load(projectId), inputs,
				this.keyValue.get(AutomationConstants.AUTOMATION_TRIGGER_TYPE_KEY));
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
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
