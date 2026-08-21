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

import java.util.Map;

import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Returns an automation graph together with its persisted per-node Python sources.
 *
 * <p>Pixel: {@code GetAutomation(project=["appId"])}
 */
public class GetAutomationReactor extends AbstractReactor {

	public GetAutomationReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		IProject project = AutomationProjectUtils.getViewableAutomationProject(this.insight.getUser(),
				this.keyValue.get(ReactorKeysEnum.PROJECT.getKey()));
		String projectId = project.getProjectId();
		if (project != null && project.requirePublish(true)) {
			// requirePublish refreshes the project assets before the definition is read.
		}

		AutomationDefinitionService.DefinitionFiles files =
				AutomationDefinitionService.load(projectId);
		AutomationMcpSync.sync(projectId, files.definition(), this.insight.getUser());
		Map<String, Object> definition = AutomationRuntimeUtils.GSON.fromJson(files.definition(),
				AutomationRuntimeUtils.MAP_TYPE);
		definition.put(AutomationConstants.DOC_NODE_SOURCES, files.nodeSources());
		definition.put(AutomationConstants.DOC_GLOBALS, AutomationRuntime.declaredGlobals(
				AutomationDefinitionValidator.parseAndValidate(files.definition()), files.nodeSources()));
		return new NounMetadata(definition, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Returns the automation graph, nodeSources, and trigger Python global defaults for a project.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.PROJECT.getKey().equals(key)) {
			return "The project ID or alias of the automation to retrieve.";
		}
		return super.getDescriptionForKey(key);
	}
}
