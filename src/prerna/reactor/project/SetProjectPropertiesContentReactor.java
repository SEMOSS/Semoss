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
package prerna.reactor.project;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.project.impl.ProjectProperties;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class SetProjectPropertiesContentReactor extends AbstractReactor {

	public SetProjectPropertiesContentReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(),
				ReactorKeysEnum.PROJECT_PROPERTIES_MAP.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);

		if (StringUtils.isBlank(projectId)) {
			throw new IllegalArgumentException("Must input an project id");
		}

		if (!SecurityProjectUtils.userIsOwner(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user is not an owner of the project");
		}

		IProject project = Utility.getProject(projectId);
		ProjectProperties props = project.getProjectProperties();

		Map<String, String> mods = getMods();
		try {
			props.updateAllProperties(mods);
		} catch (Exception e) {
			throw new IllegalArgumentException(e.getMessage());
		}
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully set new properties for project"));
		return noun;
	}

	/*
	 * Converts inputed map of pixel call into a Map<string, string>
	 */
	private Map<String, String> getMods() {
		GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.PROJECT_PROPERTIES_MAP.getKey());
		if (mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if (mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, String>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, String>) mapInputs.get(0).getValue();
		}

		throw new IllegalArgumentException("Invalid submit request");
	}

}
