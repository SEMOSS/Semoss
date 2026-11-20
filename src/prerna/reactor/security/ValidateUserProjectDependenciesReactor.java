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
package prerna.reactor.security;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.UploadInputUtility;

public class ValidateUserProjectDependenciesReactor extends AbstractSetMetadataReactor {
	
	public ValidateUserProjectDependenciesReactor() {
		this.keysToGet = new String[]{ ReactorKeysEnum.PROJECT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		String projectId = UploadInputUtility.getProjectNameOrId(this.store);
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if(!SecurityProjectUtils.userCanViewProject(user, projectId)) {
			throw new IllegalArgumentException("The user does not have access to view this project or project id is invalid");
		}
		
		Map<String, Boolean> hasAccess = new HashMap<>();
		
		List<Map<String, Object>> dependentEngines = SecurityProjectUtils.getProjectDependencies(projectId, true);
		for(Map<String, Object> depEngine : dependentEngines) {
			String depEngineId = (String) depEngine.get("engine_id");
			String depEngineType = (String) depEngine.get("engine_type");
			
			if (depEngineType == null || IEngine.CATALOG_TYPE.valueOf(depEngineType) != IEngine.CATALOG_TYPE.PROJECT) {
				boolean canView = SecurityEngineUtils.userCanViewEngine(user, depEngineId);
				hasAccess.put(depEngineId, canView);
			} else {
				boolean canView = SecurityProjectUtils.userCanViewProject(user, depEngineId);
				hasAccess.put(depEngineId, canView);
			}
		}
		
		NounMetadata noun = new NounMetadata(hasAccess, PixelDataType.MAP);
		return noun;
	}
	
	@Override
	public String getReactorDescription() {
		return "Return a map {'engineid':true/false} for the users access to each engine dependency listed in this project";
	}
	
}
