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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.project.api.IProject;
import prerna.project.impl.notebook.INotebookHelper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ValidateProjectDependenciesReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(ValidateProjectDependenciesReactor.class);
	
	public ValidateProjectDependenciesReactor() {
		this.keysToGet = new String[]{ ReactorKeysEnum.PROJECT.getKey() };
		this.keyRequired = new int[] {1};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		if(!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
			// you don't have access
			throw new IllegalArgumentException("Project/App does not exist or user does not have access to the project");
		}
		
		List<Map<String, Object>> projectDependencies = SecurityProjectUtils.getProjectDependencies(projectId, true);
		Map<String, Object> dependencyMap = new HashMap<>();
		for (Map<String, Object> dep : projectDependencies) {
			String engineId = (String) dep.get("engine_id");
			String engineType = (String) dep.get("engine_type");

			boolean canView = false;
			if (engineType == null || IEngine.CATALOG_TYPE.valueOf(engineType) != IEngine.CATALOG_TYPE.PROJECT) {
				canView = SecurityEngineUtils.userCanViewEngine(user, engineId);
			} else {	
				canView = SecurityProjectUtils.userCanViewProject(user, engineId);
			}
			dependencyMap.put(engineId, canView);
		}
		
		NounMetadata noun = new NounMetadata(dependencyMap, PixelDataType.MAP);
		return noun;
	}
	
	@Override
	public String getReactorDescription() {
		return "Return true if the user has access to all engine dependencies listed in this project";
	}
	
}
