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

import prerna.auth.utils.SecurityProjectUtils;
import prerna.ds.py.PyTranslator;
import prerna.reactor.AbstractReactor;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

public class AddRPYProjectPathReactor extends AbstractReactor {
	
	public AddRPYProjectPathReactor() {
		this.keysToGet = new String [] {ReactorKeysEnum.PROJECT.getKey()};
		this.keyRequired = new int[] {1};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		PyTranslator pyt = this.insight.getPyTranslator();
		AbstractRJavaTranslator rt = this.insight.getRJavaTranslator(this.getClass().getName());

		String projectId = keyValue.get(keysToGet[0]);
		if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must input a project id");
		}
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if (!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access to view the project");
		}

		String basePath = AssetUtility.getProjectAssetsFolder(projectId);
		String folderName = basePath + "/py";
		folderName = folderName.replace("\\", "/");


		if(pyt != null)
		{	
			pyt.runScript("import sys", "sys.path.append('" + folderName +"')");
		}
		if(rt != null)
		{
			rt.runR("setwd('" + folderName + "')");
		}
		
		return NounMetadata.getSuccessNounMessage("Added " + projectId + " to path");
	}
	
	@Override
	public String getReactorDescription() {
		return "Add the project assets folder to the python sys.path and/or the R setwd";
	}
}
