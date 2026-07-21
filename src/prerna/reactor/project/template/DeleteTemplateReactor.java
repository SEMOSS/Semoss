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
package prerna.reactor.project.template;

import java.util.Map;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;


/** This reactor will delete the existing template information from the app corresponding template property file. 
 * @author kprasannakumar
 *
 */
public class DeleteTemplateReactor extends AbstractReactor {
	public DeleteTemplateReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.TEMPLATE_NAME.getKey(),
				ReactorKeysEnum.TEMPLATE_FILE.getKey() };
	}

	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		String templateFile = this.keyValue.get(ReactorKeysEnum.TEMPLATE_FILE.getKey());
		String templateName = this.keyValue.get(ReactorKeysEnum.TEMPLATE_NAME.getKey());
		if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must input a project id");
		}
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if (!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access to edit the project");
		}

		IProject project = Utility.getProject(projectId);
		ClusterUtil.pullProjectFolder(project, AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId));
		Map<String, String> templateDataMap = TemplateUtility.deleteTemplate(projectId, templateFile, templateName);
		ClusterUtil.pushProjectFolder(project, AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId));

		// returning back the updated template information which will contain all the template
		// information with template name as key and file name as the value
		return new NounMetadata(templateDataMap, PixelDataType.MAP);
	}

}
