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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Retrieves the list of project IDs where the current user is the owner.
 *
 * Pixel: CreatedByMeApps();
 *
 * @return Map with key "createdProjects" containing a Set of project ID strings
 */
public class CreatedByMeAppsReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(CreatedByMeAppsReactor.class);

	@Override
	public NounMetadata execute() {
		try {
			organizeKeys();
			User user = this.insight.getUser();
			if (user == null || user.getPrimaryLoginToken() == null) {
				throw new IllegalArgumentException("User must be logged in to view created projects");
			}
			
			// get all project IDs
	        List<String> allProjectIds = SecurityProjectUtils.getAllProjectIds();
	        Set<String> createdProjectIds = new HashSet<>();
	        
			// check ownership for each project
			for (String projectId : allProjectIds) {
				if (SecurityProjectUtils.userIsOwner(user, projectId)) {
					createdProjectIds.add(projectId);
				}
			}
			
	        Map<String, Object> retMap = new HashMap<>();
	        retMap.put("createdProjects", createdProjectIds);
	 
	        return new NounMetadata(retMap, PixelDataType.MAP, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error("Failed to fetch project IDs created by the current user", e);
			throw new SemossPixelException(
					"An error occurred while fetching projects created by the current user. Error message: " + e.getMessage());
		}
	}
	
	@Override
	public String getReactorDescription() {
		return "Fetches the list of project IDs created by the current user.";
	}
}