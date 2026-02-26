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
package prerna.util.git.reactors;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.git.GitCollaboratorUtils;

public class SearchAppCollaborator extends GitBaseReactor {

	/**
	 * Search for another user
	 */
	
	public SearchAppCollaborator() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLLABORATOR.getKey(), 
				ReactorKeysEnum.USERNAME.getKey(), ReactorKeysEnum.PASSWORD.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();

		String searchTerm = this.keyValue.get(this.keysToGet[0]);

		Logger logger = getLogger(this.getClass().getName());
		logger.info("Establishing connection");
		logger.info("This can take several minutes depending on the speed of your internet");
		logger.info("Validating user");
		logger.info("Searching for " + searchTerm);

		List<Map<String, String>> collabList = null;
		if(keyValue.size() == 3)
		{
			String username = this.keyValue.get(this.keysToGet[1]);
			String password = this.keyValue.get(this.keysToGet[2]);
		
			collabList = GitCollaboratorUtils.searchUsers(searchTerm, username, password);
		}
		else
		{
			String token = getToken();
			collabList = GitCollaboratorUtils.searchUsers(searchTerm, token);
			
		}
		logger.info("Search Complete");
		return new NounMetadata(collabList, PixelDataType.VECTOR, PixelOperationType.MARKET_PLACE);
	}
}
