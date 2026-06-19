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

import org.apache.logging.log4j.Logger;

import prerna.masterdatabase.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.git.GitRepoUtils;

public class DeleteAppRepo extends GitBaseReactor {

	public DeleteAppRepo() {
		this.keysToGet = new String[]{
				ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.REPOSITORY.getKey(), 
				ReactorKeysEnum.USERNAME.getKey(), ReactorKeysEnum.PASSWORD.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		Logger logger = getLogger(this.getClass().getName());
		logger.info("Removing remote...");
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		String databaseName = MasterDatabaseUtility.getDatabaseAliasForId(databaseId);
		String repository = this.keyValue.get(this.keysToGet[1]);
		String databaseFolder = AssetUtility.getProjectVersionFolder(databaseName, databaseId);

		// remove it from remote
		// take it out from local in case the global fails since they have removed the repository
		GitRepoUtils.deleteRemoteRepositorySettings(databaseFolder, repository);

		if(keyValue.size() == 4)
		{
			String username = this.keyValue.get(this.keysToGet[2]);
			String password = this.keyValue.get(this.keysToGet[3]);
			// drop it from external
			GitRepoUtils.deleteRemoteRepository(repository, username, password);
		}
		else
		{
			String oauth = getToken();
			GitRepoUtils.deleteRemoteRepository(repository, oauth);
		}
	
		return new NounMetadata(true, PixelDataType.CONST_STRING, PixelOperationType.MARKET_PLACE);
	}

	
	
}
