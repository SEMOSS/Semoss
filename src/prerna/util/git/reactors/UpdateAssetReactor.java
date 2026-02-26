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

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.kohsuke.github.GitHub;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.git.GitAssetMaker;
import prerna.util.git.GitUtils;

public class UpdateAssetReactor extends AbstractReactor {

	public UpdateAssetReactor() {
		// need repository
		// Oauth
		// File name
		// Content
		this.keysToGet = new String[]{ReactorKeysEnum.REPOSITORY.getKey(), ReactorKeysEnum.FILE_NAME.getKey(), ReactorKeysEnum.CONTENT.getKey()};
	}

	@Override
	public NounMetadata execute() {

		// need to get the user
		
		organizeKeys();
		
		User user = insight.getUser();
		String oauth = null;
		AccessToken gitAccess = user.getAccessToken(AuthProvider.GITHUB);
		
		if(gitAccess == null)
		{
			Map<String, Object> retMap = new HashMap<String, Object>();
			retMap.put("type", "git");
			retMap.put("message", "Please login to your Git account");
			throwLoginError(retMap);
		}

		// check for minmum
		checkMin(3);
		
		Logger logger = getLogger(this.getClass().getName());
		logger.info("Logging In...");
		String repoName = this.keyValue.get(this.keysToGet[0]);
		String fileName =  Utility.normalizePath( this.keyValue.get(this.keysToGet[1]) );
		String content = this.keyValue.get(this.keysToGet[2]);
		
		
		GitHub ret = GitUtils.login(gitAccess.getAccess_token());

		// create the repo.. just in case we dont have it
		// even though we get repo name.. we are not using it for create asset
		
		//GitAssetMaker.createAssetRepo(gitAccess.getAccess_token());
		GitAssetMaker.updateAsset(gitAccess.getAccess_token(), repoName, content, fileName);

		if(ret == null) {
			throw new IllegalArgumentException("Could not properly login using credentials");
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.MARKET_PLACE);
	}
}
