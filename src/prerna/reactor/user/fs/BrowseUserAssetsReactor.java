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
package prerna.reactor.user.fs;

import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.FileSystemUtil;
import prerna.util.Utility;

public class BrowseUserAssetsReactor extends AbstractReactor {

	public BrowseUserAssetsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey() };
		this.keyRequired = new int[] { 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		IProject project = user.getAssetProject();
		if (project == null) {
			throw new IllegalArgumentException("Unable to find user asset app");
		}

		String relativeFilePath = this.keyValue.get(this.keysToGet[0]);
		if (relativeFilePath != null) {
			relativeFilePath = Utility.normalizePath(relativeFilePath.trim());
			if (!relativeFilePath.isEmpty()) {
				relativeFilePath = relativeFilePath.replace('\\', '/');
				if (!relativeFilePath.startsWith("/")) {
					relativeFilePath = "/" + relativeFilePath;
				}
			}
		}

		String filePath = AssetUtility.getUserAssetFolder(project.getProjectName(), project.getProjectId());
		int pathSubstringIndex = filePath.length();
		if (relativeFilePath != null && !relativeFilePath.isEmpty()) {
			filePath += relativeFilePath;
		}

		List<Map<String, Object>> retObj = FileSystemUtil.browseFileSystem(user, filePath, relativeFilePath,
				pathSubstringIndex);

		return new NounMetadata(retObj, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "List the files and directories from a relative filePath input from within the user's assets folder";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "The relative file path to list contents from. This relative path should assume the prefix of '/version/assets/' and not include the prefix in the string value.";
		}
		return super.getDescriptionForKey(key);
	}

}
