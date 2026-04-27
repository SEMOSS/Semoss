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
package prerna.reactor.workspace;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.UserAssetUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class NewDirReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(NewDirReactor.class);
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	/*
	 * TODO: DONT BELIEVE THIS WORKS WITH CLOUD ?
	 * 
	 * 
	 */

	public NewDirReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.RELATIVE_PATH.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String relativePath = this.keyValue.get(this.keysToGet[0]);

		if (relativePath == null || relativePath.isEmpty()) {
			throw new IllegalArgumentException("Must input file path and file name to delete");
		} else {
			relativePath = Utility.normalizeParam(relativePath);
		}

		String assetProjectId = null;
		User user = this.insight.getUser();
		if (user != null) {
			AuthProvider token = user.getPrimaryLogin();
			if (token != null) {
				assetProjectId = user.getAssetProjectId(token);
				Utility.getProject(assetProjectId);
			}
		}

		if (assetProjectId == null) {
			throw new IllegalArgumentException("Unable to find user asset app");
		}

		String userFolder = AssetUtility.getRootFolderPath(this.insight, AssetUtility.USER_SPACE_KEY, true);
		File relativeFolder = new File(userFolder + DIR_SEPARATOR + relativePath);

		Boolean created = false;
		if (relativeFolder.exists()) {
			throw new IllegalArgumentException("There is already a folder at this location with that name");
		} else {
			created = relativeFolder.mkdirs();
			// made folder but now we need to add a hidden file for the cloud
			if (ClusterUtil.IS_CLUSTER) {
				File hidden = new File(relativeFolder + DIR_SEPARATOR + UserAssetUtils.HIDDEN_FILE);
				// override created boolean if its cloud to be at the hidden file level
				try {
					created = hidden.createNewFile();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					classLogger.error(Constants.STACKTRACE, e);
				}
				ClusterUtil.pushEngine(assetProjectId);
			}
		}

		return new NounMetadata(created, PixelDataType.BOOLEAN, PixelOperationType.USER_DIR);
	}

	@Override
	public String getName() {
		return "NewDir";
	}

}
