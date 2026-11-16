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
package prerna.reactor.engine.fs;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONObject;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.FileSystemUtil;
import prerna.util.FileSystemUtil.SavedAsset;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

public abstract class AbstractSaveEngineAssetsReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();

		String engineId = this.keyValue.get(this.keysToGet[0]);
		if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Engine " + engineId + " does not exist or user does not have access to edit assets.");
		}
		// force to pull it from cloud if not in the container
		IEngine engine = Utility.getEngine(engineId);

		// Retrieve all file paths and contents
		List<String> filePaths = getNounAsStringList(this.keysToGet[1]);
		List<String> contents = getNounAsStringList(this.keysToGet[2]);

		if (filePaths == null || filePaths.isEmpty() || contents == null || contents.isEmpty()) {
			throw new IllegalArgumentException("Must pass in at least one file name and content to save");
		}
		if (filePaths.size() != contents.size()) {
			throw new IllegalArgumentException("Number of file names and contents must match");
		}
		filePaths = Utility.normalizeFilePaths(filePaths);

		String gitFolder = EngineUtility.getSpecificEngineVersionFolder(engine.getCatalogType(), engine.getEngineId(),
				engine.getEngineName());
		String assetFolder = EngineUtility.getSpecificEngineAssetsFolder(engine.getCatalogType(), engine.getEngineId(),
				engine.getEngineName());
		String comment = this.keyValue.get(this.keysToGet[3]);
		if (comment == null) {
			comment = "add: save engine assets executed";
		}

		// Check strict script source settings once
		boolean strictScriptSource = Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.STRICT_SCRIPT_SOURCE));
		FileSystemUtil.validateAssetFiles(filePaths, strictScriptSource);
		List<SavedAsset> savedAssets = saveAssetFiles(assetFolder, filePaths, contents,
				getBoolean(ReactorKeysEnum.OVERRIDE.getKey(), true));

		// add file to git
		List<String> gitRelativeFilePaths = new ArrayList<>();
		for (SavedAsset savedAsset : savedAssets) {
			// for git, we need to add the assets folder which is assumed in the path
			gitRelativeFilePaths.add(Constants.ASSETS_FOLDER + DIR_SEPARATOR + savedAsset.savedPath());
		}

		// Get the user's email
		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
		String email = accessToken.getEmail();
		String author = accessToken.getResolvedUsername();

		GitRepoUtils.addSpecificFiles(gitFolder, gitRelativeFilePaths);
		// commit it
		GitRepoUtils.commitAddedFiles(gitFolder, comment, author, email);
		// handle synchronization to the cloud
		ClusterUtil.pushEngineFolder(engine, assetFolder);

		return FileSystemUtil.savedAssetsAsNounMetadata(savedAssets);
	}

	/**
	 * Writes the files into the asset folder.
	 *
	 * @param assetFolder the engine's assets folder
	 * @param filePaths   the normalized file paths
	 * @param contents    the content for each path
	 * @param override    whether an existing file at a path is replaced
	 * @return where each file was saved
	 */
	protected abstract List<SavedAsset> saveAssetFiles(String assetFolder, List<String> filePaths,
			List<String> contents, boolean override);

	@Override
	public JSONObject getMcpProperties() {
		JSONObject properties = super.getMcpProperties();
		properties.getJSONObject(ReactorKeysEnum.OVERRIDE.getKey()).put("default", true);
		return properties;
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().key(ReactorKeysEnum.OVERRIDE,
				key -> key.type(Boolean.class).description(FileSystemUtil.OVERRIDE_SAVE_DESCRIPTION))
				.outputList(SavedAsset.class, FileSystemUtil.SAVED_ASSETS_OUTPUT_DESCRIPTION);
	}

}
