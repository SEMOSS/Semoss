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

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.FileSystemUtil;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

public class CopyEngineAssetReactor extends AbstractReactor {

    public CopyEngineAssetReactor() {
        this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.FILE_PATH.getKey(),
                ReactorKeysEnum.NEW_VALUE.getKey(), ReactorKeysEnum.COMMENT_KEY.getKey() };
        this.keyRequired = new int[] { 1, 1, 1, 0 };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();

        User user = this.insight.getUser();
        // check if user is logged in
        if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
            throwAnonymousUserError();
        }

        String engineId = this.keyValue.get(this.keysToGet[0]);
        if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
            throw new IllegalArgumentException(
                    "Engine " + engineId + " does not exist or user does not have access to edit assets.");
        }
        // force to pull it from cloud if not in the container
        IEngine engine = Utility.getEngine(engineId);

        String sourceFileName = Utility.normalizePath(this.keyValue.get(this.keysToGet[1]));
        String destFileName = Utility.normalizePath(this.keyValue.get(this.keysToGet[2]));
        while (sourceFileName != null && sourceFileName.startsWith("/")) {
            sourceFileName = sourceFileName.substring(1);
        }
        while (destFileName != null && destFileName.startsWith("/")) {
            destFileName = destFileName.substring(1);
        }

        if (sourceFileName == null || sourceFileName.trim().isEmpty() || destFileName == null
                || destFileName.trim().isEmpty()) {
            throw new IllegalArgumentException("Must pass both source path and destination path");
        }

        String gitFolder = EngineUtility.getSpecificEngineVersionFolder(engine.getCatalogType(), engine.getEngineId(),
                engine.getEngineName());
        String assetFolder = EngineUtility.getSpecificEngineAssetsFolder(engine.getCatalogType(), engine.getEngineId(),
                engine.getEngineName());
        String comment = this.keyValue.get(this.keysToGet[3]);
        if (comment == null) {
            comment = "copy: Copying " + sourceFileName + " to " + destFileName;
        }

        FileSystemUtil.copyAsset(assetFolder, sourceFileName, destFileName);

        // handle pushing to git and the cloud
        List<String> toAdd = new ArrayList<>();
        toAdd.add(Constants.ASSETS_FOLDER + DIR_SEPARATOR + destFileName);

        GitRepoUtils.addSpecificFiles(gitFolder, toAdd);

        // Get the user's email
        AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
        String email = accessToken.getEmail();
        String author = accessToken.getResolvedUsername();

        // commit it
        GitRepoUtils.commitAddedFiles(gitFolder, comment, author, email);
        // handle synchronization to the cloud
        ClusterUtil.pushEngineFolder(engine, assetFolder);

        NounMetadata retNoun = NounMetadata.getSuccessNounMessage("Success!");
        return retNoun;
    }

    @Override
    public String getReactorDescription() {
        return "Copy (clone) a file or directory in the engine assets folder to a new path";
    }

    @Override
    protected String getDescriptionForKey(String key) {
        if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
            return "The unique id for the engine";
        } else if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
            return "The source file or directory to copy. This relative path should assume the prefix of '/version/assets/' and not include the prefix in the string value.";
        } else if (key.equals(ReactorKeysEnum.NEW_VALUE.getKey())) {
            return "The destination path for the copy. This cannot be an existing file or directory and has the same character restrictions you would expect on a typical file system.";
        } else if (key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
            return "Comment to add while saving the files within the git repository for the engine";
        }
        return super.getDescriptionForKey(key);
    }

}
