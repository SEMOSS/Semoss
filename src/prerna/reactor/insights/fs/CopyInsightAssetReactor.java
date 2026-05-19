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
package prerna.reactor.insights.fs;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.FileSystemUtil;
import prerna.util.Utility;

public class CopyInsightAssetReactor extends AbstractReactor {

    public CopyInsightAssetReactor() {
        this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.NEW_VALUE.getKey() };
        this.keyRequired = new int[] { 1, 1 };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();

        User user = this.insight.getUser();
        // check if user is logged in
        if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
            throwAnonymousUserError();
        }

        String sourceFileName = Utility.normalizePath(this.keyValue.get(this.keysToGet[0]));
        String destFileName = Utility.normalizePath(this.keyValue.get(this.keysToGet[1]));

        if (sourceFileName == null || sourceFileName.trim().isEmpty() || destFileName == null
                || destFileName.trim().isEmpty()) {
            throw new IllegalArgumentException("Must pass both source path and destination path");
        }

        String assetFolder = this.insight.getInsightFolder();

        FileSystemUtil.copyAsset(assetFolder, sourceFileName, destFileName);

        // push room to cloud storage
        if (this.insight.getRoomId() != null) {
            ClusterUtil.pushRoomAsync(this.insight.getRoomId());
        }

        NounMetadata retNoun = NounMetadata.getSuccessNounMessage("Success!");
        return retNoun;
    }

    @Override
    public String getReactorDescription() {
        return "Copy (clone) a file or directory in the insight assets folder to a new path";
    }

    @Override
    protected String getDescriptionForKey(String key) {
        if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
            return "The source file or directory to copy. This relative path should assume the prefix of the insight folder.";
        } else if (key.equals(ReactorKeysEnum.NEW_VALUE.getKey())) {
            return "The destination path for the copy. This cannot be an existing file or directory and has the same character restrictions you would expect on a typical file system.";
        }
        return super.getDescriptionForKey(key);
    }

}
