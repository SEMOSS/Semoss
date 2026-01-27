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


import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import prerna.auth.User;
import prerna.om.CopyObject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class CtrlVAssetReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CtrlVAssetReactor.class);

	
	public CtrlVAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.SPACE.getKey(), ReactorKeysEnum.FILE_PATH.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		String filePath = Utility.normalizePath(this.keyValue.get(this.keysToGet[1]));
		String space = this.keyValue.get(this.keysToGet[0]);

		String assetFolder = AssetUtility.getRootFolderPath(this.insight, space, true);
		String relativePath = AssetUtility.getAssetRelativePath(this.insight, space);
		
		if(filePath == null)
			filePath = "";
		if(relativePath == null)
			relativePath = "";
		else
			relativePath = relativePath + DIR_SEPARATOR;
		// file / folder to be moved
		String destSource = assetFolder + DIR_SEPARATOR + relativePath + filePath;
				
		// need to make sure the destination is valid
		File file = new File(destSource);
		if(!(file.exists() && file.isDirectory()))
			throw new IllegalArgumentException("Destination  should be a directory : " + filePath);
		
		CopyObject copyObj = user.getCtrlC();
		String copySource = copyObj.source;
		boolean isDelete = copyObj.delete;
		
		
		if(copySource == null)
			throw new IllegalArgumentException("Nothing to copy, please copy something first ");

		File sfile = new File(copySource);
		boolean isSourceDir = sfile.exists() && sfile.isDirectory();

		String dirName = sfile.getName();
		
		if(isSourceDir)
			destSource = destSource + DIR_SEPARATOR + dirName;
		file = new File(destSource);
		try {
			if(isSourceDir)
			{
				FileUtils.copyDirectory(sfile, file);
				if(isDelete)
					FileUtils.deleteDirectory(sfile);
			}
			else
			{
				FileUtils.copyFileToDirectory(sfile, file);
				if(isDelete)
					sfile.delete();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}
		
		return NounMetadata.getSuccessNounMessage("Pasted " + copyObj.showSource);

	}
}
