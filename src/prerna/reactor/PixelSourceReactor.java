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
package prerna.reactor;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class PixelSourceReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(PixelSourceReactor.class);

	public PixelSourceReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String relativePath = this.keyValue.get(this.keysToGet[0]);
		// strip leading separators so we don't get a double slash after concat with
		// assetFolder
		while (relativePath != null && (relativePath.startsWith("/") || relativePath.startsWith("\\"))) {
			relativePath = relativePath.substring(1);
		}
		String space = this.keyValue.get(this.keysToGet[1]);
		String assetFolder = AssetUtility.getRootFolderPath(this.insight, space, false);
		String path = assetFolder + DIR_SEPARATOR + relativePath;

		// if we have a chroot, mount the project for that user.
		if (Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.CHROOT_ENABLE))) {
			// get the app_root folder for the project
			this.insight.getUser().getUserSymlinkHelper().symlinkFolder(assetFolder);
		}

		// read in the file
		// execute it within this insight
		// return the results
		File file = new File(Utility.normalizePath(path));
		if (!file.exists()) {
			throw new IllegalArgumentException("Could not find the file path : " + relativePath);
		}

		String pixel = null;
		try {
			pixel = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
		} catch (IOException e) {
			classLogger.error("Failed to read pixel file at '{}': {}", relativePath, e.getMessage(), e);
			throw new IllegalArgumentException("Unable to read pixel file: " + relativePath);
		}

		if (pixel == null || (pixel = pixel.trim()).isEmpty()) {
			throw new IllegalArgumentException("Pixel file is empty");
		}

		PixelRunner pixelReturn = this.insight.runPixel(pixel);
		Map<String, Object> runnerWraper = new HashMap<String, Object>();
		runnerWraper.put("runner", pixelReturn);
		NounMetadata noun = new NounMetadata(runnerWraper, PixelDataType.PIXEL_RUNNER, PixelOperationType.SUB_SCRIPT);
		return noun;
	}
}
