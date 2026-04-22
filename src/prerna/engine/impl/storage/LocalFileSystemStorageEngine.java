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
package prerna.engine.impl.storage;

import java.io.IOException;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.StorageTypeEnum;
import prerna.engine.impl.SmssUtilities;
import prerna.util.Utility;

public class LocalFileSystemStorageEngine extends AbstractRCloneStorageEngine {

	private static final Logger classLogger = LogManager.getLogger(LocalFileSystemStorageEngine.class);

	{
		this.PROVIDER = "local";
	}

	// this is not really needed
	public static final String PATH_PREFIX = "PATH_PREFIX";

	@Deprecated
	public static final String LOCAL_PATH_PREFIX = "LOCAL_PATH_PREFIX";

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		this.BUCKET = smssProp.getProperty(PATH_PREFIX);
		if (this.BUCKET == null) {
			this.BUCKET = smssProp.getProperty(LOCAL_PATH_PREFIX);
			if (this.BUCKET == null) {
				throw new IllegalArgumentException("Must provide the " + PATH_PREFIX + " for the local file system");
			} else {
				classLogger.warn("Update SMSS key for {} from {} to new key {}",
						SmssUtilities.getUniqueName(this.engineName, this.engineId), LOCAL_PATH_PREFIX, PATH_PREFIX);
			}
		}

		this.BUCKET = this.BUCKET.replace("\\", "/");
	}

	@Override
	public String createRCloneConfig() throws IOException, InterruptedException {
		String rcloneConfig = Utility.getRandomString(10);
		runRcloneProcess(rcloneConfig, RCLONE, "config", "create", rcloneConfig, PROVIDER);
		return rcloneConfig;
	}

	@Override
	public StorageTypeEnum getStorageType() {
		return StorageTypeEnum.LOCAL_FILE_SYSTEM;
	}

}
