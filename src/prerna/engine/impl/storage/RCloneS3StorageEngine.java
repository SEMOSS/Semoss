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

import prerna.engine.api.StorageTypeEnum;
import prerna.util.Utility;

public class RCloneS3StorageEngine extends AbstractRCloneStorageEngine {

	{
		this.PROVIDER = "s3";
	}

	public static final String S3_REGION_KEY = "S3_REGION";
	public static final String S3_BUCKET_KEY = "S3_BUCKET";
	public static final String S3_ACCESS_KEY = "S3_ACCESS_KEY";
	public static final String S3_SECRET_KEY = "S3_SECRET_KEY";

	// specific values - while not final they shouldn't be modified
	protected String REGION = null;
	protected transient String ACCESS_KEY = null;
	protected transient String SECRET_KEY = null;
	public boolean KEYS_PROVIDED = false;

	public RCloneS3StorageEngine() {

	}

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.REGION = smssProp.getProperty(S3_REGION_KEY);
		this.BUCKET = smssProp.getProperty(S3_BUCKET_KEY);
		this.ACCESS_KEY = smssProp.getProperty(S3_ACCESS_KEY);
		this.SECRET_KEY = smssProp.getProperty(S3_SECRET_KEY);

		// determine if keys provided or not
		if (ACCESS_KEY != null && !ACCESS_KEY.isEmpty() && SECRET_KEY != null && !SECRET_KEY.isEmpty()) {
			this.KEYS_PROVIDED = true;
		} else {
			this.KEYS_PROVIDED = false;
		}
	}

	@Override
	public String createRCloneConfig() throws IOException, InterruptedException {
		String rcloneConfig = Utility.getRandomString(10);

		if (KEYS_PROVIDED) {
			runRcloneProcess(rcloneConfig, RCLONE, "config", "create", rcloneConfig, PROVIDER, "access_key_id",
					ACCESS_KEY, "secret_access_key", SECRET_KEY, "region", REGION);
		} else {
			runRcloneProcess(rcloneConfig, RCLONE, "config", "create", rcloneConfig, PROVIDER, "env_auth", "true",
					"region", REGION);
		}
		return rcloneConfig;
	}

	@Override
	public StorageTypeEnum getStorageType() {
		return StorageTypeEnum.RCLONE_S3;
	}

}
