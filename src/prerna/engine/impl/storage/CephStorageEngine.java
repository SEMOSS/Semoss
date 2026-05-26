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

public class CephStorageEngine extends AbstractRCloneStorageEngine {

	{
		this.PROVIDER = "s3";
	}

	/**
	 * Yes, this is the ceph engine But the keys are the same as S3 with exception
	 * of requiring endpoint
	 */
	public static final String CEPH_ACCESS_KEY = "CEPH_ACCESS_KEY";
	public static final String CEPH_SECRET_KEY = "CEPH_SECRET_KEY";
	public static final String CEPH_ENDPOINT_KEY = "CEPH_ENDPOINT";

	// this is not really needed
	public static final String CEPH_BUCKET_KEY = "CEPH_BUCKET";

	// specific values - while not final they shouldn't be modified
	// REGION for ceph should be left blank
	private transient final String REGION = "";
	private transient String ACCESS_KEY = null;
	private transient String SECRET_KEY = null;
	private transient String ENDPOINT = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.ACCESS_KEY = smssProp.getProperty(CEPH_ACCESS_KEY);
		this.SECRET_KEY = smssProp.getProperty(CEPH_SECRET_KEY);
		this.ENDPOINT = smssProp.getProperty(CEPH_ENDPOINT_KEY);

		// this is technically not required for ceph
		this.BUCKET = smssProp.getProperty(CEPH_BUCKET_KEY);
	}

	@Override
	public String createRCloneConfig() throws IOException, InterruptedException {
		String rcloneConfig = Utility.getRandomString(10);

		runRcloneProcess(rcloneConfig, RCLONE, "config", "create", rcloneConfig, PROVIDER, "access_key_id", ACCESS_KEY,
				"secret_access_key", SECRET_KEY, "region", REGION, "endpoint", ENDPOINT);

		return rcloneConfig;
	}

	@Override
	public StorageTypeEnum getStorageType() {
		return StorageTypeEnum.CEPH;
	}

}
