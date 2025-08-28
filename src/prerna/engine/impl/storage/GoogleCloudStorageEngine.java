/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.engine.impl.storage;

import java.io.IOException;
import java.util.Properties;
import prerna.engine.api.StorageTypeEnum;
import prerna.util.Utility;

public class GoogleCloudStorageEngine extends AbstractRCloneStorageEngine {

	{
		this.PROVIDER = "google cloud storage";
	}

	public static final String GCS_REGION = "GCS_REGION";
	public static final String GCS_SERVICE_ACCOUNT_FILE_KEY = "GCS_SERVICE_ACCOUNT_FILE";
	public static final String GCS_BUCKET_KEY = "GCS_BUCKET";

	// specific values - while not final they shouldn't be modified
	private transient String REGION = null;
	private transient String GCP_SERVICE_ACCOUNT_FILE = null;

	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		// set this to be the db engine folder
		this.REGION = smssProp.getProperty(GCS_REGION);
		this.GCP_SERVICE_ACCOUNT_FILE = smssProp.getProperty(GCS_SERVICE_ACCOUNT_FILE_KEY);
		this.BUCKET = smssProp.getProperty(GCS_BUCKET_KEY);
	}

	@Override
	public String createRCloneConfig() throws IOException, InterruptedException {
		String rcloneConfig = Utility.getRandomString(10);

		runRcloneProcess(rcloneConfig, RCLONE, "config", "create", rcloneConfig, PROVIDER, "service_account_file",
				GCP_SERVICE_ACCOUNT_FILE, "location", REGION, "bucket_policy_only", "true");

		return rcloneConfig;
	}

	@Override
	public StorageTypeEnum getStorageType() {
		return StorageTypeEnum.GOOGLE_CLOUD_STORAGE;
	}
}
