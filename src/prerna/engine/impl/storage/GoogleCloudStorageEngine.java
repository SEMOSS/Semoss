package prerna.engine.impl.storage;

import java.io.IOException;
import java.util.Properties;

import prerna.util.Utility;

public class GoogleCloudStorageEngine extends AbstractRCloneStorageEngine {

	{
		this.PROVIDER = "google cloud storage";
	}

	private static final String GCS_REGION = "GCS_REGION";
	private static final String GCS_SERVICE_ACCOUNT_FILE_KEY = "GCS_SERVICE_ACCOUNT_FILE";
	
	// specific values - while not final they shouldn't be modified
	private String REGION = null;
	private String GCP_SERVICE_ACCOUNT_FILE = null;
	
	public void connect(Properties smssProp) throws Exception {
		super.connect(smssProp);
		
		// set this to be the db engine folder
		this.REGION = smssProp.getProperty(GCS_REGION);
		this.GCP_SERVICE_ACCOUNT_FILE = smssProp.getProperty(GCS_SERVICE_ACCOUNT_FILE_KEY);
	}
	
	@Override
	public String createRCloneConfig() throws IOException, InterruptedException {
		String rcloneConfig = Utility.getRandomString(10);

		runRcloneProcess(rcloneConfig, RCLONE, "config", "create", rcloneConfig, PROVIDER, "service_account_file",
				GCP_SERVICE_ACCOUNT_FILE, "location", REGION , "bucket_policy_only", "true");
	
		return rcloneConfig;
	}
	
	@Override
	public STORAGE_TYPE getStorageType() {
		return STORAGE_TYPE.GOOGLE_CLOUD_STORAGE;
	}

}
