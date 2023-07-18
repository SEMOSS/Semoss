package prerna.engine.impl.storage;

import java.io.IOException;
import java.util.Properties;

import prerna.util.Utility;

public class S3StorageEngine extends AbstractBaseConfigRCloneStorageEngine {

	private static final String S3_REGION_KEY = "S3_REGION";
	private static final String S3_BUCKET_KEY = "S3_BUCKET";
	private static final String S3_ACCESS_KEY = "S3_ACCESS_KEY";
	private static final String S3_SECRET_KEY = "S3_SECRET_KEY";
	
	public boolean keysProvided = false;

	public S3StorageEngine() {
		
	}
	
	public void connect(Properties smssProp) throws Exception {
		super.connect(smssProp);
		
		// set this to be the db engine folder
		this.rcloneConfigFolder = "";
		this.REGION = smssProp.getProperty(S3_REGION_KEY);
		this.BUCKET = smssProp.getProperty(S3_BUCKET_KEY);
		this.ACCESS_KEY = smssProp.getProperty(S3_ACCESS_KEY);
		this.SECRET_KEY = smssProp.getProperty(S3_SECRET_KEY);
		
		// determine if keys provided or not
		if(ACCESS_KEY != null && !ACCESS_KEY.isEmpty() && SECRET_KEY != null && !SECRET_KEY.isEmpty()) {
			this.keysProvided = true;
		} else {
			this.keysProvided = false;
		}
	}
	
	@Override
	public String createRCloneConfig() throws IOException, InterruptedException {
		String rcloneConfig = Utility.getRandomString(10);

		if (keysProvided) {
			runRcloneProcess(rcloneConfig, RCLONE, "config", "create", rcloneConfig, PROVIDER, "access_key_id",
					ACCESS_KEY, "secret_access_key", SECRET_KEY, "region", REGION);
		} else {
			runRcloneProcess(rcloneConfig, RCLONE, "config", "create", rcloneConfig, PROVIDER, "env_auth", "true",
					"region", REGION);
		}
		return rcloneConfig;
	}
	
	@Override
	public STORAGE_TYPE getStorageType() {
		return STORAGE_TYPE.AMAZON_S3;
	}
	
}
