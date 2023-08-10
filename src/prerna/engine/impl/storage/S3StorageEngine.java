package prerna.engine.impl.storage;

import java.io.IOException;
import java.util.Properties;

import prerna.engine.api.StorageTypeEnum;
import prerna.util.Utility;

public class S3StorageEngine extends AbstractRCloneStorageEngine {

	{
		this.PROVIDER = "s3";
	}
	
	public static final String S3_REGION_KEY = "S3_REGION";
	public static final String S3_BUCKET_KEY = "S3_BUCKET";
	public static final String S3_ACCESS_KEY = "S3_ACCESS_KEY";
	public static final String S3_SECRET_KEY = "S3_SECRET_KEY";
	
	// specific values - while not final they shouldn't be modified
	protected String REGION = null;
	protected String ACCESS_KEY = null;
	protected String SECRET_KEY = null;
	public boolean KEYS_PROVIDED = false;
	
	public S3StorageEngine() {
		
	}
	
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		
		// set this to be the db engine folder
		this.rcloneConfigFolder = "";
		this.REGION = smssProp.getProperty(S3_REGION_KEY);
		this.BUCKET = smssProp.getProperty(S3_BUCKET_KEY);
		this.ACCESS_KEY = smssProp.getProperty(S3_ACCESS_KEY);
		this.SECRET_KEY = smssProp.getProperty(S3_SECRET_KEY);
		
		// determine if keys provided or not
		if(ACCESS_KEY != null && !ACCESS_KEY.isEmpty() && SECRET_KEY != null && !SECRET_KEY.isEmpty()) {
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
		return StorageTypeEnum.AMAZON_S3;
	}
	
}
