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
	 * Yes, this is the ceph engine
	 * But the keys are the same as S3 with exception of requiring endpoint
	 */
	public static final String CEPH_REGION_KEY = "CEPH_REGION_KEY";
	public static final String CEPH_ACCESS_KEY = "CEPH_ACCESS_KEY";
	public static final String CEPH_SECRET_KEY = "CEPH_SECRET_KEY";
	public static final String CEPH_ENDPOINT_KEY = "CEPH_ENDPOINT";
	
	// this is not really needed
	public static final String CEPH_BUCKET_KEY = "CEPH_BUCKET";

	// specific values - while not final they shouldn't be modified
	private String REGION = null;
	private String ACCESS_KEY = null;
	private String SECRET_KEY = null;
	private String ENDPOINT = null;
	
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		
		this.REGION = smssProp.getProperty(CEPH_REGION_KEY);
		this.ACCESS_KEY = smssProp.getProperty(CEPH_ACCESS_KEY);
		this.SECRET_KEY = smssProp.getProperty(CEPH_SECRET_KEY);
		this.ENDPOINT = smssProp.getProperty(CEPH_ENDPOINT_KEY);
		
		// this is technically not required for ceph
		this.BUCKET = smssProp.getProperty(CEPH_BUCKET_KEY);
	}
	
	@Override
	public String createRCloneConfig() throws IOException, InterruptedException {
		String rcloneConfig = Utility.getRandomString(10);

		runRcloneProcess(rcloneConfig, RCLONE, "config", "create", rcloneConfig, PROVIDER, "access_key_id",
				ACCESS_KEY, "secret_access_key", SECRET_KEY, "region", REGION, "endpoint", ENDPOINT);

		return rcloneConfig;
	}

	@Override
	public StorageTypeEnum getStorageType() {
		return StorageTypeEnum.CEPH;
	}

}
