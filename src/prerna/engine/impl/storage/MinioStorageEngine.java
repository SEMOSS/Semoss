package prerna.engine.impl.storage;

import java.io.IOException;

import prerna.util.Utility;

public class MinioStorageEngine extends AbstractBaseConfigRCloneStorageEngine {

	/**
	 * Yes, this is the minio engine
	 * But the keys are the same as S3 with exception of requiring endpoint
	 */
	private static final String S3_REGION_KEY = "S3_REGION";
	private static final String S3_BUCKET_KEY = "S3_BUCKET";
	private static final String S3_ACCESS_KEY = "S3_ACCESS_KEY";
	private static final String S3_SECRET_KEY = "S3_SECRET_KEY";
	private static final String S3_ENDPOINT_KEY = "S3_ENDPOINT";
	
	private String ENDPOINT = null;
	
	@Override
	public String createRcloneConfig() throws IOException, InterruptedException {
		String rcloneConfig = Utility.getRandomString(10);

		runRcloneProcess(rcloneConfig, RCLONE, "config", "create", rcloneConfig, PROVIDER, "access_key_id",
				ACCESS_KEY, "secret_access_key", SECRET_KEY, "region", REGION, "endpoint", ENDPOINT);

		return rcloneConfig;
	}
	
}
