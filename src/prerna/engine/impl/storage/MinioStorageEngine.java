package prerna.engine.impl.storage;

import java.io.IOException;
import java.util.Properties;

import prerna.util.Utility;

public class MinioStorageEngine extends AbstractBaseConfigRCloneStorageEngine {

	/**
	 * Yes, this is the minio engine
	 * But the keys are the same as S3 with exception of requiring endpoint
	 */
	private static final String S3_REGION_KEY = "S3_REGION";
	private static final String S3_ACCESS_KEY = "S3_ACCESS_KEY";
	private static final String S3_SECRET_KEY = "S3_SECRET_KEY";
	private static final String S3_ENDPOINT_KEY = "S3_ENDPOINT";
	
	private static final String S3_BUCKET_KEY = "S3_BUCKET";

	private String ENDPOINT = null;
	
	public void connect(Properties smssProp) {
		super.connect(smssProp);
		
		// set this to be the db engine folder
		this.rcloneConfigFolder = "";
		this.REGION = smssProp.getProperty(S3_REGION_KEY);
		this.ACCESS_KEY = smssProp.getProperty(S3_ACCESS_KEY);
		this.SECRET_KEY = smssProp.getProperty(S3_SECRET_KEY);
		this.ENDPOINT = smssProp.getProperty(S3_ENDPOINT_KEY);
		
		// this is technically not required for minio
		this.BUCKET = smssProp.getProperty(S3_BUCKET_KEY);
	}
	
	@Override
	public String createRCloneConfig() throws IOException, InterruptedException {
		String rcloneConfig = Utility.getRandomString(10);

		runRcloneProcess(rcloneConfig, RCLONE, "config", "create", rcloneConfig, PROVIDER, "access_key_id",
				ACCESS_KEY, "secret_access_key", SECRET_KEY, "region", REGION, "endpoint", ENDPOINT);

		return rcloneConfig;
	}

	@Override
	public STORAGE_TYPE getStorageType() {
		return STORAGE_TYPE.MINIO;
	}
	
	
	
	
	
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	
//	public static void main(String[] args) throws IOException, InterruptedException {
//		// docker run -p 9000:9000 -p 9001:9001 quay.io/minio/minio server /data --console-address ":9001"
//		
//		// these are not real/import access/secret - only for local docker
//		Properties mockSmss = new Properties();
//		mockSmss.put(S3_REGION_KEY, "us-east-1");
//		mockSmss.put(S3_ACCESS_KEY, "***REMOVED***");
//		mockSmss.put(S3_SECRET_KEY, "***REMOVED***");
//		mockSmss.put(S3_ENDPOINT_KEY, "http://localhost:9000");
//		
//		MinioStorageEngine engine = new MinioStorageEngine();
//		engine.connect(mockSmss);
//		String rCloneConfig = engine.createRcloneConfig();
//		
//		{
//			List<String> list = engine.list("test1", rCloneConfig);
//			System.out.println(list);
//		}
//		{
//			engine.copyToStorage("C:\\Users\\mahkhalil\\Downloads\\MooseAI Logo.png", "test1", rCloneConfig);
//		}
//		{
//			engine.copyToLocal("test1/MooseAI Logo.png", "C:\\Users\\mahkhalil", rCloneConfig);
//		}
//		{
//			engine.deleteFromStorage("test1/MooseAI Logo.png", rCloneConfig);
//		}
//	}

}
