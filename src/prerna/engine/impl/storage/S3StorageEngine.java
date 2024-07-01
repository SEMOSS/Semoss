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
	protected transient String ACCESS_KEY = null;
	protected transient String SECRET_KEY = null;
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
	
	
	
	
	
	
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	
//	public static void main(String[] args) throws Exception {
//		// docker run -p 9000:9000 -p 9001:9001 quay.io/minio/minio server /data --console-address ":9001"
//		// download rclone https://rclone.org/downloads/
//		// add the rclone folder to your path 
//		// in a cmd prompt > rclone should be recognized as a command
//		
//		// these are not real/import access/secret - only for local docker
//		Properties mockSmss = new Properties();
//		mockSmss.put(MinioStorageEngine.MINIO_REGION_KEY, "us-east-1");
//		mockSmss.put(MinioStorageEngine.MINIO_ACCESS_KEY, "***REMOVED***");
//		mockSmss.put(MinioStorageEngine.MINIO_SECRET_KEY, "***REMOVED***");
//		mockSmss.put(MinioStorageEngine.MINIO_ENDPOINT_KEY, "http://localhost:9000");
//		
//		MinioStorageEngine engine = new MinioStorageEngine();
//		engine.open(mockSmss);
//		String rCloneConfig = engine.createRCloneConfig();
//		
//		{
//			List<String> list = engine.list("test1", rCloneConfig);
//			System.out.println(list);
//		}
//		{
//			List<Map<String, Object>> list = engine.listDetails("test1", rCloneConfig);
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
//		
//		engine.close();
//	}
	
	
}
