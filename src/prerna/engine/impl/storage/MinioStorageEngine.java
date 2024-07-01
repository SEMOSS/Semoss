package prerna.engine.impl.storage;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import prerna.engine.api.StorageTypeEnum;
import prerna.util.Utility;

public class MinioStorageEngine extends AbstractRCloneStorageEngine {

	{
		this.PROVIDER = "s3";
		this.TRANSFER_LIMIT = "4";
	}
	
	/**
	 * Yes, this is the minio engine
	 * But the keys are the same as S3 with exception of requiring endpoint
	 */
	public static final String MINIO_REGION_KEY = "MINIO_REGION";
	public static final String MINIO_ACCESS_KEY = "MINIO_ACCESS_KEY";
	public static final String MINIO_SECRET_KEY = "MINIO_SECRET_KEY";
	public static final String MINIO_ENDPOINT_KEY = "MINIO_ENDPOINT";
	
	// this is not really needed
	public static final String MINIO_BUCKET_KEY = "MINIO_BUCKET";

	// specific values - while not final they shouldn't be modified
	private transient String REGION = null;
	private transient String ACCESS_KEY = null;
	private transient String SECRET_KEY = null;
	private transient String ENDPOINT = null;
	
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		
		this.REGION = smssProp.getProperty(MINIO_REGION_KEY);
		this.ACCESS_KEY = smssProp.getProperty(MINIO_ACCESS_KEY);
		this.SECRET_KEY = smssProp.getProperty(MINIO_SECRET_KEY);
		this.ENDPOINT = smssProp.getProperty(MINIO_ENDPOINT_KEY);
		
		// this is technically not required for minio
		this.BUCKET = smssProp.getProperty(MINIO_BUCKET_KEY);
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
		return StorageTypeEnum.MINIO;
	}

	
	
	
	
	
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	
	public static void main(String[] args) throws Exception {
		// docker run -p 9000:9000 -p 9001:9001 quay.io/minio/minio server /data --console-address ":9001"
		// download rclone https://rclone.org/downloads/
		// add the rclone folder to your path 
		// in a cmd prompt > rclone should be recognized as a command
		
		// these are not real/import access/secret - only for local docker
		Properties mockSmss = new Properties();
		mockSmss.put(MinioStorageEngine.MINIO_REGION_KEY, "us-east-1");
		mockSmss.put(MinioStorageEngine.MINIO_ACCESS_KEY, "***REMOVED***");
		mockSmss.put(MinioStorageEngine.MINIO_SECRET_KEY, "***REMOVED***");
		mockSmss.put(MinioStorageEngine.MINIO_ENDPOINT_KEY, "http://localhost:9000");
		
		MinioStorageEngine engine = new MinioStorageEngine();
		engine.open(mockSmss);
		String rCloneConfig = engine.createRCloneConfig();
		
		{
			List<String> list = engine.list("test1", rCloneConfig);
			System.out.println(list);
		}
		{
			List<Map<String, Object>> list = engine.listDetails("test1", rCloneConfig);
			System.out.println(list);
		}
		{
			engine.copyToStorage("C:\\Users\\mahkhalil\\Downloads\\MooseAI Logo.png", "test1", rCloneConfig);
		}
		{
			engine.copyToLocal("test1/MooseAI Logo.png", "C:\\Users\\mahkhalil", rCloneConfig);
		}
		{
			engine.deleteFromStorage("test1/MooseAI Logo.png", rCloneConfig);
		}
		
		engine.close();
	}

}
