//package prerna.cluster.util.clients;
//
//import java.io.IOException;
//
//import prerna.cluster.util.ClusterUtil;
//import prerna.util.Utility;
//
//public class MinioClient extends AbstractBaseCloudClient {
//
//	{
//		this.PROVIDER = "s3";
//		this.TRANSFER_LIMIT = "4";
//	}
//	
//	private String ENDPOINT = null;
//
//	public MinioClient(MinioClientBuilder builder) {
//		super(builder);
//		
//		this.RCLONE = builder.rclonePath;
//		this.rcloneConfigFolder = builder.rcloneConfigF;
//		this.REGION = builder.region;
//		this.BUCKET = builder.bucket;
//		this.ACCESS_KEY = builder.accessKey;
//		this.SECRET_KEY = builder.secretKey;
//		this.ENDPOINT = builder.endpoint;
//		
//		this.RCLONE_DB_PATH = ":" + BUCKET + "/" + ClusterUtil.DB_BLOB + "/";
//		this.RCLONE_PROJECT_PATH = ":" + BUCKET + "/" + ClusterUtil.PROJECT_BLOB + "/";
//		this.RCLONE_USER_PATH = ":" + BUCKET + "/" + ClusterUtil.USER_BLOB + "/";
//	}
//
//	@Override
//	public String createRcloneConfig() throws IOException, InterruptedException {
//		String rcloneConfig = Utility.getRandomString(10);
//
//		runRcloneProcess(rcloneConfig, RCLONE, "config", "create", rcloneConfig, PROVIDER, "access_key_id",
//				ACCESS_KEY, "secret_access_key", SECRET_KEY, "region", REGION, "endpoint", ENDPOINT);
//
//		return rcloneConfig;
//	}
//}
