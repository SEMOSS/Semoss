//package prerna.cluster.util.clients;
//
//import java.io.IOException;
//
//import prerna.cluster.util.ClusterUtil;
//import prerna.util.Utility;
//
//public class S3Client extends AbstractBaseCloudClient {
//
//	public boolean keysProvided = false;
//
//	S3Client(S3ClientBuilder builder) {
//		super(builder);
//		
//		this.RCLONE = builder.rclonePath;
//		this.rcloneConfigFolder = builder.rcloneConfigF;
//		this.REGION = builder.region;
//		this.BUCKET = builder.bucket;
//		this.ACCESS_KEY = builder.accessKey;
//		this.SECRET_KEY = builder.secretKey;
//		this.keysProvided = builder.keysProvided;
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
//		if (keysProvided) {
//			runRcloneProcess(rcloneConfig, RCLONE, "config", "create", rcloneConfig, PROVIDER, "access_key_id",
//					ACCESS_KEY, "secret_access_key", SECRET_KEY, "region", REGION);
//		} else {
//			runRcloneProcess(rcloneConfig, RCLONE, "config", "create", rcloneConfig, PROVIDER, "env_auth", "true",
//					"region", REGION);
//		}
//		return rcloneConfig;
//	}
//
//}
