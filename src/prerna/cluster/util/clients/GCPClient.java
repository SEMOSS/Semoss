//package prerna.cluster.util.clients;
//
//import java.io.IOException;
//
//import prerna.cluster.util.ClusterUtil;
//import prerna.util.Utility;
//
//public class GCPClient extends AbstractBaseCloudClient {
//
//	{
//		this.PROVIDER = "google cloud storage";
//	}
//
//	private String GCP_SERVICE_ACCOUNT_FILE = null;
//	
//	public GCPClient(GCPClientBuilder builder) {
//		super(builder);
//
//		this.REGION = builder.region;
//		this.BUCKET = builder.bucket;
//		this.GCP_SERVICE_ACCOUNT_FILE = builder.gcpServiceAccountFile;
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
//		runRcloneProcess(rcloneConfig, RCLONE, "config", "create", rcloneConfig, PROVIDER, "service_account_file",
//				GCP_SERVICE_ACCOUNT_FILE, "location", REGION , "bucket_policy_only", "true");
//	
//		return rcloneConfig;
//	}
//	
//}
