//package prerna.cluster.util.clients;
//
//public class GCPClientBuilder extends AbstractClientBuilder {
//
//	protected String region  = null;
//	protected String bucket = null;
//	protected String gcpServiceAccountFile = null;
//	
//	public GCPClientBuilder() {};
//
//	@Override
//	public AbstractCloudClient buildClient() {
//		if(this.region == null || this.region.isEmpty()) {
//			throw new IllegalArgumentException("There is no region specified.");
//		}
//		if(this.bucket == null || this.bucket.isEmpty()) {
//			throw new IllegalArgumentException("There is no bucket key specified.");
//		}
//		if(this.gcpServiceAccountFile == null || this.gcpServiceAccountFile.isEmpty()) {
//			throw new IllegalArgumentException("There is no gcp service account file specified.");
//		}
//		return new GCPClient(this);
//	}
//	
//	public ICloudClientBuilder setRegion(String region) {
//		this.region = region;
//		return this;
//	}
//
//	public ICloudClientBuilder setBucket(String bucket) {
//		this.bucket = bucket;
//		return this;
//	}
//
//	public ICloudClientBuilder setGCPServiceAccountFile(String gcpServiceAccountFile) {
//		this.gcpServiceAccountFile = gcpServiceAccountFile;
//		return this;
//	}
//
//	@Override
//	public GCPClientBuilder pullValuesFromSystem() {
//		AppCloudClientProperties clientProps = new AppCloudClientProperties();
//		setRClonePath(clientProps.get(RCLONE_PATH));
//		setRegion(clientProps.get(GCP_REGION_KEY));
//		setBucket(clientProps.get(GCP_BUCKET_KEY));
//		setGCPServiceAccountFile(clientProps.get(GCP_SERVICE_ACCOUNT_FILE_KEY));
//		return this;
//	}
//}
