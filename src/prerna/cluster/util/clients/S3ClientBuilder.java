//package prerna.cluster.util.clients;
//
//public class S3ClientBuilder extends AbstractClientBuilder {
//
//	protected String region  = "us-east-1";
//	protected String bucket = null;
//	protected String accessKey = null;
//	protected String secretKey = null;
//	protected boolean keysProvided = true;
//	
//	public S3ClientBuilder() {};
//
//	@Override
//	public AbstractCloudClient buildClient() {
//		if(this.region == null || this.region.isEmpty()) {
//			throw new IllegalArgumentException("There is no region specified.");
//		}
//		if(this.bucket == null || this.bucket.isEmpty()) {
//			throw new IllegalArgumentException("There is no bucket key specified.");
//		}
//		determineKeysProvided();
//		return new S3Client(this);
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
//	public ICloudClientBuilder setAccessKey(String accessKey) {
//		this.accessKey = accessKey;
//		return this;
//	}
//
//	public ICloudClientBuilder setSecretKey(String secretKey) {
//		this.secretKey = secretKey;
//		return this;
//	}
//
//	public ICloudClientBuilder setKeysProvided(Boolean keysProvided) {
//		if(keysProvided != null) {
//			this.keysProvided = keysProvided;
//		}
//		return this;
//	}
//	
//	public ICloudClientBuilder determineKeysProvided() {
//		if(accessKey != null && !accessKey.isEmpty() && secretKey != null && !secretKey.isEmpty()) {
//			this.keysProvided = true;
//		} else {
//			this.keysProvided = false;
//		}
//		return this;
//	}
//
//	@Override
//	public S3ClientBuilder pullValuesFromSystem() {
//		AppCloudClientProperties clientProps = new AppCloudClientProperties();
//		setRClonePath(clientProps.get(RCLONE_PATH));
//		setRegion(clientProps.get(S3_REGION_KEY));
//		setBucket(clientProps.get(S3_BUCKET_KEY));
//		setAccessKey(clientProps.get(S3_ACCESS_KEY));
//		setSecretKey(clientProps.get(S3_SECRET_KEY));
//		return this;
//	}
//}
