//package prerna.cluster.util.clients;
//
//public class MinioClientBuilder extends AbstractClientBuilder {
//
//	protected String region  = "us-east-1";
//	protected String bucket = null;
//	protected String accessKey = null;
//	protected String secretKey = null;
//	protected String endpoint = null;
//	
//	public MinioClientBuilder() {};
//
//	@Override
//	public AbstractCloudClient buildClient() {
//		if(this.region == null || this.region.isEmpty()) {
//			throw new IllegalArgumentException("There is no region specified.");
//		}
//		if(this.bucket == null || this.bucket.isEmpty()) {
//			throw new IllegalArgumentException("There is no bucket key specified.");
//		}
//		if(this.accessKey == null || this.accessKey.isEmpty()) {
//			throw new IllegalArgumentException("There is no access key specified.");
//		}
//		if(this.secretKey == null || this.secretKey.isEmpty()) {
//			throw new IllegalArgumentException("There is no secret key specified.");
//		}
//		if(this.endpoint == null || this.endpoint.isEmpty()) {
//			throw new IllegalArgumentException("There is no endpoint specified.");
//		}
//		return new MinioClient(this);
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
//	public ICloudClientBuilder setEndpoint(String endpoint) {
//		this.endpoint = endpoint;
//		return this;
//	}
//
//	@Override
//	public MinioClientBuilder pullValuesFromSystem() {
//		AppCloudClientProperties clientProps = new AppCloudClientProperties();
//		setRClonePath(clientProps.get(RCLONE_PATH));
//		setRegion(clientProps.get(S3_REGION_KEY));
//		setBucket(clientProps.get(S3_BUCKET_KEY));
//		setAccessKey(clientProps.get(S3_ACCESS_KEY));
//		setSecretKey(clientProps.get(S3_SECRET_KEY));
//		setEndpoint(clientProps.get(S3_ENDPOINT_KEY));
//		return this;
//	}
//}
