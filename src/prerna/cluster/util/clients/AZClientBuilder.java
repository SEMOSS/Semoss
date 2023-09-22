//package prerna.cluster.util.clients;
//
//public class AZClientBuilder extends AbstractClientBuilder {
//
//	// if the zookeeper is defined.. find from zookeeper what the key is
//	// and register for the key change
//	// if not.. the storage key is sitting some place pick it up and get it
//	protected String storage = null;
//	protected String azKeyRoot = null;
//	protected String connectionString;
//	protected String name = null;
//	protected String key = null;
//	protected String blobURI = null;
//	protected String sasURL = null;
//	
//	public AZClientBuilder() {};
//
//	@Override
//	public AbstractCloudClient buildClient() {
//		if(this.connectionString == null || this.connectionString.isEmpty()) {
//			throw new IllegalArgumentException("There is no connection string specified.");
//		}
//		return new AZClient(this);
//	}
//	
//	public AZClientBuilder setStorage(String storage) {
//		this.storage = storage;
//		return this;
//	}
//
//	public AZClientBuilder setAzKeyRoot(String azKeyRoot) {
//		this.azKeyRoot = azKeyRoot;
//		return this;
//	}
//
//	public AZClientBuilder setConnectionString(String connectionString) {
//		this.connectionString = connectionString;
//		return this;
//	}
//
//	public AZClientBuilder setName(String name) {
//		this.name = name;
//		return this;
//	}
//
//	public AZClientBuilder setKey(String key) {
//		this.key = key;
//		return this;
//	}
//
//	public AZClientBuilder setBlobURI(String blobURI) {
//		this.blobURI = blobURI;
//		return this;
//	}
//
//	public AZClientBuilder setSasURL(String sasURL) {
//		this.sasURL = sasURL;
//		return this;
//	}
//
//	@Override
//	public AZClientBuilder pullValuesFromSystem() {
//		AppCloudClientProperties clientProps = new AppCloudClientProperties();
//		setRClonePath(RCLONE_PATH);
//		setStorage(clientProps.get(STORAGE));
//		setConnectionString(clientProps.get(AZ_CONN_STRING));
//		setName(clientProps.get(AZ_NAME));
//		setKey(clientProps.get(AZ_KEY));
//		setBlobURI(clientProps.get(AZ_URI));
//		setSasURL(clientProps.get(SAS_URL));
//		return this;
//	}
//}
