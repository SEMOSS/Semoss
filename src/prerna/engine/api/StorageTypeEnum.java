package prerna.engine.api;

public enum StorageTypeEnum {

	AMAZON_S3("AMAZON_S3","prerna.engine.impl.storage.S3StorageEngine"),
	GOOGLE_CLOUD_STORAGE("GOOGLE_CLOUD_STORAGE","prerna.engine.impl.storage.GoogleCloudStorageEngine"),
//	GOOGLE_DRIVE("GOOGLE_DRIVE",""),
	MICROSOFT_AZURE_BLOB_STORAGE("MICROSOFT_AZURE_BLOB_STORAGE","prerna.engine.impl.storage.AzureBlobStorageEngine"),
	MINIO("MINIO","prerna.engine.impl.storage.MinioStorageEngine"),
	SFTP("SFTP","prerna.engine.impl.storage.SFTPStorageEngine");
	
	private String storageName;
	private String storageClass;
	
	StorageTypeEnum(String storageName, String storageClass) {
		this.storageName = storageName;
		this.storageClass = storageClass;
	}
	
	/**
	 * 
	 * @return
	 */
	public String getStorageClass() {
		return this.storageClass;
	}
	
	/**
	 * 
	 * @return
	 */
	public String getStorageName() {
		return this.storageName;
	}
	
	/**
	 * 
	 * @param name
	 * @return
	 */
	public static StorageTypeEnum getEnumFromName(String name) {
		StorageTypeEnum[] allValues = values();
		for(StorageTypeEnum v : allValues) {
			if(v.getStorageName().equalsIgnoreCase(name)) {
				return v;
			}
		}
		throw new IllegalArgumentException("Invalid input for name " + name);
	}
}
