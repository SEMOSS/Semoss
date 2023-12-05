package prerna.engine.api;

import prerna.engine.impl.storage.AzureBlobStorageEngine;
import prerna.engine.impl.storage.CephStorageEngine;
import prerna.engine.impl.storage.GoogleCloudStorageEngine;
import prerna.engine.impl.storage.LocalFileSystemStorageEngine;
import prerna.engine.impl.storage.MinioStorageEngine;
import prerna.engine.impl.storage.S3StorageEngine;
import prerna.engine.impl.storage.SFTPStorageEngine;

public enum StorageTypeEnum {

	AMAZON_S3("AMAZON_S3", S3StorageEngine.class.getName()),
	CEPH("CEPH", CephStorageEngine.class.getName()),
	GOOGLE_CLOUD_STORAGE("GOOGLE_CLOUD_STORAGE", GoogleCloudStorageEngine.class.getName()),
	LOCAL_FILE_SYSTEM("LOCAL_FILE_SYSTEM", LocalFileSystemStorageEngine.class.getName()),
//	GOOGLE_DRIVE("GOOGLE_DRIVE",""),
	MICROSOFT_AZURE_BLOB_STORAGE("MICROSOFT_AZURE_BLOB_STORAGE", AzureBlobStorageEngine.class.getName()),
	MINIO("MINIO", MinioStorageEngine.class.getName()),
	SFTP("SFTP", SFTPStorageEngine.class.getName());
	
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
