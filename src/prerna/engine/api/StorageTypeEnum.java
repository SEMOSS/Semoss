package prerna.engine.api;

import prerna.engine.impl.storage.AWSNativeBlobStorageEngine;
import prerna.engine.impl.storage.AzureBlobStorageEngine;
import prerna.engine.impl.storage.AzureNativeBlobStorageEngine;
import prerna.engine.impl.storage.CephStorageEngine;
import prerna.engine.impl.storage.GoogleCloudNativeBlobStorageEngine;
import prerna.engine.impl.storage.GoogleCloudStorageEngine;
import prerna.engine.impl.storage.JCIFSStorageEngine;
import prerna.engine.impl.storage.LocalFileSystemStorageEngine;
import prerna.engine.impl.storage.MinioStorageEngine;
import prerna.engine.impl.storage.S3StorageEngine;
import prerna.engine.impl.storage.SFTPStorageEngine;

/**
 * Enumeration defining all available storage engine types for file and data storage.
 * 
 * <p>This enum provides a comprehensive registry of storage engines that support various
 * storage backends including cloud services, distributed storage systems, network
 * file systems, and local storage. Each enum value maps a storage type identifier
 * to its corresponding implementation class.</p>
 * 
 * <p>Supported storage categories include:</p>
 * <ul>
 *   <li><strong>Cloud Storage:</strong> Amazon S3, Google Cloud Storage, Azure Blob Storage</li>
 *   <li><strong>Distributed Storage:</strong> Ceph, MinIO object storage</li>
 *   <li><strong>Network Storage:</strong> SFTP, SMB/CIFS network shares</li>
 *   <li><strong>Local Storage:</strong> Local file system access</li>
 *   <li><strong>Native APIs:</strong> Cloud-native SDK implementations for optimal performance</li>
 * </ul>
 * 
 * <p>Storage engines provide standardized file operations across different backends,
 * enabling seamless data access regardless of the underlying storage infrastructure.</p>
 * 
 * @see {@link IStorageEngine} for the base storage engine interface
 * @see {@link S3StorageEngine} for Amazon S3 integration
 * @see {@link AzureBlobStorageEngine} for Azure Blob Storage
 * @see {@link GoogleCloudStorageEngine} for Google Cloud Storage
 * @author SEMOSS
 */
public enum StorageTypeEnum {

	// Cloud Storage Services
	/** Amazon S3 object storage with generic S3 API compatibility */
	AMAZON_S3("AMAZON_S3", S3StorageEngine.class.getName()),
	/** Amazon S3 with native AWS SDK for optimal performance and features */
	AMAZON_S3_NATIVE("AMAZON_S3_NATIVE", AWSNativeBlobStorageEngine.class.getName()),
	/** Google Cloud Storage with generic API compatibility */
	GOOGLE_CLOUD_STORAGE("GOOGLE_CLOUD_STORAGE", GoogleCloudStorageEngine.class.getName()),
	/** Google Cloud Storage with native GCP SDK for optimal integration */
	GOOGLE_CLOUD_NATIVE_STORAGE("GOOGLE_CLOUD_NATIVE_STORAGE", GoogleCloudNativeBlobStorageEngine.class.getName()),
	/** Microsoft Azure Blob Storage with generic API compatibility */
	MICROSOFT_AZURE_BLOB_STORAGE("MICROSOFT_AZURE_BLOB_STORAGE", AzureBlobStorageEngine.class.getName()),
	/** Microsoft Azure Blob Storage with native Azure SDK */
	MICROSOFT_AZURE_NATIVE_BLOB_STORAGE("MICROSOFT_AZURE_NATIVE_BLOB_STORAGE", AzureNativeBlobStorageEngine.class.getName()),
	
	// Distributed and Object Storage Systems
	/** Ceph distributed storage system with S3-compatible API */
	CEPH("CEPH", CephStorageEngine.class.getName()),
	/** MinIO high-performance object storage compatible with S3 API */
	MINIO("MINIO", MinioStorageEngine.class.getName()),
	
	// Local and Network File Systems
	/** Local file system storage for single-node deployments */
	LOCAL_FILE_SYSTEM("LOCAL_FILE_SYSTEM", LocalFileSystemStorageEngine.class.getName()),
	/** JCIFS library for SMB/CIFS network share access */
	JCIFS("JCIFS", JCIFSStorageEngine.class.getName()),
	/** SFTP secure file transfer protocol for remote file access */
	SFTP("SFTP", SFTPStorageEngine.class.getName()),
	/** SMB/CIFS Windows network shares using JCIFS implementation */
	SMB_CIFS("SMB_CIFS", JCIFSStorageEngine.class.getName());

	/** The human-readable name identifier for this storage type */
	private String storageName;
	/** The fully qualified class name of the implementing storage engine */
	private String storageClass;
	
	/**
	 * Constructs a storage type enum with the specified name and implementation class.
	 * 
	 * @param storageName The human-readable identifier for this storage type
	 * @param storageClass The fully qualified class name of the implementation
	 */
	StorageTypeEnum(String storageName, String storageClass) {
		this.storageName = storageName;
		this.storageClass = storageClass;
	}
	
	/**
	 * Gets the fully qualified class name of the implementing storage engine.
	 * 
	 * @return The complete class path for the storage engine implementation
	 */
	public String getStorageClass() {
		return this.storageClass;
	}
	
	/**
	 * Gets the human-readable name identifier for this storage type.
	 * 
	 * @return The storage type name used for identification and configuration
	 */
	public String getStorageName() {
		return this.storageName;
	}
	
	/**
	 * Retrieves the storage type enum that matches the specified name.
	 * 
	 * <p>This method performs a case-insensitive search through all available
	 * storage types to find the one that matches the provided name. This is
	 * commonly used for configuration parsing and dynamic engine selection.</p>
	 * 
	 * @param name The storage type name to search for (case-insensitive)
	 * @return The matching {@link StorageTypeEnum} instance
	 * @throws IllegalArgumentException If no storage type matches the provided name
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
