/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.engine.api;

import prerna.engine.impl.storage.AzureBlobStorageEngine;
import prerna.engine.impl.storage.CephStorageEngine;
import prerna.engine.impl.storage.DeveloperLocalFileSystemStorageEngine;
import prerna.engine.impl.storage.GoogleCloudStorageEngine;
import prerna.engine.impl.storage.JCIFSStorageEngine;
import prerna.engine.impl.storage.LocalFileSystemStorageEngine;
import prerna.engine.impl.storage.MinioStorageEngine;
import prerna.engine.impl.storage.RCloneAzureBlobStorageEngine;
import prerna.engine.impl.storage.RCloneGoogleCloudStorageEngine;
import prerna.engine.impl.storage.RCloneMinioStorageEngine;
import prerna.engine.impl.storage.RCloneS3StorageEngine;
import prerna.engine.impl.storage.S3StorageEngine;
import prerna.engine.impl.storage.SFTPStorageEngine;

public enum StorageTypeEnum {

	// @formatter:off
	CEPH("CEPH", CephStorageEngine.class.getName()),
	

	GOOGLE_CLOUD_STORAGE("GOOGLE_CLOUD_STORAGE", GoogleCloudStorageEngine.class.getName()),
	@Deprecated
	GOOGLE_CLOUD_NATIVE_STORAGE("GOOGLE_CLOUD_NATIVE_STORAGE", GoogleCloudStorageEngine.class.getName()),
	
	JCIFS("JCIFS", JCIFSStorageEngine.class.getName()),
	LOCAL_FILE_SYSTEM("LOCAL_FILE_SYSTEM", LocalFileSystemStorageEngine.class.getName()),
	DEVELOPER_LOCAL_FILE_SYSTEM("DEVELOPER_LOCAL_FILE_SYSTEM", DeveloperLocalFileSystemStorageEngine.class.getName()),
	
	MICROSOFT_AZURE_BLOB_STORAGE("MICROSOFT_AZURE_BLOB_STORAGE", AzureBlobStorageEngine.class.getName()),
	@Deprecated
	MICROSOFT_AZURE_NATIVE_BLOB_STORAGE("MICROSOFT_AZURE_NATIVE_BLOB_STORAGE", AzureBlobStorageEngine.class.getName()),
	
	MINIO("MINIO", MinioStorageEngine.class.getName()),
	SFTP("SFTP", SFTPStorageEngine.class.getName()),
	SMB_CIFS("SMB_CIFS", JCIFSStorageEngine.class.getName()),
	
	// all now default to native s3
	@Deprecated 
	AMAZON_S3("AMAZON_S3", S3StorageEngine.class.getName()),
	@Deprecated
	AMAZON_S3_NATIVE("AMAZON_S3_NATIVE", S3StorageEngine.class.getName()),
	S3("S3", S3StorageEngine.class.getName()),
	
	// RClone versions
	RCLONE_AZURE("RCLONE_AZURE", RCloneAzureBlobStorageEngine.class.getName()),
	RCLONE_GOOGLE("RCLONE_GOOGLE", RCloneGoogleCloudStorageEngine.class.getName()),
	RCLONE_MINIO("RCLONE_MINIO", RCloneMinioStorageEngine.class.getName()),
	RCLONE_S3("RCLONE_S3", RCloneS3StorageEngine.class.getName()),
	; 
	// @formatter:oN

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
