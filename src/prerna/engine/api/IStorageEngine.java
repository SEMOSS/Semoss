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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import prerna.logging.IgnoreEngineLogging;

public interface IStorageEngine extends IEngine {

	// this is what the FE sends for the type of storage we are creating
	// as a result, cannot be a key in the smss file
	String STORAGE_TYPE = "STORAGE_TYPE";

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	StorageTypeEnum getStorageType();

	/**
	 * 
	 * @param path
	 * @return
	 * @throws Exception
	 */
	List<String> list(String path) throws Exception;

	/**
	 * 
	 * @param path
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	List<Map<String, Object>> listDetails(String path) throws Exception;

	/**
	 * 
	 * @param localPath
	 * @param storagePath
	 * @param metadata
	 * @throws Exception
	 */
	void syncLocalToStorage(String localPath, String storagePath, Map<String, Object> metadata) throws Exception;

	/**
	 * 
	 * @param storagePath
	 * @param localPath
	 * @throws Exception
	 */
	void syncStorageToLocal(String storagePath, String localPath) throws Exception;

	/**
	 * Copy local files to a storage folder path.
	 * 
	 * @param localFilePath     the local file or folder path(s) to upload
	 * @param storageFolderPath the destination path in storage
	 * @param metadata          optional metadata to attach to the uploaded objects
	 * @throws Exception if the upload fails
	 */
	void copyToStorage(String localFilePath, String storageFolderPath, Map<String, Object> metadata) throws Exception;

	/**
	 * Check if object versioning is enabled for this storage engine.
	 * 
	 * @return true if versioning is enabled, false by default
	 */
	default boolean isVersioningEnabled() {
		return false;
	}

	/**
	 * Copy local files to storage and return the version identifier of the uploaded object.
	 * Only supported by engines with versioning enabled (S3, GCS).
	 * Default implementation delegates to {@link #copyToStorage} and returns null.
	 * 
	 * @param localFilePath     the local file or folder path(s) to upload
	 * @param storageFolderPath the destination path in storage
	 * @param metadata          optional metadata to attach to the uploaded objects
	 * @return the version identifier (S3 versionId or GCS generation), or null if not supported
	 * @throws Exception if the upload fails
	 */
	default String copyToStorageVersioned(String localFilePath, String storageFolderPath,
			Map<String, Object> metadata) throws Exception {
		copyToStorage(localFilePath, storageFolderPath, metadata);
		return null;
	}

	/**
	 * 
	 * @param storageFilePath
	 * @param localFolderPath
	 * @throws Exception
	 */
	void copyToLocal(String storageFilePath, String localFolderPath) throws Exception;

	/**
	 * 
	 * @param storageFilePath
	 * @throws Exception
	 */
	void deleteFromStorage(String storagePath) throws Exception;

	/**
	 * 
	 * @param storagePath
	 * @param leaveFolderStructure
	 * @throws Exception
	 */
	void deleteFromStorage(String storagePath, boolean leaveFolderStructure) throws Exception;

	/**
	 * 
	 * @param storageFolderPath
	 * @throws Exception
	 */
	void deleteFolderFromStorage(String storageFolderPath) throws Exception;

	/**
	 * Reads a blob/file from storage directly into memory as a byte array.
	 * 
	 * @param storagePath the path to the file in storage
	 * @return the file contents as a byte array
	 * @throws Exception if the operation is not supported or fails
	 */
	default byte[] readBlobToMemory(String storagePath) throws Exception {
		throw new UnsupportedOperationException("readBlobToMemory is not supported by this storage engine");
	}
	
	
	/**
	 * Update the metadata for a specific blob/file.
	 * 
	 * @param storagePath the path to the file in storage
	 * @param metadata the metadata to add to the blob/file
	 * @throws Exception if the operation is not supported or fails
	 */
	default void updateBlobMetadata(String storagePath, Map<String, Object> metadata) throws Exception {
		throw new UnsupportedOperationException("updateBlobMetadata is not supported by this storage engine"); 
	}

	/**
	 * Copy a specific version of a file from storage to local.
	 * 
	 * @param storageFilePath the path to the file in storage
	 * @param localFolderPath the local folder to download to
	 * @param versionId       the version identifier (S3 versionId or GCS generation number)
	 * @throws Exception if the operation is not supported or fails
	 */
	default void copyToLocal(String storageFilePath, String localFolderPath, String versionId) throws Exception {
		copyToLocal(storageFilePath, localFolderPath);
	}
}
