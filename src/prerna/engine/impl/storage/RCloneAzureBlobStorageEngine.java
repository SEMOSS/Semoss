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
package prerna.engine.impl.storage;

import java.io.File;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.sas.BlobContainerSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;

import prerna.engine.api.StorageTypeEnum;
import prerna.util.Utility;

public class RCloneAzureBlobStorageEngine extends AbstractRCloneStorageEngine {

	private static final Logger classLogger = LogManager.getLogger(RCloneAzureBlobStorageEngine.class);

	{
		this.PROVIDER = "azureblob";
	}

	public static final String AZ_ACCOUNT_NAME = "AZ_ACCOUNT_NAME";
	public static final String AZ_PRIMARY_KEY = "AZ_PRIMARY_KEY";
	public static final String AZ_USE_MSI = "AZ_USE_MSI";

	public static final String AZ_CONN_STRING = "AZ_CONN_STRING";
	public static final String AZ_SAS_URL = "SAS_URL";
	public static final String AZ_URI = "AZ_URI";

	public static final String AZ_GENERATE_DYNAMIC_SAS = "AZ_GENERATE_DYNAMIC_SAS";

	private transient String accountName = null;
	private transient String primaryKey = null;
	private transient boolean useMsi = false;
	private transient boolean keysProvided = false;

	private transient BlobServiceClient serviceClient = null;
	private transient String connectionString = null;

	private transient boolean generateDynamicSAS = true;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.accountName = smssProp.getProperty(AZ_ACCOUNT_NAME);
		this.primaryKey = smssProp.getProperty(AZ_PRIMARY_KEY);
		// determine if keys provided or not
		if (this.accountName != null && !this.accountName.isEmpty() && this.primaryKey != null
				&& !primaryKey.isEmpty()) {
			this.keysProvided = true;
		} else {
			this.keysProvided = false;
		}
		this.useMsi = Boolean.parseBoolean(smssProp.getProperty(AZ_USE_MSI, "false"));

		// default to using dynamic SAS
		this.generateDynamicSAS = Boolean.parseBoolean(smssProp.getProperty(AZ_GENERATE_DYNAMIC_SAS, "false"));

		if (this.generateDynamicSAS) {
			this.connectionString = smssProp.getProperty(AZ_CONN_STRING);
			createServiceClient();
		}
	}

	/**
	 * 
	 */
	public void createServiceClient() {
		this.serviceClient = new BlobServiceClientBuilder().connectionString(connectionString).buildClient();
	}

	/**
	 * 
	 * @param containerName
	 * @return
	 */
	public String getDynamicSAS(String containerName) {
		String retString = null;
		// Get container client
		BlobContainerClient containerClient = serviceClient.getBlobContainerClient(containerName);

		// Create container if it doesn't exist
		containerClient.createIfNotExists();

		// Generate SAS token
		BlobServiceSasSignatureValues sasValues = getSASConstraints();
		String sasToken = containerClient.generateSas(sasValues);

		// Build the full URL with SAS token
		retString = containerClient.getBlobContainerUrl() + "?" + sasToken;

		return retString;
	}

	/**
	 * 
	 * @return
	 */
	private BlobServiceSasSignatureValues getSASConstraints() {
		// set expiry time to current time + 5 minutes
		OffsetDateTime expiryTime = OffsetDateTime.now().plusMinutes(5);

		BlobContainerSasPermission permissions = new BlobContainerSasPermission().setListPermission(true)
				.setWritePermission(true).setCreatePermission(true).setReadPermission(true).setDeletePermission(true)
				.setAddPermission(true);

		return new BlobServiceSasSignatureValues(expiryTime, permissions);
	}

	@Override
	public String createRCloneConfig() throws IOException, InterruptedException {
		if (this.generateDynamicSAS) {
			classLogger.warn("Calling creation of rclone without passing in the container name to generate a SAS");
			classLogger.warn("Calling creation of rclone without passing in the container name to generate a SAS");
			classLogger.warn("Calling creation of rclone without passing in the container name to generate a SAS");
		}
		String rcloneConfig = Utility.getRandomString(10);
		runRcloneProcess(rcloneConfig, RCLONE, "config", "create", rcloneConfig, PROVIDER, "account", accountName,
				"key", primaryKey);
		return rcloneConfig;
	}

	public String createRCloneConfig(String containerName) throws IOException, InterruptedException {
		String rcloneConfig = Utility.getRandomString(10);

		if (this.generateDynamicSAS) {
			String sasUrl = getDynamicSAS(containerName);
			runRcloneProcess(rcloneConfig, RCLONE, "config", "create", rcloneConfig, PROVIDER, "sas_url", sasUrl);
		} else if (this.useMsi) {
			runRcloneProcess(rcloneConfig, RCLONE, "config", "create", rcloneConfig, PROVIDER, "use_msi", "true");
		} else if (this.keysProvided) {
			runRcloneProcess(rcloneConfig, RCLONE, "config", "create", rcloneConfig, PROVIDER, "account", accountName,
					"key", primaryKey);
		} else {
			runRcloneProcess(rcloneConfig, RCLONE, "config", "create", rcloneConfig, PROVIDER, "env_auth", "true");
		}

		return rcloneConfig;
	}

	@Override
	public boolean canReuseRcloneConfig() {
		return !this.generateDynamicSAS;
	}

	@Override
	public StorageTypeEnum getStorageType() {
		return StorageTypeEnum.RCLONE_AZURE;
	}

	/*
	 * 
	 * OVERRIDING THESE METHODS FROM BASE BECAUSE WE NEED TO FIGURE OUT THE
	 * CONTAINER WHEN USING DYNAMIC SAS
	 * 
	 */

	private String getContainerFromPath(String path) {
		if (path.startsWith("/") || path.startsWith("\\")) {
			path.substring(1);
		}
		File f = new File(path);
		while (f.getParentFile() != null) {
			f = f.getParentFile();
		}
		return f.getName();
	}

	/**
	 * List the folders/files in the path
	 */
	@Override
	public List<String> listWithConfig(String path, String rCloneConfig) throws IOException, InterruptedException {
		boolean delete = false;
		if (rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRCloneConfig(getContainerFromPath(path));
			delete = true;
		}
		try {
			String rClonePath = rCloneConfig + ":";
			if (path != null) {
				path = path.replace("\\", "/");
				if (!path.startsWith("/")) {
					rClonePath += "/" + path;
				} else {
					rClonePath += path;
				}
			}
			List<String> results = runRcloneFastListProcess(rCloneConfig, RCLONE, "lsf", rClonePath);
			return results;
		} finally {
			if (delete && rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}

	/**
	 * List the folders/files in the path
	 */
	@Override
	public List<Map<String, Object>> listDetailsWithConfig(String path, String rCloneConfig)
			throws IOException, InterruptedException {
		boolean delete = false;
		if (rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRCloneConfig(getContainerFromPath(path));
			delete = true;
		}
		try {
			return super.listDetailsWithConfig(path, rCloneConfig);
		} finally {
			if (delete && rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}

	@Override
	public StorageSyncStatus syncLocalToStorageWithConfig(String localPath, String storagePath, String rCloneConfig,
			Map<String, Object> metadata) throws IOException, InterruptedException {
		boolean delete = false;
		if (rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRCloneConfig(getContainerFromPath(storagePath));
			delete = true;
		}
		try {
			String rClonePath = rCloneConfig + ":";
			if (localPath == null || localPath.isEmpty()) {
				throw new NullPointerException("Must define the local location of the file to push");
			}
			if (storagePath == null || storagePath.isEmpty()) {
				throw new NullPointerException("Must define the location of the storage folder to move to");
			}

			storagePath = storagePath.replace("\\", "/");
			localPath = localPath.replace("\\", "/");

			if (!storagePath.startsWith("/")) {
				storagePath = "/" + storagePath;
			}
			rClonePath += storagePath;

			// Initialize metadata to an empty map if it is null
			if (metadata == null) {
				metadata = new HashMap<>();
			}

			List<String> values = new ArrayList<>(metadata.keySet().size() * 2 + 5);
			values.add(RCLONE);
			values.add("sync");
			values.add(localPath);
			values.add(rClonePath);
			values.add("--metadata");

			if (!metadata.isEmpty()) {
				for (String key : metadata.keySet()) {
					Object value = metadata.get(key);

					values.add("--metadata-set");
					// wrap around in quotes just in case ...
					values.add("\"" + key + "\"=\"" + value + "\"");
				}
			}

			runRcloneTransferProcess(rCloneConfig, values.toArray(new String[] {}));
		} finally {
			if (delete && rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}

		// rclone does the walking, so there is nothing to enumerate here. A failure
		// throws out of runRcloneTransferProcess, so reaching this point is success
		return StorageSyncStatus.of(storagePath, null, null, null);
	}

	@Override
	public void syncStorageToLocalWithConfig(String storagePath, String localPath, String rCloneConfig)
			throws IOException, InterruptedException {
		boolean delete = false;
		if (rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRCloneConfig(getContainerFromPath(storagePath));
			delete = true;
		}
		try {
			String rClonePath = rCloneConfig + ":";
			if (localPath == null || localPath.isEmpty()) {
				throw new NullPointerException("Must define the local location of the file to push");
			}
			if (storagePath == null || storagePath.isEmpty()) {
				throw new NullPointerException("Must define the location of the storage folder to move to");
			}

			storagePath = storagePath.replace("\\", "/");
			localPath = localPath.replace("\\", "/");

			if (!storagePath.startsWith("/")) {
				storagePath = "/" + storagePath;
			}
			rClonePath += storagePath;

			runRcloneTransferProcess(rCloneConfig, RCLONE, "sync", rClonePath, localPath);
		} finally {
			if (delete && rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}

	@Override
	public void copyToStorageWithConfig(String localFilePath, String storageFolderPath, String rCloneConfig,
			Map<String, Object> metadata) throws IOException, InterruptedException {
		boolean delete = false;
		if (rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRCloneConfig(getContainerFromPath(storageFolderPath));
			delete = true;
		}
		try {
			String rClonePath = rCloneConfig + ":";
			if (localFilePath == null || localFilePath.isEmpty()) {
				throw new NullPointerException("Must define the local location of the file to push");
			}
			if (storageFolderPath == null || storageFolderPath.isEmpty()) {
				throw new NullPointerException("Must define the location of the storage folder to move to");
			}

			storageFolderPath = storageFolderPath.replace("\\", "/");
			localFilePath = localFilePath.replace("\\", "/");

			if (!storageFolderPath.startsWith("/")) {
				storageFolderPath = "/" + storageFolderPath;
			}
			rClonePath += storageFolderPath;

			// Initialize metadata to an empty map if it is null
			if (metadata == null) {
				metadata = new HashMap<>();
			}

			List<String> values = new ArrayList<>(metadata.keySet().size() * 2 + 5);
			values.add(RCLONE);
			values.add("copy");
			values.add(localFilePath);
			values.add(rClonePath);
			values.add("--metadata");

			if (!metadata.isEmpty()) {
				for (String key : metadata.keySet()) {
					Object value = metadata.get(key);

					values.add("--metadata-set");
					// wrap around in quotes just in case ...
					values.add("\"" + key + "\"=\"" + value + "\"");
				}
			}

			runRcloneTransferProcess(rCloneConfig, values.toArray(new String[] {}));
		} finally {
			if (delete && rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}

	@Override
	public void copyToLocalWithConfig(String storageFilePath, String localFolderPath, String rCloneConfig)
			throws IOException, InterruptedException {
		boolean delete = false;
		if (rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRCloneConfig(getContainerFromPath(storageFilePath));
			delete = true;
		}
		try {
			String rClonePath = rCloneConfig + ":";
			if (storageFilePath == null || storageFilePath.isEmpty()) {
				throw new NullPointerException("Must define the storage location of the file to download");
			}
			if (localFolderPath == null || localFolderPath.isEmpty()) {
				throw new NullPointerException("Must define the location of the local folder to move to");
			}

			storageFilePath = storageFilePath.replace("\\", "/");
			localFolderPath = localFolderPath.replace("\\", "/");

			if (!storageFilePath.startsWith("/")) {
				storageFilePath = "/" + storageFilePath;
			}
			rClonePath += storageFilePath;

			runRcloneTransferProcess(rCloneConfig, RCLONE, "copy", rClonePath, localFolderPath);
		} finally {
			if (delete && rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}

	@Override
	public void deleteFromStorageWithConfig(String storagePath, boolean leaveFolderStructure, String rCloneConfig)
			throws IOException, InterruptedException {
		boolean delete = false;
		if (rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRCloneConfig(getContainerFromPath(storagePath));
			delete = true;
		}
		try {
			String rClonePath = rCloneConfig + ":";
			if (storagePath == null || storagePath.isEmpty()) {
				throw new NullPointerException("Must define the storage location of the file to delete");
			}

			storagePath = storagePath.replace("\\", "/");

			if (!storagePath.startsWith("/")) {
				storagePath = "/" + storagePath;
			}
			rClonePath += storagePath;

			if (leaveFolderStructure) {
				// always do delete
				runRcloneDeleteFileProcess(rCloneConfig, RCLONE, "delete", rClonePath);
			} else {
				// we can only do purge on a folder
				// so need to check
				List<String> results = runRcloneFastListProcess(rCloneConfig, RCLONE, "lsf", rClonePath);
				if (results.size() == 1 && !results.get(0).endsWith("/")) {
					runRcloneDeleteFileProcess(rCloneConfig, RCLONE, "delete", rClonePath);
				} else {
					runRcloneDeleteFileProcess(rCloneConfig, RCLONE, "purge", rClonePath);
				}
			}
		} finally {
			if (delete && rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}

	@Override
	public void deleteFolderFromStorageWithConfig(String storageFolderPath, String rCloneConfig)
			throws IOException, InterruptedException {
		boolean delete = false;
		if (rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRCloneConfig(getContainerFromPath(storageFolderPath));
			delete = true;
		}
		try {
			String rClonePath = rCloneConfig + ":";
			if (storageFolderPath == null || storageFolderPath.isEmpty()) {
				throw new NullPointerException("Must define the storage location of the folder to delete");
			}

			storageFolderPath = storageFolderPath.replace("\\", "/");

			if (!storageFolderPath.startsWith("/")) {
				storageFolderPath = "/" + storageFolderPath;
			}
			rClonePath += storageFolderPath;

			runRcloneDeleteFileProcess(rCloneConfig, RCLONE, "purge", rClonePath);
		} finally {
			if (delete && rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}

}
