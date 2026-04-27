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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;

import prerna.engine.api.StorageTypeEnum;
import prerna.util.Utility;

public class AzureNativeBlobStorageEngine extends AbstractStorageEngine {

	private static final Logger classLogger = LogManager.getLogger(AzureNativeBlobStorageEngine.class);

	private static final String AZ_CONN_STRING = "AZ_CONN_STRING";

	private transient String connectionString = null;
	private transient BlobServiceClient blobServiceClient;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		this.connectionString = smssProp.getProperty(AZ_CONN_STRING);
		if (this.connectionString == null || this.connectionString.isEmpty()) {
			classLogger.error("Azure Blob connection string is missing, cannot initialize Azure Blob client.");
			throw new IllegalStateException("Azure Blob connection string is required.");
		}
		createServiceClient();
	}

	public void createServiceClient() {
		this.blobServiceClient = new BlobServiceClientBuilder().connectionString(this.connectionString).buildClient();
		classLogger.info("Azure Blob Service client created successfully.");
	}

	@Override
	public StorageTypeEnum getStorageType() {
		return StorageTypeEnum.MICROSOFT_AZURE_NATIVE_BLOB_STORAGE;
	}

	@Override
	public List<String> list(String storagePath) throws BlobStorageException {
		List<Map<String, Object>> details = listDetails(storagePath);
		List<String> fileList = new ArrayList<>(details.size());
		for (Map<String, Object> item : details) {
			Object nameObj = item.get("Name");
			if (nameObj == null) {
				continue;
			}
			String name = nameObj.toString();
			boolean isDir = Boolean.TRUE.equals(item.get("IsDir"));
			fileList.add(isDir ? name + "/" : name);
		}
		return fileList;
	}

	@Override
	public List<Map<String, Object>> listDetails(String storagePath) throws BlobStorageException {
		List<Map<String, Object>> detailsList = new ArrayList<>();
		String[] containerAndPath = extractContainerAndPath(storagePath);
		String containerName = containerAndPath[0];
		String blobDirectory = Utility.normalizePath(containerAndPath[1]).replace("\\", "/");
		while (blobDirectory.startsWith("/")) {
			blobDirectory = blobDirectory.substring(1);
		}
		while (blobDirectory.endsWith("/")) {
			blobDirectory = blobDirectory.substring(0, blobDirectory.length() - 1);
		}
		String prefix = blobDirectory.isEmpty() ? "" : blobDirectory + "/";

		BlobContainerClient containerClient = this.blobServiceClient.getBlobContainerClient(containerName);
		ListBlobsOptions listBlobsOptions = new ListBlobsOptions().setPrefix(prefix.isEmpty() ? null : prefix);
		for (BlobItem blobItem : containerClient.listBlobsByHierarchy("/", listBlobsOptions, null)) {
			String itemPath = blobItem.getName();
			if (itemPath == null || itemPath.equals(prefix)) {
				continue;
			}
			String name = prefix.isEmpty() ? itemPath : itemPath.substring(prefix.length());
			if (name.endsWith("/")) {
				name = name.substring(0, name.length() - 1);
			}
			if (name.isEmpty() || name.contains("/")) {
				continue;
			}

			boolean isDir = blobItem.isPrefix();
			Map<String, Object> blobMap = new HashMap<>();
			blobMap.put("Path", blobDirectory.isEmpty() ? "/" + name : "/" + blobDirectory + "/" + name);
			blobMap.put("Name", name);
			blobMap.put("IsDir", isDir);

			if (isDir) {
				blobMap.put("Size", 0L);
				blobMap.put("MimeType", "inode/directory");
				blobMap.put("ModTime", null);
				blobMap.put("Metadata", Collections.emptyMap());
			} else {
				BlobClient blobClient = containerClient.getBlobClient(itemPath);
				BlobProperties properties = blobClient.getProperties();
				Map<String, String> metadata = properties.getMetadata();
				blobMap.put("Size", properties.getBlobSize());
				blobMap.put("MimeType", properties.getContentType());
				blobMap.put("ModTime",
						properties.getLastModified() == null ? null : properties.getLastModified().toString());
				blobMap.put("Metadata", (metadata == null || metadata.isEmpty()) ? Collections.emptyMap() : metadata);
			}
			detailsList.add(blobMap);
		}
		return detailsList;
	}

	@Override
	public void syncLocalToStorage(String localPath, String storagePath, Map<String, Object> metadata)
			throws Exception {
		// Extract container and blob directory
		String[] containerAndPath = extractContainerAndPath(storagePath);
		String containerName = containerAndPath[0];
		String blobDirectory = containerAndPath[1];

		BlobContainerClient containerClient = this.blobServiceClient.getBlobContainerClient(containerName);
		Path localFilePath = Paths.get(localPath);

		List<String> uploadedFiles = new ArrayList<>();
		List<String> failedFiles = new ArrayList<>();
		boolean found = false;

		try {
			if (!Files.exists(localFilePath)) {
				throw new IllegalArgumentException("Invalid path: " + localPath);
			}

			Path localBasePath = Files.isDirectory(localFilePath) ? localFilePath : localFilePath.getParent();
			// Remove empty directories locally
			deleteEmptyDirectories(localFilePath);
			// Delete extra blobs from azure storage
			syncStorageDeletion(containerClient, blobDirectory, localBasePath);

			Files.walk(localFilePath).filter(Files::isRegularFile).forEach(file -> {
				try {
					uploadedFiles.add(uploadFileToBlob(file, localFilePath, containerClient, blobDirectory, metadata));
				} catch (Exception e) {
					failedFiles.add(file.toString());
					classLogger.error("Failed to upload file: {}", file, e);
				}
			});
			found = true;
		} catch (Exception e) {
			classLogger.error("Sync operation failed. Rolling back failed uploads.", e);
			rollbackUploads(containerClient, failedFiles);
			throw e;
		}

		if (uploadedFiles.isEmpty()) {
			classLogger.info("No files were uploaded.");
		} else {
			classLogger.info("Successfully uploaded files: {}", uploadedFiles);
		}

		classLogger.info(found ? "Sync completed successfully for: {}" : "No files found to sync for: {}", storagePath);
	}

	@Override
	public void syncStorageToLocal(String storagePath, String localPath) throws Exception {
		// Extract container and blob directory
		String[] containerAndPath = extractContainerAndPath(storagePath);
		String containerName = containerAndPath[0];
		String blobDirectory = normalizeStoragePrefixPath(containerAndPath[1]);
		Path localDirectory = Paths.get(localPath);

		BlobContainerClient containerClient = this.blobServiceClient.getBlobContainerClient(containerName);
		Files.createDirectories(localDirectory); // Ensure local directory exists

		Set<String> cloudFiles = new HashSet<>();
		List<String> downloadedFiles = new ArrayList<>(), failedFiles = new ArrayList<>();
		boolean found = false;

		for (BlobItem blobItem : containerClient
				.listBlobs(new ListBlobsOptions().setPrefix(blobDirectory.isEmpty() ? null : blobDirectory), null)) {
			BlobClient blobClient = containerClient.getBlobClient(blobItem.getName());
			BlobProperties properties = blobClient.getProperties();
			// Delete empty folder from azure storage (zero-byte blob)
			deleteEmptyBlobs(containerClient);

			String relativePath = resolveRelativeStoragePath(blobItem.getName(), blobDirectory);
			if (relativePath == null) {
				continue;
			}

			Path localFilePath = localDirectory.resolve(relativePath.replace("/", File.separator));
			cloudFiles.add(localFilePath.toString());
			Files.createDirectories(localFilePath.getParent());

			if (Files.isDirectory(localFilePath)) {
				continue; // Skip directories
			}

			try {
				boolean shouldDownload = !Files.exists(localFilePath);
				if (!shouldDownload) {
					FileTime localModifiedTime = Files.getLastModifiedTime(localFilePath);
					long localFileSize = Files.size(localFilePath);
					// Download if Azure file is newer OR file size is different
					shouldDownload = properties.getBlobSize() != localFileSize
							|| properties.getLastModified().toInstant().toEpochMilli() > localModifiedTime.toMillis();
				}
				if (shouldDownload) {
					boolean isUpdated = Files.exists(localFilePath); // Check if file already exists
					retryOperation(() -> blobClient.downloadToFile(localFilePath.toString(), true),
							"Syncing file to local: " + blobItem.getName());
					downloadedFiles.add(blobItem.getName());
					classLogger.info(isUpdated ? "Updated file: {}" : "Downloaded new file: {}", localFilePath);
				}
				found = true;
			} catch (Exception e) {
				failedFiles.add(relativePath);
				classLogger.error("Failed to sync file: {}", blobItem.getName(), e);
			}
		}
		// Delete local files not present in Azure
		Files.walk(localDirectory).filter(Files::isRegularFile)
				.filter(localFile -> !cloudFiles.contains(localFile.toString())).forEach(localFile -> {
					try {
						Files.delete(localFile);
						classLogger.info("Deleted extra local file: {}", localFile);
					} catch (IOException e) {
						classLogger.error("Failed to delete extra file: {}", localFile, e);
					}
				});

		// Delete Empty Directories Locally
		deleteEmptyDirectories(localDirectory);

		if (downloadedFiles.isEmpty()) {
			classLogger.info("No files were downloaded.");
		} else {
			classLogger.info("Successfully downloaded files: {}", downloadedFiles);
		}
		if (!failedFiles.isEmpty()) {
			classLogger.error("Some files failed to sync. Rolling back...");
			rollbackDownloads(failedFiles, localDirectory);
		}
		classLogger.info(found ? "Sync completed successfully for: {}" : "No files found to sync for: {}", storagePath);
	}

	@Override
	public void copyToStorage(String localFilePath, String storageFolderPath, Map<String, Object> metadata)
			throws Exception {
		// Extract container and blob directory
		String[] containerAndPath = extractContainerAndPath(storageFolderPath);
		String containerName = containerAndPath[0];
		String blobDirectory = containerAndPath[1];

		List<String> uploadedFiles = new ArrayList<>();
		List<String> failedFiles = new ArrayList<>();
		boolean found = false;
		BlobContainerClient containerClient = this.blobServiceClient.getBlobContainerClient(containerName);
		List<Path> paths = parseLocalPaths(localFilePath);
		for (Path filePath : paths) {
			if (!Files.exists(filePath)) {
				classLogger.error("File not found: {}", filePath);
				failedFiles.add(filePath.toString());
				continue;
			}
			// Delete empty directories before upload
			deleteEmptyDirectories(filePath);

			if (Files.isDirectory(filePath)) {
				try (Stream<Path> fileStream = Files.walk(filePath).filter(Files::isRegularFile)) {
					fileStream.forEach(file -> {
						try {
							uploadedFiles.add(uploadFile(containerClient, filePath, file, blobDirectory, metadata));
						} catch (Exception e) {
							failedFiles.add(file.toString());
							classLogger.error("Failed to upload file: {}", file, e);
							rollbackUploads(containerClient, failedFiles);
						}
					});
					found = true;
				}
			} else {
				try {
					uploadedFiles
							.add(uploadFile(containerClient, filePath.getParent(), filePath, blobDirectory, metadata));
					found = true;
				} catch (Exception e) {
					failedFiles.add(filePath.toString());
					classLogger.error("Failed to upload file: {}", filePath, e);
					rollbackUploads(containerClient, failedFiles);
				}
			}
		}
		// Delete empty folder from azure storage (zero-byte blob)
		deleteEmptyBlobs(containerClient);
		if (uploadedFiles.isEmpty()) {
			classLogger.info("No files were uploaded.");
		} else {
			classLogger.info("Successfully uploaded files: {}", uploadedFiles);
		}
		classLogger.info(found ? "Copy completed successfully for: {}" : "No files found to copy for: {}",
				storageFolderPath);
	}

	@Override
	public void copyToLocal(String storageFilePath, String localFolderPath) throws Exception {
		// Extract container and blob directory
		String[] containerAndPath = extractContainerAndPath(storageFilePath);
		String containerName = containerAndPath[0];
		String blobDirectory = containerAndPath[1];
		BlobContainerClient containerClient = this.blobServiceClient.getBlobContainerClient(containerName);
		Path localDirectory = Paths.get(localFolderPath);
		List<String> paths = parseStorageObjectPaths(blobDirectory);
		// Ensure local directory exists
		Files.createDirectories(localDirectory);

		List<String> downloadedFiles = new ArrayList<>(), failedFiles = new ArrayList<>();
		boolean found = false;
		for (String path : paths) {
			String requestedPath = normalizeStoragePrefixPath(path);
			Iterable<BlobItem> getBlobItems = requestedPath.isEmpty() ? containerClient.listBlobs()
					: containerClient.listBlobs(new ListBlobsOptions().setPrefix(requestedPath), null);

			for (BlobItem blobItem : getBlobItems) {
				String blobName = blobItem.getName();
				BlobClient blobClient = containerClient.getBlobClient(blobName);

				// Delete empty folder from azure storage (zero-byte blob)
				deleteEmptyBlobs(containerClient);

				String relativePath = resolveRelativeStoragePath(blobName, requestedPath);
				if (relativePath == null) {
					continue;
				}

				Path localFilePath = localDirectory.resolve(relativePath.replace("/", File.separator));
				try {
					Files.createDirectories(localFilePath.getParent());
					retryOperation(() -> blobClient.downloadToFile(localFilePath.toString(), true),
							"Downloading file: " + blobName);
					downloadedFiles.add(blobName);
					classLogger.info("Downloaded file: {}", localFilePath);
					found = true;
				} catch (Exception e) {
					failedFiles.add(relativePath);
					classLogger.error("Failed to download: {}", blobName, e);
				}
			}
		}

		// Delete empty directories after download
		deleteEmptyDirectories(localDirectory);
		if (downloadedFiles.isEmpty()) {
			classLogger.info("No files were downloaded.");
		} else {
			classLogger.info("Successfully downloaded files: {}", downloadedFiles);
		}
		if (!failedFiles.isEmpty()) {
			classLogger.error("Some files failed to download. Retrying...");
			rollbackDownloads(failedFiles, localDirectory);
		}
		classLogger.info(found ? "Copy completed successfully for: {}" : "No files found to copy for: {}",
				storageFilePath);
	}

	@Override
	public void deleteFromStorage(String storagePath) throws Exception {
		String[] containerAndPath = extractContainerAndPath(storagePath);
		String containerName = containerAndPath[0];
		String blobDirectory = normalizeStoragePrefixPath(containerAndPath[1]);

		List<String> deletedFiles = new ArrayList<>();
		List<String> failedFiles = new ArrayList<>();

		BlobContainerClient containerClient = this.blobServiceClient.getBlobContainerClient(containerName);

		// List all blobs for deletion
		Iterable<BlobItem> blobItems = blobDirectory.isEmpty() ? containerClient.listBlobs()
				: containerClient.listBlobs(new ListBlobsOptions().setPrefix(blobDirectory), null);

		boolean hasFilesToDelete = false;

		for (BlobItem blobItem : blobItems) {
			String blobName = blobItem.getName();
			String relativePath = resolveRelativeStoragePath(blobName, blobDirectory);
			if (blobDirectory.isEmpty() || (relativePath != null && !relativePath.contains("/"))) {
				hasFilesToDelete = true;
				if (deleteBlob(containerClient, blobName)) {
					deletedFiles.add(blobName);
				} else {
					failedFiles.add(blobName);
				}
			}
		}

		if (!hasFilesToDelete) {
			classLogger.warn(
					blobDirectory.isEmpty() ? "No files found in container: {}" : "No files found in directory: {}",
					blobDirectory.isEmpty() ? containerName : blobDirectory);
			return;
		}

		if (deletedFiles.isEmpty()) {
			classLogger.info("No files were deleted.");
		} else {
			classLogger.info("Successfully deleted files: {}", deletedFiles);
		}

		if (!failedFiles.isEmpty()) {
			classLogger.error("Some files failed to delete. Retrying...");
			retryDelete(failedFiles, containerClient);
		}
	}

	@Override
	public void deleteFromStorage(String storagePath, boolean leaveFolderStructure) throws Exception {
		// Extract container and blob directory
		String[] containerAndPath = extractContainerAndPath(storagePath);
		String containerName = containerAndPath[0];
		String blobDirectory = Utility.normalizePath(containerAndPath[1]);
		if (blobDirectory.startsWith("/")) {
			blobDirectory = blobDirectory.substring(1);
		}

		List<String> deletedFiles = new ArrayList<>();
		List<String> failedFiles = new ArrayList<>();
		BlobContainerClient containerClient = this.blobServiceClient.getBlobContainerClient(containerName);
		// List all blobs for deletion
		Iterable<BlobItem> blobItems = blobDirectory.isEmpty() ? containerClient.listBlobs()
				: containerClient.listBlobs(new ListBlobsOptions().setPrefix(blobDirectory), null);
		boolean hasFilesToDelete = false;

		for (BlobItem blobItem : blobItems) {
			hasFilesToDelete = true;
			String blobName = blobItem.getName();
			if (deleteBlob(containerClient, blobName)) {
				deletedFiles.add(blobName);
			} else {
				failedFiles.add(blobName);
			}
		}
		classLogger.info(
				hasFilesToDelete ? "Deletion process completed for: {}" : "No files found to delete in path: {}",
				storagePath);
		if (deletedFiles.isEmpty()) {
			classLogger.info("No files were deleted.");
		} else {
			classLogger.info("Successfully deleted files: {}", deletedFiles);
		}

		if (!failedFiles.isEmpty()) {
			classLogger.error("Some files failed to delete. Retrying...");
			retryDelete(failedFiles, containerClient);
		}
		// Preserve folder structure if required
		if (leaveFolderStructure && !deletedFiles.isEmpty()) {
			preserveFolderStructure(containerClient, deletedFiles);
		}
	}

	@Override
	public void deleteFolderFromStorage(String storageFolderPath) throws Exception {
		String[] containerAndPath = extractContainerAndPath(storageFolderPath);
		String containerName = containerAndPath[0];
		String blobDirectory = containerAndPath[1];

		List<String> deletedFiles = new ArrayList<>();
		List<String> failedFiles = new ArrayList<>();
		BlobContainerClient containerClient = this.blobServiceClient.getBlobContainerClient(containerName);

		boolean folderExists = false;

		classLogger.info(blobDirectory.isEmpty() ? "Blob directory is empty. Deleting all files in container: {}"
				: "Deleting folder: {}", blobDirectory.isEmpty() ? containerName : blobDirectory);

		for (BlobItem blobItem : containerClient.listBlobs()) {
			String blobName = blobItem.getName();

			if (blobDirectory.isEmpty() || blobName.startsWith(blobDirectory)) {
				folderExists = true;
				try {
					retryOperation(() -> {
						BlobClient blobClient = containerClient.getBlobClient(blobName);
						if (blobClient.deleteIfExists()) {
							classLogger.info("Deleted file: {}", blobName);
							deletedFiles.add(blobName);
						}
					}, "Deleting file: " + blobName);
				} catch (Exception e) {
					failedFiles.add(blobName);
					classLogger.error("Failed to delete file: {}", blobName, e);
				}
			}
		}
		if (deletedFiles.isEmpty()) {
			classLogger.info("No files were deleted.");
		} else {
			classLogger.info("Successfully deleted files: {}", deletedFiles);
		}
		if (!failedFiles.isEmpty()) {
			classLogger.error("Some files failed to delete. Retrying...");
			retryDelete(failedFiles, containerClient);
		}
		classLogger.info(folderExists ? "Successfully deleted folder: {}" : "No files found in directory: {}",
				folderExists ? storageFolderPath : blobDirectory);
	}

	private String uploadFileToBlob(Path file, Path basePath, BlobContainerClient containerClient, String blobDirectory,
			Map<String, Object> metadata) {
		// Generate relative path for blob
		String relativePath = Utility.normalizePath(basePath.relativize(file).toString()).trim();
		String blobName = blobDirectory.isEmpty() ? relativePath
				: Utility.normalizePath(blobDirectory + "/" + relativePath);

		BlobClient blobClient = containerClient.getBlobClient(blobName);

		try {
			long localFileSize = Files.size(file);
			long localLastModified = Files.getLastModifiedTime(file).toMillis();

			retryOperation(() -> {
				try {
					BlobProperties properties = blobClient.getProperties();
					long blobFileSize = properties.getBlobSize();
					long blobLastModified = properties.getLastModified().toInstant().toEpochMilli();
					// Sync conditions: If file size or modified time differs
					if (localFileSize != blobFileSize || localLastModified > blobLastModified) {
						blobClient.uploadFromFile(file.toString(), true);
						classLogger.info("Updated file: {}", blobName);
					} else {
						classLogger.info("Skipping file (No changes detected): {}", blobName);
					}
				} catch (BlobStorageException e) {
					// If blob doesn't exist, upload it as a new file
					if (e.getStatusCode() == 404) {
						blobClient.uploadFromFile(file.toString(), true);
						classLogger.info("Uploaded new file: {}", blobName);
					} else {
						throw e;
					}
				}
				// Apply metadata if provided
				applyMetadata(blobClient, metadata);

			}, "Uploading file: " + blobName);

		} catch (IOException e) {
			classLogger.error("Failed to read file properties: {}", file, e);
			return null;
		}
		return blobName;
	}

	private boolean deleteBlob(BlobContainerClient containerClient, String blobName) {
		try {
			retryOperation(() -> {
				BlobClient blobClient = containerClient.getBlobClient(blobName);
				if (blobClient.deleteIfExists()) {
					classLogger.info("Deleted file: {}", blobName);
				}
			}, "Deleting file: " + blobName);
			return true;
		} catch (Exception e) {
			classLogger.error("Failed to delete file: {}", blobName, e);
			return false;
		}
	}

	private void preserveFolderStructure(BlobContainerClient containerClient, List<String> deletedFiles) {
		Set<String> folderPaths = deletedFiles.stream()
				.map(file -> file.contains("/") ? file.substring(0, file.lastIndexOf("/") + 1) : "")
				.filter(path -> !path.isEmpty()).collect(Collectors.toSet());

		folderPaths.forEach(folderPath -> {
			BlobClient folderBlobClient = containerClient.getBlobClient(folderPath);
			folderBlobClient.upload(new ByteArrayInputStream(new byte[0]), 0, true);
			classLogger.info("Preserved folder structure: {}", folderPath);
		});
	}

	private void deleteEmptyBlobs(BlobContainerClient containerClient) {
		for (BlobItem blobItem : containerClient.listBlobs()) {
			BlobClient blobClient = containerClient.getBlobClient(blobItem.getName());
			if (blobClient.getProperties().getBlobSize() == 0) {
				blobClient.delete();
				classLogger.info("Deleted empty blob folder: {}", blobItem.getName());
			}
		}
	}

	private String uploadFile(BlobContainerClient containerClient, Path rootPath, Path file, String blobDirectory,
			Map<String, Object> metadata) throws IOException {

		String relativePath = Utility.normalizePath(rootPath.relativize(file).toString()).trim();

		String blobName = blobDirectory.isEmpty() ? relativePath
				: Utility.normalizePath(blobDirectory + "/" + relativePath);
		BlobClient blobClient = containerClient.getBlobClient(blobName);

		retryOperation(() -> {
			blobClient.uploadFromFile(file.toString(), true);
			classLogger.info("Uploaded file: {}", blobName);
			applyMetadata(blobClient, metadata);
		}, "Uploading: " + blobName);

		return blobName;
	}

	private void applyMetadata(BlobClient blobClient, Map<String, Object> metadata) {
		if (metadata != null && !metadata.isEmpty()) {
			Map<String, String> metadataMap = metadata.entrySet().stream()
					.collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
			blobClient.setMetadata(metadataMap);
			classLogger.info("Metadata applied to: {}", blobClient.getBlobName());
		}
	}

	private void syncStorageDeletion(BlobContainerClient containerClient, String blobDirectory, Path localBasePath) {
		PagedIterable<BlobItem> blobItems = containerClient.listBlobs(new ListBlobsOptions().setPrefix(blobDirectory),
				null);
		for (BlobItem blobItem : blobItems) {
			String blobName = blobItem.getName();
			BlobClient blobClient = containerClient.getBlobClient(blobName);

			Path localFilePath = localBasePath
					.resolve(blobName.replaceFirst(blobDirectory, "").replace("/", File.separator));

			try {
				BlobProperties properties = blobClient.getProperties();
				long blobSize = properties.getBlobSize();

				if (!Files.exists(localFilePath)) {
					blobClient.delete();
					classLogger.info("Deleted storage file not found in local: {}", blobName);
				} else if (blobSize == 0) { // Check for empty blobs
					blobClient.delete();
					classLogger.info("Deleted empty folder placeholder: {}", blobName);
				}
			} catch (Exception e) {
				classLogger.error("Failed to delete blob: {}", blobName, e);
			}
		}
	}

	private void deleteEmptyDirectories(Path rootPath) {
		try {

			List<Path> directories = Files.walk(rootPath).sorted(Comparator.reverseOrder()) // Delete children first
					.filter(Files::isDirectory).collect(Collectors.toList());

			for (Path dir : directories) {
				try (DirectoryStream<Path> entries = Files.newDirectoryStream(dir)) {
					if (!entries.iterator().hasNext()) { // Directory is empty
						Files.delete(dir);
						classLogger.info("Deleted empty local folder: {}", dir);
					}
				} catch (IOException e) {
					classLogger.error("Failed to delete empty folder: {}", dir, e);
				}
			}
		} catch (IOException e) {
			classLogger.error("Error while deleting empty directories", e);
		}
	}

	private void retryOperation(Runnable operation, String actionDescription) {
		int maxRetries = 3;
		int baseDelay = 2000;

		for (int attempt = 1; attempt <= maxRetries; attempt++) {
			try {
				operation.run();
				return;
			} catch (Exception e) {
				classLogger.error("Attempt {} failed for {}", attempt, actionDescription, e);
				// If last attempt fails, throw an exception
				if (attempt == maxRetries) {
					classLogger.error("All retry attempts failed for: {}", actionDescription);
					throw new RuntimeException(
							"Operation failed after " + maxRetries + " retries: " + actionDescription, e);
				}
				try {
					long sleepTime = baseDelay * (long) Math.pow(2, attempt - 1); // Exponential backoff
					Thread.sleep(sleepTime);
				} catch (InterruptedException ie) {
					Thread.currentThread().interrupt();
					throw new RuntimeException("Retry operation interrupted: " + actionDescription, ie);
				}
			}
		}
	}

	private void rollbackUploads(BlobContainerClient containerClient, List<String> failedFiles) {
		for (String blobName : failedFiles) {
			try {
				retryOperation(() -> {
					BlobClient blobClient = containerClient.getBlobClient(blobName);
					if (blobClient.exists()) {
						blobClient.delete();
						classLogger.info("Rolled back failed upload: {}", blobName);
					}
				}, "Rolling back failed upload: " + blobName);
			} catch (Exception e) {
				classLogger.error("Rollback failed for: {}", blobName, e);
			}
		}
	}

	private void rollbackDownloads(List<String> failedFiles, Path localDirectory) {
		for (String file : failedFiles) {
			Path localFile = localDirectory.resolve(file);

			if (Files.isRegularFile(localFile)) { // Ensures it's not a directory or symbolic link
				try {
					Files.delete(localFile);
					classLogger.info("Rolled back partially downloaded file: {}", file);
				} catch (IOException e) {
					classLogger.error("Failed to rollback file: {}", file, e);
				}
			} else {
				classLogger.warn("Skipping rollback for non-regular file: {}", file);
			}
		}
	}

	private void retryDelete(List<String> failedFiles, BlobContainerClient containerClient) {
		List<String> remainingFailedFiles = new ArrayList<>();

		for (String blobName : failedFiles) {
			try {
				BlobClient blobClient = containerClient.getBlobClient(blobName);

				// Check if the blob exists before retrying delete
				if (!blobClient.exists()) {
					classLogger.info("Blob already deleted: {}", blobName);
					continue;
				}

				retryOperation(() -> {
					blobClient.delete();
					classLogger.info("Retried and deleted file: {}", blobName);
				}, "Retrying delete for file: " + blobName);

			} catch (Exception e) {
				remainingFailedFiles.add(blobName);
				classLogger.error("Retry failed for file: {}", blobName, e);
			}
		}

		if (!remainingFailedFiles.isEmpty()) {
			classLogger.error("Some files still failed to delete after retries: {}", remainingFailedFiles);
		} else {
			classLogger.info("All files deleted successfully after retries.");
		}
	}

	private String[] extractContainerAndPath(String storagePath) {
		if (storagePath == null || storagePath.trim().isEmpty()) {
			throw new IllegalArgumentException("Storage path cannot be null or empty.");
		}

		// Use the utility method for normalization
		String normalizedPath = Utility.normalizePath(storagePath).trim();

		// Remove leading slash if present
		if (normalizedPath.startsWith("/")) {
			normalizedPath = normalizedPath.substring(1);
		}

		if (normalizedPath.isEmpty()) {
			throw new IllegalArgumentException("Invalid storage path after normalization: " + storagePath);
		}

		// Find the first slash to separate container name and blob path
		int slashIndex = normalizedPath.indexOf('/');
		String containerName = (slashIndex == -1) ? normalizedPath : normalizedPath.substring(0, slashIndex);
		String blobDirectory = (slashIndex == -1) ? "" : normalizedPath.substring(slashIndex + 1);

		if (containerName.isEmpty()) {
			throw new IllegalArgumentException("Container name is missing in storage path: " + storagePath);
		}

		if (blobDirectory.isEmpty()) {
			classLogger.warn("Blob directory is empty for container: {}", containerName);
		}

		return new String[] { containerName, blobDirectory };
	}

	@Override
	public void close() throws IOException {
		// there is no disconnect logic
	}

}
