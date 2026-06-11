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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import prerna.engine.api.StorageTypeEnum;
import prerna.util.Utility;

public class GoogleCloudNativeBlobStorageEngine extends AbstractStorageEngine {

	private static final Logger classLogger = LogManager.getLogger(GoogleCloudNativeBlobStorageEngine.class);

	public static final String GCS_SERVICE_ACCOUNT_FILE_KEY = "GCS_SERVICE_ACCOUNT_FILE";
	public static final String GCS_BUCKET_KEY = "GCS_BUCKET";
	public static final String GCS_PROJECT_ID = "GCS_PROJECT_ID";

	private transient String GCP_SERVICE_ACCOUNT_FILE = null;
	private transient String BUCKET;
	private transient String PROJECT_ID = null;
	private transient Storage storage;
	private transient Bucket bucket;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		// Load properties
		this.PROJECT_ID = smssProp.getProperty(GCS_PROJECT_ID);
		this.GCP_SERVICE_ACCOUNT_FILE = smssProp.getProperty(GCS_SERVICE_ACCOUNT_FILE_KEY);
		this.BUCKET = smssProp.getProperty(GCS_BUCKET_KEY);
		// Validate required properties
		if (this.BUCKET == null || this.BUCKET.isEmpty()) {
			throw new IllegalArgumentException("Bucket name is missing in properties.");
		}
		if (this.PROJECT_ID == null || this.PROJECT_ID.isEmpty()) {
			throw new IllegalArgumentException("Project ID is missing in properties.");
		}
		if (this.GCP_SERVICE_ACCOUNT_FILE == null || this.GCP_SERVICE_ACCOUNT_FILE.isEmpty()) {
			throw new IllegalArgumentException("Service account file is missing in properties.");
		}
		// Create service client
		createServiceClient();

		// Fetch the bucket
		this.bucket = storage.get(this.BUCKET);
		if (bucket == null) {
			throw new IllegalArgumentException("Bucket does not exist: " + this.BUCKET);
		}

		classLogger.info("Successfully connected to GCS Bucket: {}", this.BUCKET);
	}

	public void createServiceClient() throws FileNotFoundException, IOException {
		this.storage = StorageOptions.newBuilder().setProjectId(this.PROJECT_ID)
				.setCredentials(GoogleCredentials.fromStream(new FileInputStream(this.GCP_SERVICE_ACCOUNT_FILE)))
				.build().getService();
		classLogger.info("Google cloud storage Service client created successfully.");
	}

	@Override
	public StorageTypeEnum getStorageType() {
		return StorageTypeEnum.GOOGLE_CLOUD_NATIVE_STORAGE;
	}

	@Override
	public byte[] readBlobToMemory(String storagePath) throws Exception {
		Blob blob = this.bucket.get(storagePath);
		if (blob == null) {
			throw new IllegalArgumentException("Blob not found: " + storagePath);
		}
		return blob.getContent();
	}

	@Override
	public void updateBlobMetadata(String storagePath, Map<String, Object> metadata) throws Exception {
		String normalizedPath = Utility.normalizePath(storagePath);
		if (normalizedPath.startsWith("/")) {
			normalizedPath = normalizedPath.substring(1);
		}
		final String blobPath = normalizedPath;
		BlobId blobId = BlobId.of(this.BUCKET, blobPath);
		Blob blob = storage.get(blobId);
		if (blob == null) {
			throw new IllegalArgumentException("Blob not found: " + blobPath);
		}
		if (metadata == null || metadata.isEmpty()) {
			throw new IllegalArgumentException("Metadata cannot be null or empty.");
		}

		Map<String, String> flatMetadata = new HashMap<>();
		for (Map.Entry<String, Object> entry : metadata.entrySet()) {
			Object val = entry.getValue();
			if (val instanceof String) {
				flatMetadata.put(entry.getKey(), (String) val);
			} else {
				// Lists, Maps, etc. JSON string
				flatMetadata.put(entry.getKey(), GSON.toJson(val));
			}
		}

		retryOperation(() -> {
			storage.get(blobId).toBuilder().setMetadata(flatMetadata).build().update();
			classLogger.info("Updated metadata for: {}", blobPath);
		}, "Updating metadata for: " + blobPath);
	}

	@Override
	public List<String> list(String containerPrefix) throws Exception {
		List<Map<String, Object>> details = listDetails(containerPrefix);
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
	public List<Map<String, Object>> listDetails(String containerPrefix) throws Exception {
		List<Map<String, Object>> detailsList = new ArrayList<>();
		containerPrefix = containerPrefix == null ? "" : Utility.normalizePath(containerPrefix);
		if (containerPrefix.startsWith("/")) {
			containerPrefix = containerPrefix.substring(1);
		}
		if (containerPrefix.endsWith("/")) {
			containerPrefix = containerPrefix.substring(0, containerPrefix.length() - 1);
		}
		String prefix = containerPrefix.isEmpty() ? "" : containerPrefix + "/";

		Page<Blob> page = prefix.isEmpty() ? this.bucket.list(Storage.BlobListOption.currentDirectory())
				: this.bucket.list(Storage.BlobListOption.prefix(prefix), Storage.BlobListOption.currentDirectory());

		for (Blob blob : page.iterateAll()) {
			String blobName = blob.getName();
			if (blobName == null || blobName.equals(prefix)) {
				continue;
			}
			String name = prefix.isEmpty() ? blobName : blobName.substring(prefix.length());
			boolean isDir = blob.isDirectory();
			if (name.endsWith("/")) {
				name = name.substring(0, name.length() - 1);
			}
			if (name.isEmpty() || name.contains("/")) {
				continue;
			}

			Map<String, Object> details = new HashMap<>();
			details.put("Path", containerPrefix.isEmpty() ? "/" + name : "/" + containerPrefix + "/" + name);
			details.put("Name", name);
			details.put("IsDir", isDir);

			if (isDir) {
				details.put("Size", 0L);
				details.put("MimeType", "inode/directory");
				details.put("ModTime", null);
				details.put("Metadata", Collections.emptyMap());
			} else {
				details.put("Size", blob.getSize());
				details.put("MimeType", blob.getContentType());
				OffsetDateTime updateTime = blob.getUpdateTimeOffsetDateTime();
				details.put("ModTime",
						updateTime == null ? null : String.valueOf(updateTime.toInstant().toEpochMilli()));

				Map<String, String> rawMetadata = blob.getMetadata();
				if (rawMetadata != null && !rawMetadata.isEmpty()) {
					Map<String, Object> parsed = new HashMap<>();
					for (Map.Entry<String, String> entry : rawMetadata.entrySet()) {
						try {
							Object jsonVal = GSON.fromJson(entry.getValue(), Object.class);
							parsed.put(entry.getKey(), jsonVal);
						} catch (Exception e) {
							// Not valid JSON, keep as plain string
							parsed.put(entry.getKey(), entry.getValue());
						}
					}
					details.put("Metadata", parsed);
				} else {
					details.put("Metadata", Collections.emptyMap());
				}
			}
			detailsList.add(details);
		}
		return detailsList;
	}

	@Override
	public List<Map<String, Object>> listVersions(String storagePath) throws Exception {
		List<Map<String, Object>> versions = new ArrayList<>();
		String key = storagePath == null ? "" : Utility.normalizePath(storagePath).trim();
		if (key.startsWith("/")) {
			key = key.substring(1);
		}
		if (key.endsWith("/")) {
			key = key.substring(0, key.length() - 1);
		}

		// List all versions using versions(true) option
		Page<Blob> page = this.bucket.list(
				Storage.BlobListOption.prefix(key),
				Storage.BlobListOption.versions(true));

		for (Blob blob : page.iterateAll()) {
			// Only include exact key matches
			if (!blob.getName().equals(key)) {
				continue;
			}
			Map<String, Object> versionInfo = new LinkedHashMap<>();
			versionInfo.put("versionId", String.valueOf(blob.getGeneration()));
			versionInfo.put("lastModified", blob.getUpdateTimeOffsetDateTime() != null
					? blob.getUpdateTimeOffsetDateTime().toString() : null);
			versionInfo.put("size", blob.getSize());
			versionInfo.put("isLatest", !blob.isDirectory());
			versionInfo.put("key", blob.getName());
			versions.add(versionInfo);
		}

		return versions;
	}

	@Override
	public void syncLocalToStorage(String localPath, String storagePath, Map<String, Object> metadata)
			throws Exception {
		Path localFilePath = Paths.get(localPath);

		if (!Files.exists(localFilePath)) {
			throw new IllegalArgumentException("Invalid path: " + localPath);
		}

		Path localBasePath = Files.isDirectory(localFilePath) ? localFilePath : localFilePath.getParent();

		List<String> uploadedFiles = new ArrayList<>();
		List<String> failedFiles = new ArrayList<>();
		boolean found = false;

		try {
			// Remove empty directories locally
			deleteEmptyDirectories(localFilePath);
			// Delete extra blobs from azure storage
			syncStorageDeletion(storage, storagePath, localBasePath);

			try (Stream<Path> stream = Files.walk(localFilePath)) {
				stream.filter(Files::isRegularFile).forEach(file -> {
					try {
						uploadedFiles.add(uploadingFileToGCS(file, localBasePath, storagePath, metadata));
					} catch (Exception e) {
						failedFiles.add(file.toString());
						classLogger.error("Failed to upload file: {}", file, e);
					}
				});
			}
			found = true;
		} catch (Exception e) {
			classLogger.error("Sync operation failed. Rolling back failed uploads.", e);
			rollbackUploads(storage, failedFiles);
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
		Path localDirectory = Paths.get(localPath);
		Files.createDirectories(localDirectory);
		Set<String> cloudFiles = new HashSet<>();
		List<String> downloadedFiles = new ArrayList<>(), failedFiles = new ArrayList<>();
		boolean found = false;
		String requestedPath = normalizeStoragePrefixPath(storagePath);
		// Delete empty blobs from GCS
		deleteEmptyBlobs(requestedPath);
		for (Blob blob : this.bucket.list(Storage.BlobListOption.prefix(requestedPath)).iterateAll()) {
			String relativePath = resolveRelativeStoragePath(blob.getName(), requestedPath);
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
				boolean fileExists = Files.exists(localFilePath);
				boolean shouldDownload = !fileExists;
				if (fileExists) {
					FileTime localModifiedTime = Files.getLastModifiedTime(localFilePath);
					long localFileSize = Files.size(localFilePath);
					long cloudFileSize = blob.getSize();
					long cloudModifiedTime = blob.getUpdateTimeOffsetDateTime().toInstant().toEpochMilli();
					shouldDownload = cloudFileSize != localFileSize || cloudModifiedTime > localModifiedTime.toMillis();
				}

				if (shouldDownload) {
					retryOperation(() -> {
						try {
							Files.write(localFilePath, blob.getContent());
						} catch (IOException e) {
							classLogger.error("Failed to write file: {}", localFilePath, e);
							throw new RuntimeException("Error writing file: " + localFilePath, e);
						}
					}, "Syncing file to local: " + blob.getName());
					downloadedFiles.add(blob.getName());
					classLogger.info(fileExists ? "Updated file: {}" : "Downloaded new file: {}", localFilePath);
				}
				found = true;
			} catch (Exception e) {
				failedFiles.add(relativePath);
				classLogger.error("Failed to sync file: {}", blob.getName(), e);
			}
		}

		// Delete local files not present in GCS
		try (Stream<Path> stream = Files.walk(localDirectory)) {
			stream.filter(Files::isRegularFile).filter(localFile -> !cloudFiles.contains(localFile.toString()))
					.forEach(localFile -> {
						try {
							Files.delete(localFile);
							classLogger.info("Deleted extra local file: {}", localFile);
						} catch (IOException e) {
							classLogger.error("Failed to delete extra file: {}", localFile, e);
						}
					});
		}
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
		List<Path> paths = parseLocalPaths(localFilePath);
		List<String> uploadedFiles = new ArrayList<>();
		List<String> failedFiles = new ArrayList<>();
		boolean found = false;
		for (Path filePath : paths) {
			if (!Files.exists(filePath)) {
				classLogger.error("File not found: {}", filePath);
				failedFiles.add(filePath.toString());
				continue;
			}

			// Delete empty directories before upload
			deleteEmptyDirectories(filePath);

			if (Files.isDirectory(filePath)) {
				try (Stream<Path> stream = Files.walk(filePath)) {
					stream.filter(Files::isRegularFile).forEach(file -> {
						try {
							uploadedFiles.add(uploadFileToGCS(filePath, file, storageFolderPath, metadata));
						} catch (Exception e) {
							failedFiles.add(file.toString());
							classLogger.error("Failed to upload file: {}", file, e);
							rollbackUploads(storage, failedFiles);
						}
					});
					found = true;
				}
			} else {
				try {
					uploadedFiles.add(uploadFileToGCS(filePath.getParent(), filePath, storageFolderPath, metadata));
					found = true;
				} catch (Exception e) {
					failedFiles.add(filePath.toString());
					classLogger.error("Failed to upload file: {}", filePath, e);
					rollbackUploads(storage, failedFiles);
				}
			}
		}

		// Delete empty blobs from GCS
		deleteEmptyBlobs(storageFolderPath);

		if (uploadedFiles.isEmpty()) {
			classLogger.info("No files were uploaded.");
		} else {
			classLogger.info("Successfully uploaded files: {}", uploadedFiles);
		}
		classLogger.info(found ? "Copy completed successfully for: {}" : "No files found to copy for: {}",
				storageFolderPath);
	}

	@Override
	public String copyToStorageVersioned(String localFilePath, String storageFolderPath, Map<String, Object> metadata)
			throws Exception {
		List<Path> paths = parseLocalPaths(localFilePath);
		List<String> uploadedFiles = new ArrayList<>();
		List<String> failedFiles = new ArrayList<>();
		AtomicReference<String> lastVersionId = new AtomicReference<>(null);
		boolean found = false;
		for (Path filePath : paths) {
			if (!Files.exists(filePath)) {
				classLogger.error("File not found: {}", filePath);
				failedFiles.add(filePath.toString());
				continue;
			}

			// Delete empty directories before upload
			deleteEmptyDirectories(filePath);

			if (Files.isDirectory(filePath)) {
				try (Stream<Path> stream = Files.walk(filePath)) {
					stream.filter(Files::isRegularFile).forEach(file -> {
						try {
							String generation = uploadFileToGCSVersioned(filePath, file, storageFolderPath, metadata);
							uploadedFiles.add(file.toString());
							if (generation != null) {
								lastVersionId.set(generation);
							}
						} catch (Exception e) {
							failedFiles.add(file.toString());
							classLogger.error("Failed to upload file: {}", file, e);
							rollbackUploads(storage, failedFiles);
						}
					});
					found = true;
				}
			} else {
				try {
					String generation = uploadFileToGCSVersioned(filePath.getParent(), filePath, storageFolderPath,
							metadata);
					uploadedFiles.add(filePath.toString());
					if (generation != null) {
						lastVersionId.set(generation);
					}
					found = true;
				} catch (Exception e) {
					failedFiles.add(filePath.toString());
					classLogger.error("Failed to upload file: {}", filePath, e);
					rollbackUploads(storage, failedFiles);
				}
			}
		}

		// Delete empty blobs from GCS
		deleteEmptyBlobs(storageFolderPath);

		if (uploadedFiles.isEmpty()) {
			classLogger.info("No files were uploaded.");
		} else {
			classLogger.info("Successfully uploaded files: {}", uploadedFiles);
		}
		classLogger.info(found ? "Copy completed successfully for: {}" : "No files found to copy for: {}",
				storageFolderPath);
		return lastVersionId.get();
	}

	@Override
	public void copyToLocal(String storageFilePath, String localFolderPath) throws Exception {
		List<String> paths = parseStorageObjectPaths(storageFilePath);
		Path localDirectory = Paths.get(localFolderPath);
		// Ensure local directory exists
		Files.createDirectories(localDirectory);

		List<String> downloadedFiles = new ArrayList<>(), failedFiles = new ArrayList<>();
		boolean found = false;
		for (String path : paths) {
			String requestedPath = normalizeStoragePrefixPath(path);
			// Delete empty blobs (zero-byte files)
			deleteEmptyBlobs(requestedPath);

			// Fetch all files matching the given prefix
			for (Blob blob : this.bucket.list(Storage.BlobListOption.prefix(requestedPath)).iterateAll()) {
				String blobName = blob.getName();
				String relativePath = resolveRelativeStoragePath(blobName, requestedPath);
				if (relativePath == null) {
					continue;
				}
				Path localFilePath = localDirectory.resolve(relativePath.replace("/", File.separator));
				try {
					Files.createDirectories(localFilePath.getParent());
					retryOperation(() -> {
						try {
							downloadFile(blob, localFilePath);
						} catch (IOException e) {
							classLogger.error("Failed to add file to downloaded list: {}", blobName, e);
						}
					}, "Downloading file: " + blobName);
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
	public void copyToLocal(String storageFilePath, String localFolderPath, String versionId) throws Exception {
		if (versionId != null && !versionId.isEmpty()) {
			String key = normalizeStoragePrefixPath(storageFilePath);
			Path localDirectory = Paths.get(localFolderPath);
			Files.createDirectories(localDirectory);

			String fileName = key.contains("/") ? key.substring(key.lastIndexOf("/") + 1) : key;
			Path localFilePath = localDirectory.resolve(fileName);

			BlobId blobId = BlobId.of(this.BUCKET, key, Long.parseLong(versionId));
			Blob blob = storage.get(blobId);
			if (blob == null) {
				throw new IllegalArgumentException(
						"Object not found in GCS: " + key + " with generation=" + versionId);
			}

			downloadFile(blob, localFilePath);
			classLogger.info("Downloaded versioned file: {} (generation={})", localFilePath, versionId);
		} else {
			copyToLocal(storageFilePath, localFolderPath);
		}
	}

	@Override
	public void deleteFromStorage(String storagePath) throws Exception {
		List<String> deletedFiles = new ArrayList<>();
		List<String> failedFiles = new ArrayList<>();
		storagePath = Utility.normalizePath(storagePath);
		// Remove leading and trailing slashes if present
		if (storagePath.startsWith("/")) {
			storagePath = storagePath.substring(1);
		}
		if (storagePath.endsWith("/")) {
			storagePath = storagePath.substring(0, storagePath.length() - 1);
		}
		boolean hasFilesToDelete = false;

		// List all files based on the path
		Page<Blob> blobs = storagePath.isEmpty() ? this.bucket.list()
				: this.bucket.list(Storage.BlobListOption.prefix(storagePath));

		for (Blob blob : blobs.iterateAll()) {
			String blobName = blob.getName();

			if (storagePath.isEmpty() || blobName.equals(storagePath) || blobName.startsWith(storagePath + "/")) {
				hasFilesToDelete = true;
				if (deleteBlob(blob)) {
					deletedFiles.add(blobName);
				} else {
					failedFiles.add(blobName);
				}
			}
		}

		if (!hasFilesToDelete) {
			classLogger.warn(storagePath.isEmpty() ? "No files found in bucket: {}" : "No files found in directory: {}",
					storagePath.isEmpty() ? this.BUCKET : storagePath);
			return;
		}

		if (deletedFiles.isEmpty()) {
			classLogger.info("No files were deleted.");
		} else {
			classLogger.info("Successfully deleted files: {}", deletedFiles);
		}

		if (!failedFiles.isEmpty()) {
			classLogger.error("Some files failed to delete. Retrying...");
			retryDelete(failedFiles, bucket);
		}
	}

	@Override
	public void deleteFromStorage(String storagePath, boolean leaveFolderStructure) throws Exception {
		List<String> deletedFiles = new ArrayList<>();
		List<String> failedFiles = new ArrayList<>();
		storagePath = Utility.normalizePath(storagePath);
		// Remove leading and trailing slashes if present
		if (storagePath.startsWith("/")) {
			storagePath = storagePath.substring(1);
		}
		if (storagePath.endsWith("/")) {
			storagePath = storagePath.substring(0, storagePath.length() - 1);
		}
		boolean hasFilesToDelete = false;

		// List all files based on the path
		Page<Blob> blobs = storagePath.isEmpty() ? this.bucket.list()
				: this.bucket.list(Storage.BlobListOption.prefix(storagePath));

		for (Blob blob : blobs.iterateAll()) {
			String blobName = blob.getName();
			hasFilesToDelete = true;
			if (deleteBlob(blob)) {
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
			retryDelete(failedFiles, bucket);
		}

		// Preserve folder structure if required
		if (leaveFolderStructure && !deletedFiles.isEmpty()) {
			preserveFolderStructure(storage, bucket, deletedFiles);
		}
	}

	public void preserveFolderStructure(Storage storage, Bucket bucket, List<String> deletedFiles) {
		Set<String> folderPaths = deletedFiles.stream()
				.map(file -> file.contains("/") ? file.substring(0, file.lastIndexOf("/") + 1) : "")
				.filter(path -> !path.isEmpty()).collect(Collectors.toSet());

		for (String folderPath : folderPaths) {
			BlobId blobId = BlobId.of(this.BUCKET, folderPath);
			BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
			storage.create(blobInfo, "".getBytes(StandardCharsets.UTF_8));
			classLogger.info("Preserved folder structure: {}", folderPath);
		}
	}

	@Override
	public void deleteFolderFromStorage(String storageFolderPath) throws Exception {
		List<String> deletedFiles = new ArrayList<>();
		List<String> failedFiles = new ArrayList<>();

		storageFolderPath = Utility.normalizePath(storageFolderPath);

		// Remove the leading slash if present
		if (storageFolderPath.startsWith("/")) {
			storageFolderPath = storageFolderPath.substring(1);
		}

		boolean folderExists = false;

		classLogger.info(storageFolderPath.isEmpty() ? "Folder path is empty. Deleting all files in bucket: {}"
				: "Deleting folder: {}", storageFolderPath.isEmpty() ? this.bucket : storageFolderPath);

		Page<Blob> blobs = this.bucket.list(Storage.BlobListOption.prefix(storageFolderPath));

		for (Blob blob : blobs.iterateAll()) {
			folderExists = true;
			String blobName = blob.getName();
			try {
				retryOperation(() -> {
					boolean deleted = blob.delete();
					if (deleted) {
						classLogger.info("Deleted file: {}", blobName);
						deletedFiles.add(blobName);
					}
				}, "Deleting file: " + blobName);
			} catch (Exception e) {
				failedFiles.add(blobName);
				classLogger.error("Failed to delete file: {}", blobName, e);
			}
		}

		if (deletedFiles.isEmpty()) {
			classLogger.info("No files were deleted.");
		} else {
			classLogger.info("Successfully deleted files: {}", deletedFiles);
		}
		if (!failedFiles.isEmpty()) {
			classLogger.error("Some files failed to delete. Retrying...");
			retryDelete(failedFiles, this.bucket);
		}

		classLogger.info(folderExists ? "Successfully deleted folder: {}" : "No files found in directory: {}",
				storageFolderPath);

	}

	private String uploadingFileToGCS(Path file, Path basePath, String storagePath, Map<String, Object> metadata) {
		String relativePath = Utility.normalizePath(basePath.relativize(file).toString()).trim();
		String normalizedStoragePath = Utility.normalizePath(storagePath);
		// Remove leading and trailing slashes if present
		if (normalizedStoragePath.startsWith("/")) {
			normalizedStoragePath = normalizedStoragePath.substring(1);
		}
		if (normalizedStoragePath.endsWith("/")) {
			normalizedStoragePath = normalizedStoragePath.substring(0, normalizedStoragePath.length() - 1);
		}
		String blobName = normalizedStoragePath.isEmpty() ? relativePath : normalizedStoragePath + "/" + relativePath;

		BlobId blobId = BlobId.of(this.BUCKET, blobName);
		BlobInfo.Builder blobInfoBuilder = BlobInfo.newBuilder(blobId);

		try {
			long localFileSize = Files.size(file);
			long localLastModified = Files.getLastModifiedTime(file).toMillis();

			retryOperation(() -> {
				Blob blob = storage.get(blobId);
				if (blob != null) {
					long cloudFileSize = blob.getSize();
					long cloudLastModified = blob.getUpdateTimeOffsetDateTime().toInstant().toEpochMilli();

					if (localFileSize != cloudFileSize || localLastModified > cloudLastModified) {
						try {
							storage.create(blobInfoBuilder.build(), Files.readAllBytes(file));
							classLogger.info("Updated file: {}", blobName);
						} catch (IOException ioException) {
							classLogger.error("Failed to read file: {}", file, ioException);
							throw new RuntimeException("Failed to read file: " + file, ioException);
						}

					} else {
						classLogger.info("Skipping file (No changes detected): {}", blobName);
					}
				} else {
					try {
						storage.create(blobInfoBuilder.build(), Files.readAllBytes(file));
						classLogger.info("Uploaded new file: {}", blobName);
					} catch (IOException ioException) {
						classLogger.error("Failed to read file: {}", file, ioException);
						throw new RuntimeException("Failed to read file: " + file, ioException);
					}
				}
				applyMetadata(blobId, metadata);
			}, "Uploading file: " + blobName);

		} catch (IOException e) {
			classLogger.error("Failed to read file properties: {}", file, e);
			return null;
		}
		return blobName;
	}

	private void applyMetadata(BlobId blobId, Map<String, Object> metadata) {
		if (metadata != null && !metadata.isEmpty()) {
			Blob blob = storage.get(blobId);
			if (blob != null) {
				blob.toBuilder()
						.setMetadata(metadata.entrySet().stream()
								.collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString())))
						.build().update();
			}
		}
	}

	private void rollbackUploads(Storage storage, List<String> failedFiles) {
		for (String blobName : failedFiles) {
			try {
				retryOperation(() -> {
					BlobId blobId = BlobId.of(this.BUCKET, blobName);
					boolean deleted = storage.delete(blobId);
					if (deleted) {
						classLogger.info("Rolled back failed upload: {}", blobName);
					}
				}, "Rolling back failed upload: " + blobName);
			} catch (Exception e) {
				classLogger.error("Rollback failed for: {}", blobName, e);
			}
		}
	}

	private boolean deleteBlob(Blob blob) {
		try {
			retryOperation(() -> {
				if (blob.delete()) {
					classLogger.info("Deleted file: {}", blob.getName());
				}
			}, "Deleting file: " + blob.getName());
			return true;
		} catch (Exception e) {
			classLogger.error("Failed to delete file: {}", blob.getName(), e);
			return false;
		}
	}

	private void retryDelete(List<String> failedFiles, Bucket bucket) {
		List<String> remainingFailedFiles = new ArrayList<>();

		for (String blobName : failedFiles) {
			Blob blob = this.bucket.get(blobName);
			if (blob != null && deleteBlob(blob)) {
				classLogger.info("Successfully deleted on retry: {}", blobName);
			} else {
				remainingFailedFiles.add(blobName);
			}
		}

		if (!remainingFailedFiles.isEmpty()) {
			classLogger.error("Some files still failed to delete after retries: {}", remainingFailedFiles);
		} else {
			classLogger.info("All files deleted successfully after retries.");
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

	private void deleteEmptyDirectories(Path rootPath) {
		try (Stream<Path> stream = Files.walk(rootPath)) {
			List<Path> directories = stream.sorted(Comparator.reverseOrder()) // Delete children first
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

	private void syncStorageDeletion(Storage storage, String blobDirectory, Path localBasePath) {
		Page<Blob> blobs = storage.list(this.BUCKET, Storage.BlobListOption.prefix(blobDirectory));

		for (Blob blob : blobs.iterateAll()) {
			String blobName = blob.getName();
			Path localFilePath = localBasePath
					.resolve(blobName.replaceFirst(blobDirectory, "").replace("/", File.separator));

			try {
				long blobSize = blob.getSize();

				if (!Files.exists(localFilePath)) {
					storage.delete(blob.getBlobId());
					classLogger.info("Deleted storage file not found in local: {}", blobName);
				} else if (blobSize == 0) { // Check for empty blobs
					storage.delete(blob.getBlobId());
					classLogger.info("Deleted empty folder placeholder: {}", blobName);
				}
			} catch (Exception e) {
				classLogger.error("Failed to delete blob: {}", blobName, e);
			}
		}
	}

	private void rollbackDownloads(List<String> failedFiles, Path localDirectory) {
		for (String file : failedFiles) {
			Path localFile = localDirectory.resolve(file.replace("/", File.separator));

			if (Files.exists(localFile) && Files.isRegularFile(localFile)) {
				try {
					Files.delete(localFile);
					classLogger.info("Rolled back partially downloaded file: {}", localFile);
				} catch (IOException e) {
					classLogger.error("Failed to rollback file: {}", localFile, e);
				}
			} else {
				classLogger.warn("Skipping rollback for non-existing or non-regular file: {}", localFile);
			}
		}
	}

	private String uploadFileToGCS(Path rootPath, Path file, String storageFolderPath, Map<String, Object> metadata)
			throws IOException {
		String normalizedPath = Utility.normalizePath(storageFolderPath).trim();
		if (normalizedPath.startsWith("/")) {
			normalizedPath = normalizedPath.substring(1);
		}
		String relativePath = Utility.normalizePath(rootPath.relativize(file).toString()).trim();
		String blobName = normalizedPath.isEmpty() ? relativePath
				: (normalizedPath.endsWith("/") ? normalizedPath + relativePath : normalizedPath + "/" + relativePath);

		BlobId blobId = BlobId.of(this.BUCKET, blobName);
		BlobInfo.Builder blobInfoBuilder = BlobInfo.newBuilder(blobId);

		if (metadata != null && !metadata.isEmpty()) {
			blobInfoBuilder.setMetadata(metadata.entrySet().stream()
					.collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString())));
		}

		retryOperation(() -> {
			try {
				storage.create(blobInfoBuilder.build(), Files.readAllBytes(file));
				classLogger.info("Uploaded file to GCS: {}", blobName);
			} catch (IOException e) {
				classLogger.error("Failed to upload file to GCS: {}", blobName, e);
			}
		}, "Uploading: " + blobName);

		return blobName;
	}

	private String uploadFileToGCSVersioned(Path rootPath, Path file, String storageFolderPath,
			Map<String, Object> metadata) throws IOException {
		String normalizedPath = Utility.normalizePath(storageFolderPath).trim();
		if (normalizedPath.startsWith("/")) {
			normalizedPath = normalizedPath.substring(1);
		}
		String relativePath = Utility.normalizePath(rootPath.relativize(file).toString()).trim();
		String blobName = normalizedPath.isEmpty() ? relativePath
				: (normalizedPath.endsWith("/") ? normalizedPath + relativePath : normalizedPath + "/" + relativePath);

		BlobId blobId = BlobId.of(this.BUCKET, blobName);
		BlobInfo.Builder blobInfoBuilder = BlobInfo.newBuilder(blobId);

		if (metadata != null && !metadata.isEmpty()) {
			blobInfoBuilder.setMetadata(metadata.entrySet().stream()
					.collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString())));
		}

		AtomicReference<String> generationRef = new AtomicReference<>(null);
		retryOperation(() -> {
			try {
				Blob blob = storage.create(blobInfoBuilder.build(), Files.readAllBytes(file));
				classLogger.info("Uploaded file to GCS: {}", blobName);
				if (blob.getGeneration() != null) {
					generationRef.set(String.valueOf(blob.getGeneration()));
					classLogger.info("Generation for {}: {}", blobName, blob.getGeneration());
				}
			} catch (IOException e) {
				classLogger.error("Failed to upload file to GCS: {}", blobName, e);
			}
		}, "Uploading: " + blobName);

		return generationRef.get();
	}

	private void deleteEmptyBlobs(String storageFolderPath) {
		Page<Blob> blobs = storage.list(this.BUCKET, Storage.BlobListOption.prefix(storageFolderPath));

		for (Blob blob : blobs.iterateAll()) {
			if (blob.getSize() == 0) { // Check if the blob is empty (zero-byte file)
				storage.delete(blob.getBlobId());
				classLogger.info("Deleted empty blob folder: {}", blob.getName());
			}
		}
	}

	private void downloadFile(Blob blob, Path localFilePath) throws IOException {
		try (FileOutputStream outputStream = new FileOutputStream(localFilePath.toFile())) {
			outputStream.write(blob.getContent());
		}
	}

	@Override
	public void close() throws IOException {
		if (storage != null) {
			try {
				storage.close();
			} catch (Exception e) {
				classLogger.error("Failed to close Google Cloud Storage client for bucket='{}'.", this.BUCKET, e);
			}
		}
	}
}
