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
import java.io.IOException;
import java.io.UncheckedIOException;
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
import java.util.concurrent.Callable;
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

public class GoogleCloudStorageEngine extends AbstractStorageEngine {

	private static final Logger classLogger = LogManager.getLogger(GoogleCloudStorageEngine.class);

	public static final String GCS_SERVICE_ACCOUNT_FILE_KEY = "GCS_SERVICE_ACCOUNT_FILE";
	public static final String GCS_BUCKET_KEY = "GCS_BUCKET";
	public static final String GCS_PROJECT_ID = "GCS_PROJECT_ID";

	// files at or under this go out in a single request, which costs one round trip
	// and caps how much of a file is ever held in the heap. Larger files stream in
	// chunks instead.
	//
	// rclone has no gcs chunk size or cutoff flag to copy here - its gcs backend
	// does not expose one - so these are ours. The threshold is deliberately low
	// because it is the line between holding a file in the heap and streaming it,
	// not a throughput tradeoff
	private static final long SINGLE_REQUEST_UPLOAD_BYTES = 8L * 1024 * 1024;
	private static final int UPLOAD_CHUNK_SIZE_BYTES = 8 * 1024 * 1024;

	private transient String GCP_SERVICE_ACCOUNT_FILE = null;
	private transient String BUCKET;
	private transient String PROJECT_ID = null;
	private transient Storage storage;
	private transient Bucket bucket;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		migrateLegacyProperties(smssProp);

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

	/**
	 * Fills in the project id when an older smss did not carry one.
	 *
	 * The previous implementation of this storage type did not need a project id,
	 * so smss files already out there do not set it. The service account json holds
	 * the project it belongs to, so it can be recovered rather than making every
	 * catalog be edited by hand.
	 *
	 * @param smssProp the properties being opened, updated in place
	 */
	protected void migrateLegacyProperties(Properties smssProp) {
		String projectId = smssProp.getProperty(GCS_PROJECT_ID);
		if (projectId != null && !projectId.trim().isEmpty()) {
			return;
		}

		String serviceAccountFile = smssProp.getProperty(GCS_SERVICE_ACCOUNT_FILE_KEY);
		if (serviceAccountFile == null || serviceAccountFile.trim().isEmpty()) {
			// nothing to recover it from, open() reports the missing project id
			return;
		}

		try {
			String json = new String(Files.readAllBytes(Paths.get(Utility.normalizePath(serviceAccountFile.trim()))),
					StandardCharsets.UTF_8);
			Map<?, ?> serviceAccount = GSON.fromJson(json, Map.class);
			Object projectFromFile = serviceAccount == null ? null : serviceAccount.get("project_id");
			if (projectFromFile != null && !projectFromFile.toString().trim().isEmpty()) {
				smssProp.put(GCS_PROJECT_ID, projectFromFile.toString().trim());
				classLogger.warn("{} was not set, using the project_id found in the service account file. "
						+ "The smss should be updated to set it explicitly.", GCS_PROJECT_ID);
			}
		} catch (Exception e) {
			classLogger.error("Unable to read a project id out of the service account file: {}", serviceAccountFile, e);
		}
	}

	public void createServiceClient() throws FileNotFoundException, IOException {
		this.storage = StorageOptions.newBuilder().setProjectId(this.PROJECT_ID)
				.setCredentials(GoogleCredentials.fromStream(new FileInputStream(this.GCP_SERVICE_ACCOUNT_FILE)))
				.build().getService();
		classLogger.info("Google cloud storage Service client created successfully.");
	}

	@Override
	public StorageTypeEnum getStorageType() {
		return StorageTypeEnum.GOOGLE_CLOUD_STORAGE;
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
		String normalizedPath = Utility.normalizePath(storagePath).trim();
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

		Map<String, String> flatMetadata = flattenMetadata(metadata);

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
		containerPrefix = normalizeStoragePrefixPath(containerPrefix);
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
		String key = normalizeStoragePrefixPath(storagePath);

		// List all versions using versions(true) option
		Page<Blob> page = this.bucket.list(Storage.BlobListOption.prefix(key), Storage.BlobListOption.versions(true));

		for (Blob blob : page.iterateAll()) {
			// Only include exact key matches
			if (!blob.getName().equals(key)) {
				continue;
			}
			Map<String, Object> versionInfo = new LinkedHashMap<>();
			versionInfo.put("versionId", String.valueOf(blob.getGeneration()));
			versionInfo.put("lastModified",
					blob.getUpdateTimeOffsetDateTime() != null ? blob.getUpdateTimeOffsetDateTime().toString() : null);
			versionInfo.put("size", blob.getSize());
			versionInfo.put("isLatest", blob.getDeleteTimeOffsetDateTime() == null);
			versionInfo.put("key", blob.getName());
			versions.add(versionInfo);
		}

		return versions;
	}

	@Override
	public StorageSyncStatus syncLocalToStorage(String localPath, String storagePath, Map<String, Object> metadata)
			throws Exception {
		Path localFilePath = Paths.get(localPath);

		if (!Files.exists(localFilePath)) {
			throw new IllegalArgumentException("Invalid path: " + localPath);
		}

		Path localBasePath = Files.isDirectory(localFilePath) ? localFilePath : localFilePath.getParent();

		List<String> uploadedFiles = new ArrayList<>();
		List<String> skippedFiles = new ArrayList<>();
		List<String> failedFiles = new ArrayList<>();
		boolean found = false;

		try {
			// Remove empty directories locally
			deleteEmptyDirectories(localFilePath);
			// Delete extra blobs from the bucket
			syncStorageDeletion(storage, storagePath, localBasePath);

			// one listing up front instead of fetching each blob's metadata
			Map<String, StoredObjectStat> alreadyStored = listStoredBlobs(storagePath);

			List<Path> localFiles;
			try (Stream<Path> stream = Files.walk(localFilePath)) {
				localFiles = stream.filter(Files::isRegularFile).toList();
			}

			List<Callable<TransferOutcome>> transfers = new ArrayList<>(localFiles.size());
			for (Path file : localFiles) {
				String blobName = buildBlobName(storagePath, file, localBasePath);
				if (!needsUpload(file, alreadyStored.get(blobName))) {
					classLogger.info("Skipping file (No changes detected): {}", blobName);
					skippedFiles.add(blobName);
					continue;
				}
				transfers.add(() -> {
					try {
						uploadBlob(blobName, file, metadata);
						return new TransferOutcome(blobName, null);
					} catch (Exception e) {
						classLogger.error("Failed to upload file: {}", file, e);
						return new TransferOutcome(blobName, e);
					}
				});
			}

			for (TransferOutcome outcome : runTransfersInParallel(transfers)) {
				if (outcome.failure() == null) {
					uploadedFiles.add(outcome.fileKey());
				} else {
					failedFiles.add(outcome.fileKey());
				}
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
			classLogger.info("Successfully uploaded {} files to: {}", uploadedFiles.size(), storagePath);
		}
		if (!skippedFiles.isEmpty()) {
			classLogger.info("Skipped {} unchanged files", skippedFiles.size());
		}
		if (!failedFiles.isEmpty()) {
			classLogger.error("Failed to sync: {}", failedFiles);
		}
		classLogger.info(found ? "Sync completed successfully for: {}" : "No files found to sync for: {}", storagePath);

		return StorageSyncStatus.of(storagePath, uploadedFiles, skippedFiles, failedFiles);
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
		List<Callable<TransferOutcome>> transfers = new ArrayList<>();
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
			found = true;

			boolean fileExists = Files.exists(localFilePath);
			if (fileExists && blob.getSize() != null && blob.getUpdateTimeOffsetDateTime() != null) {
				FileTime localModifiedTime = Files.getLastModifiedTime(localFilePath);
				if (blob.getSize() == Files.size(localFilePath) && blob.getUpdateTimeOffsetDateTime().toInstant()
						.toEpochMilli() <= localModifiedTime.toMillis()) {
					continue;
				}
			}

			transfers.add(() -> {
				try {
					retryOperation(() -> {
						// downloadTo streams to the file. getContent would pull the whole
						// object into the heap, which does not survive several of these
						// running at once
						blob.downloadTo(localFilePath);
					}, "Syncing file to local: " + blob.getName());
					classLogger.info(fileExists ? "Updated file: {}" : "Downloaded new file: {}", localFilePath);
					return new TransferOutcome(blob.getName(), null);
				} catch (Exception e) {
					classLogger.error("Failed to sync file: {}", blob.getName(), e);
					// the rollback works off local paths, so report the relative one
					return new TransferOutcome(relativePath, e);
				}
			});
		}

		for (TransferOutcome outcome : runTransfersInParallel(transfers)) {
			if (outcome.failure() == null) {
				downloadedFiles.add(outcome.fileKey());
			} else {
				failedFiles.add(outcome.fileKey());
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
	public String copyToStorage(String localFilePath, String storageFolderPath, Map<String, Object> metadata)
			throws Exception {
		List<Path> paths = parseLocalPaths(localFilePath);
		List<String> uploadedFiles = new ArrayList<>();
		List<String> failedFiles = new ArrayList<>();
		AtomicReference<String> lastVersionId = new AtomicReference<>(null);
		boolean found = false;

		// gather everything first so it can all go out together
		Map<Path, String> filesToUpload = new LinkedHashMap<>();
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
					for (Path file : stream.filter(Files::isRegularFile).toList()) {
						filesToUpload.put(file, buildBlobName(storageFolderPath, file, filePath));
					}
				}
				found = true;
			} else {
				filesToUpload.put(filePath, buildBlobName(storageFolderPath, filePath, filePath.getParent()));
				found = true;
			}
		}

		List<Callable<TransferOutcome>> transfers = new ArrayList<>(filesToUpload.size());
		for (Map.Entry<Path, String> entry : filesToUpload.entrySet()) {
			Path file = entry.getKey();
			String blobName = entry.getValue();
			transfers.add(() -> {
				try {
					String generation = uploadBlob(blobName, file, metadata);
					if (generation != null) {
						lastVersionId.set(generation);
					}
					// the blob name, not the local path - rollbackUploads deletes by name
					return new TransferOutcome(blobName, null);
				} catch (Exception e) {
					classLogger.error("Failed to upload file: {}", file, e);
					return new TransferOutcome(blobName, e);
				}
			});
		}

		for (TransferOutcome outcome : runTransfersInParallel(transfers)) {
			if (outcome.failure() == null) {
				uploadedFiles.add(outcome.fileKey());
			} else {
				failedFiles.add(outcome.fileKey());
			}
		}
		if (!failedFiles.isEmpty()) {
			// once at the end, not inside the loop where it re-deleted the whole list
			// on every failure
			rollbackUploads(storage, failedFiles);
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
		List<Callable<TransferOutcome>> transfers = new ArrayList<>();
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
				// made here rather than inside the download so parallel tasks are not
				// racing to create the same parent
				Files.createDirectories(localFilePath.getParent());
				found = true;

				transfers.add(() -> {
					try {
						retryOperation(() -> downloadFile(blob, localFilePath), "Downloading file: " + blobName);
						classLogger.info("Downloaded file: {}", localFilePath);
						return new TransferOutcome(blobName, null);
					} catch (Exception e) {
						classLogger.error("Failed to download: {}", blobName, e);
						// the rollback works off local paths, so report the relative one
						return new TransferOutcome(relativePath, e);
					}
				});
			}
		}

		for (TransferOutcome outcome : runTransfersInParallel(transfers)) {
			if (outcome.failure() == null) {
				downloadedFiles.add(outcome.fileKey());
			} else {
				failedFiles.add(outcome.fileKey());
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
				throw new IllegalArgumentException("Object not found in GCS: " + key + " with generation=" + versionId);
			}

			downloadFile(blob, localFilePath);
			classLogger.info("Downloaded versioned file: {} (generation={})", localFilePath, versionId);
		} else {
			copyToLocal(storageFilePath, localFolderPath);
		}
	}

	@Override
	public void deleteFromStorage(String storagePath, boolean leaveFolderStructure) throws Exception {
		List<String> deletedFiles = new ArrayList<>();
		List<String> failedFiles = new ArrayList<>();
		storagePath = normalizeStoragePrefixPath(storagePath);
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

		storageFolderPath = normalizeStoragePrefixPath(storageFolderPath);

		boolean folderExists = false;

		classLogger.info(storageFolderPath.isEmpty() ? "Folder path is empty. Deleting all files in bucket: {}"
				: "Deleting folder: {}", storageFolderPath.isEmpty() ? this.bucket : storageFolderPath);

		// the trailing slash bounds the listing to this folder. A bare prefix of "dir"
		// also returns "dirty/..." and this method deletes everything it lists. An
		// empty path is still the whole bucket by design
		String prefix = storageFolderPath.isEmpty() ? "" : storageFolderPath + "/";

		Page<Blob> blobs = prefix.isEmpty() ? this.bucket.list()
				: this.bucket.list(Storage.BlobListOption.prefix(prefix));

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

	/**
	 * Writes one local file to a blob, always. Whether it needed writing is decided
	 * by the caller from a listing rather than by asking about this one blob.
	 *
	 * Small files go out in a single request. Anything larger is streamed in chunks
	 * instead of being read into a byte array, so the heap cost stays flat no
	 * matter how big the file is - the previous readAllBytes meant a 1GB file
	 * needed 1GB of heap, which does not survive several uploads running at once.
	 *
	 * Metadata is set on the blob as part of the same write, rather than in a
	 * separate update call afterwards.
	 *
	 * @param blobName the blob to write
	 * @param file     the local file
	 * @param metadata user metadata to attach, may be null
	 * @return the generation of the written blob, or null when the bucket does not
	 *         report one
	 * @throws IOException if the local file cannot be read
	 */
	private String uploadBlob(String blobName, Path file, Map<String, Object> metadata) throws IOException {
		BlobId blobId = BlobId.of(this.BUCKET, blobName);
		BlobInfo.Builder blobInfoBuilder = BlobInfo.newBuilder(blobId);
		Map<String, String> flatMetadata = flattenMetadata(metadata);
		if (!flatMetadata.isEmpty()) {
			blobInfoBuilder.setMetadata(flatMetadata);
		}
		BlobInfo blobInfo = blobInfoBuilder.build();

		long fileSize = Files.size(file);
		AtomicReference<String> generationRef = new AtomicReference<>(null);
		retryOperation(() -> {
			try {
				Blob blob;
				if (fileSize <= SINGLE_REQUEST_UPLOAD_BYTES) {
					// one round trip, and the heap cost is capped by the threshold
					blob = storage.create(blobInfo, Files.readAllBytes(file));
				} else {
					blob = storage.createFrom(blobInfo, file, UPLOAD_CHUNK_SIZE_BYTES);
				}
				classLogger.info("Uploaded file to GCS: {}", blobName);
				if (blob.getGeneration() != null) {
					generationRef.set(String.valueOf(blob.getGeneration()));
				}
			} catch (IOException e) {
				// retryOperation takes a Runnable, so this has to travel unchecked
				throw new UncheckedIOException("Failed to upload " + file, e);
			}
		}, "Uploading: " + blobName);

		return generationRef.get();
	}

	/**
	 * Builds the blob name for a local file under a storage path.
	 *
	 * @param storagePath the folder in the bucket, may be empty
	 * @param file        the local file
	 * @param basePath    the local folder the name is relative to
	 * @return the blob name to write
	 */
	private String buildBlobName(String storagePath, Path file, Path basePath) {
		String normalizedStoragePath = normalizeStoragePrefixPath(storagePath);
		String relativePath = Utility.normalizePath(basePath.relativize(file).toString()).trim();
		return normalizedStoragePath.isEmpty() ? relativePath : normalizedStoragePath + "/" + relativePath;
	}

	/**
	 * Snapshots what the bucket already holds under a prefix, so a sync can compare
	 * in memory instead of fetching each blob's metadata individually.
	 *
	 * @param storagePath the folder being synced to
	 * @return blob name to size and last modified
	 */
	private Map<String, StoredObjectStat> listStoredBlobs(String storagePath) {
		String normalizedStoragePath = normalizeStoragePrefixPath(storagePath);
		String prefix = normalizedStoragePath.isEmpty() ? "" : normalizedStoragePath + "/";

		Map<String, StoredObjectStat> stored = new HashMap<>();
		try {
			Page<Blob> blobs = prefix.isEmpty() ? this.bucket.list()
					: this.bucket.list(Storage.BlobListOption.prefix(prefix));
			for (Blob blob : blobs.iterateAll()) {
				if (blob.getSize() == null || blob.getUpdateTimeOffsetDateTime() == null) {
					continue;
				}
				stored.put(blob.getName(), new StoredObjectStat(blob.getSize(),
						blob.getUpdateTimeOffsetDateTime().toInstant().toEpochMilli()));
			}
		} catch (Exception e) {
			// losing the listing only costs the skip optimization, so upload everything
			classLogger.warn("Unable to list {} before syncing, every file will be uploaded", prefix, e);
			return Collections.emptyMap();
		}
		return stored;
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
		// normalize the same way uploadBlob does, otherwise we list a prefix that does
		// not match the blobs we write
		String normalizedDirectory = normalizeStoragePrefixPath(blobDirectory);
		// the trailing slash bounds the listing to this folder. A bare prefix of "dir"
		// also returns "dirty/..." and none of those resolve to a local file, so every
		// one of them would look stale and get deleted
		String prefix = normalizedDirectory.isEmpty() ? "" : normalizedDirectory + "/";

		Page<Blob> blobs = storage.list(this.BUCKET, Storage.BlobListOption.prefix(prefix));

		for (Blob blob : blobs.iterateAll()) {
			String blobName = blob.getName();
			// safe because listing by prefix only returns names that start with it
			String relativePath = blobName.substring(prefix.length());
			if (relativePath.isEmpty()) {
				// the zero byte placeholder for the folder itself, not a file
				continue;
			}
			Path localFilePath = localBasePath.resolve(relativePath.replace("/", File.separator)).normalize();

			try {
				// nothing local to match it, so it is stale. Size is deliberately not
				// part of this - an empty file the user uploaded is still a file, and
				// a placeholder has no local counterpart anyway
				if (!Files.exists(localFilePath)) {
					storage.delete(blob.getBlobId());
					classLogger.info("Deleted storage file not found in local: {}", blobName);
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

	private void deleteEmptyBlobs(String storageFolderPath) {
		// normalize and bound to this folder. A bare prefix of "dir" would also match
		// "dirty/..." and clean up placeholders outside the folder we just wrote to
		String normalizedPrefix = normalizeStoragePrefixPath(storageFolderPath);
		String prefix = normalizedPrefix.isEmpty() ? "" : normalizedPrefix + "/";

		Page<Blob> blobs = storage.list(this.BUCKET, Storage.BlobListOption.prefix(prefix));

		for (Blob blob : blobs.iterateAll()) {
			if (isFolderPlaceholder(blob.getName(), blob.getSize())) {
				storage.delete(blob.getBlobId());
				classLogger.info("Deleted folder placeholder: {}", blob.getName());
			}
		}
	}

	/**
	 * Streams a blob to a local file.
	 *
	 * downloadTo is used rather than getContent because getContent materializes the
	 * whole object as a byte array first, which does not survive several concurrent
	 * downloads of large files.
	 *
	 * @param blob          the blob to fetch
	 * @param localFilePath where to write it
	 */
	private void downloadFile(Blob blob, Path localFilePath) {
		blob.downloadTo(localFilePath);
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
