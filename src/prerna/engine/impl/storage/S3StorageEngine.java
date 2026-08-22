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
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
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

import prerna.engine.api.StorageTypeEnum;
import prerna.util.Utility;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ObjectVersion;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

public class S3StorageEngine extends AbstractStorageEngine {

	private static final Logger classLogger = LogManager.getLogger(S3StorageEngine.class);

	public static final String S3_REGION_KEY = "S3_REGION";
	public static final String S3_BUCKET_KEY = "S3_BUCKET";
	public static final String S3_ACCESS_KEY = "S3_ACCESS";
	public static final String S3_SECRET_KEY = "S3_SECRET";
	public static final String S3_ENDPOINT_KEY = "S3_ENDPOINT";
	public static final String S3_PATH_STYLE_ACCESS_KEY = "S3_PATH_STYLE_ACCESS";
	public static final String S3_KMS_ID_KEY = "S3_KMS_ID";

	// these match rclone's s3 defaults, which are a well travelled set of numbers:
	// --s3-upload-cutoff 200Mi, --s3-chunk-size 5Mi, --s3-upload-concurrency 4.
	// A single PUT is also capped at 5GB by S3, so past the cutoff multipart is the
	// only option regardless of speed
	private static final long MULTIPART_THRESHOLD_BYTES = 200L * 1024 * 1024;
	private static final long MULTIPART_PART_SIZE_BYTES = 5L * 1024 * 1024;
	private static final int MULTIPART_UPLOAD_CONCURRENCY = 4;
	// S3 will not accept more than this many parts for one object
	private static final int MULTIPART_MAX_PARTS = 10000;

	// older property names this engine still accepts. The types that used to be
	// backed by a different implementation wrote these instead, and the smss files
	// out there have not been rewritten
	private static final String LEGACY_S3_ACCESS_KEY = "S3_ACCESS_KEY";
	private static final String LEGACY_S3_SECRET_KEY = "S3_SECRET_KEY";
	private static final String LEGACY_MINIO_ACCESS_KEY = "MINIO_ACCESS_KEY";
	private static final String LEGACY_MINIO_SECRET_KEY = "MINIO_SECRET_KEY";
	private static final String LEGACY_MINIO_REGION_KEY = "MINIO_REGION";
	private static final String LEGACY_MINIO_BUCKET_KEY = "MINIO_BUCKET";
	private static final String LEGACY_MINIO_ENDPOINT_KEY = "MINIO_ENDPOINT";
	// public so the masking list can name them - the Ceph type no longer has an
	// engine class of its own that owns these
	public static final String LEGACY_CEPH_ACCESS_KEY = "CEPH_ACCESS_KEY";
	public static final String LEGACY_CEPH_SECRET_KEY = "CEPH_SECRET_KEY";
	private static final String LEGACY_CEPH_BUCKET_KEY = "CEPH_BUCKET";
	private static final String LEGACY_CEPH_ENDPOINT_KEY = "CEPH_ENDPOINT";

	private transient String accessKey;
	private transient String secretKey;
	private transient String region;
	private transient String bucket;
	private transient String endpoint;
	// Use full ARN or the Key ID. Must match the S3 bucket's AWS Region.
	private transient String kmsId;
	protected boolean pathStyleAccess = false;

	private transient S3Client client = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		migrateLegacyProperties(smssProp);

		this.accessKey = smssProp.getProperty(S3_ACCESS_KEY);
		this.secretKey = smssProp.getProperty(S3_SECRET_KEY);
		this.region = smssProp.getProperty(S3_REGION_KEY);
		this.bucket = smssProp.getProperty(S3_BUCKET_KEY);
		this.endpoint = smssProp.getProperty(S3_ENDPOINT_KEY);
		String pathStyleAccessStr = smssProp.getProperty(S3_PATH_STYLE_ACCESS_KEY);
		if (pathStyleAccessStr != null && !pathStyleAccessStr.isEmpty()) {
			this.pathStyleAccess = Boolean.parseBoolean(pathStyleAccessStr);
		}
		this.kmsId = smssProp.getProperty(S3_KMS_ID_KEY);
		if (this.kmsId != null) {
			this.kmsId = this.kmsId.trim();
		}

		if (this.accessKey == null || this.accessKey.isEmpty()) {
			throw new IllegalArgumentException("Must pass in an access key");
		}
		if (this.secretKey == null || this.secretKey.isEmpty()) {
			throw new IllegalArgumentException("Must pass in a secret key");
		}
		if (this.region == null || this.region.isEmpty()) {
			throw new IllegalArgumentException("Must pass in a region");
		}
		if (this.bucket == null || this.bucket.isEmpty()) {
			throw new IllegalArgumentException("Must pass in a S3BucketPath");
		}
		createServiceClient();
	}

	/**
	 * Carries the older property names forward onto the ones this engine reads.
	 *
	 * The S3, MINIO and CEPH storage types were all backed by a different
	 * implementation that read its own property names, so smss files already out
	 * there use those. Anything explicitly set under the current key wins; these
	 * are only a fallback.
	 *
	 * @param smssProp the properties being opened, updated in place
	 */
	protected void migrateLegacyProperties(Properties smssProp) {
		migrateLegacyProperty(smssProp, S3_ACCESS_KEY, LEGACY_S3_ACCESS_KEY, LEGACY_MINIO_ACCESS_KEY,
				LEGACY_CEPH_ACCESS_KEY);
		migrateLegacyProperty(smssProp, S3_SECRET_KEY, LEGACY_S3_SECRET_KEY, LEGACY_MINIO_SECRET_KEY,
				LEGACY_CEPH_SECRET_KEY);
		migrateLegacyProperty(smssProp, S3_REGION_KEY, LEGACY_MINIO_REGION_KEY);
		migrateLegacyProperty(smssProp, S3_BUCKET_KEY, LEGACY_MINIO_BUCKET_KEY, LEGACY_CEPH_BUCKET_KEY);
		migrateLegacyProperty(smssProp, S3_ENDPOINT_KEY, LEGACY_MINIO_ENDPOINT_KEY, LEGACY_CEPH_ENDPOINT_KEY);
	}

	public void createServiceClient() {
		software.amazon.awssdk.services.s3.S3ClientBuilder builder = S3Client.builder();
		builder.region(Region.of(this.region)).credentialsProvider(
				StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)));

		if (this.endpoint != null && !this.endpoint.isEmpty()) {
			try {
				builder.endpointOverride(new java.net.URI(this.endpoint));
				if (this.pathStyleAccess) {
					builder.forcePathStyle(true);
				}
				classLogger.info("Using S3 endpoint override: {}", this.endpoint);
			} catch (java.net.URISyntaxException e) {
				classLogger.error("Invalid S3 endpoint URI: {}", this.endpoint, e);
				throw new RuntimeException("Invalid S3 endpoint URI: " + this.endpoint, e);
			}
		}

		this.client = builder.build();
		classLogger.info("S3 Blob Service client created successfully.");
	}

	@Override
	public StorageTypeEnum getStorageType() {
		return StorageTypeEnum.S3;
	}

	@Override
	public List<String> list(String path) throws Exception {
		List<Map<String, Object>> details = listDetails(path);
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
	public List<Map<String, Object>> listDetails(String path) throws Exception {
		List<Map<String, Object>> objectDetails = new ArrayList<>();
		path = normalizeStoragePrefixPath(path);
		String prefix = path.isEmpty() ? "" : path + "/";

		try {
			ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder().bucket(this.bucket)
					.delimiter("/");
			if (!prefix.isEmpty()) {
				requestBuilder.prefix(prefix);
			}
			ListObjectsV2Iterable listObjectsV2Response = this.client.listObjectsV2Paginator(requestBuilder.build());
			for (CommonPrefix commonPrefix : listObjectsV2Response.commonPrefixes()) {
				String dirPath = commonPrefix.prefix();
				if (dirPath == null || dirPath.equals(prefix)) {
					continue;
				}
				String dirName = prefix.isEmpty() ? dirPath : dirPath.substring(prefix.length());
				if (dirName.endsWith("/")) {
					dirName = dirName.substring(0, dirName.length() - 1);
				}
				if (dirName.isEmpty() || dirName.contains("/")) {
					continue;
				}
				Map<String, Object> objectInfo = new HashMap<>();
				objectInfo.put("Path", path.isEmpty() ? "/" + dirName : "/" + path + "/" + dirName);
				objectInfo.put("Name", dirName);
				objectInfo.put("Size", 0L);
				objectInfo.put("MimeType", "inode/directory");
				objectInfo.put("ModTime", null);
				objectInfo.put("IsDir", true);
				objectInfo.put("Metadata", Collections.emptyMap());
				objectDetails.add(objectInfo);
			}

			for (S3Object object : listObjectsV2Response.contents()) {
				String key = object.key();
				if (key == null || key.equals(prefix)) {
					continue;
				}
				String fileName = prefix.isEmpty() ? key : key.substring(prefix.length());
				if (fileName.isEmpty() || fileName.contains("/")) {
					continue;
				}
				Map<String, Object> objectInfo = new HashMap<>();
				// Fetch object metadata
				HeadObjectRequest headRequest = HeadObjectRequest.builder().bucket(this.bucket).key(key).build();
				HeadObjectResponse headResponse = this.client.headObject(headRequest);
				Map<String, String> metadata = headResponse.metadata();

				objectInfo.put("Path", path.isEmpty() ? "/" + fileName : "/" + path + "/" + fileName);
				objectInfo.put("Name", fileName);
				objectInfo.put("Size", object.size());
				objectInfo.put("MimeType", headResponse.contentType());
				objectInfo.put("ModTime", object.lastModified() == null ? null : object.lastModified().toString());
				objectInfo.put("IsDir", false);
				objectInfo.put("Metadata", (metadata != null) ? metadata : Collections.emptyMap());
				objectDetails.add(objectInfo);
			}

		} catch (S3Exception e) {
			classLogger.error("Failed to list S3 storage details for bucket='{}' path='{}'.", this.bucket, path, e);
		}

		return objectDetails;
	}

	@Override
	public List<Map<String, Object>> listVersions(String storagePath) throws Exception {
		List<Map<String, Object>> versions = new ArrayList<>();
		String key = normalizeStoragePrefixPath(storagePath);

		ListObjectVersionsRequest request = ListObjectVersionsRequest.builder().bucket(this.bucket).prefix(key).build();
		ListObjectVersionsResponse response = this.client.listObjectVersions(request);
		for (ObjectVersion version : response.versions()) {
			// Only include exact key matches (not prefix matches)
			if (!version.key().equals(key)) {
				continue;
			}
			Map<String, Object> versionInfo = new LinkedHashMap<>();
			versionInfo.put("versionId", version.versionId());
			versionInfo.put("lastModified", version.lastModified() != null ? version.lastModified().toString() : null);
			versionInfo.put("size", version.size());
			versionInfo.put("isLatest", version.isLatest());
			versionInfo.put("key", version.key());
			versions.add(versionInfo);
		}

		return versions;
	}

	@Override
	public StorageSyncStatus syncLocalToStorage(String localPath, String storagePath, Map<String, Object> metadata)
			throws Exception {

		Path localFilePath = Paths.get(localPath);
		List<String> uploadedFiles = new ArrayList<>();
		List<String> skippedFiles = new ArrayList<>();
		List<String> failedFiles = new ArrayList<>();
		boolean found = false;
		try {
			if (!Files.exists(localFilePath)) {
				throw new Exception("Invalid path:" + localPath);
			}
			Path localBasePath = Files.isDirectory(localFilePath) ? localFilePath : localFilePath.getParent();

			// Remove empty directories locally
			deleteEmptyDirectories(localFilePath);

			// Delete extra directory from AWS s3 storage
			syncStorageDeletion(storagePath, localBasePath);

			// one listing up front instead of a HEAD per file. A listing page carries the
			// size and last modified of up to 1000 keys, which is all the comparison needs
			Map<String, StoredObjectStat> alreadyStored = listStoredObjects(storagePath);

			List<Path> localFiles;
			try (Stream<Path> stream = Files.walk(localFilePath)) {
				localFiles = stream.filter(Files::isRegularFile).toList();
			}

			List<Callable<TransferOutcome>> transfers = new ArrayList<>(localFiles.size());
			for (Path file : localFiles) {
				String fileKey = buildFileKey(storagePath, file, localBasePath);
				if (!needsUpload(file, alreadyStored.get(fileKey))) {
					classLogger.info("Skipping unchanged file: {}", fileKey);
					skippedFiles.add(fileKey);
					continue;
				}
				transfers.add(() -> {
					try {
						uploadFile(fileKey, file, metadata);
						return new TransferOutcome(fileKey, null);
					} catch (Exception e) {
						// kept per file so one bad file does not abandon the rest, and the
						// status still reports it
						classLogger.error("Failed to upload file: {}", file, e);
						return new TransferOutcome(fileKey, e);
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
			classLogger.error("Sync operation failed for: {}", storagePath, e);
			throw e;

		}

		if (uploadedFiles.isEmpty()) {
			classLogger.info("No files were uploaded.");
		} else {
			classLogger.info("Successfully synced {} files to: {}", uploadedFiles.size(), storagePath);
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

	/**
	 * Snapshots what storage already holds under a prefix, so the sync can compare
	 * in memory instead of issuing a HEAD per file.
	 *
	 * @param storagePath the folder being synced to
	 * @return key to size and last modified, empty when the prefix holds nothing
	 */
	private Map<String, StoredObjectStat> listStoredObjects(String storagePath) {
		String normalizedStoragePath = normalizeStoragePrefixPath(storagePath);
		String prefix = normalizedStoragePath.isEmpty() ? "" : normalizedStoragePath + "/";

		Map<String, StoredObjectStat> stored = new HashMap<>();
		try {
			ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(this.bucket).prefix(prefix).build();
			for (S3Object s3Object : this.client.listObjectsV2Paginator(request).contents()) {
				stored.put(s3Object.key(),
						new StoredObjectStat(s3Object.size(), s3Object.lastModified().toEpochMilli()));
			}
		} catch (S3Exception e) {
			// an unreadable listing only costs us the skip optimization, so upload
			// everything rather than failing the sync
			classLogger.warn("Unable to list {} before syncing, every file will be uploaded", prefix, e);
			return Collections.emptyMap();
		}
		return stored;
	}

	@Override
	public void syncStorageToLocal(String storagePath, String localPath) throws Exception {
		Path localDirectory = Paths.get(localPath);
		Files.createDirectories(localDirectory);

		Set<String> cloudFiles = new HashSet<>();
		List<String> downloadedFiles = new ArrayList<>(), failedFiles = new ArrayList<>();
		boolean found = false;
		String requestedPath = normalizeStoragePrefixPath(storagePath);

		// Delete zero-byte objects from S3
		deleteEmptyBlobsFromS3(requestedPath);

		ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(this.bucket).prefix(requestedPath).build();
		ListObjectsV2Iterable response = this.client.listObjectsV2Paginator(request);
		SdkIterable<S3Object> contents = response.contents();

		// the listing already carries size and last modified, so deciding what to pull
		// costs nothing extra. Directories are created up front because doing it inside
		// the parallel downloads would have several threads racing on the same parent
		List<Callable<TransferOutcome>> transfers = new ArrayList<>();
		for (S3Object s3Object : contents) {
			String key = s3Object.key();
			String relativePath = resolveRelativeStoragePath(key, requestedPath);
			if (relativePath == null) {
				continue;
			}

			Path localFilePath = localDirectory.resolve(relativePath.replace("/", File.separator));
			cloudFiles.add(localFilePath.toString());
			Files.createDirectories(localFilePath.getParent());

			if (Files.isDirectory(localFilePath)) {
				continue;
			}
			found = true;

			boolean fileExists = Files.exists(localFilePath);
			if (fileExists) {
				FileTime localModifiedTime = Files.getLastModifiedTime(localFilePath);
				if (s3Object.size() == Files.size(localFilePath)
						&& s3Object.lastModified().toEpochMilli() <= localModifiedTime.toMillis()) {
					continue;
				}
			}

			transfers.add(() -> {
				try {
					retryOperation(() -> {
						try {
							downloadFile(this.bucket, key, localFilePath, null);
						} catch (IOException e) {
							classLogger.error("Failed to write file: {}", localFilePath, e);
							throw new RuntimeException("Error writing file: " + localFilePath, e);
						}
					}, "Syncing file to local: " + key);
					classLogger.info(fileExists ? "Updated file: {}" : "Downloaded new file: {}", localFilePath);
					return new TransferOutcome(key, null);
				} catch (Exception e) {
					classLogger.error("Failed to sync file: {}", key, e);
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

		// Delete local files not present in S3
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
		// Delete empty local directories
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

		// gather every file first so they can all go out together rather than one
		// directory at a time
		Map<Path, String> filesToUpload = new LinkedHashMap<>();
		for (Path path : paths) {
			if (!Files.exists(path)) {
				classLogger.error("File not found: {}", path);
				failedFiles.add(path.toString());
				continue;
			}

			deleteEmptyDirectories(path);

			if (Files.isDirectory(path)) {
				try (Stream<Path> stream = Files.walk(path)) {
					for (Path file : stream.filter(Files::isRegularFile).toList()) {
						filesToUpload.put(file, buildFileKey(storageFolderPath, file, path));
					}
				}
				found = true;
			} else {
				filesToUpload.put(path, buildFileKey(storageFolderPath, path, path.getParent()));
				found = true;
			}
		}

		List<Callable<TransferOutcome>> transfers = new ArrayList<>(filesToUpload.size());
		for (Map.Entry<Path, String> entry : filesToUpload.entrySet()) {
			Path file = entry.getKey();
			String fileKey = entry.getValue();
			transfers.add(() -> {
				try {
					String versionId = uploadFile(fileKey, file, metadata);
					if (versionId != null) {
						lastVersionId.set(versionId);
					}
					return new TransferOutcome(file.toString(), null);
				} catch (Exception e) {
					classLogger.error("Failed to upload file: {}", file, e);
					return new TransferOutcome(file.toString(), e);
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
			// rolled back once at the end rather than inside the loop, which used to
			// re-delete the whole failed list on every failure
			rollbackUploads(this.client, failedFiles);
		}
		// Delete empty blobs from S3
		deleteEmptyBlobsFromS3(storageFolderPath);
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
	public void copyToLocal(String storageFilePath, String localFolderPath, String versionId) throws Exception {
		List<String> paths = parseStorageObjectPaths(storageFilePath);
		Path localDirectory = Paths.get(localFolderPath);

		// Ensure local directory exists
		Files.createDirectories(localDirectory);

		List<String> downloadedFiles = new ArrayList<>();
		List<String> failedFiles = new ArrayList<>();
		boolean found = false;

		List<Callable<TransferOutcome>> transfers = new ArrayList<>();
		for (String s3FolderPath : paths) {
			String requestedPath = normalizeStoragePrefixPath(s3FolderPath);

			// Delete empty folder blobs
			deleteEmptyBlobsFromS3(requestedPath);

			ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(this.bucket).prefix(requestedPath)
					.build();
			ListObjectsV2Iterable response = this.client.listObjectsV2Paginator(request);
			SdkIterable<S3Object> contents = response.contents();
			for (S3Object s3Object : contents) {
				String key = s3Object.key();

				// Skip empty folder markers (zero-byte keys ending with "/")
				if (key.endsWith("/") && s3Object.size() == 0) {
					continue;
				}

				String relativePath = resolveRelativeStoragePath(key, requestedPath);
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
						retryOperation(() -> {
							try {
								downloadFile(this.bucket, key, localFilePath, versionId);
							} catch (Exception e) {
								// rethrow so retryOperation can retry and the outcome
								// reports the failure, instead of logging and calling it
								// a success
								throw new RuntimeException("Failed to download: " + key, e);
							}
						}, "Downloading file: " + key);
						classLogger.info("Downloaded file: {}", localFilePath);
						return new TransferOutcome(key, null);
					} catch (Exception e) {
						classLogger.error("Failed to download: {}", key, e);
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

		// delete empty local folders
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
	public void copyToLocal(String storageFilePath, String localFolderPath) throws Exception {
		copyToLocal(storageFilePath, localFolderPath, null);
	}

	@Override
	public void deleteFromStorage(String storagePath, boolean leaveFolderStructure) throws Exception {
		storagePath = normalizeStoragePrefixPath(storagePath);

		List<String> deletedFiles = new ArrayList<>();
		List<String> failedFiles = new ArrayList<>();
		boolean hasFilesToDelete = false;

		ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(this.bucket).prefix(storagePath).build();
		ListObjectsV2Iterable response = this.client.listObjectsV2Paginator(request);
		SdkIterable<S3Object> contents = response.contents();
		for (S3Object s3Object : contents) {
			String objectKey = s3Object.key();

			if (storagePath.isEmpty() || objectKey.equals(storagePath) || objectKey.startsWith(storagePath + "/")) {
				hasFilesToDelete = true;

				if (deleteObject(objectKey)) {
					deletedFiles.add(objectKey);
				} else {
					failedFiles.add(objectKey);
				}
			}
		}

		if (!hasFilesToDelete) {
			classLogger.warn(storagePath.isEmpty() ? "No files found in bucket: {}" : "No files found in directory: {}",
					storagePath.isEmpty() ? this.bucket : storagePath);
			return;
		}
		if (deletedFiles.isEmpty()) {
			classLogger.info("No files were deleted.");
		} else {
			classLogger.info("Successfully deleted files: {}", deletedFiles);
		}

		if (!failedFiles.isEmpty()) {
			classLogger.error("Some files failed to delete. Retrying...");
			retryDelete(failedFiles);
		}

		if (leaveFolderStructure && !deletedFiles.isEmpty()) {
			preserveFolderStructure(deletedFiles);
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

		ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(this.bucket).prefix(prefix).build();
		ListObjectsV2Iterable response = this.client.listObjectsV2Paginator(request);
		SdkIterable<S3Object> contents = response.contents();
		for (S3Object s3Object : contents) {
			folderExists = true;
			String objectKey = s3Object.key();
			try {
				retryOperation(() -> {
					DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder().bucket(this.bucket).key(objectKey)
							.build();
					this.client.deleteObject(deleteRequest);
					classLogger.info("Deleted file: {}", objectKey);
					deletedFiles.add(objectKey);
				}, "Deleting file: " + objectKey);
			} catch (Exception e) {
				failedFiles.add(objectKey);
				classLogger.error("Failed to delete file: {}", objectKey, e);
			}
		}

		if (deletedFiles.isEmpty()) {
			classLogger.info("No files were deleted.");
		} else {
			classLogger.info("Successfully deleted files: {}", deletedFiles);
		}

		if (!failedFiles.isEmpty()) {
			classLogger.error("Some files failed to delete. Retrying...");
			retryDelete(failedFiles);
		}

		classLogger.info(folderExists ? "Successfully deleted folder: {}" : "No files found in directory: {}",
				storageFolderPath);
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

	private void syncStorageDeletion(String storagePath, Path localBasePath) {
		try {
			// normalize the same way uploadFile does, otherwise we list a prefix that
			// does not match the keys we write
			String normalizedStoragePath = normalizeStoragePrefixPath(storagePath);
			// the trailing slash bounds the listing to this folder. A bare prefix of
			// "dir" also returns "dirty/..." and none of those resolve to a local file,
			// so every one of them would look stale and get deleted
			String prefix = normalizedStoragePath.isEmpty() ? "" : normalizedStoragePath + "/";

			ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(this.bucket).prefix(prefix).build();
			ListObjectsV2Iterable response = this.client.listObjectsV2Paginator(request);
			SdkIterable<S3Object> contents = response.contents();
			for (S3Object s3Object : contents) {
				String objectKey = s3Object.key();
				// safe because listing by prefix only returns keys that start with it
				String relativePath = objectKey.substring(prefix.length());
				if (relativePath.isEmpty()) {
					// the zero byte folder marker for the folder itself, not a file
					continue;
				}
				Path localFile = localBasePath.resolve(relativePath.replace("/", File.separator)).normalize();

				if (!Files.exists(localFile)) {
					this.client.deleteObject(DeleteObjectRequest.builder().bucket(this.bucket).key(objectKey).build());

					classLogger.info("Deleted stale object from S3: {}", objectKey);
				}
			}
		} catch (S3Exception e) {
			classLogger.error("Error while deleting stale objects from S3", e);
		}
	}

	private void deleteEmptyDirectories(Path path) {
		try (Stream<Path> stream = Files.walk(path)) {
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

	private void downloadFile(String bucketName, String key, Path destinationPath, String versionId)
			throws IOException {
		GetObjectRequest.Builder getRequestBuilder = GetObjectRequest.builder().bucket(this.bucket).key(key);
		if (versionId != null && !(versionId = versionId.trim()).isEmpty()) {
			getRequestBuilder.versionId(versionId);
		}
		GetObjectRequest getRequest = getRequestBuilder.build();

		Files.createDirectories(destinationPath.getParent());
		// Download and write the object
		try (ResponseInputStream<GetObjectResponse> s3Object = this.client.getObject(getRequest)) {
			Files.copy(s3Object, destinationPath, StandardCopyOption.REPLACE_EXISTING);
		}

		classLogger.info("File downloaded from S3: {} -> {}", key, destinationPath);
	}

	private boolean deleteObject(String objectKey) {
		try {
			retryOperation(() -> {
				this.client.deleteObject(DeleteObjectRequest.builder().bucket(this.bucket).key(objectKey).build());
				classLogger.info("Deleted object: {}", objectKey);
			}, "Deleting object: " + objectKey);
			return true;
		} catch (Exception e) {
			classLogger.error("Failed to delete object: {}", objectKey, e);
			return false;
		}
	}

	private void preserveFolderStructure(List<String> deletedFiles) {
		Set<String> folderPaths = deletedFiles.stream()
				.map(file -> file.contains("/") ? file.substring(0, file.lastIndexOf("/") + 1) : "")
				.filter(path -> !path.isEmpty()).collect(Collectors.toSet());

		for (String folderPath : folderPaths) {
			PutObjectRequest request = PutObjectRequest.builder().bucket(this.bucket).key(folderPath).build();

			this.client.putObject(request, RequestBody.empty());
			classLogger.info("Preserved folder structure: {}", folderPath);
		}
	}

	private void rollbackDownloads(List<String> failedKeys, Path baseDirectory) {
		for (String key : failedKeys) {
			try {
				Path filePath = baseDirectory.resolve(key.replace("/", File.separator));
				Files.deleteIfExists(filePath);
				classLogger.info("Rolled back file: {}", filePath);
			} catch (IOException e) {
				classLogger.error("Failed to rollback file: {}", key, e);
			}
		}
	}

	private void deleteEmptyBlobsFromS3(String storagePrefix) {
		// normalize and bound to this folder, the same as syncStorageDeletion. A bare
		// prefix of "dir" would also match "dirty/..." and delete zero byte objects
		// that live outside the folder we just wrote to
		String normalizedPrefix = normalizeStoragePrefixPath(storagePrefix);
		String prefix = normalizedPrefix.isEmpty() ? "" : normalizedPrefix + "/";

		ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(this.bucket).prefix(prefix).build();
		ListObjectsV2Iterable response = this.client.listObjectsV2Paginator(request);
		SdkIterable<S3Object> contents = response.contents();
		for (S3Object s3Object : contents) {
			if (s3Object.size() == 0) {
				DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder().bucket(this.bucket)
						.key(s3Object.key()).build();
				this.client.deleteObject(deleteRequest);
				classLogger.info("Deleted empty blob from S3: {}", s3Object.key());
			}
		}
	}

	private void rollbackUploads(S3Client client, List<String> failedFiles) {
		for (String fileKey : failedFiles) {
			try {
				retryOperation(() -> {
					DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder().bucket(this.bucket).key(fileKey)
							.build();
					this.client.deleteObject(deleteRequest);
					classLogger.info("Rolled back failed upload: {}", fileKey);
				}, "Rolling back failed upload: " + fileKey);
			} catch (Exception e) {
				classLogger.error("Rollback failed for: {}", fileKey, e);
			}
		}
	}

	/**
	 * Builds the S3 object key for a local file. The storage path is run through
	 * normalizeStoragePrefixPath, so callers can pass whatever the user typed - see
	 * that method for the assumptions made about whitespace and leading/trailing
	 * slashes.
	 *
	 * @param storagePath the storage folder the file belongs under, may be empty to
	 *                    write at the root of the bucket
	 * @param filePath    the local file being uploaded
	 * @param basePath    the local folder the key is made relative to, so walking a
	 *                    directory keeps its structure in S3
	 * @return the object key to write to
	 */
	private String buildFileKey(String storagePath, Path filePath, Path basePath) {
		String normalizedStoragePath = normalizeStoragePrefixPath(storagePath);
		String relativePath = Utility.normalizePath(basePath.relativize(filePath).toString()).trim();
		return normalizedStoragePath.isEmpty() ? relativePath : normalizedStoragePath + "/" + relativePath;
	}

	/**
	 * Uploads a single local file to S3. Always writes - the sync path decides what
	 * to skip by comparing against a listing before it gets here.
	 *
	 * @param fileKey  the object key to write, from buildFileKey
	 * @param filePath the local file to upload
	 * @param metadata user metadata to attach, may be null. Values are converted
	 *                 with toString since S3 metadata is string only
	 * @return the S3 version id of the object just written, or null when the bucket
	 *         is not versioned. A null here never means failure: a failed upload
	 *         throws instead
	 * @throws Exception if the upload still fails after retryOperation exhausts its
	 *                   attempts
	 */
	private String uploadFile(String fileKey, Path filePath, Map<String, Object> metadata) throws Exception {
		AtomicReference<String> versionIdRef = new AtomicReference<>(null);
		// Prepare metadata
		Map<String, String> metaMap = flattenMetadata(metadata);

		long fileSize = Files.size(filePath);
		if (fileSize > MULTIPART_THRESHOLD_BYTES) {
			return uploadFileInParts(fileKey, filePath, metaMap, fileSize);
		}

		retryOperation(() -> {
			PutObjectRequest.Builder putRequestBuilder = PutObjectRequest.builder().bucket(this.bucket).key(fileKey);
			if (!metaMap.isEmpty()) {
				putRequestBuilder.metadata(metaMap);
			}
			if (this.kmsId != null && !this.kmsId.isEmpty()) {
				putRequestBuilder.serverSideEncryption(ServerSideEncryption.AWS_KMS).ssekmsKeyId(this.kmsId);
			}
			PutObjectRequest putRequest = putRequestBuilder.build();

			PutObjectResponse response = this.client.putObject(putRequest, filePath);
			classLogger.info("Uploaded/Updated file: {}", fileKey);
			if (response.versionId() != null) {
				classLogger.info("Version ID for {}: {}", fileKey, response.versionId());
				versionIdRef.set(response.versionId());
			}
		}, "Uploading file to S3: " + fileKey);

		return versionIdRef.get();
	}

	/**
	 * Sends one file as a multipart upload.
	 *
	 * A single PUT cannot exceed 5GB, so anything large has to go out this way
	 * regardless of speed. Parts are streamed from the file rather than read into
	 * memory, so the heap cost stays flat no matter how big the file is.
	 *
	 * Parts go out MULTIPART_UPLOAD_CONCURRENCY at a time, from their own permit
	 * pool. They cannot draw from the file level pool: the file already holds one
	 * of those permits, so sharing would let the outer batch starve its own parts.
	 * The product is what to watch - transferLimit files times this concurrency is
	 * the most requests in flight, and the sdk's connection pool defaults to 50.
	 *
	 * @param fileKey  the object key to write
	 * @param filePath the local file
	 * @param metaMap  already flattened metadata
	 * @param fileSize size in bytes, already known by the caller
	 * @return the version id, or null when the bucket is not versioned
	 * @throws Exception if any part fails. The upload is aborted first so the parts
	 *                   do not linger in the bucket
	 */
	private String uploadFileInParts(String fileKey, Path filePath, Map<String, String> metaMap, long fileSize)
			throws Exception {
		CreateMultipartUploadRequest.Builder createBuilder = CreateMultipartUploadRequest.builder().bucket(this.bucket)
				.key(fileKey);
		if (!metaMap.isEmpty()) {
			createBuilder.metadata(metaMap);
		}
		if (this.kmsId != null && !this.kmsId.isEmpty()) {
			createBuilder.serverSideEncryption(ServerSideEncryption.AWS_KMS).ssekmsKeyId(this.kmsId);
		}

		long partSize = partSizeFor(fileSize);
		String uploadId = this.client.createMultipartUpload(createBuilder.build()).uploadId();
		classLogger.info("Started multipart upload of {} ({} bytes, {} byte parts) as {}", fileKey, fileSize, partSize,
				uploadId);

		try {
			List<Callable<CompletedPart>> partUploads = new ArrayList<>();
			int partNumber = 1;
			for (long offset = 0; offset < fileSize; offset += partSize, partNumber++) {
				long partLength = Math.min(partSize, fileSize - offset);
				final long partOffset = offset;
				final int currentPart = partNumber;

				partUploads.add(() -> {
					UploadPartRequest partRequest = UploadPartRequest.builder().bucket(this.bucket).key(fileKey)
							.uploadId(uploadId).partNumber(currentPart).contentLength(partLength).build();
					// a fresh stream per call, since the sdk reopens the body to retry a part
					String eTag = this.client.uploadPart(partRequest, RequestBody.fromContentProvider(() -> {
						try {
							return new FileSliceStream(filePath, partOffset, partLength);
						} catch (IOException e) {
							throw new UncheckedIOException(e);
						}
					}, partLength, "application/octet-stream")).eTag();
					return CompletedPart.builder().partNumber(currentPart).eTag(eTag).build();
				});
			}

			// results come back in submission order, which is part number order, and
			// CompleteMultipartUpload requires them ascending
			List<CompletedPart> completedParts = runTransfersInParallel(partUploads, MULTIPART_UPLOAD_CONCURRENCY);

			CompleteMultipartUploadResponse response = this.client.completeMultipartUpload(
					CompleteMultipartUploadRequest.builder().bucket(this.bucket).key(fileKey).uploadId(uploadId)
							.multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build()).build());
			classLogger.info("Uploaded/Updated file: {} in {} parts", fileKey, completedParts.size());
			return response.versionId();
		} catch (Exception e) {
			classLogger.error("Multipart upload of {} failed, aborting {}", fileKey, uploadId, e);
			try {
				this.client.abortMultipartUpload(AbortMultipartUploadRequest.builder().bucket(this.bucket).key(fileKey)
						.uploadId(uploadId).build());
			} catch (Exception abortException) {
				// the parts are now orphaned and will bill until a lifecycle rule reaps them
				classLogger.error("Unable to abort multipart upload {} for {}", uploadId, fileKey, abortException);
			}
			throw e;
		}
	}

	/**
	 * Picks the part size for a file.
	 *
	 * The configured size is used as-is until the file is big enough that it would
	 * need more than the 10,000 parts S3 allows, at which point the parts have to
	 * grow. With 5MiB parts that happens just under 50GB.
	 *
	 * @param fileSize the size of the file about to be uploaded
	 * @return the part size to use, never smaller than the configured one
	 */
	private long partSizeFor(long fileSize) {
		long partSize = MULTIPART_PART_SIZE_BYTES;
		if (fileSize / partSize >= MULTIPART_MAX_PARTS) {
			// round up so the last part is not left over the limit
			partSize = (fileSize + MULTIPART_MAX_PARTS - 1) / MULTIPART_MAX_PARTS;
			classLogger.info("File is {} bytes, growing the part size to {} to stay within {} parts", fileSize,
					partSize, MULTIPART_MAX_PARTS);
		}
		return partSize;
	}

	/**
	 * One slice of a file exposed as a stream, so a multipart part can be sent
	 * without holding it in the heap.
	 */
	private static final class FileSliceStream extends InputStream {

		private final FileChannel channel;
		private long remaining;

		private FileSliceStream(Path file, long offset, long length) throws IOException {
			this.channel = FileChannel.open(file, StandardOpenOption.READ);
			this.channel.position(offset);
			this.remaining = length;
		}

		@Override
		public int read() throws IOException {
			byte[] single = new byte[1];
			int read = read(single, 0, 1);
			return read == -1 ? -1 : single[0] & 0xFF;
		}

		@Override
		public int read(byte[] buffer, int offset, int length) throws IOException {
			if (this.remaining <= 0) {
				return -1;
			}
			int read = this.channel.read(ByteBuffer.wrap(buffer, offset, (int) Math.min(length, this.remaining)));
			if (read > 0) {
				this.remaining -= read;
			}
			return read;
		}

		@Override
		public void close() throws IOException {
			this.channel.close();
		}
	}

	private void retryDelete(List<String> failedFiles) {
		List<String> remainingFailedFiles = new ArrayList<>();

		for (String objectKey : failedFiles) {
			if (deleteObject(objectKey)) {
				classLogger.info("Successfully deleted on retry: {}", objectKey);
			} else {
				remainingFailedFiles.add(objectKey);
			}
		}

		if (!remainingFailedFiles.isEmpty()) {
			classLogger.error("Some files still failed to delete after retries: {}", remainingFailedFiles);
		} else {
			classLogger.info("All previously failed files were successfully deleted after retry.");
		}
	}

	@Override
	public void close() throws IOException {
		if (client != null) {
			client.close();
		}
	}

}
