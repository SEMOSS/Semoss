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
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.StorageTypeEnum;
import prerna.util.Utility;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

public class AWSNativeBlobStorageEngine extends AbstractStorageEngine {

	private static final Logger classLogger = LogManager.getLogger(AWSNativeBlobStorageEngine.class);

	public static final String S3_REGION_KEY = "S3_REGION";
	public static final String S3_BUCKET_KEY = "S3_BUCKET";
	public static final String S3_ACCESS_KEY = "S3_ACCESS";
	public static final String S3_SECRET_KEY = "S3_SECRET";
	public static final String S3_ENDPOINT_KEY = "S3_ENDPOINT";
	public static final String S3_PATH_STYLE_ACCESS_KEY = "S3_PATH_STYLE_ACCESS";

	private transient String accessKey;
	private transient String secretKey;
	private transient String region;
	private transient String bucket;
	private transient String endpoint;
	private boolean pathStyleAccess = false;

	private transient S3Client client = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.accessKey = smssProp.getProperty(S3_ACCESS_KEY);
		this.secretKey = smssProp.getProperty(S3_SECRET_KEY);
		this.region = smssProp.getProperty(S3_REGION_KEY);
		this.bucket = smssProp.getProperty(S3_BUCKET_KEY);
		this.endpoint = smssProp.getProperty(S3_ENDPOINT_KEY);
		String pathStyleAccessStr = smssProp.getProperty(S3_PATH_STYLE_ACCESS_KEY);
		if (pathStyleAccessStr != null && !pathStyleAccessStr.isEmpty()) {
			this.pathStyleAccess = Boolean.parseBoolean(pathStyleAccessStr);
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
		return StorageTypeEnum.AMAZON_S3_NATIVE;
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
		path = path == null ? "" : Utility.normalizePath(path);
		if (path.startsWith("/")) {
			path = path.substring(1);
		}
		if (path.endsWith("/")) {
			path = path.substring(0, path.length() - 1);
		}
		String prefix = path.isEmpty() ? "" : path + "/";

		try {
			ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder().bucket(this.bucket)
					.delimiter("/");
			if (!prefix.isEmpty()) {
				requestBuilder.prefix(prefix);
			}
			ListObjectsV2Response listObjectsV2Response = this.client.listObjectsV2(requestBuilder.build());

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
	public void syncLocalToStorage(String localPath, String storagePath, Map<String, Object> metadata)
			throws Exception {

		Path localFilePath = Paths.get(localPath);
		List<String> uploadedFiles = new ArrayList<>();
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

			try (Stream<Path> stream = Files.walk(localFilePath)) {
				stream.filter(Files::isRegularFile).forEach(file -> {
					try {
						uploadedFiles.add(uploadFileToS3(storagePath, file, localBasePath, metadata));
					} catch (Exception e) {
						failedFiles.add(file.toString());
						classLogger.error("Failed to upload file: {}", file, e);
					}
				});
			}
			found = true;
		} catch (Exception e) {
			classLogger.error("Sync operation failed.Rolling back failed uploads.", e);
			throw e;

		}

		if (uploadedFiles.isEmpty()) {
			classLogger.info("No files were uploaded.");
		} else {
			classLogger.info("Sucessfully uploaded: {}", uploadedFiles);
		}
		classLogger.info(found ? "Sync complted successfully for: {}" : "No files found to sync for: {}", storagePath);
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

		ListObjectsV2Response listObjectsResponse = s3ListObjectResponse(requestedPath);

		for (S3Object s3Object : listObjectsResponse.contents()) {
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

			try {
				boolean fileExists = Files.exists(localFilePath);
				boolean shouldDownload = !fileExists;

				if (fileExists) {
					FileTime localModifiedTime = Files.getLastModifiedTime(localFilePath);
					long localFileSize = Files.size(localFilePath);
					long cloudFileSize = s3Object.size();
					long cloudModifiedTime = s3Object.lastModified().toEpochMilli();
					shouldDownload = cloudFileSize != localFileSize || cloudModifiedTime > localModifiedTime.toMillis();
				}

				if (shouldDownload) {
					retryOperation(() -> {
						try {
							downloadedFile(this.bucket, key, localFilePath);
						} catch (IOException e) {
							classLogger.error("Failed to write file: {}", localFilePath, e);
							throw new RuntimeException("Error writing file: " + localFilePath, e);
						}
					}, "Syncing file to local: " + key);

					downloadedFiles.add(key);
					classLogger.info(fileExists ? "Updated file: {}" : "Downloaded new file: {}", localFilePath);
				}
				found = true;
			} catch (Exception e) {
				failedFiles.add(relativePath);
				classLogger.error("Failed to sync file: {}", key, e);
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
	public void copyToStorage(String localFilePath, String storageFolderPath, Map<String, Object> metadata)
			throws Exception {
		List<Path> paths = parseLocalPaths(localFilePath);
		List<String> uploadedFiles = new ArrayList<>();
		List<String> failedFiles = new ArrayList<>();
		boolean found = false;

		for (Path path : paths) {
			if (!Files.exists(path)) {
				classLogger.error("File not found: {}", path);
				failedFiles.add(path.toString());
				continue;
			}

			deleteEmptyDirectories(path);

			if (Files.isDirectory(path)) {
				try (Stream<Path> stream = Files.walk(path)) {
					stream.filter(Files::isRegularFile).forEach(file -> {
						try {
							uploadedFiles.add(uploadFile(path, file, storageFolderPath, metadata));
						} catch (Exception e) {
							failedFiles.add(file.toString());
							classLogger.error("Failed to upload file: {}", file, e);
							rollbackUploads(this.client, failedFiles);
						}
					});
					found = true;
				}
			} else {
				try {
					uploadedFiles.add(uploadFile(path.getParent(), path, storageFolderPath, metadata));
					found = true;
				} catch (Exception e) {
					failedFiles.add(path.toString());
					classLogger.error("Failed to upload file: {}", path, e);
					rollbackUploads(this.client, failedFiles);
				}
			}
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
	}

	@Override
	public String copyToStorageVersioned(String localFilePath, String storageFolderPath, Map<String, Object> metadata)
			throws Exception {
		List<Path> paths = parseLocalPaths(localFilePath);
		List<String> uploadedFiles = new ArrayList<>();
		List<String> failedFiles = new ArrayList<>();
		AtomicReference<String> lastVersionId = new AtomicReference<>(null);
		boolean found = false;

		for (Path path : paths) {
			if (!Files.exists(path)) {
				classLogger.error("File not found: {}", path);
				failedFiles.add(path.toString());
				continue;
			}

			deleteEmptyDirectories(path);

			if (Files.isDirectory(path)) {
				try (Stream<Path> stream = Files.walk(path)) {
					stream.filter(Files::isRegularFile).forEach(file -> {
						try {
							String versionId = uploadFileVersioned(path, file, storageFolderPath, metadata);
							uploadedFiles.add(file.toString());
							if (versionId != null) {
								lastVersionId.set(versionId);
							}
						} catch (Exception e) {
							failedFiles.add(file.toString());
							classLogger.error("Failed to upload file: {}", file, e);
							rollbackUploads(this.client, failedFiles);
						}
					});
					found = true;
				}
			} else {
				try {
					String versionId = uploadFileVersioned(path.getParent(), path, storageFolderPath, metadata);
					uploadedFiles.add(path.toString());
					if (versionId != null) {
						lastVersionId.set(versionId);
					}
					found = true;
				} catch (Exception e) {
					failedFiles.add(path.toString());
					classLogger.error("Failed to upload file: {}", path, e);
					rollbackUploads(this.client, failedFiles);
				}
			}
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
	public void copyToLocal(String storageFilePath, String localFolderPath) throws Exception {
		List<String> paths = parseStorageObjectPaths(storageFilePath);
		Path localDirectory = Paths.get(localFolderPath);

		// Ensure local directory exists
		Files.createDirectories(localDirectory);

		List<String> downloadedFiles = new ArrayList<>();
		List<String> failedFiles = new ArrayList<>();
		boolean found = false;

		for (String s3FolderPath : paths) {
			String requestedPath = normalizeStoragePrefixPath(s3FolderPath);

			// Delete empty folder blobs
			deleteEmptyBlobsFromS3(requestedPath);

			ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(this.bucket).prefix(requestedPath)
					.build();

			ListObjectsV2Response response;
			do {
				response = this.client.listObjectsV2(request);

				for (S3Object s3Object : response.contents()) {
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

					try {
						Files.createDirectories(localFilePath.getParent());

						retryOperation(() -> {
							try {
								downloadFile(key, localFilePath);
							} catch (Exception e) {
								classLogger.error("Failed to download: {}", key, e);
							}
						}, "Downloading file: " + key);

						downloadedFiles.add(key);
						classLogger.info("Downloaded file: {}", localFilePath);
						found = true;
					} catch (Exception e) {
						failedFiles.add(relativePath);
						classLogger.error("Failed to download: {}", key, e);
					}
				}

				request = request.toBuilder().continuationToken(response.nextContinuationToken()).build();

			} while (response.isTruncated());
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
	public void copyToLocal(String storageFilePath, String localFolderPath, String versionId) throws Exception {
		if (versionId != null && !versionId.isEmpty()) {
			String key = normalizeStoragePrefixPath(storageFilePath);
			Path localDirectory = Paths.get(localFolderPath);
			Files.createDirectories(localDirectory);

			String fileName = key.contains("/") ? key.substring(key.lastIndexOf("/") + 1) : key;
			Path localFilePath = localDirectory.resolve(fileName);

			GetObjectRequest getRequest = GetObjectRequest.builder()
					.bucket(this.bucket)
					.key(key)
					.versionId(versionId)
					.build();

			try (ResponseInputStream<GetObjectResponse> responseStream = this.client.getObject(getRequest)) {
				Files.copy(responseStream, localFilePath, StandardCopyOption.REPLACE_EXISTING);
				classLogger.info("Downloaded versioned file: {} (versionId={})", localFilePath, versionId);
			}
		} else {
			copyToLocal(storageFilePath, localFolderPath);
		}
	}

	@Override
	public void deleteFromStorage(String storagePath) throws Exception {
		storagePath = Utility.normalizePath(storagePath);

		// Remove leading and trailing slashes if present
		if (storagePath.startsWith("/")) {
			storagePath = storagePath.substring(1);
		}
		if (storagePath.endsWith("/")) {
			storagePath = storagePath.substring(0, storagePath.length() - 1);
		}

		List<String> deletedFiles = new ArrayList<>();
		List<String> failedFiles = new ArrayList<>();
		boolean hasFilesToDelete = false;
		ListObjectsV2Response responseListObjects = s3ListObjectResponse(storagePath);
		if (responseListObjects == null || responseListObjects.contents() == null
				|| responseListObjects.contents().isEmpty()) {
			classLogger.warn(storagePath.isEmpty() ? "No files found in bucket: {}" : "No files found in directory: {}",
					storagePath.isEmpty() ? this.bucket : storagePath);
			return;
		}
		for (S3Object object : responseListObjects.contents()) {
			String objectKey = object.key();

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
	}

	@Override
	public void deleteFromStorage(String storagePath, boolean leaveFolderStructure) throws Exception {
		storagePath = Utility.normalizePath(storagePath);

		// Remove leading and trailing slashes if present
		if (storagePath.startsWith("/")) {
			storagePath = storagePath.substring(1);
		}
		if (storagePath.endsWith("/")) {
			storagePath = storagePath.substring(0, storagePath.length() - 1);
		}
		List<String> deletedFiles = new ArrayList<>();
		List<String> failedFiles = new ArrayList<>();
		ListObjectsV2Response responseListObjects = s3ListObjectResponse(storagePath);
		List<S3Object> s3Objects = responseListObjects.contents();
		boolean hasFilesToDelete = false;

		for (S3Object s3Object : s3Objects) {
			hasFilesToDelete = true;
			String objectName = s3Object.key();

			if (deleteObject(objectName)) {
				deletedFiles.add(objectName);
			} else {
				failedFiles.add(objectName);
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

		storageFolderPath = Utility.normalizePath(storageFolderPath);

		// Remove leading and trailing slashes if present
		if (storageFolderPath.startsWith("/")) {
			storageFolderPath = storageFolderPath.substring(1);
		}
		if (storageFolderPath.endsWith("/")) {
			storageFolderPath = storageFolderPath.substring(0, storageFolderPath.length() - 1);
		}
		boolean folderExists = false;

		classLogger.info(storageFolderPath.isEmpty() ? "Folder path is empty. Deleting all files in bucket: {}"
				: "Deleting folder: {}", storageFolderPath.isEmpty() ? this.bucket : storageFolderPath);
		ListObjectsV2Response listObjectsV2Response = s3ListObjectResponse(storageFolderPath);
		for (S3Object object : listObjectsV2Response.contents()) {
			folderExists = true;
			String objectKey = object.key();
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

	private String uploadFileToS3(String storagePath, Path filePath, Path basePath, Map<String, Object> metadata)
			throws Exception {
		String relativePath = Utility.normalizePath(basePath.relativize(filePath).toString());
		String normalizedStoragePath = Utility.normalizePath(storagePath);
		String fileKey = normalizedStoragePath + (normalizedStoragePath.endsWith("/") ? "" : "/") + relativePath;

		try {
			HeadObjectRequest headRequest = HeadObjectRequest.builder().bucket(this.bucket).key(fileKey).build();

			HeadObjectResponse headResponse = this.client.headObject(headRequest);

			long cloudSize = headResponse.contentLength();
			long cloudLastModified = headResponse.lastModified().toEpochMilli();

			long localSize = Files.size(filePath);
			long localLastModified = Files.getLastModifiedTime(filePath).toMillis();

			if (localSize == cloudSize && localLastModified <= cloudLastModified) {
				classLogger.info("Skipping unchanged file: {}", fileKey);
				return null;
			}
		} catch (S3Exception e) {
			if (e.statusCode() != 404) {
				classLogger.error("Error checking S3 object: {}", fileKey, e);
				throw e;
			}
		}

		// Prepare metadata
		Map<String, String> metaMap = metadata != null
				? metadata.entrySet().stream()
						.collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()))
				: Collections.emptyMap();
		retryOperation(() -> {
			PutObjectRequest putRequest = PutObjectRequest.builder().bucket(this.bucket).key(fileKey).metadata(metaMap)
					.build();

			PutObjectResponse response = this.client.putObject(putRequest, filePath);
			classLogger.info("Uploaded/Updated file: {}", fileKey);
			if (this.versioningEnabled && response.versionId() != null) {
				classLogger.info("Version ID for {}: {}", fileKey, response.versionId());
			}
		}, "Uploading file to S3: " + fileKey);

		return fileKey;
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
			ListObjectsV2Response listResponse = s3ListObjectResponse(storagePath);
			for (S3Object s3Object : listResponse.contents()) {
				String objectKey = s3Object.key();
				String relativePath = objectKey.replaceFirst("^" + Pattern.quote(storagePath + "/"), "");
				Path localFile = localBasePath.resolve(relativePath).normalize();

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

	private void downloadedFile(String bucketName, String key, Path destinationPath) throws IOException {
		GetObjectRequest getRequest = GetObjectRequest.builder().bucket(bucketName).key(key).build();

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

		ListObjectsV2Response response = s3ListObjectResponse(storagePrefix);

		for (S3Object obj : response.contents()) {
			if (obj.size() == 0) {
				DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder().bucket(this.bucket).key(obj.key())
						.build();
				this.client.deleteObject(deleteRequest);
				classLogger.info("Deleted empty blob from S3: {}", obj.key());
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

	private String uploadFile(Path rootPath, Path file, String storageFolderPath, Map<String, Object> metadata)
			throws IOException {
		String normalizedPath = Utility.normalizePath(storageFolderPath).trim();
		if (normalizedPath.startsWith("/")) {
			normalizedPath = normalizedPath.substring(1);
		}

		String relativePath = Utility.normalizePath(rootPath.relativize(file).toString()).trim();
		String fileKey = normalizedPath.isEmpty() ? relativePath
				: (normalizedPath.endsWith("/") ? normalizedPath + relativePath : normalizedPath + "/" + relativePath);

		PutObjectRequest.Builder putBuilder = PutObjectRequest.builder().bucket(this.bucket).key(fileKey);

		if (metadata != null && !metadata.isEmpty()) {
			putBuilder.metadata(metadata.entrySet().stream()
					.collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString())));
		}

		retryOperation(() -> {
			this.client.putObject(putBuilder.build(), file);
			classLogger.info("Uploaded file to S3: {}", fileKey);
		}, "Uploading to S3: " + fileKey);

		return fileKey;
	}

	private String uploadFileVersioned(Path rootPath, Path file, String storageFolderPath,
			Map<String, Object> metadata) throws IOException {
		String normalizedPath = Utility.normalizePath(storageFolderPath).trim();
		if (normalizedPath.startsWith("/")) {
			normalizedPath = normalizedPath.substring(1);
		}

		String relativePath = Utility.normalizePath(rootPath.relativize(file).toString()).trim();
		String fileKey = normalizedPath.isEmpty() ? relativePath
				: (normalizedPath.endsWith("/") ? normalizedPath + relativePath : normalizedPath + "/" + relativePath);

		PutObjectRequest.Builder putBuilder = PutObjectRequest.builder().bucket(this.bucket).key(fileKey);

		if (metadata != null && !metadata.isEmpty()) {
			putBuilder.metadata(metadata.entrySet().stream()
					.collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString())));
		}

		AtomicReference<String> versionIdRef = new AtomicReference<>(null);
		retryOperation(() -> {
			PutObjectResponse response = this.client.putObject(putBuilder.build(), file);
			classLogger.info("Uploaded file to S3: {}", fileKey);
			if (response.versionId() != null) {
				versionIdRef.set(response.versionId());
				classLogger.info("Version ID for {}: {}", fileKey, response.versionId());
			}
		}, "Uploading to S3: " + fileKey);

		return versionIdRef.get();
	}

	private void downloadFile(String key, Path localFilePath) throws IOException {
		GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(this.bucket).key(key).build();

		try (InputStream s3InputStream = this.client.getObject(getObjectRequest)) {
			Files.copy(s3InputStream, localFilePath, StandardCopyOption.REPLACE_EXISTING);
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

	private ListObjectsV2Response s3ListObjectResponse(String storagePrefix) {
		ListObjectsV2Request listObjects = ListObjectsV2Request.builder().bucket(this.bucket).prefix(storagePrefix)
				.build();
		ListObjectsV2Response listObjectsResponse = this.client.listObjectsV2(listObjects);
		return listObjectsResponse;
	}

	@Override
	public void close() throws IOException {
		if (client != null) {
			client.close();
		}
	}

}
