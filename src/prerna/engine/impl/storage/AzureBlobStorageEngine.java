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
import java.net.URI;
import java.net.URISyntaxException;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobContainerItem;
import com.azure.storage.blob.models.BlobContainerItemProperties;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.models.BlobListDetails;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.blob.options.BlobUploadFromFileOptions;

import prerna.engine.api.StorageTypeEnum;
import prerna.util.Utility;

public class AzureBlobStorageEngine extends AbstractStorageEngine {

	private static final Logger classLogger = LogManager.getLogger(AzureBlobStorageEngine.class);

	public static final String AZ_CONN_STRING = "AZ_CONN_STRING";
	// alternatives to a connection string, for accounts that do not hand one out
	public static final String AZ_ACCOUNT_NAME = "AZ_ACCOUNT_NAME";
	public static final String AZ_SAS_URL = "SAS_URL";
	public static final String AZ_USE_MSI = "AZ_USE_MSI";

	// block size and concurrency match rclone's azureblob defaults
	// (--azureblob-chunk-size 4Mi, --azureblob-upload-concurrency 16). rclone has
	//
	// no equivalent of a single upload cutoff, so that stays at the Azure sdk's own
	// default of 256Mi - meaning blocks only come into play for genuinely large
	// files, and the concurrency below is not multiplied across a folder of small
	// ones
	private static final long UPLOAD_BLOCK_SIZE_BYTES = 4L * 1024 * 1024;
	private static final long SINGLE_UPLOAD_SIZE_BYTES = 256L * 1024 * 1024;
	private static final int UPLOAD_BLOCK_CONCURRENCY = 16;

	private transient String connectionString = null;
	private transient String accountName = null;
	private transient String sasUrl = null;
	private transient boolean useMsi = false;
	private transient BlobServiceClient blobServiceClient;

	// set only when the SAS url names a container, which pins this engine to that
	// one container instead of the whole account. See parseSasUrl
	private String sasContainerName = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		migrateLegacyProperties(smssProp);

		this.connectionString = smssProp.getProperty(AZ_CONN_STRING);
		this.accountName = smssProp.getProperty(AZ_ACCOUNT_NAME);
		this.sasUrl = smssProp.getProperty(AZ_SAS_URL);
		this.useMsi = Boolean.parseBoolean(smssProp.getProperty(AZ_USE_MSI, "false"));

		boolean hasConnectionString = this.connectionString != null && !this.connectionString.trim().isEmpty();
		boolean hasSasUrl = this.sasUrl != null && !this.sasUrl.trim().isEmpty();
		boolean hasAccountName = this.accountName != null && !this.accountName.trim().isEmpty();
		if (!hasConnectionString && !hasSasUrl && !(this.useMsi && hasAccountName)) {
			classLogger.error("Azure Blob engine has no usable credentials, cannot initialize the client.");
			throw new IllegalStateException("Set one of " + AZ_CONN_STRING + ", " + AZ_SAS_URL + ", or " + AZ_USE_MSI
					+ " with " + AZ_ACCOUNT_NAME + ".");
		}
		createServiceClient();
	}

	/**
	 * Builds a connection string out of the older account name and primary key when
	 * no connection string was given, so an smss written for the previous
	 * implementation keeps working.
	 *
	 * Account name plus key is the only pair that has to be converted. Managed
	 * identity and SAS url are read directly by createServiceClient, so they need
	 * nothing here.
	 *
	 * @param smssProp the properties being opened, updated in place
	 */
	protected void migrateLegacyProperties(Properties smssProp) {
		String current = smssProp.getProperty(AZ_CONN_STRING);
		if (current != null && !current.trim().isEmpty()) {
			return;
		}

		String legacyAccountName = smssProp.getProperty(AZ_ACCOUNT_NAME);
		String primaryKey = smssProp.getProperty(RCloneAzureBlobStorageEngine.AZ_PRIMARY_KEY);
		if (legacyAccountName != null && !legacyAccountName.trim().isEmpty() && primaryKey != null
				&& !primaryKey.trim().isEmpty()) {
			smssProp.put(AZ_CONN_STRING, "DefaultEndpointsProtocol=https;AccountName=" + legacyAccountName.trim()
					+ ";AccountKey=" + primaryKey.trim() + ";EndpointSuffix=core.windows.net");
			classLogger.warn(
					"Azure Blob engine is still configured with {} and {}. Building a connection string from them for "
							+ "now, but the smss should be updated to set {}.",
					AZ_ACCOUNT_NAME, RCloneAzureBlobStorageEngine.AZ_PRIMARY_KEY, AZ_CONN_STRING);
		}
	}

	/**
	 * Builds the service client from whichever credential the smss supplies.
	 *
	 * A connection string wins when present since it is the most specific. A SAS
	 * url carries its own signature, so it needs no separate credential. Managed
	 * identity needs the account name to know which endpoint to talk to, because
	 * unlike the other two it contains no address.
	 */
	public void createServiceClient() {
		BlobServiceClientBuilder builder = new BlobServiceClientBuilder();

		if (this.connectionString != null && !this.connectionString.trim().isEmpty()) {
			builder.connectionString(this.connectionString);
			classLogger.info("Azure Blob client using a connection string.");
		} else if (this.sasUrl != null && !this.sasUrl.trim().isEmpty()) {
			parseSasUrl(builder);
		} else {
			builder.endpoint("https://" + this.accountName.trim() + ".blob.core.windows.net")
					.credential(new DefaultAzureCredentialBuilder().build());
			classLogger.info("Azure Blob client using managed identity for account: {}", this.accountName);
		}

		this.blobServiceClient = builder.buildClient();
		classLogger.info("Azure Blob Service client created successfully.");
	}

	/**
	 * Points the builder at the account the SAS url belongs to and works out
	 * whether that SAS covers the whole account or a single container.
	 *
	 * The url path is what says which. An account level SAS is issued against the
	 * account itself and has no path
	 * (<code>https://acct.blob.core.windows.net/?sv=...&amp;ss=b&amp;srt=sco&amp;sig=...</code>),
	 * so the account stays the root of this engine and the first segment of every
	 * path is the container, the same as a connection string. A service SAS is
	 * issued against one container and names it in the path
	 * (<code>https://acct.blob.core.windows.net/mycontainer?sv=...&amp;sr=c&amp;sig=...</code>).
	 * That container becomes the root: listing "/" shows what is inside it rather
	 * than the containers in the account, which the token could not read anyway,
	 * and paths are relative to it rather than repeating its name.
	 *
	 * The endpoint handed to the builder is always the bare account, never the
	 * container. A service client built on a container url would glue the container
	 * name on twice as soon as it made a container client.
	 *
	 * @param builder the builder being configured
	 */
	private void parseSasUrl(BlobServiceClientBuilder builder) {
		String trimmedUrl = this.sasUrl.trim();
		URI uri;
		try {
			uri = new URI(trimmedUrl);
		} catch (URISyntaxException e) {
			throw new IllegalArgumentException(AZ_SAS_URL + " is not a valid url", e);
		}

		String token = uri.getRawQuery();
		if (token == null || token.isEmpty()) {
			throw new IllegalArgumentException(
					AZ_SAS_URL + " has no token on it. Expected something like " + "https://<account>.blob.core."
							+ "windows.net/<container>?sv=...&sig=..., copied whole including the query string.");
		}

		String path = uri.getPath() == null ? "" : uri.getPath();
		while (path.startsWith("/")) {
			path = path.substring(1);
		}
		while (path.endsWith("/")) {
			path = path.substring(0, path.length() - 1);
		}
		if (!path.isEmpty()) {
			// a blob level SAS carries container/blob, so only the first segment is
			// the container
			int slashIndex = path.indexOf('/');
			this.sasContainerName = slashIndex < 0 ? path : path.substring(0, slashIndex);
		}

		try {
			builder.endpoint(new URI(uri.getScheme(), uri.getAuthority(), null, null, null).toString());
		} catch (URISyntaxException e) {
			throw new IllegalArgumentException("Could not read the account out of " + AZ_SAS_URL, e);
		}
		builder.sasToken(token);

		if (this.sasContainerName == null) {
			classLogger.info("Azure Blob client using an account level SAS url, the root lists containers.");
		} else {
			classLogger.info("Azure Blob client using a SAS url scoped to container: {}. The root of this engine is "
					+ "that container.", this.sasContainerName);
		}
	}

	/**
	 * @return true when the credential only reaches one container, so that
	 *         container is the root of this engine rather than the account
	 */
	private boolean isContainerScoped() {
		return this.sasContainerName != null;
	}

	/**
	 * Container client for a path being written to, creating the container when it
	 * is not there yet.
	 *
	 * Writing is the only place this happens. A read or a delete against a missing
	 * container is a caller mistake worth surfacing, but a write cannot land
	 * without one, and callers that lay out a container per engine have no earlier
	 * point to create it. This is also what the rclone azureblob backend does on
	 * copy, so it keeps that behavior.
	 *
	 * @param containerName the container being written to
	 * @return the client, with the container guaranteed to exist
	 */
	private BlobContainerClient writableContainerClient(String containerName) {
		BlobContainerClient containerClient = this.blobServiceClient.getBlobContainerClient(containerName);
		if (containerClient.createIfNotExists()) {
			classLogger.info("Created container: {}", containerName);
		}
		return containerClient;
	}

	@Override
	public StorageTypeEnum getStorageType() {
		return StorageTypeEnum.MICROSOFT_AZURE_BLOB_STORAGE;
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

		// a connection string points at an account, not a container, so the root of
		// this engine is the account and the containers in it are its folders. Every
		// path below the root carries its container as the first segment. A SAS url
		// scoped to one container is the exception - there the root is that container,
		// and listing containers is not something the token is allowed to do
		if (normalizeStoragePrefixPath(storagePath).isEmpty() && !isContainerScoped()) {
			try {
				return listContainers();
			} catch (BlobStorageException e) {
				if (e.getStatusCode() == 403) {
					// an account SAS still has to have been issued with the service
					// resource type and the list permission to do this
					throw new IllegalArgumentException("This engine's credential is not allowed to list the containers "
							+ "in the account. Name a container in the path, for example mycontainer/myfolder, or "
							+ "reissue the SAS with the service resource type and list permission.", e);
				}
				throw e;
			}
		}

		String[] containerAndPath = extractContainerAndPath(storagePath);
		String containerName = containerAndPath[0];
		String blobDirectory = normalizeStoragePrefixPath(containerAndPath[1]);
		String prefix = blobDirectory.isEmpty() ? "" : blobDirectory + "/";
		// what the caller asked for, which is what the paths handed back have to be
		// relative to. On an account scoped engine this still carries the container,
		// and dropping it would hand back paths that cannot be listed again
		String requestedPath = normalizeStoragePrefixPath(storagePath);

		BlobContainerClient containerClient = this.blobServiceClient.getBlobContainerClient(containerName);
		// ask for the metadata up front. Otherwise every file needs its own
		// getProperties call just to fill in this listing
		ListBlobsOptions listBlobsOptions = new ListBlobsOptions()
				.setDetails(new BlobListDetails().setRetrieveMetadata(true))
				.setPrefix(prefix.isEmpty() ? null : prefix);
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

			boolean isDir = Boolean.TRUE.equals(blobItem.isPrefix());
			Map<String, Object> blobMap = new HashMap<>();
			blobMap.put("Path", requestedPath.isEmpty() ? "/" + name : "/" + requestedPath + "/" + name);
			blobMap.put("Name", name);
			blobMap.put("IsDir", isDir);

			if (isDir) {
				blobMap.put("Size", 0L);
				blobMap.put("MimeType", "inode/directory");
				blobMap.put("ModTime", null);
				blobMap.put("Metadata", Collections.emptyMap());
			} else {
				BlobItemProperties properties = blobItem.getProperties();
				Map<String, String> metadata = blobItem.getMetadata();
				blobMap.put("Size", properties == null ? 0L : properties.getContentLength());
				blobMap.put("MimeType", properties == null ? null : properties.getContentType());
				blobMap.put("ModTime", properties == null || properties.getLastModified() == null ? null
						: properties.getLastModified().toString());
				blobMap.put("Metadata", (metadata == null || metadata.isEmpty()) ? Collections.emptyMap() : metadata);
			}
			detailsList.add(blobMap);
		}
		return detailsList;
	}

	@Override
	public StorageSyncStatus syncLocalToStorage(String localPath, String storagePath, Map<String, Object> metadata)
			throws Exception {
		// Extract container and blob directory
		String[] containerAndPath = extractContainerAndPath(storagePath);
		String containerName = containerAndPath[0];
		String blobDirectory = containerAndPath[1];

		BlobContainerClient containerClient = writableContainerClient(containerName);
		Path localFilePath = Paths.get(localPath);

		List<String> uploadedFiles = new ArrayList<>();
		List<String> skippedFiles = new ArrayList<>();
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

			// one listing up front instead of asking about each blob individually
			Map<String, StoredObjectStat> alreadyStored = listStoredBlobs(containerClient, blobDirectory);

			List<Path> localFiles;
			try (Stream<Path> stream = Files.walk(localFilePath)) {
				localFiles = stream.filter(Files::isRegularFile).toList();
			}

			List<Callable<TransferOutcome>> transfers = new ArrayList<>(localFiles.size());
			for (Path file : localFiles) {
				String blobName = buildBlobName(blobDirectory, file, localFilePath);
				if (!needsUpload(file, alreadyStored.get(blobName))) {
					classLogger.info("Skipping file (No changes detected): {}", blobName);
					skippedFiles.add(blobName);
					continue;
				}
				transfers.add(() -> {
					try {
						uploadBlob(containerClient, blobName, file, metadata);
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
			rollbackUploads(containerClient, failedFiles);
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

		// once, not once per blob. This used to sit inside the loop below, where it
		// listed the whole container again for every single blob
		deleteEmptyBlobs(containerClient, blobDirectory);

		List<Callable<TransferOutcome>> transfers = new ArrayList<>();
		for (BlobItem blobItem : containerClient
				.listBlobs(new ListBlobsOptions().setPrefix(blobDirectory.isEmpty() ? null : blobDirectory), null)) {
			String blobName = blobItem.getName();
			// the listing already carries size and last modified, so there is no need to
			// fetch each blob's properties separately
			BlobItemProperties properties = blobItem.getProperties();

			String relativePath = resolveRelativeStoragePath(blobName, blobDirectory);
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
			if (fileExists && properties != null && properties.getContentLength() != null
					&& properties.getLastModified() != null) {
				FileTime localModifiedTime = Files.getLastModifiedTime(localFilePath);
				if (properties.getContentLength() == Files.size(localFilePath)
						&& properties.getLastModified().toInstant().toEpochMilli() <= localModifiedTime.toMillis()) {
					continue;
				}
			}

			BlobClient blobClient = containerClient.getBlobClient(blobName);
			transfers.add(() -> {
				try {
					retryOperation(() -> blobClient.downloadToFile(localFilePath.toString(), true),
							"Syncing file to local: " + blobName);
					classLogger.info(fileExists ? "Updated file: {}" : "Downloaded new file: {}", localFilePath);
					return new TransferOutcome(blobName, null);
				} catch (Exception e) {
					classLogger.error("Failed to sync file: {}", blobName, e);
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
		// Delete local files not present in Azure
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
		// Extract container and blob directory
		String[] containerAndPath = extractContainerAndPath(storageFolderPath);
		String containerName = containerAndPath[0];
		String blobDirectory = containerAndPath[1];

		List<String> uploadedFiles = new ArrayList<>();
		List<String> failedFiles = new ArrayList<>();
		boolean found = false;
		BlobContainerClient containerClient = writableContainerClient(containerName);
		List<Path> paths = parseLocalPaths(localFilePath);

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
						filesToUpload.put(file, buildBlobName(blobDirectory, file, filePath));
					}
				}
				found = true;
			} else {
				filesToUpload.put(filePath, buildBlobName(blobDirectory, filePath, filePath.getParent()));
				found = true;
			}
		}

		List<Callable<TransferOutcome>> transfers = new ArrayList<>(filesToUpload.size());
		for (Map.Entry<Path, String> entry : filesToUpload.entrySet()) {
			Path file = entry.getKey();
			String blobName = entry.getValue();
			transfers.add(() -> {
				try {
					uploadBlob(containerClient, blobName, file, metadata);
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
			rollbackUploads(containerClient, failedFiles);
		}
		// Delete empty folder from azure storage (zero-byte blob)
		deleteEmptyBlobs(containerClient, blobDirectory);
		if (uploadedFiles.isEmpty()) {
			classLogger.info("No files were uploaded.");
		} else {
			classLogger.info("Successfully uploaded files: {}", uploadedFiles);
		}
		classLogger.info(found ? "Copy completed successfully for: {}" : "No files found to copy for: {}",
				storageFolderPath);
		return null;
	}

	@Override
	public void copyToLocal(String storageFilePath, String localFolderPath, String versionId) throws Exception {
		// TODO: account for versionId

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
		// once, not once per blob. This used to sit inside the loop below, where it
		// listed the whole container again for every single blob
		deleteEmptyBlobs(containerClient, blobDirectory);

		List<Callable<TransferOutcome>> transfers = new ArrayList<>();
		for (String path : paths) {
			String requestedPath = normalizeStoragePrefixPath(path);
			Iterable<BlobItem> getBlobItems = requestedPath.isEmpty() ? containerClient.listBlobs()
					: containerClient.listBlobs(new ListBlobsOptions().setPrefix(requestedPath), null);

			for (BlobItem blobItem : getBlobItems) {
				String blobName = blobItem.getName();
				BlobClient blobClient = containerClient.getBlobClient(blobName);

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
						retryOperation(() -> blobClient.downloadToFile(localFilePath.toString(), true),
								"Downloading file: " + blobName);
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
			// compare against blobDirectory, not storagePath - storagePath still carries
			// the container name, which blob names never do, so nothing would match
			if (blobDirectory.isEmpty() || blobItem.getName().equals(blobDirectory)
					|| blobItem.getName().startsWith(blobDirectory + "/")) {

				hasFilesToDelete = true;
				String blobName = blobItem.getName();
				if (deleteBlob(containerClient, blobName)) {
					deletedFiles.add(blobName);
				} else {
					failedFiles.add(blobName);
				}
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
		String blobDirectory = normalizeStoragePrefixPath(containerAndPath[1]);

		List<String> deletedFiles = new ArrayList<>();
		List<String> failedFiles = new ArrayList<>();
		BlobContainerClient containerClient = this.blobServiceClient.getBlobContainerClient(containerName);

		boolean folderExists = false;

		classLogger.info(blobDirectory.isEmpty() ? "Blob directory is empty. Deleting all files in container: {}"
				: "Deleting folder: {}", blobDirectory.isEmpty() ? containerName : blobDirectory);

		// the trailing slash bounds the listing to this folder. A bare prefix of "dir"
		// also returns "dirty/..." and this method deletes everything it lists. An
		// empty path is still the whole container by design
		String prefix = blobDirectory.isEmpty() ? "" : blobDirectory + "/";

		Iterable<BlobItem> blobItems = prefix.isEmpty() ? containerClient.listBlobs()
				: containerClient.listBlobs(new ListBlobsOptions().setPrefix(prefix), null);

		for (BlobItem blobItem : blobItems) {
			String blobName = blobItem.getName();

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

	/**
	 * Writes one local file to a blob, always. Whether it needed writing is decided
	 * by the caller from a listing, not by asking the service about this one blob.
	 *
	 * Metadata rides along with the upload rather than being applied in a second
	 * call, and larger files are split into blocks that go out concurrently.
	 *
	 * @param containerClient the container being written to
	 * @param blobName        the blob to write
	 * @param file            the local file
	 * @param metadata        user metadata to attach, may be null
	 * @return the blob name written, for logging
	 */
	private String uploadBlob(BlobContainerClient containerClient, String blobName, Path file,
			Map<String, Object> metadata) {
		BlobClient blobClient = containerClient.getBlobClient(blobName);

		ParallelTransferOptions transferOptions = new ParallelTransferOptions()
				.setBlockSizeLong(UPLOAD_BLOCK_SIZE_BYTES).setMaxSingleUploadSizeLong(SINGLE_UPLOAD_SIZE_BYTES)
				.setMaxConcurrency(UPLOAD_BLOCK_CONCURRENCY);

		BlobUploadFromFileOptions uploadOptions = new BlobUploadFromFileOptions(file.toString())
				.setParallelTransferOptions(transferOptions);
		Map<String, String> flatMetadata = flattenMetadata(metadata);
		if (!flatMetadata.isEmpty()) {
			// attaching it here saves the extra setMetadata round trip per file
			uploadOptions.setMetadata(flatMetadata);
		}

		retryOperation(() -> {
			blobClient.uploadFromFileWithResponse(uploadOptions, null, null);
			classLogger.info("Uploaded file: {}", blobName);
		}, "Uploading file: " + blobName);

		return blobName;
	}

	/**
	 * Builds the blob name for a local file under a blob directory.
	 *
	 * @param blobDirectory the folder inside the container, may be empty
	 * @param file          the local file
	 * @param basePath      the local folder the name is relative to
	 * @return the blob name to write
	 */
	private String buildBlobName(String blobDirectory, Path file, Path basePath) {
		String relativePath = Utility.normalizePath(basePath.relativize(file).toString()).trim();
		return blobDirectory.isEmpty() ? relativePath : Utility.normalizePath(blobDirectory + "/" + relativePath);
	}

	/**
	 * Snapshots what the container already holds under a prefix, so a sync can
	 * compare in memory instead of asking about each blob individually.
	 *
	 * @param containerClient the container to list
	 * @param blobDirectory   the folder inside it, may be empty for the whole
	 *                        container
	 * @return blob name to size and last modified
	 */
	private Map<String, StoredObjectStat> listStoredBlobs(BlobContainerClient containerClient, String blobDirectory) {
		String normalizedDirectory = normalizeStoragePrefixPath(blobDirectory);
		String prefix = normalizedDirectory.isEmpty() ? null : normalizedDirectory + "/";

		Map<String, StoredObjectStat> stored = new HashMap<>();
		try {
			for (BlobItem blobItem : containerClient.listBlobs(new ListBlobsOptions().setPrefix(prefix), null)) {
				BlobItemProperties properties = blobItem.getProperties();
				if (properties == null || properties.getContentLength() == null
						|| properties.getLastModified() == null) {
					continue;
				}
				stored.put(blobItem.getName(), new StoredObjectStat(properties.getContentLength(),
						properties.getLastModified().toInstant().toEpochMilli()));
			}
		} catch (BlobStorageException e) {
			// losing the listing only costs the skip optimization, so upload everything
			classLogger.warn("Unable to list {} before syncing, every file will be uploaded", blobDirectory, e);
			return Collections.emptyMap();
		}
		return stored;
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

	/**
	 * Cleans up the zero byte placeholders under a folder.
	 *
	 * @param containerClient the container being worked in
	 * @param storagePath     the folder to bound the cleanup to, empty for the
	 *                        whole container
	 */
	private void deleteEmptyBlobs(BlobContainerClient containerClient, String storagePath) {
		// bound to this folder. Listing the whole container cleaned up placeholders
		// belonging to folders this call never touched, and a bare prefix of "dir"
		// would also match "dirty/..."
		String normalizedPrefix = normalizeStoragePrefixPath(storagePath);
		String prefix = normalizedPrefix.isEmpty() ? "" : normalizedPrefix + "/";

		Iterable<BlobItem> blobItems = prefix.isEmpty() ? containerClient.listBlobs()
				: containerClient.listBlobs(new ListBlobsOptions().setPrefix(prefix), null);
		for (BlobItem blobItem : blobItems) {
			// the size is already on the listing, no need for a getProperties round trip
			// per blob
			Long contentLength = blobItem.getProperties() == null ? null : blobItem.getProperties().getContentLength();
			if (contentLength != null && isFolderPlaceholder(blobItem.getName(), contentLength)) {
				containerClient.getBlobClient(blobItem.getName()).delete();
				classLogger.info("Deleted folder placeholder: {}", blobItem.getName());
			}
		}
	}

	private void syncStorageDeletion(BlobContainerClient containerClient, String blobDirectory, Path localBasePath) {
		// normalize the same way uploadBlob does, otherwise we list a prefix that does
		// not match the blobs we write
		String normalizedDirectory = normalizeStoragePrefixPath(blobDirectory);
		// the trailing slash bounds the listing to this folder. A bare prefix of "dir"
		// also returns "dirty/..." and none of those resolve to a local file, so every
		// one of them would look stale and get deleted
		String prefix = normalizedDirectory.isEmpty() ? "" : normalizedDirectory + "/";

		Iterable<BlobItem> blobItems = prefix.isEmpty() ? containerClient.listBlobs()
				: containerClient.listBlobs(new ListBlobsOptions().setPrefix(prefix), null);
		for (BlobItem blobItem : blobItems) {
			String blobName = blobItem.getName();
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
					containerClient.getBlobClient(blobName).delete();
					classLogger.info("Deleted storage file not found in local: {}", blobName);
				}
			} catch (Exception e) {
				classLogger.error("Failed to delete blob: {}", blobName, e);
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

	/**
	 * Lists the containers in the account as directory entries, so browsing the
	 * root of this engine shows what is available to descend into.
	 *
	 * Containers are the top level of an Azure account. They are reported the same
	 * way a virtual folder inside a container is, so a caller walking the tree does
	 * not need to treat the first level specially.
	 *
	 * @return one entry per container, all marked as directories
	 */
	private List<Map<String, Object>> listContainers() {
		List<Map<String, Object>> detailsList = new ArrayList<>();
		for (BlobContainerItem container : this.blobServiceClient.listBlobContainers()) {
			String name = container.getName();
			if (name == null || name.isEmpty()) {
				continue;
			}

			BlobContainerItemProperties properties = container.getProperties();
			Map<String, Object> containerMap = new HashMap<>();
			containerMap.put("Path", "/" + name);
			containerMap.put("Name", name);
			containerMap.put("IsDir", true);
			containerMap.put("Size", 0L);
			containerMap.put("MimeType", "inode/directory");
			containerMap.put("ModTime", properties == null || properties.getLastModified() == null ? null
					: properties.getLastModified().toString());
			// metadata hangs off the item itself, not off its properties
			Map<String, String> metadata = container.getMetadata();
			containerMap.put("Metadata", metadata == null || metadata.isEmpty() ? Collections.emptyMap() : metadata);
			detailsList.add(containerMap);
		}
		return detailsList;
	}

	private String[] extractContainerAndPath(String storagePath) {
		if (storagePath == null || storagePath.trim().isEmpty()) {
			if (isContainerScoped()) {
				// the root of a container scoped engine is the container itself, which
				// is a perfectly good thing to name
				return new String[] { this.sasContainerName, "" };
			}
			throw new IllegalArgumentException("Storage path cannot be null or empty.");
		}

		// Use the utility method for normalization
		String normalizedPath = Utility.normalizePath(storagePath).trim();

		// Remove leading slash if present
		if (normalizedPath.startsWith("/")) {
			normalizedPath = normalizedPath.substring(1);
		}

		if (isContainerScoped()) {
			// the container is fixed by the credential, so the path never carries it -
			// everything the caller passes is a blob path inside that container
			return new String[] { this.sasContainerName, normalizedPath };
		}

		if (normalizedPath.isEmpty()) {
			// the account root is only meaningful for listing, where it enumerates the
			// containers. Reading or writing needs one of them named
			throw new IllegalArgumentException("Storage path '" + storagePath
					+ "' does not name a container. Azure paths start with the container, for example "
					+ "mycontainer/myfolder. List the root of this engine to see the containers available.");
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
