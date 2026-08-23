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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbFile;
import prerna.engine.api.StorageTypeEnum;
import prerna.util.Constants;
import prerna.util.Utility;

public class JCIFSStorageEngine extends AbstractStorageEngine {

	private static final Logger classLogger = LogManager.getLogger(JCIFSStorageEngine.class);

	private static final String NETWORK_DOMAIN = "NETWORK_DOMAIN";
	private static final String PATH_PREFIX = "PATH_PREFIX";

	private transient String networkDomain = null;
	private transient String networkUsername = null;
	private transient String networkPassword = null;
	private transient NtlmPasswordAuthentication auth = null;

	private transient String pathPrefix = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.networkDomain = smssProp.getProperty(NETWORK_DOMAIN);
		this.networkUsername = smssProp.getProperty(Constants.USERNAME);
		this.networkPassword = smssProp.getProperty(Constants.PASSWORD);
		this.auth = new NtlmPasswordAuthentication(this.networkDomain, this.networkUsername, this.networkPassword);

		this.pathPrefix = smssProp.getProperty(PATH_PREFIX);
		if (this.pathPrefix == null) {
			this.pathPrefix = "";
		}
	}

	@Override
	public StorageTypeEnum getStorageType() {
		return StorageTypeEnum.SMB_CIFS;
	}

	@Override
	public List<String> list(String path) throws Exception {
		List<Map<String, Object>> details = listDetails(path);
		List<String> names = new ArrayList<>(details.size());
		for (Map<String, Object> item : details) {
			Object nameObj = item.get("Name");
			if (nameObj == null) {
				continue;
			}
			String name = nameObj.toString();
			boolean isDir = Boolean.TRUE.equals(item.get("IsDir"));
			names.add(isDir ? name + "/" : name);
		}
		return names;
	}

	@Override
	public List<Map<String, Object>> listDetails(String path) throws Exception {
		String relativeBasePath = normalizeStoragePrefixPath(path);
		SmbFile smbF = smbFile(relativeBasePath, true);
		SmbFile[] children = smbF.listFiles();
		if (children == null) {
			return Collections.emptyList();
		}

		List<Map<String, Object>> details = new ArrayList<>(children.length);
		for (SmbFile child : children) {
			String name = trimTrailingSlash(child.getName());
			if (name.isEmpty()) {
				continue;
			}

			boolean isDir = child.isDirectory();
			Map<String, Object> item = new HashMap<>();
			item.put("Path", relativeBasePath.isEmpty() ? "/" + name : "/" + relativeBasePath + "/" + name);
			item.put("Name", name);
			item.put("Size", isDir ? 0L : child.length());
			item.put("MimeType", isDir ? "inode/directory" : null);
			item.put("ModTime", Instant.ofEpochMilli(child.getLastModified()).toString());
			item.put("IsDir", isDir);
			item.put("Metadata", Collections.emptyMap());
			details.add(item);
		}

		return details;
	}

	@Override
	public StorageSyncStatus syncLocalToStorage(String localPath, String storagePath, Map<String, Object> metadata)
			throws Exception {
		if (metadata != null && !metadata.isEmpty()) {
			// there is nowhere to put it - smb has no user metadata, only file attributes
			classLogger.warn("SMB/CIFS has no user metadata, ignoring {} entries for: {}", metadata.size(),
					storagePath);
		}

		Path localFolder = Paths.get(localPath);
		if (!Files.isDirectory(localFolder)) {
			throw new IllegalArgumentException("Local path is not a directory: " + localPath);
		}
		String storageFolder = normalizeStoragePrefixPath(storagePath);

		// one listing of what is already there, so the comparison below is local
		Map<String, StoredObjectStat> stored = listStoredFiles(storageFolder);

		List<Path> localFiles;
		try (Stream<Path> stream = Files.walk(localFolder)) {
			localFiles = stream.filter(Files::isRegularFile).toList();
		}

		List<String> uploadedFiles = new ArrayList<>();
		List<String> skippedFiles = new ArrayList<>();
		List<String> failedFiles = new ArrayList<>();
		for (Path localFile : localFiles) {
			String relativePath = Utility.normalizePath(localFolder.relativize(localFile).toString()).trim()
					.replace('\\', '/');
			String targetPath = joinStoragePath(storageFolder, relativePath);
			try {
				if (!needsUpload(localFile, stored.get(relativePath))) {
					skippedFiles.add(targetPath);
					continue;
				}
				uploadFile(localFile, targetPath);
				uploadedFiles.add(targetPath);
			} catch (Exception e) {
				// one bad file should not abandon the rest of the folder
				classLogger.error("Failed to upload {} to {}", localFile, targetPath, e);
				failedFiles.add(targetPath);
			}
		}

		classLogger.info("Sync of {} to {} uploaded {}, skipped {}, failed {}", localPath, storagePath,
				uploadedFiles.size(), skippedFiles.size(), failedFiles.size());
		return StorageSyncStatus.of(storageFolder, uploadedFiles, skippedFiles, failedFiles);
	}

	@Override
	public void syncStorageToLocal(String storagePath, String localPath) throws Exception {
		String storageFolder = normalizeStoragePrefixPath(storagePath);
		SmbFile source = smbFile(storageFolder, true);
		if (!source.exists()) {
			throw new IllegalArgumentException("Storage path does not exist: " + storagePath);
		}

		Path localFolder = Paths.get(localPath);
		Files.createDirectories(localFolder);

		List<String> downloadedFiles = new ArrayList<>();
		// unlike the upload side this pulls everything down, matching how the other
		// engines behave when the local copy is treated as disposable
		downloadDirectoryContents(source, localFolder, downloadedFiles);

		classLogger.info("Sync of {} to {} downloaded {} files", storagePath, localPath, downloadedFiles.size());
	}

	/**
	 * Walks the share below the given folder and records the size and modified time
	 * of every file, keyed by its path relative to that folder.
	 *
	 * smb has no bulk listing, so this is one round trip per directory. Doing it
	 * once up front is still far cheaper than stat'ing each file again while
	 * uploading.
	 *
	 * @param storageFolder the already normalized folder to walk
	 * @return relative path to what is stored, empty when the folder is not there
	 * @throws Exception if the share cannot be reached
	 */
	private Map<String, StoredObjectStat> listStoredFiles(String storageFolder) throws Exception {
		Map<String, StoredObjectStat> stored = new HashMap<>();
		SmbFile folder = smbFile(storageFolder, true);
		if (!folder.exists()) {
			return stored;
		}
		collectStoredFiles(folder, "", stored);
		return stored;
	}

	private void collectStoredFiles(SmbFile directory, String relativePrefix, Map<String, StoredObjectStat> stored)
			throws Exception {
		SmbFile[] children = directory.listFiles();
		if (children == null) {
			return;
		}
		for (SmbFile child : children) {
			String name = trimTrailingSlash(child.getName());
			if (name.isEmpty()) {
				continue;
			}
			String relativePath = relativePrefix.isEmpty() ? name : relativePrefix + "/" + name;
			if (child.isDirectory()) {
				collectStoredFiles(child, relativePath, stored);
			} else {
				stored.put(relativePath, new StoredObjectStat(child.length(), child.getLastModified()));
			}
		}
	}

	/**
	 * Copies the contents of a remote directory into a local one. Unlike
	 * {@link #downloadDirectory(SmbFile, Path, List)} the remote directory's own
	 * name is not added to the local path, since a sync fills the target folder
	 * rather than nesting inside it.
	 *
	 * @param directory       the remote directory to read
	 * @param localDirectory  where its contents land
	 * @param downloadedFiles collects what was written, for logging
	 * @throws Exception if a read or write fails
	 */
	private void downloadDirectoryContents(SmbFile directory, Path localDirectory, List<String> downloadedFiles)
			throws Exception {
		Files.createDirectories(localDirectory);

		SmbFile[] children = directory.listFiles();
		if (children == null) {
			return;
		}
		for (SmbFile child : children) {
			String name = trimTrailingSlash(child.getName());
			if (name.isEmpty()) {
				continue;
			}
			if (child.isDirectory()) {
				downloadDirectoryContents(child, localDirectory.resolve(name), downloadedFiles);
			} else {
				downloadFile(child, localDirectory.resolve(name));
				downloadedFiles.add(child.getPath());
			}
		}
	}

	@Override
	public String copyToStorage(String localFilePath, String storageFolderPath, Map<String, Object> metadata)
			throws Exception {
		if (metadata != null && !metadata.isEmpty()) {
			// there is nowhere to put it - smb has no user metadata, only file attributes
			classLogger.warn("SMB/CIFS has no user metadata, ignoring {} entries for: {}", metadata.size(),
					storageFolderPath);
		}

		List<Path> localPaths = parseLocalPaths(localFilePath);
		String storageFolder = normalizeStoragePrefixPath(storageFolderPath);
		List<String> uploadedFiles = new ArrayList<>();

		SmbFile destinationFolder = smbFile(storageFolder, true);
		if (!destinationFolder.exists()) {
			destinationFolder.mkdirs();
		}

		for (Path localPath : localPaths) {
			if (!Files.exists(localPath)) {
				throw new IllegalArgumentException("Local path does not exist: " + localPath);
			}

			if (Files.isDirectory(localPath)) {
				List<Path> files;
				try (Stream<Path> stream = Files.walk(localPath)) {
					files = stream.filter(Files::isRegularFile).toList();
				}
				for (Path file : files) {
					String relativePath = Utility.normalizePath(localPath.relativize(file).toString()).trim();
					uploadedFiles.add(uploadFile(file, joinStoragePath(storageFolder, relativePath)));
				}
			} else {
				String fileName = localPath.getFileName().toString().trim();
				uploadedFiles.add(uploadFile(localPath, joinStoragePath(storageFolder, fileName)));
			}
		}

		if (uploadedFiles.isEmpty()) {
			classLogger.info("No files were uploaded to: {}", storageFolderPath);
		} else {
			classLogger.info("Successfully uploaded files: {}", uploadedFiles);
		}

		// smb has no object versioning, so there is no version id to hand back
		return null;
	}

	@Override
	public void copyToLocal(String storageFilePath, String localFolderPath, String versionId) throws Exception {
		if (versionId != null && !versionId.trim().isEmpty()) {
			throw new UnsupportedOperationException("Object versioning is not supported by SMB/CIFS");
		}

		Path localFolder = Paths.get(localFolderPath);
		Files.createDirectories(localFolder);

		List<String> downloadedFiles = new ArrayList<>();
		for (String storagePath : parseStorageObjectPaths(storageFilePath)) {
			SmbFile source = openExisting(storagePath);
			if (!source.exists()) {
				throw new IllegalArgumentException("Storage path does not exist: " + storagePath);
			}

			String name = trimTrailingSlash(source.getName());
			if (source.isDirectory()) {
				downloadDirectory(source, localFolder.resolve(name), downloadedFiles);
			} else {
				downloadFile(source, localFolder.resolve(name));
				downloadedFiles.add(storagePath);
			}
		}

		if (downloadedFiles.isEmpty()) {
			classLogger.info("No files were downloaded from: {}", storageFilePath);
		} else {
			classLogger.info("Successfully downloaded files: {}", downloadedFiles);
		}
	}

	@Override
	public void deleteFromStorage(String storagePath, boolean leaveFolderStructure) throws Exception {
		SmbFile target = openExisting(storagePath);
		if (!target.exists()) {
			classLogger.warn("Nothing to delete, path does not exist: {}", storagePath);
			return;
		}

		if (!target.isDirectory()) {
			target.delete();
			classLogger.info("Deleted file: {}", storagePath);
			return;
		}

		deleteChildren(target);
		if (leaveFolderStructure) {
			classLogger.info("Emptied folder but left the folder itself: {}", storagePath);
		} else {
			target.delete();
			classLogger.info("Deleted folder: {}", storagePath);
		}
	}

	@Override
	public void deleteFolderFromStorage(String storageFolderPath) throws Exception {
		SmbFile folder = smbFile(storageFolderPath, true);
		if (!folder.exists()) {
			classLogger.warn("Nothing to delete, folder does not exist: {}", storageFolderPath);
			return;
		}
		if (!folder.isDirectory()) {
			throw new IllegalArgumentException("Not a folder: " + storageFolderPath);
		}

		deleteChildren(folder);
		folder.delete();
		classLogger.info("Deleted folder: {}", storageFolderPath);
	}

	@Override
	public void close() throws IOException {
		// jcifs 1.x pools its transports statically, so there is no per engine
		// connection to release
	}

	/**
	 * Builds the smb url for a storage path by joining it to the configured
	 * PATH_PREFIX. The path is run through normalizeStoragePrefixPath so this
	 * engine treats whitespace and leading/trailing slashes the same way the others
	 * do.
	 *
	 * @param storagePath the caller supplied path, may be null or empty for the
	 *                    root of the share
	 * @param directory   true to add the trailing slash jcifs needs on directory
	 *                    urls, without which listing and mkdirs misbehave
	 * @return the full smb url
	 */
	private String smbUrl(String storagePath, boolean directory) {
		String normalized = normalizeStoragePrefixPath(storagePath);

		StringBuilder url = new StringBuilder(this.pathPrefix);
		if (!normalized.isEmpty()) {
			// do not rely on PATH_PREFIX ending in a slash, or the first path segment
			// gets glued onto the share name
			if (url.length() > 0 && url.charAt(url.length() - 1) != '/') {
				url.append('/');
			}
			url.append(normalized);
		}
		if (directory && (url.length() == 0 || url.charAt(url.length() - 1) != '/')) {
			url.append('/');
		}
		return url.toString();
	}

	private SmbFile smbFile(String storagePath, boolean directory) throws Exception {
		return new SmbFile(smbUrl(storagePath, directory), this.auth);
	}

	/**
	 * Opens a path without knowing up front whether it is a file or a directory.
	 * Directories need the trailing slash, so if the plain url turns out to be one
	 * it gets reopened with it.
	 *
	 * @param storagePath the path to open
	 * @return the file, which may not exist
	 * @throws Exception if the share cannot be reached
	 */
	private SmbFile openExisting(String storagePath) throws Exception {
		SmbFile file = smbFile(storagePath, false);
		if (file.exists() && file.isDirectory()) {
			return smbFile(storagePath, true);
		}
		return file;
	}

	/**
	 * Writes one local file to the share, creating parent directories as needed.
	 *
	 * @param localFile   the file to upload
	 * @param storagePath where to write it
	 * @return the storage path written, for logging
	 * @throws Exception if the write fails
	 */
	private String uploadFile(Path localFile, String storagePath) throws Exception {
		String parentPath = parentOf(storagePath);
		if (!parentPath.isEmpty()) {
			SmbFile parent = smbFile(parentPath, true);
			if (!parent.exists()) {
				parent.mkdirs();
			}
		}

		SmbFile target = smbFile(storagePath, false);
		try (InputStream input = Files.newInputStream(localFile); OutputStream output = target.getOutputStream()) {
			input.transferTo(output);
		}
		// keep the local timestamp so a later sync can compare them
		target.setLastModified(Files.getLastModifiedTime(localFile).toMillis());

		classLogger.info("Uploaded file: {}", storagePath);
		return storagePath;
	}

	private void downloadDirectory(SmbFile directory, Path localDirectory, List<String> downloadedFiles)
			throws Exception {
		Files.createDirectories(localDirectory);

		SmbFile[] children = directory.listFiles();
		if (children == null) {
			return;
		}
		for (SmbFile child : children) {
			String name = trimTrailingSlash(child.getName());
			if (name.isEmpty()) {
				continue;
			}
			if (child.isDirectory()) {
				downloadDirectory(child, localDirectory.resolve(name), downloadedFiles);
			} else {
				downloadFile(child, localDirectory.resolve(name));
				downloadedFiles.add(child.getPath());
			}
		}
	}

	private void downloadFile(SmbFile source, Path destination) throws Exception {
		Path parent = destination.getParent();
		if (parent != null) {
			Files.createDirectories(parent);
		}

		try (InputStream input = source.getInputStream()) {
			Files.copy(input, destination, StandardCopyOption.REPLACE_EXISTING);
		}
		classLogger.info("Downloaded file: {}", destination);
	}

	/**
	 * Empties a directory. jcifs delete() is documented to recurse, but doing it
	 * here keeps the behavior explicit and the logging per file.
	 *
	 * @param directory the directory to empty, it is left in place
	 * @throws Exception if a delete fails
	 */
	private void deleteChildren(SmbFile directory) throws Exception {
		SmbFile[] children = directory.listFiles();
		if (children == null) {
			return;
		}
		for (SmbFile child : children) {
			if (child.isDirectory()) {
				deleteChildren(child);
			}
			child.delete();
			classLogger.info("Deleted: {}", child.getPath());
		}
	}

	private String joinStoragePath(String storageFolder, String relativePath) {
		return storageFolder.isEmpty() ? relativePath : storageFolder + "/" + relativePath;
	}

	private String parentOf(String storagePath) {
		int lastSlash = storagePath.lastIndexOf('/');
		return lastSlash < 0 ? "" : storagePath.substring(0, lastSlash);
	}

	private String trimTrailingSlash(String value) {
		String trimmed = value == null ? "" : value;
		while (trimmed.endsWith("/")) {
			trimmed = trimmed.substring(0, trimmed.length() - 1);
		}
		return trimmed;
	}
}
