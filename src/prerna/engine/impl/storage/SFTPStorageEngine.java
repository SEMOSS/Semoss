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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.FileAttributes;
import net.schmizz.sshj.sftp.FileMode;
import net.schmizz.sshj.sftp.FileMode.Type;
import net.schmizz.sshj.sftp.RemoteResourceInfo;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.xfer.FileSystemFile;
import net.schmizz.sshj.xfer.LocalDestFile;
import net.schmizz.sshj.xfer.LocalSourceFile;
import prerna.engine.api.StorageTypeEnum;
import prerna.util.Constants;

public class SFTPStorageEngine extends AbstractStorageEngine {

	private static final Logger classLogger = LogManager.getLogger(SFTPStorageEngine.class);

	private static final String KEEP_ALIVE_INTERVAL = "KEEP_ALIVE_INTERVAL";
	private static final String SSH_TIMEOUT = "SSH_TIMEOUT";
	private static final String NEW_CONNECTION = "NEW_CONNECTION";

	// sftp keeps modified times in whole seconds, so anything under a second apart
	// is the same file as far as a sync is concerned
	private static final long SFTP_MTIME_TOLERANCE_MILLIS = 1000L;

	private transient SSHClient sshClient = null;
	private transient SFTPClient sftpClient = null;

	private String host = null;
	private String port = "22";
	private transient String username = null;
	private transient String password = null;

	// this is in seconds
	private int keepAlive = 60;
	// this is in millisecond
	private int sshConnectionTimeout = 300000; // 5min
	// do we always establish a new connection
	private boolean newConnection = false;

	@Override
	public StorageTypeEnum getStorageType() {
		return StorageTypeEnum.SFTP;
	}

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.host = smssProp.getProperty(Constants.HOSTNAME);
		this.port = smssProp.getProperty(Constants.PORT);
		if (this.port == null || this.port.isEmpty()) {
			this.port = "22"; // default sftp port
		}
		this.username = smssProp.getProperty(Constants.USERNAME);
		this.password = smssProp.getProperty(Constants.PASSWORD);

		if (this.smssProp.getProperty(KEEP_ALIVE_INTERVAL) != null) {
			String keepAliveStr = this.smssProp.getProperty(KEEP_ALIVE_INTERVAL);
			if (!(keepAliveStr = keepAliveStr.trim()).isEmpty()) {
				try {
					this.keepAlive = Integer.parseInt(keepAliveStr);
				} catch (Exception e) {
					classLogger.warn("Error occurred trying to parse and get the keep alive interval");
					classLogger.error("Failed to parse {}='{}'. Using default keepAlive={}.", KEEP_ALIVE_INTERVAL,
							keepAliveStr, this.keepAlive, e);
				}
			}
		}
		if (this.smssProp.getProperty(SSH_TIMEOUT) != null) {
			String sshConnectionTimeoutStr = this.smssProp.getProperty(SSH_TIMEOUT);
			if (!(sshConnectionTimeoutStr = sshConnectionTimeoutStr.trim()).isEmpty()) {
				try {
					this.sshConnectionTimeout = Integer.parseInt(sshConnectionTimeoutStr);
				} catch (Exception e) {
					classLogger.warn("Error occurred trying to parse and get the ssh connection timeout");
					classLogger.error("Failed to parse {}='{}'. Using default sshConnectionTimeout={}.", SSH_TIMEOUT,
							sshConnectionTimeoutStr, this.sshConnectionTimeout, e);
				}
			}
		}
		if (this.smssProp.getProperty(NEW_CONNECTION) != null) {
			String newConnectionStr = this.smssProp.getProperty(NEW_CONNECTION);
			if (!(newConnectionStr = newConnectionStr.trim()).isEmpty()) {
				try {
					this.newConnection = Boolean.parseBoolean(newConnectionStr);
				} catch (Exception e) {
					classLogger.warn("Error occurred trying to parse and get the new connection boolean");
					classLogger.error("Failed to parse {}='{}'. Using default newConnection={}.", NEW_CONNECTION,
							newConnectionStr, this.newConnection, e);
				}
			}
		}

		if (!this.newConnection) {
			classLogger.info("Attempting to establishing connection to {} on port {}", this.host, this.port);
			this.sshClient = getSSHClient();
			this.sftpClient = this.sshClient.newSFTPClient();
			classLogger.info("Successfully establishing connection to {} on port {}", this.host, this.port);
		}
	}

	private SSHClient getSSHClient() throws Exception {
		SSHClient sshClient = new SSHClient();
		try {
			sshClient.loadKnownHosts();
		} catch (IOException e) {
			classLogger.warn("Unable to find/load known hosts... ignoring error");
		}
		sshClient.addHostKeyVerifier(new PromiscuousVerifier());
		sshClient.connect(this.host, Integer.parseInt(this.port.trim()));
		sshClient.setTimeout(this.sshConnectionTimeout);
		sshClient.getConnection().getKeepAlive().setKeepAliveInterval(this.keepAlive);
		sshClient.authPassword(this.username, this.password);
		return sshClient;
	}

	@Override
	public void close() {
		close(this.sftpClient, this.sshClient);
	}

	/**
	 * 
	 * @param sftpClient
	 * @param sshClient
	 */
	private void close(SFTPClient sftpClient, SSHClient sshClient) {
		if (sftpClient != null) {
			try {
				sftpClient.close();
			} catch (IOException e) {
				classLogger.error("Failed to close SFTP client for host={} port={}.", this.host, this.port, e);
			}
		}
		if (sshClient != null) {
			try {
				sshClient.disconnect();
			} catch (IOException e) {
				classLogger.error("Failed to disconnect SSH client for host={} port={}.", this.host, this.port, e);
			}
		}
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
		// making these match the rclone names
		// [
		// {Path=Uber Eats July 17.pdf, Name=Uber Eats July 17.pdf, Size=51049.0,
		// MimeType=application/pdf, ModTime=2023-07-17T20:54:33.767000000Z,
		// IsDir=false, Tier=STANDARD},
		// {Path=college.csv, Name=college.csv, Size=1698156.0, MimeType=text/csv,
		// ModTime=2023-07-15T17:30:38.574000000Z, IsDir=false, Tier=STANDARD},
		// {Path=testFolder, Name=testFolder, Size=0.0, MimeType=inode/directory,
		// ModTime=2023-07-18T17:46:11.358608500-04:00, IsDir=true}
		// ]

		SSHClient sshClient = null;
		SFTPClient sftpClient = null;
		String basePath = path == null ? "" : path.replace("\\", "/").trim();
		if (basePath.isEmpty()) {
			basePath = "/";
		} else {
			if (!basePath.startsWith("/")) {
				basePath = "/" + basePath;
			}
			while (basePath.length() > 1 && basePath.endsWith("/")) {
				basePath = basePath.substring(0, basePath.length() - 1);
			}
		}
		try {
			if (this.newConnection) {
				classLogger.info("Attempting to establishing connection to {} on port {}", this.host, this.port);
				sshClient = getSSHClient();
				sftpClient = sshClient.newSFTPClient();
				classLogger.info("Successfully establishing connection to {} on port {}", this.host, this.port);
			} else {
				sftpClient = this.sftpClient;
			}

			List<RemoteResourceInfo> info = sftpClient.ls(path);
			List<Map<String, Object>> names = new ArrayList<>(info.size());
			for (RemoteResourceInfo remoteInfo : info) {
				String name = remoteInfo.getName();
				if (".".equals(name) || "..".equals(name)) {
					continue;
				}
				Map<String, Object> item = new HashMap<>();
				item.put("Name", name);
				item.put("Path", "/".equals(basePath) ? "/" + name : basePath + "/" + name);
				FileAttributes attributes = remoteInfo.getAttributes();
				item.put("Size", attributes.getSize());
				boolean isDir = attributes.getType() == Type.DIRECTORY;
				item.put("MimeType", isDir ? "inode/directory" : null);
				item.put("ModTime", Instant.ofEpochMilli(attributes.getMtime() * 1000L).toString());
				item.put("IsDir", isDir);
				item.put("Metadata", Collections.emptyMap());
				names.add(item);
			}
			return names;
		} finally {
			if (this.newConnection) {
				close(sftpClient, sshClient);
			}
		}
	}

	@Override
	public StorageSyncStatus syncLocalToStorage(String localPath, String storagePath, Map<String, Object> metadata)
			throws Exception {
		if (metadata != null && !metadata.isEmpty()) {
			// there is nowhere to put it - sftp only carries file attributes
			classLogger.warn("SFTP has no user metadata, ignoring {} entries for: {}", metadata.size(), storagePath);
		}

		Path localFolder = Paths.get(localPath);
		if (!Files.isDirectory(localFolder)) {
			throw new IllegalArgumentException("Local path is not a directory: " + localPath);
		}
		String remoteFolder = toRemotePath(storagePath);

		SSHClient sshClient = null;
		SFTPClient sftpClient = null;
		try {
			if (this.newConnection) {
				classLogger.info("Attempting to establishing connection to {} on port {}", this.host, this.port);
				sshClient = getSSHClient();
				sftpClient = sshClient.newSFTPClient();
				classLogger.info("Successfully establishing connection to {} on port {}", this.host, this.port);
			} else {
				sftpClient = this.sftpClient;
			}

			// one walk of what is already there, so the comparison below is local
			Map<String, StoredObjectStat> stored = new HashMap<>();
			collectStoredFiles(sftpClient, remoteFolder, "", stored);

			List<Path> localFiles;
			try (Stream<Path> stream = Files.walk(localFolder)) {
				localFiles = stream.filter(Files::isRegularFile).toList();
			}

			List<String> uploadedFiles = new ArrayList<>();
			List<String> skippedFiles = new ArrayList<>();
			List<String> failedFiles = new ArrayList<>();
			for (Path localFile : localFiles) {
				String relativePath = localFolder.relativize(localFile).toString().replace("\\", "/").trim();
				String targetPath = remoteFolder + "/" + relativePath;
				try {
					if (!needsUpload(localFile, stored.get(relativePath), SFTP_MTIME_TOLERANCE_MILLIS)) {
						skippedFiles.add(targetPath);
						continue;
					}
					int lastSlash = targetPath.lastIndexOf('/');
					if (lastSlash > 0) {
						sftpClient.mkdirs(targetPath.substring(0, lastSlash));
					}
					// sshj preserves the local mtime by default, which is what lets the
					// comparison above work on the next pass
					sftpClient.put(new FileSystemFile(localFile.toString()), targetPath);
					uploadedFiles.add(targetPath);
				} catch (Exception e) {
					// one bad file should not abandon the rest of the folder
					classLogger.error("Failed to upload {} to {}", localFile, targetPath, e);
					failedFiles.add(targetPath);
				}
			}

			classLogger.info("Sync of {} to {} uploaded {}, skipped {}, failed {}", localPath, remoteFolder,
					uploadedFiles.size(), skippedFiles.size(), failedFiles.size());
			return StorageSyncStatus.of(remoteFolder, uploadedFiles, skippedFiles, failedFiles);
		} finally {
			if (this.newConnection) {
				close(sftpClient, sshClient);
			}
		}
	}

	@Override
	public void syncStorageToLocal(String storagePath, String localPath) throws Exception {
		String remoteFolder = toRemotePath(storagePath);
		Path localFolder = Paths.get(localPath);
		Files.createDirectories(localFolder);

		SSHClient sshClient = null;
		SFTPClient sftpClient = null;
		try {
			if (this.newConnection) {
				classLogger.info("Attempting to establishing connection to {} on port {}", this.host, this.port);
				sshClient = getSSHClient();
				sftpClient = sshClient.newSFTPClient();
				classLogger.info("Successfully establishing connection to {} on port {}", this.host, this.port);
			} else {
				sftpClient = this.sftpClient;
			}

			if (sftpClient.statExistence(remoteFolder) == null) {
				throw new IllegalArgumentException("Storage path does not exist: " + storagePath);
			}

			// unlike the upload side this pulls everything down, matching how the other
			// engines behave when the local copy is treated as disposable
			int downloaded = downloadDirectoryContents(sftpClient, remoteFolder, localFolder);
			classLogger.info("Sync of {} to {} downloaded {} files", remoteFolder, localPath, downloaded);
		} finally {
			if (this.newConnection) {
				close(sftpClient, sshClient);
			}
		}
	}

	/**
	 * Turns a user supplied storage path into the absolute remote path sshj wants.
	 * Everything here is rooted at the server's path, so a relative path and an
	 * absolute one mean the same thing.
	 *
	 * @param storagePath the path as it came in, may be null or empty for the root
	 * @return the path with a leading slash and no trailing one
	 */
	private String toRemotePath(String storagePath) {
		String normalized = normalizeStoragePrefixPath(storagePath);
		return normalized.isEmpty() ? "/" : "/" + normalized;
	}

	/**
	 * Walks the server below the given folder and records the size and modified
	 * time of every file, keyed by its path relative to that folder.
	 *
	 * @param sftpClient      the open client
	 * @param remoteDirectory the directory to walk, absolute
	 * @param relativePrefix  where this directory sits under the sync root
	 * @param stored          collects the results
	 * @throws IOException if a listing fails for a reason other than the folder not
	 *                     being there
	 */
	private void collectStoredFiles(SFTPClient sftpClient, String remoteDirectory, String relativePrefix,
			Map<String, StoredObjectStat> stored) throws IOException {
		if (sftpClient.statExistence(remoteDirectory) == null) {
			// nothing uploaded yet, so every local file is new
			return;
		}
		for (RemoteResourceInfo remoteInfo : sftpClient.ls(remoteDirectory)) {
			String name = remoteInfo.getName();
			if (".".equals(name) || "..".equals(name)) {
				continue;
			}
			String relativePath = relativePrefix.isEmpty() ? name : relativePrefix + "/" + name;
			FileAttributes attributes = remoteInfo.getAttributes();
			if (attributes.getType() == Type.DIRECTORY) {
				collectStoredFiles(sftpClient, remoteInfo.getPath(), relativePath, stored);
			} else {
				// sftp reports mtime in whole seconds
				stored.put(relativePath, new StoredObjectStat(attributes.getSize(), attributes.getMtime() * 1000L));
			}
		}
	}

	/**
	 * Copies the contents of a remote directory into a local one, without nesting
	 * the remote directory's own name underneath it.
	 *
	 * @param sftpClient      the open client
	 * @param remoteDirectory the directory to read, absolute
	 * @param localDirectory  where its contents land
	 * @return how many files were written
	 * @throws IOException if a read or write fails
	 */
	private int downloadDirectoryContents(SFTPClient sftpClient, String remoteDirectory, Path localDirectory)
			throws IOException {
		Files.createDirectories(localDirectory);

		int downloaded = 0;
		for (RemoteResourceInfo remoteInfo : sftpClient.ls(remoteDirectory)) {
			String name = remoteInfo.getName();
			if (".".equals(name) || "..".equals(name)) {
				continue;
			}
			if (remoteInfo.getAttributes().getType() == Type.DIRECTORY) {
				downloaded += downloadDirectoryContents(sftpClient, remoteInfo.getPath(), localDirectory.resolve(name));
			} else {
				sftpClient.get(remoteInfo.getPath(), new FileSystemFile(localDirectory.resolve(name).toString()));
				downloaded++;
			}
		}
		return downloaded;
	}

	@Override
	public String copyToStorage(String localFilePath, String storageFolderPath, Map<String, Object> metadata)
			throws Exception {
		SSHClient sshClient = null;
		SFTPClient sftpClient = null;
		try {
			if (this.newConnection) {
				classLogger.info("Attempting to establishing connection to {} on port {}", this.host, this.port);
				sshClient = getSSHClient();
				sftpClient = sshClient.newSFTPClient();
				classLogger.info("Successfully establishing connection to {} on port {}", this.host, this.port);
			} else {
				sftpClient = this.sftpClient;
			}
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

			LocalSourceFile lsf = new FileSystemFile(localFilePath);
			sftpClient.put(lsf, storageFolderPath);
		} finally {
			if (this.newConnection) {
				close(sftpClient, sshClient);
			}
		}
		return null;
	}

	@Override
	public void copyToLocal(String storageFilePath, String localFolderPath, String versionId) throws Exception {
		if (versionId != null && !versionId.trim().isEmpty()) {
			throw new UnsupportedOperationException("Object versioning is not supported by SFTP");
		}

		SSHClient sshClient = null;
		SFTPClient sftpClient = null;
		try {
			if (this.newConnection) {
				classLogger.info("Attempting to establishing connection to {} on port {}", this.host, this.port);
				sshClient = getSSHClient();
				sftpClient = sshClient.newSFTPClient();
				classLogger.info("Successfully establishing connection to {} on port {}", this.host, this.port);
			} else {
				sftpClient = this.sftpClient;
			}
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

			LocalDestFile ldf = new FileSystemFile(localFolderPath);
			sftpClient.get(storageFilePath, ldf);
		} finally {
			if (this.newConnection) {
				close(sftpClient, sshClient);
			}
		}
	}

	@Override
	public void deleteFromStorage(String storagePath, boolean leaveFolderStructure) throws Exception {
		SSHClient sshClient = null;
		SFTPClient sftpClient = null;
		try {
			if (this.newConnection) {
				classLogger.info("Attempting to establishing connection to {} on port {}", this.host, this.port);
				sshClient = getSSHClient();
				sftpClient = sshClient.newSFTPClient();
				classLogger.info("Successfully establishing connection to {} on port {}", this.host, this.port);
			} else {
				sftpClient = this.sftpClient;
			}
			if (storagePath == null || storagePath.isEmpty()) {
				throw new NullPointerException("Must define the storage location of the file to delete");
			}
			storagePath = storagePath.replace("\\", "/");

			if (!storagePath.startsWith("/")) {
				storagePath = "/" + storagePath;
			}

			FileAttributes attributes = sftpClient.statExistence(storagePath);
			if (attributes == null) {
				throw new IllegalArgumentException("Storage file/folder " + storagePath + " does not exist");
			}

			if (attributes.getType() == FileMode.Type.DIRECTORY) {
				if (leaveFolderStructure) {
					recursivelyDeleteFiles(sftpClient, storagePath);
				} else {
					sftpClient.rmdir(storagePath);
				}
			} else {
				sftpClient.rm(storagePath);
			}
		} finally {
			if (this.newConnection) {
				close(sftpClient, sshClient);
			}
		}
	}

	@Override
	public void deleteFolderFromStorage(String storageFolderPath) throws Exception {
		SSHClient sshClient = null;
		SFTPClient sftpClient = null;
		try {
			if (this.newConnection) {
				classLogger.info("Attempting to establishing connection to {} on port {}", this.host, this.port);
				sshClient = getSSHClient();
				sftpClient = sshClient.newSFTPClient();
				classLogger.info("Successfully establishing connection to {} on port {}", this.host, this.port);
			} else {
				sftpClient = this.sftpClient;
			}
			if (storageFolderPath == null || storageFolderPath.isEmpty()) {
				throw new NullPointerException("Must define the storage location of the folder to delete");
			}
			storageFolderPath = storageFolderPath.replace("\\", "/");

			if (!storageFolderPath.startsWith("/")) {
				storageFolderPath = "/" + storageFolderPath;
			}

			FileAttributes attributes = sftpClient.statExistence(storageFolderPath);
			if (attributes == null) {
				throw new IllegalArgumentException("Storage folder " + storageFolderPath + " does not exist");
			}

			if (attributes.getType() != FileMode.Type.DIRECTORY) {
				throw new IllegalArgumentException("Storage path " + storageFolderPath + " is not a directory");
			}

			sftpClient.rmdir(storageFolderPath);
		} finally {
			if (this.newConnection) {
				close(sftpClient, sshClient);
			}
		}
	}

	/**
	 * 
	 * @param sftpClient
	 * @param storageDirectory
	 * @throws IOException
	 */
	private void recursivelyDeleteFiles(SFTPClient sftpClient, String storageDirectory) throws IOException {
		List<RemoteResourceInfo> ls = sftpClient.ls(storageDirectory);
		for (RemoteResourceInfo f : ls) {
			if (f.getAttributes().getType() == FileMode.Type.DIRECTORY) {
				recursivelyDeleteFiles(sftpClient, f.getPath());
			} else {
				sftpClient.rm(f.getPath());
			}
		}
	}

}
