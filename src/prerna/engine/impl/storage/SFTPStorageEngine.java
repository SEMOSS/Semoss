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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
		} catch (Exception e) {
			try {
				sshClient.close();
			} catch (IOException closeException) {
				e.addSuppressed(closeException);
			}
			throw e;
		}
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
	public void syncLocalToStorage(String localPath, String storagePath, Map<String, Object> metadata)
			throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void syncStorageToLocal(String storagePath, String localPath) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void copyToStorage(String localFilePath, String storageFolderPath, Map<String, Object> metadata)
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
	}

	@Override
	public void copyToLocal(String storageFilePath, String localFolderPath) throws Exception {
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
	public void deleteFromStorage(String storagePath) throws Exception {
		deleteFromStorage(storagePath, false);
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

	////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////

//	public static void main(String[] args) throws Exception {
//		/**
//		 * 
//		 	version: '3.1'
//			services:
//			    sftp:
//			        image: atmoz/sftp
//			        volumes:
//			            - C:\Users\mahkhalil\Documents\sftp\mount:/home/foo/upload
//			        ports:
//			            - "2222:22"
//			        command: foo:pass:1001
//		 */
//
//		// these are not real/import access/secret - only for local docker
//		Properties mockSmss = new Properties();
//		mockSmss.put(Constants.HOSTNAME, "localhost");
//		mockSmss.put(Constants.PORT, "2222");
//		mockSmss.put(Constants.USERNAME, "foo");
//		mockSmss.put(Constants.PASSWORD, "pass");
//
//		SFTPStorageEngine engine = new SFTPStorageEngine();
//		engine.connect(mockSmss);
//
//		{
//			List<String> list = engine.list("/");
//			System.out.println(list);
//		}
//		{
//			List<Map<String, Object>> list = engine.listDetails("/upload/");
//			System.out.println(list);
//		}
//		{
//			engine.copyToStorage("C:\\Users\\mahkhalil\\Downloads\\MooseAI Logo.png", "upload/test1");
//		}
//		{
//			engine.copyToLocal("upload/MooseAI Logo.png", "C:\\Users\\mahkhalil");
//		}
//		{
//			engine.deleteFromStorage("upload/test1/MooseAI Logo.png");
//		}
//		
//		engine.disconnect();
//	}

}
