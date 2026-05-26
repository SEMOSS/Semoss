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

import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbFile;
import prerna.engine.api.StorageTypeEnum;
import prerna.util.Constants;

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
		String normalizedPath = path == null ? "" : path.replace("\\", "/").trim();
		if (!normalizedPath.isEmpty() && !normalizedPath.endsWith("/")) {
			normalizedPath += "/";
		}
		SmbFile smbF = new SmbFile(this.pathPrefix + normalizedPath, this.auth);
		SmbFile[] children = smbF.listFiles();
		if (children == null) {
			return Collections.emptyList();
		}

		String relativeBasePath = normalizedPath;
		while (relativeBasePath.startsWith("/")) {
			relativeBasePath = relativeBasePath.substring(1);
		}
		while (relativeBasePath.endsWith("/")) {
			relativeBasePath = relativeBasePath.substring(0, relativeBasePath.length() - 1);
		}

		List<Map<String, Object>> details = new ArrayList<>(children.length);
		for (SmbFile child : children) {
			String name = child.getName();
			while (name.endsWith("/")) {
				name = name.substring(0, name.length() - 1);
			}
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
		// TODO Auto-generated method stub

	}

	@Override
	public void copyToLocal(String storageFilePath, String localFolderPath) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteFromStorage(String storagePath) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteFromStorage(String storagePath, boolean leaveFolderStructure) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteFolderFromStorage(String storageFolderPath) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

}
