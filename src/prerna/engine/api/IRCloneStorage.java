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
package prerna.engine.api;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import prerna.engine.impl.storage.StorageSyncStatus;
import prerna.logging.IgnoreEngineLogging;

public interface IRCloneStorage extends IStorageEngine {

	/**
	 * Determine if a shared/created r clone config can be shared between methods
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	boolean canReuseRcloneConfig();

	/**
	 * Set the folder path for writing the config files on execution
	 * 
	 * @param folderPath
	 */
	@IgnoreEngineLogging
	void setRCloneConfigFolder(String folderPath);

	/**
	 * This method is responsible for creating the specific r clone configuration
	 * object for this storage type
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@IgnoreEngineLogging
	String createRCloneConfig() throws IOException, InterruptedException;

	/**
	 * 
	 * @param rCloneConfig
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@IgnoreEngineLogging
	void deleteRcloneConfig(String rCloneConfig) throws IOException, InterruptedException;

	/**
	 * Lists the folders and files for the relative path provided Note - not
	 * recursive
	 * 
	 * @param path
	 * @param rCloneConfig
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	List<String> listWithConfig(String path, String rCloneConfig) throws IOException, InterruptedException;

	/**
	 * 
	 * @param path
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	List<Map<String, Object>> listDetailsWithConfig(String path, String rCloneConfig)
			throws IOException, InterruptedException;

	/**
	 * 
	 * @param localPath
	 * @param storagePath
	 * @param rCloneConfig
	 * @param metadata
	 * @throws IOException
	 * @throws InterruptedException
	 */
	StorageSyncStatus syncLocalToStorageWithConfig(String localPath, String storagePath, String rCloneConfig,
			Map<String, Object> metadata) throws IOException, InterruptedException;

	/**
	 * 
	 * @param storagePath
	 * @param localPath
	 * @param rCloneConfig
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void syncStorageToLocalWithConfig(String storagePath, String localPath, String rCloneConfig)
			throws IOException, InterruptedException;

	/**
	 * Copy (without deleting) the file to the storage engine
	 * 
	 * @param localFilePath
	 * @param storageFolderPath
	 * @param rCloneConfig
	 * @param metadata
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void copyToStorageWithConfig(String localFilePath, String storageFolderPath, String rCloneConfig,
			Map<String, Object> metadata) throws IOException, InterruptedException;

	/**
	 * Copy (without deleting) the file to a local location
	 * 
	 * @param storageFilePath
	 * @param localFolderPath
	 * @param rCloneConfig
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void copyToLocalWithConfig(String storageFilePath, String localFolderPath, String rCloneConfig)
			throws IOException, InterruptedException;

	/**
	 * Delete the folder or file from the storage engine Will delete the directory
	 * structure
	 * 
	 * @param storageFilePath
	 * @param rCloneConfig
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void deleteFromStorageWithConfig(String storageFilePath, String rCloneConfig)
			throws IOException, InterruptedException;

	/**
	 * Delete the folder or file from the storage engine
	 * 
	 * @param storageFilePath
	 * @param leaveFolderStructure
	 * @param rCloneConfig
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void deleteFromStorageWithConfig(String storageFilePath, boolean leaveFolderStructure, String rCloneConfig)
			throws IOException, InterruptedException;

	/**
	 * 
	 * @param storageFolderPath
	 * @param rCloneConfig
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void deleteFolderFromStorageWithConfig(String storageFolderPath, String rCloneConfig)
			throws IOException, InterruptedException;

}
