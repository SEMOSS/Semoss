/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.engine.api;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface IRCloneStorage extends IStorageEngine {

	/**
	 * Determine if a shared/created r clone config can be shared between methods
	 *
	 * @return
	 */
	boolean canReuseRcloneConfig();

	/**
	 * Set the folder path for writing the config files on execution
	 *
	 * @param folderPath
	 */
	void setRCloneConfigFolder(String folderPath);

	/**
	 * This method is responsible for creating the specific r clone configuration
	 * object for this storage type
	 *
	 * @throws IOException
	 * @throws InterruptedException
	 */
	String createRCloneConfig() throws IOException, InterruptedException;

	/**
	 * @param rCloneConfig
	 * @throws IOException
	 * @throws InterruptedException
	 */
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
	List<String> list(String path, String rCloneConfig) throws IOException, InterruptedException;

	/**
	 * @param path
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	List<Map<String, Object>> listDetails(String path, String rCloneConfig) throws IOException, InterruptedException;

	/**
	 * @param localPath
	 * @param storagePath
	 * @param rCloneConfig
	 * @param metadata
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void syncLocalToStorage(String localPath, String storagePath, String rCloneConfig, Map<String, Object> metadata)
			throws IOException, InterruptedException;

	/**
	 * @param storagePath
	 * @param localPath
	 * @param rCloneConfig
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void syncStorageToLocal(String storagePath, String localPath, String rCloneConfig)
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
	void copyToStorage(String localFilePath, String storageFolderPath, String rCloneConfig,
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
	void copyToLocal(String storageFilePath, String localFolderPath, String rCloneConfig)
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
	void deleteFromStorage(String storageFilePath, String rCloneConfig) throws IOException, InterruptedException;

	/**
	 * Delete the folder or file from the storage engine
	 *
	 * @param storageFilePath
	 * @param leaveFolderStructure
	 * @param rCloneConfig
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void deleteFromStorage(String storageFilePath, boolean leaveFolderStructure, String rCloneConfig)
			throws IOException, InterruptedException;

	/**
	 * @param storageFolderPath
	 * @param rCloneConfig
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void deleteFolderFromStorage(String storageFolderPath, String rCloneConfig)
			throws IOException, InterruptedException;
}
