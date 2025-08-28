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
import java.util.Properties;

public interface IStorageEngine extends IEngine {

  // this is what the FE sends for the type of storage we are creating
  // as a result, cannot be a key in the smss file
  String STORAGE_TYPE = "STORAGE_TYPE";

  /**
   * @return
   */
  StorageTypeEnum getStorageType();

  /**
   * @param smssProp
   */
  void open(Properties smssProp) throws Exception;

  /**
   * @param path
   * @return
   * @throws Exception
   */
  List<String> list(String path) throws Exception;

  /**
   * @param path
   * @return
   * @throws IOException
   * @throws InterruptedException
   */
  List<Map<String, Object>> listDetails(String path) throws Exception;

  /**
   * @param localPath
   * @param storagePath
   * @param metadata
   * @throws Exception
   */
  void syncLocalToStorage(String localPath, String storagePath, Map<String, Object> metadata)
      throws Exception;

  /**
   * @param storagePath
   * @param localPath
   * @throws Exception
   */
  void syncStorageToLocal(String storagePath, String localPath) throws Exception;

  /**
   * @param localFilePath
   * @param storageFolderPath
   * @param metadata
   * @throws Exception
   */
  void copyToStorage(String localFilePath, String storageFolderPath, Map<String, Object> metadata)
      throws Exception;

  /**
   * @param storageFilePath
   * @param localFolderPath
   * @throws Exception
   */
  void copyToLocal(String storageFilePath, String localFolderPath) throws Exception;

  /**
   * @param storageFilePath
   * @throws Exception
   */
  void deleteFromStorage(String storagePath) throws Exception;

  /**
   * @param storagePath
   * @param leaveFolderStructure
   * @throws Exception
   */
  void deleteFromStorage(String storagePath, boolean leaveFolderStructure) throws Exception;

  /**
   * @param storageFolderPath
   * @throws Exception
   */
  void deleteFolderFromStorage(String storageFolderPath) throws Exception;
}
