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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import prerna.engine.impl.storage.AzureBlobStorageEngine;
import prerna.engine.impl.storage.CephStorageEngine;
import prerna.engine.impl.storage.GoogleCloudStorageEngine;
import prerna.engine.impl.storage.JCIFSStorageEngine;
import prerna.engine.impl.storage.LocalFileSystemStorageEngine;
import prerna.engine.impl.storage.MinioStorageEngine;
import prerna.engine.impl.storage.S3StorageEngine;
import prerna.engine.impl.storage.SFTPStorageEngine;

public class StorageTypeEnumUnitTests {

  @Test
  void testS3() {
    StorageTypeEnum testEnum = StorageTypeEnum.AMAZON_S3;
    assertEquals("AMAZON_S3", testEnum.getStorageName());
    assertEquals(S3StorageEngine.class.getName(), testEnum.getStorageClass());
  }

  @Test
  void testCeph() {
    StorageTypeEnum testEnum = StorageTypeEnum.CEPH;
    assertEquals("CEPH", testEnum.getStorageName());
    assertEquals(CephStorageEngine.class.getName(), testEnum.getStorageClass());
  }

  @Test
  void testGoogle() {
    StorageTypeEnum testEnum = StorageTypeEnum.GOOGLE_CLOUD_STORAGE;
    assertEquals("GOOGLE_CLOUD_STORAGE", testEnum.getStorageName());
    assertEquals(GoogleCloudStorageEngine.class.getName(), testEnum.getStorageClass());
  }

  @Test
  void testJCIFS() {
    StorageTypeEnum testEnum = StorageTypeEnum.JCIFS;
    assertEquals("JCIFS", testEnum.getStorageName());
    assertEquals(JCIFSStorageEngine.class.getName(), testEnum.getStorageClass());
  }

  @Test
  void testLocalFiles() {
    StorageTypeEnum testEnum = StorageTypeEnum.LOCAL_FILE_SYSTEM;
    assertEquals("LOCAL_FILE_SYSTEM", testEnum.getStorageName());
    assertEquals(LocalFileSystemStorageEngine.class.getName(), testEnum.getStorageClass());
  }

  @Test
  void testMicrosoft() {
    StorageTypeEnum testEnum = StorageTypeEnum.MICROSOFT_AZURE_BLOB_STORAGE;
    assertEquals("MICROSOFT_AZURE_BLOB_STORAGE", testEnum.getStorageName());
    assertEquals(AzureBlobStorageEngine.class.getName(), testEnum.getStorageClass());
  }

  @Test
  void testMinio() {
    StorageTypeEnum testEnum = StorageTypeEnum.MINIO;
    assertEquals("MINIO", testEnum.getStorageName());
    assertEquals(MinioStorageEngine.class.getName(), testEnum.getStorageClass());
  }

  @Test
  void testSFTP() {
    StorageTypeEnum testEnum = StorageTypeEnum.SFTP;
    assertEquals("SFTP", testEnum.getStorageName());
    assertEquals(SFTPStorageEngine.class.getName(), testEnum.getStorageClass());
  }

  @Test
  void testSMB() {
    StorageTypeEnum testEnum = StorageTypeEnum.SMB_CIFS;
    assertEquals("SMB_CIFS", testEnum.getStorageName());
    assertEquals(JCIFSStorageEngine.class.getName(), testEnum.getStorageClass());
  }

  @Test
  void testBadFunctionName() {
    String badName = "NOT_A_REAL_STORAGE_TYPE";
    Exception thrown =
        assertThrows(
            IllegalArgumentException.class, () -> StorageTypeEnum.getEnumFromName(badName));
    assertEquals("Invalid input for name " + badName, thrown.getMessage());
  }
}
