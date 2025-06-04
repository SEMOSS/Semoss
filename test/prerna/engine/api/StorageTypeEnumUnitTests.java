package prerna.engine.api;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import prerna.engine.api.StorageTypeEnum;
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
		Exception thrown = assertThrows(
				IllegalArgumentException.class,
				() -> StorageTypeEnum.getEnumFromName(badName)
				);
		assertEquals("Invalid input for name " + badName, thrown.getMessage());
	}
}
