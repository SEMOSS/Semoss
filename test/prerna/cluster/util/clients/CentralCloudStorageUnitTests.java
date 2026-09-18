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
package prerna.cluster.util.clients;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InOrder;
import org.mockito.MockedStatic;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.engine.impl.storage.AbstractStorageEngine;
import prerna.project.api.IProject;
import prerna.project.impl.ProjectHelper;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineSyncUtility;
import prerna.util.EngineUtility;
import prerna.util.ProjectSyncUtility;
import prerna.util.ProjectWatcher;
import prerna.util.SMSSNoInitEngineWatcher;
import prerna.util.SMSSWebWatcher;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;
import sun.misc.Unsafe;

class CentralCloudStorageUnitTests {

	// -------------------------------------------------------
	// Main blob constants
	// -------------------------------------------------------

	@Test
	void testDatabaseBlob() {
		assertEquals("semoss-db", CentralCloudStorage.DATABASE_BLOB);
	}

	@Test
	void testStorageBlob() {
		assertEquals("semoss-storage", CentralCloudStorage.STORAGE_BLOB);
	}

	@Test
	void testModelBlob() {
		assertEquals("semoss-model", CentralCloudStorage.MODEL_BLOB);
	}

	@Test
	void testVectorBlob() {
		assertEquals("semoss-vector", CentralCloudStorage.VECTOR_BLOB);
	}

	@Test
	void testFunctionBlob() {
		assertEquals("semoss-function", CentralCloudStorage.FUNCTION_BLOB);
	}

	@Test
	void testGuardrailBlob() {
		assertEquals("semoss-guardrail", CentralCloudStorage.GUARDRAIL_BLOB);
	}

	@Test
	void testVenvBlob() {
		assertEquals("semoss-venv", CentralCloudStorage.VENV_BLOB);
	}

	@Test
	void testProjectBlob() {
		assertEquals("semoss-project", CentralCloudStorage.PROJECT_BLOB);
	}

	@Test
	void testUserBlob() {
		assertEquals("semoss-user", CentralCloudStorage.USER_BLOB);
	}

	@Test
	void testRoomBlob() {
		assertEquals("semoss-room", CentralCloudStorage.ROOM_BLOB);
	}

	// -------------------------------------------------------
	// Image blob constants
	// -------------------------------------------------------

	@Test
	void testDbImagesBlob() {
		assertEquals("semoss-dbimagecontainer", CentralCloudStorage.DB_IMAGES_BLOB);
	}

	@Test
	void testStorageImagesBlob() {
		assertEquals("semoss-storageimagecontainer", CentralCloudStorage.STORAGE_IMAGES_BLOB);
	}

	@Test
	void testModelImagesBlob() {
		assertEquals("semoss-modelimagecontainer", CentralCloudStorage.MODEL_IMAGES_BLOB);
	}

	@Test
	void testVectorImagesBlob() {
		assertEquals("semoss-vectorimagecontainer", CentralCloudStorage.VECTOR_IMAGES_BLOB);
	}

	@Test
	void testFunctionImagesBlob() {
		assertEquals("semoss-functionimagecontainer", CentralCloudStorage.FUNCTION_IMAGES_BLOB);
	}

	@Test
	void testGuardrailImagesBlob() {
		assertEquals("semoss-guardrailimagecontainer", CentralCloudStorage.GUARDRAIL_IMAGES_BLOB);
	}

	@Test
	void testVenvImagesBlob() {
		assertEquals("semoss-venvimagecontainer", CentralCloudStorage.VENV_IMAGES_BLOB);
	}

	@Test
	void testProjectImagesBlob() {
		assertEquals("semoss-projectimagecontainer", CentralCloudStorage.PROJECT_IMAGES_BLOB);
	}

	// -------------------------------------------------------
	// SMSS postfix constant
	// -------------------------------------------------------

	@Test
	void testSmssPostfix() {
		assertEquals("-smss", CentralCloudStorage.SMSS_POSTFIX);
	}

	// -------------------------------------------------------
	// Consistency checks across blob names
	// -------------------------------------------------------

	@Test
	void testAllMainBlobsStartWithSemoss() {
		assertTrue(CentralCloudStorage.DATABASE_BLOB.startsWith("semoss-"));
		assertTrue(CentralCloudStorage.STORAGE_BLOB.startsWith("semoss-"));
		assertTrue(CentralCloudStorage.MODEL_BLOB.startsWith("semoss-"));
		assertTrue(CentralCloudStorage.VECTOR_BLOB.startsWith("semoss-"));
		assertTrue(CentralCloudStorage.FUNCTION_BLOB.startsWith("semoss-"));
		assertTrue(CentralCloudStorage.GUARDRAIL_BLOB.startsWith("semoss-"));
		assertTrue(CentralCloudStorage.VENV_BLOB.startsWith("semoss-"));
		assertTrue(CentralCloudStorage.PROJECT_BLOB.startsWith("semoss-"));
		assertTrue(CentralCloudStorage.USER_BLOB.startsWith("semoss-"));
		assertTrue(CentralCloudStorage.ROOM_BLOB.startsWith("semoss-"));
	}

	@Test
	void testAllImageBlobsEndWithImagecontainer() {
		assertTrue(CentralCloudStorage.DB_IMAGES_BLOB.endsWith("imagecontainer"));
		assertTrue(CentralCloudStorage.STORAGE_IMAGES_BLOB.endsWith("imagecontainer"));
		assertTrue(CentralCloudStorage.MODEL_IMAGES_BLOB.endsWith("imagecontainer"));
		assertTrue(CentralCloudStorage.VECTOR_IMAGES_BLOB.endsWith("imagecontainer"));
		assertTrue(CentralCloudStorage.FUNCTION_IMAGES_BLOB.endsWith("imagecontainer"));
		assertTrue(CentralCloudStorage.GUARDRAIL_IMAGES_BLOB.endsWith("imagecontainer"));
		assertTrue(CentralCloudStorage.VENV_IMAGES_BLOB.endsWith("imagecontainer"));
		assertTrue(CentralCloudStorage.PROJECT_IMAGES_BLOB.endsWith("imagecontainer"));
	}

	@Test
	void testDbImageBlobCorrespondsToDbBlob() {
		assertTrue(CentralCloudStorage.DB_IMAGES_BLOB.startsWith("semoss-db"));
	}

	@Test
	void testStorageImageBlobCorrespondsToStorageBlob() {
		assertTrue(CentralCloudStorage.STORAGE_IMAGES_BLOB.startsWith("semoss-storage"));
	}

	@Test
	void testModelImageBlobCorrespondsToModelBlob() {
		assertTrue(CentralCloudStorage.MODEL_IMAGES_BLOB.startsWith("semoss-model"));
	}

	// -------------------------------------------------------
	// Instance method tests using Unsafe.allocateInstance
	// -------------------------------------------------------

	@Nested
	class InstanceMethodTests {

		private CentralCloudStorage instance;

		@BeforeEach
		void setUp() throws Exception {
			Field f = Unsafe.class.getDeclaredField("theUnsafe");
			f.setAccessible(true);
			Unsafe unsafe = (Unsafe) f.get(null);
			instance = (CentralCloudStorage) unsafe.allocateInstance(CentralCloudStorage.class);
		}

		// ------- getCloudPrefixForEngine -------

		@Test
		void testGetCloudPrefixForEngine_DATABASE() throws Exception {
			String result = instance.getCloudPrefixForEngine(IEngine.CATALOG_TYPE.DATABASE);
			assertNotNull(result);
			Field dbField = CentralCloudStorage.class.getDeclaredField("DB_CONTAINER_PREFIX");
			dbField.setAccessible(true);
			assertEquals(dbField.get(null), result);
		}

		@Test
		void testGetCloudPrefixForEngine_STORAGE() throws Exception {
			String result = instance.getCloudPrefixForEngine(IEngine.CATALOG_TYPE.STORAGE);
			assertNotNull(result);
			Field f = CentralCloudStorage.class.getDeclaredField("STORAGE_CONTAINER_PREFIX");
			f.setAccessible(true);
			assertEquals(f.get(null), result);
		}

		@Test
		void testGetCloudPrefixForEngine_MODEL() throws Exception {
			String result = instance.getCloudPrefixForEngine(IEngine.CATALOG_TYPE.MODEL);
			assertNotNull(result);
			Field f = CentralCloudStorage.class.getDeclaredField("MODEL_CONTAINER_PREFIX");
			f.setAccessible(true);
			assertEquals(f.get(null), result);
		}

		@Test
		void testGetCloudPrefixForEngine_VECTOR() throws Exception {
			String result = instance.getCloudPrefixForEngine(IEngine.CATALOG_TYPE.VECTOR);
			assertNotNull(result);
			Field f = CentralCloudStorage.class.getDeclaredField("VECTOR_CONTAINER_PREFIX");
			f.setAccessible(true);
			assertEquals(f.get(null), result);
		}

		@Test
		void testGetCloudPrefixForEngine_FUNCTION() throws Exception {
			String result = instance.getCloudPrefixForEngine(IEngine.CATALOG_TYPE.FUNCTION);
			assertNotNull(result);
			Field f = CentralCloudStorage.class.getDeclaredField("FUNCTION_CONTAINER_PREFIX");
			f.setAccessible(true);
			assertEquals(f.get(null), result);
		}

		@Test
		void testGetCloudPrefixForEngine_GUARDRAIL() throws Exception {
			String result = instance.getCloudPrefixForEngine(IEngine.CATALOG_TYPE.GUARDRAIL);
			assertNotNull(result);
			Field f = CentralCloudStorage.class.getDeclaredField("GUARDRAIL_CONTAINER_PREFIX");
			f.setAccessible(true);
			assertEquals(f.get(null), result);
		}

		@Test
		void testGetCloudPrefixForEngine_VENV() throws Exception {
			String result = instance.getCloudPrefixForEngine(IEngine.CATALOG_TYPE.VENV);
			assertNotNull(result);
			Field f = CentralCloudStorage.class.getDeclaredField("VENV_CONTAINER_PREFIX");
			f.setAccessible(true);
			assertEquals(f.get(null), result);
		}

		@Test
		void testGetCloudPrefixForEngine_PROJECT() throws Exception {
			String result = instance.getCloudPrefixForEngine(IEngine.CATALOG_TYPE.PROJECT);
			assertNotNull(result);
			Field f = CentralCloudStorage.class.getDeclaredField("PROJECT_CONTAINER_PREFIX");
			f.setAccessible(true);
			assertEquals(f.get(null), result);
		}

		@Test
		void testGetCloudPrefixForEngine_nullThrows() {
			assertThrows(IllegalArgumentException.class, () -> instance.getCloudPrefixForEngine(null));
		}

		// ------- getCloudEngineImageBucket -------

		@Test
		void testGetCloudEngineImageBucket_DATABASE() {
			assertEquals(CentralCloudStorage.DB_IMAGES_BLOB,
					instance.getCloudEngineImageBucket(IEngine.CATALOG_TYPE.DATABASE));
		}

		@Test
		void testGetCloudEngineImageBucket_STORAGE() {
			assertEquals(CentralCloudStorage.STORAGE_IMAGES_BLOB,
					instance.getCloudEngineImageBucket(IEngine.CATALOG_TYPE.STORAGE));
		}

		@Test
		void testGetCloudEngineImageBucket_MODEL() {
			assertEquals(CentralCloudStorage.MODEL_IMAGES_BLOB,
					instance.getCloudEngineImageBucket(IEngine.CATALOG_TYPE.MODEL));
		}

		@Test
		void testGetCloudEngineImageBucket_VECTOR() {
			assertEquals(CentralCloudStorage.VECTOR_IMAGES_BLOB,
					instance.getCloudEngineImageBucket(IEngine.CATALOG_TYPE.VECTOR));
		}

		@Test
		void testGetCloudEngineImageBucket_FUNCTION() {
			assertEquals(CentralCloudStorage.FUNCTION_IMAGES_BLOB,
					instance.getCloudEngineImageBucket(IEngine.CATALOG_TYPE.FUNCTION));
		}

		@Test
		void testGetCloudEngineImageBucket_GUARDRAIL() {
			assertEquals(CentralCloudStorage.GUARDRAIL_IMAGES_BLOB,
					instance.getCloudEngineImageBucket(IEngine.CATALOG_TYPE.GUARDRAIL));
		}

		@Test
		void testGetCloudEngineImageBucket_VENV() {
			assertEquals(CentralCloudStorage.VENV_IMAGES_BLOB,
					instance.getCloudEngineImageBucket(IEngine.CATALOG_TYPE.VENV));
		}

		@Test
		void testGetCloudEngineImageBucket_PROJECT() {
			assertEquals(CentralCloudStorage.PROJECT_IMAGES_BLOB,
					instance.getCloudEngineImageBucket(IEngine.CATALOG_TYPE.PROJECT));
		}

		@Test
		void testGetCloudEngineImageBucket_nullThrows() {
			assertThrows(IllegalArgumentException.class, () -> instance.getCloudEngineImageBucket(null));
		}

		// ------- getSqlLiteFile -------

		@TempDir
		Path tempDir;

		@Test
		void testGetSqlLiteFile_findsOneSqliteFile() throws Exception {
			Files.createFile(tempDir.resolve("mydb.sqlite"));
			Files.createFile(tempDir.resolve("other.txt"));

			Method m = CentralCloudStorage.class.getDeclaredMethod("getSqlLiteFile", String.class);
			m.setAccessible(true);
			@SuppressWarnings("unchecked")
			List<String> result = (List<String>) m.invoke(instance, tempDir.toString());

			assertEquals(1, result.size());
			assertEquals("mydb.sqlite", result.get(0));
		}

		@Test
		void testGetSqlLiteFile_emptyDirectory() throws Exception {

			Method m = CentralCloudStorage.class.getDeclaredMethod("getSqlLiteFile", String.class);
			m.setAccessible(true);
			@SuppressWarnings("unchecked")
			List<String> result = (List<String>) m.invoke(instance, tempDir.toString());

			assertTrue(result.isEmpty());
		}

		@Test
		void testGetSqlLiteFile_multipleSqlite_removesInsightsDatabase() throws Exception {
			Files.createFile(tempDir.resolve("mydb.sqlite"));
			Files.createFile(tempDir.resolve("insights_database.sqlite"));

			Method m = CentralCloudStorage.class.getDeclaredMethod("getSqlLiteFile", String.class);
			m.setAccessible(true);
			@SuppressWarnings("unchecked")
			List<String> result = (List<String>) m.invoke(instance, tempDir.toString());

			assertEquals(1, result.size());
			assertEquals("mydb.sqlite", result.get(0));
		}

		@Test
		void testGetSqlLiteFile_singleInsightsDatabase_kept() throws Exception {
			Files.createFile(tempDir.resolve("insights_database.sqlite"));

			Method m = CentralCloudStorage.class.getDeclaredMethod("getSqlLiteFile", String.class);
			m.setAccessible(true);
			@SuppressWarnings("unchecked")
			List<String> result = (List<String>) m.invoke(instance, tempDir.toString());

			assertEquals(1, result.size());
			assertEquals("insights_database.sqlite", result.get(0));
		}

		@Test
		void testGetSqlLiteFile_noSqliteFilesPresent() throws Exception {
			Files.createFile(tempDir.resolve("data.csv"));
			Files.createFile(tempDir.resolve("config.json"));

			Method m = CentralCloudStorage.class.getDeclaredMethod("getSqlLiteFile", String.class);
			m.setAccessible(true);
			@SuppressWarnings("unchecked")
			List<String> result = (List<String>) m.invoke(instance, tempDir.toString());

			assertTrue(result.isEmpty());
		}

		@Test
		void testGetSqlLiteFile_threeSqliteFiles_removesInsights() throws Exception {
			Files.createFile(tempDir.resolve("first.sqlite"));
			Files.createFile(tempDir.resolve("second.sqlite"));
			Files.createFile(tempDir.resolve("insights_database.sqlite"));

			Method m = CentralCloudStorage.class.getDeclaredMethod("getSqlLiteFile", String.class);
			m.setAccessible(true);
			@SuppressWarnings("unchecked")
			List<String> result = (List<String>) m.invoke(instance, tempDir.toString());

			assertEquals(2, result.size());
			assertTrue(result.contains("first.sqlite"));
			assertTrue(result.contains("second.sqlite"));
			assertFalse(result.contains("insights_database.sqlite"));
		}

		@Test
		void testGetSqlLiteFile_ignoresSubdirectories() throws Exception {
			Files.createFile(tempDir.resolve("mydb.sqlite"));
			Files.createDirectory(tempDir.resolve("subdir"));

			Method m = CentralCloudStorage.class.getDeclaredMethod("getSqlLiteFile", String.class);
			m.setAccessible(true);
			@SuppressWarnings("unchecked")
			List<String> result = (List<String>) m.invoke(instance, tempDir.toString());

			assertEquals(1, result.size());
			assertEquals("mydb.sqlite", result.get(0));
		}

		// ------- getH2File -------

		@Test
		void testGetH2File_findsOneMvDbFile() throws Exception {
			Files.createFile(tempDir.resolve("mydb.mv.db"));
			Files.createFile(tempDir.resolve("other.txt"));

			Method m = CentralCloudStorage.class.getDeclaredMethod("getH2File", String.class);
			m.setAccessible(true);
			@SuppressWarnings("unchecked")
			List<String> result = (List<String>) m.invoke(instance, tempDir.toString());

			assertEquals(1, result.size());
			assertEquals("mydb.mv.db", result.get(0));
		}

		@Test
		void testGetH2File_emptyDirectory() throws Exception {

			Method m = CentralCloudStorage.class.getDeclaredMethod("getH2File", String.class);
			m.setAccessible(true);
			@SuppressWarnings("unchecked")
			List<String> result = (List<String>) m.invoke(instance, tempDir.toString());

			assertTrue(result.isEmpty());
		}

		@Test
		void testGetH2File_multipleH2_removesInsightsDatabase() throws Exception {
			Files.createFile(tempDir.resolve("mydb.mv.db"));
			Files.createFile(tempDir.resolve("insights_database.mv.db"));

			Method m = CentralCloudStorage.class.getDeclaredMethod("getH2File", String.class);
			m.setAccessible(true);
			@SuppressWarnings("unchecked")
			List<String> result = (List<String>) m.invoke(instance, tempDir.toString());

			assertEquals(1, result.size());
			assertEquals("mydb.mv.db", result.get(0));
		}

		@Test
		void testGetH2File_singleInsightsDatabase_kept() throws Exception {
			Files.createFile(tempDir.resolve("insights_database.mv.db"));

			Method m = CentralCloudStorage.class.getDeclaredMethod("getH2File", String.class);
			m.setAccessible(true);
			@SuppressWarnings("unchecked")
			List<String> result = (List<String>) m.invoke(instance, tempDir.toString());

			assertEquals(1, result.size());
			assertEquals("insights_database.mv.db", result.get(0));
		}

		@Test
		void testGetH2File_noH2FilesPresent() throws Exception {
			Files.createFile(tempDir.resolve("data.csv"));
			Files.createFile(tempDir.resolve("config.json"));

			Method m = CentralCloudStorage.class.getDeclaredMethod("getH2File", String.class);
			m.setAccessible(true);
			@SuppressWarnings("unchecked")
			List<String> result = (List<String>) m.invoke(instance, tempDir.toString());

			assertTrue(result.isEmpty());
		}

		@Test
		void testGetH2File_threeH2Files_removesInsights() throws Exception {
			Files.createFile(tempDir.resolve("first.mv.db"));
			Files.createFile(tempDir.resolve("second.mv.db"));
			Files.createFile(tempDir.resolve("insights_database.mv.db"));

			Method m = CentralCloudStorage.class.getDeclaredMethod("getH2File", String.class);
			m.setAccessible(true);
			@SuppressWarnings("unchecked")
			List<String> result = (List<String>) m.invoke(instance, tempDir.toString());

			assertEquals(2, result.size());
			assertTrue(result.contains("first.mv.db"));
			assertTrue(result.contains("second.mv.db"));
			assertFalse(result.contains("insights_database.mv.db"));
		}

		@Test
		void testGetH2File_ignoresSubdirectories() throws Exception {
			Files.createFile(tempDir.resolve("mydb.mv.db"));
			Files.createDirectory(tempDir.resolve("subdir"));

			Method m = CentralCloudStorage.class.getDeclaredMethod("getH2File", String.class);
			m.setAccessible(true);
			@SuppressWarnings("unchecked")
			List<String> result = (List<String>) m.invoke(instance, tempDir.toString());

			assertEquals(1, result.size());
			assertEquals("mydb.mv.db", result.get(0));
		}

		// ------- Prefix values consistency -------

		@Test
		void testAllPrefixesAreNonNull() {
			for (IEngine.CATALOG_TYPE type : new IEngine.CATALOG_TYPE[] { IEngine.CATALOG_TYPE.DATABASE,
					IEngine.CATALOG_TYPE.STORAGE, IEngine.CATALOG_TYPE.MODEL, IEngine.CATALOG_TYPE.VECTOR,
					IEngine.CATALOG_TYPE.FUNCTION, IEngine.CATALOG_TYPE.GUARDRAIL, IEngine.CATALOG_TYPE.VENV,
					IEngine.CATALOG_TYPE.PROJECT }) {
				assertNotNull(instance.getCloudPrefixForEngine(type), "Prefix for " + type + " should not be null");
				assertNotNull(instance.getCloudEngineImageBucket(type),
						"Image bucket for " + type + " should not be null");
			}
		}

		@Test
		void testPrefixAndImageBucketDifferent() {
			for (IEngine.CATALOG_TYPE type : new IEngine.CATALOG_TYPE[] { IEngine.CATALOG_TYPE.DATABASE,
					IEngine.CATALOG_TYPE.STORAGE, IEngine.CATALOG_TYPE.MODEL, IEngine.CATALOG_TYPE.VECTOR,
					IEngine.CATALOG_TYPE.FUNCTION, IEngine.CATALOG_TYPE.GUARDRAIL, IEngine.CATALOG_TYPE.VENV,
					IEngine.CATALOG_TYPE.PROJECT }) {
				String prefix = instance.getCloudPrefixForEngine(type);
				String image = instance.getCloudEngineImageBucket(type);
				assertNotEquals(prefix, image, "Prefix and image bucket should differ for " + type);
			}
		}

		@Test
		void testAllPrefixesAreNonEmpty() {
			for (IEngine.CATALOG_TYPE type : new IEngine.CATALOG_TYPE[] { IEngine.CATALOG_TYPE.DATABASE,
					IEngine.CATALOG_TYPE.STORAGE, IEngine.CATALOG_TYPE.MODEL, IEngine.CATALOG_TYPE.VECTOR,
					IEngine.CATALOG_TYPE.FUNCTION, IEngine.CATALOG_TYPE.GUARDRAIL, IEngine.CATALOG_TYPE.VENV,
					IEngine.CATALOG_TYPE.PROJECT }) {
				String prefix = instance.getCloudPrefixForEngine(type);
				assertFalse(prefix.isEmpty(), "Prefix for " + type + " should not be empty");
			}
		}

		@Test
		void testAllImageBucketsAreNonEmpty() {
			for (IEngine.CATALOG_TYPE type : new IEngine.CATALOG_TYPE[] { IEngine.CATALOG_TYPE.DATABASE,
					IEngine.CATALOG_TYPE.STORAGE, IEngine.CATALOG_TYPE.MODEL, IEngine.CATALOG_TYPE.VECTOR,
					IEngine.CATALOG_TYPE.FUNCTION, IEngine.CATALOG_TYPE.GUARDRAIL, IEngine.CATALOG_TYPE.VENV,
					IEngine.CATALOG_TYPE.PROJECT }) {
				String bucket = instance.getCloudEngineImageBucket(type);
				assertFalse(bucket.isEmpty(), "Image bucket for " + type + " should not be empty");
			}
		}

		@Test
		void testEachCatalogTypeReturnsDifferentPrefix() {
			IEngine.CATALOG_TYPE[] types = { IEngine.CATALOG_TYPE.DATABASE, IEngine.CATALOG_TYPE.STORAGE,
					IEngine.CATALOG_TYPE.MODEL, IEngine.CATALOG_TYPE.VECTOR, IEngine.CATALOG_TYPE.FUNCTION,
					IEngine.CATALOG_TYPE.GUARDRAIL, IEngine.CATALOG_TYPE.VENV, IEngine.CATALOG_TYPE.PROJECT };

			for (int i = 0; i < types.length; i++) {
				for (int j = i + 1; j < types.length; j++) {
					String prefixI = instance.getCloudPrefixForEngine(types[i]);
					String prefixJ = instance.getCloudPrefixForEngine(types[j]);
					assertNotEquals(prefixI, prefixJ,
							"Prefixes for " + types[i] + " and " + types[j] + " should be different");
				}
			}
		}

		@Test
		void testEachCatalogTypeReturnsDifferentImageBucket() {
			IEngine.CATALOG_TYPE[] types = { IEngine.CATALOG_TYPE.DATABASE, IEngine.CATALOG_TYPE.STORAGE,
					IEngine.CATALOG_TYPE.MODEL, IEngine.CATALOG_TYPE.VECTOR, IEngine.CATALOG_TYPE.FUNCTION,
					IEngine.CATALOG_TYPE.GUARDRAIL, IEngine.CATALOG_TYPE.VENV, IEngine.CATALOG_TYPE.PROJECT };

			for (int i = 0; i < types.length; i++) {
				for (int j = i + 1; j < types.length; j++) {
					String bucketI = instance.getCloudEngineImageBucket(types[i]);
					String bucketJ = instance.getCloudEngineImageBucket(types[j]);
					assertNotEquals(bucketI, bucketJ,
							"Image buckets for " + types[i] + " and " + types[j] + " should be different");
				}
			}
		}

		// ------- catalogPulledEngine throws for null -------

		@Test
		void testCatalogPulledEngine_nullThrows() {
			assertThrows(IllegalArgumentException.class,
					() -> instance.catalogPulledEngine("alias__id", "file.smss", null));
		}

		// ------- getInsightDB -------

		@Test
		void testGetInsightDB_h2Type() throws Exception {
			Path projectDir = tempDir.resolve("projectDir");
			Files.createDirectories(projectDir);
			Files.createFile(projectDir.resolve("insights_database.mv.db"));
			Files.createFile(projectDir.resolve("other.txt"));

			IProject mockProject = mock(IProject.class);
			IRDBMSEngine mockInsightDb = mock(IRDBMSEngine.class);
			when(mockProject.getInsightDatabase()).thenReturn(mockInsightDb);
			when(mockInsightDb.getDbType()).thenReturn(RdbmsTypeEnum.H2_DB);

			Method m = CentralCloudStorage.class.getDeclaredMethod("getInsightDB", IProject.class, String.class);
			m.setAccessible(true);
			String result = (String) m.invoke(instance, mockProject, projectDir.toString());
			assertEquals("insights_database.mv.db", result);
		}

		@Test
		void testGetInsightDB_sqliteType() throws Exception {
			Path projectDir = tempDir.resolve("projectDir2");
			Files.createDirectories(projectDir);
			Files.createFile(projectDir.resolve("insights_database.sqlite"));

			IProject mockProject = mock(IProject.class);
			IRDBMSEngine mockInsightDb = mock(IRDBMSEngine.class);
			when(mockProject.getInsightDatabase()).thenReturn(mockInsightDb);
			when(mockInsightDb.getDbType()).thenReturn(RdbmsTypeEnum.SQLITE);

			Method m = CentralCloudStorage.class.getDeclaredMethod("getInsightDB", IProject.class, String.class);
			m.setAccessible(true);
			String result = (String) m.invoke(instance, mockProject, projectDir.toString());
			assertEquals("insights_database.sqlite", result);
		}

		@Test
		void testGetInsightDB_notFound_throws() throws Exception {
			Path projectDir = tempDir.resolve("projectDir3");
			Files.createDirectories(projectDir);
			Files.createFile(projectDir.resolve("other.txt"));

			IProject mockProject = mock(IProject.class);
			IRDBMSEngine mockInsightDb = mock(IRDBMSEngine.class);
			when(mockProject.getInsightDatabase()).thenReturn(mockInsightDb);
			when(mockInsightDb.getDbType()).thenReturn(RdbmsTypeEnum.SQLITE);
			when(mockProject.getProjectName()).thenReturn("TestProject");

			Method m = CentralCloudStorage.class.getDeclaredMethod("getInsightDB", IProject.class, String.class);
			m.setAccessible(true);
			try {
				m.invoke(instance, mockProject, projectDir.toString());
				fail("Expected IllegalArgumentException");
			} catch (java.lang.reflect.InvocationTargetException e) {
				assertTrue(e.getCause() instanceof IllegalArgumentException);
				assertTrue(e.getCause().getMessage().contains("TestProject"));
			}
		}

		@Test
		void testGetInsightDB_h2CaseInsensitive() throws Exception {
			Path projectDir = tempDir.resolve("projectDir4");
			Files.createDirectories(projectDir);
			Files.createFile(projectDir.resolve("Insights_Database.mv.db"));

			IProject mockProject = mock(IProject.class);
			IRDBMSEngine mockInsightDb = mock(IRDBMSEngine.class);
			when(mockProject.getInsightDatabase()).thenReturn(mockInsightDb);
			when(mockInsightDb.getDbType()).thenReturn(RdbmsTypeEnum.H2_DB);

			Method m = CentralCloudStorage.class.getDeclaredMethod("getInsightDB", IProject.class, String.class);
			m.setAccessible(true);
			String result = (String) m.invoke(instance, mockProject, projectDir.toString());
			assertEquals("Insights_Database.mv.db", result);
		}

		// ------- propertiesMigratePut -------

		@Test
		void testPropertiesMigratePut_copiesValue() throws Exception {
			Properties props = new Properties();
			AppCloudClientProperties clientProps = mock(AppCloudClientProperties.class);
			when(clientProps.get("OLD_KEY")).thenReturn("myValue");

			Method m = CentralCloudStorage.class.getDeclaredMethod("propertiesMigratePut", Properties.class,
					String.class, AppCloudClientProperties.class, String.class);
			m.setAccessible(true);
			m.invoke(null, props, "NEW_KEY", clientProps, "OLD_KEY");

			assertEquals("myValue", props.getProperty("NEW_KEY"));
		}

		@Test
		void testPropertiesMigratePut_nullValueSkips() throws Exception {
			Properties props = new Properties();
			AppCloudClientProperties clientProps = mock(AppCloudClientProperties.class);
			when(clientProps.get("OLD_KEY")).thenReturn(null);

			Method m = CentralCloudStorage.class.getDeclaredMethod("propertiesMigratePut", Properties.class,
					String.class, AppCloudClientProperties.class, String.class);
			m.setAccessible(true);
			m.invoke(null, props, "NEW_KEY", clientProps, "OLD_KEY");

			assertNull(props.getProperty("NEW_KEY"));
		}

		@Test
		void testPropertiesMigratePut_overwritesExisting() throws Exception {
			Properties props = new Properties();
			props.put("NEW_KEY", "oldValue");
			AppCloudClientProperties clientProps = mock(AppCloudClientProperties.class);
			when(clientProps.get("OLD_KEY")).thenReturn("newValue");

			Method m = CentralCloudStorage.class.getDeclaredMethod("propertiesMigratePut", Properties.class,
					String.class, AppCloudClientProperties.class, String.class);
			m.setAccessible(true);
			m.invoke(null, props, "NEW_KEY", clientProps, "OLD_KEY");

			assertEquals("newValue", props.getProperty("NEW_KEY"));
		}

		@Test
		void testPropertiesMigratePut_preservesOtherKeys() throws Exception {
			Properties props = new Properties();
			props.put("EXISTING", "keep");
			AppCloudClientProperties clientProps = mock(AppCloudClientProperties.class);
			when(clientProps.get("OLD_KEY")).thenReturn("myValue");

			Method m = CentralCloudStorage.class.getDeclaredMethod("propertiesMigratePut", Properties.class,
					String.class, AppCloudClientProperties.class, String.class);
			m.setAccessible(true);
			m.invoke(null, props, "NEW_KEY", clientProps, "OLD_KEY");

			assertEquals("keep", props.getProperty("EXISTING"));
			assertEquals("myValue", props.getProperty("NEW_KEY"));
		}

		// ------- Additional getSqlLiteFile edge cases -------

		@Test
		void testGetSqlLiteFile_onlyInsightsDatabase_kept() throws Exception {
			Files.createFile(tempDir.resolve("insights_database.sqlite"));

			Method m = CentralCloudStorage.class.getDeclaredMethod("getSqlLiteFile", String.class);
			m.setAccessible(true);
			@SuppressWarnings("unchecked")
			List<String> result = (List<String>) m.invoke(instance, tempDir.toString());

			assertEquals(1, result.size());
			assertTrue(result.contains("insights_database.sqlite"));
		}

		@Test
		void testGetSqlLiteFile_similarExtensionIgnored() throws Exception {
			Files.createFile(tempDir.resolve("mydb.sqlite"));
			Files.createFile(tempDir.resolve("not_sqlite.sqlitex"));
			Files.createFile(tempDir.resolve("fake.sqlite_bak"));

			Method m = CentralCloudStorage.class.getDeclaredMethod("getSqlLiteFile", String.class);
			m.setAccessible(true);
			@SuppressWarnings("unchecked")
			List<String> result = (List<String>) m.invoke(instance, tempDir.toString());

			assertEquals(1, result.size());
			assertEquals("mydb.sqlite", result.get(0));
		}

		// ------- Additional getH2File edge cases -------

		@Test
		void testGetH2File_onlyInsightsDatabase_kept() throws Exception {
			Files.createFile(tempDir.resolve("insights_database.mv.db"));

			Method m = CentralCloudStorage.class.getDeclaredMethod("getH2File", String.class);
			m.setAccessible(true);
			@SuppressWarnings("unchecked")
			List<String> result = (List<String>) m.invoke(instance, tempDir.toString());

			assertEquals(1, result.size());
			assertTrue(result.contains("insights_database.mv.db"));
		}

		@Test
		void testGetH2File_similarExtensionIgnored() throws Exception {
			Files.createFile(tempDir.resolve("mydb.mv.db"));
			Files.createFile(tempDir.resolve("mydb.db"));
			Files.createFile(tempDir.resolve("mydb.mv.db.bak"));

			Method m = CentralCloudStorage.class.getDeclaredMethod("getH2File", String.class);
			m.setAccessible(true);
			@SuppressWarnings("unchecked")
			List<String> result = (List<String>) m.invoke(instance, tempDir.toString());

			assertEquals(1, result.size());
			assertEquals("mydb.mv.db", result.get(0));
		}

		// ------- Default container prefix values -------

		@Test
		void testDefaultDbContainerPrefix() throws Exception {
			Field f = CentralCloudStorage.class.getDeclaredField("DB_CONTAINER_PREFIX");
			f.setAccessible(true);
			String value = (String) f.get(null);
			assertTrue(value.contains(CentralCloudStorage.DATABASE_BLOB));
		}

		@Test
		void testDefaultStorageContainerPrefix() throws Exception {
			Field f = CentralCloudStorage.class.getDeclaredField("STORAGE_CONTAINER_PREFIX");
			f.setAccessible(true);
			String value = (String) f.get(null);
			assertTrue(value.contains(CentralCloudStorage.STORAGE_BLOB));
		}

		@Test
		void testDefaultModelContainerPrefix() throws Exception {
			Field f = CentralCloudStorage.class.getDeclaredField("MODEL_CONTAINER_PREFIX");
			f.setAccessible(true);
			String value = (String) f.get(null);
			assertTrue(value.contains(CentralCloudStorage.MODEL_BLOB));
		}

		@Test
		void testDefaultVectorContainerPrefix() throws Exception {
			Field f = CentralCloudStorage.class.getDeclaredField("VECTOR_CONTAINER_PREFIX");
			f.setAccessible(true);
			String value = (String) f.get(null);
			assertTrue(value.contains(CentralCloudStorage.VECTOR_BLOB));
		}

		@Test
		void testDefaultFunctionContainerPrefix() throws Exception {
			Field f = CentralCloudStorage.class.getDeclaredField("FUNCTION_CONTAINER_PREFIX");
			f.setAccessible(true);
			String value = (String) f.get(null);
			assertTrue(value.contains(CentralCloudStorage.FUNCTION_BLOB));
		}

		@Test
		void testDefaultGuardrailContainerPrefix() throws Exception {
			Field f = CentralCloudStorage.class.getDeclaredField("GUARDRAIL_CONTAINER_PREFIX");
			f.setAccessible(true);
			String value = (String) f.get(null);
			assertTrue(value.contains(CentralCloudStorage.GUARDRAIL_BLOB));
		}

		@Test
		void testDefaultVenvContainerPrefix() throws Exception {
			Field f = CentralCloudStorage.class.getDeclaredField("VENV_CONTAINER_PREFIX");
			f.setAccessible(true);
			String value = (String) f.get(null);
			assertTrue(value.contains(CentralCloudStorage.VENV_BLOB));
		}

		@Test
		void testDefaultProjectContainerPrefix() throws Exception {
			Field f = CentralCloudStorage.class.getDeclaredField("PROJECT_CONTAINER_PREFIX");
			f.setAccessible(true);
			String value = (String) f.get(null);
			assertTrue(value.contains(CentralCloudStorage.PROJECT_BLOB));
		}

		@Test
		void testDefaultUserContainerPrefix() throws Exception {
			Field f = CentralCloudStorage.class.getDeclaredField("USER_CONTAINER_PREFIX");
			f.setAccessible(true);
			String value = (String) f.get(null);
			assertTrue(value.contains(CentralCloudStorage.USER_BLOB));
		}

		@Test
		void testDefaultRoomContainerPrefix() throws Exception {
			Field f = CentralCloudStorage.class.getDeclaredField("ROOM_CONTAINER_PREFIX");
			f.setAccessible(true);
			String value = (String) f.get(null);
			assertTrue(value.contains(CentralCloudStorage.ROOM_BLOB));
		}

		// ------- FILE_SEPARATOR -------

		@Test
		void testFileSeparatorIsNonNull() throws Exception {
			Field f = CentralCloudStorage.class.getDeclaredField("FILE_SEPARATOR");
			f.setAccessible(true);
			String value = (String) f.get(null);
			assertNotNull(value);
			assertFalse(value.isEmpty());
		}

		// ------- getCloudPrefixForEngine additional edge cases -------

		@Test
		void testGetCloudPrefixForEngine_allTypesReturnNonBlankStrings() {
			for (IEngine.CATALOG_TYPE type : IEngine.CATALOG_TYPE.values()) {
				String prefix = instance.getCloudPrefixForEngine(type);
				assertNotNull(prefix, "Prefix for " + type + " should not be null");
				assertFalse(prefix.isBlank(), "Prefix for " + type + " should not be blank");
			}
		}

		@Test
		void testGetCloudEngineImageBucket_allTypesReturnNonBlankStrings() {
			for (IEngine.CATALOG_TYPE type : IEngine.CATALOG_TYPE.values()) {
				String bucket = instance.getCloudEngineImageBucket(type);
				assertNotNull(bucket, "Image bucket for " + type + " should not be null");
				assertFalse(bucket.isBlank(), "Image bucket for " + type + " should not be blank");
			}
		}
	}

	// -------------------------------------------------------
	// Storage operation tests with mocked centralStorageEngine
	// -------------------------------------------------------

	@Nested
	@TestInstance(TestInstance.Lifecycle.PER_CLASS)
	class StorageOperationTests {

		private CentralCloudStorage instance;
		private AbstractStorageEngine mockStorageEngine;

		@BeforeAll
		void ensureEngineUtilityLoaded() {
			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(Utility::getBaseFolder).thenReturn("/test/base");
				try (MockedStatic<EngineUtility> ignored = mockStatic(EngineUtility.class)) {
					// Force EngineUtility class loading with mocked Utility
				}
			} catch (Throwable ignored) {
			}
		}

		@BeforeEach
		void setUp() throws Exception {
			Field f = Unsafe.class.getDeclaredField("theUnsafe");
			f.setAccessible(true);
			Unsafe unsafe = (Unsafe) f.get(null);
			instance = (CentralCloudStorage) unsafe.allocateInstance(CentralCloudStorage.class);

			mockStorageEngine = mock(AbstractStorageEngine.class);
			Field storageField = CentralCloudStorage.class.getDeclaredField("centralStorageEngine");
			storageField.setAccessible(true);
			storageField.set(null, mockStorageEngine);
		}

		@AfterEach
		void tearDown() throws Exception {
			Field storageField = CentralCloudStorage.class.getDeclaredField("centralStorageEngine");
			storageField.setAccessible(true);
			storageField.set(null, null);
		}

		// ------- Null engine checks -------

		@Test
		void testPushEngine_nullEngine_throws() {
			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(() -> Utility.getEngine("eng1", false)).thenReturn(null);
				assertThrows(IllegalArgumentException.class, () -> instance.pushEngine("eng1"));
			}
		}

		@Test
		void testPullEngine_alreadyLoaded_nullEngine_throws() {
			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(() -> Utility.getEngine("eng1", false)).thenReturn(null);
				assertThrows(IllegalArgumentException.class,
						() -> instance.pullEngine("eng1", IEngine.CATALOG_TYPE.DATABASE, true));
			}
		}

		@Test
		void testPushEngineFolder_nullEngine_throws() {
			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(() -> Utility.getEngine("eng1", false)).thenReturn(null);
				assertThrows(IllegalArgumentException.class,
						() -> instance.pushEngineFolder("eng1", "/local", "/storage"));
			}
		}

		@Test
		void testPullEngineFolder_nullEngine_throws() {
			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(() -> Utility.getEngine("eng1", false)).thenReturn(null);
				assertThrows(IllegalArgumentException.class,
						() -> instance.pullEngineFolder("eng1", "/local", "/storage"));
			}
		}

		@Test
		void testCopyLocalFileToEngineCloudFolder_nullEngine_throws() {
			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(() -> Utility.getEngine("eng1", false)).thenReturn(null);
				assertThrows(IllegalArgumentException.class, () -> instance.copyLocalFileToEngineCloudFolder("eng1",
						IEngine.CATALOG_TYPE.DATABASE, "/file"));
			}
		}

		@Test
		void testCopyEngineCloudFileToLocalFile_nullEngine_throws() {
			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(() -> Utility.getEngine("eng1", false)).thenReturn(null);
				assertThrows(IllegalArgumentException.class,
						() -> instance.copyEngineCloudFileToLocalFile("eng1", IEngine.CATALOG_TYPE.DATABASE, "/file"));
			}
		}

		@Test
		void testDeleteEngineCloudFile_nullEngine_throws() {
			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(() -> Utility.getEngine("eng1", false)).thenReturn(null);
				assertThrows(IllegalArgumentException.class,
						() -> instance.deleteEngineCloudFile("eng1", IEngine.CATALOG_TYPE.DATABASE, "/file"));
			}
		}

		// ------- Null database checks -------

		@Test
		void testPushLocalDatabaseFile_nullDatabase_throws() {
			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(() -> Utility.getDatabase("db1", false)).thenReturn(null);
				assertThrows(IllegalArgumentException.class,
						() -> instance.pushLocalDatabaseFile("db1", RdbmsTypeEnum.SQLITE));
			}
		}

		@Test
		void testPullLocalDatabaseFile_nullDatabase_throws() {
			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(() -> Utility.getDatabase("db1", false)).thenReturn(null);
				assertThrows(IllegalArgumentException.class,
						() -> instance.pullLocalDatabaseFile("db1", RdbmsTypeEnum.SQLITE));
			}
		}

		@Test
		void testPushOwl_nullDatabase_throws() {
			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(() -> Utility.getDatabase("db1", false)).thenReturn(null);
				assertThrows(IllegalArgumentException.class, () -> instance.pushOwl("db1"));
			}
		}

		@Test
		void testPullOwl_nullDatabase_throws() {
			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(() -> Utility.getDatabase("db1", false)).thenReturn(null);
				assertThrows(IllegalArgumentException.class, () -> instance.pullOwl("db1"));
			}
		}

		// ------- Null project checks -------

		@Test
		void testPushProject_nullProject_throws() {
			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(() -> Utility.getProject("proj1", false)).thenReturn(null);
				assertThrows(IllegalArgumentException.class, () -> instance.pushProject("proj1"));
			}
		}

		@Test
		void testPullProject_alreadyLoaded_nullProject_throws() {
			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(() -> Utility.getProject("proj1", false)).thenReturn(null);
				assertThrows(IllegalArgumentException.class, () -> instance.pullProject("proj1", true));
			}
		}

		@Test
		void testPushInsight_nullProject_throws() {
			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(() -> Utility.getProject("proj1")).thenReturn(null);
				assertThrows(IllegalArgumentException.class, () -> instance.pushInsight("proj1", "insight1"));
			}
		}

		@Test
		void testPullInsight_nullProject_throws() {
			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(() -> Utility.getProject("proj1")).thenReturn(null);
				assertThrows(IllegalArgumentException.class, () -> instance.pullInsight("proj1", "insight1"));
			}
		}

		@Test
		void testPushInsightImage_nullProject_throws() {
			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(() -> Utility.getProject("proj1", false)).thenReturn(null);
				assertThrows(IllegalArgumentException.class,
						() -> instance.pushInsightImage("proj1", "i1", "old.png", "new.png"));
			}
		}

		@Test
		void testPullInsightsDB_nullProject_throws() {
			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(() -> Utility.getProject("proj1", false)).thenReturn(null);
				assertThrows(IllegalArgumentException.class, () -> instance.pullInsightsDB("proj1"));
			}
		}

		@Test
		void testPushInsightDB_nullProject_throws() {
			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(() -> Utility.getProject("proj1", false)).thenReturn(null);
				assertThrows(IllegalArgumentException.class, () -> instance.pushInsightDB("proj1"));
			}
		}

		@Test
		void testPushProjectFolder_nullProject_throws() {
			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(() -> Utility.getProject("proj1", false)).thenReturn(null);
				assertThrows(IllegalArgumentException.class,
						() -> instance.pushProjectFolder("proj1", "/local", "/storage"));
			}
		}

		@Test
		void testPullProjectFolder_nullProject_throws() {
			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(() -> Utility.getProject("proj1", false)).thenReturn(null);
				assertThrows(IllegalArgumentException.class,
						() -> instance.pullProjectFolder("proj1", "/local", "/storage"));
			}
		}

		@Test
		void testPullUserAsset_alreadyLoaded_nullProject_throws() {
			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(() -> Utility.getUserAssetProject("proj1")).thenReturn(null);
				assertThrows(IllegalArgumentException.class, () -> instance.pullUserAsset("proj1", true));
			}
		}

		@Test
		void testPushUserAsset_nullProject_throws() {
			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(() -> Utility.getUserAssetProject("proj1")).thenReturn(null);
				assertThrows(IllegalArgumentException.class, () -> instance.pushUserAsset("proj1"));
			}
		}

		// ------- DbType validation -------

		@Test
		void testPushLocalDatabaseFile_mysql_throws() {
			assertThrows(IllegalArgumentException.class,
					() -> instance.pushLocalDatabaseFile("db1", RdbmsTypeEnum.MYSQL));
		}

		@Test
		void testPushLocalDatabaseFile_postgres_throws() {
			assertThrows(IllegalArgumentException.class,
					() -> instance.pushLocalDatabaseFile("db1", RdbmsTypeEnum.POSTGRES));
		}

		@Test
		void testPullLocalDatabaseFile_mysql_throws() {
			assertThrows(IllegalArgumentException.class,
					() -> instance.pullLocalDatabaseFile("db1", RdbmsTypeEnum.MYSQL));
		}

		@Test
		void testPullLocalDatabaseFile_oracle_throws() {
			assertThrows(IllegalArgumentException.class,
					() -> instance.pullLocalDatabaseFile("db1", RdbmsTypeEnum.ORACLE));
		}

		// ------- deleteEngineAndProjectImage -------

		@Test
		void testDeleteEngineAndProjectImage_database() throws Exception {
			instance.deleteEngineAndProjectImage(IEngine.CATALOG_TYPE.DATABASE, "image.png");
			verify(mockStorageEngine).deleteFromStorage(CentralCloudStorage.DB_IMAGES_BLOB + "/image.png");
		}

		@Test
		void testDeleteEngineAndProjectImage_model() throws Exception {
			instance.deleteEngineAndProjectImage(IEngine.CATALOG_TYPE.MODEL, "thumb.jpg");
			verify(mockStorageEngine).deleteFromStorage(CentralCloudStorage.MODEL_IMAGES_BLOB + "/thumb.jpg");
		}

		@Test
		void testDeleteEngineAndProjectImage_project() throws Exception {
			instance.deleteEngineAndProjectImage(IEngine.CATALOG_TYPE.PROJECT, "proj.png");
			verify(mockStorageEngine).deleteFromStorage(CentralCloudStorage.PROJECT_IMAGES_BLOB + "/proj.png");
		}

		// ------- listAllContainersByBucket -------

		@Test
		void testListAllContainersByBucket_coversEveryBucket() throws Exception {
			when(mockStorageEngine.list(anyString())).thenReturn(Arrays.asList("item1"));

			Map<String, List<String>> result = instance.listAllContainersByBucket();

			assertNotNull(result);
			assertEquals(7, result.size());
			assertTrue(result.containsKey(CentralCloudStorage.DATABASE_BLOB));
			assertTrue(result.containsKey(CentralCloudStorage.STORAGE_BLOB));
			assertTrue(result.containsKey(CentralCloudStorage.MODEL_BLOB));
			assertTrue(result.containsKey(CentralCloudStorage.VECTOR_BLOB));
			assertTrue(result.containsKey(CentralCloudStorage.FUNCTION_BLOB));
			assertTrue(result.containsKey(CentralCloudStorage.VENV_BLOB));
			assertTrue(result.containsKey(CentralCloudStorage.PROJECT_BLOB));
			verify(mockStorageEngine, times(7)).list(anyString());
		}

		@Test
		void testListAllContainersByBucket_returnsEachBucketsFiles() throws Exception {
			when(mockStorageEngine.list(anyString())).thenReturn(Arrays.asList("f1", "f2"));

			Map<String, List<String>> result = instance.listAllContainersByBucket();

			assertEquals(7, result.size());
			for (List<String> files : result.values()) {
				assertEquals(2, files.size());
			}
			verify(mockStorageEngine, times(7)).list(anyString());
		}

		// ------- deleteEngineAndProjectImageById -------

		@Test
		void testDeleteEngineAndProjectImageById_matchingFiles(@TempDir Path testDir) throws Exception {
			Files.createFile(testDir.resolve("eng123_img1.png"));
			Files.createFile(testDir.resolve("eng123_img2.jpg"));
			Files.createFile(testDir.resolve("other.png"));

			try (MockedStatic<EngineUtility> euMock = mockStatic(EngineUtility.class)) {
				euMock.when(() -> EngineUtility.getLocalEngineImageDirectory(IEngine.CATALOG_TYPE.DATABASE))
						.thenReturn(testDir.toString());

				instance.deleteEngineAndProjectImageById(IEngine.CATALOG_TYPE.DATABASE, "eng123");

				verify(mockStorageEngine, times(2)).deleteFromStorage(anyString());
				assertFalse(Files.exists(testDir.resolve("eng123_img1.png")));
				assertFalse(Files.exists(testDir.resolve("eng123_img2.jpg")));
				assertTrue(Files.exists(testDir.resolve("other.png")));
			}
		}

		@Test
		void testDeleteEngineAndProjectImageById_noMatchingFiles(@TempDir Path testDir) throws Exception {
			Files.createFile(testDir.resolve("other.png"));

			try (MockedStatic<EngineUtility> euMock = mockStatic(EngineUtility.class)) {
				euMock.when(() -> EngineUtility.getLocalEngineImageDirectory(IEngine.CATALOG_TYPE.DATABASE))
						.thenReturn(testDir.toString());

				instance.deleteEngineAndProjectImageById(IEngine.CATALOG_TYPE.DATABASE, "eng123");

				verify(mockStorageEngine, never()).deleteFromStorage(anyString());
				assertTrue(Files.exists(testDir.resolve("other.png")));
			}
		}

		// ------- Room methods -------

		@Test
		void testPullRoomFolderFromCloud() throws Exception {
			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(Utility::getBaseFolder).thenReturn("/base");
				instance.pullRoomFolderFromCloud("room1");
				// a copy, not a sync - a sync would delete local files missing on the
				// cloud side and wipe in-flight session state
				verify(mockStorageEngine).copyToLocal(contains("room1"), contains("room1"));
			}
		}

		@Test
		void testPushRoomFolderToCloud_withFiles() throws Exception {
			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(Utility::getBaseFolder).thenReturn("/base");
				utilMock.when(() -> Utility.folderIsNotEmpty(anyString())).thenReturn(true);

				instance.pushRoomFolderToCloud("room1");

				verify(mockStorageEngine).syncLocalToStorage(contains("room1"), contains("room1"),
						(Map<String, Object>) isNull());
			}
		}

		@Test
		void testPushRoomFolderToCloud_noFiles() throws Exception {
			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(Utility::getBaseFolder).thenReturn("/base");
				utilMock.when(() -> Utility.folderHasAnyFiles(anyString())).thenReturn(false);

				instance.pushRoomFolderToCloud("room1");

				verifyNoInteractions(mockStorageEngine);
			}
		}

		// ------- pushEngineSmss / pullEngineSmss happy path -------

		@Test
		void testPushEngineSmss_withType() throws Exception {
			ReentrantLock lock = new ReentrantLock();
			try (MockedStatic<SecurityEngineUtils> secMock = mockStatic(SecurityEngineUtils.class);
					MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class);
					MockedStatic<EngineUtility> euMock = mockStatic(EngineUtility.class);
					MockedStatic<Utility> utilMock = mockStatic(Utility.class);
					MockedStatic<EngineSyncUtility> syncMock = mockStatic(EngineSyncUtility.class)) {

				secMock.when(() -> SecurityEngineUtils.engineExists("eng1")).thenReturn(true);
				secMock.when(() -> SecurityEngineUtils.getEngineAliasForId("eng1")).thenReturn("MyEngine");
				smssMock.when(() -> SmssUtilities.getUniqueName("MyEngine", "eng1")).thenReturn("MyEngine__eng1");
				euMock.when(() -> EngineUtility.getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE.DATABASE))
						.thenReturn("/base/db");
				utilMock.when(() -> Utility.normalizePath(anyString())).thenAnswer(inv -> inv.getArgument(0));
				syncMock.when(() -> EngineSyncUtility.getEngineLock("eng1")).thenReturn(lock);

				instance.pushEngineSmss("eng1", IEngine.CATALOG_TYPE.DATABASE);

				verify(mockStorageEngine).copyToStorage(anyString(),
						contains("eng1" + CentralCloudStorage.SMSS_POSTFIX), (Map<String, Object>) isNull());
			}
		}

		@Test
		void testPullEngineSmss_withType() throws Exception {
			ReentrantLock lock = new ReentrantLock();
			try (MockedStatic<SecurityEngineUtils> secMock = mockStatic(SecurityEngineUtils.class);
					MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class);
					MockedStatic<EngineSyncUtility> syncMock = mockStatic(EngineSyncUtility.class)) {

				secMock.when(() -> SecurityEngineUtils.engineExists("eng1")).thenReturn(true);
				secMock.when(() -> SecurityEngineUtils.getEngineAliasForId("eng1")).thenReturn("MyEngine");
				smssMock.when(() -> SmssUtilities.getUniqueName("MyEngine", "eng1")).thenReturn("MyEngine__eng1");
				syncMock.when(() -> EngineSyncUtility.getEngineLock("eng1")).thenReturn(lock);

				instance.pullEngineSmss("eng1", IEngine.CATALOG_TYPE.DATABASE);

				verify(mockStorageEngine).copyToLocal(contains("eng1" + CentralCloudStorage.SMSS_POSTFIX), anyString());
			}
		}

		// ------- pushProjectSmss / pullProjectSmss happy path -------

		@Test
		void testPushProjectSmss() throws Exception {
			ReentrantLock lock = new ReentrantLock();
			try (MockedStatic<SecurityProjectUtils> secMock = mockStatic(SecurityProjectUtils.class);
					MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class);
					MockedStatic<Utility> utilMock = mockStatic(Utility.class);
					MockedStatic<ProjectSyncUtility> syncMock = mockStatic(ProjectSyncUtility.class)) {

				secMock.when(() -> SecurityProjectUtils.projectExists("proj1")).thenReturn(true);
				secMock.when(() -> SecurityProjectUtils.getProjectAliasForId("proj1")).thenReturn("MyProject");
				smssMock.when(() -> SmssUtilities.getUniqueName("MyProject", "proj1")).thenReturn("MyProject__proj1");
				utilMock.when(() -> Utility.normalizePath(anyString())).thenAnswer(inv -> inv.getArgument(0));
				syncMock.when(() -> ProjectSyncUtility.getProjectLock("proj1")).thenReturn(lock);

				instance.pushProjectSmss("proj1");

				verify(mockStorageEngine).copyToStorage(anyString(),
						contains("proj1" + CentralCloudStorage.SMSS_POSTFIX), (Map<String, Object>) isNull());
			}
		}

		@Test
		void testPullProjectSmss() throws Exception {
			ReentrantLock lock = new ReentrantLock();
			try (MockedStatic<SecurityProjectUtils> secMock = mockStatic(SecurityProjectUtils.class);
					MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class);
					MockedStatic<ProjectSyncUtility> syncMock = mockStatic(ProjectSyncUtility.class)) {

				secMock.when(() -> SecurityProjectUtils.projectExists("proj1")).thenReturn(true);
				secMock.when(() -> SecurityProjectUtils.getProjectAliasForId("proj1")).thenReturn("MyProject");
				smssMock.when(() -> SmssUtilities.getUniqueName("MyProject", "proj1")).thenReturn("MyProject__proj1");
				syncMock.when(() -> ProjectSyncUtility.getProjectLock("proj1")).thenReturn(lock);

				instance.pullProjectSmss("proj1");

				verify(mockStorageEngine).copyToLocal(contains("proj1" + CentralCloudStorage.SMSS_POSTFIX),
						anyString());
			}
		}

		// ------- deleteEngine / deleteProject happy path -------

		@Test
		void testDeleteEngine_withType(@TempDir Path testDir) throws Exception {
			try (MockedStatic<EngineUtility> euMock = mockStatic(EngineUtility.class)) {
				euMock.when(() -> EngineUtility.getLocalEngineImageDirectory(IEngine.CATALOG_TYPE.DATABASE))
						.thenReturn(testDir.toString());

				instance.deleteEngine("eng1", IEngine.CATALOG_TYPE.DATABASE);

				verify(mockStorageEngine, times(2)).deleteFolderFromStorage(contains("eng1"));
				verify(mockStorageEngine).deleteFolderFromStorage(
						argThat(s -> s.contains("eng1") && s.contains(CentralCloudStorage.SMSS_POSTFIX)));
			}
		}

		@Test
		void testDeleteEngine_singleArg(@TempDir Path testDir) throws Exception {
			try (MockedStatic<SecurityEngineUtils> secMock = mockStatic(SecurityEngineUtils.class);
					MockedStatic<EngineUtility> euMock = mockStatic(EngineUtility.class)) {

				secMock.when(() -> SecurityEngineUtils.getEngineTypeAndSubtype("eng1"))
						.thenReturn(new Object[] { IEngine.CATALOG_TYPE.MODEL, null });
				euMock.when(() -> EngineUtility.getLocalEngineImageDirectory(IEngine.CATALOG_TYPE.MODEL))
						.thenReturn(testDir.toString());

				instance.deleteEngine("eng1");

				verify(mockStorageEngine, times(2)).deleteFolderFromStorage(anyString());
			}
		}

		@Test
		void testDeleteProject(@TempDir Path testDir) throws Exception {
			try (MockedStatic<EngineUtility> euMock = mockStatic(EngineUtility.class)) {
				euMock.when(() -> EngineUtility.getLocalEngineImageDirectory(IEngine.CATALOG_TYPE.PROJECT))
						.thenReturn(testDir.toString());

				instance.deleteProject("proj1");

				verify(mockStorageEngine, times(2)).deleteFolderFromStorage(contains("proj1"));
				verify(mockStorageEngine).deleteFolderFromStorage(
						argThat(s -> s.contains("proj1") && s.contains(CentralCloudStorage.SMSS_POSTFIX)));
			}
		}

		// ------- Image operations happy path -------

		@Test
		void testPushEngineAndProjectImage() throws Exception {
			try (MockedStatic<EngineUtility> euMock = mockStatic(EngineUtility.class)) {
				euMock.when(() -> EngineUtility.getLocalEngineImageDirectory(IEngine.CATALOG_TYPE.MODEL))
						.thenReturn("/images/model");

				instance.pushEngineAndProjectImage(IEngine.CATALOG_TYPE.MODEL, "img.png");

				verify(mockStorageEngine).copyToStorage(eq("/images/model/img.png"),
						eq(CentralCloudStorage.MODEL_IMAGES_BLOB), (Map<String, Object>) isNull());
			}
		}

		@Test
		void testPullEngineAndProjectImageFolder(@TempDir Path testDir) throws Exception {
			Path imageDir = testDir.resolve("images");
			Files.createDirectories(imageDir);
			try (MockedStatic<EngineUtility> euMock = mockStatic(EngineUtility.class)) {
				euMock.when(() -> EngineUtility.getLocalEngineImageDirectory(IEngine.CATALOG_TYPE.VECTOR))
						.thenReturn(imageDir.toString());

				instance.pullEngineAndProjectImageFolder(IEngine.CATALOG_TYPE.VECTOR);

				verify(mockStorageEngine).copyToLocal(eq(CentralCloudStorage.VECTOR_IMAGES_BLOB),
						eq(imageDir.toString()));
			}
		}

		// ------- pushEngine happy path -------

		@Test
		void testPushEngine_databaseWithoutPrimaryFileLocks_isClosedAndReopened(@TempDir Path testDir) throws Exception {
			IEngine mockEngine = mock(IEngine.class);
			when(mockEngine.getCatalogType()).thenReturn(IEngine.CATALOG_TYPE.DATABASE);
			when(mockEngine.holdsFileLocks()).thenReturn(false);
			ReentrantLock lock = new ReentrantLock();

			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
					MockedStatic<SecurityEngineUtils> secMock = mockStatic(SecurityEngineUtils.class);
					MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class);
					MockedStatic<EngineUtility> euMock = mockStatic(EngineUtility.class);
					MockedStatic<EngineSyncUtility> syncMock = mockStatic(EngineSyncUtility.class);
					MockedStatic<ClusterUtil> clusterMock = mockStatic(ClusterUtil.class);
					MockedStatic<DIHelper> diMock = mockStatic(DIHelper.class)) {

				utilMock.when(() -> Utility.getEngine("eng1", false)).thenReturn(mockEngine);
				utilMock.when(() -> Utility.normalizePath(anyString())).thenAnswer(inv -> inv.getArgument(0));
				secMock.when(() -> SecurityEngineUtils.engineExists("eng1")).thenReturn(true);
				secMock.when(() -> SecurityEngineUtils.getEngineAliasForId("eng1")).thenReturn("MyEngine");
				smssMock.when(() -> SmssUtilities.getUniqueName("MyEngine", "eng1")).thenReturn("MyEngine__eng1");
				euMock.when(() -> EngineUtility.getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE.DATABASE))
						.thenReturn(testDir.toString());
				syncMock.when(() -> EngineSyncUtility.getEngineLock("eng1")).thenReturn(lock);
				DIHelper mockDI = mock(DIHelper.class);
				diMock.when(DIHelper::getInstance).thenReturn(mockDI);
				when(mockDI.getEngineProperty(Constants.ENGINES)).thenReturn("eng1");
				when(mockDI.getProjectProperty(Constants.PROJECTS)).thenReturn("proj1");

				instance.pushEngine("eng1");

				InOrder closeBeforeSync = inOrder(mockEngine, mockStorageEngine);
				closeBeforeSync.verify(mockEngine).close();
				closeBeforeSync.verify(mockStorageEngine).syncLocalToStorage(anyString(), anyString(), any());
				verify(mockStorageEngine).copyToStorage(argThat(s -> s.contains("MyEngine__eng1.smss")),
						argThat(s -> s.contains(CentralCloudStorage.SMSS_POSTFIX)), any());
				utilMock.verify(() -> Utility.getEngine("eng1", false), times(2));
				assertFalse(lock.isLocked());
			}
		}

		@Test
		void testPushEngine_nonDatabaseWithoutFileLocks_staysOpen(@TempDir Path testDir) throws Exception {
			IEngine mockEngine = mock(IEngine.class);
			when(mockEngine.getCatalogType()).thenReturn(IEngine.CATALOG_TYPE.MODEL);
			when(mockEngine.holdsFileLocks()).thenReturn(false);
			ReentrantLock lock = new ReentrantLock();

			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
					MockedStatic<SecurityEngineUtils> secMock = mockStatic(SecurityEngineUtils.class);
					MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class);
					MockedStatic<EngineUtility> euMock = mockStatic(EngineUtility.class);
					MockedStatic<EngineSyncUtility> syncMock = mockStatic(EngineSyncUtility.class);
					MockedStatic<ClusterUtil> clusterMock = mockStatic(ClusterUtil.class)) {

				utilMock.when(() -> Utility.getEngine("eng1", false)).thenReturn(mockEngine);
				utilMock.when(() -> Utility.normalizePath(anyString())).thenAnswer(inv -> inv.getArgument(0));
				secMock.when(() -> SecurityEngineUtils.getEngineAliasForId("eng1")).thenReturn("MyEngine");
				smssMock.when(() -> SmssUtilities.getUniqueName("MyEngine", "eng1")).thenReturn("MyEngine__eng1");
				euMock.when(() -> EngineUtility.getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE.MODEL))
						.thenReturn(testDir.toString());
				syncMock.when(() -> EngineSyncUtility.getEngineLock("eng1")).thenReturn(lock);

				instance.pushEngine("eng1");

				verify(mockEngine, never()).close();
				utilMock.verify(() -> Utility.getEngine("eng1", false), times(1));
				assertFalse(lock.isLocked());
			}
		}

		@Test
		void testPushEngine_databaseReopensAndUnlocksWhenTransferFails(@TempDir Path testDir) throws Exception {
			IEngine mockEngine = mock(IEngine.class);
			when(mockEngine.getCatalogType()).thenReturn(IEngine.CATALOG_TYPE.DATABASE);
			when(mockEngine.holdsFileLocks()).thenReturn(false);
			when(mockStorageEngine.syncLocalToStorage(anyString(), anyString(), isNull()))
					.thenThrow(new IOException("simulated transfer failure"));
			ReentrantLock lock = new ReentrantLock();

			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
					MockedStatic<SecurityEngineUtils> secMock = mockStatic(SecurityEngineUtils.class);
					MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class);
					MockedStatic<EngineUtility> euMock = mockStatic(EngineUtility.class);
					MockedStatic<EngineSyncUtility> syncMock = mockStatic(EngineSyncUtility.class);
					MockedStatic<ClusterUtil> clusterMock = mockStatic(ClusterUtil.class);
					MockedStatic<DIHelper> diMock = mockStatic(DIHelper.class)) {

				utilMock.when(() -> Utility.getEngine("eng1", false)).thenReturn(mockEngine);
				utilMock.when(() -> Utility.normalizePath(anyString())).thenAnswer(inv -> inv.getArgument(0));
				secMock.when(() -> SecurityEngineUtils.getEngineAliasForId("eng1")).thenReturn("MyEngine");
				smssMock.when(() -> SmssUtilities.getUniqueName("MyEngine", "eng1")).thenReturn("MyEngine__eng1");
				euMock.when(() -> EngineUtility.getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE.DATABASE))
						.thenReturn(testDir.toString());
				syncMock.when(() -> EngineSyncUtility.getEngineLock("eng1")).thenReturn(lock);
				DIHelper mockDI = mock(DIHelper.class);
				diMock.when(DIHelper::getInstance).thenReturn(mockDI);
				when(mockDI.getEngineProperty(Constants.ENGINES)).thenReturn("eng1");
				when(mockDI.getProjectProperty(Constants.PROJECTS)).thenReturn("proj1");

				assertThrows(IOException.class, () -> instance.pushEngine("eng1"));

				verify(mockEngine).close();
				utilMock.verify(() -> Utility.getEngine("eng1", false), times(2));
				assertFalse(lock.isLocked());
			}
		}

		@Test
		void testPushEngine_withFileLocks(@TempDir Path testDir) throws Exception {
			IEngine mockEngine = mock(IEngine.class);
			when(mockEngine.getCatalogType()).thenReturn(IEngine.CATALOG_TYPE.MODEL);
			when(mockEngine.holdsFileLocks()).thenReturn(true);
			ReentrantLock lock = new ReentrantLock();

			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
					MockedStatic<SecurityEngineUtils> secMock = mockStatic(SecurityEngineUtils.class);
					MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class);
					MockedStatic<EngineUtility> euMock = mockStatic(EngineUtility.class);
					MockedStatic<EngineSyncUtility> syncMock = mockStatic(EngineSyncUtility.class);
					MockedStatic<ClusterUtil> clusterMock = mockStatic(ClusterUtil.class);
					MockedStatic<DIHelper> diMock = mockStatic(DIHelper.class)) {

				utilMock.when(() -> Utility.getEngine("eng1", false)).thenReturn(mockEngine);
				utilMock.when(() -> Utility.normalizePath(anyString())).thenAnswer(inv -> inv.getArgument(0));
				secMock.when(() -> SecurityEngineUtils.engineExists("eng1")).thenReturn(true);
				secMock.when(() -> SecurityEngineUtils.getEngineAliasForId("eng1")).thenReturn("Eng");
				smssMock.when(() -> SmssUtilities.getUniqueName("Eng", "eng1")).thenReturn("Eng__eng1");
				euMock.when(() -> EngineUtility.getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE.MODEL))
						.thenReturn(testDir.toString());
				syncMock.when(() -> EngineSyncUtility.getEngineLock("eng1")).thenReturn(lock);
				DIHelper mockDI = mock(DIHelper.class);
				diMock.when(DIHelper::getInstance).thenReturn(mockDI);
				when(mockDI.getEngineProperty(Constants.ENGINES)).thenReturn("eng1");
				when(mockDI.getProjectProperty(Constants.PROJECTS)).thenReturn("proj1");

				instance.pushEngine("eng1");

				verify(mockDI).removeEngineProperty("eng1");
				verify(mockEngine).close();
				verify(mockStorageEngine).syncLocalToStorage(anyString(), anyString(), any());
				verify(mockStorageEngine).copyToStorage(anyString(), anyString(), any());
			}
		}

		// ------- pullEngine happy path -------

		@Test
		void testPullEngine_notLoaded_withType(@TempDir Path testDir) throws Exception {
			ReentrantLock lock = new ReentrantLock();

			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
					MockedStatic<SecurityEngineUtils> secMock = mockStatic(SecurityEngineUtils.class);
					MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class);
					MockedStatic<EngineUtility> euMock = mockStatic(EngineUtility.class);
					MockedStatic<EngineSyncUtility> syncMock = mockStatic(EngineSyncUtility.class);
					MockedStatic<SMSSWebWatcher> watcherMock = mockStatic(SMSSWebWatcher.class)) {

				utilMock.when(() -> Utility.normalizePath(anyString())).thenAnswer(inv -> inv.getArgument(0));
				utilMock.when(() -> Utility.cleanLogString(anyString())).thenAnswer(inv -> inv.getArgument(0));
				secMock.when(() -> SecurityEngineUtils.engineExists("eng1")).thenReturn(true);
				secMock.when(() -> SecurityEngineUtils.getEngineAliasForId("eng1")).thenReturn("MyEng");
				smssMock.when(() -> SmssUtilities.getUniqueName("MyEng", "eng1")).thenReturn("MyEng__eng1");
				euMock.when(() -> EngineUtility.getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE.DATABASE))
						.thenReturn(testDir.toString());
				syncMock.when(() -> EngineSyncUtility.getEngineLock("eng1")).thenReturn(lock);

				instance.pullEngine("eng1", IEngine.CATALOG_TYPE.DATABASE, false);

				verify(mockStorageEngine).syncStorageToLocal(anyString(), anyString());
				verify(mockStorageEngine).copyToLocal(argThat(s -> s.contains(CentralCloudStorage.SMSS_POSTFIX)),
						eq(testDir.toString()));
				watcherMock.verify(() -> SMSSWebWatcher.catalogEngine(eq("MyEng__eng1.smss"), any()));
			}
		}

		@Test
		void testPullEngine_alreadyLoaded(@TempDir Path testDir) throws Exception {
			IEngine mockEngine = mock(IEngine.class);
			when(mockEngine.getCatalogType()).thenReturn(IEngine.CATALOG_TYPE.STORAGE);
			ReentrantLock lock = new ReentrantLock();

			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
					MockedStatic<SecurityEngineUtils> secMock = mockStatic(SecurityEngineUtils.class);
					MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class);
					MockedStatic<EngineUtility> euMock = mockStatic(EngineUtility.class);
					MockedStatic<EngineSyncUtility> syncMock = mockStatic(EngineSyncUtility.class);
					MockedStatic<DIHelper> diMock = mockStatic(DIHelper.class)) {

				utilMock.when(() -> Utility.getEngine("eng1", false)).thenReturn(mockEngine);
				utilMock.when(() -> Utility.getEngine("eng1", IEngine.CATALOG_TYPE.STORAGE, false))
						.thenReturn(mockEngine);
				utilMock.when(() -> Utility.normalizePath(anyString())).thenAnswer(inv -> inv.getArgument(0));
				utilMock.when(() -> Utility.cleanLogString(anyString())).thenAnswer(inv -> inv.getArgument(0));
				secMock.when(() -> SecurityEngineUtils.engineExists("eng1")).thenReturn(true);
				secMock.when(() -> SecurityEngineUtils.getEngineAliasForId("eng1")).thenReturn("Eng");
				smssMock.when(() -> SmssUtilities.getUniqueName("Eng", "eng1")).thenReturn("Eng__eng1");
				euMock.when(() -> EngineUtility.getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE.STORAGE))
						.thenReturn(testDir.toString());
				syncMock.when(() -> EngineSyncUtility.getEngineLock("eng1")).thenReturn(lock);
				DIHelper mockDI = mock(DIHelper.class);
				diMock.when(DIHelper::getInstance).thenReturn(mockDI);
				when(mockDI.getEngineProperty(Constants.ENGINES)).thenReturn("eng1");
				when(mockDI.getProjectProperty(Constants.PROJECTS)).thenReturn("proj1");

				instance.pullEngine("eng1", IEngine.CATALOG_TYPE.STORAGE, true);

				verify(mockDI).removeEngineProperty("eng1");
				verify(mockEngine).close();
				verify(mockStorageEngine).syncStorageToLocal(anyString(), anyString());
				verify(mockStorageEngine).copyToLocal(anyString(), anyString());
			}
		}

		@Test
		void testPullEngine_notLoaded_nullType() throws Exception {
			ReentrantLock lock = new ReentrantLock();

			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
					MockedStatic<SecurityEngineUtils> secMock = mockStatic(SecurityEngineUtils.class);
					MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class);
					MockedStatic<EngineUtility> euMock = mockStatic(EngineUtility.class);
					MockedStatic<EngineSyncUtility> syncMock = mockStatic(EngineSyncUtility.class);
					MockedStatic<SMSSNoInitEngineWatcher> watcherMock = mockStatic(SMSSNoInitEngineWatcher.class)) {

				secMock.when(() -> SecurityEngineUtils.getEngineTypeAndSubtype("eng1"))
						.thenReturn(new Object[] { IEngine.CATALOG_TYPE.MODEL, null });
				secMock.when(() -> SecurityEngineUtils.engineExists("eng1")).thenReturn(true);
				secMock.when(() -> SecurityEngineUtils.getEngineAliasForId("eng1")).thenReturn("Eng");
				smssMock.when(() -> SmssUtilities.getUniqueName("Eng", "eng1")).thenReturn("Eng__eng1");
				euMock.when(() -> EngineUtility.getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE.MODEL))
						.thenReturn("/tmp/model");
				utilMock.when(() -> Utility.normalizePath(anyString())).thenAnswer(inv -> inv.getArgument(0));
				utilMock.when(() -> Utility.cleanLogString(anyString())).thenAnswer(inv -> inv.getArgument(0));
				syncMock.when(() -> EngineSyncUtility.getEngineLock("eng1")).thenReturn(lock);

				instance.pullEngine("eng1", null, false);

				verify(mockStorageEngine).syncStorageToLocal(anyString(), anyString());
				watcherMock.verify(() -> SMSSNoInitEngineWatcher.catalogEngine(anyString(), any()));
			}
		}

		// ------- Delegation tests -------

		@Test
		void testPullEngine_singleArg_delegates() throws Exception {
			ReentrantLock lock = new ReentrantLock();

			try (MockedStatic<SecurityEngineUtils> secMock = mockStatic(SecurityEngineUtils.class);
					MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class);
					MockedStatic<EngineUtility> euMock = mockStatic(EngineUtility.class);
					MockedStatic<EngineSyncUtility> syncMock = mockStatic(EngineSyncUtility.class);
					MockedStatic<Utility> utilMock = mockStatic(Utility.class);
					MockedStatic<SMSSNoInitEngineWatcher> watcherMock = mockStatic(SMSSNoInitEngineWatcher.class)) {

				secMock.when(() -> SecurityEngineUtils.getEngineTypeAndSubtype("eng1"))
						.thenReturn(new Object[] { IEngine.CATALOG_TYPE.VECTOR, null });
				secMock.when(() -> SecurityEngineUtils.engineExists("eng1")).thenReturn(true);
				secMock.when(() -> SecurityEngineUtils.getEngineAliasForId("eng1")).thenReturn("Eng");
				smssMock.when(() -> SmssUtilities.getUniqueName("Eng", "eng1")).thenReturn("Eng__eng1");
				euMock.when(() -> EngineUtility.getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE.VECTOR))
						.thenReturn("/tmp/vector");
				utilMock.when(() -> Utility.normalizePath(anyString())).thenAnswer(inv -> inv.getArgument(0));
				utilMock.when(() -> Utility.cleanLogString(anyString())).thenAnswer(inv -> inv.getArgument(0));
				syncMock.when(() -> EngineSyncUtility.getEngineLock("eng1")).thenReturn(lock);

				instance.pullEngine("eng1");

				verify(mockStorageEngine).syncStorageToLocal(anyString(), anyString());
			}
		}

		@Test
		void testPushEngineSmss_singleArg_delegates() throws Exception {
			ReentrantLock lock = new ReentrantLock();
			try (MockedStatic<SecurityEngineUtils> secMock = mockStatic(SecurityEngineUtils.class);
					MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class);
					MockedStatic<EngineUtility> euMock = mockStatic(EngineUtility.class);
					MockedStatic<Utility> utilMock = mockStatic(Utility.class);
					MockedStatic<EngineSyncUtility> syncMock = mockStatic(EngineSyncUtility.class)) {

				secMock.when(() -> SecurityEngineUtils.getEngineTypeAndSubtype("eng1"))
						.thenReturn(new Object[] { IEngine.CATALOG_TYPE.DATABASE, null });
				secMock.when(() -> SecurityEngineUtils.engineExists("eng1")).thenReturn(true);
				secMock.when(() -> SecurityEngineUtils.getEngineAliasForId("eng1")).thenReturn("E");
				smssMock.when(() -> SmssUtilities.getUniqueName("E", "eng1")).thenReturn("E__eng1");
				euMock.when(() -> EngineUtility.getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE.DATABASE))
						.thenReturn("/base/db");
				utilMock.when(() -> Utility.normalizePath(anyString())).thenAnswer(inv -> inv.getArgument(0));
				syncMock.when(() -> EngineSyncUtility.getEngineLock("eng1")).thenReturn(lock);

				instance.pushEngineSmss("eng1");

				verify(mockStorageEngine).copyToStorage(anyString(), contains(CentralCloudStorage.SMSS_POSTFIX),
						(Map<String, Object>) isNull());
			}
		}

		@Test
		void testPullEngineSmss_singleArg_delegates() throws Exception {
			ReentrantLock lock = new ReentrantLock();
			try (MockedStatic<SecurityEngineUtils> secMock = mockStatic(SecurityEngineUtils.class);
					MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class);
					MockedStatic<EngineSyncUtility> syncMock = mockStatic(EngineSyncUtility.class)) {

				secMock.when(() -> SecurityEngineUtils.getEngineTypeAndSubtype("eng1"))
						.thenReturn(new Object[] { IEngine.CATALOG_TYPE.STORAGE, null });
				secMock.when(() -> SecurityEngineUtils.engineExists("eng1")).thenReturn(true);
				secMock.when(() -> SecurityEngineUtils.getEngineAliasForId("eng1")).thenReturn("E");
				smssMock.when(() -> SmssUtilities.getUniqueName("E", "eng1")).thenReturn("E__eng1");
				syncMock.when(() -> EngineSyncUtility.getEngineLock("eng1")).thenReturn(lock);

				instance.pullEngineSmss("eng1");

				verify(mockStorageEngine).copyToLocal(contains(CentralCloudStorage.SMSS_POSTFIX), anyString());
			}
		}

		// ------- pushEngineFolder / pullEngineFolder path normalization -------

		@Test
		void testPushEngineFolder_normalizesPath(@TempDir Path testDir) throws Exception {
			IEngine mockEngine = mock(IEngine.class);
			when(mockEngine.getCatalogType()).thenReturn(IEngine.CATALOG_TYPE.DATABASE);
			ReentrantLock lock = new ReentrantLock();

			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
					MockedStatic<SecurityEngineUtils> secMock = mockStatic(SecurityEngineUtils.class);
					MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class);
					MockedStatic<EngineUtility> euMock = mockStatic(EngineUtility.class);
					MockedStatic<EngineSyncUtility> syncMock = mockStatic(EngineSyncUtility.class);
					MockedStatic<ClusterUtil> clusterMock = mockStatic(ClusterUtil.class)) {

				utilMock.when(() -> Utility.getEngine("eng1", false)).thenReturn(mockEngine);
				utilMock.when(() -> Utility.normalizePath(anyString())).thenAnswer(inv -> inv.getArgument(0));
				secMock.when(() -> SecurityEngineUtils.engineExists("eng1")).thenReturn(true);
				secMock.when(() -> SecurityEngineUtils.getEngineAliasForId("eng1")).thenReturn("E");
				smssMock.when(() -> SmssUtilities.getUniqueName("E", "eng1")).thenReturn("E__eng1");
				euMock.when(() -> EngineUtility.getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE.DATABASE))
						.thenReturn(testDir.toString());
				syncMock.when(() -> EngineSyncUtility.getEngineLock("eng1")).thenReturn(lock);

				// backslash and leading slash should be normalized
				instance.pushEngineFolder("eng1", "/local/path", "\\sub\\path");

				verify(mockStorageEngine).syncLocalToStorage(eq("/local/path"), anyString(),
						(Map<String, Object>) isNull());
			}
		}

		@Test
		void testPullEngineFolder_normalizesPath(@TempDir Path testDir) throws Exception {
			IEngine mockEngine = mock(IEngine.class);
			when(mockEngine.getCatalogType()).thenReturn(IEngine.CATALOG_TYPE.DATABASE);
			ReentrantLock lock = new ReentrantLock();

			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
					MockedStatic<SecurityEngineUtils> secMock = mockStatic(SecurityEngineUtils.class);
					MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class);
					MockedStatic<EngineUtility> euMock = mockStatic(EngineUtility.class);
					MockedStatic<EngineSyncUtility> syncMock = mockStatic(EngineSyncUtility.class);
					MockedStatic<ClusterUtil> clusterMock = mockStatic(ClusterUtil.class)) {

				utilMock.when(() -> Utility.getEngine("eng1", false)).thenReturn(mockEngine);
				utilMock.when(() -> Utility.normalizePath(anyString())).thenAnswer(inv -> inv.getArgument(0));
				secMock.when(() -> SecurityEngineUtils.engineExists("eng1")).thenReturn(true);
				secMock.when(() -> SecurityEngineUtils.getEngineAliasForId("eng1")).thenReturn("E");
				smssMock.when(() -> SmssUtilities.getUniqueName("E", "eng1")).thenReturn("E__eng1");
				euMock.when(() -> EngineUtility.getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE.DATABASE))
						.thenReturn(testDir.toString());
				syncMock.when(() -> EngineSyncUtility.getEngineLock("eng1")).thenReturn(lock);

				instance.pullEngineFolder("eng1", "/local/path", "/leading/slash");

				verify(mockStorageEngine).syncStorageToLocal(argThat(s -> s.contains("leading/slash")),
						eq("/local/path"));
			}
		}

		// ------- deleteEngineCloudFile happy path -------

		@Test
		void testDeleteEngineCloudFile_happyPath(@TempDir Path testDir) throws Exception {
			IEngine mockEngine = mock(IEngine.class);
			when(mockEngine.getEngineName()).thenReturn("E");
			ReentrantLock lock = new ReentrantLock();

			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
					MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class);
					MockedStatic<EngineUtility> euMock = mockStatic(EngineUtility.class);
					MockedStatic<EngineSyncUtility> syncMock = mockStatic(EngineSyncUtility.class)) {

				utilMock.when(() -> Utility.getEngine("eng1", false)).thenReturn(mockEngine);
				smssMock.when(() -> SmssUtilities.getUniqueName("E", "eng1")).thenReturn("E__eng1");
				euMock.when(() -> EngineUtility.getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE.DATABASE))
						.thenReturn(testDir.toString());
				syncMock.when(() -> EngineSyncUtility.getEngineLock("eng1")).thenReturn(lock);

				String localFile = testDir.toString() + "/E__eng1/data/file.db";
				instance.deleteEngineCloudFile("eng1", IEngine.CATALOG_TYPE.DATABASE, localFile);

				verify(mockStorageEngine).deleteFromStorage(argThat(s -> s.contains("eng1")));
			}
		}

		// ------- pushInsight / pullInsight happy path -------

		@Test
		void testPushInsight_happyPath() throws Exception {
			IProject mockProject = mock(IProject.class);
			when(mockProject.getProjectName()).thenReturn("MyProj");

			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
					MockedStatic<AssetUtility> assetMock = mockStatic(AssetUtility.class)) {

				utilMock.when(() -> Utility.getProject("proj1")).thenReturn(mockProject);
				utilMock.when(() -> Utility.normalizePath(anyString())).thenAnswer(inv -> inv.getArgument(0));
				utilMock.when(() -> Utility.cleanLogString(anyString())).thenAnswer(inv -> inv.getArgument(0));
				assetMock.when(() -> AssetUtility.getProjectVersionFolder("MyProj", "proj1"))
						.thenReturn("/projects/MyProj__proj1/version");

				instance.pushInsight("proj1", "ins1");

				verify(mockStorageEngine).syncLocalToStorage(argThat(s -> s.contains("ins1")),
						argThat(s -> s.contains("proj1") && s.contains("ins1")), (Map<String, Object>) isNull());
			}
		}

		@Test
		void testPullInsight_happyPath() throws Exception {
			IProject mockProject = mock(IProject.class);
			when(mockProject.getProjectName()).thenReturn("MyProj");

			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
					MockedStatic<AssetUtility> assetMock = mockStatic(AssetUtility.class)) {

				utilMock.when(() -> Utility.getProject("proj1")).thenReturn(mockProject);
				utilMock.when(() -> Utility.normalizePath(anyString())).thenAnswer(inv -> inv.getArgument(0));
				utilMock.when(() -> Utility.cleanLogString(anyString())).thenAnswer(inv -> inv.getArgument(0));
				assetMock.when(() -> AssetUtility.getProjectVersionFolder("MyProj", "proj1"))
						.thenReturn("/projects/MyProj__proj1/version");

				instance.pullInsight("proj1", "ins1");

				verify(mockStorageEngine).syncStorageToLocal(argThat(s -> s.contains("proj1") && s.contains("ins1")),
						argThat(s -> s.contains("ins1")));
			}
		}

		// ------- pushInsightImage variations -------

		@Test
		void testPushInsightImage_bothOldAndNew() throws Exception {
			IProject mockProject = mock(IProject.class);
			when(mockProject.getProjectName()).thenReturn("Proj");

			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
					MockedStatic<AssetUtility> assetMock = mockStatic(AssetUtility.class)) {

				utilMock.when(() -> Utility.getProject("proj1", false)).thenReturn(mockProject);
				utilMock.when(() -> Utility.normalizePath(anyString())).thenAnswer(inv -> inv.getArgument(0));
				utilMock.when(() -> Utility.cleanLogString(anyString())).thenAnswer(inv -> inv.getArgument(0));
				assetMock.when(() -> AssetUtility.getProjectVersionFolder("Proj", "proj1"))
						.thenReturn("/projects/Proj__proj1/version");

				instance.pushInsightImage("proj1", "ins1", "old.png", "new.png");

				verify(mockStorageEngine).deleteFromStorage(argThat(s -> s.contains("old.png")));
				verify(mockStorageEngine).copyToStorage(argThat(s -> s.contains("new.png")), anyString(), any());
			}
		}

		@Test
		void testPushInsightImage_oldNull_newPresent() throws Exception {
			IProject mockProject = mock(IProject.class);
			when(mockProject.getProjectName()).thenReturn("Proj");

			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
					MockedStatic<AssetUtility> assetMock = mockStatic(AssetUtility.class)) {

				utilMock.when(() -> Utility.getProject("proj1", false)).thenReturn(mockProject);
				utilMock.when(() -> Utility.normalizePath(anyString())).thenAnswer(inv -> inv.getArgument(0));
				utilMock.when(() -> Utility.cleanLogString(anyString())).thenAnswer(inv -> inv.getArgument(0));
				assetMock.when(() -> AssetUtility.getProjectVersionFolder("Proj", "proj1"))
						.thenReturn("/projects/Proj__proj1/version");

				instance.pushInsightImage("proj1", "ins1", null, "new.png");

				verify(mockStorageEngine, never()).deleteFromStorage(anyString());
				verify(mockStorageEngine).copyToStorage(argThat(s -> s.contains("new.png")), anyString(), any());
			}
		}

		@Test
		void testPushInsightImage_oldPresent_newNull() throws Exception {
			IProject mockProject = mock(IProject.class);
			when(mockProject.getProjectName()).thenReturn("Proj");

			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
					MockedStatic<AssetUtility> assetMock = mockStatic(AssetUtility.class)) {

				utilMock.when(() -> Utility.getProject("proj1", false)).thenReturn(mockProject);
				utilMock.when(() -> Utility.cleanLogString(anyString())).thenAnswer(inv -> inv.getArgument(0));

				instance.pushInsightImage("proj1", "ins1", "old.png", null);

				verify(mockStorageEngine).deleteFromStorage(argThat(s -> s.contains("old.png")));
				verify(mockStorageEngine, never()).copyToStorage(anyString(), anyString(), any());
			}
		}

		// ------- catalogPulledEngine happy paths -------

		@Test
		void testCatalogPulledEngine_DATABASE() {
			try (MockedStatic<SMSSWebWatcher> watcherMock = mockStatic(SMSSWebWatcher.class)) {
				instance.catalogPulledEngine("alias__id", "file.smss", IEngine.CATALOG_TYPE.DATABASE);
				watcherMock.verify(() -> SMSSWebWatcher.catalogEngine(eq("file.smss"), any()));
			}
		}

		@Test
		void testCatalogPulledEngine_STORAGE() {
			try (MockedStatic<SMSSNoInitEngineWatcher> watcherMock = mockStatic(SMSSNoInitEngineWatcher.class)) {
				instance.catalogPulledEngine("alias__id", "file.smss", IEngine.CATALOG_TYPE.STORAGE);
				watcherMock.verify(() -> SMSSNoInitEngineWatcher.catalogEngine(eq("file.smss"), any()));
			}
		}

		@Test
		void testCatalogPulledEngine_MODEL() {
			try (MockedStatic<SMSSNoInitEngineWatcher> watcherMock = mockStatic(SMSSNoInitEngineWatcher.class)) {
				instance.catalogPulledEngine("alias__id", "file.smss", IEngine.CATALOG_TYPE.MODEL);
				watcherMock.verify(() -> SMSSNoInitEngineWatcher.catalogEngine(eq("file.smss"), any()));
			}
		}

		@Test
		void testCatalogPulledEngine_VECTOR() {
			try (MockedStatic<SMSSNoInitEngineWatcher> watcherMock = mockStatic(SMSSNoInitEngineWatcher.class)) {
				instance.catalogPulledEngine("alias__id", "file.smss", IEngine.CATALOG_TYPE.VECTOR);
				watcherMock.verify(() -> SMSSNoInitEngineWatcher.catalogEngine(eq("file.smss"), any()));
			}
		}

		@Test
		void testCatalogPulledEngine_FUNCTION() {
			try (MockedStatic<SMSSNoInitEngineWatcher> watcherMock = mockStatic(SMSSNoInitEngineWatcher.class)) {
				instance.catalogPulledEngine("alias__id", "file.smss", IEngine.CATALOG_TYPE.FUNCTION);
				watcherMock.verify(() -> SMSSNoInitEngineWatcher.catalogEngine(eq("file.smss"), any()));
			}
		}

		@Test
		void testCatalogPulledEngine_GUARDRAIL() {
			try (MockedStatic<SMSSNoInitEngineWatcher> watcherMock = mockStatic(SMSSNoInitEngineWatcher.class)) {
				instance.catalogPulledEngine("alias__id", "file.smss", IEngine.CATALOG_TYPE.GUARDRAIL);
				watcherMock.verify(() -> SMSSNoInitEngineWatcher.catalogEngine(eq("file.smss"), any()));
			}
		}

		@Test
		void testCatalogPulledEngine_VENV() {
			try (MockedStatic<SMSSNoInitEngineWatcher> watcherMock = mockStatic(SMSSNoInitEngineWatcher.class)) {
				instance.catalogPulledEngine("alias__id", "file.smss", IEngine.CATALOG_TYPE.VENV);
				watcherMock.verify(() -> SMSSNoInitEngineWatcher.catalogEngine(eq("file.smss"), any()));
			}
		}

		@Test
		void testCatalogPulledEngine_PROJECT() {
			try (MockedStatic<ProjectWatcher> watcherMock = mockStatic(ProjectWatcher.class)) {
				instance.catalogPulledEngine("alias__id", "file.smss", IEngine.CATALOG_TYPE.PROJECT);
				watcherMock.verify(() -> ProjectWatcher.catalogProject(eq("file.smss"), any()));
			}
		}

		// ------- copyLocalFileToEngineCloudFolder happy path -------

		@Test
		void testCopyLocalFileToEngineCloudFolder_file(@TempDir Path testDir) throws Exception {
			IEngine mockEngine = mock(IEngine.class);
			when(mockEngine.getEngineName()).thenReturn("E");
			ReentrantLock lock = new ReentrantLock();

			Path engineDir = testDir.resolve("E__eng1");
			Files.createDirectories(engineDir.resolve("data"));
			Path localFile = Files.createFile(engineDir.resolve("data/test.db"));

			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
					MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class);
					MockedStatic<EngineUtility> euMock = mockStatic(EngineUtility.class);
					MockedStatic<EngineSyncUtility> syncMock = mockStatic(EngineSyncUtility.class)) {

				utilMock.when(() -> Utility.getEngine("eng1", false)).thenReturn(mockEngine);
				smssMock.when(() -> SmssUtilities.getUniqueName("E", "eng1")).thenReturn("E__eng1");
				euMock.when(() -> EngineUtility.getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE.DATABASE))
						.thenReturn(testDir.toString());
				syncMock.when(() -> EngineSyncUtility.getEngineLock("eng1")).thenReturn(lock);

				instance.copyLocalFileToEngineCloudFolder("eng1", IEngine.CATALOG_TYPE.DATABASE, localFile.toString());

				verify(mockStorageEngine).copyToStorage(eq(localFile.toString()), anyString(),
						(Map<String, Object>) isNull());
			}
		}

		@Test
		void testCopyLocalFileToEngineCloudFolder_directory(@TempDir Path testDir) throws Exception {
			IEngine mockEngine = mock(IEngine.class);
			when(mockEngine.getEngineName()).thenReturn("E");
			ReentrantLock lock = new ReentrantLock();

			Path engineDir = testDir.resolve("E__eng1");
			Path subDir = engineDir.resolve("assets");
			Files.createDirectories(subDir);

			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
					MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class);
					MockedStatic<EngineUtility> euMock = mockStatic(EngineUtility.class);
					MockedStatic<EngineSyncUtility> syncMock = mockStatic(EngineSyncUtility.class);
					MockedStatic<ClusterUtil> clusterMock = mockStatic(ClusterUtil.class)) {

				utilMock.when(() -> Utility.getEngine("eng1", false)).thenReturn(mockEngine);
				smssMock.when(() -> SmssUtilities.getUniqueName("E", "eng1")).thenReturn("E__eng1");
				euMock.when(() -> EngineUtility.getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE.DATABASE))
						.thenReturn(testDir.toString());
				syncMock.when(() -> EngineSyncUtility.getEngineLock("eng1")).thenReturn(lock);

				instance.copyLocalFileToEngineCloudFolder("eng1", IEngine.CATALOG_TYPE.DATABASE, subDir.toString());

				verify(mockStorageEngine).copyToStorage(eq(subDir.toString()), anyString(),
						(Map<String, Object>) isNull());
				clusterMock.verify(() -> ClusterUtil.validateFolder(subDir.toAbsolutePath().toString()));
			}
		}

		// ------- copyEngineCloudFileToLocalFile happy path -------

		@Test
		void testCopyEngineCloudFileToLocalFile(@TempDir Path testDir) throws Exception {
			IEngine mockEngine = mock(IEngine.class);
			when(mockEngine.getEngineName()).thenReturn("E");
			ReentrantLock lock = new ReentrantLock();

			Path engineDir = testDir.resolve("E__eng1");
			Files.createDirectories(engineDir);
			Path localFile = engineDir.resolve("data/test.db");

			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
					MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class);
					MockedStatic<EngineUtility> euMock = mockStatic(EngineUtility.class);
					MockedStatic<EngineSyncUtility> syncMock = mockStatic(EngineSyncUtility.class)) {

				utilMock.when(() -> Utility.getEngine("eng1", false)).thenReturn(mockEngine);
				smssMock.when(() -> SmssUtilities.getUniqueName("E", "eng1")).thenReturn("E__eng1");
				euMock.when(() -> EngineUtility.getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE.DATABASE))
						.thenReturn(testDir.toString());
				syncMock.when(() -> EngineSyncUtility.getEngineLock("eng1")).thenReturn(lock);

				instance.copyEngineCloudFileToLocalFile("eng1", IEngine.CATALOG_TYPE.DATABASE, localFile.toString());

				verify(mockStorageEngine).copyToLocal(anyString(), eq(localFile.toString()));
			}
		}

		// ------- pullEngine engineId equals engineName branch -------

		@Test
		void testPullEngine_engineIdEqualsName_setsNameNull(@TempDir Path testDir) throws Exception {
			ReentrantLock lock = new ReentrantLock();

			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
					MockedStatic<SecurityEngineUtils> secMock = mockStatic(SecurityEngineUtils.class);
					MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class);
					MockedStatic<EngineUtility> euMock = mockStatic(EngineUtility.class);
					MockedStatic<EngineSyncUtility> syncMock = mockStatic(EngineSyncUtility.class);
					MockedStatic<SMSSNoInitEngineWatcher> watcherMock = mockStatic(SMSSNoInitEngineWatcher.class)) {

				// engineName == engineId triggers the name-null branch
				secMock.when(() -> SecurityEngineUtils.engineExists("eng1")).thenReturn(true);
				secMock.when(() -> SecurityEngineUtils.getEngineAliasForId("eng1")).thenReturn("eng1");
				smssMock.when(() -> SmssUtilities.getUniqueName(null, "eng1")).thenReturn("eng1");
				euMock.when(() -> EngineUtility.getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE.MODEL))
						.thenReturn(testDir.toString());
				utilMock.when(() -> Utility.normalizePath(anyString())).thenAnswer(inv -> inv.getArgument(0));
				utilMock.when(() -> Utility.cleanLogString(anyString())).thenAnswer(inv -> inv.getArgument(0));
				syncMock.when(() -> EngineSyncUtility.getEngineLock("eng1")).thenReturn(lock);

				instance.pullEngine("eng1", IEngine.CATALOG_TYPE.MODEL, false);

				// Verify getUniqueName was called with null name
				smssMock.verify(() -> SmssUtilities.getUniqueName(null, "eng1"));
			}
		}

		// ------- Helper to set static final fields via Unsafe -------

		private static void setStaticFinalField(Class<?> clazz, String fieldName, Object value) throws Exception {
			Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
			unsafeField.setAccessible(true);
			Unsafe unsafe = (Unsafe) unsafeField.get(null);
			Field target = clazz.getDeclaredField(fieldName);
			Object base = unsafe.staticFieldBase(target);
			long offset = unsafe.staticFieldOffset(target);
			unsafe.putObject(base, offset, value);
		}

		// ------- pushProjectFolder / pullProjectFolder happy path -------

		@Test
		void testPushProjectFolder_happyPath() throws Exception {
			IProject mockProject = mock(IProject.class);
			ReentrantLock lock = new ReentrantLock();

			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
					MockedStatic<SecurityProjectUtils> secMock = mockStatic(SecurityProjectUtils.class);
					MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class);
					MockedStatic<ProjectSyncUtility> syncMock = mockStatic(ProjectSyncUtility.class)) {

				utilMock.when(() -> Utility.getProject("proj1", false)).thenReturn(mockProject);
				utilMock.when(() -> Utility.normalizePath(anyString())).thenAnswer(inv -> inv.getArgument(0));
				secMock.when(() -> SecurityProjectUtils.projectExists("proj1")).thenReturn(true);
				secMock.when(() -> SecurityProjectUtils.getProjectAliasForId("proj1")).thenReturn("P");
				smssMock.when(() -> SmssUtilities.getUniqueName("P", "proj1")).thenReturn("P__proj1");
				syncMock.when(() -> ProjectSyncUtility.getProjectLock("proj1")).thenReturn(lock);

				instance.pushProjectFolder("proj1", "/local/assets", "\\sub\\path");

				verify(mockStorageEngine).syncLocalToStorage(eq("/local/assets"),
						argThat(s -> s.contains("proj1") && s.contains("sub/path")), (Map<String, Object>) isNull());
			}
		}

		@Test
		void testPullProjectFolder_happyPath() throws Exception {
			IProject mockProject = mock(IProject.class);
			ReentrantLock lock = new ReentrantLock();

			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
					MockedStatic<SecurityProjectUtils> secMock = mockStatic(SecurityProjectUtils.class);
					MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class);
					MockedStatic<ProjectSyncUtility> syncMock = mockStatic(ProjectSyncUtility.class)) {

				utilMock.when(() -> Utility.getProject("proj1", false)).thenReturn(mockProject);
				secMock.when(() -> SecurityProjectUtils.projectExists("proj1")).thenReturn(true);
				secMock.when(() -> SecurityProjectUtils.getProjectAliasForId("proj1")).thenReturn("P");
				smssMock.when(() -> SmssUtilities.getUniqueName("P", "proj1")).thenReturn("P__proj1");
				syncMock.when(() -> ProjectSyncUtility.getProjectLock("proj1")).thenReturn(lock);

				instance.pullProjectFolder("proj1", "/local/assets", "/leading/path");

				verify(mockStorageEngine).syncStorageToLocal(
						argThat(s -> s.contains("proj1") && s.contains("leading/path")), eq("/local/assets"));
			}
		}

		@Test
		void testPushProjectFolder_nullRelativePath() throws Exception {
			IProject mockProject = mock(IProject.class);
			ReentrantLock lock = new ReentrantLock();

			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
					MockedStatic<SecurityProjectUtils> secMock = mockStatic(SecurityProjectUtils.class);
					MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class);
					MockedStatic<ProjectSyncUtility> syncMock = mockStatic(ProjectSyncUtility.class)) {

				utilMock.when(() -> Utility.getProject("proj1", false)).thenReturn(mockProject);
				utilMock.when(() -> Utility.normalizePath(anyString())).thenAnswer(inv -> inv.getArgument(0));
				secMock.when(() -> SecurityProjectUtils.projectExists("proj1")).thenReturn(true);
				secMock.when(() -> SecurityProjectUtils.getProjectAliasForId("proj1")).thenReturn("P");
				smssMock.when(() -> SmssUtilities.getUniqueName("P", "proj1")).thenReturn("P__proj1");
				syncMock.when(() -> ProjectSyncUtility.getProjectLock("proj1")).thenReturn(lock);

				// storageRelativePath is null, so the target is just container + projectId
				instance.pushProjectFolder("proj1", "/local", null);

				verify(mockStorageEngine).syncLocalToStorage(eq("/local"), argThat(target -> target.endsWith("proj1")),
						any());
			}
		}

		// ------- pushProject / pullProject happy path -------

		@Test
		void testPushProject_happyPath(@TempDir Path testDir) throws Exception {
			String origProjectFolder = EngineUtility.PROJECT_FOLDER;
			try {
				setStaticFinalField(EngineUtility.class, "PROJECT_FOLDER", testDir.toString());

				IProject mockProject = mock(IProject.class);
				when(mockProject.getProjectName()).thenReturn("MyProj");
				ReentrantLock lock = new ReentrantLock();

				try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
						MockedStatic<DIHelper> diMock = mockStatic(DIHelper.class);
						MockedStatic<ProjectSyncUtility> syncMock = mockStatic(ProjectSyncUtility.class)) {

					utilMock.when(() -> Utility.getProject("proj1", false)).thenReturn(mockProject);
					DIHelper mockDI = mock(DIHelper.class);
					diMock.when(DIHelper::getInstance).thenReturn(mockDI);
					when(mockDI.getEngineProperty(Constants.ENGINES)).thenReturn("eng1");
					when(mockDI.getProjectProperty(Constants.PROJECTS)).thenReturn("proj1");
					syncMock.when(() -> ProjectSyncUtility.getProjectLock("proj1")).thenReturn(lock);

					instance.pushProject("proj1");

					verify(mockDI).removeProjectProperty("proj1");
					verify(mockProject).close();
					verify(mockStorageEngine).syncLocalToStorage(anyString(), anyString(), any());
					verify(mockStorageEngine).copyToStorage(argThat(s -> s.contains(".smss")), anyString(), any());
				}
			} finally {
				setStaticFinalField(EngineUtility.class, "PROJECT_FOLDER", origProjectFolder);
			}
		}

		@Test
		void testPushProject_nameNull_fallsBackToAlias(@TempDir Path testDir) throws Exception {
			String origProjectFolder = EngineUtility.PROJECT_FOLDER;
			try {
				setStaticFinalField(EngineUtility.class, "PROJECT_FOLDER", testDir.toString());

				IProject mockProject = mock(IProject.class);
				when(mockProject.getProjectName()).thenReturn(null);
				ReentrantLock lock = new ReentrantLock();

				try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
						MockedStatic<SecurityProjectUtils> secMock = mockStatic(SecurityProjectUtils.class);
						MockedStatic<DIHelper> diMock = mockStatic(DIHelper.class);
						MockedStatic<ProjectSyncUtility> syncMock = mockStatic(ProjectSyncUtility.class)) {

					utilMock.when(() -> Utility.getProject("proj1", false)).thenReturn(mockProject);
					secMock.when(() -> SecurityProjectUtils.projectExists("proj1")).thenReturn(true);
					secMock.when(() -> SecurityProjectUtils.getProjectAliasForId("proj1")).thenReturn("AliasProj");
					DIHelper mockDI = mock(DIHelper.class);
					diMock.when(DIHelper::getInstance).thenReturn(mockDI);
					when(mockDI.getEngineProperty(Constants.ENGINES)).thenReturn("eng1");
					when(mockDI.getProjectProperty(Constants.PROJECTS)).thenReturn("proj1");
					syncMock.when(() -> ProjectSyncUtility.getProjectLock("proj1")).thenReturn(lock);

					instance.pushProject("proj1");

					verify(mockStorageEngine).syncLocalToStorage(argThat(s -> s.contains("AliasProj__proj1")),
							anyString(), any());
				}
			} finally {
				setStaticFinalField(EngineUtility.class, "PROJECT_FOLDER", origProjectFolder);
			}
		}

		@Test
		void testPullProject_notLoaded(@TempDir Path testDir) throws Exception {
			String origProjectFolder = EngineUtility.PROJECT_FOLDER;
			try {
				setStaticFinalField(EngineUtility.class, "PROJECT_FOLDER", testDir.toString());

				ReentrantLock lock = new ReentrantLock();

				try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
						MockedStatic<SecurityProjectUtils> secMock = mockStatic(SecurityProjectUtils.class);
						MockedStatic<ProjectSyncUtility> syncMock = mockStatic(ProjectSyncUtility.class);
						MockedStatic<ProjectWatcher> watcherMock = mockStatic(ProjectWatcher.class)) {

					utilMock.when(() -> Utility.normalizePath(anyString())).thenAnswer(inv -> inv.getArgument(0));
					secMock.when(() -> SecurityProjectUtils.projectExists("proj1")).thenReturn(true);
					secMock.when(() -> SecurityProjectUtils.getProjectAliasForId("proj1")).thenReturn("P");
					syncMock.when(() -> ProjectSyncUtility.getProjectLock("proj1")).thenReturn(lock);

					instance.pullProject("proj1", false);

					verify(mockStorageEngine).syncStorageToLocal(anyString(), anyString());
					verify(mockStorageEngine).copyToLocal(argThat(s -> s.contains(CentralCloudStorage.SMSS_POSTFIX)),
							eq(testDir.toString()));
					watcherMock.verify(() -> ProjectWatcher.catalogProject(eq("P__proj1.smss"), any()));
				}
			} finally {
				setStaticFinalField(EngineUtility.class, "PROJECT_FOLDER", origProjectFolder);
			}
		}

		@Test
		void testPullProject_alreadyLoaded(@TempDir Path testDir) throws Exception {
			String origProjectFolder = EngineUtility.PROJECT_FOLDER;
			try {
				setStaticFinalField(EngineUtility.class, "PROJECT_FOLDER", testDir.toString());

				IProject mockProject = mock(IProject.class);
				ReentrantLock lock = new ReentrantLock();

				try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
						MockedStatic<SecurityProjectUtils> secMock = mockStatic(SecurityProjectUtils.class);
						MockedStatic<DIHelper> diMock = mockStatic(DIHelper.class);
						MockedStatic<ProjectSyncUtility> syncMock = mockStatic(ProjectSyncUtility.class)) {

					utilMock.when(() -> Utility.getProject("proj1", false)).thenReturn(mockProject);
					utilMock.when(() -> Utility.normalizePath(anyString())).thenAnswer(inv -> inv.getArgument(0));
					secMock.when(() -> SecurityProjectUtils.projectExists("proj1")).thenReturn(true);
					secMock.when(() -> SecurityProjectUtils.getProjectAliasForId("proj1")).thenReturn("P");
					DIHelper mockDI = mock(DIHelper.class);
					diMock.when(DIHelper::getInstance).thenReturn(mockDI);
					when(mockDI.getEngineProperty(Constants.ENGINES)).thenReturn("eng1");
					when(mockDI.getProjectProperty(Constants.PROJECTS)).thenReturn("proj1");
					syncMock.when(() -> ProjectSyncUtility.getProjectLock("proj1")).thenReturn(lock);

					instance.pullProject("proj1", true);

					verify(mockDI).removeProjectProperty("proj1");
					verify(mockProject).close();
					verify(mockStorageEngine).syncStorageToLocal(anyString(), anyString());
				}
			} finally {
				setStaticFinalField(EngineUtility.class, "PROJECT_FOLDER", origProjectFolder);
			}
		}

		@Test
		void testPullProject_singleArg_delegates(@TempDir Path testDir) throws Exception {
			String origProjectFolder = EngineUtility.PROJECT_FOLDER;
			try {
				setStaticFinalField(EngineUtility.class, "PROJECT_FOLDER", testDir.toString());

				ReentrantLock lock = new ReentrantLock();

				try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
						MockedStatic<SecurityProjectUtils> secMock = mockStatic(SecurityProjectUtils.class);
						MockedStatic<ProjectSyncUtility> syncMock = mockStatic(ProjectSyncUtility.class);
						MockedStatic<ProjectWatcher> watcherMock = mockStatic(ProjectWatcher.class)) {

					utilMock.when(() -> Utility.normalizePath(anyString())).thenAnswer(inv -> inv.getArgument(0));
					secMock.when(() -> SecurityProjectUtils.projectExists("proj1")).thenReturn(true);
					secMock.when(() -> SecurityProjectUtils.getProjectAliasForId("proj1")).thenReturn("P");
					syncMock.when(() -> ProjectSyncUtility.getProjectLock("proj1")).thenReturn(lock);

					instance.pullProject("proj1");

					verify(mockStorageEngine).syncStorageToLocal(anyString(), anyString());
				}
			} finally {
				setStaticFinalField(EngineUtility.class, "PROJECT_FOLDER", origProjectFolder);
			}
		}

		// ------- pushLocalDatabaseFile / pullLocalDatabaseFile -------

		@Test
		void testPushLocalDatabaseFile_sqlite(@TempDir Path testDir) throws Exception {
			String origDbFolder = EngineUtility.DATABASE_FOLDER;
			try {
				setStaticFinalField(EngineUtility.class, "DATABASE_FOLDER", testDir.toString());

				// Create a sqlite file in the expected directory
				Path dbDir = testDir.resolve("MyDb__db1");
				Files.createDirectories(dbDir);
				Files.createFile(dbDir.resolve("mydata.sqlite"));

				IDatabaseEngine mockDb = mock(IDatabaseEngine.class);
				ReentrantLock lock = new ReentrantLock();

				try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
						MockedStatic<SecurityEngineUtils> secMock = mockStatic(SecurityEngineUtils.class);
						MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class);
						MockedStatic<EngineSyncUtility> syncMock = mockStatic(EngineSyncUtility.class);
						MockedStatic<DIHelper> diMock = mockStatic(DIHelper.class)) {

					utilMock.when(() -> Utility.getDatabase("db1", false)).thenReturn(mockDb);
					secMock.when(() -> SecurityEngineUtils.getEngineAliasForId("db1")).thenReturn("MyDb");
					smssMock.when(() -> SmssUtilities.getUniqueName("MyDb", "db1")).thenReturn("MyDb__db1");
					syncMock.when(() -> EngineSyncUtility.getEngineLock("db1")).thenReturn(lock);
					DIHelper mockDI = mock(DIHelper.class);
					diMock.when(DIHelper::getInstance).thenReturn(mockDI);
					when(mockDI.getEngineProperty(Constants.ENGINES)).thenReturn("eng1");
					when(mockDI.getProjectProperty(Constants.PROJECTS)).thenReturn("proj1");

					instance.pushLocalDatabaseFile("db1", RdbmsTypeEnum.SQLITE);

					verify(mockDb).close();
					verify(mockStorageEngine).copyToStorage(argThat(s -> s.contains("mydata.sqlite")), anyString(),
							(Map<String, Object>) isNull());
				}
			} finally {
				setStaticFinalField(EngineUtility.class, "DATABASE_FOLDER", origDbFolder);
			}
		}

		@Test
		void testPushLocalDatabaseFile_h2(@TempDir Path testDir) throws Exception {
			String origDbFolder = EngineUtility.DATABASE_FOLDER;
			try {
				setStaticFinalField(EngineUtility.class, "DATABASE_FOLDER", testDir.toString());

				Path dbDir = testDir.resolve("MyDb__db1");
				Files.createDirectories(dbDir);
				Files.createFile(dbDir.resolve("mydata.mv.db"));

				IDatabaseEngine mockDb = mock(IDatabaseEngine.class);
				ReentrantLock lock = new ReentrantLock();

				try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
						MockedStatic<SecurityEngineUtils> secMock = mockStatic(SecurityEngineUtils.class);
						MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class);
						MockedStatic<EngineSyncUtility> syncMock = mockStatic(EngineSyncUtility.class);
						MockedStatic<DIHelper> diMock = mockStatic(DIHelper.class)) {

					utilMock.when(() -> Utility.getDatabase("db1", false)).thenReturn(mockDb);
					secMock.when(() -> SecurityEngineUtils.getEngineAliasForId("db1")).thenReturn("MyDb");
					smssMock.when(() -> SmssUtilities.getUniqueName("MyDb", "db1")).thenReturn("MyDb__db1");
					syncMock.when(() -> EngineSyncUtility.getEngineLock("db1")).thenReturn(lock);
					DIHelper mockDI = mock(DIHelper.class);
					diMock.when(DIHelper::getInstance).thenReturn(mockDI);
					when(mockDI.getEngineProperty(Constants.ENGINES)).thenReturn("eng1");
					when(mockDI.getProjectProperty(Constants.PROJECTS)).thenReturn("proj1");

					instance.pushLocalDatabaseFile("db1", RdbmsTypeEnum.H2_DB);

					verify(mockStorageEngine).copyToStorage(argThat(s -> s.contains("mydata.mv.db")), anyString(),
							(Map<String, Object>) isNull());
				}
			} finally {
				setStaticFinalField(EngineUtility.class, "DATABASE_FOLDER", origDbFolder);
			}
		}

		@Test
		void testPullLocalDatabaseFile_sqlite(@TempDir Path testDir) throws Exception {
			String origDbFolder = EngineUtility.DATABASE_FOLDER;
			try {
				setStaticFinalField(EngineUtility.class, "DATABASE_FOLDER", testDir.toString());

				IDatabaseEngine mockDb = mock(IDatabaseEngine.class);
				ReentrantLock lock = new ReentrantLock();
				when(mockStorageEngine.list(anyString())).thenReturn(Arrays.asList("mydata.sqlite", "other.txt"));

				try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
						MockedStatic<SecurityEngineUtils> secMock = mockStatic(SecurityEngineUtils.class);
						MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class);
						MockedStatic<EngineSyncUtility> syncMock = mockStatic(EngineSyncUtility.class);
						MockedStatic<DIHelper> diMock = mockStatic(DIHelper.class)) {

					utilMock.when(() -> Utility.getDatabase("db1", false)).thenReturn(mockDb);
					secMock.when(() -> SecurityEngineUtils.getEngineAliasForId("db1")).thenReturn("MyDb");
					smssMock.when(() -> SmssUtilities.getUniqueName("MyDb", "db1")).thenReturn("MyDb__db1");
					syncMock.when(() -> EngineSyncUtility.getEngineLock("db1")).thenReturn(lock);
					DIHelper mockDI = mock(DIHelper.class);
					diMock.when(DIHelper::getInstance).thenReturn(mockDI);
					when(mockDI.getEngineProperty(Constants.ENGINES)).thenReturn("eng1");
					when(mockDI.getProjectProperty(Constants.PROJECTS)).thenReturn("proj1");

					instance.pullLocalDatabaseFile("db1", RdbmsTypeEnum.SQLITE);

					// Should only pull .sqlite files, not other.txt
					verify(mockStorageEngine).copyToLocal(argThat(s -> s.contains("mydata.sqlite")), anyString());
					verify(mockStorageEngine, times(1)).copyToLocal(anyString(), anyString());
				}
			} finally {
				setStaticFinalField(EngineUtility.class, "DATABASE_FOLDER", origDbFolder);
			}
		}

		@Test
		void testPullLocalDatabaseFile_h2(@TempDir Path testDir) throws Exception {
			String origDbFolder = EngineUtility.DATABASE_FOLDER;
			try {
				setStaticFinalField(EngineUtility.class, "DATABASE_FOLDER", testDir.toString());

				IDatabaseEngine mockDb = mock(IDatabaseEngine.class);
				ReentrantLock lock = new ReentrantLock();
				when(mockStorageEngine.list(anyString())).thenReturn(Arrays.asList("mydata.mv.db", "other.txt"));

				try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
						MockedStatic<SecurityEngineUtils> secMock = mockStatic(SecurityEngineUtils.class);
						MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class);
						MockedStatic<EngineSyncUtility> syncMock = mockStatic(EngineSyncUtility.class);
						MockedStatic<DIHelper> diMock = mockStatic(DIHelper.class)) {

					utilMock.when(() -> Utility.getDatabase("db1", false)).thenReturn(mockDb);
					secMock.when(() -> SecurityEngineUtils.getEngineAliasForId("db1")).thenReturn("MyDb");
					smssMock.when(() -> SmssUtilities.getUniqueName("MyDb", "db1")).thenReturn("MyDb__db1");
					syncMock.when(() -> EngineSyncUtility.getEngineLock("db1")).thenReturn(lock);
					DIHelper mockDI = mock(DIHelper.class);
					diMock.when(DIHelper::getInstance).thenReturn(mockDI);
					when(mockDI.getEngineProperty(Constants.ENGINES)).thenReturn("eng1");
					when(mockDI.getProjectProperty(Constants.PROJECTS)).thenReturn("proj1");

					instance.pullLocalDatabaseFile("db1", RdbmsTypeEnum.H2_DB);

					verify(mockStorageEngine).copyToLocal(argThat(s -> s.contains("mydata.mv.db")), anyString());
				}
			} finally {
				setStaticFinalField(EngineUtility.class, "DATABASE_FOLDER", origDbFolder);
			}
		}

		// ------- pushOwl / pullOwl -------

		@Test
		void testPushOwl_happyPath(@TempDir Path testDir) throws Exception {
			Path owlFile = Files.createFile(testDir.resolve("mydb.owl"));
			Files.createFile(testDir.resolve("positions.json"));

			IDatabaseEngine mockDb = mock(IDatabaseEngine.class);
			ReentrantLock lock = new ReentrantLock();

			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
					MockedStatic<SecurityEngineUtils> secMock = mockStatic(SecurityEngineUtils.class);
					MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class);
					MockedStatic<EngineSyncUtility> syncMock = mockStatic(EngineSyncUtility.class)) {

				utilMock.when(() -> Utility.getDatabase("db1", false)).thenReturn(mockDb);
				secMock.when(() -> SecurityEngineUtils.getEngineAliasForId("db1")).thenReturn("MyDb");
				smssMock.when(() -> SmssUtilities.getUniqueName("MyDb", "db1")).thenReturn("MyDb__db1");
				smssMock.when(() -> SmssUtilities.getOwlFile(any(), any())).thenReturn(owlFile.toFile());
				syncMock.when(() -> EngineSyncUtility.getEngineLock("db1")).thenReturn(lock);

				instance.pushOwl("db1", null);

				verify(mockStorageEngine).copyToStorage(eq(owlFile.toAbsolutePath().toString()), anyString(), any());
			}
		}

		@Test
		void testPushOwl_singleArg_delegates() {
			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(() -> Utility.getDatabase("db1", false)).thenReturn(null);
				assertThrows(IllegalArgumentException.class, () -> instance.pushOwl("db1"));
			}
		}

		@Test
		void testPullOwl_singleArg_delegates() {
			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(() -> Utility.getDatabase("db1", false)).thenReturn(null);
				assertThrows(IllegalArgumentException.class, () -> instance.pullOwl("db1"));
			}
		}

		// ------- pullUserAsset / pushUserAsset -------

		@Test
		void testPullUserAsset_notLoaded(@TempDir Path testDir) throws Exception {
			String origUserFolder = EngineUtility.USER_FOLDER;
			try {
				setStaticFinalField(EngineUtility.class, "USER_FOLDER", testDir.toString());

				try (MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class)) {
					smssMock.when(
							() -> SmssUtilities.getUniqueName(prerna.auth.utils.UserAssetUtils.ASSET_APP_NAME, "proj1"))
							.thenReturn("Asset__proj1");

					instance.pullUserAsset("proj1", false);

					verify(mockStorageEngine).syncStorageToLocal(anyString(), anyString());
					verify(mockStorageEngine).copyToLocal(anyString(), eq(testDir.toString()));
				}
			} finally {
				setStaticFinalField(EngineUtility.class, "USER_FOLDER", origUserFolder);
			}
		}

		@Test
		void testPullUserAsset_loaded(@TempDir Path testDir) throws Exception {
			String origUserFolder = EngineUtility.USER_FOLDER;
			try {
				setStaticFinalField(EngineUtility.class, "USER_FOLDER", testDir.toString());

				IProject mockProject = mock(IProject.class);
				when(mockProject.getProjectName()).thenReturn("AssetApp");

				try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
						MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class);
						MockedStatic<DIHelper> diMock = mockStatic(DIHelper.class)) {

					utilMock.when(() -> Utility.getUserAssetProject("proj1")).thenReturn(mockProject);
					smssMock.when(() -> SmssUtilities.getUniqueName("AssetApp", "proj1")).thenReturn("AssetApp__proj1");
					DIHelper mockDI = mock(DIHelper.class);
					diMock.when(DIHelper::getInstance).thenReturn(mockDI);
					when(mockDI.getEngineProperty(Constants.ENGINES)).thenReturn("eng1");
					when(mockDI.getProjectProperty(Constants.PROJECTS)).thenReturn("proj1");

					instance.pullUserAsset("proj1", true);

					verify(mockDI).removeProjectProperty("proj1");
					verify(mockProject).close();
					verify(mockStorageEngine).syncStorageToLocal(anyString(), anyString());
				}
			} finally {
				setStaticFinalField(EngineUtility.class, "USER_FOLDER", origUserFolder);
			}
		}

		@Test
		void testPushUserAsset_happyPath(@TempDir Path testDir) throws Exception {
			String origUserFolder = EngineUtility.USER_FOLDER;
			try {
				setStaticFinalField(EngineUtility.class, "USER_FOLDER", testDir.toString());

				IProject mockProject = mock(IProject.class);
				when(mockProject.getProjectName()).thenReturn("AssetApp");

				try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
						MockedStatic<DIHelper> diMock = mockStatic(DIHelper.class)) {

					utilMock.when(() -> Utility.getUserAssetProject("proj1")).thenReturn(mockProject);
					DIHelper mockDI = mock(DIHelper.class);
					diMock.when(DIHelper::getInstance).thenReturn(mockDI);
					when(mockDI.getEngineProperty(Constants.ENGINES)).thenReturn("eng1");
					when(mockDI.getProjectProperty(Constants.PROJECTS)).thenReturn("proj1");

					instance.pushUserAsset("proj1");

					verify(mockDI).removeProjectProperty("proj1");
					verify(mockProject).close();
					verify(mockStorageEngine).syncLocalToStorage(anyString(), anyString(), any());
					verify(mockStorageEngine).copyToStorage(argThat(s -> s.contains(".smss")), anyString(), any());
				}
			} finally {
				setStaticFinalField(EngineUtility.class, "USER_FOLDER", origUserFolder);
			}
		}

		// ------- pullOwl happy path -------

		@Test
		void testPullOwl_withOwlEngine(@TempDir Path testDir) throws Exception {
			String origDbFolder = EngineUtility.DATABASE_FOLDER;
			try {
				setStaticFinalField(EngineUtility.class, "DATABASE_FOLDER", testDir.toString());

				Path owlFile = Files.createFile(testDir.resolve("mydb.owl"));
				IDatabaseEngine mockDb = mock(IDatabaseEngine.class);
				WriteOWLEngine mockOwlEngine = mock(WriteOWLEngine.class);

				try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
						MockedStatic<SecurityEngineUtils> secMock = mockStatic(SecurityEngineUtils.class);
						MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class)) {

					utilMock.when(() -> Utility.getDatabase("db1", false)).thenReturn(mockDb);
					secMock.when(() -> SecurityEngineUtils.getEngineAliasForId("db1")).thenReturn("Db");
					smssMock.when(() -> SmssUtilities.getUniqueName("Db", "db1")).thenReturn("Db__db1");
					smssMock.when(() -> SmssUtilities.getOwlFile(any(), any())).thenReturn(owlFile.toFile());

					instance.pullOwl("db1", mockOwlEngine);

					verify(mockOwlEngine).closeOwl();
					verify(mockStorageEngine, times(2)).copyToLocal(anyString(), anyString());
					verify(mockOwlEngine).reloadOWLFile();
					// owlEngine was provided, so close should NOT be called
					verify(mockOwlEngine, never()).close();
				}
			} finally {
				setStaticFinalField(EngineUtility.class, "DATABASE_FOLDER", origDbFolder);
			}
		}

		@Test
		void testPullOwl_nullOwlEngine_autoCloses(@TempDir Path testDir) throws Exception {
			String origDbFolder = EngineUtility.DATABASE_FOLDER;
			try {
				setStaticFinalField(EngineUtility.class, "DATABASE_FOLDER", testDir.toString());

				Path owlFile = Files.createFile(testDir.resolve("mydb.owl"));
				IDatabaseEngine mockDb = mock(IDatabaseEngine.class);
				WriteOWLEngine mockOwlEngine = mock(WriteOWLEngine.class);
				prerna.engine.impl.owl.OWLEngineFactory mockFactory = mock(
						prerna.engine.impl.owl.OWLEngineFactory.class);
				when(mockDb.getOWLEngineFactory()).thenReturn(mockFactory);
				when(mockFactory.getWriteOWL()).thenReturn(mockOwlEngine);

				try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
						MockedStatic<SecurityEngineUtils> secMock = mockStatic(SecurityEngineUtils.class);
						MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class)) {

					utilMock.when(() -> Utility.getDatabase("db1", false)).thenReturn(mockDb);
					secMock.when(() -> SecurityEngineUtils.getEngineAliasForId("db1")).thenReturn("Db");
					smssMock.when(() -> SmssUtilities.getUniqueName("Db", "db1")).thenReturn("Db__db1");
					smssMock.when(() -> SmssUtilities.getOwlFile(any(), any())).thenReturn(owlFile.toFile());

					instance.pullOwl("db1", null);

					verify(mockOwlEngine).closeOwl();
					verify(mockStorageEngine, times(2)).copyToLocal(anyString(), anyString());
					verify(mockOwlEngine).reloadOWLFile();
					// autoClose should be true - owlEngine.close() should be called
					verify(mockOwlEngine).close();
				}
			} finally {
				setStaticFinalField(EngineUtility.class, "DATABASE_FOLDER", origDbFolder);
			}
		}

		// ------- pullInsightsDB / pushInsightDB -------

		@Test
		void testPullInsightsDB_happyPath(@TempDir Path testDir) throws Exception {
			String origProjectFolder = EngineUtility.PROJECT_FOLDER;
			try {
				setStaticFinalField(EngineUtility.class, "PROJECT_FOLDER", testDir.toString());

				// Create project dir with insight db
				Path projDir = testDir.resolve("P__proj1");
				Files.createDirectories(projDir);
				Files.createFile(projDir.resolve("insights_database.sqlite"));

				IProject mockProject = mock(IProject.class);
				when(mockProject.getProjectName()).thenReturn("P");
				IRDBMSEngine mockInsightDb = mock(IRDBMSEngine.class);
				when(mockProject.getInsightDatabase()).thenReturn(mockInsightDb);
				when(mockInsightDb.getDbType()).thenReturn(RdbmsTypeEnum.SQLITE);
				ReentrantLock lock = new ReentrantLock();

				java.io.File mockInsightFile = new java.io.File(projDir.resolve("insights_database.sqlite").toString());

				try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
						MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class);
						MockedStatic<ProjectSyncUtility> syncMock = mockStatic(ProjectSyncUtility.class);
						MockedStatic<ProjectHelper> phMock = mockStatic(ProjectHelper.class)) {

					utilMock.when(() -> Utility.getProject("proj1", false)).thenReturn(mockProject);
					smssMock.when(() -> SmssUtilities.getUniqueName("P", "proj1")).thenReturn("P__proj1");
					smssMock.when(() -> SmssUtilities.getInsightsRdbmsFile(any())).thenReturn(mockInsightFile);
					syncMock.when(() -> ProjectSyncUtility.getProjectLock("proj1")).thenReturn(lock);
					phMock.when(() -> ProjectHelper.loadInsightsEngine(any(), any())).thenReturn(mockInsightDb);

					instance.pullInsightsDB("proj1");

					verify(mockInsightDb).close();
					verify(mockStorageEngine).copyToLocal(argThat(s -> s.contains("insights_database.sqlite")),
							anyString());
					verify(mockProject).setInsightDatabase(mockInsightDb);
				}
			} finally {
				setStaticFinalField(EngineUtility.class, "PROJECT_FOLDER", origProjectFolder);
			}
		}

		@Test
		void testPushInsightDB_happyPath(@TempDir Path testDir) throws Exception {
			String origProjectFolder = EngineUtility.PROJECT_FOLDER;
			try {
				setStaticFinalField(EngineUtility.class, "PROJECT_FOLDER", testDir.toString());

				Path projDir = testDir.resolve("P__proj1");
				Files.createDirectories(projDir);
				Files.createFile(projDir.resolve("insights_database.sqlite"));

				IProject mockProject = mock(IProject.class);
				when(mockProject.getProjectName()).thenReturn("P");
				IRDBMSEngine mockInsightDb = mock(IRDBMSEngine.class);
				when(mockProject.getInsightDatabase()).thenReturn(mockInsightDb);
				when(mockInsightDb.getDbType()).thenReturn(RdbmsTypeEnum.SQLITE);
				ReentrantLock lock = new ReentrantLock();

				java.io.File mockInsightFile = new java.io.File(projDir.resolve("insights_database.sqlite").toString());

				try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
						MockedStatic<SmssUtilities> smssMock = mockStatic(SmssUtilities.class);
						MockedStatic<ProjectSyncUtility> syncMock = mockStatic(ProjectSyncUtility.class);
						MockedStatic<ProjectHelper> phMock = mockStatic(ProjectHelper.class)) {

					utilMock.when(() -> Utility.getProject("proj1", false)).thenReturn(mockProject);
					smssMock.when(() -> SmssUtilities.getUniqueName("P", "proj1")).thenReturn("P__proj1");
					smssMock.when(() -> SmssUtilities.getInsightsRdbmsFile(any())).thenReturn(mockInsightFile);
					syncMock.when(() -> ProjectSyncUtility.getProjectLock("proj1")).thenReturn(lock);
					phMock.when(() -> ProjectHelper.loadInsightsEngine(any(), any())).thenReturn(mockInsightDb);

					instance.pushInsightDB("proj1");

					verify(mockInsightDb).close();
					verify(mockStorageEngine).copyToStorage(argThat(s -> s.contains("insights_database.sqlite")),
							anyString(), (Map<String, Object>) isNull());
					verify(mockProject).setInsightDatabase(mockInsightDb);
				}
			} finally {
				setStaticFinalField(EngineUtility.class, "PROJECT_FOLDER", origProjectFolder);
			}
		}

	}
}
