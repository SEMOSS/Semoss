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
package prerna.cluster.util;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import prerna.cluster.sync.IClusterSynchronizer;
import prerna.cluster.sync.impl.ClusterSynchronizerFactory;
import prerna.cluster.util.clients.CentralCloudStorage;
import prerna.engine.api.IEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.project.api.IProject;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.DefaultImageGeneratorUtil;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import sun.misc.Unsafe;

class ClusterUtilUnitTests {

	// Force-load EngineUtility safely - its clinit calls Utility.getBaseFolder()
	// which returns null in test context, causing NPE on .replace().
	static {
		try (MockedStatic<Utility> util = mockStatic(Utility.class)) {
			util.when(Utility::getBaseFolder).thenReturn(System.getProperty("java.io.tmpdir"));
			Class.forName("prerna.util.EngineUtility");
		} catch (Exception e) {
			// class already loaded or other issue - ok for tests
		}
	}

	@Nested
	class GetSubdirsTests {
		@TempDir
		Path tempDir;

		@Test
		void emptyDir_returnsEmptyList() {
			List<File> subdirs = ClusterUtil.getSubdirs(tempDir.toString());
			assertNotNull(subdirs);
			assertTrue(subdirs.isEmpty());
		}

		@Test
		void dirWithOneSub() throws IOException {
			Path child = Files.createDirectory(tempDir.resolve("subA"));
			List<File> subdirs = ClusterUtil.getSubdirs(tempDir.toString());
			assertEquals(1, subdirs.size());
			assertEquals(child.toFile().getAbsolutePath(), subdirs.get(0).getAbsolutePath());
		}

		@Test
		void nestedSubs() throws IOException {
			Path a = Files.createDirectory(tempDir.resolve("a"));
			Path b = Files.createDirectory(a.resolve("b"));
			Path c = Files.createDirectory(b.resolve("c"));
			List<File> subdirs = ClusterUtil.getSubdirs(tempDir.toString());
			assertEquals(3, subdirs.size());
			Set<String> paths = subdirs.stream().map(File::getAbsolutePath).collect(Collectors.toSet());
			assertTrue(paths.contains(a.toFile().getAbsolutePath()));
			assertTrue(paths.contains(b.toFile().getAbsolutePath()));
			assertTrue(paths.contains(c.toFile().getAbsolutePath()));
		}

		@Test
		void nonDirPath_throws() throws IOException {
			Path file = Files.createFile(tempDir.resolve("regularFile.txt"));
			assertThrows(IllegalArgumentException.class, () -> ClusterUtil.getSubdirs(file.toString()));
		}

		@Test
		void mixOfFilesAndSubdirs() throws IOException {
			Path dA = Files.createDirectory(tempDir.resolve("dirA"));
			Path dB = Files.createDirectory(tempDir.resolve("dirB"));
			Files.createFile(tempDir.resolve("file1.txt"));
			List<File> subdirs = ClusterUtil.getSubdirs(tempDir.toString());
			assertEquals(2, subdirs.size());
			Set<String> paths = subdirs.stream().map(File::getAbsolutePath).collect(Collectors.toSet());
			assertTrue(paths.contains(dA.toFile().getAbsolutePath()));
			assertTrue(paths.contains(dB.toFile().getAbsolutePath()));
		}
	}

	@Nested
	class AddHiddenFileToDirTests {
		@TempDir
		Path tempDir;

		@Test
		void createsHiddenFile() {
			File folder = tempDir.toFile();
			ClusterUtil.addHiddenFileToDir(folder);
			assertTrue(new File(folder, ".hidden").exists());
		}

		@Test
		void hiddenFileName() {
			File folder = tempDir.toFile();
			ClusterUtil.addHiddenFileToDir(folder);
			File[] files = folder.listFiles();
			assertNotNull(files);
			assertEquals(1, files.length);
			assertEquals(".hidden", files[0].getName());
		}

		@Test
		void callingTwiceNoThrow() {
			File folder = tempDir.toFile();
			ClusterUtil.addHiddenFileToDir(folder);
			assertDoesNotThrow(() -> ClusterUtil.addHiddenFileToDir(folder));
		}
	}

	@Nested
	class ValidateFolderTests {
		@TempDir
		Path tempDir;

		@Test
		void addsHiddenToEmpty() throws IOException {
			Path a = Files.createDirectory(tempDir.resolve("emptyA"));
			Path b = Files.createDirectory(tempDir.resolve("emptyB"));
			ClusterUtil.validateFolder(tempDir.toString());
			assertTrue(new File(a.toFile(), ".hidden").exists());
			assertTrue(new File(b.toFile(), ".hidden").exists());
		}

		@Test
		void doesNotAddHiddenToNonEmpty() throws IOException {
			Path ne = Files.createDirectory(tempDir.resolve("nonEmpty"));
			Files.createFile(ne.resolve("existing.txt"));
			ClusterUtil.validateFolder(tempDir.toString());
			assertFalse(new File(ne.toFile(), ".hidden").exists());
		}

		@Test
		void worksOnNestedEmpty() throws IOException {
			Path outer = Files.createDirectory(tempDir.resolve("outer"));
			Path inner = Files.createDirectory(outer.resolve("inner"));
			ClusterUtil.validateFolder(tempDir.toString());
			assertTrue(new File(inner.toFile(), ".hidden").exists());
		}
	}

	@Nested
	class StaticFieldsAndMethodsTests {
		@Test
		void testIsClusterDefaultsFalse() {
			assertFalse(ClusterUtil.IS_CLUSTER);
		}

		@Test
		void testIsClusterSyncDefaultsFalse() {
			assertFalse(ClusterSynchronizerFactory.IS_CLUSTER_SYNC_SETUP);
		}

		@Test
		void testIsClusteredSchedulerDefaultsFalse() {
			assertFalse(ClusterUtil.IS_CLUSTERED_SCHEDULER);
		}

		@Test
		void testRemoteRserveDefaultsFalse() {
			assertFalse(ClusterUtil.REMOTE_RSERVE);
		}

		@Test
		void testLoadEnginesLocallyDefaultsFalse() {
			assertFalse(ClusterUtil.LOAD_ENGINES_LOCALLY);
		}

		@Test
		void testStorageProviderIsNullOrEmpty() {
			assertTrue(ClusterUtil.STORAGE_PROVIDER == null || ClusterUtil.STORAGE_PROVIDER.isEmpty());
		}

		@Test
		void testIsSchedulerExecutor_true() {
			assertTrue(ClusterUtil.isSchedulerExecutor());
		}

		@Test
		void testpullEngine_1arg_noThrow() {
			assertDoesNotThrow(() -> ClusterUtil.pullEngine("id"));
		}

		@Test
		void testpushEngine_noThrow() {
			assertDoesNotThrow(() -> ClusterUtil.pushEngine("id"));
		}

		@Test
		void testpushEngineSmss_1arg_noThrow() {
			assertDoesNotThrow(() -> ClusterUtil.pushEngineSmss("id"));
		}

		@Test
		void testpushProjectSmss_noThrow() {
			assertDoesNotThrow(() -> ClusterUtil.pushProjectSmss("id"));
		}

		@Test
		void testpullProject_1arg_noThrow() {
			assertDoesNotThrow(() -> ClusterUtil.pullProject("id"));
		}

		@Test
		void testpushProject_noThrow() {
			assertDoesNotThrow(() -> ClusterUtil.pushProject("id"));
		}

		@Test
		void testdeleteEngine_1arg_noThrow() {
			assertDoesNotThrow(() -> ClusterUtil.deleteEngine("id"));
		}

		@Test
		void testdeleteProject_noThrow() {
			assertDoesNotThrow(() -> ClusterUtil.deleteProject("id"));
		}
	}

	@Nested
	class AdditionalNoOpMethodsTests {
		@Test
		void testPullEng_DATABASE() {
			assertDoesNotThrow(() -> ClusterUtil.pullEngine("e", IEngine.CATALOG_TYPE.DATABASE));
		}

		@Test
		void testPullEng_STORAGE() {
			assertDoesNotThrow(() -> ClusterUtil.pullEngine("e", IEngine.CATALOG_TYPE.STORAGE));
		}

		@Test
		void testPullEng_MODEL() {
			assertDoesNotThrow(() -> ClusterUtil.pullEngine("e", IEngine.CATALOG_TYPE.MODEL));
		}

		@Test
		void testPullEng_VECTOR() {
			assertDoesNotThrow(() -> ClusterUtil.pullEngine("e", IEngine.CATALOG_TYPE.VECTOR));
		}

		@Test
		void testPullEng_FUNCTION() {
			assertDoesNotThrow(() -> ClusterUtil.pullEngine("e", IEngine.CATALOG_TYPE.FUNCTION));
		}

		@Test
		void testPullEng_3arg_false() {
			assertDoesNotThrow(() -> ClusterUtil.pullEngine("e", IEngine.CATALOG_TYPE.DATABASE, false));
		}

		@Test
		void testPullEng_3arg_true() {
			assertDoesNotThrow(() -> ClusterUtil.pullEngine("e", IEngine.CATALOG_TYPE.DATABASE, true));
		}

		@Test
		void testPullEng_3arg_allTypes() {
			for (IEngine.CATALOG_TYPE type : IEngine.CATALOG_TYPE.values()) {
				assertDoesNotThrow(() -> ClusterUtil.pullEngine("e", type, false));
			}
		}

		@Test
		void testPushEngSmss_DATABASE() {
			assertDoesNotThrow(() -> ClusterUtil.pushEngineSmss("e", IEngine.CATALOG_TYPE.DATABASE));
		}

		@Test
		void testPushEngSmss_STORAGE() {
			assertDoesNotThrow(() -> ClusterUtil.pushEngineSmss("e", IEngine.CATALOG_TYPE.STORAGE));
		}

		@Test
		void testPushEngSmss_MODEL() {
			assertDoesNotThrow(() -> ClusterUtil.pushEngineSmss("e", IEngine.CATALOG_TYPE.MODEL));
		}

		@Test
		void testDelEng_DATABASE() {
			assertDoesNotThrow(() -> ClusterUtil.deleteEngine("e", IEngine.CATALOG_TYPE.DATABASE));
		}

		@Test
		void testDelEng_STORAGE() {
			assertDoesNotThrow(() -> ClusterUtil.deleteEngine("e", IEngine.CATALOG_TYPE.STORAGE));
		}

		@Test
		void testDelEng_MODEL() {
			assertDoesNotThrow(() -> ClusterUtil.deleteEngine("e", IEngine.CATALOG_TYPE.MODEL));
		}

		@Test
		void testCopyLocal() {
			assertDoesNotThrow(
					() -> ClusterUtil.copyLocalFileToEngineCloudFolder("e", IEngine.CATALOG_TYPE.DATABASE, "/p"));
		}

		@Test
		void testCopyLocal_S() {
			assertDoesNotThrow(
					() -> ClusterUtil.copyLocalFileToEngineCloudFolder("e", IEngine.CATALOG_TYPE.STORAGE, "/p"));
		}

		@Test
		void testCopyCloud() {
			assertDoesNotThrow(
					() -> ClusterUtil.copyEngineCloudFileToLocalFile("e", IEngine.CATALOG_TYPE.DATABASE, "/p"));
		}

		@Test
		void testCopyCloud_M() {
			assertDoesNotThrow(() -> ClusterUtil.copyEngineCloudFileToLocalFile("e", IEngine.CATALOG_TYPE.MODEL, "/p"));
		}

		@Test
		void testDelCloud() {
			assertDoesNotThrow(() -> ClusterUtil.deleteEngineCloudFile("e", IEngine.CATALOG_TYPE.DATABASE, "/p"));
		}

		@Test
		void testDelCloud_V() {
			assertDoesNotThrow(() -> ClusterUtil.deleteEngineCloudFile("e", IEngine.CATALOG_TYPE.VECTOR, "/p"));
		}

		@Test
		void testPullImgFolder_DB() {
			assertDoesNotThrow(() -> ClusterUtil.pullEngineAndProjectImageFolder(IEngine.CATALOG_TYPE.DATABASE));
		}

		@Test
		void testPullImgFolder_P() {
			assertDoesNotThrow(() -> ClusterUtil.pullEngineAndProjectImageFolder(IEngine.CATALOG_TYPE.PROJECT));
		}

		@Test
		void testPullImgFolder_all() {
			for (IEngine.CATALOG_TYPE t : IEngine.CATALOG_TYPE.values()) {
				assertDoesNotThrow(() -> ClusterUtil.pullEngineAndProjectImageFolder(t));
			}
		}

		@Test
		void testPushImg_DB() {
			assertDoesNotThrow(() -> ClusterUtil.pushEngineAndProjectImage(IEngine.CATALOG_TYPE.DATABASE, "i.png"));
		}

		@Test
		void testPushImg_P() {
			assertDoesNotThrow(() -> ClusterUtil.pushEngineAndProjectImage(IEngine.CATALOG_TYPE.PROJECT, "i.jpg"));
		}

		@Test
		void testDelImg_DB() {
			assertDoesNotThrow(() -> ClusterUtil.deleteEngineAndProjectImage(IEngine.CATALOG_TYPE.DATABASE, "i.png"));
		}

		@Test
		void testDelImg_S() {
			assertDoesNotThrow(() -> ClusterUtil.deleteEngineAndProjectImage(IEngine.CATALOG_TYPE.STORAGE, "i.png"));
		}

		@Test
		void testDelImgById_DB() {
			assertDoesNotThrow(
					() -> ClusterUtil.deleteEngineAndProjectImageById(IEngine.CATALOG_TYPE.DATABASE, "e123"));
		}

		@Test
		void testDelImgById_P() {
			assertDoesNotThrow(() -> ClusterUtil.deleteEngineAndProjectImageById(IEngine.CATALOG_TYPE.PROJECT, "p456"));
		}

		@Test
		void testPullProj_2f() {
			assertDoesNotThrow(() -> ClusterUtil.pullProject("p", false));
		}

		@Test
		void testPullProj_2t() {
			assertDoesNotThrow(() -> ClusterUtil.pullProject("p", true));
		}

		@Test
		void testPullInsDB() {
			assertDoesNotThrow(() -> ClusterUtil.pullInsightsDB("p"));
		}

		@Test
		void testPullInsDB_o() {
			assertDoesNotThrow(() -> ClusterUtil.pullInsightsDB("o"));
		}

		@Test
		void testPushInsDB() {
			assertDoesNotThrow(() -> ClusterUtil.pushInsightDB("p"));
		}

		@Test
		void testPushInsDB_o() {
			assertDoesNotThrow(() -> ClusterUtil.pushInsightDB("o"));
		}

		@Test
		void testPullOwl1() {
			assertDoesNotThrow(() -> ClusterUtil.pullOwl("d"));
		}

		@Test
		void testPullOwlN() {
			assertDoesNotThrow(() -> ClusterUtil.pullOwl("d", null));
		}

		@Test
		void testPushOwl1() {
			assertDoesNotThrow(() -> ClusterUtil.pushOwl("d"));
		}

		@Test
		void testPushOwlN() {
			assertDoesNotThrow(() -> ClusterUtil.pushOwl("d", null));
		}

		@Test
		void testPushProjF3() {
			assertDoesNotThrow(() -> ClusterUtil.pushProjectFolder("p", "/a", "r"));
		}

		@Test
		void testPushProjFE() {
			assertDoesNotThrow(() -> ClusterUtil.pushProjectFolder("", "", ""));
		}

		@Test
		void testPullProjF3() {
			assertDoesNotThrow(() -> ClusterUtil.pullProjectFolder("p", "/a", "r"));
		}

		@Test
		void testPullProjFE() {
			assertDoesNotThrow(() -> ClusterUtil.pullProjectFolder("", "", ""));
		}

		@Test
		void testPushEngF3() {
			assertDoesNotThrow(() -> ClusterUtil.pushEngineFolder("e", "/a", "r"));
		}

		@Test
		void testPushEngFE() {
			assertDoesNotThrow(() -> ClusterUtil.pushEngineFolder("", "", ""));
		}

		@Test
		void testPullEngF3() {
			assertDoesNotThrow(() -> ClusterUtil.pullEngineFolder("e", "/a", "r"));
		}

		@Test
		void testPullEngFE() {
			assertDoesNotThrow(() -> ClusterUtil.pullEngineFolder("", "", ""));
		}

		@Test
		void testPushIns() {
			assertDoesNotThrow(() -> ClusterUtil.pushInsight("p", "r"));
		}

		@Test
		void testPushIns_o() {
			assertDoesNotThrow(() -> ClusterUtil.pushInsight("p2", "r2"));
		}

		@Test
		void testPullIns() {
			assertDoesNotThrow(() -> ClusterUtil.pullInsight("p", "r"));
		}

		@Test
		void testPullIns_o() {
			assertDoesNotThrow(() -> ClusterUtil.pullInsight("p2", "r2"));
		}

		@Test
		void testPushInsImg() {
			assertDoesNotThrow(() -> ClusterUtil.pushInsightImage("p", "i", "old", "new"));
		}

		@Test
		void testPushInsImgN() {
			assertDoesNotThrow(() -> ClusterUtil.pushInsightImage("p", "i", null, "new"));
		}

		@Test
		void testPushRoom() {
			assertDoesNotThrow(() -> ClusterUtil.pushRoom("rm"));
		}

		@Test
		void testPushRoom_o() {
			assertDoesNotThrow(() -> ClusterUtil.pushRoom("rm2"));
		}

		@Test
		void testPullRoom() {
			assertDoesNotThrow(() -> ClusterUtil.pullRoom("rm"));
		}

		@Test
		void testPullRoom_o() {
			assertDoesNotThrow(() -> ClusterUtil.pullRoom("rm2"));
		}

		@Test
		void testPushUserAsset() {
			assertDoesNotThrow(() -> ClusterUtil.pushUserAsset("p"));
		}

		@Test
		void testPullUserAsset_1arg() {
			assertDoesNotThrow(() -> ClusterUtil.pullUserAsset("p"));
		}

		@Test
		void testPullUserAsset_notAlreadyLoaded() {
			assertDoesNotThrow(() -> ClusterUtil.pullUserAsset("p", false));
		}

		@Test
		void testPullUserAsset_alreadyLoaded() {
			assertDoesNotThrow(() -> ClusterUtil.pullUserAsset("p", true));
		}
	}

	// -- Helpers for modifying static final fields --------------------------

	private static Unsafe getUnsafe() throws Exception {
		Field f = Unsafe.class.getDeclaredField("theUnsafe");
		f.setAccessible(true);
		return (Unsafe) f.get(null);
	}

	private static void setStaticFinalBoolean(Class<?> clazz, String fieldName, boolean value) throws Exception {
		Unsafe unsafe = getUnsafe();
		Field target = clazz.getDeclaredField(fieldName);
		Object base = unsafe.staticFieldBase(target);
		long offset = unsafe.staticFieldOffset(target);
		unsafe.putBoolean(base, offset, value);
	}

	// -- IS_CLUSTER=true delegation tests -----------------------------------

	@Nested
	class ClusteredDelegationTests {

		@Test
		void pullEngine_1arg() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.pullEngine("e1");
				verify(mock).pullEngine("e1");
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullEngine_2arg() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.pullEngine("e2", IEngine.CATALOG_TYPE.DATABASE);
				verify(mock).pullEngine("e2", IEngine.CATALOG_TYPE.DATABASE);
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullEngine_3arg() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.pullEngine("e3", IEngine.CATALOG_TYPE.MODEL, true);
				verify(mock).pullEngine("e3", IEngine.CATALOG_TYPE.MODEL, true);
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushEngine() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.pushEngine("e4");
				verify(mock).pushEngine("e4");
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushEngineSmss_1arg() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.pushEngineSmss("e5");
				verify(mock).pushEngineSmss("e5");
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushEngineSmss_2arg() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.pushEngineSmss("e6", IEngine.CATALOG_TYPE.STORAGE);
				verify(mock).pushEngineSmss("e6", IEngine.CATALOG_TYPE.STORAGE);
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void deleteEngine_1arg() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.deleteEngine("e7");
				verify(mock).deleteEngine("e7");
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void deleteEngine_2arg() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.deleteEngine("e8", IEngine.CATALOG_TYPE.VECTOR);
				verify(mock).deleteEngine("e8", IEngine.CATALOG_TYPE.VECTOR);
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void copyLocalFileToEngineCloudFolder() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.copyLocalFileToEngineCloudFolder("e9", IEngine.CATALOG_TYPE.DATABASE, "/f");
				verify(mock).copyLocalFileToEngineCloudFolder("e9", IEngine.CATALOG_TYPE.DATABASE, "/f");
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void copyEngineCloudFileToLocalFile() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.copyEngineCloudFileToLocalFile("e10", IEngine.CATALOG_TYPE.MODEL, "/g");
				verify(mock).copyEngineCloudFileToLocalFile("e10", IEngine.CATALOG_TYPE.MODEL, "/g");
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void deleteEngineCloudFile() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.deleteEngineCloudFile("e11", IEngine.CATALOG_TYPE.FUNCTION, "/h");
				verify(mock).deleteEngineCloudFile("e11", IEngine.CATALOG_TYPE.FUNCTION, "/h");
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullEngineAndProjectImageFolder() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.pullEngineAndProjectImageFolder(IEngine.CATALOG_TYPE.DATABASE);
				verify(mock).pullEngineAndProjectImageFolder(IEngine.CATALOG_TYPE.DATABASE);
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushEngineAndProjectImage() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.pushEngineAndProjectImage(IEngine.CATALOG_TYPE.PROJECT, "img.png");
				verify(mock).pushEngineAndProjectImage(IEngine.CATALOG_TYPE.PROJECT, "img.png");
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void deleteEngineAndProjectImage() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.deleteEngineAndProjectImage(IEngine.CATALOG_TYPE.STORAGE, "del.png");
				verify(mock).deleteEngineAndProjectImage(IEngine.CATALOG_TYPE.STORAGE, "del.png");
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void deleteEngineAndProjectImageById() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.deleteEngineAndProjectImageById(IEngine.CATALOG_TYPE.DATABASE, "e12");
				verify(mock).deleteEngineAndProjectImageById(IEngine.CATALOG_TYPE.DATABASE, "e12");
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullProject_1arg() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.pullProject("p1");
				verify(mock).pullProject("p1");
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullProject_2arg() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.pullProject("p2", true);
				verify(mock).pullProject("p2", true);
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void deleteProject() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.deleteProject("p3");
				verify(mock).deleteProject("p3");
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullInsightsDB() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.pullInsightsDB("p4");
				verify(mock).pullInsightsDB("p4");
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushInsightDB() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.pushInsightDB("p5");
				verify(mock).pushInsightDB("p5");
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullOwl_1arg() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.pullOwl("d1");
				verify(mock).pullOwl("d1", null);
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullOwl_2arg() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				WriteOWLEngine owlEng = mock(WriteOWLEngine.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.pullOwl("d2", owlEng);
				verify(mock).pullOwl("d2", owlEng);
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushOwl_1arg() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.pushOwl("d3");
				verify(mock).pushOwl("d3", null);
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushOwl_2arg() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				WriteOWLEngine owlEng = mock(WriteOWLEngine.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.pushOwl("d4", owlEng);
				verify(mock).pushOwl("d4", owlEng);
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushProject() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.pushProject("p6");
				verify(mock).pushProject("p6");
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushProjectSmss() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.pushProjectSmss("p7");
				verify(mock).pushProjectSmss("p7");
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushProjectFolder_3arg() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.pushProjectFolder("p8", "/abs", "rel");
				verify(mock).pushProjectFolder("p8", "/abs", "rel");
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullProjectFolder_3arg() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.pullProjectFolder("p9", "/abs2", "rel2");
				verify(mock).pullProjectFolder("p9", "/abs2", "rel2");
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushEngineFolder_3arg() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.pushEngineFolder("e13", "/abs3", "rel3");
				verify(mock).pushEngineFolder("e13", "/abs3", "rel3");
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullEngineFolder_3arg() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.pullEngineFolder("e14", "/abs4", "rel4");
				verify(mock).pullEngineFolder("e14", "/abs4", "rel4");
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushInsight() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.pushInsight("p10", "r1");
				verify(mock).pushInsight("p10", "r1");
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullInsight() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.pullInsight("p11", "r2");
				verify(mock).pullInsight("p11", "r2");
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushInsightImage() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.pushInsightImage("p12", "ins1", "old.png", "new.png");
				verify(mock).pushInsightImage("p12", "ins1", "old.png", "new.png");
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushRoom() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.pushRoom("rm1");
				verify(mock).pushRoomFolderToCloud("rm1");
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullRoom() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.pullRoom("rm2");
				verify(mock).pullRoomFolderFromCloud("rm2");
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushUserAsset() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.pushUserAsset("p13");
				verify(mock).pushUserAsset("p13");
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullUserAsset_1arg() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.pullUserAsset("p14");
				verify(mock).pullUserAsset("p14", false);
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullUserAsset_2arg() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				ClusterUtil.pullUserAsset("p15", true);
				verify(mock).pullUserAsset("p15", true);
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}
	}

	// -- Error path tests (CCS throws -> SemossPixelException) ---------------

	@Nested
	class ClusteredErrorTests {

		@Test
		void pullEngine_1arg_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).pullEngine("e1");
				SemossPixelException ex = assertThrows(SemossPixelException.class, () -> ClusterUtil.pullEngine("e1"));
				assertFalse(ex.isContinueThreadOfExecution());
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullEngine_2arg_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).pullEngine("e2", IEngine.CATALOG_TYPE.DATABASE);
				assertThrows(SemossPixelException.class,
						() -> ClusterUtil.pullEngine("e2", IEngine.CATALOG_TYPE.DATABASE));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullEngine_3arg_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).pullEngine("e3", IEngine.CATALOG_TYPE.MODEL, false);
				assertThrows(SemossPixelException.class,
						() -> ClusterUtil.pullEngine("e3", IEngine.CATALOG_TYPE.MODEL, false));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushEngine_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).pushEngine("e4");
				SemossPixelException ex = assertThrows(SemossPixelException.class, () -> ClusterUtil.pushEngine("e4"));
				assertFalse(ex.isContinueThreadOfExecution());
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushEngineSmss_1arg_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).pushEngineSmss("e5");
				assertThrows(SemossPixelException.class, () -> ClusterUtil.pushEngineSmss("e5"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushEngineSmss_2arg_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).pushEngineSmss("e6", IEngine.CATALOG_TYPE.STORAGE);
				assertThrows(SemossPixelException.class,
						() -> ClusterUtil.pushEngineSmss("e6", IEngine.CATALOG_TYPE.STORAGE));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void deleteEngine_1arg_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).deleteEngine("e7");
				assertThrows(SemossPixelException.class, () -> ClusterUtil.deleteEngine("e7"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void deleteEngine_2arg_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).deleteEngine("e8", IEngine.CATALOG_TYPE.VECTOR);
				assertThrows(SemossPixelException.class,
						() -> ClusterUtil.deleteEngine("e8", IEngine.CATALOG_TYPE.VECTOR));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void copyLocal_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).copyLocalFileToEngineCloudFolder("e",
						IEngine.CATALOG_TYPE.DATABASE, "/f");
				assertThrows(SemossPixelException.class,
						() -> ClusterUtil.copyLocalFileToEngineCloudFolder("e", IEngine.CATALOG_TYPE.DATABASE, "/f"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void copyCloud_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).copyEngineCloudFileToLocalFile("e",
						IEngine.CATALOG_TYPE.MODEL, "/g");
				assertThrows(SemossPixelException.class,
						() -> ClusterUtil.copyEngineCloudFileToLocalFile("e", IEngine.CATALOG_TYPE.MODEL, "/g"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void deleteCloudFile_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).deleteEngineCloudFile("e",
						IEngine.CATALOG_TYPE.FUNCTION, "/h");
				assertThrows(SemossPixelException.class,
						() -> ClusterUtil.deleteEngineCloudFile("e", IEngine.CATALOG_TYPE.FUNCTION, "/h"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullImageFolder_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock)
						.pullEngineAndProjectImageFolder(IEngine.CATALOG_TYPE.DATABASE);
				assertThrows(SemossPixelException.class,
						() -> ClusterUtil.pullEngineAndProjectImageFolder(IEngine.CATALOG_TYPE.DATABASE));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushImage_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).pushEngineAndProjectImage(IEngine.CATALOG_TYPE.PROJECT,
						"i.png");
				assertThrows(SemossPixelException.class,
						() -> ClusterUtil.pushEngineAndProjectImage(IEngine.CATALOG_TYPE.PROJECT, "i.png"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void deleteImage_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock)
						.deleteEngineAndProjectImage(IEngine.CATALOG_TYPE.STORAGE, "d.png");
				assertThrows(SemossPixelException.class,
						() -> ClusterUtil.deleteEngineAndProjectImage(IEngine.CATALOG_TYPE.STORAGE, "d.png"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void deleteImageById_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock)
						.deleteEngineAndProjectImageById(IEngine.CATALOG_TYPE.DATABASE, "e12");
				assertThrows(SemossPixelException.class,
						() -> ClusterUtil.deleteEngineAndProjectImageById(IEngine.CATALOG_TYPE.DATABASE, "e12"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullProject_1arg_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).pullProject("p1");
				assertThrows(SemossPixelException.class, () -> ClusterUtil.pullProject("p1"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullProject_2arg_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).pullProject("p2", false);
				assertThrows(SemossPixelException.class, () -> ClusterUtil.pullProject("p2", false));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void deleteProject_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).deleteProject("p3");
				assertThrows(SemossPixelException.class, () -> ClusterUtil.deleteProject("p3"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullInsightsDB_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).pullInsightsDB("p4");
				assertThrows(SemossPixelException.class, () -> ClusterUtil.pullInsightsDB("p4"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushInsightDB_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).pushInsightDB("p5");
				assertThrows(SemossPixelException.class, () -> ClusterUtil.pushInsightDB("p5"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullOwl_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).pullOwl(eq("d1"), any());
				assertThrows(SemossPixelException.class, () -> ClusterUtil.pullOwl("d1"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushOwl_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).pushOwl(eq("d2"), any());
				assertThrows(SemossPixelException.class, () -> ClusterUtil.pushOwl("d2"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushProject_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).pushProject("p6");
				assertThrows(SemossPixelException.class, () -> ClusterUtil.pushProject("p6"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushProjectSmss_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).pushProjectSmss("p7");
				assertThrows(SemossPixelException.class, () -> ClusterUtil.pushProjectSmss("p7"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushProjectFolder_3arg_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).pushProjectFolder("p8", "/abs", "rel");
				assertThrows(SemossPixelException.class, () -> ClusterUtil.pushProjectFolder("p8", "/abs", "rel"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullProjectFolder_3arg_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).pullProjectFolder("p9", "/abs2", "rel2");
				assertThrows(SemossPixelException.class, () -> ClusterUtil.pullProjectFolder("p9", "/abs2", "rel2"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushEngineFolder_3arg_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).pushEngineFolder("e13", "/abs3", "rel3");
				assertThrows(SemossPixelException.class, () -> ClusterUtil.pushEngineFolder("e13", "/abs3", "rel3"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullEngineFolder_3arg_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).pullEngineFolder("e14", "/abs4", "rel4");
				assertThrows(SemossPixelException.class, () -> ClusterUtil.pullEngineFolder("e14", "/abs4", "rel4"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushInsight_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).pushInsight("p10", "r1");
				assertThrows(SemossPixelException.class, () -> ClusterUtil.pushInsight("p10", "r1"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullInsight_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).pullInsight("p11", "r2");
				assertThrows(SemossPixelException.class, () -> ClusterUtil.pullInsight("p11", "r2"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushInsightImage_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).pushInsightImage("p", "i", "old", "new");
				assertThrows(SemossPixelException.class, () -> ClusterUtil.pushInsightImage("p", "i", "old", "new"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushRoom_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).pushRoomFolderToCloud("rm1");
				assertThrows(SemossPixelException.class, () -> ClusterUtil.pushRoom("rm1"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullRoom_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).pullRoomFolderFromCloud("rm2");
				assertThrows(SemossPixelException.class, () -> ClusterUtil.pullRoom("rm2"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushUserAsset_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).pushUserAsset("p");
				assertThrows(SemossPixelException.class, () -> ClusterUtil.pushUserAsset("p"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullUserAsset_1arg_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).pullUserAsset("p", false);
				assertThrows(SemossPixelException.class, () -> ClusterUtil.pullUserAsset("p"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullUserAsset_2arg_error() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				doThrow(new RuntimeException("fail")).when(mock).pullUserAsset("p", true);
				assertThrows(SemossPixelException.class, () -> ClusterUtil.pullUserAsset("p", true));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}
	}

	// -- ZK synchronizer tests ----------------------------------------------

	@Nested
	class ZkSynchronizerTests {

		@Test
		void pushEngine_publishesZk() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<ClusterSynchronizerFactory> zk = mockStatic(ClusterSynchronizerFactory.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				IClusterSynchronizer mockSync = mock(IClusterSynchronizer.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);
				zk.when(ClusterSynchronizerFactory::getClusterSynchronizer).thenReturn(mockSync);
				ClusterUtil.pushEngine("eng");
				verify(mockStorage).pushEngine("eng");
				verify(mockSync).publishEngineChange(eq("eng"), eq(ClusterSyncMethod.PULL_ENGINE), eq("eng"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
				setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", false);
			}
		}

		@Test
		void pushEngineSmss_1arg_publishesZk() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<ClusterSynchronizerFactory> zk = mockStatic(ClusterSynchronizerFactory.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				IClusterSynchronizer mockSync = mock(IClusterSynchronizer.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);
				zk.when(ClusterSynchronizerFactory::getClusterSynchronizer).thenReturn(mockSync);
				ClusterUtil.pushEngineSmss("eng2");
				verify(mockSync).publishEngineChange(eq("eng2"), eq(ClusterSyncMethod.PULL_ENGINE), eq("eng2"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
				setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", false);
			}
		}

		@Test
		void pushEngineSmss_2arg_publishesZk() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<ClusterSynchronizerFactory> zk = mockStatic(ClusterSynchronizerFactory.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				IClusterSynchronizer mockSync = mock(IClusterSynchronizer.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);
				zk.when(ClusterSynchronizerFactory::getClusterSynchronizer).thenReturn(mockSync);
				ClusterUtil.pushEngineSmss("eng3", IEngine.CATALOG_TYPE.MODEL);
				verify(mockSync).publishEngineChange(eq("eng3"), eq(ClusterSyncMethod.PULL_ENGINE), eq("eng3"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
				setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", false);
			}
		}

		@Test
		void pushInsightDB_publishesZk() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<ClusterSynchronizerFactory> zk = mockStatic(ClusterSynchronizerFactory.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				IClusterSynchronizer mockSync = mock(IClusterSynchronizer.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);
				zk.when(ClusterSynchronizerFactory::getClusterSynchronizer).thenReturn(mockSync);
				ClusterUtil.pushInsightDB("proj");
				verify(mockSync).publishProjectChange(eq("proj"), eq(ClusterSyncMethod.PULL_INSIGHTS_DB), eq("proj"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
				setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", false);
			}
		}

		@Test
		void pushOwl_publishesZk() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<ClusterSynchronizerFactory> zk = mockStatic(ClusterSynchronizerFactory.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				IClusterSynchronizer mockSync = mock(IClusterSynchronizer.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);
				zk.when(ClusterSynchronizerFactory::getClusterSynchronizer).thenReturn(mockSync);
				ClusterUtil.pushOwl("db1");
				verify(mockSync).publishEngineChange(eq("db1"), eq(ClusterSyncMethod.PULL_OWL), eq("db1"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
				setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", false);
			}
		}

		@Test
		void pushProject_publishesZk() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<ClusterSynchronizerFactory> zk = mockStatic(ClusterSynchronizerFactory.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				IClusterSynchronizer mockSync = mock(IClusterSynchronizer.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);
				zk.when(ClusterSynchronizerFactory::getClusterSynchronizer).thenReturn(mockSync);
				ClusterUtil.pushProject("proj2");
				verify(mockSync).publishProjectChange(eq("proj2"), eq(ClusterSyncMethod.PULL_PROJECT), eq("proj2"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
				setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", false);
			}
		}

		@Test
		void pushProjectFolder_3arg_publishesZk() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<ClusterSynchronizerFactory> zk = mockStatic(ClusterSynchronizerFactory.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				IClusterSynchronizer mockSync = mock(IClusterSynchronizer.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);
				zk.when(ClusterSynchronizerFactory::getClusterSynchronizer).thenReturn(mockSync);
				ClusterUtil.pushProjectFolder("proj3", "/a", "r");
				verify(mockSync).publishProjectChange(eq("proj3"), eq(ClusterSyncMethod.PULL_PROJECT_FOLDER),
						eq("proj3"), eq("/a"), eq("r"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
				setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", false);
			}
		}

		@Test
		void pushEngineFolder_3arg_publishesZk() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<ClusterSynchronizerFactory> zk = mockStatic(ClusterSynchronizerFactory.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				IClusterSynchronizer mockSync = mock(IClusterSynchronizer.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);
				zk.when(ClusterSynchronizerFactory::getClusterSynchronizer).thenReturn(mockSync);
				ClusterUtil.pushEngineFolder("eng4", "/b", "s");
				verify(mockSync).publishEngineChange(eq("eng4"), eq(ClusterSyncMethod.PULL_ENGINE_FOLDER), eq("eng4"),
						eq("/b"), eq("s"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
				setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", false);
			}
		}

		@Test
		void pushInsight_publishesZk() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<ClusterSynchronizerFactory> zk = mockStatic(ClusterSynchronizerFactory.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				IClusterSynchronizer mockSync = mock(IClusterSynchronizer.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);
				zk.when(ClusterSynchronizerFactory::getClusterSynchronizer).thenReturn(mockSync);
				ClusterUtil.pushInsight("proj4", "rdbms1");
				verify(mockSync).publishProjectChange(eq("proj4"), eq(ClusterSyncMethod.PULL_INSIGHT), eq("proj4"),
						eq("rdbms1"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
				setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", false);
			}
		}

		@Test
		void pushUserAsset_publishesZk() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<ClusterSynchronizerFactory> zk = mockStatic(ClusterSynchronizerFactory.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				IClusterSynchronizer mockSync = mock(IClusterSynchronizer.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);
				zk.when(ClusterSynchronizerFactory::getClusterSynchronizer).thenReturn(mockSync);
				ClusterUtil.pushUserAsset("proj5");
				verify(mockSync).publishUserChange(eq("proj5"), eq(ClusterSyncMethod.PULL_USER_ASSET), eq("proj5"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
				setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", false);
			}
		}

		// ZK error paths
		@Test
		void pushEngine_zkError_throwsContinuable() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<ClusterSynchronizerFactory> zk = mockStatic(ClusterSynchronizerFactory.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);
				zk.when(ClusterSynchronizerFactory::getClusterSynchronizer).thenThrow(new RuntimeException("zk fail"));
				SemossPixelException ex = assertThrows(SemossPixelException.class, () -> ClusterUtil.pushEngine("eng"));
				assertTrue(ex.isContinueThreadOfExecution());
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
				setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", false);
			}
		}

		@Test
		void pushEngineSmss_1arg_zkError() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<ClusterSynchronizerFactory> zk = mockStatic(ClusterSynchronizerFactory.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);
				zk.when(ClusterSynchronizerFactory::getClusterSynchronizer).thenThrow(new RuntimeException("zk fail"));
				SemossPixelException ex = assertThrows(SemossPixelException.class,
						() -> ClusterUtil.pushEngineSmss("eng2"));
				assertTrue(ex.isContinueThreadOfExecution());
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
				setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", false);
			}
		}

		@Test
		void pushEngineSmss_2arg_zkError() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<ClusterSynchronizerFactory> zk = mockStatic(ClusterSynchronizerFactory.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);
				zk.when(ClusterSynchronizerFactory::getClusterSynchronizer).thenThrow(new RuntimeException("zk fail"));
				SemossPixelException ex = assertThrows(SemossPixelException.class,
						() -> ClusterUtil.pushEngineSmss("eng3", IEngine.CATALOG_TYPE.MODEL));
				assertTrue(ex.isContinueThreadOfExecution());
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
				setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", false);
			}
		}

		@Test
		void pushInsightDB_zkError() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<ClusterSynchronizerFactory> zk = mockStatic(ClusterSynchronizerFactory.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);
				zk.when(ClusterSynchronizerFactory::getClusterSynchronizer).thenThrow(new RuntimeException("zk fail"));
				SemossPixelException ex = assertThrows(SemossPixelException.class,
						() -> ClusterUtil.pushInsightDB("proj"));
				assertTrue(ex.isContinueThreadOfExecution());
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
				setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", false);
			}
		}

		@Test
		void pushOwl_zkError() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<ClusterSynchronizerFactory> zk = mockStatic(ClusterSynchronizerFactory.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);
				zk.when(ClusterSynchronizerFactory::getClusterSynchronizer).thenThrow(new RuntimeException("zk fail"));
				SemossPixelException ex = assertThrows(SemossPixelException.class, () -> ClusterUtil.pushOwl("db1"));
				assertTrue(ex.isContinueThreadOfExecution());
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
				setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", false);
			}
		}

		@Test
		void pushProject_zkError() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<ClusterSynchronizerFactory> zk = mockStatic(ClusterSynchronizerFactory.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);
				zk.when(ClusterSynchronizerFactory::getClusterSynchronizer).thenThrow(new RuntimeException("zk fail"));
				SemossPixelException ex = assertThrows(SemossPixelException.class,
						() -> ClusterUtil.pushProject("proj2"));
				assertTrue(ex.isContinueThreadOfExecution());
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
				setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", false);
			}
		}

		@Test
		void pushProjectFolder_zkError() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<ClusterSynchronizerFactory> zk = mockStatic(ClusterSynchronizerFactory.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);
				zk.when(ClusterSynchronizerFactory::getClusterSynchronizer).thenThrow(new RuntimeException("zk fail"));
				SemossPixelException ex = assertThrows(SemossPixelException.class,
						() -> ClusterUtil.pushProjectFolder("p", "/a", "r"));
				assertTrue(ex.isContinueThreadOfExecution());
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
				setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", false);
			}
		}

		@Test
		void pushEngineFolder_zkError() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<ClusterSynchronizerFactory> zk = mockStatic(ClusterSynchronizerFactory.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);
				zk.when(ClusterSynchronizerFactory::getClusterSynchronizer).thenThrow(new RuntimeException("zk fail"));
				SemossPixelException ex = assertThrows(SemossPixelException.class,
						() -> ClusterUtil.pushEngineFolder("e", "/b", "s"));
				assertTrue(ex.isContinueThreadOfExecution());
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
				setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", false);
			}
		}

		@Test
		void pushInsight_zkError() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<ClusterSynchronizerFactory> zk = mockStatic(ClusterSynchronizerFactory.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);
				zk.when(ClusterSynchronizerFactory::getClusterSynchronizer).thenThrow(new RuntimeException("zk fail"));
				SemossPixelException ex = assertThrows(SemossPixelException.class,
						() -> ClusterUtil.pushInsight("proj4", "rdbms1"));
				assertTrue(ex.isContinueThreadOfExecution());
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
				setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", false);
			}
		}

		@Test
		void pushUserAsset_zkError() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<ClusterSynchronizerFactory> zk = mockStatic(ClusterSynchronizerFactory.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);
				zk.when(ClusterSynchronizerFactory::getClusterSynchronizer).thenThrow(new RuntimeException("zk fail"));
				SemossPixelException ex = assertThrows(SemossPixelException.class,
						() -> ClusterUtil.pushUserAsset("proj5"));
				assertTrue(ex.isContinueThreadOfExecution());
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
				setStaticFinalBoolean(ClusterSynchronizerFactory.class, "IS_CLUSTER_SYNC_SETUP", false);
			}
		}
	}

	// -- IProject folder overload tests -------------------------------------

	@Nested
	class ProjectFolderOverloadTests {
		@TempDir
		Path tempDir;

		@Test
		void pushProjectFolder_IProject_nullRelative() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<EngineUtility> eu = mockStatic(EngineUtility.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);

				IProject project = mock(IProject.class);
				when(project.getProjectId()).thenReturn("pid");
				when(project.getProjectName()).thenReturn("pname");

				String projHome = tempDir.toString();
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.PROJECT, "pid", "pname"))
						.thenReturn(projHome);

				String absPath = tempDir.resolve("sub").toString();
				ClusterUtil.pushProjectFolder(project, absPath, null);

				verify(mockStorage).pushProjectFolder(eq("pid"), eq(absPath), eq("sub"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushProjectFolder_IProject_withRelative() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<EngineUtility> eu = mockStatic(EngineUtility.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);

				IProject project = mock(IProject.class);
				when(project.getProjectId()).thenReturn("pid2");
				when(project.getProjectName()).thenReturn("pname2");

				String projHome = tempDir.toString();
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.PROJECT, "pid2", "pname2"))
						.thenReturn(projHome);

				String absPath = tempDir.resolve("base").toString();
				// relativePath is "extra" - will be appended with separator
				String separator = java.nio.file.FileSystems.getDefault().getSeparator();
				String expectedAbs = absPath + separator + "extra";
				ClusterUtil.pushProjectFolder(project, absPath, "extra");

				verify(mockStorage).pushProjectFolder(eq("pid2"), eq(expectedAbs), anyString());
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushProjectFolder_IProject_2arg_delegatesToNull() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<EngineUtility> eu = mockStatic(EngineUtility.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);

				IProject project = mock(IProject.class);
				when(project.getProjectId()).thenReturn("pid3");
				when(project.getProjectName()).thenReturn("pname3");

				String projHome = tempDir.toString();
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.PROJECT, "pid3", "pname3"))
						.thenReturn(projHome);

				String absPath = tempDir.resolve("child").toString();
				ClusterUtil.pushProjectFolder(project, absPath);

				verify(mockStorage).pushProjectFolder(eq("pid3"), eq(absPath), eq("child"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullProjectFolder_IProject_nullRelative() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<EngineUtility> eu = mockStatic(EngineUtility.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);

				IProject project = mock(IProject.class);
				when(project.getProjectId()).thenReturn("pid4");
				when(project.getProjectName()).thenReturn("pname4");

				String projHome = tempDir.toString();
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.PROJECT, "pid4", "pname4"))
						.thenReturn(projHome);

				String absPath = tempDir.resolve("data").toString();
				ClusterUtil.pullProjectFolder(project, absPath, null);

				verify(mockStorage).pullProjectFolder(eq("pid4"), eq(absPath), eq("data"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullProjectFolder_IProject_withRelative() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<EngineUtility> eu = mockStatic(EngineUtility.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);

				IProject project = mock(IProject.class);
				when(project.getProjectId()).thenReturn("pidR");
				when(project.getProjectName()).thenReturn("pnameR");

				String projHome = tempDir.toString();
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.PROJECT, "pidR", "pnameR"))
						.thenReturn(projHome);

				String absPath = tempDir.resolve("base").toString();
				String separator = java.nio.file.FileSystems.getDefault().getSeparator();
				ClusterUtil.pullProjectFolder(project, absPath, "extra");

				verify(mockStorage).pullProjectFolder(eq("pidR"), eq(absPath + separator + "extra"), anyString());
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullProjectFolder_IProject_absPathEndsSep() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<EngineUtility> eu = mockStatic(EngineUtility.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);

				IProject project = mock(IProject.class);
				when(project.getProjectId()).thenReturn("pidS");
				when(project.getProjectName()).thenReturn("pnameS");

				String projHome = tempDir.toString();
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.PROJECT, "pidS", "pnameS"))
						.thenReturn(projHome);

				String separator = java.nio.file.FileSystems.getDefault().getSeparator();
				String absPath = tempDir.resolve("folder").toString() + separator;
				ClusterUtil.pullProjectFolder(project, absPath, "deeper");

				verify(mockStorage).pullProjectFolder(eq("pidS"), eq(absPath + "deeper"), anyString());
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullProjectFolder_IProject_emptyRelative() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<EngineUtility> eu = mockStatic(EngineUtility.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);

				IProject project = mock(IProject.class);
				when(project.getProjectId()).thenReturn("pidT");
				when(project.getProjectName()).thenReturn("pnameT");

				String projHome = tempDir.toString();
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.PROJECT, "pidT", "pnameT"))
						.thenReturn(projHome);

				String absPath = tempDir.resolve("sub3").toString();
				ClusterUtil.pullProjectFolder(project, absPath, "  ");

				verify(mockStorage).pullProjectFolder(eq("pidT"), eq(absPath), eq("sub3"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullProjectFolder_IProject_2arg() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<EngineUtility> eu = mockStatic(EngineUtility.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);

				IProject project = mock(IProject.class);
				when(project.getProjectId()).thenReturn("pid5");
				when(project.getProjectName()).thenReturn("pname5");

				String projHome = tempDir.toString();
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.PROJECT, "pid5", "pname5"))
						.thenReturn(projHome);

				String absPath = tempDir.resolve("files").toString();
				ClusterUtil.pullProjectFolder(project, absPath);

				verify(mockStorage).pullProjectFolder(eq("pid5"), eq(absPath), eq("files"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushProjectFolder_IProject_emptyRelative() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<EngineUtility> eu = mockStatic(EngineUtility.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);

				IProject project = mock(IProject.class);
				when(project.getProjectId()).thenReturn("pid6");
				when(project.getProjectName()).thenReturn("pname6");

				String projHome = tempDir.toString();
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.PROJECT, "pid6", "pname6"))
						.thenReturn(projHome);

				String absPath = tempDir.resolve("sub2").toString();
				// empty relative -> treated as null-like (trimmed to empty -> skipped)
				ClusterUtil.pushProjectFolder(project, absPath, "  ");

				verify(mockStorage).pushProjectFolder(eq("pid6"), eq(absPath), eq("sub2"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushProjectFolder_IProject_absPathEndsSep() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<EngineUtility> eu = mockStatic(EngineUtility.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);

				IProject project = mock(IProject.class);
				when(project.getProjectId()).thenReturn("pid7");
				when(project.getProjectName()).thenReturn("pname7");

				String projHome = tempDir.toString();
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.PROJECT, "pid7", "pname7"))
						.thenReturn(projHome);

				String separator = java.nio.file.FileSystems.getDefault().getSeparator();
				String absPath = tempDir.resolve("folder").toString() + separator;
				// abs ends with separator -> relativePath appended directly
				ClusterUtil.pushProjectFolder(project, absPath, "deeper");

				verify(mockStorage).pushProjectFolder(eq("pid7"), eq(absPath + "deeper"), anyString());
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}
	}

	// -- isSchedulerExecutor tests (IS_CLUSTER=true paths) ----------------

	@Nested
	class IsSchedulerExecutorTests {

		@Test
		void isCluster_diPropertySet_returnsTrue() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<Utility> util = mockStatic(Utility.class)) {
				util.when(() -> Utility.getDIHelperProperty("SCHEDULER_EXECUTOR")).thenReturn("true");
				assertTrue(ClusterUtil.isSchedulerExecutor());
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void isCluster_diPropertyFalse_returnsFalse() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<Utility> util = mockStatic(Utility.class)) {
				util.when(() -> Utility.getDIHelperProperty("SCHEDULER_EXECUTOR")).thenReturn("false");
				assertFalse(ClusterUtil.isSchedulerExecutor());
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void isCluster_diPropertyEmpty_fallsToEnvCheck() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<Utility> util = mockStatic(Utility.class)) {
				// DI property returns empty -> falls through to env check
				util.when(() -> Utility.getDIHelperProperty("SCHEDULER_EXECUTOR")).thenReturn("");
				// env var won't be set (no SCHEDULER_EXECUTOR env), so falls to ZK
				// We can't easily test the ZK path, but we test the branching logic
				// The SchedulerListener.getListener().isZKLeader() will be called
				// which will fail -> just verify the DI empty branch triggers
				try (MockedStatic<SchedulerListener> sl = mockStatic(SchedulerListener.class)) {
					SchedulerListener mockListener = mock(SchedulerListener.class);
					sl.when(SchedulerListener::getListener).thenReturn(mockListener);
					when(mockListener.isZKLeader()).thenReturn(true);
					assertTrue(ClusterUtil.isSchedulerExecutor());
				}
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void isCluster_diNull_envNull_fallsToZk() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<Utility> util = mockStatic(Utility.class);
					MockedStatic<SchedulerListener> sl = mockStatic(SchedulerListener.class)) {
				util.when(() -> Utility.getDIHelperProperty("SCHEDULER_EXECUTOR")).thenReturn(null);
				SchedulerListener mockListener = mock(SchedulerListener.class);
				sl.when(SchedulerListener::getListener).thenReturn(mockListener);
				when(mockListener.isZKLeader()).thenReturn(false);
				assertFalse(ClusterUtil.isSchedulerExecutor());
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}
	}

	// -- IEngine folder overload tests --------------------------------------

	@Nested
	class EngineFolderOverloadTests {
		@TempDir
		Path tempDir;

		@Test
		void pushEngineFolder_IEngine_nullRelative() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<EngineUtility> eu = mockStatic(EngineUtility.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);

				IEngine engine = mock(IEngine.class);
				when(engine.getCatalogType()).thenReturn(IEngine.CATALOG_TYPE.DATABASE);
				when(engine.getEngineId()).thenReturn("eid");
				when(engine.getEngineName()).thenReturn("ename");

				String engHome = tempDir.toString();
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.DATABASE, "eid", "ename"))
						.thenReturn(engHome);

				String absPath = tempDir.resolve("dbfolder").toString();
				ClusterUtil.pushEngineFolder(engine, absPath, null);

				verify(mockStorage).pushEngineFolder(eq("eid"), eq(absPath), eq("dbfolder"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushEngineFolder_IEngine_withRelative() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<EngineUtility> eu = mockStatic(EngineUtility.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);

				IEngine engine = mock(IEngine.class);
				when(engine.getCatalogType()).thenReturn(IEngine.CATALOG_TYPE.MODEL);
				when(engine.getEngineId()).thenReturn("eid2");
				when(engine.getEngineName()).thenReturn("ename2");

				String engHome = tempDir.toString();
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.MODEL, "eid2", "ename2"))
						.thenReturn(engHome);

				String absPath = tempDir.resolve("mdir").toString();
				String separator = java.nio.file.FileSystems.getDefault().getSeparator();
				ClusterUtil.pushEngineFolder(engine, absPath, "sub");

				verify(mockStorage).pushEngineFolder(eq("eid2"), eq(absPath + separator + "sub"), anyString());
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pushEngineFolder_IEngine_2arg() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<EngineUtility> eu = mockStatic(EngineUtility.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);

				IEngine engine = mock(IEngine.class);
				when(engine.getCatalogType()).thenReturn(IEngine.CATALOG_TYPE.STORAGE);
				when(engine.getEngineId()).thenReturn("eid3");
				when(engine.getEngineName()).thenReturn("ename3");

				String engHome = tempDir.toString();
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.STORAGE, "eid3", "ename3"))
						.thenReturn(engHome);

				String absPath = tempDir.resolve("sdir").toString();
				ClusterUtil.pushEngineFolder(engine, absPath);

				verify(mockStorage).pushEngineFolder(eq("eid3"), eq(absPath), eq("sdir"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullEngineFolder_IEngine_nullRelative() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<EngineUtility> eu = mockStatic(EngineUtility.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);

				IEngine engine = mock(IEngine.class);
				when(engine.getCatalogType()).thenReturn(IEngine.CATALOG_TYPE.VECTOR);
				when(engine.getEngineId()).thenReturn("eid4");
				when(engine.getEngineName()).thenReturn("ename4");

				String engHome = tempDir.toString();
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, "eid4", "ename4"))
						.thenReturn(engHome);

				String absPath = tempDir.resolve("vdir").toString();
				ClusterUtil.pullEngineFolder(engine, absPath, null);

				verify(mockStorage).pullEngineFolder(eq("eid4"), eq(absPath), eq("vdir"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullEngineFolder_IEngine_withRelative() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<EngineUtility> eu = mockStatic(EngineUtility.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);

				IEngine engine = mock(IEngine.class);
				when(engine.getCatalogType()).thenReturn(IEngine.CATALOG_TYPE.DATABASE);
				when(engine.getEngineId()).thenReturn("eidR");
				when(engine.getEngineName()).thenReturn("enameR");

				String engHome = tempDir.toString();
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.DATABASE, "eidR",
						"enameR")).thenReturn(engHome);

				String absPath = tempDir.resolve("base").toString();
				String separator = java.nio.file.FileSystems.getDefault().getSeparator();
				ClusterUtil.pullEngineFolder(engine, absPath, "child");

				verify(mockStorage).pullEngineFolder(eq("eidR"), eq(absPath + separator + "child"), anyString());
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullEngineFolder_IEngine_absPathEndsSep() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<EngineUtility> eu = mockStatic(EngineUtility.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);

				IEngine engine = mock(IEngine.class);
				when(engine.getCatalogType()).thenReturn(IEngine.CATALOG_TYPE.MODEL);
				when(engine.getEngineId()).thenReturn("eidS");
				when(engine.getEngineName()).thenReturn("enameS");

				String engHome = tempDir.toString();
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.MODEL, "eidS", "enameS"))
						.thenReturn(engHome);

				String separator = java.nio.file.FileSystems.getDefault().getSeparator();
				String absPath = tempDir.resolve("dir2").toString() + separator;
				ClusterUtil.pullEngineFolder(engine, absPath, "more");

				verify(mockStorage).pullEngineFolder(eq("eidS"), eq(absPath + "more"), anyString());
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullEngineFolder_IEngine_emptyRelative() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<EngineUtility> eu = mockStatic(EngineUtility.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);

				IEngine engine = mock(IEngine.class);
				when(engine.getCatalogType()).thenReturn(IEngine.CATALOG_TYPE.STORAGE);
				when(engine.getEngineId()).thenReturn("eidT");
				when(engine.getEngineName()).thenReturn("enameT");

				String engHome = tempDir.toString();
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.STORAGE, "eidT", "enameT"))
						.thenReturn(engHome);

				String absPath = tempDir.resolve("sub4").toString();
				ClusterUtil.pullEngineFolder(engine, absPath, "   ");

				verify(mockStorage).pullEngineFolder(eq("eidT"), eq(absPath), eq("sub4"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}

		@Test
		void pullEngineFolder_IEngine_2arg() throws Exception {
			setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", true);
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<EngineUtility> eu = mockStatic(EngineUtility.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);

				IEngine engine = mock(IEngine.class);
				when(engine.getCatalogType()).thenReturn(IEngine.CATALOG_TYPE.FUNCTION);
				when(engine.getEngineId()).thenReturn("eid5");
				when(engine.getEngineName()).thenReturn("ename5");

				String engHome = tempDir.toString();
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.FUNCTION, "eid5",
						"ename5")).thenReturn(engHome);

				String absPath = tempDir.resolve("fdir").toString();
				ClusterUtil.pullEngineFolder(engine, absPath);

				verify(mockStorage).pullEngineFolder(eq("eid5"), eq(absPath), eq("fdir"));
			} finally {
				setStaticFinalBoolean(ClusterUtil.class, "IS_CLUSTER", false);
			}
		}
	}

	// -- getEngineAndProjectImage tests -------------------------------------

	@Nested
	class GetEngineAndProjectImageTests {
		@TempDir
		Path tempDir;

		@Test
		void folderExists_imageFound() throws Exception {
			// Create image folder with matching file
			Path imgFolder = Files.createDirectory(tempDir.resolve("images"));
			Files.createFile(imgFolder.resolve("engine123.png"));

			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<EngineUtility> eu = mockStatic(EngineUtility.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);
				eu.when(() -> EngineUtility.getLocalEngineImageDirectory(IEngine.CATALOG_TYPE.DATABASE))
						.thenReturn(imgFolder.toString());

				File result = ClusterUtil.getEngineAndProjectImage("engine123", IEngine.CATALOG_TYPE.DATABASE);
				assertNotNull(result);
				assertTrue(result.getName().contains("engine123"));
				// Should NOT have called pull since folder exists and image found
				verify(mockStorage, never()).pullEngineAndProjectImageFolder(IEngine.CATALOG_TYPE.DATABASE);
			}
		}

		@Test
		void folderNotExists_pullsThenFinds() throws Exception {
			// imgFolder doesn't exist yet
			Path imgFolder = tempDir.resolve("notExist");

			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<EngineUtility> eu = mockStatic(EngineUtility.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);
				eu.when(() -> EngineUtility.getLocalEngineImageDirectory(IEngine.CATALOG_TYPE.MODEL))
						.thenReturn(imgFolder.toString());

				// When pull is called, create the folder and file
				doAnswer(inv -> {
					Files.createDirectories(imgFolder);
					Files.createFile(imgFolder.resolve("eng99.jpg"));
					return null;
				}).when(mockStorage).pullEngineAndProjectImageFolder(IEngine.CATALOG_TYPE.MODEL);

				File result = ClusterUtil.getEngineAndProjectImage("eng99", IEngine.CATALOG_TYPE.MODEL);
				assertNotNull(result);
				assertTrue(result.getName().contains("eng99"));
				verify(mockStorage).pullEngineAndProjectImageFolder(IEngine.CATALOG_TYPE.MODEL);
			}
		}

		@Test
		void folderExists_noImage_pullsAgain_findsImage() throws Exception {
			// Folder exists but no matching image initially
			Path imgFolder = Files.createDirectory(tempDir.resolve("imgs"));
			Files.createFile(imgFolder.resolve("otherfile.txt"));

			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<EngineUtility> eu = mockStatic(EngineUtility.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);
				eu.when(() -> EngineUtility.getLocalEngineImageDirectory(IEngine.CATALOG_TYPE.STORAGE))
						.thenReturn(imgFolder.toString());

				// When pull is called, create the matching file
				doAnswer(inv -> {
					Files.createFile(imgFolder.resolve("engX.png"));
					return null;
				}).when(mockStorage).pullEngineAndProjectImageFolder(IEngine.CATALOG_TYPE.STORAGE);

				File result = ClusterUtil.getEngineAndProjectImage("engX", IEngine.CATALOG_TYPE.STORAGE);
				assertNotNull(result);
				assertTrue(result.getName().contains("engX"));
				// Should have pulled since image wasn't found initially
				verify(mockStorage).pullEngineAndProjectImageFolder(IEngine.CATALOG_TYPE.STORAGE);
			}
		}

		@Test
		void folderExists_noImage_pullsAgain_stillNoImage_generatesNew() throws Exception {
			// Folder exists but no matching image even after pull
			Path imgFolder = Files.createDirectory(tempDir.resolve("imgs2"));

			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class);
					MockedStatic<EngineUtility> eu = mockStatic(EngineUtility.class);
					MockedStatic<DefaultImageGeneratorUtil> dig = mockStatic(DefaultImageGeneratorUtil.class)) {
				CentralCloudStorage mockStorage = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mockStorage);
				eu.when(() -> EngineUtility.getLocalEngineImageDirectory(IEngine.CATALOG_TYPE.VECTOR))
						.thenReturn(imgFolder.toString());

				// pickRandomImage creates the file
				dig.when(() -> DefaultImageGeneratorUtil.pickRandomImage(anyString())).thenAnswer(inv -> {
					String path = inv.getArgument(0);
					new File(path).createNewFile();
					return null;
				});

				File result = ClusterUtil.getEngineAndProjectImage("engNew", IEngine.CATALOG_TYPE.VECTOR);
				assertNotNull(result);
				assertTrue(result.getName().startsWith("engNew"));
				assertTrue(result.getName().endsWith(".png"));
				// Verify pull was called and new image was pushed
				verify(mockStorage).pullEngineAndProjectImageFolder(IEngine.CATALOG_TYPE.VECTOR);
				verify(mockStorage).pushEngineAndProjectImage(eq(IEngine.CATALOG_TYPE.VECTOR), eq("engNew.png"));
			}
		}
	}

	// -- getCentralStorageClient / getClusterSynchronizer tests -------------

	@Nested
	class StaticAccessorTests {
		@Test
		void getCentralStorageClient_delegatesToCCS() throws Exception {
			try (MockedStatic<CentralCloudStorage> ccs = mockStatic(CentralCloudStorage.class)) {
				CentralCloudStorage mock = mock(CentralCloudStorage.class);
				ccs.when(CentralCloudStorage::getInstance).thenReturn(mock);
				assertSame(mock, ClusterUtil.getCentralStorageClient());
			}
		}

		@Test
		void getClusterSynchronizer_delegatesToFactory() throws Exception {
			try (MockedStatic<ClusterSynchronizerFactory> cs = mockStatic(ClusterSynchronizerFactory.class)) {
				IClusterSynchronizer mock = mock(IClusterSynchronizer.class);
				cs.when(ClusterSynchronizerFactory::getClusterSynchronizer).thenReturn(mock);
				assertSame(mock, ClusterUtil.getClusterSynchronizer());
			}
		}
	}
}
