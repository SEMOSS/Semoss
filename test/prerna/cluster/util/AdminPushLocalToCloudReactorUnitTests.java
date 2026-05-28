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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.clients.CentralCloudStorage;
import prerna.om.Insight;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

class AdminPushLocalToCloudReactorUnitTests {

	private AdminPushLocalToCloudReactor reactor;
	private Insight mockInsight;
	private User mockUser;
	private NounStore mockNounStore;

	@BeforeEach
	void setUp() {
		reactor = new AdminPushLocalToCloudReactor();
		mockInsight = mock(Insight.class);
		mockUser = mock(User.class);
		mockNounStore = mock(NounStore.class);
		when(mockInsight.getUser()).thenReturn(mockUser);
		reactor.setInsight(mockInsight);
		reactor.setNounStore(mockNounStore);
	}

	@Nested
	@DisplayName("Constructor and keysToGet tests")
	class ConstructorTests {

		@Test
		@DisplayName("keysToGet should contain DRY_RUN key")
		void keysToGet_containsDryRunKey() {
			String[] keys = reactor.keysToGet;
			assertNotNull(keys, "keysToGet should not be null");
			assertEquals(1, keys.length, "keysToGet should have exactly one key");
			assertEquals(ReactorKeysEnum.DRY_RUN.getKey(), keys[0]);
		}

		@Test
		@DisplayName("DRY_RUN key value should be dryRun")
		void dryRunKey_hasExpectedStringValue() {
			assertEquals("dryRun", reactor.keysToGet[0]);
		}
	}

	@Nested
	@DisplayName("Non-admin authorization tests")
	class NonAdminTests {

		@Test
		@DisplayName("execute throws IllegalArgumentException when user is not admin")
		void execute_nonAdmin_throwsIllegalArgumentException() {
			try (MockedStatic<SecurityAdminUtils> mockedAdmin = mockStatic(SecurityAdminUtils.class)) {
				mockedAdmin.when(() -> SecurityAdminUtils.userIsAdmin(mockUser)).thenReturn(false);
				IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> reactor.execute());
				assertEquals("User must be an admin for this operation!", ex.getMessage());
			}
		}

		@Test
		@DisplayName("execute does not throw admin exception when user is admin")
		void execute_admin_doesNotThrowAdminException() throws Exception {
			try (MockedStatic<SecurityAdminUtils> mockedAdmin = mockStatic(SecurityAdminUtils.class);
				 MockedStatic<SecurityEngineUtils> mockedEngine = mockStatic(SecurityEngineUtils.class);
				 MockedStatic<SecurityProjectUtils> mockedProject = mockStatic(SecurityProjectUtils.class);
				 MockedStatic<CentralCloudStorage> mockedCloud = mockStatic(CentralCloudStorage.class)) {
				mockedAdmin.when(() -> SecurityAdminUtils.userIsAdmin(mockUser)).thenReturn(true);
				mockedEngine.when(() -> SecurityEngineUtils.getAllEngineIds(anyList())).thenReturn(new ArrayList<>());
				mockedProject.when(SecurityProjectUtils::getAllProjectIds).thenReturn(new ArrayList<>());
				CentralCloudStorage mockCc = mock(CentralCloudStorage.class);
				mockedCloud.when(CentralCloudStorage::getInstance).thenReturn(mockCc);
				when(mockCc.listAllContainersByBucket()).thenReturn(new HashMap<>());
				reactor.keyValue.put(ReactorKeysEnum.DRY_RUN.getKey(), "true");
				NounMetadata result = reactor.execute();
				assertNotNull(result, "Result should not be null for admin user");
			}
		}
	}

	@Nested
	@DisplayName("removeExisitngIds via reflection tests")
	class RemoveExistingIdsTests {

		private Method removeMethod;

		@BeforeEach
		void setUpReflection() throws Exception {
			removeMethod = AdminPushLocalToCloudReactor.class.getDeclaredMethod(
					"removeExisitngIds", String.class, List.class, List.class);
			removeMethod.setAccessible(true);
		}

		@Test
		@DisplayName("Removes IDs that exist in cloud as folders ending with /")
		void removesIds_thatExistInCloud_asFolders() throws Exception {
			List<String> cloudFiles = new ArrayList<>(Arrays.asList("engine-id-1/", "engine-id-2/"));
			List<String> startingList = new ArrayList<>(Arrays.asList("engine-id-1", "engine-id-2", "engine-id-3"));
			removeMethod.invoke(reactor, "semoss-db", cloudFiles, startingList);
			assertEquals(1, startingList.size(), "Should have removed 2 IDs");
			assertTrue(startingList.contains("engine-id-3"));
			assertFalse(startingList.contains("engine-id-1"));
			assertFalse(startingList.contains("engine-id-2"));
		}

		@Test
		@DisplayName("Skips non-folder entries that do not end with /")
		void skipsNonFolderEntries() throws Exception {
			List<String> cloudFiles = new ArrayList<>(Arrays.asList("engine-id-1", "some-file.txt"));
			List<String> startingList = new ArrayList<>(Arrays.asList("engine-id-1", "some-file.txt", "engine-id-2"));
			removeMethod.invoke(reactor, "semoss-db", cloudFiles, startingList);
			assertEquals(3, startingList.size(), "No entries should be removed for non-folder cloud files");
		}

		@Test
		@DisplayName("Skips SMSS folders ending with -smss/")
		void skipsSmsFolders() throws Exception {
			List<String> cloudFiles = new ArrayList<>(Arrays.asList("engine-id-1-smss/", "engine-id-2/"));
			List<String> startingList = new ArrayList<>(Arrays.asList("engine-id-1-smss", "engine-id-1", "engine-id-2"));
			removeMethod.invoke(reactor, "semoss-db", cloudFiles, startingList);
			assertEquals(2, startingList.size(), "Only the normal folder ID should be removed");
			assertTrue(startingList.contains("engine-id-1-smss"));
			assertTrue(startingList.contains("engine-id-1"));
			assertFalse(startingList.contains("engine-id-2"));
		}

		@Test
		@DisplayName("Empty cloud list does not modify startingList")
		void emptyCloudList_doesNotModifyStartingList() throws Exception {
			List<String> cloudFiles = new ArrayList<>();
			List<String> startingList = new ArrayList<>(Arrays.asList("id-1", "id-2", "id-3"));
			removeMethod.invoke(reactor, "semoss-db", cloudFiles, startingList);
			assertEquals(3, startingList.size());
			assertEquals(Arrays.asList("id-1", "id-2", "id-3"), startingList);
		}

		@Test
		@DisplayName("Multiple entries removed correctly from startingList")
		void multipleEntries_removedCorrectly() throws Exception {
			List<String> cloudFiles = new ArrayList<>(Arrays.asList("aaa-111/", "bbb-222/", "ccc-333/", "ddd-444/", "eee-555/"));
			List<String> startingList = new ArrayList<>(Arrays.asList("aaa-111", "bbb-222", "ccc-333", "ddd-444", "eee-555", "fff-666"));
			removeMethod.invoke(reactor, "semoss-db", cloudFiles, startingList);
			assertEquals(1, startingList.size());
			assertEquals("fff-666", startingList.get(0));
		}

		@Test
		@DisplayName("Mixed entries: folders, non-folders, and SMSS folders processed correctly")
		void mixedEntries_processedCorrectly() throws Exception {
			List<String> cloudFiles = new ArrayList<>(Arrays.asList("engine-a/", "engine-b-smss/", "random-file.dat", "engine-c/", "engine-d-smss/"));
			List<String> startingList = new ArrayList<>(Arrays.asList("engine-a", "engine-b", "engine-c", "engine-d", "engine-e"));
			removeMethod.invoke(reactor, "semoss-db", cloudFiles, startingList);
			assertEquals(3, startingList.size());
			assertTrue(startingList.contains("engine-b"));
			assertTrue(startingList.contains("engine-d"));
			assertTrue(startingList.contains("engine-e"));
			assertFalse(startingList.contains("engine-a"));
			assertFalse(startingList.contains("engine-c"));
		}

		@Test
		@DisplayName("Cloud entry not in startingList does not cause error")
		void cloudEntryNotInStartingList_noCrash() throws Exception {
			List<String> cloudFiles = new ArrayList<>(Arrays.asList("not-in-list/", "also-missing/"));
			List<String> startingList = new ArrayList<>(Arrays.asList("my-engine-1", "my-engine-2"));
			removeMethod.invoke(reactor, "semoss-db", cloudFiles, startingList);
			assertEquals(2, startingList.size());
			assertTrue(startingList.contains("my-engine-1"));
			assertTrue(startingList.contains("my-engine-2"));
		}

		@Test
		@DisplayName("SMSS_POSTFIX constant value is -smss")
		void smssPostfix_hasExpectedValue() {
			assertEquals("-smss", CentralCloudStorage.SMSS_POSTFIX);
		}
	}

	@Nested
	@DisplayName("execute return type tests")
	class ExecuteReturnTypeTests {

		@Test
		@DisplayName("execute returns NounMetadata with MAP PixelDataType")
		void execute_returnsMapType() throws Exception {
			try (MockedStatic<SecurityAdminUtils> mockedAdmin = mockStatic(SecurityAdminUtils.class);
				 MockedStatic<SecurityEngineUtils> mockedEngine = mockStatic(SecurityEngineUtils.class);
				 MockedStatic<SecurityProjectUtils> mockedProject = mockStatic(SecurityProjectUtils.class);
				 MockedStatic<CentralCloudStorage> mockedCloud = mockStatic(CentralCloudStorage.class)) {
				mockedAdmin.when(() -> SecurityAdminUtils.userIsAdmin(mockUser)).thenReturn(true);
				mockedEngine.when(() -> SecurityEngineUtils.getAllEngineIds(anyList())).thenReturn(new ArrayList<>());
				mockedProject.when(SecurityProjectUtils::getAllProjectIds).thenReturn(new ArrayList<>());
				CentralCloudStorage mockCc = mock(CentralCloudStorage.class);
				mockedCloud.when(CentralCloudStorage::getInstance).thenReturn(mockCc);
				when(mockCc.listAllContainersByBucket()).thenReturn(new HashMap<>());
				reactor.keyValue.put(ReactorKeysEnum.DRY_RUN.getKey(), "true");
				NounMetadata result = reactor.execute();
				assertNotNull(result);
				assertEquals(PixelDataType.MAP, result.getNounType());
			}
		}

		@Test
		@DisplayName("execute result contains dryRun true by default")
		void execute_resultContainsDryRunTrue() throws Exception {
			try (MockedStatic<SecurityAdminUtils> mockedAdmin = mockStatic(SecurityAdminUtils.class);
				 MockedStatic<SecurityEngineUtils> mockedEngine = mockStatic(SecurityEngineUtils.class);
				 MockedStatic<SecurityProjectUtils> mockedProject = mockStatic(SecurityProjectUtils.class);
				 MockedStatic<CentralCloudStorage> mockedCloud = mockStatic(CentralCloudStorage.class)) {
				mockedAdmin.when(() -> SecurityAdminUtils.userIsAdmin(mockUser)).thenReturn(true);
				mockedEngine.when(() -> SecurityEngineUtils.getAllEngineIds(anyList())).thenReturn(new ArrayList<>());
				mockedProject.when(SecurityProjectUtils::getAllProjectIds).thenReturn(new ArrayList<>());
				CentralCloudStorage mockCc = mock(CentralCloudStorage.class);
				mockedCloud.when(CentralCloudStorage::getInstance).thenReturn(mockCc);
				when(mockCc.listAllContainersByBucket()).thenReturn(new HashMap<>());
				NounMetadata result = reactor.execute();
				@SuppressWarnings("unchecked")
				Map<String, Object> map = (Map<String, Object>) result.getValue();
				assertEquals(true, map.get("dryRun"));
			}
		}

		@Test
		@DisplayName("execute result contains dryRun false when set to false")
		void execute_resultContainsDryRunFalse() throws Exception {
			try (MockedStatic<SecurityAdminUtils> mockedAdmin = mockStatic(SecurityAdminUtils.class);
				 MockedStatic<SecurityEngineUtils> mockedEngine = mockStatic(SecurityEngineUtils.class);
				 MockedStatic<SecurityProjectUtils> mockedProject = mockStatic(SecurityProjectUtils.class);
				 MockedStatic<CentralCloudStorage> mockedCloud = mockStatic(CentralCloudStorage.class)) {
				mockedAdmin.when(() -> SecurityAdminUtils.userIsAdmin(mockUser)).thenReturn(true);
				mockedEngine.when(() -> SecurityEngineUtils.getAllEngineIds(anyList())).thenReturn(new ArrayList<>());
				mockedProject.when(SecurityProjectUtils::getAllProjectIds).thenReturn(new ArrayList<>());
				CentralCloudStorage mockCc = mock(CentralCloudStorage.class);
				mockedCloud.when(CentralCloudStorage::getInstance).thenReturn(mockCc);
				when(mockCc.listAllContainersByBucket()).thenReturn(new HashMap<>());
				reactor.keyValue.put(ReactorKeysEnum.DRY_RUN.getKey(), "false");
				NounMetadata result = reactor.execute();
				@SuppressWarnings("unchecked")
				Map<String, Object> map = (Map<String, Object>) result.getValue();
				assertEquals(false, map.get("dryRun"));
			}
		}
	}
}
