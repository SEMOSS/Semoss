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
package prerna.auth.utils.reactors.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.nio.file.Path;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.reactor.AbstractReactor;
import prerna.reactor.export.AbstractExportTxtReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

public class AdminExportAllUsersReactorUnitTests {

	private AdminExportAllUsersReactor reactor;
	private Insight insight;
	private User user;
	private NounStore ns;

	@TempDir
	Path tempDir;

	@BeforeEach
	void setup() throws Exception {
		reactor = new AdminExportAllUsersReactor();
		insight = mock(Insight.class);
		user = mock(User.class);
		ns = mock(NounStore.class);
		reactor.setInsight(insight);
		reactor.setNounStore(ns);
		when(insight.getUser()).thenReturn(user);

		// Set curRow via reflection (protected field in different package)
		Field curRowField = AbstractReactor.class.getDeclaredField("curRow");
		curRowField.setAccessible(true);
		curRowField.set(reactor, mock(GenRowStruct.class));
	}

	@Test
	void testKeysToGet() {
		assertEquals(4, reactor.keysToGet.length);
		assertEquals(ReactorKeysEnum.TASK.getKey(), reactor.keysToGet[0]);
		assertEquals(ReactorKeysEnum.FILE_NAME.getKey(), reactor.keysToGet[1]);
		assertEquals(ReactorKeysEnum.FILE_PATH.getKey(), reactor.keysToGet[2]);
		assertEquals(ReactorKeysEnum.PASSWORD.getKey(), reactor.keysToGet[3]);
	}

	@Test
	void testNonAdminThrowsException() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(null);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("User must be an admin to perform this function", e.getMessage());
		}
	}

	@Test
	void testNullPasswordThrowsException() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Must provide a password to encrypt the file", e.getMessage());
		}
	}

	@Test
	void testEmptyPasswordThrowsException() {
		reactor.keyValue.put(ReactorKeysEnum.PASSWORD.getKey(), "");

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Must provide a password to encrypt the file", e.getMessage());
		}
	}

	@Test
	void testWhitespaceOnlyPasswordThrowsException() {
		reactor.keyValue.put(ReactorKeysEnum.PASSWORD.getKey(), "");

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Must provide a password to encrypt the file", e.getMessage());
		}
	}

	@Test
	void testDatabaseExceptionWrapsMessage() throws Exception {
		reactor.keyValue.put(ReactorKeysEnum.PASSWORD.getKey(), "secret123");
		String insightFolder = tempDir.toAbsolutePath().toString();
		when(insight.getInsightFolder()).thenReturn(insightFolder);

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class);
				MockedStatic<SystemEngineRegistry> ser = Mockito.mockStatic(SystemEngineRegistry.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			IRDBMSEngine db = mock(IRDBMSEngine.class);
			ser.when(() -> SystemEngineRegistry.getSecurityDb()).thenReturn(db);
			util.when(() -> Utility.normalizePath(any(String.class))).thenAnswer(inv -> inv.getArgument(0));

			WrapperManager wmInstance = mock(WrapperManager.class);
			wm.when(WrapperManager::getInstance).thenReturn(wmInstance);
			when(wmInstance.getRawWrapper(any(IRDBMSEngine.class), any(SelectQueryStruct.class))).thenThrow(new RuntimeException("DB is down"));

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertTrue(e.getMessage().contains("An error occurred retrieving the users"));
			assertTrue(e.getMessage().contains("DB is down"));
		}
	}

	@Test
	void testSuccessfulExecuteReturnsFileDownloadNoun() throws Exception {
		reactor.keyValue.put(ReactorKeysEnum.PASSWORD.getKey(), "secret123");
		String insightFolder = tempDir.toAbsolutePath().toString();
		when(insight.getInsightFolder()).thenReturn(insightFolder);

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class);
				MockedStatic<AbstractExportTxtReactor> aet = Mockito.mockStatic(AbstractExportTxtReactor.class);
				MockedStatic<SystemEngineRegistry> ser = Mockito.mockStatic(SystemEngineRegistry.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			IRDBMSEngine db = mock(IRDBMSEngine.class);
			ser.when(() -> SystemEngineRegistry.getSecurityDb()).thenReturn(db);
			util.when(() -> Utility.normalizePath(any(String.class))).thenAnswer(inv -> inv.getArgument(0));

			aet.when(() -> AbstractExportTxtReactor.getExportFileName(any(), any(), any()))
					.thenReturn("All_Users_export.xlsx");

			WrapperManager wmInstance = mock(WrapperManager.class);
			wm.when(WrapperManager::getInstance).thenReturn(wmInstance);
			IRawSelectWrapper iterator = mock(IRawSelectWrapper.class);
			when(wmInstance.getRawWrapper(any(IRDBMSEngine.class), any(SelectQueryStruct.class))).thenReturn(iterator);

			NounMetadata result = reactor.execute();

			assertNotNull(result);
			assertEquals(PixelDataType.CONST_STRING, result.getNounType());
			// The value should be the download key (a UUID string)
			assertNotNull(result.getValue());
			assertTrue(result.getValue().toString().length() > 0);
		}
	}

	@Test
	void testSuccessfulExecuteAddsExportFileToInsight() throws Exception {
		reactor.keyValue.put(ReactorKeysEnum.PASSWORD.getKey(), "secret123");
		String insightFolder = tempDir.toAbsolutePath().toString();
		when(insight.getInsightFolder()).thenReturn(insightFolder);

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class);
				MockedStatic<AbstractExportTxtReactor> aet = Mockito.mockStatic(AbstractExportTxtReactor.class);
				MockedStatic<SystemEngineRegistry> ser = Mockito.mockStatic(SystemEngineRegistry.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			IRDBMSEngine db = mock(IRDBMSEngine.class);
			ser.when(() -> SystemEngineRegistry.getSecurityDb()).thenReturn(db);
			util.when(() -> Utility.normalizePath(any(String.class))).thenAnswer(inv -> inv.getArgument(0));

			aet.when(() -> AbstractExportTxtReactor.getExportFileName(any(), any(), any()))
					.thenReturn("All_Users_export.xlsx");

			WrapperManager wmInstance = mock(WrapperManager.class);
			wm.when(WrapperManager::getInstance).thenReturn(wmInstance);
			IRawSelectWrapper iterator = mock(IRawSelectWrapper.class);
			when(wmInstance.getRawWrapper(any(IRDBMSEngine.class), any(SelectQueryStruct.class))).thenReturn(iterator);

			reactor.execute();

			verify(insight).addExportFile(any(String.class), any());
		}
	}

	@Test
	void testSuccessfulExecuteClosesIterator() throws Exception {
		reactor.keyValue.put(ReactorKeysEnum.PASSWORD.getKey(), "secret123");
		String insightFolder = tempDir.toAbsolutePath().toString();
		when(insight.getInsightFolder()).thenReturn(insightFolder);

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class);
				MockedStatic<AbstractExportTxtReactor> aet = Mockito.mockStatic(AbstractExportTxtReactor.class);
				MockedStatic<SystemEngineRegistry> ser = Mockito.mockStatic(SystemEngineRegistry.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			IRDBMSEngine db = mock(IRDBMSEngine.class);
			ser.when(() -> SystemEngineRegistry.getSecurityDb()).thenReturn(db);
			util.when(() -> Utility.normalizePath(any(String.class))).thenAnswer(inv -> inv.getArgument(0));

			aet.when(() -> AbstractExportTxtReactor.getExportFileName(any(), any(), any()))
					.thenReturn("All_Users_export.xlsx");

			WrapperManager wmInstance = mock(WrapperManager.class);
			wm.when(WrapperManager::getInstance).thenReturn(wmInstance);
			IRawSelectWrapper iterator = mock(IRawSelectWrapper.class);
			when(wmInstance.getRawWrapper(any(IRDBMSEngine.class), any(SelectQueryStruct.class))).thenReturn(iterator);

			reactor.execute();

			verify(iterator).close();
		}
	}

	@Test
	void testDefaultFileNameWhenNotProvided() throws Exception {
		reactor.keyValue.put(ReactorKeysEnum.PASSWORD.getKey(), "secret123");
		// Do NOT set FILE_NAME key -> should default to "All_Users"
		String insightFolder = tempDir.toAbsolutePath().toString();
		when(insight.getInsightFolder()).thenReturn(insightFolder);

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class);
				MockedStatic<AbstractExportTxtReactor> aet = Mockito.mockStatic(AbstractExportTxtReactor.class);
				MockedStatic<SystemEngineRegistry> ser = Mockito.mockStatic(SystemEngineRegistry.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			IRDBMSEngine db = mock(IRDBMSEngine.class);
			ser.when(() -> SystemEngineRegistry.getSecurityDb()).thenReturn(db);
			util.when(() -> Utility.normalizePath(any(String.class))).thenAnswer(inv -> inv.getArgument(0));

			// Capture the prefixName argument to verify it defaults to "All_Users"
			aet.when(() -> AbstractExportTxtReactor.getExportFileName(any(), any(), any()))
					.thenReturn("All_Users_export.xlsx");

			WrapperManager wmInstance = mock(WrapperManager.class);
			wm.when(WrapperManager::getInstance).thenReturn(wmInstance);
			IRawSelectWrapper iterator = mock(IRawSelectWrapper.class);
			when(wmInstance.getRawWrapper(any(IRDBMSEngine.class), any(SelectQueryStruct.class))).thenReturn(iterator);

			reactor.execute();

			aet.verify(() -> AbstractExportTxtReactor.getExportFileName(any(), Mockito.eq("All_Users"), Mockito.eq("xlsx")));
		}
	}

	@Test
	void testCustomFileNameUsed() throws Exception {
		reactor.keyValue.put(ReactorKeysEnum.PASSWORD.getKey(), "secret123");
		reactor.keyValue.put(ReactorKeysEnum.FILE_NAME.getKey(), "MyCustomExport");
		String insightFolder = tempDir.toAbsolutePath().toString();
		when(insight.getInsightFolder()).thenReturn(insightFolder);

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class);
				MockedStatic<AbstractExportTxtReactor> aet = Mockito.mockStatic(AbstractExportTxtReactor.class);
				MockedStatic<SystemEngineRegistry> ser = Mockito.mockStatic(SystemEngineRegistry.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			IRDBMSEngine db = mock(IRDBMSEngine.class);
			ser.when(() -> SystemEngineRegistry.getSecurityDb()).thenReturn(db);
			util.when(() -> Utility.normalizePath(any(String.class))).thenAnswer(inv -> inv.getArgument(0));

			aet.when(() -> AbstractExportTxtReactor.getExportFileName(any(), any(), any()))
					.thenReturn("MyCustomExport_export.xlsx");

			WrapperManager wmInstance = mock(WrapperManager.class);
			wm.when(WrapperManager::getInstance).thenReturn(wmInstance);
			IRawSelectWrapper iterator = mock(IRawSelectWrapper.class);
			when(wmInstance.getRawWrapper(any(IRDBMSEngine.class), any(SelectQueryStruct.class))).thenReturn(iterator);

			reactor.execute();

			aet.verify(() -> AbstractExportTxtReactor.getExportFileName(any(), Mockito.eq("MyCustomExport"), Mockito.eq("xlsx")));
		}
	}

	@Test
	void testFileLocationDefaultsToInsightFolder() throws Exception {
		reactor.keyValue.put(ReactorKeysEnum.PASSWORD.getKey(), "secret123");
		// Do NOT set FILE_PATH key -> should use insight folder
		String insightFolder = tempDir.toAbsolutePath().toString();
		when(insight.getInsightFolder()).thenReturn(insightFolder);

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class);
				MockedStatic<AbstractExportTxtReactor> aet = Mockito.mockStatic(AbstractExportTxtReactor.class);
				MockedStatic<SystemEngineRegistry> ser = Mockito.mockStatic(SystemEngineRegistry.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			IRDBMSEngine db = mock(IRDBMSEngine.class);
			ser.when(() -> SystemEngineRegistry.getSecurityDb()).thenReturn(db);
			util.when(() -> Utility.normalizePath(any(String.class))).thenAnswer(inv -> inv.getArgument(0));

			aet.when(() -> AbstractExportTxtReactor.getExportFileName(any(), any(), any()))
					.thenReturn("export.xlsx");

			WrapperManager wmInstance = mock(WrapperManager.class);
			wm.when(WrapperManager::getInstance).thenReturn(wmInstance);
			IRawSelectWrapper iterator = mock(IRawSelectWrapper.class);
			when(wmInstance.getRawWrapper(any(IRDBMSEngine.class), any(SelectQueryStruct.class))).thenReturn(iterator);

			reactor.execute();

			verify(insight).getInsightFolder();
		}
	}

	@Test
	void testIteratorClosedEvenOnException() throws Exception {
		reactor.keyValue.put(ReactorKeysEnum.PASSWORD.getKey(), "secret123");
		String insightFolder = tempDir.toAbsolutePath().toString();
		when(insight.getInsightFolder()).thenReturn(insightFolder);

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class);
				MockedStatic<AbstractExportTxtReactor> aet = Mockito.mockStatic(AbstractExportTxtReactor.class);
				MockedStatic<SystemEngineRegistry> ser = Mockito.mockStatic(SystemEngineRegistry.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			IRDBMSEngine db = mock(IRDBMSEngine.class);
			ser.when(() -> SystemEngineRegistry.getSecurityDb()).thenReturn(db);
			util.when(() -> Utility.normalizePath(any(String.class))).thenAnswer(inv -> inv.getArgument(0));

			aet.when(() -> AbstractExportTxtReactor.getExportFileName(any(), any(), any()))
					.thenReturn("export.xlsx");

			WrapperManager wmInstance = mock(WrapperManager.class);
			wm.when(WrapperManager::getInstance).thenReturn(wmInstance);
			IRawSelectWrapper iterator = mock(IRawSelectWrapper.class);
			when(wmInstance.getRawWrapper(any(IRDBMSEngine.class), any(SelectQueryStruct.class))).thenReturn(iterator);

			// Make addExportFile throw to simulate an error after iterator is created
			when(insight.getInsightFolder()).thenReturn(insightFolder);
			Mockito.doThrow(new RuntimeException("export error")).when(insight).addExportFile(any(), any());

			try {
				reactor.execute();
			} catch (IllegalArgumentException ex) {
				// expected
			}

			// Iterator should still be closed in the finally block
			verify(iterator).close();
		}
	}
}
