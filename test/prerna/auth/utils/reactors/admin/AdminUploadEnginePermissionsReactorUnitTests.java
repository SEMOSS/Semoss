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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.FileOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRDBMSEngine;
import prerna.om.Insight;
import prerna.poi.main.helper.excel.ExcelSheetFileIterator;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.SystemEngineRegistry;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class AdminUploadEnginePermissionsReactorUnitTests {

	private AdminUploadEnginePermissionsReactor reactor;
	private Insight insight;
	private User user;
	private NounStore ns;

	@TempDir
	Path tempDir;

	@BeforeEach
	void setup() throws Exception {
		reactor = new AdminUploadEnginePermissionsReactor();
		insight = mock(Insight.class);
		user = mock(User.class);
		ns = mock(NounStore.class);

		reactor.setInsight(insight);
		reactor.setNounStore(ns);
		when(insight.getUser()).thenReturn(user);

		// loadExcelFile and getExcelIterator use an instance logger that is
		// normally set inside execute(). Set it via reflection so direct calls work.
		Field loggerField = AdminUploadEnginePermissionsReactor.class.getDeclaredField("logger");
		loggerField.setAccessible(true);
		loggerField.set(reactor, LogManager.getLogger(AdminUploadEnginePermissionsReactor.class));
	}

	// ===================================================================
	// Reflection helpers
	// ===================================================================

	private void invokeLoadExcelFile(Connection conn, ExcelSheetFileIterator it) throws Exception {
		Method m = AdminUploadEnginePermissionsReactor.class.getDeclaredMethod(
				"loadExcelFile", Connection.class, AbstractSqlQueryUtil.class, ExcelSheetFileIterator.class);
		m.setAccessible(true);
		try {
			m.invoke(reactor, conn, null, it);
		} catch (InvocationTargetException e) {
			if (e.getCause() instanceof Exception) {
				throw (Exception) e.getCause();
			}
			throw e;
		}
	}

	private ExcelSheetFileIterator invokeGetExcelIterator(String fileLocation) throws Exception {
		Method m = AdminUploadEnginePermissionsReactor.class.getDeclaredMethod(
				"getExcelIterator", String.class);
		m.setAccessible(true);
		try {
			return (ExcelSheetFileIterator) m.invoke(reactor, fileLocation);
		} catch (InvocationTargetException e) {
			if (e.getCause() instanceof Exception) {
				throw (Exception) e.getCause();
			}
			throw e;
		}
	}

	// ===================================================================
	// execute() guard-clause tests
	// ===================================================================

	@Test
	void testKeysToGet() {
		assertEquals(2, reactor.keysToGet.length);
		assertEquals(ReactorKeysEnum.FILE_PATH.getKey(), reactor.keysToGet[0]);
		assertEquals(ReactorKeysEnum.SPACE.getKey(), reactor.keysToGet[1]);
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
	void testFileNotFoundThrowsException() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<UploadInputUtility> uiu = Mockito.mockStatic(UploadInputUtility.class);
				MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
			uiu.when(() -> UploadInputUtility.getFilePath(ns, insight)).thenReturn("/nonexistent/file.xlsx");
			util.when(() -> Utility.normalizePath("/nonexistent/file.xlsx")).thenReturn("/nonexistent/file.xlsx");

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Could not find the specified file", e.getMessage());
		}
	}

	@Test
	void testDbConnectionFailureThrowsException() throws Exception {
		Path dummyFile = tempDir.resolve("dummy.xlsx");
		Files.createFile(dummyFile);
		String filePath = dummyFile.toAbsolutePath().toString();

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<UploadInputUtility> uiu = Mockito.mockStatic(UploadInputUtility.class);
				MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<SystemEngineRegistry> ser = Mockito.mockStatic(SystemEngineRegistry.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
			uiu.when(() -> UploadInputUtility.getFilePath(ns, insight)).thenReturn(filePath);
			util.when(() -> Utility.normalizePath(filePath)).thenReturn(filePath);

			IRDBMSEngine db = mock(IRDBMSEngine.class);
			ser.when(() -> SystemEngineRegistry.getSecurityDb()).thenReturn(db);
			when(db.getConnection()).thenThrow(new SQLException("connection refused"));

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Could not connect to database.", e.getMessage());
		}
	}

	// ===================================================================
	// execute() end-to-end tests (real Excel files)
	// ===================================================================

	@Test
	void testExecuteFullSuccess() throws Exception {
		Path excelFile = createPermissionsExcel("perms.xlsx",
				new Object[][] { { "eng1", "user1", "READ_ONLY" } });
		String filePath = excelFile.toAbsolutePath().toString();

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<UploadInputUtility> uiu = Mockito.mockStatic(UploadInputUtility.class);
				MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<SecurityEngineUtils> seu = Mockito.mockStatic(SecurityEngineUtils.class);
				MockedStatic<SystemEngineRegistry> ser = Mockito.mockStatic(SystemEngineRegistry.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
			uiu.when(() -> UploadInputUtility.getFilePath(ns, insight)).thenReturn(filePath);
			util.when(() -> Utility.normalizePath(filePath)).thenReturn(filePath);

			IRDBMSEngine db = mock(IRDBMSEngine.class);
			ser.when(() -> SystemEngineRegistry.getSecurityDb()).thenReturn(db);

			Connection conn = mock(Connection.class);
			PreparedStatement ps = mock(PreparedStatement.class);
			when(db.getConnection()).thenReturn(conn);
			when(db.getQueryUtil()).thenReturn(null);
			when(conn.prepareStatement(anyString())).thenReturn(ps);

			seu.when(() -> SecurityEngineUtils.checkUserHasAccessToDatabase(anyString(), anyString()))
					.thenReturn(false);

			NounMetadata result = reactor.execute();

			assertEquals(PixelDataType.CONST_STRING, result.getNounType());
			assertTrue(((String) result.getValue()).startsWith("Time to finish = "));
			verify(conn).setAutoCommit(false);
			verify(conn).commit();
			verify(ps).addBatch();
			verify(ps).executeBatch();
		}
	}

	@Test
	void testExecuteExceptionWrapping() throws Exception {
		// Use a valid Excel file so getExcelIterator succeeds and the iterator
		// can be properly closed (avoids Windows file-lock issues with corrupt files).
		// Force an error by making getQueryUtil() throw before loadExcelFile runs.
		Path excelFile = createPermissionsExcel("err_wrap.xlsx",
				new Object[][] { { "eng1", "user1", "READ_ONLY" } });
		String filePath = excelFile.toAbsolutePath().toString();

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<UploadInputUtility> uiu = Mockito.mockStatic(UploadInputUtility.class);
				MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<SystemEngineRegistry> ser = Mockito.mockStatic(SystemEngineRegistry.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
			uiu.when(() -> UploadInputUtility.getFilePath(ns, insight)).thenReturn(filePath);
			util.when(() -> Utility.normalizePath(filePath)).thenReturn(filePath);

			IRDBMSEngine db = mock(IRDBMSEngine.class);
			ser.when(() -> SystemEngineRegistry.getSecurityDb()).thenReturn(db);
			Connection conn = mock(Connection.class);
			when(db.getConnection()).thenReturn(conn);
			when(db.getQueryUtil()).thenThrow(new RuntimeException("query util unavailable"));

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertTrue(e.getMessage().startsWith("Error loading admin users"));
		}
	}

	@Test
	void testExecuteCommitFailure() throws Exception {
		Path excelFile = createPermissionsExcel("perms_commit.xlsx",
				new Object[][] { { "eng1", "user1", "READ_ONLY" } });
		String filePath = excelFile.toAbsolutePath().toString();

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<UploadInputUtility> uiu = Mockito.mockStatic(UploadInputUtility.class);
				MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<SecurityEngineUtils> seu = Mockito.mockStatic(SecurityEngineUtils.class);
				MockedStatic<SystemEngineRegistry> ser = Mockito.mockStatic(SystemEngineRegistry.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
			uiu.when(() -> UploadInputUtility.getFilePath(ns, insight)).thenReturn(filePath);
			util.when(() -> Utility.normalizePath(filePath)).thenReturn(filePath);

			IRDBMSEngine db = mock(IRDBMSEngine.class);
			ser.when(() -> SystemEngineRegistry.getSecurityDb()).thenReturn(db);

			Connection conn = mock(Connection.class);
			PreparedStatement ps = mock(PreparedStatement.class);
			when(db.getConnection()).thenReturn(conn);
			when(db.getQueryUtil()).thenReturn(null);
			when(conn.prepareStatement(anyString())).thenReturn(ps);
			doThrow(new SQLException("commit failed")).when(conn).commit();

			seu.when(() -> SecurityEngineUtils.checkUserHasAccessToDatabase(anyString(), anyString()))
					.thenReturn(false);

			NounMetadata result = reactor.execute();

			// Commit failure is logged but not rethrown — result is still returned
			assertTrue(((String) result.getValue()).startsWith("Time to finish = "));
			verify(conn).commit();
		}
	}

	// ===================================================================
	// getExcelIterator() test — real Excel file via reflection
	// ===================================================================

	@Test
	void testGetExcelIterator() throws Exception {
		Path excelFile = createPermissionsExcel("iter_test.xlsx",
				new Object[][] { { "eng1", "user1", "READ_ONLY" } });

		ExcelSheetFileIterator it = invokeGetExcelIterator(excelFile.toAbsolutePath().toString());
		try {
			assertNotNull(it);
			String[] headers = it.getHeaders();
			assertNotNull(headers);
			assertTrue(headers.length >= 3);
			assertTrue(it.hasNext());
		} finally {
			it.close();
		}
	}

	// ===================================================================
	// loadExcelFile() logic tests — no real files, minimal static mocking
	// ===================================================================

	@Test
	void testValidRowInsertsPermission() throws Exception {
		Connection conn = mock(Connection.class);
		PreparedStatement ps = mock(PreparedStatement.class);
		when(conn.prepareStatement(anyString())).thenReturn(ps);

		ExcelSheetFileIterator it = mockIterator(
				new String[] { "ENGINEID", "USERID", "PERMISSION" },
				new Object[][] { { "eng1", "user1", "READ_ONLY" } });

		try (MockedStatic<SecurityEngineUtils> seu = Mockito.mockStatic(SecurityEngineUtils.class)) {
			seu.when(() -> SecurityEngineUtils.checkUserHasAccessToDatabase("eng1", "user1")).thenReturn(false);

			invokeLoadExcelFile(conn, it);

			verify(ps).setString(1, "eng1");
			verify(ps).setString(2, "user1");
			verify(ps).setInt(3, 3); // READ_ONLY id = 3
			verify(ps).addBatch();
			verify(ps).executeBatch();
		}
	}

	@Test
	void testMultipleRowsBatchInserted() throws Exception {
		Connection conn = mock(Connection.class);
		PreparedStatement ps = mock(PreparedStatement.class);
		when(conn.prepareStatement(anyString())).thenReturn(ps);

		ExcelSheetFileIterator it = mockIterator(
				new String[] { "ENGINEID", "USERID", "PERMISSION" },
				new Object[][] {
					{ "eng1", "user1", "READ_ONLY" },
					{ "eng2", "user2", "EDIT" },
					{ "eng3", "user3", "OWNER" }
				});

		try (MockedStatic<SecurityEngineUtils> seu = Mockito.mockStatic(SecurityEngineUtils.class)) {
			seu.when(() -> SecurityEngineUtils.checkUserHasAccessToDatabase(anyString(), anyString()))
					.thenReturn(false);

			invokeLoadExcelFile(conn, it);

			verify(ps, times(3)).addBatch();
			verify(ps).executeBatch();
		}
	}

	@Test
	void testDuplicatePermissionSkipped() throws Exception {
		Connection conn = mock(Connection.class);
		PreparedStatement ps = mock(PreparedStatement.class);
		when(conn.prepareStatement(anyString())).thenReturn(ps);

		ExcelSheetFileIterator it = mockIterator(
				new String[] { "ENGINEID", "USERID", "PERMISSION" },
				new Object[][] {
					{ "eng1", "existingUser", "READ_ONLY" },
					{ "eng1", "newUser", "EDIT" }
				});

		try (MockedStatic<SecurityEngineUtils> seu = Mockito.mockStatic(SecurityEngineUtils.class)) {
			seu.when(() -> SecurityEngineUtils.checkUserHasAccessToDatabase("eng1", "existingUser")).thenReturn(true);
			seu.when(() -> SecurityEngineUtils.checkUserHasAccessToDatabase("eng1", "newUser")).thenReturn(false);

			invokeLoadExcelFile(conn, it);

			verify(ps, times(1)).addBatch();
			verify(ps).executeBatch();
		}
	}

	@Test
	void testMissingHeadersNoInsert() throws Exception {
		Connection conn = mock(Connection.class);
		PreparedStatement ps = mock(PreparedStatement.class);
		when(conn.prepareStatement(anyString())).thenReturn(ps);

		ExcelSheetFileIterator it = mockIterator(
				new String[] { "WRONG", "COLUMNS", "HERE" },
				new Object[][] {});

		invokeLoadExcelFile(conn, it);

		verify(ps, never()).addBatch();
		verify(ps, never()).executeBatch();
		verify(ps).close();
	}

	@Test
	void testEmptyEngineIdNoInsert() throws Exception {
		Connection conn = mock(Connection.class);
		PreparedStatement ps = mock(PreparedStatement.class);
		when(conn.prepareStatement(anyString())).thenReturn(ps);

		ExcelSheetFileIterator it = mockIterator(
				new String[] { "ENGINEID", "USERID", "PERMISSION" },
				new Object[][] { { "", "user1", "READ_ONLY" } });

		invokeLoadExcelFile(conn, it);

		verify(ps, never()).addBatch();
		verify(ps, never()).executeBatch();
	}

	@Test
	void testEmptyUserIdNoInsert() throws Exception {
		Connection conn = mock(Connection.class);
		PreparedStatement ps = mock(PreparedStatement.class);
		when(conn.prepareStatement(anyString())).thenReturn(ps);

		ExcelSheetFileIterator it = mockIterator(
				new String[] { "ENGINEID", "USERID", "PERMISSION" },
				new Object[][] { { "eng1", "", "READ_ONLY" } });

		invokeLoadExcelFile(conn, it);

		verify(ps, never()).addBatch();
		verify(ps, never()).executeBatch();
	}

	@Test
	void testEmptyPermissionNoInsert() throws Exception {
		Connection conn = mock(Connection.class);
		PreparedStatement ps = mock(PreparedStatement.class);
		when(conn.prepareStatement(anyString())).thenReturn(ps);

		ExcelSheetFileIterator it = mockIterator(
				new String[] { "ENGINEID", "USERID", "PERMISSION" },
				new Object[][] { { "eng1", "user1", "" } });

		invokeLoadExcelFile(conn, it);

		verify(ps, never()).addBatch();
		verify(ps, never()).executeBatch();
	}

	@Test
	void testNoRowsNoInsert() throws Exception {
		Connection conn = mock(Connection.class);
		PreparedStatement ps = mock(PreparedStatement.class);
		when(conn.prepareStatement(anyString())).thenReturn(ps);

		ExcelSheetFileIterator it = mockIterator(
				new String[] { "ENGINEID", "USERID", "PERMISSION" },
				new Object[][] {});

		invokeLoadExcelFile(conn, it);

		verify(ps, never()).addBatch();
		verify(ps, never()).executeBatch();
		verify(ps).close();
	}

	@Test
	void testNullEngineIdNoInsert() throws Exception {
		Connection conn = mock(Connection.class);
		PreparedStatement ps = mock(PreparedStatement.class);
		when(conn.prepareStatement(anyString())).thenReturn(ps);

		ExcelSheetFileIterator it = mockIterator(
				new String[] { "ENGINEID", "USERID", "PERMISSION" },
				new Object[][] { { null, "user1", "READ_ONLY" } });

		invokeLoadExcelFile(conn, it);

		verify(ps, never()).addBatch();
		verify(ps, never()).executeBatch();
	}

	@Test
	void testNullUserIdNoInsert() throws Exception {
		Connection conn = mock(Connection.class);
		PreparedStatement ps = mock(PreparedStatement.class);
		when(conn.prepareStatement(anyString())).thenReturn(ps);

		ExcelSheetFileIterator it = mockIterator(
				new String[] { "ENGINEID", "USERID", "PERMISSION" },
				new Object[][] { { "eng1", null, "READ_ONLY" } });

		invokeLoadExcelFile(conn, it);

		verify(ps, never()).addBatch();
		verify(ps, never()).executeBatch();
	}

	@Test
	void testNullPermissionNoInsert() throws Exception {
		Connection conn = mock(Connection.class);
		PreparedStatement ps = mock(PreparedStatement.class);
		when(conn.prepareStatement(anyString())).thenReturn(ps);

		ExcelSheetFileIterator it = mockIterator(
				new String[] { "ENGINEID", "USERID", "PERMISSION" },
				new Object[][] { { "eng1", "user1", null } });

		invokeLoadExcelFile(conn, it);

		verify(ps, never()).addBatch();
		verify(ps, never()).executeBatch();
	}

	@Test
	void testInvalidPermissionRoleNoInsert() throws Exception {
		Connection conn = mock(Connection.class);
		PreparedStatement ps = mock(PreparedStatement.class);
		when(conn.prepareStatement(anyString())).thenReturn(ps);

		ExcelSheetFileIterator it = mockIterator(
				new String[] { "ENGINEID", "USERID", "PERMISSION" },
				new Object[][] { { "eng1", "user1", "INVALID_ROLE" } });

		// Enum.valueOf throws IllegalArgumentException, caught internally
		invokeLoadExcelFile(conn, it);

		verify(ps, never()).addBatch();
		verify(ps, never()).executeBatch();
	}

	// ===================================================================
	// Static field tests
	// ===================================================================

	@Test
	void testStaticInsertQuery() throws Exception {
		Field f = AdminUploadEnginePermissionsReactor.class.getDeclaredField("insertQuery");
		f.setAccessible(true);
		String query = (String) f.get(null);
		assertEquals("INSERT INTO ENGINEPERMISSION (ENGINEID, USERID, PERMISSION) VALUES (?, ?, ?)", query);
	}

	@Test
	void testStaticKeyConstants() {
		assertEquals("ENGINEID", AdminUploadEnginePermissionsReactor.ENGINE_ID_KEY);
		assertEquals("USERID", AdminUploadEnginePermissionsReactor.USER_ID_KEY);
		assertEquals("PERMISSION", AdminUploadEnginePermissionsReactor.PERMISSION_KEY);
	}

	// ===================================================================
	// Helpers
	// ===================================================================

	private Path createPermissionsExcel(String fileName, Object[][] dataRows) throws Exception {
		Path excelFile = tempDir.resolve(fileName);
		try (XSSFWorkbook wb = new XSSFWorkbook()) {
			Sheet sheet = wb.createSheet("Sheet1");
			Row headerRow = sheet.createRow(0);
			headerRow.createCell(0).setCellValue("ENGINEID");
			headerRow.createCell(1).setCellValue("USERID");
			headerRow.createCell(2).setCellValue("PERMISSION");
			for (int i = 0; i < dataRows.length; i++) {
				Row row = sheet.createRow(i + 1);
				for (int j = 0; j < dataRows[i].length; j++) {
					row.createCell(j).setCellValue((String) dataRows[i][j]);
				}
			}
			try (FileOutputStream fos = new FileOutputStream(excelFile.toFile())) {
				wb.write(fos);
			}
		}
		return excelFile;
	}

	private ExcelSheetFileIterator mockIterator(String[] headers, Object[][] rows) {
		ExcelSheetFileIterator it = mock(ExcelSheetFileIterator.class);
		when(it.getHeaders()).thenReturn(headers);

		if (rows.length == 0) {
			when(it.hasNext()).thenReturn(false);
			return it;
		}

		Boolean[] hasNextSeq = new Boolean[rows.length + 1];
		Arrays.fill(hasNextSeq, 0, rows.length, true);
		hasNextSeq[rows.length] = false;
		when(it.hasNext()).thenReturn(hasNextSeq[0],
				Arrays.copyOfRange(hasNextSeq, 1, hasNextSeq.length));

		IHeadersDataRow[] dataRows = new IHeadersDataRow[rows.length];
		for (int i = 0; i < rows.length; i++) {
			dataRows[i] = mock(IHeadersDataRow.class);
			when(dataRows[i].getRawValues()).thenReturn(rows[i]);
		}
		when(it.next()).thenReturn(dataRows[0],
				Arrays.copyOfRange(dataRows, 1, dataRows.length));

		return it;
	}
}
