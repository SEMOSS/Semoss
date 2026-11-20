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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRDBMSEngine;
import prerna.om.Insight;
import prerna.poi.main.helper.excel.ExcelSheetFileIterator;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.Constants;
import prerna.util.SystemEngineRegistry;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class AdminUploadUsersReactorUnitTests {

	private static final String[] VALID_HEADERS = {
			"NAME", "EMAIL", "TYPE", "ID", "PASSWORD", "SALT", "USERNAME", "ADMIN", "PUBLISHER"
	};

	private AdminUploadUsersReactor reactor;
	private Insight insight;
	private User user;
	private NounStore ns;
	private Connection conn;
	private PreparedStatement ps;
	private AbstractSqlQueryUtil queryUtil;

	@TempDir
	Path tempDir;

	@BeforeEach
	void setup() throws Exception {
		reactor = new AdminUploadUsersReactor();
		insight = mock(Insight.class);
		user = mock(User.class);
		ns = mock(NounStore.class);
		reactor.setInsight(insight);
		reactor.setNounStore(ns);
		when(insight.getUser()).thenReturn(user);

		conn = mock(Connection.class);
		ps = mock(PreparedStatement.class);
		when(conn.prepareStatement(anyString())).thenReturn(ps);
		queryUtil = mock(AbstractSqlQueryUtil.class);

		// Set logger via reflection (private field)
		Field loggerField = AdminUploadUsersReactor.class.getDeclaredField("logger");
		loggerField.setAccessible(true);
		loggerField.set(reactor, LogManager.getLogger(AdminUploadUsersReactor.class));
	}

	private void invokeLoadExcelFile(ExcelSheetFileIterator it) throws Exception {
		Method m = AdminUploadUsersReactor.class.getDeclaredMethod(
				"loadExcelFile", Connection.class, AbstractSqlQueryUtil.class, ExcelSheetFileIterator.class);
		m.setAccessible(true);
		try {
			m.invoke(reactor, conn, queryUtil, it);
		} catch (InvocationTargetException e) {
			if (e.getCause() instanceof Exception) {
				throw (Exception) e.getCause();
			}
			throw e;
		}
	}

	private ExcelSheetFileIterator mockIterator(String[] headers, Object[]... rows) {
		ExcelSheetFileIterator it = mock(ExcelSheetFileIterator.class);
		when(it.getHeaders()).thenReturn(headers);
		if (rows.length == 0) {
			when(it.hasNext()).thenReturn(false);
		} else {
			// Create data rows first to avoid nested stubbing issues
			IHeadersDataRow[] dataRows = new IHeadersDataRow[rows.length];
			for (int i = 0; i < rows.length; i++) {
				dataRows[i] = mock(IHeadersDataRow.class);
				when(dataRows[i].getRawValues()).thenReturn(rows[i]);
			}

			// Set up hasNext: true for each row, then false
			if (rows.length == 1) {
				when(it.hasNext()).thenReturn(true).thenReturn(false);
				when(it.next()).thenReturn(dataRows[0]);
			} else {
				Boolean[] remaining = new Boolean[rows.length - 1];
				for (int i = 0; i < remaining.length; i++) remaining[i] = true;
				when(it.hasNext()).thenReturn(true, remaining).thenReturn(false);

				IHeadersDataRow[] rest = new IHeadersDataRow[rows.length - 1];
				System.arraycopy(dataRows, 1, rest, 0, rest.length);
				when(it.next()).thenReturn(dataRows[0], rest);
			}
		}
		return it;
	}

	// --- Constructor / keys ---

	@Test
	void testKeysToGet() {
		assertEquals(2, reactor.keysToGet.length);
		assertEquals(ReactorKeysEnum.FILE_PATH.getKey(), reactor.keysToGet[0]);
		assertEquals(ReactorKeysEnum.SPACE.getKey(), reactor.keysToGet[1]);
	}

	// --- Static constants ---

	@Test
	void testStaticKeyConstants() {
		assertEquals("NAME", AdminUploadUsersReactor.NAME_KEY);
		assertEquals("EMAIL", AdminUploadUsersReactor.EMAIL_KEY);
		assertEquals("TYPE", AdminUploadUsersReactor.TYPE_KEY);
		assertEquals("ID", AdminUploadUsersReactor.ID_KEY);
		assertEquals("PASSWORD", AdminUploadUsersReactor.PASSWORD_KEY);
		assertEquals("SALT", AdminUploadUsersReactor.SALT_KEY);
		assertEquals("USERNAME", AdminUploadUsersReactor.USERNAME_KEY);
		assertEquals("ADMIN", AdminUploadUsersReactor.ADMIN_KEY);
		assertEquals("PUBLISHER", AdminUploadUsersReactor.PUBLISHER_KEY);
	}

	@Test
	void testInsertQueryFormat() throws Exception {
		Field f = AdminUploadUsersReactor.class.getDeclaredField("insertQuery");
		f.setAccessible(true);
		String query = (String) f.get(null);
		assertTrue(query.startsWith("INSERT INTO SMSS_USER ("));
		assertTrue(query.contains("NAME"));
		assertTrue(query.contains("EMAIL"));
		assertTrue(query.contains("TYPE"));
		assertTrue(query.contains("ID"));
		assertTrue(query.contains("PASSWORD"));
		assertTrue(query.contains("SALT"));
		assertTrue(query.contains("USERNAME"));
		assertTrue(query.contains("ADMIN"));
		assertTrue(query.contains("PUBLISHER"));
		assertTrue(query.contains("VALUES ("));
		// 9 placeholders
		assertEquals(9, query.chars().filter(c -> c == '?').count());
	}

	// --- Admin check ---

	@Test
	void testNonAdminThrowsException() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(null);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("User must be an admin to perform this function", e.getMessage());
		}
	}

	// --- File validation ---

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
	void testDirectoryInsteadOfFileThrowsException() throws Exception {
		Path subDir = tempDir.resolve("notafile");
		Files.createDirectory(subDir);
		String dirPath = subDir.toAbsolutePath().toString();

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<UploadInputUtility> uiu = Mockito.mockStatic(UploadInputUtility.class);
				MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
			uiu.when(() -> UploadInputUtility.getFilePath(ns, insight)).thenReturn(dirPath);
			util.when(() -> Utility.normalizePath(dirPath)).thenReturn(dirPath);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Could not find the specified file", e.getMessage());
		}
	}

	// --- Database connection ---

	@Test
	void testDbConnectionFailureThrowsException() throws Exception {
		Path dummyFile = tempDir.resolve("users.xlsx");
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

	@Test
	void testDbConnectionNullMessageThrowsException() throws Exception {
		Path dummyFile = tempDir.resolve("users.xlsx");
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
			when(db.getConnection()).thenThrow(new SQLException((String) null));

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Could not connect to database.", e.getMessage());
		}
	}

	@Test
	void testSetAutoCommitFailureThrowsException() throws Exception {
		Path dummyFile = tempDir.resolve("users.xlsx");
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
			Connection c = mock(Connection.class);
			when(db.getConnection()).thenReturn(c);
			Mockito.doThrow(new SQLException("cannot set auto commit")).when(c).setAutoCommit(false);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Could not connect to database.", e.getMessage());
		}
	}

	// --- loadExcelFile: header validation ---

	@Test
	void testLoadExcelFileMissingNameHeader() {
		String[] badHeaders = {"EMAIL", "TYPE", "ID", "PASSWORD", "SALT", "USERNAME", "ADMIN", "PUBLISHER"};
		ExcelSheetFileIterator it = mockIterator(badHeaders);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> invokeLoadExcelFile(it));
			assertEquals("One or more headers are missing from the excel", e.getMessage());
		}
	}

	@Test
	void testLoadExcelFileMissingIdHeader() {
		String[] badHeaders = {"NAME", "EMAIL", "TYPE", "PASSWORD", "SALT", "USERNAME", "ADMIN", "PUBLISHER"};
		ExcelSheetFileIterator it = mockIterator(badHeaders);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> invokeLoadExcelFile(it));
			assertEquals("One or more headers are missing from the excel", e.getMessage());
		}
	}

	// --- loadExcelFile: valid data ---

	@Test
	void testLoadExcelFileValidRowWithPasswordAndSalt() throws Exception {
		// NAME=0, EMAIL=1, TYPE=2, ID=3, PASSWORD=4, SALT=5, USERNAME=6, ADMIN=7, PUBLISHER=8
		Object[] row = {"John Doe", "john@test.com", "NATIVE", "user-1", "pass123", "salt123", "jdoe", "true", "false"};
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<SecurityQueryUtils> sqUtils = Mockito.mockStatic(SecurityQueryUtils.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);
			sqUtils.when(() -> SecurityQueryUtils.checkUserExist("user-1")).thenReturn(false);

			invokeLoadExcelFile(it);

			verify(ps).setString(4, "user-1");           // ID
			verify(ps).setString(3, "NATIVE");            // TYPE
			verify(ps).setString(1, "John Doe");          // NAME
			verify(ps).setString(2, "john@test.com");     // EMAIL
			verify(ps).setString(7, "jdoe");              // USERNAME
			verify(ps).setBoolean(8, true);               // ADMIN
			verify(ps).setBoolean(9, false);              // PUBLISHER
			verify(ps).setString(5, "pass123");           // PASSWORD
			verify(ps).setString(6, "salt123");           // SALT
			verify(ps).addBatch();
			verify(ps).executeBatch();
		}
	}

	@Test
	void testLoadExcelFilePasswordWithoutSaltGeneratesSalt() throws Exception {
		Object[] row = {"Jane", "jane@test.com", "NATIVE", "user-2", "mypassword", null, "jane", "false", "true"};
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<SecurityQueryUtils> sqUtils = Mockito.mockStatic(SecurityQueryUtils.class);
				MockedStatic<AbstractSecurityUtils> secUtils = Mockito.mockStatic(AbstractSecurityUtils.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);
			sqUtils.when(() -> SecurityQueryUtils.checkUserExist("user-2")).thenReturn(false);
			secUtils.when(AbstractSecurityUtils::generateSalt).thenReturn("generated-salt");
			secUtils.when(() -> AbstractSecurityUtils.hash("mypassword", "generated-salt")).thenReturn("hashed-pw");

			invokeLoadExcelFile(it);

			verify(ps).setString(5, "hashed-pw");        // PASSWORD (hashed)
			verify(ps).setString(6, "generated-salt");   // SALT (generated)
			verify(ps).addBatch();
		}
	}

	@Test
	void testLoadExcelFileNoPasswordSetsNull() throws Exception {
		Object[] row = {"Bob", "bob@test.com", "NATIVE", "user-3", null, null, "bob", "false", "false"};
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<SecurityQueryUtils> sqUtils = Mockito.mockStatic(SecurityQueryUtils.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);
			sqUtils.when(() -> SecurityQueryUtils.checkUserExist("user-3")).thenReturn(false);

			invokeLoadExcelFile(it);

			verify(ps).setNull(5, java.sql.Types.VARCHAR);  // PASSWORD null
			verify(ps).setNull(6, java.sql.Types.VARCHAR);  // SALT null
			verify(ps).addBatch();
		}
	}

	@Test
	void testLoadExcelFileNullNameEmailUsernameSetsNull() throws Exception {
		Object[] row = {null, null, "NATIVE", "user-4", null, null, null, "false", "false"};
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<SecurityQueryUtils> sqUtils = Mockito.mockStatic(SecurityQueryUtils.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);
			sqUtils.when(() -> SecurityQueryUtils.checkUserExist("user-4")).thenReturn(false);

			invokeLoadExcelFile(it);

			verify(ps).setNull(1, java.sql.Types.VARCHAR);  // NAME null
			verify(ps).setNull(2, java.sql.Types.VARCHAR);  // EMAIL null
			verify(ps).setNull(7, java.sql.Types.VARCHAR);  // USERNAME null
			verify(ps).addBatch();
		}
	}

	// --- loadExcelFile: ID handling ---

	@Test
	void testLoadExcelFileNullIdThrows() {
		Object[] row = {"John", "john@test.com", "NATIVE", null, null, null, "jdoe", "false", "false"};
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> invokeLoadExcelFile(it));
			assertTrue(e.getMessage().contains("Must have the id for the user defined"));
		}
	}

	@Test
	void testLoadExcelFileEmptyStringIdThrows() {
		Object[] row = {"John", "john@test.com", "NATIVE", "  ", null, null, "jdoe", "false", "false"};
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> invokeLoadExcelFile(it));
			assertTrue(e.getMessage().contains("Must have the id for the user defined"));
		}
	}

	@Test
	void testLoadExcelFileNumericIdConvertedToBigDecimal() throws Exception {
		// When ID is a Number (e.g. from Excel), it should be converted via BigDecimal
		Object[] row = {"John", "john@test.com", "NATIVE", 12345.0, null, null, "jdoe", "false", "false"};
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<SecurityQueryUtils> sqUtils = Mockito.mockStatic(SecurityQueryUtils.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);
			sqUtils.when(() -> SecurityQueryUtils.checkUserExist("12345.0")).thenReturn(false);

			invokeLoadExcelFile(it);

			verify(ps).setString(4, "12345.0");
			verify(ps).addBatch();
		}
	}

	// --- loadExcelFile: existing user ---

	@Test
	void testLoadExcelFileExistingUserSkipped() throws Exception {
		Object[] row = {"John", "john@test.com", "NATIVE", "existing-user", null, null, "jdoe", "false", "false"};
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<SecurityQueryUtils> sqUtils = Mockito.mockStatic(SecurityQueryUtils.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);
			sqUtils.when(() -> SecurityQueryUtils.checkUserExist("existing-user")).thenReturn(true);

			invokeLoadExcelFile(it);

			// Should NOT have added to batch since user exists
			verify(ps, never()).addBatch();
			verify(ps).executeBatch();
		}
	}

	// --- loadExcelFile: type validation ---

	@Test
	void testLoadExcelFileNullTypeThrows() {
		Object[] row = {"John", "john@test.com", null, "user-1", null, null, "jdoe", "false", "false"};
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<SecurityQueryUtils> sqUtils = Mockito.mockStatic(SecurityQueryUtils.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);
			sqUtils.when(() -> SecurityQueryUtils.checkUserExist("user-1")).thenReturn(false);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> invokeLoadExcelFile(it));
			assertTrue(e.getMessage().contains("Must have the type of login for the user defined"));
		}
	}

	@Test
	void testLoadExcelFileEmptyTypeThrows() {
		Object[] row = {"John", "john@test.com", "", "user-1", null, null, "jdoe", "false", "false"};
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<SecurityQueryUtils> sqUtils = Mockito.mockStatic(SecurityQueryUtils.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);
			sqUtils.when(() -> SecurityQueryUtils.checkUserExist("user-1")).thenReturn(false);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> invokeLoadExcelFile(it));
			assertTrue(e.getMessage().contains("Must have the type of login for the user defined"));
		}
	}

	// --- loadExcelFile: user limit ---

	@Test
	void testLoadExcelFileUserLimitExceeded() {
		Object[] row = {"John", "john@test.com", "NATIVE", "user-1", null, null, "jdoe", "false", "false"};
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<SecurityQueryUtils> sqUtils = Mockito.mockStatic(SecurityQueryUtils.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn("5");
			sqUtils.when(SecurityQueryUtils::getApplicationUserCount).thenReturn(5);

			SemossPixelException e = assertThrows(SemossPixelException.class,
					() -> invokeLoadExcelFile(it));
			assertTrue(e.getMessage().contains("User Limit exceeded the max value of 5"));
		}
	}

	@Test
	void testLoadExcelFileUserLimitNotExceeded() throws Exception {
		Object[] row = {"John", "john@test.com", "NATIVE", "user-1", null, null, "jdoe", "false", "false"};
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<SecurityQueryUtils> sqUtils = Mockito.mockStatic(SecurityQueryUtils.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn("10");
			sqUtils.when(SecurityQueryUtils::getApplicationUserCount).thenReturn(5);
			sqUtils.when(() -> SecurityQueryUtils.checkUserExist("user-1")).thenReturn(false);

			invokeLoadExcelFile(it);

			verify(ps).addBatch();
		}
	}

	@Test
	void testLoadExcelFileInvalidUserLimitIgnored() throws Exception {
		Object[] row = {"John", "john@test.com", "NATIVE", "user-1", null, null, "jdoe", "false", "false"};
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<SecurityQueryUtils> sqUtils = Mockito.mockStatic(SecurityQueryUtils.class)) {
			// Non-numeric limit should be caught and ignored
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn("not-a-number");
			sqUtils.when(() -> SecurityQueryUtils.checkUserExist("user-1")).thenReturn(false);

			invokeLoadExcelFile(it);

			verify(ps).addBatch();
		}
	}

	// --- loadExcelFile: multiple rows ---

	@Test
	void testLoadExcelFileMultipleRows() throws Exception {
		Object[] row1 = {"John", "john@test.com", "NATIVE", "user-1", "p1", "s1", "jdoe", "true", "false"};
		Object[] row2 = {"Jane", "jane@test.com", "NATIVE", "user-2", "p2", "s2", "jane", "false", "true"};
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row1, row2);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<SecurityQueryUtils> sqUtils = Mockito.mockStatic(SecurityQueryUtils.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);
			sqUtils.when(() -> SecurityQueryUtils.checkUserExist(anyString())).thenReturn(false);

			invokeLoadExcelFile(it);

			// Both rows should be added
			verify(ps, Mockito.times(2)).addBatch();
			verify(ps).executeBatch();
		}
	}

	// --- loadExcelFile: empty rows ---

	@Test
	void testLoadExcelFileNoDataRows() throws Exception {
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS); // no rows

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);

			invokeLoadExcelFile(it);

			verify(ps, never()).addBatch();
			verify(ps).executeBatch();
		}
	}

	// --- loadExcelFile: email lowercase ---

	@Test
	void testLoadExcelFileEmailConvertedToLowercase() throws Exception {
		Object[] row = {"John", "JOHN@TEST.COM", "NATIVE", "user-1", null, null, "jdoe", "false", "false"};
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<SecurityQueryUtils> sqUtils = Mockito.mockStatic(SecurityQueryUtils.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);
			sqUtils.when(() -> SecurityQueryUtils.checkUserExist("user-1")).thenReturn(false);

			invokeLoadExcelFile(it);

			verify(ps).setString(2, "john@test.com");
		}
	}

	// --- loadExcelFile: empty password with empty salt ---

	@Test
	void testLoadExcelFileEmptyPasswordWithEmptySaltSetsNull() throws Exception {
		Object[] row = {"John", "john@test.com", "NATIVE", "user-1", "", "", "jdoe", "false", "false"};
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<SecurityQueryUtils> sqUtils = Mockito.mockStatic(SecurityQueryUtils.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);
			sqUtils.when(() -> SecurityQueryUtils.checkUserExist("user-1")).thenReturn(false);

			invokeLoadExcelFile(it);

			verify(ps).setNull(5, java.sql.Types.VARCHAR);
			verify(ps).setNull(6, java.sql.Types.VARCHAR);
		}
	}

	// --- loadExcelFile: password with empty salt generates salt ---

	@Test
	void testLoadExcelFilePasswordWithEmptySaltGeneratesSalt() throws Exception {
		Object[] row = {"John", "john@test.com", "NATIVE", "user-1", "mypass", "", "jdoe", "false", "false"};
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<SecurityQueryUtils> sqUtils = Mockito.mockStatic(SecurityQueryUtils.class);
				MockedStatic<AbstractSecurityUtils> secUtils = Mockito.mockStatic(AbstractSecurityUtils.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);
			sqUtils.when(() -> SecurityQueryUtils.checkUserExist("user-1")).thenReturn(false);
			secUtils.when(AbstractSecurityUtils::generateSalt).thenReturn("gen-salt");
			secUtils.when(() -> AbstractSecurityUtils.hash("mypass", "gen-salt")).thenReturn("hashed");

			invokeLoadExcelFile(it);

			verify(ps).setString(5, "hashed");
			verify(ps).setString(6, "gen-salt");
		}
	}
}
