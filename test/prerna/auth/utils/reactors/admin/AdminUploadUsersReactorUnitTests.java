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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.FileOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.AfterEach;
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
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.SystemEngineRegistry;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

public class AdminUploadUsersReactorUnitTests {

	private static final String[] VALID_HEADERS = { "NAME", "EMAIL", "TYPE", "ID", "PASSWORD", "SALT", "USERNAME",
			"ADMIN", "PUBLISHER" };

	private AdminUploadUsersReactor reactor;
	private Insight insight;
	private User user;
	private NounStore ns;
	private Connection connection;
	private IRDBMSEngine database;

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

		// a security database of its own, where an id of "refused" cannot be saved
		connection = spy(DriverManager.getConnection("jdbc:h2:mem:upload_users_" + UUID.randomUUID()));
		try (Statement statement = connection.createStatement()) {
			statement.execute("CREATE TABLE SMSS_USER (NAME VARCHAR(255), EMAIL VARCHAR(255), TYPE VARCHAR(255), "
					+ "ID VARCHAR(255) CHECK (ID <> 'refused'), PASSWORD VARCHAR(255), SALT VARCHAR(255), "
					+ "USERNAME VARCHAR(255), ADMIN BOOLEAN, PUBLISHER BOOLEAN)");
		}
		clearInvocations(connection);
		database = mock(IRDBMSEngine.class);
		when(database.getConnection()).thenReturn(connection);
		when(database.getQueryUtil()).thenReturn(SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB));

		// loadExcelFile and getExcelIterator use an instance logger that is normally
		// set inside execute(). Set it via reflection so direct calls work.
		Field loggerField = AdminUploadUsersReactor.class.getDeclaredField("logger");
		loggerField.setAccessible(true);
		loggerField.set(reactor, LogManager.getLogger(AdminUploadUsersReactor.class));
	}

	@AfterEach
	void cleanup() throws Exception {
		connection.close();
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

	// --- execute() end-to-end (real Excel files) ---

	@Test
	void testExecuteImportsTheFile() throws Exception {
		String filePath = createUsersExcel("users.xlsx",
				new String[] { "John Doe", "john@test.com", "NATIVE", "user-1", "p1", "s1", "jdoe", "true", "false" },
				new String[] { "Jane Doe", "jane@test.com", "NATIVE", "user-2", "p2", "s2", "jane", "false", "true" })
				.toAbsolutePath().toString();

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<UploadInputUtility> uiu = Mockito.mockStatic(UploadInputUtility.class);
				MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<SystemEngineRegistry> ser = Mockito.mockStatic(SystemEngineRegistry.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
			uiu.when(() -> UploadInputUtility.getFilePath(ns, insight)).thenReturn(filePath);
			util.when(() -> Utility.normalizePath(filePath)).thenReturn(filePath);
			ser.when(SystemEngineRegistry::getSecurityDb).thenReturn(database);

			NounMetadata result = reactor.execute();

			assertEquals(PixelDataType.CONST_STRING, result.getNounType());
			assertTrue(((String) result.getValue()).startsWith("Time to finish = "));
		}
		assertEquals(List.of("user-1", "user-2"), userIds());
		assertEquals("jdoe", user("user-1").get("USERNAME"));
		assertEquals("jane@test.com", user("user-2").get("EMAIL"));
	}

	@Test
	void testExecuteWrapsADatabaseFailure() throws Exception {
		String filePath = createUsersExcel("users.xlsx",
				new String[] { "John Doe", "john@test.com", "NATIVE", "user-1", "p1", "s1", "jdoe", "true", "false" })
				.toAbsolutePath().toString();

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<UploadInputUtility> uiu = Mockito.mockStatic(UploadInputUtility.class);
				MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<SystemEngineRegistry> ser = Mockito.mockStatic(SystemEngineRegistry.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
			uiu.when(() -> UploadInputUtility.getFilePath(ns, insight)).thenReturn(filePath);
			util.when(() -> Utility.normalizePath(filePath)).thenReturn(filePath);
			IRDBMSEngine unreachable = mock(IRDBMSEngine.class);
			when(unreachable.getConnection()).thenThrow(new SQLException("connection refused"));
			ser.when(SystemEngineRegistry::getSecurityDb).thenReturn(unreachable);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Error loading admin users : connection refused", e.getMessage());
		}
	}

	// --- loadExcelFile: header validation ---

	@Test
	void testLoadExcelFileMissingNameHeader() {
		String[] badHeaders = { "EMAIL", "TYPE", "ID", "PASSWORD", "SALT", "USERNAME", "ADMIN", "PUBLISHER" };
		ExcelSheetFileIterator it = mockIterator(badHeaders);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> invokeLoadExcelFile(it));
			assertEquals("One or more headers are missing from the excel", e.getMessage());
		}
	}

	@Test
	void testLoadExcelFileMissingIdHeader() throws Exception {
		String[] badHeaders = { "NAME", "EMAIL", "TYPE", "PASSWORD", "SALT", "USERNAME", "ADMIN", "PUBLISHER" };
		ExcelSheetFileIterator it = mockIterator(badHeaders);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> invokeLoadExcelFile(it));
			assertEquals("One or more headers are missing from the excel", e.getMessage());
		}
		verify(connection).rollback();
		assertTrue(userIds().isEmpty());
	}

	// --- loadExcelFile: valid data ---

	@Test
	void testLoadExcelFileValidRowWithPasswordAndSalt() throws Exception {
		// NAME=0, EMAIL=1, TYPE=2, ID=3, PASSWORD=4, SALT=5, USERNAME=6, ADMIN=7,
		// PUBLISHER=8
		Object[] row = { "John Doe", "john@test.com", "NATIVE", "user-1", "pass123", "salt123", "jdoe", "true",
				"false" };
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);

			invokeLoadExcelFile(it);
		}

		Map<String, Object> saved = user("user-1");
		assertEquals("NATIVE", saved.get("TYPE"));
		assertEquals("John Doe", saved.get("NAME"));
		assertEquals("john@test.com", saved.get("EMAIL"));
		assertEquals("jdoe", saved.get("USERNAME"));
		assertEquals(Boolean.TRUE, saved.get("ADMIN"));
		assertEquals(Boolean.FALSE, saved.get("PUBLISHER"));
		assertEquals("pass123", saved.get("PASSWORD"));
		assertEquals("salt123", saved.get("SALT"));
		verify(connection).commit();
	}

	@Test
	void testLoadExcelFilePasswordWithoutSaltGeneratesSalt() throws Exception {
		Object[] row = { "Jane", "jane@test.com", "NATIVE", "user-2", "mypassword", null, "jane", "false", "true" };
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<AbstractSecurityUtils> secUtils = Mockito.mockStatic(AbstractSecurityUtils.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);
			secUtils.when(AbstractSecurityUtils::generateSalt).thenReturn("generated-salt");
			secUtils.when(() -> AbstractSecurityUtils.hash("mypassword", "generated-salt")).thenReturn("hashed-pw");

			invokeLoadExcelFile(it);
		}

		Map<String, Object> saved = user("user-2");
		assertEquals("hashed-pw", saved.get("PASSWORD"));
		assertEquals("generated-salt", saved.get("SALT"));
	}

	@Test
	void testLoadExcelFileNoPasswordSetsNull() throws Exception {
		Object[] row = { "Bob", "bob@test.com", "NATIVE", "user-3", null, null, "bob", "false", "false" };
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);

			invokeLoadExcelFile(it);
		}

		Map<String, Object> saved = user("user-3");
		assertNull(saved.get("PASSWORD"));
		assertNull(saved.get("SALT"));
	}

	@Test
	void testLoadExcelFileNullNameEmailUsernameSetsNull() throws Exception {
		Object[] row = { null, null, "NATIVE", "user-4", null, null, null, "false", "false" };
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);

			invokeLoadExcelFile(it);
		}

		Map<String, Object> saved = user("user-4");
		assertNull(saved.get("NAME"));
		assertNull(saved.get("EMAIL"));
		assertNull(saved.get("USERNAME"));
	}

	// --- loadExcelFile: ID handling ---

	@Test
	void testLoadExcelFileNullIdThrows() throws Exception {
		Object[] row = { "John", "john@test.com", "NATIVE", null, null, null, "jdoe", "false", "false" };
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> invokeLoadExcelFile(it));
			assertTrue(e.getMessage().contains("Must have the id for the user defined"));
		}
		assertTrue(userIds().isEmpty());
	}

	@Test
	void testLoadExcelFileEmptyStringIdThrows() throws Exception {
		Object[] row = { "John", "john@test.com", "NATIVE", "  ", null, null, "jdoe", "false", "false" };
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> invokeLoadExcelFile(it));
			assertTrue(e.getMessage().contains("Must have the id for the user defined"));
		}
		assertTrue(userIds().isEmpty());
	}

	@Test
	void testLoadExcelFileNumericIdConvertedToBigDecimal() throws Exception {
		// When ID is a Number (e.g. from Excel), it should be converted via BigDecimal
		Object[] row = { "John", "john@test.com", "NATIVE", 12345.0, null, null, "jdoe", "false", "false" };
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);

			invokeLoadExcelFile(it);
		}

		assertEquals(List.of("12345.0"), userIds());
	}

	// --- loadExcelFile: existing user ---

	@Test
	void testLoadExcelFileExistingUserIsSkippedAndTheRestImported() throws Exception {
		try (PreparedStatement ps = connection
				.prepareStatement("INSERT INTO SMSS_USER (ID, NAME, TYPE) VALUES (?,?,?)")) {
			ps.setString(1, "existing-user");
			ps.setString(2, "Original");
			ps.setString(3, "NATIVE");
			ps.executeUpdate();
		}
		Object[] row1 = { "John", "john@test.com", "NATIVE", "existing-user", null, null, "jdoe", "false", "false" };
		Object[] row2 = { "Jane", "jane@test.com", "NATIVE", "user-2", null, null, "jane", "false", "false" };
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row1, row2);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);

			invokeLoadExcelFile(it);
		}

		assertEquals(List.of("existing-user", "user-2"), userIds());
		// the existing account is kept as it is
		assertEquals("Original", user("existing-user").get("NAME"));
	}

	// --- loadExcelFile: type validation ---

	@Test
	void testLoadExcelFileRowWithoutTypeStopsTheImportWithNothingSaved() throws Exception {
		Object[] row1 = { "John", "john@test.com", "NATIVE", "user-1", null, null, "jdoe", "false", "false" };
		Object[] row2 = { "Jane", "jane@test.com", null, "user-2", null, null, "jane", "false", "false" };
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row1, row2);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> invokeLoadExcelFile(it));
			assertEquals("Must have the type of login for the user defined - check row 1", e.getMessage());
		}

		// the row read before it is not saved either
		assertTrue(userIds().isEmpty());
		verify(connection).rollback();
		verify(connection, never()).commit();
	}

	@Test
	void testLoadExcelFileEmptyTypeThrows() throws Exception {
		Object[] row = { "John", "john@test.com", "", "user-1", null, null, "jdoe", "false", "false" };
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> invokeLoadExcelFile(it));
			assertTrue(e.getMessage().contains("Must have the type of login for the user defined"));
		}
		assertTrue(userIds().isEmpty());
	}

	// --- loadExcelFile: database failure ---

	@Test
	void testLoadExcelFileDatabaseFailureRollsBackTheWholeImport() throws Exception {
		Object[] row1 = { "John", "john@test.com", "NATIVE", "user-1", null, null, "jdoe", "false", "false" };
		Object[] row2 = { "Jane", "jane@test.com", "NATIVE", "refused", null, null, "jane", "false", "false" };
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row1, row2);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);

			// the database refuses the second row
			assertThrows(SQLException.class, () -> invokeLoadExcelFile(it));
		}

		assertTrue(userIds().isEmpty());
		verify(connection).rollback();
		verify(connection, never()).commit();
	}

	// --- loadExcelFile: user limit ---

	@Test
	void testLoadExcelFileUserLimitExceeded() throws Exception {
		Object[] row = { "John", "john@test.com", "NATIVE", "user-1", null, null, "jdoe", "false", "false" };
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<SecurityQueryUtils> sqUtils = Mockito.mockStatic(SecurityQueryUtils.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn("5");
			sqUtils.when(SecurityQueryUtils::getApplicationUserCount).thenReturn(5);

			SemossPixelException e = assertThrows(SemossPixelException.class, () -> invokeLoadExcelFile(it));
			assertTrue(e.getMessage().contains("User Limit exceeded the max value of 5"));
		}
		assertTrue(userIds().isEmpty());
	}

	@Test
	void testLoadExcelFileUserLimitNotExceeded() throws Exception {
		Object[] row = { "John", "john@test.com", "NATIVE", "user-1", null, null, "jdoe", "false", "false" };
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<SecurityQueryUtils> sqUtils = Mockito.mockStatic(SecurityQueryUtils.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn("10");
			sqUtils.when(SecurityQueryUtils::getApplicationUserCount).thenReturn(5);

			invokeLoadExcelFile(it);
		}

		assertEquals(List.of("user-1"), userIds());
	}

	@Test
	void testLoadExcelFileUserLimitCountsTheUsersAddedFromTheFile() throws Exception {
		Object[] row1 = { "John", "john@test.com", "NATIVE", "user-1", null, null, "jdoe", "false", "false" };
		Object[] row2 = { "Jane", "jane@test.com", "NATIVE", "user-2", null, null, "jane", "false", "false" };
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row1, row2);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<SecurityQueryUtils> sqUtils = Mockito.mockStatic(SecurityQueryUtils.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn("6");
			sqUtils.when(SecurityQueryUtils::getApplicationUserCount).thenReturn(5);

			// the first user fills the last spot, so the second one goes over the limit
			SemossPixelException e = assertThrows(SemossPixelException.class, () -> invokeLoadExcelFile(it));
			assertTrue(e.getMessage().contains("User Limit exceeded the max value of 6"));
		}
		assertTrue(userIds().isEmpty());
		verify(connection).rollback();
	}

	@Test
	void testLoadExcelFileUserLimitIgnoresExistingUsers() throws Exception {
		try (PreparedStatement ps = connection
				.prepareStatement("INSERT INTO SMSS_USER (ID, NAME, TYPE) VALUES (?,?,?)")) {
			ps.setString(1, "existing-user");
			ps.setString(2, "Original");
			ps.setString(3, "NATIVE");
			ps.executeUpdate();
		}
		Object[] row = { "John", "john@test.com", "NATIVE", "existing-user", null, null, "jdoe", "false", "false" };
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<SecurityQueryUtils> sqUtils = Mockito.mockStatic(SecurityQueryUtils.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn("5");
			sqUtils.when(SecurityQueryUtils::getApplicationUserCount).thenReturn(5);

			// the existing user is skipped, so nothing is added and the limit is not hit
			invokeLoadExcelFile(it);
		}

		assertEquals(List.of("existing-user"), userIds());
	}

	@Test
	void testLoadExcelFileInvalidUserLimitIgnored() throws Exception {
		Object[] row = { "John", "john@test.com", "NATIVE", "user-1", null, null, "jdoe", "false", "false" };
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
			// Non-numeric limit should be caught and ignored
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn("not-a-number");

			invokeLoadExcelFile(it);
		}

		assertEquals(List.of("user-1"), userIds());
	}

	// --- loadExcelFile: multiple rows ---

	@Test
	void testLoadExcelFileMultipleRows() throws Exception {
		Object[] row1 = { "John", "john@test.com", "NATIVE", "user-1", "p1", "s1", "jdoe", "true", "false" };
		Object[] row2 = { "Jane", "jane@test.com", "NATIVE", "user-2", "p2", "s2", "jane", "false", "true" };
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row1, row2);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);

			invokeLoadExcelFile(it);
		}

		// Both rows should be added, in one transaction
		assertEquals(List.of("user-1", "user-2"), userIds());
		verify(connection).commit();
	}

	// --- loadExcelFile: empty rows ---

	@Test
	void testLoadExcelFileNoDataRows() throws Exception {
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS); // no rows

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);

			invokeLoadExcelFile(it);
		}

		assertTrue(userIds().isEmpty());
		verify(connection).commit();
	}

	// --- loadExcelFile: email lowercase ---

	@Test
	void testLoadExcelFileEmailConvertedToLowercase() throws Exception {
		Object[] row = { "John", "JOHN@TEST.COM", "NATIVE", "user-1", null, null, "jdoe", "false", "false" };
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);

			invokeLoadExcelFile(it);
		}

		assertEquals("john@test.com", user("user-1").get("EMAIL"));
	}

	// --- loadExcelFile: empty password with empty salt ---

	@Test
	void testLoadExcelFileEmptyPasswordWithEmptySaltSetsNull() throws Exception {
		Object[] row = { "John", "john@test.com", "NATIVE", "user-1", "", "", "jdoe", "false", "false" };
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);

			invokeLoadExcelFile(it);
		}

		Map<String, Object> saved = user("user-1");
		assertNull(saved.get("PASSWORD"));
		assertNull(saved.get("SALT"));
	}

	// --- loadExcelFile: password with empty salt generates salt ---

	@Test
	void testLoadExcelFilePasswordWithEmptySaltGeneratesSalt() throws Exception {
		Object[] row = { "John", "john@test.com", "NATIVE", "user-1", "mypass", "", "jdoe", "false", "false" };
		ExcelSheetFileIterator it = mockIterator(VALID_HEADERS, row);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<AbstractSecurityUtils> secUtils = Mockito.mockStatic(AbstractSecurityUtils.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT)).thenReturn(null);
			secUtils.when(AbstractSecurityUtils::generateSalt).thenReturn("gen-salt");
			secUtils.when(() -> AbstractSecurityUtils.hash("mypass", "gen-salt")).thenReturn("hashed");

			invokeLoadExcelFile(it);
		}

		Map<String, Object> saved = user("user-1");
		assertEquals("hashed", saved.get("PASSWORD"));
		assertEquals("gen-salt", saved.get("SALT"));
	}

	// --- Helpers ---

	private void invokeLoadExcelFile(ExcelSheetFileIterator it) throws Exception {
		Method m = AdminUploadUsersReactor.class.getDeclaredMethod("loadExcelFile", IRDBMSEngine.class,
				ExcelSheetFileIterator.class);
		m.setAccessible(true);
		try {
			m.invoke(reactor, database, it);
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
				for (int i = 0; i < remaining.length; i++) {
					remaining[i] = true;
				}
				when(it.hasNext()).thenReturn(true, remaining).thenReturn(false);

				IHeadersDataRow[] rest = new IHeadersDataRow[rows.length - 1];
				System.arraycopy(dataRows, 1, rest, 0, rest.length);
				when(it.next()).thenReturn(dataRows[0], rest);
			}
		}
		return it;
	}

	private Path createUsersExcel(String fileName, String[]... dataRows) throws Exception {
		Path excelFile = tempDir.resolve(fileName);
		try (XSSFWorkbook wb = new XSSFWorkbook()) {
			Sheet sheet = wb.createSheet("Sheet1");
			Row headerRow = sheet.createRow(0);
			for (int i = 0; i < VALID_HEADERS.length; i++) {
				headerRow.createCell(i).setCellValue(VALID_HEADERS[i]);
			}
			for (int i = 0; i < dataRows.length; i++) {
				Row row = sheet.createRow(i + 1);
				for (int j = 0; j < dataRows[i].length; j++) {
					row.createCell(j).setCellValue(dataRows[i][j]);
				}
			}
			try (FileOutputStream fos = new FileOutputStream(excelFile.toFile())) {
				wb.write(fos);
			}
		}
		return excelFile;
	}

	/**
	 * @return the ids of the saved users, in order
	 */
	private List<String> userIds() throws SQLException {
		List<String> ids = new ArrayList<>();
		try (Statement statement = connection.createStatement();
				ResultSet rs = statement.executeQuery("SELECT ID FROM SMSS_USER ORDER BY ID")) {
			while (rs.next()) {
				ids.add(rs.getString(1));
			}
		}
		return ids;
	}

	/**
	 * @return the saved user's columns and values
	 */
	private Map<String, Object> user(String id) throws SQLException {
		try (PreparedStatement ps = connection.prepareStatement("SELECT * FROM SMSS_USER WHERE ID=?")) {
			ps.setString(1, id);
			try (ResultSet rs = ps.executeQuery()) {
				assertTrue(rs.next(), id);
				ResultSetMetaData metadata = rs.getMetaData();
				Map<String, Object> row = new HashMap<>();
				for (int i = 1; i <= metadata.getColumnCount(); i++) {
					row.put(metadata.getColumnLabel(i), rs.getObject(i));
				}
				return row;
			}
		}
	}
}
