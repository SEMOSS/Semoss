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
package prerna.auth.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.date.SemossDate;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.SystemEngineRegistry;

public class AbstractSecurityUtilsUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private IRDBMSEngine securityDb;
	private List<String> tables = new ArrayList<>();

	@BeforeEach
	void setup() {
		securityDb = SystemEngineRegistry.getSecurityDb();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		assertNotNull(this.securityDb);
	}

	@AfterEach
	void cleanup() throws SQLException {
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		// clear test database inside of temp directory
		// quicker than deleting and recreating
		tables = UnitTestSecurityAuthUtils.clearSecurityDB(securityDb, tables);
	}

	@Test
	void testDefaultVariables() {
		assertFalse(AbstractSecurityUtils.anonymousUsersEnabled());
		assertFalse(AbstractSecurityUtils.anonymousUserUploadData());
		assertFalse(AbstractSecurityUtils.adminSetExporter());
		assertFalse(AbstractSecurityUtils.adminOnlyProjectAdd());
		assertFalse(AbstractSecurityUtils.adminOnlyProjectDelete());
		assertFalse(AbstractSecurityUtils.adminOnlyProjectAddAccess());
		assertFalse(AbstractSecurityUtils.adminOnlyProjectSetPublic());
		assertFalse(AbstractSecurityUtils.adminOnlyProjectSetDiscoverable());
		assertFalse(AbstractSecurityUtils.adminOnlyDatabaseAdd());
		assertFalse(AbstractSecurityUtils.adminOnlyDatabaseDelete());
		assertFalse(AbstractSecurityUtils.adminOnlyDatabaseAddAccess());
		assertFalse(AbstractSecurityUtils.adminOnlyDatabaseSetPublic());
		assertFalse(AbstractSecurityUtils.adminOnlyDatabaseSetDiscoverable());
		assertFalse(AbstractSecurityUtils.adminOnlyModelAdd());
		assertFalse(AbstractSecurityUtils.adminOnlyModelDelete());
		assertFalse(AbstractSecurityUtils.adminOnlyModelAddAccess());
		assertFalse(AbstractSecurityUtils.adminOnlyModelSetPublic());
		assertFalse(AbstractSecurityUtils.adminOnlyModelSetDiscoverable());
		assertFalse(AbstractSecurityUtils.adminOnlyStorageAdd());
		assertFalse(AbstractSecurityUtils.adminOnlyStorageDelete());
		assertFalse(AbstractSecurityUtils.adminOnlyStorageAddAccess());
		assertFalse(AbstractSecurityUtils.adminOnlyStorageSetPublic());
		assertFalse(AbstractSecurityUtils.adminOnlyVectorAdd());
		assertFalse(AbstractSecurityUtils.adminOnlyVectorDelete());
		assertFalse(AbstractSecurityUtils.adminOnlyVectorAddAccess());
		assertFalse(AbstractSecurityUtils.adminOnlyVectorSetPublic());
		assertFalse(AbstractSecurityUtils.adminOnlyVectorSetDiscoverable());
		assertFalse(AbstractSecurityUtils.adminOnlyFunctionAdd());
		assertFalse(AbstractSecurityUtils.adminOnlyFunctionDelete());
		assertFalse(AbstractSecurityUtils.adminOnlyFunctionAddAccess());
		assertFalse(AbstractSecurityUtils.adminOnlyFunctionSetPublic());
		assertFalse(AbstractSecurityUtils.adminOnlyFunctionSetDiscoverable());
		assertFalse(AbstractSecurityUtils.adminOnlyGuardrailAdd());
		assertFalse(AbstractSecurityUtils.adminOnlyGuardrailDelete());
		assertFalse(AbstractSecurityUtils.adminOnlyGuardrailSetPublic());
		assertFalse(AbstractSecurityUtils.adminOnlyGuardrailSetDiscoverable());
		assertFalse(AbstractSecurityUtils.adminOnlyInsightSetPublic());
		assertFalse(AbstractSecurityUtils.adminOnlyInsightAddAccess());
		assertFalse(AbstractSecurityUtils.adminOnlyInsightShare());
	}

	@Test
	void testAdminOnlyEngineAdd_EngineId() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testEngine", "testName", user);

		assertFalse(AbstractSecurityUtils.adminOnlyEngineAdd("testEngine"));
	}

	@ParameterizedTest
	@EnumSource(IEngine.CATALOG_TYPE.class)
	void testAdminOnlyEngineAdd(IEngine.CATALOG_TYPE type) {
		if (type == IEngine.CATALOG_TYPE.VENV || type == IEngine.CATALOG_TYPE.PROJECT) {
			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> AbstractSecurityUtils.adminOnlyEngineAdd(type));
			assertEquals("Admin only configuration must be defined for catalog type = " + type, e.getMessage());
		} else {
			assertFalse(AbstractSecurityUtils.adminOnlyEngineAdd(type));
		}
	}

	@Test
	void testAdminOnlyEngineDelete_EngineId() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testEngine", "testName", user);

		assertFalse(AbstractSecurityUtils.adminOnlyEngineDelete("testEngine"));
	}

	@ParameterizedTest
	@EnumSource(IEngine.CATALOG_TYPE.class)
	void testAdminOnlyEngineDelete(IEngine.CATALOG_TYPE type) {
		if (type == IEngine.CATALOG_TYPE.VENV || type == IEngine.CATALOG_TYPE.PROJECT) {
			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> AbstractSecurityUtils.adminOnlyEngineDelete(type));
			assertEquals("Admin only configuration must be defined for catalog type = " + type, e.getMessage());
		} else {
			assertFalse(AbstractSecurityUtils.adminOnlyEngineDelete(type));
		}
	}

	@Test
	void testAdminOnlyEngineAddAccess_EngineId() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testEngine", "testName", user);

		assertFalse(AbstractSecurityUtils.adminOnlyEngineAddAccess("testEngine"));
	}

	@ParameterizedTest
	@EnumSource(IEngine.CATALOG_TYPE.class)
	void testAdminOnlyEngineAddAccess(IEngine.CATALOG_TYPE type) {
		if (type == IEngine.CATALOG_TYPE.VENV || type == IEngine.CATALOG_TYPE.PROJECT) {
			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> AbstractSecurityUtils.adminOnlyEngineAddAccess(type));
			assertEquals("Admin only configuration must be defined for catalog type = " + type, e.getMessage());
		} else {
			assertFalse(AbstractSecurityUtils.adminOnlyEngineAddAccess(type));
		}
	}

	@Test
	void testAdminOnlyEngineSetPublic_EngineId() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testEngine", "testName", user);

		assertFalse(AbstractSecurityUtils.adminOnlyEngineSetPublic("testEngine"));
	}

	@ParameterizedTest
	@EnumSource(IEngine.CATALOG_TYPE.class)
	void testAdminOnlyEngineSetPublic(IEngine.CATALOG_TYPE type) {
		if (type == IEngine.CATALOG_TYPE.VENV || type == IEngine.CATALOG_TYPE.PROJECT) {
			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> AbstractSecurityUtils.adminOnlyEngineSetPublic(type));
			assertEquals("Admin only configuration must be defined for catalog type = " + type, e.getMessage());
		} else {
			assertFalse(AbstractSecurityUtils.adminOnlyEngineSetPublic(type));
		}
	}

	@Test
	void testAdminOnlyEngineDiscoverable_EngineId() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testEngine", "testName", user);

		assertFalse(AbstractSecurityUtils.adminOnlyEngineSetDiscoverable("testEngine"));
	}

	@ParameterizedTest
	@EnumSource(IEngine.CATALOG_TYPE.class)
	void testAdminOnlyEngineDiscoverable(IEngine.CATALOG_TYPE type) {
		if (type == IEngine.CATALOG_TYPE.VENV || type == IEngine.CATALOG_TYPE.PROJECT) {
			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> AbstractSecurityUtils.adminOnlyEngineSetDiscoverable(type));
			assertEquals("Admin only configuration must be defined for catalog type = " + type, e.getMessage());
		} else {
			assertFalse(AbstractSecurityUtils.adminOnlyEngineSetDiscoverable(type));
		}
	}

	@Test
	void testInitialize_alreadyInitialized() throws Exception {
		// just testing no errors thrown
		AbstractSecurityUtils.initialize();
	}

	@Test
	void testContainsEngineName_doesContain() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testEngine", "testName", user);
		assertTrue(AbstractSecurityUtils.containsEngineName("testName"));
	}

	@Test
	void testContainsEngineName_notThere() {
		assertFalse(AbstractSecurityUtils.containsEngineName("testName"));
	}

	@Test
	void testContainsProjectName_present() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProject", "testName", user);
		assertTrue(AbstractSecurityUtils.containsProjectName("testName"));
	}

	@Test
	void testContainsProjectName_notPresent() {
		assertFalse(AbstractSecurityUtils.containsProjectName("testName"));
	}

	@Test
	void testContainsEngineId_Security() {
		assertTrue(AbstractSecurityUtils.containsEngineId("security"));
	}

	@Test
	void testContainsEngineId_present() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testEngine", "testName", user);
		assertTrue(AbstractSecurityUtils.containsEngineId("testEngine"));
	}

	@Test
	void testContainsEngineId_missing() {
		assertFalse(AbstractSecurityUtils.containsEngineId("testEngine"));
	}

	@Test
	void testContainsProjectId_Security() {
		assertTrue(AbstractSecurityUtils.containsProjectId("security"));
	}

	@Test
	void testContainsProjectId_present() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProject", "testName", user);
		assertTrue(AbstractSecurityUtils.containsProjectId("testProject"));
	}

	@Test
	void testContainsProjectId_missing() {
		assertFalse(AbstractSecurityUtils.containsProjectId("testProject"));
	}

	@Test
	void testIgnoreDatabase_security() {
		assertTrue(AbstractSecurityUtils.ignoreDatabase("security"));
	}

	@Test
	void testIgnoreDatabase_isAssetProject() throws SQLException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProject", "testName", user);

		UserAssetUtils.registerUserAssetProject(user, AuthProvider.NATIVE, "testProject");

		assertTrue(AbstractSecurityUtils.ignoreDatabase("testProject"));
	}

	@Test
	void testIgnoreDatabase_enginePropertiesDatabaseApp() throws IOException {
		Properties properties = new Properties();
		properties.setProperty(Constants.IS_ASSET_APP, "true");
		UnitTestSecurityAuthUtils.createSmssFileFromProps(properties, databaseFolder, "test.smss");
		String smssFile = databaseFolder + File.separator + "test.smss";
		DIHelper.getInstance().setEngineProperty("test_" + Constants.STORE, smssFile);

		assertTrue(AbstractSecurityUtils.ignoreDatabase("test"));

		// teardown this test
		DIHelper.getInstance().removeEngineProperty("test_" + Constants.STORE);
	}

	@Test
	void testIgnoreDatabase_false() {
		assertFalse(AbstractSecurityUtils.ignoreDatabase("noDatabase"));
	}

	@Test
	void testGetStockImage_layoutNull() {
		UnitTestSecurityAuthUtils.setupImageDir(semossDir);

		File f = AbstractSecurityUtils.getStockImage("blah", "blah");

		assertTrue(f.exists());
		assertTrue(f.isFile());
		assertEquals("color-logo.png", f.getName());
	}

	@Test
	void testGetStockImage_layoutTreemap() {
		UnitTestSecurityAuthUtils.setupImageDir(semossDir, "treemap.png");

		User adminUser = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProject", "testName", adminUser);
		UnitTestSecurityAuthUtils.createInsight("testProject", "insightId", "insightName", "treemap");

		File f = AbstractSecurityUtils.getStockImage("testProject", "insightId");
		assertTrue(f.exists());
		assertTrue(f.isFile());
		assertEquals("treemap.png", f.getName());
	}

	@Test
	void testGetStockImage_unknownLayout() {
		UnitTestSecurityAuthUtils.setupImageDir(semossDir, "treemap.png");

		User adminUser = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProject", "testName", adminUser);
		UnitTestSecurityAuthUtils.createInsight("testProject", "insightId", "insightName", "foobar");

		File f = AbstractSecurityUtils.getStockImage("testProject", "insightId");
		assertTrue(f.exists());
		assertTrue(f.isFile());
		assertEquals("color-logo.png", f.getName());
	}

	@Test
	void testCreateFilter_singleFilter() {
		assertEquals(" IN ('testOne')", AbstractSecurityUtils.createFilter("testOne"));
	}

	@Test
	void testCreateFilter_multipleFilters() {
		assertEquals(" IN ('testOne', 'testTwo')", AbstractSecurityUtils.createFilter("testOne", "testTwo"));
	}

	@Test
	void testCreateFilter_Empty() {
		List<String> values = new ArrayList<>();
		String s = AbstractSecurityUtils.createFilter(values);
		assertEquals(" IN () ", s);
	}

	@Test
	void testCreateFilter_EscapeSQLStatement() {
		List<String> values = new ArrayList<>();
		values.add("'DELETE * FROM SMSS_USER'");
		values.add("test");
		String s = AbstractSecurityUtils.createFilter(values);
		assertEquals(" IN ('''DELETE * FROM SMSS_USER''', 'test')", s);
	}

	@Test
	void testGetUserFilters_userNull() {
		assertEquals("()", AbstractSecurityUtils.getUserFilters(null));
	}

	@Test
	void testGetUserFilters_singleLogin() {
		User user = new User();
		AccessToken at = UnitTestSecurityAuthUtils.createAccessToken("one");
		user.setAccessToken(at);
		assertEquals("('oneid')", AbstractSecurityUtils.getUserFilters(user));
	}

	@Test
	void testGetUserFilters_multipleLogin() {
		User user = new User();
		AccessToken at = UnitTestSecurityAuthUtils.createAccessToken("one");
		user.setAccessToken(at);

		AccessToken at2 = new AccessToken();
		at2.setProvider(AuthProvider.DROPBOX);
		at2.setId("dropbox");
		user.setAccessToken(at2);
		assertEquals("('oneid', 'dropbox')", AbstractSecurityUtils.getUserFilters(user));
	}

	@Test
	void testGetUserFiltersQs_multiple() {
		User user = new User();
		AccessToken at = UnitTestSecurityAuthUtils.createAccessToken("one");
		user.setAccessToken(at);

		AccessToken at2 = new AccessToken();
		at2.setProvider(AuthProvider.DROPBOX);
		at2.setId("dropbox");
		user.setAccessToken(at2);
		Collection<String> values = AbstractSecurityUtils.getUserFiltersQs(user);
		assertEquals(2, values.size());
		assertTrue(values.contains("oneid"));
		assertTrue(values.contains("dropbox"));
	}

	@Test
	void testGetSimpleQuery_smssUsers() {
		UnitTestSecurityAuthUtils.createUser("admin", true);
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__TYPE"));

		List<Map<String, Object>> map = AbstractSecurityUtils.getSimpleQuery(qs);

		assertEquals(1, map.size());
		Map<String, Object> user = map.getFirst();
		assertEquals("NATIVE", user.get("type"));
	}

	@Test
	void testValidEmail_emailNull() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> AbstractSecurityUtils.validEmail(null, true));
		assertEquals("null is not a valid email address. ", e.getMessage());
	}

	@ParameterizedTest
	@ValueSource(strings = { "", "blah", "hey@blah" })
	void testValidEmail_emailMalformed(String test) {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> AbstractSecurityUtils.validEmail(test, true));
		assertEquals(test + " is not a valid email address. ", e.getMessage());
	}

	@Test
	void testValidEmail_userExists() {
		User user = UnitTestSecurityAuthUtils.createUser("one", true);
		String email = user.getPrimaryLoginToken().getEmail();
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> AbstractSecurityUtils.validEmail(email, true));
		assertEquals("This email already exists. Please login. ", e.getMessage());
	}

	@Test
	void testValidEmail_validEmail() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("one", true);

		AbstractSecurityUtils.validEmail("email@test.com", true);
	}

	@ParameterizedTest
	@NullAndEmptySource
	void testValidPassword_nullAndEmpty(String password) {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> AbstractSecurityUtils.validPassword("oneId", AuthProvider.NATIVE, password));
		assertEquals("Password cannot be empty. ", e.getMessage());
	}

	@Test
	void testValidPassord_reusingOldPassword() {
		AccessToken at = UnitTestSecurityAuthUtils.createAccessToken("one");
		SecurityNativeUserUtils.addNativeUser(at, "Test123!");

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> AbstractSecurityUtils.validPassword("oneid", AuthProvider.NATIVE, "Test123!"));

		assertEquals("Cannot reuse old password. ", e.getMessage());
	}

	@Test
	void testValidPassord_valid() throws Exception {
		AccessToken at = UnitTestSecurityAuthUtils.createAccessToken("one");
		SecurityNativeUserUtils.addNativeUser(at, "Test123!");
		AbstractSecurityUtils.validPassword("oneid", AuthProvider.NATIVE, "Foobar123!");
	}

	@Test
	void testFormatPhone_null() throws Exception {
		String phone = AbstractSecurityUtils.formatPhone(null);
		assertNull(phone);
	}

	@Test
	void testFormatPhone_empty() throws Exception {
		String phone = AbstractSecurityUtils.formatPhone("");
		assertEquals("", phone);
	}

	@ParameterizedTest
	@ValueSource(strings = { "hello", "333hj3h3333", "1092!93323" })
	void testFormatPhone_invalidCharacters(String source) {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> AbstractSecurityUtils.formatPhone(source));
		assertEquals("Phone number " + source + " contains invalid characters. ", e.getMessage());
	}

	@ParameterizedTest
	@ValueSource(strings = { "7777777", "1112223333444" })
	void testFormatPhone_wrongLengths(String source) {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> AbstractSecurityUtils.formatPhone(source));

		assertEquals(source + " is not a valid phone number. ", e.getMessage());
	}

	@ParameterizedTest
	@NullAndEmptySource
	void testValidUsername_nullandempty(String source) {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> AbstractSecurityUtils.validUsername(source));
		assertEquals("Username cannot be empty. ", e.getMessage());
	}

	@Test
	void testValidUsername_userExists() {
		User user = UnitTestSecurityAuthUtils.createUser("one", true);
		String username = user.getPrimaryLoginToken().getUsername();
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> AbstractSecurityUtils.validUsername(username));
		assertEquals("Username already exists. ", e.getMessage());
	}

	@Test
	void testValidUsername_valid() {
		// no error thrown good
		AbstractSecurityUtils.validUsername("working");
	}

	@Test
	void testGenerateSalt_success() {
		String salt = AbstractSecurityUtils.generateSalt();
		assertNotNull(salt);
		assertFalse(salt.isEmpty());
	}

	@Test
	void testHash_successful() {
		String salt = AbstractSecurityUtils.generateSalt();
		String hashed = AbstractSecurityUtils.hash("password", salt);
		assertNotNull(hashed);
		assertFalse(hashed.isEmpty());
	}

	@Test
	void testCalculateEndDate() {
		ZonedDateTime now = ZonedDateTime.now();
		String nowString = now.toString();
		java.sql.Timestamp ts = AbstractSecurityUtils.calculateEndDate(nowString);
		// probably figure out a better way to test the timestamp is in utc.
		assertNotNull(ts);
	}

	@Test
	void testEndDateIsExpired_null() throws Exception {
		assertFalse(AbstractSecurityUtils.endDateIsExpired(null));
	}

	@Test
	void testEndDateIsExpired_true() throws Exception {
		SemossDate semossDate = new SemossDate(LocalDateTime.now().minusDays(2), ZoneId.of("UTC"));
		assertTrue(AbstractSecurityUtils.endDateIsExpired(semossDate));
	}

	@Test
	void testEndDateIsExpired_false() throws Exception {
		SemossDate semossDate = new SemossDate(LocalDateTime.now().plusDays(2), ZoneId.of("UTC"));
		assertFalse(AbstractSecurityUtils.endDateIsExpired(semossDate));
	}

}
