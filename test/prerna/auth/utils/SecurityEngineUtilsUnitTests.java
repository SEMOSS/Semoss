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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.access.ModelUsageLimit;
import prerna.auth.access.ModelUsageLimitKey;
import prerna.auth.access.ModelUsageLimitRule;
import prerna.auth.access.ModelUsageLimitRule.Counts;
import prerna.auth.access.ModelUsageLimitRule.Metric;
import prerna.auth.access.ModelUsageLimitRule.Window;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.notifications.NotificationDbUtils;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.EmailUtility;
import prerna.util.NotificationConstants;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

public class SecurityEngineUtilsUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

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

	///
	/// addEngine
	///

	@Test
	void testAddEngine_ignoreDatabase() {
		SecurityEngineUtils.addEngine("security", false, null);
	}

	@Test
	void testAddEngine_newEngineThenUpdate() {
		// create user
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);

		// create smss file for engine
		Properties props = UnitTestSecurityAuthUtils.getDefaultDBProperties("testdb");
		Path smssFile = UnitTestSecurityAuthUtils.createSmssFileFromProps(props, dbDir, "testdb.smss");
		UnitTestSecurityAuthUtils.addEngineStoreProp("testdb", smssFile);

		// assert engine does not exist
		assertFalse(SecurityEngineUtils.engineExists("testdb"));

		// add engine
		SecurityEngineUtils.addEngine("testdb", false, user);

		// verify engine exists
		assertTrue(SecurityEngineUtils.engineExists("testdb"));

		// make sure user is owner to that way we can query for it
		SecurityEngineUtils.addEngineOwner("testdb", "adminid");

		// assert engine exists now
		List<Map<String, Object>> engineList = SecurityEngineUtils.getUserEngineList(user, null, null);
		assertEquals(1, engineList.size());
		assertEquals("testdb", engineList.getFirst().get("database_id"));
		assertEquals("testdb", engineList.getFirst().get("database_name"));
		assertEquals("DATABASE", engineList.getFirst().get("database_type"));
		assertEquals("testdb", engineList.getFirst().get("engine_id"));
		assertEquals("testdb", engineList.getFirst().get("engine_name"));
		assertEquals("DATABASE", engineList.getFirst().get("engine_type"));

		// change engine type to OpenAiEngine
		Properties updateProps = UnitTestSecurityAuthUtils.getDefaultOpenAiProperties("testdb");
		UnitTestSecurityAuthUtils.createSmssFileFromProps(updateProps, dbDir, "testdb.smss");

		// add engine again
		SecurityEngineUtils.addEngine("testdb", false, user);

		// assert updates happened
		List<Map<String, Object>> updateEngineList = SecurityEngineUtils.getUserEngineList(user, null, null);
		assertEquals(1, updateEngineList.size());
		assertEquals("testdb", updateEngineList.getFirst().get("database_id"));
		assertEquals("testdb", updateEngineList.getFirst().get("database_name"));
		assertEquals("MODEL", updateEngineList.getFirst().get("database_type"));
		assertEquals("testdb", updateEngineList.getFirst().get("engine_id"));
		assertEquals("testdb", updateEngineList.getFirst().get("engine_name"));
		assertEquals("MODEL", updateEngineList.getFirst().get("engine_type"));

		// remove engine id from DIHelper
		UnitTestSecurityAuthUtils.removeEngineStoreProp("testdb");
	}

	///
	/// getEngineTypeAndSubTypeAndCost
	///

	@Test
	void testGetEngineTypeAndSubTypeAndCost_Successful() {
		Properties props = UnitTestSecurityAuthUtils.getDefaultDBProperties("testdb");
		Object[] result = SecurityEngineUtils.getEngineTypeAndSubTypeAndCost(props);
		assertEquals("DATABASE", result[0].toString());
		assertEquals("H2_DB", result[1].toString());
		assertEquals("$", result[2].toString());
	}

	@Test
	void testGetEngineTypeAndSubTypeAndCost_classNameDoesNotExist() {
		Properties props = new Properties();
		props.setProperty(Constants.ENGINE_TYPE, "no class named this");
		Object[] result = SecurityEngineUtils.getEngineTypeAndSubTypeAndCost(props);
		assertNull(result[0]);
		assertNull(result[1]);
		assertEquals("$", result[2].toString());
	}

	///
	/// getEngineType
	///

	@Test
	void testGetEngineType_engineDoesNotExist() {
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
				() -> SecurityEngineUtils.getEngineType("bad"));
		assertEquals("Could not find engine with id bad", ex.getMessage());
	}

	@Test
	void testGetEngineType_engineExists() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testdb", "testname", user);
		assertEquals(IEngine.CATALOG_TYPE.DATABASE, SecurityEngineUtils.getEngineType("testdb"));
	}

	///
	/// getEngineTypeAndSubtype
	///

	@Test
	void testGetEngineTypeAndSubType_notPresent() {
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
				() -> SecurityEngineUtils.getEngineTypeAndSubtype("bad"));
		assertEquals("Could not find engine with id bad", ex.getMessage());
	}

	@Test
	void testGetEngineTypeAndSubType() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngineWithSubtype("testdb", "testname", user, "foobar");
		Object[] vals = SecurityEngineUtils.getEngineTypeAndSubtype("testdb");
		assertEquals(IEngine.CATALOG_TYPE.DATABASE, vals[0]);
		assertEquals("foobar", vals[1]);
	}

	///
	/// addEngine
	///

	@Test
	void testAddEngine() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		SecurityEngineUtils.addEngine("engineId", "engineName", IEngine.CATALOG_TYPE.DATABASE, "subtype", "cost", true,
				user);
		assertTrue(SecurityEngineUtils.engineExists("engineId"));
		Object[] os = SecurityEngineUtils.getEngineTypeAndSubtype("engineId");
		assertEquals(IEngine.CATALOG_TYPE.DATABASE, os[0]);
		assertEquals("subtype", os[1]);
	}

	@Test
	void testAddEngine_allNull() {
		SecurityEngineUtils.addEngine("engineId", null, IEngine.CATALOG_TYPE.DATABASE, null, null, false, null);
		assertTrue(SecurityEngineUtils.engineExists("engineId"));
		Object[] os = SecurityEngineUtils.getEngineTypeAndSubtype("engineId");
		assertEquals(IEngine.CATALOG_TYPE.DATABASE, os[0]);
		assertNull(os[1]);
	}

	///
	/// updateEngineTypeAndSubType
	///

	@Test
	void testUpdateEngineTypeAndSubType() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		SecurityEngineUtils.addEngine("engineId", "engineName", IEngine.CATALOG_TYPE.DATABASE, "subtype", "cost", true,
				user);
		SecurityEngineUtils.updateEngineTypeAndSubType("engineId", IEngine.CATALOG_TYPE.FUNCTION, "subFunctionType");
		Object[] os = SecurityEngineUtils.getEngineTypeAndSubtype("engineId");
		assertEquals(IEngine.CATALOG_TYPE.FUNCTION, os[0]);
		assertEquals("subFunctionType", os[1]);
	}

	///
	/// addEngineOwner
	///

	@Test
	void testAddEngineOwner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		SecurityEngineUtils.addEngine("engineId", "engineName", IEngine.CATALOG_TYPE.DATABASE, "subtype", "cost", true,
				user);
		String userId = user.getPrimaryLoginToken().getId();

		SecurityEngineUtils.addEngineOwner("engineId", userId);

		List<String> owners = SecurityEngineUtils.getEngineOwners("engineId");
		assertEquals(1, owners.size());
		assertEquals("admin@test.com", owners.getFirst());
	}

	///
	/// getENgineAliasForId
	///

	@Test
	void testGetEngineAliasForId_none() {
		assertNull(SecurityEngineUtils.getEngineAliasForId(""));
	}

	@Test
	void testGetEngineALiasForId() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);

		String alias = SecurityEngineUtils.getEngineAliasForId("testId");
		assertEquals("testAlias", alias);
	}

	///
	/// getActualUserEnginePermission
	///

	@Test
	void testGetActualUserEnginePermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);

		assertEquals("OWNER", SecurityEngineUtils.getActualUserEnginePermission(user, "testId"));
	}

	///
	/// getAllEngineIds
	///

	@Test
	void testGetAllEngineIds() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);
		UnitTestSecurityAuthUtils.createEngine("testId2", "testAlias", user);
		UnitTestSecurityAuthUtils.createEngine("testId3", "testAlias", user);

		List<String> engineIds = SecurityEngineUtils.getAllEngineIds();

		assertEquals(3, engineIds.size());
		assertTrue(engineIds.contains("testId"));
		assertTrue(engineIds.contains("testId2"));
		assertTrue(engineIds.contains("testId3"));
	}

	///
	/// getAllEngineIds(List<String> engineTypes)
	///

	@Test
	void testGetAllEngineIds_filter() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("id1", "name1", IEngine.CATALOG_TYPE.DATABASE, user);
		UnitTestSecurityAuthUtils.createEngine("id2", "name2", IEngine.CATALOG_TYPE.FUNCTION, user);
		UnitTestSecurityAuthUtils.createEngine("id3", "name3", IEngine.CATALOG_TYPE.GUARDRAIL, user);
		UnitTestSecurityAuthUtils.createEngine("id4", "name4", IEngine.CATALOG_TYPE.VENV, user);

		List<String> filter = new ArrayList<>();
		filter.add(IEngine.CATALOG_TYPE.DATABASE.toString());
		filter.add(IEngine.CATALOG_TYPE.VENV.toString());
		List<String> engineIds = SecurityEngineUtils.getAllEngineIds(filter);

		assertEquals(2, engineIds.size());
		assertTrue(engineIds.contains("id1"));
		assertTrue(engineIds.contains("id4"));
	}

	///
	/// testGetEngineMarkdown
	///

	@Test
	void testGetEngineMarkdown_noMarkdown() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("id1", "name1", IEngine.CATALOG_TYPE.DATABASE, user);

		String val = SecurityEngineUtils.getEngineMarkdown(user, "id1");

		assertNull(val);
	}

	@Test
	void testSetDefaultEngineMarkdownOnlyFillsEmptyMetadata() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("id1", "name1", IEngine.CATALOG_TYPE.GUARDRAIL, user);

		SecurityEngineUtils.setDefaultEngineMarkdown("id1", "First default");
		assertEquals("First default", SecurityEngineUtils
				.getAggregateEngineMetadata("id1", List.of(Constants.MARKDOWN), false).get(Constants.MARKDOWN));

		SecurityEngineUtils.setDefaultEngineMarkdown("id1", "Second default");
		assertEquals("First default", SecurityEngineUtils
				.getAggregateEngineMetadata("id1", List.of(Constants.MARKDOWN), false).get(Constants.MARKDOWN));

		SecurityEngineUtils.updateEngineMetadata("id1", Map.of(Constants.MARKDOWN, ""));
		SecurityEngineUtils.setDefaultEngineMarkdown("id1", "Replacement default");
		assertEquals("Replacement default", SecurityEngineUtils
				.getAggregateEngineMetadata("id1", List.of(Constants.MARKDOWN), false).get(Constants.MARKDOWN));
	}

	///
	/// getUserEnginePermission
	///

	@Test
	void testGetUserEnginePermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);
		String id = user.getPrimaryLoginToken().getId();
		assertEquals(1, SecurityEngineUtils.getUserEnginePermission(id, "testId"));
	}

	///
	/// getUserAccessRequestEnginePermission
	///

	@Test
	void testGetUserAccessRequestEnginePermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);

		assertNull(SecurityEngineUtils.getUserAccessRequestEnginePermission("adminid", "testId"));
	}

	///
	/// approveEngineUserAccessRequests
	///

	@Test
	void testGetApproveEngineUserAccessRequests_userNotEditor() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User secondUser = UnitTestSecurityAuthUtils.createUser("second", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);
		List<Map<String, String>> requests = new ArrayList<>();

		IllegalAccessException e = assertThrows(IllegalAccessException.class,
				() -> SecurityEngineUtils.approveEngineUserAccessRequests(secondUser, "testId", requests, null));
		assertEquals("Insufficient privileges to modify this engine's permissions.", e.getMessage());
	}

	@Test
	void testGetApproveEngineUserAccessRequests_userEditor() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);

		// change user to editor
		List<Map<String, Object>> updates = new ArrayList<>();
		Map<String, Object> updatesMap = new HashMap<>();
		updatesMap.put("engineId", "testId");
		updatesMap.put("engineName", "testAlias");
		updatesMap.put("engineType", IEngine.CATALOG_TYPE.DATABASE);
		updatesMap.put("permission", "EDIT");
		updates.add(updatesMap);
		SecurityEngineUtils.updateEngineUserPermissions(user, updates);

		UnitTestSecurityAuthUtils.createUser("second", false);
		List<Map<String, String>> requests = new ArrayList<>();
		Map<String, String> request = new HashMap<>();
		request.put("userid", "secondid");
		request.put("permission", "OWNER");
		requests.add(request);

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> SecurityEngineUtils.approveEngineUserAccessRequests(user, "testId", requests, null));
		assertEquals("As a non-owner, you cannot grant owner access.", e.getMessage());
	}

	@Test
	void testApproveEngineUserAccessRequests_approveOwnerThenApproveEdit() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User secondUser = UnitTestSecurityAuthUtils.createUser("second", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);
		SecurityEngineUtils.setUserAccessRequest("secondid", "NATIVE", "testId", "reason", 1, secondUser);

		List<Map<String, String>> requests = new ArrayList<>();
		Map<String, String> request = new HashMap<>();
		request.put("userid", "secondid");
		request.put("permission", "OWNER");
		requests.add(request);

		assertFalse(SecurityEngineUtils.userIsOwner(secondUser, "testId"));
		assertEquals(1, SecurityEngineUtils.getUserPendingAccessRequest(secondUser, "testId"));

		List<Map<String, Object>> pending = SecurityEngineUtils.getUserAccessRequestsByEngine("testId");
		assertEquals(1, pending.size());
		request.put("requestid", pending.getFirst().get("ID").toString());

		SecurityEngineUtils.approveEngineUserAccessRequests(user, "testId", requests, null);

		assertTrue(SecurityEngineUtils.userIsOwner(secondUser, "testId"));
		assertEquals(-1, SecurityEngineUtils.getUserPendingAccessRequest(secondUser, "testId"));

		// request editor and approve
		request.put("permission", "EDIT");

		SecurityEngineUtils.setUserAccessRequest("secondid", "NATIVE", "testId", "reason", 2, secondUser);
		pending = SecurityEngineUtils.getUserAccessRequestsByEngine("testId");
		assertEquals(1, pending.size());
		request.put("requestid", pending.getFirst().get("ID").toString());
		SecurityEngineUtils.approveEngineUserAccessRequests(user, "testId", requests, null);

		assertFalse(SecurityEngineUtils.userIsOwner(secondUser, "testId"));
		assertTrue(SecurityEngineUtils.userCanEditEngine(secondUser, "testId"));
		assertEquals(-1, SecurityEngineUtils.getUserPendingAccessRequest(secondUser, "testId"));
	}

	///
	/// denyEngineUserAccessRequests
	///

	@Test
	void testDenyEngineUserAccessRequests_notEditor() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User secondUser = UnitTestSecurityAuthUtils.createUser("second", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);
		List<String> requestIds = new ArrayList<>();

		IllegalAccessException e = assertThrows(IllegalAccessException.class,
				() -> SecurityEngineUtils.denyEngineUserAccessRequests(secondUser, "testId", requestIds));
		assertEquals("Insufficient privileges to modify this engine's permissions.", e.getMessage());
	}

	@Test
	void testGetDenyEngineUserAccessRequests_userEditor() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);

		// change user to editor
		List<Map<String, Object>> updates = new ArrayList<>();
		Map<String, Object> updatesMap = new HashMap<>();
		updatesMap.put("engineId", "testId");
		updatesMap.put("engineName", "testAlias");
		updatesMap.put("engineType", IEngine.CATALOG_TYPE.DATABASE);
		updatesMap.put("permission", "EDIT");
		updates.add(updatesMap);
		SecurityEngineUtils.updateEngineUserPermissions(user, updates);

		List<String> requestIds = new ArrayList<>();

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> SecurityEngineUtils.denyEngineUserAccessRequests(user, "testId", requestIds));
		assertEquals("Insufficient privileges to deny user access requests.", e.getMessage());
	}

	@Test
	void testDenyEngineUserAccessRequests() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User secondUser = UnitTestSecurityAuthUtils.createUser("second", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);
		SecurityEngineUtils.setUserAccessRequest("secondid", "NATIVE", "testId", "reason", 1, secondUser);

		List<String> requestIds = new ArrayList<>();

		assertFalse(SecurityEngineUtils.userIsOwner(secondUser, "testId"));
		assertEquals(1, SecurityEngineUtils.getUserPendingAccessRequest(secondUser, "testId"));

		List<Map<String, Object>> pending = SecurityEngineUtils.getUserAccessRequestsByEngine("testId");
		assertEquals(1, pending.size());
		requestIds.add(pending.getFirst().get("ID").toString());

		SecurityEngineUtils.denyEngineUserAccessRequests(user, "testId", requestIds);

		// assure original user still has access
		assertTrue(SecurityEngineUtils.userIsOwner(user, "testId"));

		// user requesting has no access
		assertFalse(SecurityEngineUtils.userIsOwner(secondUser, "testId"));
		assertFalse(SecurityEngineUtils.userCanEditEngine(secondUser, "testId"));
		assertFalse(SecurityEngineUtils.userCanViewEngine(secondUser, "testId"));

		// there are no pending requests
		assertEquals(-1, SecurityEngineUtils.getUserPendingAccessRequest(secondUser, "testId"));
	}

	///
	/// getUserAccessRequestsByEngine
	///

	@Test
	void testGetUserAccessRequestByEngine() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User secondUser = UnitTestSecurityAuthUtils.createUser("second", true);
		User thirdUser = UnitTestSecurityAuthUtils.createUser("third", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);

		SecurityEngineUtils.setUserAccessRequest("secondid", "NATIVE", "testId", "reason", 1, secondUser);
		SecurityEngineUtils.setUserAccessRequest("thirdid", "NATIVE", "testId", "reason", 2, thirdUser);

		List<Map<String, Object>> requests = SecurityEngineUtils.getUserAccessRequestsByEngine("testId");
		assertEquals(2, requests.size());

		Map<String, Object> request = requests.getFirst();

		assertNotNull(request.get("ID"));

		String id = (String) request.get("REQUEST_USERID");
		if (id.equals("thirdid")) {
			request = requests.getLast();
		}

		assertEquals("secondid", request.get("REQUEST_USERID"));
		assertEquals("NATIVE", request.get("REQUEST_TYPE"));
		assertEquals("secondname", request.get("NAME"));
		assertEquals("second@test.com", request.get("EMAIL"));
		assertEquals("secondid", request.get("USERNAME"));
		assertNotNull(request.get("REQUEST_TIMESTAMP"));
		assertEquals("testId", request.get("ENGINEID"));
		assertNull(request.get("APPROVER_USERID"));
		assertNull(request.get("APPROVER_TYPE"));
		assertNull(request.get("APPROVER_DECISISON"));
		assertNull(request.get("APPROVER_TIMESTAMP"));
	}

	///
	/// engineExists
	///

	@Test
	void testEngineExists_doesNot() {
		assertFalse(SecurityEngineUtils.engineExists("foobar"));
	}

	///
	/// engineIsGlobal
	///

	@Test
	void testEngineIsGlobal() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);
		UnitTestSecurityAuthUtils.createEngineGlobal("global", "globalalias", user);
		assertFalse(SecurityEngineUtils.engineIsGlobal("testId"));
		assertTrue(SecurityEngineUtils.engineIsGlobal("global"));
	}

	///
	/// getDisplayDatabaseOwnersAndEditors
	///

	@Test
	void testGetDisplayDatabaseOwnersAndEditors_engineNotGlobal() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createUser("admin2", true);
		UnitTestSecurityAuthUtils.createUser("admin3", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, "admin2id", "testId", "EDIT");
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, "admin3id", "testId", "READ_ONLY");

		List<Map<String, Object>> ownersAndEditors = SecurityEngineUtils.getDisplayDatabaseOwnersAndEditors("testId");

		// UnitTestSecurityAuthUtils.dumpTable("ENGINEPERMISSION", securityDb);

		assertEquals(3, ownersAndEditors.size());

		List<String> users = ownersAndEditors.stream().map(s -> s.get("id").toString()).toList();
		assertEquals(3, users.size());
		assertTrue(users.contains("adminid"));
		assertTrue(users.contains("admin2id"));
		assertTrue(users.contains("admin3id"));
	}

	@Test
	void testGetDisplayDatabaseOwnersAndEditors_engineGlobal() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createUser("admin2", true);
		UnitTestSecurityAuthUtils.createUser("admin3", true);
		UnitTestSecurityAuthUtils.createEngineGlobal("testId", "testAlias", user);
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, "admin2id", "testId", "EDIT");
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, "admin3id", "testId", "READ_ONLY");

		List<Map<String, Object>> ownersAndEditors = SecurityEngineUtils.getDisplayDatabaseOwnersAndEditors("testId");

		// UnitTestSecurityAuthUtils.dumpTable("ENGINEPERMISSION", securityDb);

		assertEquals(3, ownersAndEditors.size());

		Map<String, Object> admin = ownersAndEditors.stream().filter(s -> s.get("name").equals("adminname")).findFirst()
				.get();
		assertEquals("OWNER", admin.get("permission"));

		Map<String, Object> admin2 = ownersAndEditors.stream().filter(s -> s.get("name").equals("admin2name"))
				.findFirst().get();
		assertEquals("EDIT", admin2.get("permission"));

		Map<String, Object> publicDatabase = ownersAndEditors.stream()
				.filter(s -> s.get("name").equals("PUBLIC DATABASE")).findFirst().get();
		assertEquals("READ_ONLY", publicDatabase.get("permission").toString());
	}

	///
	/// getEngineUsers
	///

	@Test
	void testGetEngineUsers_noAccess() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("admin2", true);

		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityEngineUtils.getEngineUsers(user2, "testId", null, null, 1, 1));

		assertEquals("The user does not have access to view this engine", ex.getMessage());
	}

	@Test
	void testGetEngineUsers() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);
		User user2 = UnitTestSecurityAuthUtils.createUser("admin2", true);

		List<Map<String, Object>> users = SecurityEngineUtils.getEngineUsers(user, "testId", null, null, 0, 0);

		assertEquals(1, users.size());
		assertEquals("adminid", users.getFirst().get("id").toString());
	}

	///
	/// getEngineUsersCount
	///
	@Test
	void testGetEngineUsersCount_noAccess() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("admin2", true);

		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityEngineUtils.getEngineUsersCount(user2, "testId", null, null));

		assertEquals("The user does not have access to view this engine", ex.getMessage());
	}

	@Test
	void testGetEngineUsersCount() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);
		User user2 = UnitTestSecurityAuthUtils.createUser("admin2", true);

		long users = SecurityEngineUtils.getEngineUsersCount(user, "testId", null, null);
		assertEquals(1, users);

		// filter out on permission
		assertEquals(0, SecurityEngineUtils.getEngineUsersCount(user, "testId", null, "EDIT"));

		// filter out on name
		assertEquals(0, SecurityEngineUtils.getEngineUsersCount(user, "testId", "blake", null));
	}

	///
	/// addEngineUser
	///

	@Test
	void testAddEngineUser() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("admin2", true);
		User user3 = UnitTestSecurityAuthUtils.createUser("user3", true);

		// user not editor
		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityEngineUtils.addEngineUser(user2, "user3id", "testId", "OWNER", null, null));

		assertEquals("Insufficient privileges to modify this engine's permissions.", ex.getMessage());

		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, "admin2id", "testId", "EDIT");

		// user already has permissions
		IllegalArgumentException argEx = assertThrows(IllegalArgumentException.class,
				() -> SecurityEngineUtils.addEngineUser(user, "admin2id", "testId", "OWNER", null, null));
		assertEquals("This user already has access to this engine. Please edit the existing permission level.",
				argEx.getMessage());

		// user not owner and tries to give ownership
		ex = assertThrows(IllegalAccessException.class,
				() -> SecurityEngineUtils.addEngineUser(user2, "user3id", "testId", "OWNER", null, null));
		assertEquals("Cannot give owner level access to this engine since you are not currently an owner.",
				ex.getMessage());

		// user successfully gives edit as edit user
		SecurityEngineUtils.addEngineUser(user2, "user3id", "testId", "EDIT", null, null);

		assertTrue(SecurityEngineUtils.userCanEditEngine(user3, "testId"));

		// other permissions still good
		assertTrue(SecurityEngineUtils.userIsOwner(user, "testId"));
		assertTrue(SecurityEngineUtils.userCanEditEngine(user2, "testId"));
	}

	///
	/// addEngineUserPermissions
	///

	@Test
	void testAddEngineUsersPermissions_errors() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("admin2", true);
		User user3 = UnitTestSecurityAuthUtils.createUser("user3", true);

		List<Map<String, Object>> permissions = List.of(Map.of("userid", "user3id", "permission", "OWNER"));

		// user not editor
		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityEngineUtils.addEngineUserPermissions(user2, "testId", permissions));
		assertEquals("Insufficient privileges to modify this engine's permissions.", ex.getMessage());

		// make user2 editor
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, "admin2id", "testId", "EDIT");

		// user already has permissions
		List<Map<String, Object>> permission2 = List.of(Map.of("userid", "admin2id", "permission", "OWNER"));
		IllegalArgumentException argEx = assertThrows(IllegalArgumentException.class,
				() -> SecurityEngineUtils.addEngineUserPermissions(user, "testId", permission2));
		assertEquals(
				"The following users already have access to this engine. Please edit the existing permission level: admin2id",
				argEx.getMessage());

		// user not owner and tries to give ownership
		List<Map<String, Object>> permission3 = List.of(Map.of("userid", "user3id", "permission", "OWNER"));
		argEx = assertThrows(IllegalArgumentException.class,
				() -> SecurityEngineUtils.addEngineUserPermissions(user2, "testId", permission3));
		assertEquals("As a non-owner, you cannot add owner user access.", argEx.getMessage());
	}

	///
	/// editEngineUser
	///

	@Test
	void testEditEngineUser() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("admin2", true);
		User user3 = UnitTestSecurityAuthUtils.createUser("user3", true);

		// user not editor
		IllegalAccessException ex = assertThrows(IllegalAccessException.class, () -> SecurityEngineUtils
				.editEngineUserPermission(user2, "user3id", "NATIVE", "testId", "OWNER", null));

		assertEquals("Insufficient privileges to modify this engine's permissions.", ex.getMessage());

		// user does not have permissions
		IllegalArgumentException argEx = assertThrows(IllegalArgumentException.class, () -> SecurityEngineUtils
				.editEngineUserPermission(user, "admin2id", "NATIVE", "testId", "OWNER", null));
		assertEquals(
				"Attempting to modify engine permission for a user who does not currently have access to the engine",
				argEx.getMessage());

		// give user2 and user3, edit and read only permissions, respectively
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, "admin2id", "testId", "EDIT");
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, "user3id", "testId", "READ_ONLY");

		// edit user tries to edit owner user
		ex = assertThrows(IllegalAccessException.class,
				() -> SecurityEngineUtils.editEngineUserPermission(user2, "adminid", "NATIVE", "testId", "EDIT", null));
		assertEquals("The user doesn't have the high enough permissions to modify this users engine permission.",
				ex.getMessage());

		// user not owner and tries to give ownership
		ex = assertThrows(IllegalAccessException.class, () -> SecurityEngineUtils.editEngineUserPermission(user2,
				"user3id", "NATIVE", "testId", "OWNER", null));
		assertEquals("Cannot give owner level access to this engine since you are not currently an owner.",
				ex.getMessage());

		// user2 successfully changes user3 to edit as edit user
		SecurityEngineUtils.editEngineUserPermission(user2, "user3id", "NATIVE", "testId", "EDIT", null);

		assertTrue(SecurityEngineUtils.userCanEditEngine(user3, "testId"));

		// other permissions still good
		assertTrue(SecurityEngineUtils.userIsOwner(user, "testId"));
		assertTrue(SecurityEngineUtils.userCanEditEngine(user2, "testId"));
	}

	///
	/// editEngineUserPermissions
	///

	@Test
	void testEditEngineUsersPermissions() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("admin2", true);
		User user3 = UnitTestSecurityAuthUtils.createUser("user3", true);

		List<Map<String, Object>> permissions = List.of(Map.of("userid", "user3id", "permission", "OWNER"));

		// user not editor
		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityEngineUtils.editEngineUserPermissions(user2, "testId", permissions));
		assertEquals("Insufficient privileges to modify this engine's permissions.", ex.getMessage());

		// user 2 does not have permissions
		List<Map<String, Object>> permission2 = List.of(Map.of("userid", "admin2id", "permission", "OWNER"));
		IllegalArgumentException argEx = assertThrows(IllegalArgumentException.class,
				() -> SecurityEngineUtils.editEngineUserPermissions(user, "testId", permission2));
		assertEquals(
				"Attempting to modify user permission for the following users who do not currently have access to the engine: admin2id",
				argEx.getMessage());

		// make user2 editor
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, "admin2id", "testId", "EDIT");
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, "user3id", "testId", "READ_ONLY");

		// edit user tries to edit owner user
		List<Map<String, Object>> permission5 = List.of(Map.of("userid", "adminid", "permission", "READ_ONLY"));
		argEx = assertThrows(IllegalArgumentException.class,
				() -> SecurityEngineUtils.editEngineUserPermissions(user2, "testId", permission5));
		assertEquals("As a non-owner, you cannot edit access of an owner.", argEx.getMessage());

		// user not owner and tries to give ownership
		List<Map<String, Object>> permission3 = List.of(Map.of("userid", "user3id", "permission", "OWNER"));
		argEx = assertThrows(IllegalArgumentException.class,
				() -> SecurityEngineUtils.editEngineUserPermissions(user2, "testId", permission3));
		assertEquals("As a non-owner, you cannot give a user access as an owner.", argEx.getMessage());

		List<Map<String, Object>> permission4 = List.of(Map.of("userid", "user3id", "permission", "EDIT"));
		SecurityEngineUtils.editEngineUserPermissions(user2, "testId", permission4);

		assertTrue(SecurityEngineUtils.userCanEditEngine(user3, "testId"));

		// other permissions still good
		assertTrue(SecurityEngineUtils.userIsOwner(user, "testId"));
		assertTrue(SecurityEngineUtils.userCanEditEngine(user2, "testId"));
	}

	///
	/// deleteEngine
	///

	@Test
	void testDeleteEngine() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);

		SecurityEngineUtils.deleteEngine("testId");

		assertFalse(SecurityEngineUtils.engineExists("testId"));
		assertFalse(SecurityEngineUtils.userIsOwner(user, "testId"));
		// assert that no longer present in insight table
		// assert that no longer present in enginemeta table
		// assert that no longer present in workspaceengine table
		// assert that no longer present in assetengine table
	}

	@Test
	void testDeleteEngine_removesModelUsageLimits() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", IEngine.CATALOG_TYPE.MODEL, user);
		UnitTestSecurityAuthUtils.createEngine("otherId", "otherAlias", IEngine.CATALOG_TYPE.MODEL, user);
		saveLimit(ModelUsageLimitKey.direct("testId", "adminid", null), tokensADay(100));
		saveLimit(ModelUsageLimitKey.team("testId", "data", "CUSTOM"), ModelUsageLimit.unlimited());
		saveLimit(ModelUsageLimitKey.publicUse("testId"), tokensADay(10));
		saveLimit(ModelUsageLimitKey.direct("otherId", "adminid", null), tokensADay(100));

		SecurityEngineUtils.deleteEngine("testId");

		assertTrue(SecurityModelUsageLimitUtils.getEngineLimits("testId").isEmpty());
		assertNotNull(SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.direct("otherId", "adminid", null)));
	}

	///
	/// addEngineUser and editEngineUserPermission with a model usage limit
	///

	@Test
	void testAddAndEditEngineUser_usageLimit() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createUser("second", false);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", IEngine.CATALOG_TYPE.MODEL, user);
		ModelUsageLimitKey key = ModelUsageLimitKey.direct("testId", "secondid", null);
		ModelUsageLimit twoRules = ModelUsageLimit
				.of(List.of(new ModelUsageLimitRule(Metric.TOKENS, 1000L, Window.MINUTE, Counts.EACH_PERSON),
						new ModelUsageLimitRule(Metric.TOKENS, 50000L, Window.DAY, Counts.EACH_PERSON)));

		SecurityEngineUtils.addEngineUser(user, "secondid", "testId", "READ_ONLY", null, twoRules);
		assertEquals(2, SecurityModelUsageLimitUtils.getLimit(key).getRules().size());

		// a change of level only keeps the limit
		SecurityEngineUtils.editEngineUserPermission(user, "secondid", "NATIVE", "testId", "EDIT", null);
		assertEquals(AccessPermissionEnum.EDIT.getId(),
				SecurityEngineUtils.getUserEnginePermission("secondid", "testId"));
		assertEquals(2, SecurityModelUsageLimitUtils.getLimit(key).getRules().size());

		SecurityEngineUtils.editEngineUserPermission(user, "secondid", "NATIVE", "testId", "READ_ONLY", null,
				(ModelUsageLimit) null);
		assertNull(SecurityModelUsageLimitUtils.getLimit(key));
	}

	///
	/// removeEngineUser
	///

	@Test
	void testRemoveEngineUser() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("admin2", true);
		User user3 = UnitTestSecurityAuthUtils.createUser("user3", true);

		// user not editor
		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityEngineUtils.removeEngineUser(user2, "user3id", "testId"));

		assertEquals("Insufficient privileges to modify this engine's permissions.", ex.getMessage());

		// user does not have permissions
		IllegalArgumentException argEx = assertThrows(IllegalArgumentException.class,
				() -> SecurityEngineUtils.removeEngineUser(user, "admin2id", "testId"));
		assertEquals(
				"Attempting to modify engine permission for a user who does not currently have access to the engine",
				argEx.getMessage());

		// give user2 and user3, edit and read only permissions, respectively
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, "admin2id", "testId", "EDIT");
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, "user3id", "testId", "READ_ONLY");

		// edit user tries to remove owner user
		ex = assertThrows(IllegalAccessException.class,
				() -> SecurityEngineUtils.removeEngineUser(user2, "adminid", "testId"));
		assertEquals("The user doesn't have the high enough permissions to modify this users engine permission.",
				ex.getMessage());

		// user2 successfully changes user3 to edit as edit user
		SecurityEngineUtils.removeEngineUser(user2, "user3id", "testId");

		assertFalse(SecurityEngineUtils.userCanViewEngine(user3, "testId"));

		// other permissions still good
		assertTrue(SecurityEngineUtils.userIsOwner(user, "testId"));
		assertTrue(SecurityEngineUtils.userCanEditEngine(user2, "testId"));
	}

	///
	/// removeEngineUserPermissions
	///

	@Test
	void testRemoveEngineUsersPermissions() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("admin2", true);
		User user3 = UnitTestSecurityAuthUtils.createUser("user3", true);

		// user not editor
		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityEngineUtils.removeEngineUsers(user2, List.of("user3id"), "testId"));
		assertEquals("Insufficient privileges to modify this engine's permissions.", ex.getMessage());

		// user 2 does not have permissions
		IllegalArgumentException argEx = assertThrows(IllegalArgumentException.class,
				() -> SecurityEngineUtils.removeEngineUsers(user, List.of("admin2id"), "testId"));
		assertEquals(
				"Attempting to modify engine permission for the following users who do not currently have access to the engine: admin2id",
				argEx.getMessage());

		// make user2 editor
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, "admin2id", "testId", "EDIT");
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, "user3id", "testId", "READ_ONLY");

		// edit user tries to remove owner user
		ex = assertThrows(IllegalAccessException.class,
				() -> SecurityEngineUtils.removeEngineUsers(user2, List.of("adminid"), "testId"));
		assertEquals("As a non-owner, you cannot remove access of an owner.", ex.getMessage());

		// remove read only user as editor
		SecurityEngineUtils.removeEngineUsers(user2, List.of("user3id"), "testId");

		assertFalse(SecurityEngineUtils.userCanViewEngine(user3, "testId"));

		// other permissions still good
		assertTrue(SecurityEngineUtils.userIsOwner(user, "testId"));
		assertTrue(SecurityEngineUtils.userCanEditEngine(user2, "testId"));
	}

	///
	/// removeExpiredEngineUser
	///

	@Test
	void testRemoveExpiredEngineUser() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);

		SecurityEngineUtils.removeExpiredEngineUser("adminid", "testId");
		assertFalse(SecurityEngineUtils.userIsOwner(user, "testId"));
		assertFalse(SecurityEngineUtils.userCanViewEngine(user, "testId"));
	}

	///
	/// setEngineGlobal
	///

	@Test
	void testEngineGlobal() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("admin2", true);

		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityEngineUtils.setEngineGlobal(user2, "testId", true));
		assertEquals("The user doesn't have the permission to set this engine as global. "
				+ "Only the owner can perform this action.", ex.getMessage());

		assertFalse(SecurityEngineUtils.engineIsGlobal("testId"));
		SecurityEngineUtils.setEngineGlobal(user, "testId", true);
		assertTrue(SecurityEngineUtils.engineIsGlobal("testId"));
	}

	///
	/// setEngineCompletelyGlobal
	///

	@Test
	void testSetEngineCompletelyGlobal() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);

		assertFalse(SecurityEngineUtils.engineIsGlobal("testId"));
		SecurityEngineUtils.setEngineCompletelyGlobal("testId");
		assertTrue(SecurityEngineUtils.engineIsGlobal("testId"));
	}

	///
	/// setEngineDiscoverable
	///

	@Test
	void testEngineDiscoverable() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("admin2", true);

		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityEngineUtils.setEngineDiscoverable(user2, "testId", true));
		assertEquals("The user doesn't have the permission to set this engine as discoverable. Only the owner or an "
				+ "admin can perform this action.", ex.getMessage());

		assertFalse(SecurityEngineUtils.engineIsDiscoverable("testId"));
		SecurityEngineUtils.setEngineDiscoverable(user, "testId", true);
		assertTrue(SecurityEngineUtils.engineIsDiscoverable("testId"));
	}

	///
	/// setEngineVisibility
	///

	@Test
	void testEngineVisibility() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("admin2", true);

		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityEngineUtils.setEngineVisibility(user2, "testId", false));
		assertEquals("The user doesn't have the permission to modify his visibility of this engine", ex.getMessage());

		assertEquals(1, SecurityEngineUtils.getVisibleUserDatabaseIds(user).size());
		SecurityEngineUtils.setEngineVisibility(user, "testId", false);
		assertEquals(0, SecurityEngineUtils.getVisibleUserDatabaseIds(user).size());
	}

	@Test
	void testEngineVisibility_onlyChangesThatEngine() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);
		UnitTestSecurityAuthUtils.createEngine("testId2", "testAlias2", user);

		SecurityEngineUtils.setEngineVisibility(user, "testId", false);

		assertEquals(List.of("testId2"), SecurityEngineUtils.getVisibleUserDatabaseIds(user));
		List<Map<String, Object>> engines = SecurityEngineUtils.getUserEngineList(user, null, null, false, null, null,
				null, null, null, null);
		assertEquals(1, engines.size());
		assertEquals("testId2", engines.getFirst().get("engine_id"));

		SecurityEngineUtils.setEngineVisibility(user, "testId", true);
		assertEquals(2, SecurityEngineUtils.getVisibleUserDatabaseIds(user).size());
	}

	///
	/// setEngineFavorite
	///

	@Test
	void testEngineFavorite() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("admin2", true);

		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityEngineUtils.setEngineFavorite(user2, "testId", true));
		assertEquals("The user doesn't have the permission to modify his visibility of this engine", ex.getMessage());

		List<Map<String, Object>> list = SecurityEngineUtils.getUserEngineList(user, null, null, false, null, null,
				null, null, null, null);
		assertEquals(1, list.size());
		Map<String, Object> userEngine = list.getFirst();
		assertEquals("0", userEngine.get("engine_favorite").toString());

		SecurityEngineUtils.setEngineFavorite(user, "testId", true);

		assertEquals("1",
				SecurityEngineUtils.getUserEngineList(user, null, null, false, null, null, null, null, null, null)
						.getFirst().get("engine_favorite").toString());
	}

	@Test
	void testEngineFavorite_publicEngineWithoutAccess() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);
		SecurityEngineUtils.setEngineGlobal(user, "testId", true);

		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		String user2Id = user2.getPrimaryLoginToken().getId();
		SecurityEngineUtils.setEngineFavorite(user2, "testId", true);

		List<Map<String, Object>> favorites = SecurityEngineUtils.getUserEngineList(user2, null, null, true, null, null,
				null, null, null, null);
		assertEquals(1, favorites.size());
		assertEquals("1", favorites.getFirst().get("engine_favorite").toString());
		// the favorite is kept apart from the permissions, so no access row is added
		assertEquals(0L,
				((Number) UnitTestGroupTables.queryValue(securityDb,
						"SELECT COUNT(*) FROM ENGINEPERMISSION WHERE USERID=? AND ENGINEID=?", user2Id, "testId"))
						.longValue());

		SecurityEngineUtils.setEngineFavorite(user2, "testId", false);
		assertTrue(SecurityEngineUtils.getUserEngineList(user2, null, null, true, null, null, null, null, null, null)
				.isEmpty());
		assertEquals(1L,
				((Number) UnitTestGroupTables.queryValue(securityDb,
						"SELECT COUNT(*) FROM USERENGINEPREFERENCE WHERE USERID=? AND TYPE=? AND ENGINEID=?", user2Id,
						"NATIVE", "testId")).longValue());
	}

	///
	/// setEngineName
	///

	@Test
	void testSetEngineName() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("admin2", true);
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, "admin2id", "testId", "EDIT");

		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityEngineUtils.setEngineName(user2, "testId", "newName"));
		assertEquals(
				"The user doesn't have the permission to change the engine name. Only the owner can perform this action.",
				ex.getMessage());

		assertEquals("testAlias", SecurityEngineUtils.getEngineAliasForId("testId"));
		assertTrue(SecurityEngineUtils.setEngineName(user, "testId", "newName"));
		assertEquals("newName", SecurityEngineUtils.getEngineAliasForId("testId"));
	}

	///
	/// metadata
	///

	@Test
	void testMetadata() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);

		List<String> metakeys = SecurityEngineUtils.getAllMetakeys();
		assertEquals(6, metakeys.size());
		assertTrue(metakeys.contains("data classification"));
		assertTrue(metakeys.contains("data restrictions"));
		assertTrue(metakeys.contains("description"));
		assertTrue(metakeys.contains("domain"));
		assertTrue(metakeys.contains("markdown"));
		assertTrue(metakeys.contains("tag"));

		Map<String, Object> meta = Map.of("domain", "value1", "description", List.of("list1", "list2"), "markdown",
				"#title");
		SecurityEngineUtils.updateEngineMetadata("testId", meta);

		Map<String, Object> ret = SecurityEngineUtils.getAggregateEngineMetadata("testId",
				List.of("domain", "description"), true);

		assertEquals(2, ret.size());

		assertEquals("value1", ret.get("domain"));
		assertEquals("[list1, list2]", ret.get("description").toString());

	}

	///
	/// checkUserHasAccessToDatabase
	///

	@Test
	void testCheckUserHasAccessToDatabase() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);
		User user2 = UnitTestSecurityAuthUtils.createUser("second", true);

		assertTrue(SecurityEngineUtils.checkUserHasAccessToDatabase("testId", "adminid"));
		assertFalse(SecurityEngineUtils.checkUserHasAccessToDatabase("testId", "secondid"));
	}

	///
	/// getEngineUsersNoCredentials
	///

	@Test
	void testGetEngineUsersNoCredentials() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);
		User user2 = UnitTestSecurityAuthUtils.createUser("second", true);
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, "secondid", "testId", "EDIT");
		User user3 = UnitTestSecurityAuthUtils.createUser("three", true);
		User user4 = UnitTestSecurityAuthUtils.createUser("four", true);

		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityEngineUtils.getEngineUsersNoCredentials(user3, "testId", null, 0, 0));
		assertEquals("The user does not have access to view this engine", ex.getMessage());

		List<Map<String, Object>> noCreds = SecurityEngineUtils.getEngineUsersNoCredentials(user, "testId", null, 0, 0);
		assertEquals(2, noCreds.size());
		List<String> noCredsUsers = noCreds.stream().map(s -> s.get("id").toString()).toList();
		assertEquals(2, noCredsUsers.size());
		assertTrue(noCredsUsers.contains("threeid"));
		assertTrue(noCredsUsers.contains("fourid"));
	}

	///
	/// getUserHasExplicitAccess
	///

	@Test
	void testGetUserHasExplicitAccess() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);
		User user2 = UnitTestSecurityAuthUtils.createUser("second", true);
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, "secondid", "testId", "EDIT");
		UnitTestSecurityAuthUtils.createEngineGlobal("testId2", "testAlias2", user);
		UnitTestSecurityAuthUtils.createEngine("testId3", "testAlias3", user);

		Set<String> engines = SecurityEngineUtils.getEngineUserHasExplicitAccess(user2);
		assertEquals(2, engines.size());
		assertTrue(engines.contains("testId"));
		assertTrue(engines.contains("testId2"));

		// NOTE: This seems weird that two methods called something similar differ on if
		// the engine is set to
		// global
		assertTrue(SecurityEngineUtils.userHasExplicitAccess(user2, "testId"));
		assertFalse(SecurityEngineUtils.userHasExplicitAccess(user2, "testId2"));
	}

	///
	/// getUserRequestableEngines
	///

	@Test
	void testGetUserRequestableEngines() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);
		SecurityEngineUtils.setEngineDiscoverable(user, "testId", true);

		User user2 = UnitTestSecurityAuthUtils.createUser("second", true);
		UnitTestSecurityAuthUtils.createEngine("second", "blah", user2);

		List<Map<String, Object>> requestable = SecurityEngineUtils.getUserRequestableEngines(List.of("second"));
		assertEquals(1, requestable.size());
		assertEquals("testId", requestable.getFirst().get("ENGINEID"));
	}

	///
	/// getEngineInfo
	///

	@Test
	void testGetEngineInfo() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);

		List<Map<String, Object>> engineInfo = SecurityEngineUtils.getEngineInfo(List.of("testId"));
		assertEquals(1, engineInfo.size());
		assertEquals("testId", engineInfo.getFirst().get("ENGINEID"));

		engineInfo = SecurityEngineUtils.getEngineInfo(List.of("foobar"));
		assertEquals(0, engineInfo.size());
	}

	///
	/// getGlobalEngineIds
	///

	@Test
	void testGetGlobalEngineIds() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);

		UnitTestSecurityAuthUtils.createEngine("id", "ta", user);
		UnitTestSecurityAuthUtils.createEngineGlobal("id2", "ta2", user);

		Set<String> globalEngineIds = SecurityEngineUtils.getGlobalEngineIds();
		assertEquals(1, globalEngineIds.size());
		assertTrue(globalEngineIds.contains("id2"));
	}

	///
	/// getUserEngineIdList
	///

	@Test
	void testGetUserEngineIdList() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("admin2", true);
		UnitTestSecurityAuthUtils.createEngine("id", "ta", user);
		UnitTestSecurityAuthUtils.createEngine("id2", "ta2", user2);
		UnitTestSecurityAuthUtils.createEngineGlobal("id3", "ta3", user2);
		UnitTestSecurityAuthUtils.createEngine("id4", "ta4", IEngine.CATALOG_TYPE.MODEL, user);
		UnitTestSecurityAuthUtils.createEngine("id5", "ta5", IEngine.CATALOG_TYPE.FUNCTION, user);

		List<String> ids = SecurityEngineUtils.getUserEngineIdList(user, List.of("DATABASE", "MODEL"), true, true,
				true);

		assertEquals(3, ids.size());
		assertTrue(ids.contains("id"));
		assertTrue(ids.contains("id3"));
		assertTrue(ids.contains("id4"));
	}

	///
	/// getAvailableMetaValues
	///

	@Test
	void testGetAvailableMetaValues() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "ta", user);
		UnitTestSecurityAuthUtils.createEngine("testId2", "ta2", user);

		Map<String, Object> meta = Map.of("domain", "value1", "description", List.of("list1", "list2"), "markdown",
				"#title");
		SecurityEngineUtils.updateEngineMetadata("testId", meta);

		List<Map<String, Object>> metadata = SecurityEngineUtils.getAvailableMetaValues(List.of("testId"),
				List.of("domain"));

		assertEquals(1, metadata.size());
		Map<String, Object> map = metadata.getFirst();
		assertEquals("domain", map.get("METAKEY").toString());
		assertEquals("value1", map.get("METAVALUE").toString());
	}

	///
	/// getAllUserDatabaseList
	///

	@Test
	void testGetAllUserDatabaseList() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("second", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "ta", user);
		UnitTestSecurityAuthUtils.createEngineGlobal("testId2", "ta2", user2);

		List<Map<String, Object>> vals = SecurityEngineUtils.getAllUserDatabaseList(user);
		assertEquals(2, vals.size());

		Map<String, Object> users = vals.getFirst();
		assertEquals("testId", users.get("app_id"));

		Map<String, Object> global = vals.get(1);
		assertEquals("testId2", global.get("app_id"));
	}

	///
	/// getUserEngineList
	///

	@Test
	void testGetUserEngineList() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("second", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "ta", user);
		UnitTestSecurityAuthUtils.createEngineGlobal("testId2", "ta2", user2);
		UnitTestSecurityAuthUtils.createEngine("testId3", "ta3", user2);

		List<Map<String, Object>> vals = SecurityEngineUtils.getUserEngineList(user, List.of("DATABASE"), 0, 0);

		assertEquals(2, vals.size());

		List<String> ids = vals.stream().map(s -> s.get("app_id").toString()).toList();
		assertTrue(ids.contains("testId"));
		assertTrue(ids.contains("testId2"));
	}

	///
	/// getDiscoverableEngineList
	///

	@Test
	void testGetDiscoverableEngineList() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("second", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "ta", user);
		UnitTestSecurityAuthUtils.createEngine("testId2", "ta2", user2);
		UnitTestSecurityAuthUtils.createEngine("testId3", "ta3", IEngine.CATALOG_TYPE.FUNCTION, user);
		UnitTestSecurityAuthUtils.createEngine("testId4", "ta4", IEngine.CATALOG_TYPE.MODEL, user);

		SecurityEngineUtils.setEngineDiscoverable(user, "testId", true);
		SecurityEngineUtils.setEngineDiscoverable(user2, "testId2", true);
		SecurityEngineUtils.setEngineDiscoverable(user, "testId3", true);
		SecurityEngineUtils.setEngineDiscoverable(user, "testId4", true);

		List<Map<String, Object>> engines = SecurityEngineUtils.getDiscoverableEngineList(null,
				List.of("DATABASE", "FUNCTION"));

		assertEquals(3, engines.size());

		List<String> ids = engines.stream().map(s -> s.get("engine_id").toString()).toList();
		assertTrue(ids.contains("testId"));
		assertTrue(ids.contains("testId2"));
		assertTrue(ids.contains("testId3"));
	}

	///
	/// getUserDiscoverableEngineList
	///

	@Test
	void testGetUserDiscoverableEngineList() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("second", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "ta", user);
		UnitTestSecurityAuthUtils.createEngine("testId2", "ta2", user2);
		UnitTestSecurityAuthUtils.createEngine("testId3", "ta3", IEngine.CATALOG_TYPE.FUNCTION, user);
		UnitTestSecurityAuthUtils.createEngine("testId4", "ta4", IEngine.CATALOG_TYPE.MODEL, user);

		SecurityEngineUtils.setEngineDiscoverable(user, "testId", true);
		SecurityEngineUtils.setEngineDiscoverable(user2, "testId2", true);
		SecurityEngineUtils.setEngineDiscoverable(user, "testId3", true);
		SecurityEngineUtils.setEngineDiscoverable(user, "testId4", true);

		Map<String, Object> meta = Map.of("domain", "value1", "description", List.of("list1", "list2"), "markdown",
				"#title");
		SecurityEngineUtils.updateEngineMetadata("testId2", meta);

		List<Map<String, Object>> vals = SecurityEngineUtils.getUserDiscoverableEngineList(user,
				List.of("DATABASE", "FUNCTION"), List.of("testId", "testId2"), Map.of("domain", "value1"), null, "0",
				"0");

		assertEquals(1, vals.size());
		assertEquals("testId2", vals.getFirst().get("engine_id"));
	}

	///
	/// getFullUserEngineIds
	///

	@Test
	void testGetFullUserEngineIds() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("second", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "ta", user);
		UnitTestSecurityAuthUtils.createEngine("testId2", "ta2", user2);
		UnitTestSecurityAuthUtils.createEngine("testId3", "ta3", IEngine.CATALOG_TYPE.FUNCTION, user);

		List<String> userIds = SecurityEngineUtils.getFullUserEngineIds(user);

		assertEquals(2, userIds.size());
		assertTrue(userIds.contains("testId"));
		assertTrue(userIds.contains("testId3"));
	}

	///
	/// getMetakeyOptions
	///

	@Test
	void testGetMetakeyOptions() {
		List<Map<String, Object>> metakeys = SecurityEngineUtils.getMetakeyOptions("domain");
		assertEquals(1, metakeys.size());
		assertEquals("domain", metakeys.getFirst().get("metakey"));
	}

	///
	/// updateEngineUserPermissions (the login sync)
	///

	@Test
	void testUpdateEngineUserPermissions_keepsTheLimitsOfKeptGrants() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("keptId", "keptAlias", IEngine.CATALOG_TYPE.MODEL, user);
		UnitTestSecurityAuthUtils.createEngine("droppedId", "droppedAlias", IEngine.CATALOG_TYPE.MODEL, user);
		saveLimit(ModelUsageLimitKey.direct("keptId", "adminid", null), tokensADay(100));
		saveLimit(ModelUsageLimitKey.direct("droppedId", "adminid", null), tokensADay(200));

		List<Map<String, Object>> updates = new ArrayList<>();
		Map<String, Object> updatesMap = new HashMap<>();
		updatesMap.put("engineId", "keptId");
		updatesMap.put("engineName", "keptAlias");
		updatesMap.put("engineType", IEngine.CATALOG_TYPE.MODEL);
		updatesMap.put("permission", "READ_ONLY");
		updates.add(updatesMap);
		SecurityEngineUtils.updateEngineUserPermissions(user, updates);

		assertEquals(AccessPermissionEnum.READ_ONLY.getId(),
				SecurityEngineUtils.getUserEnginePermission("adminid", "keptId"));
		assertNull(SecurityEngineUtils.getUserEnginePermission("adminid", "droppedId"));
		ModelUsageLimit kept = SecurityModelUsageLimitUtils
				.getLimit(ModelUsageLimitKey.direct("keptId", "adminid", null));
		assertNotNull(kept);
		assertEquals(100L, kept.getRules().get(0).getAmount());
		assertNull(SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.direct("droppedId", "adminid", null)));
	}

	@Test
	void testUpdateEngineUserPermissions_addsTheEnginesNotYetRegistered() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);

		List<Map<String, Object>> updates = new ArrayList<>();
		updates.add(loginSyncGrant("newId", "EDIT"));
		List<Map<String, Object>> added = SecurityEngineUtils.updateEngineUserPermissions(user, updates);

		assertEquals(1, added.size());
		assertEquals("newId", added.getFirst().get("engineId"));
		assertTrue(SecurityEngineUtils.engineExists("newId"));
		assertEquals(AccessPermissionEnum.EDIT.getId(),
				SecurityEngineUtils.getUserEnginePermission("adminid", "newId"));
	}

	@Test
	void testUpdateEngineUserPermissions_aFailureSavesNothing() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("keptId", "keptAlias", IEngine.CATALOG_TYPE.MODEL, user);
		saveLimit(ModelUsageLimitKey.direct("keptId", "adminid", null), tokensADay(100));

		List<Map<String, Object>> updates = new ArrayList<>();
		updates.add(loginSyncGrant("newId", "READ_ONLY"));
		updates.add(loginSyncGrant("rejectId", "READ_ONLY"));
		// the grant on the last engine fails, after both engines were added and the
		// person's grants were deleted
		addCheck("SYNC_FAIL", "ENGINEPERMISSION", "ENGINEID <> 'rejectId'");
		try {
			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> SecurityEngineUtils.updateEngineUserPermissions(user, updates));
			assertEquals("An error occurred while updating the user engine permissions in db ", e.getMessage());
		} finally {
			dropCheck("SYNC_FAIL", "ENGINEPERMISSION");
		}

		assertFalse(SecurityEngineUtils.engineExists("newId"));
		assertFalse(SecurityEngineUtils.engineExists("rejectId"));
		assertEquals(AccessPermissionEnum.OWNER.getId(),
				SecurityEngineUtils.getUserEnginePermission("adminid", "keptId"));
		assertNotNull(SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.direct("keptId", "adminid", null)));
	}

	private static Map<String, Object> loginSyncGrant(String engineId, String permission) {
		Map<String, Object> grant = new HashMap<>();
		grant.put("engineId", engineId);
		grant.put("engineName", engineId + "Alias");
		grant.put("engineType", IEngine.CATALOG_TYPE.DATABASE);
		grant.put("permission", permission);
		return grant;
	}

	private static void saveLimit(ModelUsageLimitKey key, ModelUsageLimit limit) throws Exception {
		QueryExecutionUtility.write(SystemEngineRegistry.getSecurityDb(), connection -> {
			SecurityModelUsageLimitUtils.setLimit(connection, key, limit, null);
			return null;
		});
	}

	private static ModelUsageLimit tokensADay(long amount) {
		return ModelUsageLimit
				.of(List.of(new ModelUsageLimitRule(Metric.TOKENS, amount, Window.DAY, Counts.EACH_PERSON)));
	}

	///
	/// updateEngineToolApp
	///

	@Test
	void testUpdateEngineToolApp() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "ta", user);

		SecurityEngineUtils.updateEngineToolApp("testId", "testToolApp");

		List<Map<String, Object>> engineList = SecurityEngineUtils.getUserEngineList(user, null, null);
		assertEquals(1, engineList.size());

		assertEquals("testToolApp", engineList.getFirst().get("engine_tool_app"));
	}

	///
	/// access request decisions and permission changes: one transaction, then
	/// notifications
	///

	@Test
	void testApproveEngineUserAccessRequests_notifiesOnlyAfterTheApprovalIsSaved() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User secondUser = UnitTestSecurityAuthUtils.createUser("second", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);
		SecurityEngineUtils.setUserAccessRequest("secondid", "NATIVE", "testId", "reason", 2, secondUser);
		List<Map<String, String>> requests = List.of(Map.of("requestid", pendingRequestId("testId"), "userid",
				"secondid", "type", "NATIVE", "permission", "EDIT"));

		try (MockedStatic<Utility> utility = mockStatic(Utility.class, CALLS_REAL_METHODS);
				MockedStatic<NotificationDbUtils> notifications = mockStatic(NotificationDbUtils.class);
				MockedStatic<EmailUtility> emails = mockStatic(EmailUtility.class)) {
			utility.when(Utility::isNotificationDatabaseEnabled).thenReturn(true);
			notifications.when(() -> NotificationDbUtils.createNotification(any(), any(), any(), any(), any(), any(),
					any(), any(), any(), any())).thenAnswer(invocation -> {
						// the save has committed: no transaction holds the connection, and
						// a read sees the grant and the approval
						assertFalse(insideSecurityTransaction());
						assertEquals(AccessPermissionEnum.EDIT.getId(),
								SecurityEngineUtils.getUserEnginePermission("secondid", "testId"));
						assertTrue(SecurityEngineUtils.getUserAccessRequestsByEngine("testId").isEmpty());
						return null;
					});

			SecurityEngineUtils.approveEngineUserAccessRequests(user, "testId", requests, null);

			notifications.verify(() -> NotificationDbUtils.createNotification(eq(user), eq("secondid"), eq("NATIVE"),
					eq("testId"), eq(NotificationConstants.Type.REQUEST_APPROVAL), eq("database"), any(), isNull(),
					eq("EDIT"), any()), times(1));
			emails.verify(() -> EmailUtility.sendAccessRequestApprovalEmailNotification(user, "secondid", "testId",
					"EDIT", EmailUtility.RESOURCE_TYPE.ENGINE), times(1));
		}
	}

	@Test
	void testApproveEngineUserAccessRequests_aFailedApprovalSavesNothingAndTellsNoOne() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User secondUser = UnitTestSecurityAuthUtils.createUser("second", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);
		SecurityEngineUtils.setUserAccessRequest("secondid", "NATIVE", "testId", "reason", 2, secondUser);
		List<Map<String, String>> requests = List.of(Map.of("requestid", pendingRequestId("testId"), "userid",
				"secondid", "type", "NATIVE", "permission", "EDIT"));

		// marking the request approved fails after the grant was written
		addCheck("APPROVE_FAIL", "ENGINEACCESSREQUEST", "APPROVER_DECISION <> 'APPROVED'");
		try (MockedStatic<Utility> utility = mockStatic(Utility.class, CALLS_REAL_METHODS);
				MockedStatic<NotificationDbUtils> notifications = mockStatic(NotificationDbUtils.class);
				MockedStatic<EmailUtility> emails = mockStatic(EmailUtility.class)) {
			utility.when(Utility::isNotificationDatabaseEnabled).thenReturn(true);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> SecurityEngineUtils.approveEngineUserAccessRequests(user, "testId", requests, null));
			assertEquals("An error occurred while approving the access requests", e.getMessage());
			notifications.verifyNoInteractions();
			emails.verifyNoInteractions();
		} finally {
			dropCheck("APPROVE_FAIL", "ENGINEACCESSREQUEST");
		}

		assertNull(SecurityEngineUtils.getUserEnginePermission("secondid", "testId"));
		assertEquals(1, SecurityEngineUtils.getUserAccessRequestsByEngine("testId").size());
	}

	@Test
	void testDenyEngineUserAccessRequests_tellsTheEnginesRequestersOnlyAfterTheDenialIsSaved() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User secondUser = UnitTestSecurityAuthUtils.createUser("second", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);
		UnitTestSecurityAuthUtils.createEngine("otherId", "otherAlias", user);
		SecurityEngineUtils.setUserAccessRequest("secondid", "NATIVE", "testId", "reason", 2, secondUser);
		SecurityEngineUtils.setUserAccessRequest("secondid", "NATIVE", "otherId", "reason", 3, secondUser);
		String requestId = pendingRequestId("testId");
		String otherRequestId = pendingRequestId("otherId");

		try (MockedStatic<Utility> utility = mockStatic(Utility.class, CALLS_REAL_METHODS);
				MockedStatic<NotificationDbUtils> notifications = mockStatic(NotificationDbUtils.class)) {
			utility.when(Utility::isNotificationDatabaseEnabled).thenReturn(true);
			notifications.when(() -> NotificationDbUtils.createNotification(any(), any(), any(), any(), any(), any(),
					any(), any(), any(), any())).thenAnswer(invocation -> {
						// the denial has committed: no transaction holds the connection,
						// and a read sees it
						assertFalse(insideSecurityTransaction());
						assertTrue(SecurityEngineUtils.getUserAccessRequestsByEngine("testId").isEmpty());
						return null;
					});

			// a request of another engine is neither denied nor told
			SecurityEngineUtils.denyEngineUserAccessRequests(user, "testId", List.of(requestId, otherRequestId));

			notifications.verify(() -> NotificationDbUtils.createNotification(eq(user), eq("secondid"), eq("NATIVE"),
					eq("testId"), eq(NotificationConstants.Type.REQUEST_DENIAL), eq("database"), any(), isNull(),
					eq("EDIT"), any()), times(1));
			notifications.verify(() -> NotificationDbUtils.createNotification(any(), any(), any(), eq("otherId"), any(),
					any(), any(), any(), any(), any()), never());
		}

		assertEquals(1, SecurityEngineUtils.getUserAccessRequestsByEngine("otherId").size());
	}

	@Test
	void testDenyEngineUserAccessRequests_aFailedDenialTellsNoOne() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User secondUser = UnitTestSecurityAuthUtils.createUser("second", true);
		User thirdUser = UnitTestSecurityAuthUtils.createUser("third", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);
		SecurityEngineUtils.setUserAccessRequest("secondid", "NATIVE", "testId", "reason", 2, secondUser);
		SecurityEngineUtils.setUserAccessRequest("thirdid", "NATIVE", "testId", "reason", 3, thirdUser);
		List<String> requestIds = new ArrayList<>();
		for (Map<String, Object> request : SecurityEngineUtils.getUserAccessRequestsByEngine("testId")) {
			requestIds.add(request.get("ID").toString());
		}

		// the third person's request cannot be denied, so neither is
		addCheck("DENY_FAIL", "ENGINEACCESSREQUEST", "REQUEST_USERID <> 'thirdid' OR APPROVER_DECISION <> 'DENIED'");
		try (MockedStatic<Utility> utility = mockStatic(Utility.class, CALLS_REAL_METHODS);
				MockedStatic<NotificationDbUtils> notifications = mockStatic(NotificationDbUtils.class)) {
			utility.when(Utility::isNotificationDatabaseEnabled).thenReturn(true);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> SecurityEngineUtils.denyEngineUserAccessRequests(user, "testId", requestIds));
			assertEquals("An error occurred while updating user access request", e.getMessage());
			notifications.verifyNoInteractions();
		} finally {
			dropCheck("DENY_FAIL", "ENGINEACCESSREQUEST");
		}

		assertEquals(2, SecurityEngineUtils.getUserAccessRequestsByEngine("testId").size());
	}

	@Test
	void testAddEngineUserPermissions_notifiesOnlyAfterEveryoneIsSaved() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createUser("second", false);
		UnitTestSecurityAuthUtils.createUser("third", false);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);
		List<Map<String, Object>> permissions = List.of(Map.of("userid", "secondid", "permission", "READ_ONLY"),
				Map.of("userid", "thirdid", "permission", "EDIT"));

		try (MockedStatic<Utility> utility = mockStatic(Utility.class, CALLS_REAL_METHODS);
				MockedStatic<NotificationDbUtils> notifications = mockStatic(NotificationDbUtils.class)) {
			utility.when(Utility::isNotificationDatabaseEnabled).thenReturn(true);
			notifications.when(() -> NotificationDbUtils.createNotification(any(), any(), any(), any(), any(), any(),
					any(), any(), any(), any())).thenAnswer(invocation -> {
						// the save has committed: no transaction holds the connection, and
						// a read sees everyone it added
						assertFalse(insideSecurityTransaction());
						assertEquals(AccessPermissionEnum.READ_ONLY.getId(),
								SecurityEngineUtils.getUserEnginePermission("secondid", "testId"));
						assertEquals(AccessPermissionEnum.EDIT.getId(),
								SecurityEngineUtils.getUserEnginePermission("thirdid", "testId"));
						return null;
					});

			SecurityEngineUtils.addEngineUserPermissions(user, "testId", permissions);

			notifications.verify(() -> NotificationDbUtils.createNotification(any(), any(), any(), eq("testId"),
					eq(NotificationConstants.Type.USER_ADDITION), any(), any(), any(), any(), any()), times(2));
		}
	}

	@Test
	void testAddEngineUserPermissions_aFailedSaveAddsNoOneAndTellsNoOne() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createUser("second", false);
		UnitTestSecurityAuthUtils.createUser("third", false);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);
		List<Map<String, Object>> permissions = List.of(Map.of("userid", "secondid", "permission", "READ_ONLY"),
				Map.of("userid", "thirdid", "permission", "EDIT"));

		// the second grant fails after the first was written
		addCheck("ADD_FAIL", "ENGINEPERMISSION", "USERID <> 'thirdid'");
		try (MockedStatic<Utility> utility = mockStatic(Utility.class, CALLS_REAL_METHODS);
				MockedStatic<NotificationDbUtils> notifications = mockStatic(NotificationDbUtils.class)) {
			utility.when(Utility::isNotificationDatabaseEnabled).thenReturn(true);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> SecurityEngineUtils.addEngineUserPermissions(user, "testId", permissions));
			assertEquals("An error occurred adding the user permissions for this engine", e.getMessage());
			notifications.verifyNoInteractions();
		} finally {
			dropCheck("ADD_FAIL", "ENGINEPERMISSION");
		}

		assertNull(SecurityEngineUtils.getUserEnginePermission("secondid", "testId"));
		assertNull(SecurityEngineUtils.getUserEnginePermission("thirdid", "testId"));
	}

	@Test
	void testEditEngineUserPermission_notifiesOnlyAfterTheChangeIsSaved() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createUser("second", false);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, "secondid", "testId", "READ_ONLY");

		try (MockedStatic<Utility> utility = mockStatic(Utility.class, CALLS_REAL_METHODS);
				MockedStatic<NotificationDbUtils> notifications = mockStatic(NotificationDbUtils.class)) {
			utility.when(Utility::isNotificationDatabaseEnabled).thenReturn(true);
			notifications.when(() -> NotificationDbUtils.createNotification(any(), any(), any(), any(), any(), any(),
					any(), any(), any(), any())).thenAnswer(invocation -> {
						// the save has committed: no transaction holds the connection, and
						// a read sees the change
						assertFalse(insideSecurityTransaction());
						assertEquals(AccessPermissionEnum.EDIT.getId(),
								SecurityEngineUtils.getUserEnginePermission("secondid", "testId"));
						return null;
					});

			SecurityEngineUtils.editEngineUserPermission(user, "secondid", "NATIVE", "testId", "EDIT", null);

			notifications.verify(() -> NotificationDbUtils.createNotification(eq(user), eq("secondid"), any(),
					eq("testId"), eq(NotificationConstants.Type.PERMISSION_CHANGE), eq("database"), any(),
					eq("READ_ONLY"), eq("EDIT"), any()), times(1));
		}
	}

	///
	/// setEngineFavorite: every login or none
	///

	@Test
	void testEngineFavorite_savesEveryLoginOrNone() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testId", "testAlias", user);
		AccessToken github = new AccessToken();
		github.setId("admingithub");
		github.setName("adminname");
		github.setProvider(AuthProvider.GITHUB);
		user.setAccessToken(github);

		// the second login's row fails after the first login's was written
		addCheck("FAVORITE_FAIL", "USERENGINEPREFERENCE", "TYPE <> 'GITHUB'");
		try {
			assertThrows(Exception.class, () -> SecurityEngineUtils.setEngineFavorite(user, "testId", true));
		} finally {
			dropCheck("FAVORITE_FAIL", "USERENGINEPREFERENCE");
		}
		assertEquals(0L, favoriteRows("testId"));

		SecurityEngineUtils.setEngineFavorite(user, "testId", true);
		assertEquals(2L, favoriteRows("testId"));
	}

	private long favoriteRows(String engineId) throws SQLException {
		return ((Number) UnitTestGroupTables.queryValue(securityDb,
				"SELECT COUNT(*) FROM USERENGINEPREFERENCE WHERE ENGINEID=? AND FAVORITE=TRUE", engineId)).longValue();
	}

	private static String pendingRequestId(String engineId) {
		List<Map<String, Object>> pending = SecurityEngineUtils.getUserAccessRequestsByEngine(engineId);
		assertEquals(1, pending.size());
		return pending.getFirst().get("ID").toString();
	}

	/**
	 * Whether this thread is inside a security database transaction: a write holds
	 * the engine's connection until it has committed or rolled back.
	 */
	private boolean insideSecurityTransaction() throws SQLException {
		Connection connection = securityDb.getConnection();
		try {
			return Thread.holdsLock(connection);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, connection);
		}
	}

	/**
	 * Adds a check to a security table, so a test can fail a later statement of a
	 * save on purpose; {@link #dropCheck} takes it off again.
	 */
	private void addCheck(String name, String table, String condition) throws Exception {
		QueryExecutionUtility.executeUpdate(securityDb,
				"ALTER TABLE " + table + " ADD CONSTRAINT " + name + " CHECK (" + condition + ")",
				QueryExecutionUtility.NO_PARAMETERS);
	}

	private void dropCheck(String name, String table) throws Exception {
		QueryExecutionUtility.executeUpdate(securityDb, "ALTER TABLE " + table + " DROP CONSTRAINT " + name,
				QueryExecutionUtility.NO_PARAMETERS);
	}

}
