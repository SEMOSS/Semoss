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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.SystemEngineRegistry;

/**
 * Checks how custom groups keep their members and managers when a group is
 * created, renamed or deleted, the group id rules, and the trimmed member
 * projections and member counts.
 */
class AdminSecurityGroupUtilsCustomGroupUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private static final String CUSTOM = SecurityGroupManagerUtils.CUSTOM_GROUP_TYPE;
	private static final String NATIVE = AuthProvider.NATIVE.getLabel();
	private static final String MICROSOFT = AuthProvider.MICROSOFT.getLabel();

	private static final String ADMIN_ID = "adminid";
	private static final String MEMBER_ID = "memberid";
	private static final String STRAY_ID = "strayid";
	private static final String ADMIN_MANAGER = ADMIN_ID + ":" + NATIVE;
	private static final String MEMBER_ROW = MEMBER_ID + ":" + NATIVE;

	private static final String EMPTY_ID_MESSAGE = "The group id cannot be null or empty";
	private static final String APOSTROPHE_MESSAGE = "Group names cannot contain an apostrophe (')";

	private static final Set<String> MEMBER_KEYS = Set.of("userid", "type", "dateadded", "name", "username", "email");
	private static final Set<String> NON_MEMBER_KEYS = Set.of("id", "type", "name", "username", "email");
	private static final List<String> REMOVED_KEYS = List.of("phone", "phoneextension", "countrycode", "admin",
			"publisher", "exporter", "enddate", "groupid", "permissiongrantedby", "permissiongrantedbytype");

	private IRDBMSEngine securityDb;
	private List<String> tables = new ArrayList<>();

	private User admin;
	private User member;
	private AdminSecurityGroupUtils groups;

	@BeforeEach
	void setup() {
		securityDb = SystemEngineRegistry.getSecurityDb();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		admin = UnitTestSecurityAuthUtils.createUser("admin", true);
		member = UnitTestSecurityAuthUtils.createUser("member", false);
		groups = AdminSecurityGroupUtils.getInstance(admin);
	}

	@AfterEach
	void cleanup() throws Exception {
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		tables = UnitTestSecurityAuthUtils.clearSecurityDB(securityDb, tables);
	}

	///
	/// getUncheckedInstance
	///

	@Test
	void getUncheckedInstance_isTheAdminInstanceWithoutTheAdminCheck() {
		assertSame(AdminSecurityGroupUtils.getInstance(admin), AdminSecurityGroupUtils.getUncheckedInstance());
		assertNull(AdminSecurityGroupUtils.getInstance(member));
		assertNotNull(AdminSecurityGroupUtils.getUncheckedInstance());
	}

	///
	/// validateGroupId
	///

	@ParameterizedTest
	@NullAndEmptySource
	@ValueSource(strings = { "  ", "\t" })
	void validateGroupId_rejectsABlankId(String groupId) {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> AdminSecurityGroupUtils.validateGroupId(groupId));
		assertEquals(EMPTY_ID_MESSAGE, e.getMessage());
	}

	@ParameterizedTest
	@ValueSource(strings = { "o'brien", "'", "team'" })
	void validateGroupId_rejectsAnApostrophe(String groupId) {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> AdminSecurityGroupUtils.validateGroupId(groupId));
		assertEquals(APOSTROPHE_MESSAGE, e.getMessage());
	}

	@ParameterizedTest
	@ValueSource(strings = { "team", "Team Two", "team-3_x", "o\"brien" })
	void validateGroupId_acceptsOtherIds(String groupId) {
		assertDoesNotThrow(() -> AdminSecurityGroupUtils.validateGroupId(groupId));
	}

	@Test
	void addGroup_rejectsAnInvalidIdWithoutCreatingAnything() throws Exception {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> groups.addGroup(admin, "o'brien", CUSTOM, "desc"));

		assertEquals(APOSTROPHE_MESSAGE, e.getMessage());
		assertFalse(groups.groupExists("o'brien", CUSTOM));
		assertTrue(UnitTestGroupTables.managers(securityDb, "o'brien").isEmpty());
		assertThrows(IllegalArgumentException.class, () -> groups.addGroup(admin, " ", MICROSOFT, "desc"));
	}

	@Test
	void editGroupDetailsAndPropagate_rejectsAnInvalidNewIdAndKeepsTheGroup() throws Exception {
		groups.addGroup(admin, "team", CUSTOM, "desc");
		groups.addUserToGroup(admin, "team", MEMBER_ID, NATIVE, null);

		assertEquals(APOSTROPHE_MESSAGE,
				assertThrows(IllegalArgumentException.class,
						() -> groups.editGroupDetailsAndPropagate(admin, "team", CUSTOM, "te'am", "desc"))
						.getMessage());
		assertEquals(EMPTY_ID_MESSAGE, assertThrows(IllegalArgumentException.class,
				() -> groups.editGroupDetailsAndPropagate(admin, "team", CUSTOM, "", "desc")).getMessage());

		assertTrue(groups.groupExists("team", CUSTOM));
		assertEquals(List.of(ADMIN_MANAGER), UnitTestGroupTables.managers(securityDb, "team"));
		assertEquals(List.of(MEMBER_ROW), UnitTestGroupTables.members(securityDb, "team"));
	}

	///
	/// addGroup
	///

	@Test
	void addGroup_makesTheCreatorTheFirstManagerOfACustomGroup() throws Exception {
		groups.addGroup(admin, "team", CUSTOM, "desc");

		assertEquals(List.of(ADMIN_MANAGER), UnitTestGroupTables.managers(securityDb, "team"));
		assertTrue(SecurityGroupManagerUtils.userIsGroupManager(admin, "team"));
		assertEquals(ADMIN_ID + ":" + NATIVE, UnitTestGroupTables.queryValue(securityDb,
				"SELECT PERMISSIONGRANTEDBY || ':' || PERMISSIONGRANTEDBYTYPE FROM GROUPMANAGERS WHERE GROUPID=?",
				"team"));
		assertNotNull(UnitTestGroupTables.queryValue(securityDb, "SELECT DATEADDED FROM GROUPMANAGERS WHERE GROUPID=?",
				"team"));
	}

	@Test
	void addGroup_addsNoManagerToOtherGroupTypes() throws Exception {
		groups.addGroup(admin, "msgroup", MICROSOFT, "desc");

		assertTrue(groups.groupExists("msgroup", MICROSOFT));
		assertTrue(UnitTestGroupTables.managers(securityDb, "msgroup").isEmpty());
	}

	@Test
	void addGroup_dropsMembersAndManagersLeftUnderTheId() throws Exception {
		UnitTestGroupTables.insertMember(securityDb, "team", MEMBER_ID, NATIVE);
		UnitTestGroupTables.insertManager(securityDb, "team", "formermanager", NATIVE);

		groups.addGroup(admin, "team", CUSTOM, "desc");

		assertEquals(List.of(ADMIN_MANAGER), UnitTestGroupTables.managers(securityDb, "team"));
		assertTrue(UnitTestGroupTables.members(securityDb, "team").isEmpty());
	}

	@Test
	void addGroup_ofAnotherTypeLeavesACustomGroupWithTheSameIdAlone() throws Exception {
		groups.addGroup(admin, "shared", CUSTOM, "desc");
		groups.addUserToGroup(admin, "shared", MEMBER_ID, NATIVE, null);

		groups.addGroup(admin, "shared", MICROSOFT, "desc");

		assertEquals(List.of(ADMIN_MANAGER), UnitTestGroupTables.managers(securityDb, "shared"));
		assertEquals(List.of(MEMBER_ROW), UnitTestGroupTables.members(securityDb, "shared"));
	}

	///
	/// editGroupDetailsAndPropagate
	///

	@Test
	void editGroupDetailsAndPropagate_movesACustomGroupsMembersAndManagers() throws Exception {
		createCustomGroupWithMemberAndSecondManager("old");

		groups.editGroupDetailsAndPropagate(admin, "old", CUSTOM, "new", "renamed");

		assertFalse(groups.groupExists("old", CUSTOM));
		assertTrue(groups.groupExists("new", CUSTOM));
		assertEquals(List.of(ADMIN_MANAGER, MEMBER_ROW), UnitTestGroupTables.managers(securityDb, "new"));
		assertEquals(List.of(MEMBER_ROW), UnitTestGroupTables.members(securityDb, "new"));
		assertTrue(UnitTestGroupTables.managers(securityDb, "old").isEmpty());
		assertTrue(UnitTestGroupTables.members(securityDb, "old").isEmpty());
		assertTrue(SecurityGroupManagerUtils.userIsGroupManager(member, "new"));
	}

	@Test
	void editGroupDetailsAndPropagate_dropsRowsLeftUnderTheNewId() throws Exception {
		createCustomGroupWithMemberAndSecondManager("old");
		UnitTestGroupTables.insertManager(securityDb, "new", STRAY_ID, NATIVE);
		UnitTestGroupTables.insertMember(securityDb, "new", STRAY_ID, NATIVE);

		groups.editGroupDetailsAndPropagate(admin, "old", CUSTOM, "new", "renamed");

		assertEquals(List.of(ADMIN_MANAGER, MEMBER_ROW), UnitTestGroupTables.managers(securityDb, "new"));
		assertEquals(List.of(MEMBER_ROW), UnitTestGroupTables.members(securityDb, "new"));
	}

	@Test
	void editGroupDetailsAndPropagate_ofAnotherTypeDoesNotMoveTheCustomRows() throws Exception {
		createCustomGroupWithMemberAndSecondManager("shared");
		groups.addGroup(admin, "shared", MICROSOFT, "desc");

		groups.editGroupDetailsAndPropagate(admin, "shared", MICROSOFT, "renamed", "desc");

		assertTrue(groups.groupExists("renamed", MICROSOFT));
		assertTrue(groups.groupExists("shared", CUSTOM));
		assertEquals(List.of(ADMIN_MANAGER, MEMBER_ROW), UnitTestGroupTables.managers(securityDb, "shared"));
		assertEquals(List.of(MEMBER_ROW), UnitTestGroupTables.members(securityDb, "shared"));
		assertTrue(UnitTestGroupTables.managers(securityDb, "renamed").isEmpty());
		assertTrue(UnitTestGroupTables.members(securityDb, "renamed").isEmpty());
	}

	@Test
	void editGroupDetailsAndPropagate_descriptionOnlyKeepsTheRows() throws Exception {
		createCustomGroupWithMemberAndSecondManager("team");

		groups.editGroupDetailsAndPropagate(admin, "team", CUSTOM, "team", "new description");

		assertEquals("new description", groups.getGroupDetails("team", CUSTOM).get("description"));
		assertEquals(List.of(ADMIN_MANAGER, MEMBER_ROW), UnitTestGroupTables.managers(securityDb, "team"));
		assertEquals(List.of(MEMBER_ROW), UnitTestGroupTables.members(securityDb, "team"));
	}

	///
	/// deleteGroupAndPropagate
	///

	@Test
	void deleteGroupAndPropagate_customDeletesMembersAndManagers() throws Exception {
		createCustomGroupWithMemberAndSecondManager("team");

		groups.deleteGroupAndPropagate("team", CUSTOM);

		assertFalse(groups.groupExists("team", CUSTOM));
		assertTrue(UnitTestGroupTables.managers(securityDb, "team").isEmpty());
		assertTrue(UnitTestGroupTables.members(securityDb, "team").isEmpty());
		assertFalse(SecurityGroupManagerUtils.userIsGroupManager(admin, "team"));
	}

	@Test
	void deleteGroupAndPropagate_ofAnotherTypeLeavesTheCustomRowsAlone() throws Exception {
		createCustomGroupWithMemberAndSecondManager("shared");
		groups.addGroup(admin, "shared", MICROSOFT, "desc");

		groups.deleteGroupAndPropagate("shared", MICROSOFT);

		assertFalse(groups.groupExists("shared", MICROSOFT));
		assertTrue(groups.groupExists("shared", CUSTOM));
		assertEquals(List.of(ADMIN_MANAGER, MEMBER_ROW), UnitTestGroupTables.managers(securityDb, "shared"));
		assertEquals(List.of(MEMBER_ROW), UnitTestGroupTables.members(securityDb, "shared"));
	}

	///
	/// getGroupMembers and getNonGroupMembers projections
	///

	@Test
	void getGroupMembers_returnsOnlyTheMemberKeys() throws Exception {
		groups.addGroup(admin, "team", CUSTOM, "desc");
		groups.addUserToGroup(admin, "team", MEMBER_ID, NATIVE, null);

		List<Map<String, Object>> members = groups.getGroupMembers("team", null, 0, 0);

		assertEquals(1, members.size());
		Map<String, Object> row = members.getFirst();
		assertEquals(MEMBER_KEYS, row.keySet());
		assertNoRemovedKeys(row);
		assertEquals(MEMBER_ID, row.get("userid"));
		assertEquals(NATIVE, row.get("type"));
		assertEquals("membername", row.get("name"));
		assertEquals(MEMBER_ID, row.get("username"));
		assertEquals("member@test.com", row.get("email"));
		assertNotNull(row.get("dateadded"));
	}

	@Test
	void getNonGroupMembers_returnsOnlyTheUserKeys() throws Exception {
		groups.addGroup(admin, "team", CUSTOM, "desc");
		groups.addUserToGroup(admin, "team", ADMIN_ID, NATIVE, null);

		List<Map<String, Object>> nonMembers = groups.getNonGroupMembers("team", null, 0, 0);

		assertEquals(1, nonMembers.size());
		Map<String, Object> row = nonMembers.getFirst();
		assertEquals(NON_MEMBER_KEYS, row.keySet());
		assertNoRemovedKeys(row);
		assertEquals(MEMBER_ID, row.get("id"));
		assertEquals("member@test.com", row.get("email"));
	}

	///
	/// addMemberCounts
	///

	@Test
	void getGroups_addsMemberCountsToCustomGroupsOnly() throws Exception {
		groups.addGroup(admin, "team", CUSTOM, "desc");
		groups.addGroup(admin, "empty", CUSTOM, "desc");
		groups.addGroup(admin, "msgroup", MICROSOFT, "desc");
		groups.addUserToGroup(admin, "team", MEMBER_ID, NATIVE, null);
		groups.addUserToGroup(admin, "team", ADMIN_ID, NATIVE, null);

		Map<Object, Map<String, Object>> byId = groups.getGroups(null, 0, 0).stream()
				.collect(Collectors.toMap(row -> row.get("id"), Function.identity()));

		assertEquals(2L, byId.get("team").get("member_count"));
		assertEquals(0L, byId.get("empty").get("member_count"));
		assertFalse(byId.get("msgroup").containsKey("member_count"));
	}

	@Test
	void addMemberCounts_countsOnlyMembersWithAMatchingUser() throws Exception {
		groups.addGroup(admin, "team", CUSTOM, "desc");
		groups.addUserToGroup(admin, "team", MEMBER_ID, NATIVE, null);
		UnitTestGroupTables.insertMember(securityDb, "team", "deleteduser", NATIVE);
		UnitTestGroupTables.insertMember(securityDb, "team", MEMBER_ID, MICROSOFT);

		List<Map<String, Object>> rows = List.of(groupRow("team", CUSTOM));
		AdminSecurityGroupUtils.addMemberCounts(rows);

		assertEquals(1L, rows.getFirst().get("member_count"));
	}

	@Test
	void addMemberCounts_skipsRowsThatAreNotCustomOrHaveNoId() throws Exception {
		groups.addGroup(admin, "team", CUSTOM, "desc");
		groups.addUserToGroup(admin, "team", MEMBER_ID, NATIVE, null);
		Map<String, Object> noId = new HashMap<>();
		noId.put("type", CUSTOM);

		List<Map<String, Object>> rows = List.of(groupRow("team", CUSTOM), groupRow("team", MICROSOFT),
				groupRow("unknown", CUSTOM), noId);
		AdminSecurityGroupUtils.addMemberCounts(rows);

		assertEquals(1L, rows.get(0).get("member_count"));
		assertFalse(rows.get(1).containsKey("member_count"));
		assertEquals(0L, rows.get(2).get("member_count"));
		assertFalse(noId.containsKey("member_count"));
	}

	@Test
	void addMemberCounts_acceptsAnEmptyOrNonCustomList() {
		List<Map<String, Object>> nonCustom = List.of(groupRow("msgroup", MICROSOFT));

		assertDoesNotThrow(() -> AdminSecurityGroupUtils.addMemberCounts(new ArrayList<>()));
		AdminSecurityGroupUtils.addMemberCounts(nonCustom);
		assertFalse(nonCustom.getFirst().containsKey("member_count"));
	}

	private void createCustomGroupWithMemberAndSecondManager(String groupId) throws Exception {
		groups.addGroup(admin, groupId, CUSTOM, "desc");
		groups.addUserToGroup(admin, groupId, MEMBER_ID, NATIVE, null);
		UnitTestGroupTables.insertManager(securityDb, groupId, MEMBER_ID, NATIVE);
	}

	private static Map<String, Object> groupRow(String id, String type) {
		Map<String, Object> row = new HashMap<>();
		row.put("id", id);
		row.put("type", type);
		return row;
	}

	private static void assertNoRemovedKeys(Map<String, Object> row) {
		for (String key : REMOVED_KEYS) {
			assertFalse(row.containsKey(key), "unexpected key " + key);
		}
	}
}
