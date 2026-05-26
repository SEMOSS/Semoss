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

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.date.SemossDate;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.SystemEngineRegistry;

public class AdminSecurityGroupUtilsUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private IRDBMSEngine securityDb;
	private List<String> tables = new ArrayList<>();

	private User adminUser;
	private AdminSecurityGroupUtils instance;

	@BeforeEach
	void setup() {
		securityDb = SystemEngineRegistry.getSecurityDb();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		assertNotNull(this.securityDb);

		adminUser = UnitTestSecurityAuthUtils.createUser("admin", true);
		instance = AdminSecurityGroupUtils.getInstance(adminUser);
	}

	@AfterEach
	void cleanup() throws SQLException {
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		// clear test database inside of temp directory
		// quicker than deleting and recreating
		tables = UnitTestSecurityAuthUtils.clearSecurityDB(securityDb, tables);
	}

	///
	/// getInstance
	///
	@Test
	void testGetInstance_userNull() {
		assertNull(AdminSecurityGroupUtils.getInstance(null));
	}

	@Test
	void testGetInstance_userNotAdmin() {
		User nonAdmin = UnitTestSecurityAuthUtils.createUser("nonAdmin", false);
		assertNull(AdminSecurityGroupUtils.getInstance(nonAdmin));
	}

	@Test
	void testGetInstance_userAdmin() {
		assertNotNull(AdminSecurityGroupUtils.getInstance(adminUser));
	}

	///
	/// getMatchingGroupsByType
	///
	@Test
	void testGetMatchingGroupsByType_groupTypeWrong() throws Exception {
		instance.addGroup(adminUser, "gid", "gtype", "desc");

		Collection<String> groupIds = new ArrayList<>();
		groupIds.add("gid");
		Set<String> rets = AdminSecurityGroupUtils.getMatchingGroupsByType(groupIds, "wrong");
		assertTrue(rets.isEmpty());
	}

	@Test
	void testGetMatchingGroupsByType_groupIdWrong() throws Exception {
		instance.addGroup(adminUser, "gid", "gtype", "desc");

		Collection<String> groupIds = new ArrayList<>();
		groupIds.add("wrong");
		Set<String> rets = AdminSecurityGroupUtils.getMatchingGroupsByType(groupIds, "gtype");
		assertTrue(rets.isEmpty());
	}

	///
	/// addGroup
	///
	@Test
	void testAddGroup_successful() throws Exception {
		instance.addGroup(adminUser, "gid", "gtype", "desc");

		Collection<String> groupIds = new ArrayList<>();
		groupIds.add("gid");
		Set<String> rets = AdminSecurityGroupUtils.getMatchingGroupsByType(groupIds, "gtype");
		assertEquals(1, rets.size());
		assertTrue(rets.stream().findFirst().isPresent());
		assertEquals("gid", rets.stream().findFirst().get());
	}

	@Test
	void testAddGroup_alreadyExists() throws Exception {
		instance.addGroup(adminUser, "gid", "gtype", "desc");

		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
			instance.addGroup(adminUser, "gid", "gtype", "desc");
		});

		assertEquals("Group gid with type gtype already exists", ex.getMessage());
	}

	///
	/// deleteGroupAndPropogate
	///
	@Test
	void testDeleteGroupAndPropgoate_groupDoesNotExist() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> {
			instance.deleteGroupAndPropagate("wrong", "wrong");
		});
		assertEquals("Group wrong does not exist", e.getMessage());
	}

	@Test
	void testDeleteGroupAndPropogate_CustomGroup() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");

		UnitTestSecurityAuthUtils.addUserToGroup(adminUser, "gid", "adminid", AuthProvider.NATIVE.getLabel());

		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", adminUser);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", adminUser);

		// add group permissions
		String endDate = ZonedDateTime.now().plusDays(2).toString();
		SecurityGroupProjectUtils.addProjectGroupPermission(adminUser, "gid", "CUSTOM", "pid1", "EDIT", endDate);
		SecurityGroupEngineUtils.addEngineGroupPermission(adminUser, "gid", "CUSTOM", "eid1", "EDIT", endDate);

		// delete all
		instance.deleteGroupAndPropagate("gid", "CUSTOM");

		assertFalse(instance.groupExists("gid", "CUSTOM"));
		assertNull(SecurityGroupEngineUtils.getGroupDatabasePermission("gid", "CUSTOM", "eid1"));
		assertNull(SecurityGroupProjectUtils.getGroupProjectPermission("gid", "CUSTOM", "pid1"));

		List<String> userCustomGroups = AdminSecurityGroupUtils
				.getUserCustomGroups(adminUser.getAccessToken(AuthProvider.NATIVE));
		assertEquals(0, userCustomGroups.size());
	}

	@Test
	void testDeleteGroupAndPropogate_nonCustomGroup() throws Exception {
		instance.addGroup(adminUser, "gid", "reg", "desc");

		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", adminUser);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", adminUser);

		// add group permissions
		String endDate = ZonedDateTime.now().plusDays(2).toString();
		SecurityGroupProjectUtils.addProjectGroupPermission(adminUser, "gid", "reg", "pid1", "EDIT", endDate);
		SecurityGroupEngineUtils.addEngineGroupPermission(adminUser, "gid", "reg", "eid1", "EDIT", endDate);

		// delete all
		instance.deleteGroupAndPropagate("gid", "reg");

		assertFalse(instance.groupExists("gid", "reg"));
		assertNull(SecurityGroupEngineUtils.getGroupDatabasePermission("gid", "reg", "eid1"));
		assertNull(SecurityGroupProjectUtils.getGroupProjectPermission("gid", "reg", "pid1"));
	}

	///
	/// editGroupAndPropogate
	/// deprecated so only supporting exception thrown
	///

	@Test
	void testEditGroupAndPropogate_groupDoesNotExist() {
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
			instance.editGroupAndPropagate(adminUser, "cur", "type", "new", "newType", "newDesc");
		});
		assertEquals("Group cur does not exist", ex.getMessage());
	}

	///
	/// editGroupDetailsAndPropagate
	///

	@Test
	void testEditGroupDetailsAndPropagate_groupDoesNotExist() {
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
			instance.editGroupDetailsAndPropagate(adminUser, "cur", "type", "new", "newDesc");
		});

		assertEquals("Group cur does not exist", ex.getMessage());
	}

	@Test
	void testEditGroupDetailsAndPropagate_curGroupEqualsNewGroup() throws Exception {
		instance.addGroup(adminUser, "new", "CUSTOM", "desc");
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
				() -> instance.editGroupDetailsAndPropagate(adminUser, "cur", "CUSTOM", "new", "newDesc"));

		assertEquals("Group cur does not exist", ex.getMessage());
	}

	@Test
	void testEditGroupDetailsAndPropagate_successful() throws Exception {
		instance.addGroup(adminUser, "oldGroupId", "CUSTOM", "desc");

		UnitTestSecurityAuthUtils.addUserToGroup(adminUser, "oldGroupId", "adminid", AuthProvider.NATIVE.getLabel());

		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", adminUser);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", adminUser);

		// add group permissions
		String endDate = ZonedDateTime.now().plusDays(2).toString();
		SecurityGroupProjectUtils.addProjectGroupPermission(adminUser, "oldGroupId", "CUSTOM", "pid1", "EDIT", endDate);
		SecurityGroupEngineUtils.addEngineGroupPermission(adminUser, "oldGroupId", "CUSTOM", "eid1", "EDIT", endDate);

		// edit
		instance.editGroupDetailsAndPropagate(adminUser, "oldGroupId", "CUSTOM", "newGroupId", "newDesc");

		// assert old doesnt work
		assertFalse(instance.groupExists("oldGroupId", "CUSTOM"));
		assertNull(SecurityGroupEngineUtils.getGroupDatabasePermission("oldGroupId", "CUSTOM", "eid1"));
		assertNull(SecurityGroupProjectUtils.getGroupProjectPermission("oldGroupId", "CUSTOM", "pid1"));

		// assert new does
		assertTrue(instance.groupExists("newGroupId", "CUSTOM"));
		assertEquals(2, SecurityGroupEngineUtils.getGroupDatabasePermission("newGroupId", "CUSTOM", "eid1"));
		assertEquals(2, SecurityGroupProjectUtils.getGroupProjectPermission("newGroupId", "CUSTOM", "pid1"));

		List<String> userCustomGroups = AdminSecurityGroupUtils
				.getUserCustomGroups(adminUser.getAccessToken(AuthProvider.NATIVE));
		assertEquals(1, userCustomGroups.size());
		assertEquals("newGroupId", userCustomGroups.getFirst());
	}

	///
	/// addUserToGroup
	///

	@Test
	void testAddUserToGroup_groupDoesNotExist() {
		String endDate = ZonedDateTime.now().plusDays(2).toString();
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
				() -> instance.addUserToGroup(adminUser, "gid", "adminid", "NATIVE", endDate));
		assertEquals("Group gid does not exist", ex.getMessage());
	}

	@Test
	void testAddUserToGroup_UserAlreadyInCustomGroup() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		instance.addUserToGroup(adminUser, "gid", "adminid", "NATIVE", endDate);

		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
				() -> instance.addUserToGroup(adminUser, "gid", "adminid", "NATIVE", endDate));
		assertEquals("User adminid already has access to group gid", ex.getMessage());
	}

	@Test
	void testAddUserToGroup_UserDoesNotExist() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");

		String endDate = ZonedDateTime.now().plusDays(2).toString();

		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
				() -> instance.addUserToGroup(adminUser, "gid", "wrong", "NATIVE", endDate));
		assertEquals("User wrong does not exist", ex.getMessage());
	}

	@Test
	void testAddUserToGroup_Successful() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		instance.addUserToGroup(adminUser, "gid", "adminid", "NATIVE", endDate);

		List<Map<String, Object>> groupMembers = instance.getGroupMembers("gid", null, 0, 0);
		assertEquals(1, groupMembers.size());
		Map<String, Object> groupMember = groupMembers.get(0);
		assertEquals("adminname", groupMember.get("name"));

		SemossDate ed = (SemossDate) groupMember.get("enddate");
		LocalDateTime eldt = ed.getLocalDateTime();

		// assert end date is 2 days later as specified
		assertTrue(LocalDateTime.now().plusDays(1).isBefore(eldt));
		assertTrue(LocalDateTime.now().plusDays(3).isAfter(eldt));
	}

	@Test
	void testAddUserToGroup_EndDateNull_Successful() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");

		instance.addUserToGroup(adminUser, "gid", "adminid", "NATIVE", null);

		List<Map<String, Object>> groupMembers = instance.getGroupMembers("gid", null, 0, 0);
		assertEquals(1, groupMembers.size());
		Map<String, Object> groupMember = groupMembers.get(0);
		assertEquals("adminname", groupMember.get("name"));

		assertNull(groupMember.get("enddate"));
	}

	///
	/// removeUserFromGroup
	///

	@Test
	void testRemoveUserToGroup_groupDoesNotExist() {
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
				() -> instance.removeUserFromGroup("gid", "adminid", "NATIVE"));
		assertEquals("Group gid does not exist", ex.getMessage());
	}

	@Test
	void testRemoveUserToGroup_UserNotInCustomGroup() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");

		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
				() -> instance.removeUserFromGroup("gid", "adminid", "NATIVE"));
		assertEquals("User adminid does not have access to group gid", ex.getMessage());
	}

	@Test
	void testRemoveUserFromGroup_Successful() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		instance.addUserToGroup(adminUser, "gid", "adminid", "NATIVE", endDate);

		List<Map<String, Object>> groupMembers = instance.getGroupMembers("gid", null, 0, 0);
		assertEquals(1, groupMembers.size());

		instance.removeUserFromGroup("gid", "adminid", "NATIVE");

		groupMembers = instance.getGroupMembers("gid", null, 0, 0);
		assertEquals(0, groupMembers.size());
	}

	///
	/// getGroups
	///

	@Test
	void testGetGroups_successful() throws Exception {
		instance.addGroup(adminUser, "foo1", "CUSTOM", "desc");
		instance.addGroup(adminUser, "foo2", "CUSTOM", "desc");
		instance.addGroup(adminUser, "bar1", "CUSTOM", "desc");
		instance.addGroup(adminUser, "bar2", "CUSTOM", "desc");

		List<Map<String, Object>> groups = instance.getGroups("foo", 1, 1);
		assertEquals(1, groups.size());
		assertEquals("foo2", groups.getFirst().get("id"));
	}

	///
	/// getGroupMembers
	///

	@Test
	void testGetGroupMembers_groupDoesNotExist() {
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
				() -> instance.getGroupMembers("gid", null, 0, 0));
		assertEquals("Group gid with type custom does not exist", ex.getMessage());
	}

	@Test
	void testGetGroupMembers_Successful() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		instance.addUserToGroup(adminUser, "gid", "adminid", "NATIVE", endDate);

		UnitTestSecurityAuthUtils.createUser("b2", false);
		UnitTestSecurityAuthUtils.createUser("c3", false);
		UnitTestSecurityAuthUtils.createUser("d4", false);

		instance.addUserToGroup(adminUser, "gid", "b2id", "NATIVE", endDate);
		instance.addUserToGroup(adminUser, "gid", "c3id", "NATIVE", endDate);
		instance.addUserToGroup(adminUser, "gid", "d4id", "NATIVE", endDate);

		List<Map<String, Object>> groupMembers = instance.getGroupMembers("gid", "id", 2, 2);

		assertEquals(2, groupMembers.size());
		assertEquals("c3id", groupMembers.getFirst().get("userid"));
		assertEquals("d4id", groupMembers.getLast().get("userid"));
	}

	@Test
	void testGetGroupMembers_searchByEmail_Successful() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		instance.addUserToGroup(adminUser, "gid", "adminid", "NATIVE", endDate);

		UnitTestSecurityAuthUtils.createUser("b2", false);
		UnitTestSecurityAuthUtils.createUser("c3", false);
		UnitTestSecurityAuthUtils.createUser("d4", false);

		instance.addUserToGroup(adminUser, "gid", "b2id", "NATIVE", endDate);
		instance.addUserToGroup(adminUser, "gid", "c3id", "NATIVE", endDate);
		instance.addUserToGroup(adminUser, "gid", "d4id", "NATIVE", endDate);

		// testing the like functionality on email
		List<Map<String, Object>> groupMembers = instance.getGroupMembers("gid", "2@test.com", 0, 0);

		assertEquals(1, groupMembers.size());
		assertEquals("b2id", groupMembers.getFirst().get("userid"));
	}

	///
	/// getNumMembersInGroup
	///

	@Test
	void testGetNumMembersInGroup_groupDoesNotExist() {
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
				() -> instance.getNumMembersInGroup("gid", null));
		assertEquals("Group gid with type custom does not exist", ex.getMessage());
	}

	@Test
	void testGetNumMembersInGroup_NoSearchTerm() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		instance.addUserToGroup(adminUser, "gid", "adminid", "NATIVE", endDate);

		UnitTestSecurityAuthUtils.createUser("b2", false);
		UnitTestSecurityAuthUtils.createUser("c3", false);
		UnitTestSecurityAuthUtils.createUser("d4", false);

		instance.addUserToGroup(adminUser, "gid", "b2id", "NATIVE", endDate);
		instance.addUserToGroup(adminUser, "gid", "c3id", "NATIVE", endDate);
		instance.addUserToGroup(adminUser, "gid", "d4id", "NATIVE", endDate);

		Long num = instance.getNumMembersInGroup("gid", null);
		assertEquals(4, num.intValue());
	}

	@Test
	void testGetNumMembersInGroup_SearchForUser() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		instance.addUserToGroup(adminUser, "gid", "adminid", "NATIVE", endDate);

		UnitTestSecurityAuthUtils.createUser("b2", false);
		UnitTestSecurityAuthUtils.createUser("c3", false);
		UnitTestSecurityAuthUtils.createUser("d4", false);

		instance.addUserToGroup(adminUser, "gid", "b2id", "NATIVE", endDate);
		instance.addUserToGroup(adminUser, "gid", "c3id", "NATIVE", endDate);
		instance.addUserToGroup(adminUser, "gid", "d4id", "NATIVE", endDate);

		Long num = instance.getNumMembersInGroup("gid", "c3");
		assertEquals(1, num.intValue());
	}

	///
	/// GetNonGroupMembers
	///

	@Test
	void testGetNonGroupMembers_groupDoesNotExist() {
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
				() -> instance.getNonGroupMembers("gid", null, 0, 0));
		assertEquals("Group gid with type custom does not exist", ex.getMessage());
	}

	@Test
	void testGetNonGroupMembers_NoSearchTerm() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		instance.addUserToGroup(adminUser, "gid", "adminid", "NATIVE", endDate);

		UnitTestSecurityAuthUtils.createUser("b2", false);
		UnitTestSecurityAuthUtils.createUser("c3", false);
		UnitTestSecurityAuthUtils.createUser("d4", false);

		List<Map<String, Object>> nonGroupMembers = instance.getNonGroupMembers("gid", null, 0, 0);

		assertEquals(3, nonGroupMembers.size());
		assertEquals(0, nonGroupMembers.stream().map(s -> s.get("id")).filter(s -> s.equals("adminid")).count());

	}

	@Test
	void testGetNonGroupMembers_search() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		instance.addUserToGroup(adminUser, "gid", "adminid", "NATIVE", endDate);

		UnitTestSecurityAuthUtils.createUser("b2", false);
		UnitTestSecurityAuthUtils.createUser("c3", false);
		UnitTestSecurityAuthUtils.createUser("d4", false);

		List<Map<String, Object>> nonGroupMembers = instance.getNonGroupMembers("gid", "b2", 0, 0);

		assertEquals(1, nonGroupMembers.size());
		assertEquals("b2id", nonGroupMembers.get(0).get("id"));
	}

	///
	/// getNumNonMembersInGroup
	///

	@Test
	void testGetNumNonMembersInGroup_groupDoesNotExist() {
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
				() -> instance.getNumNonMembersInGroup("gid", null));
		assertEquals("Group gid with type custom does not exist", ex.getMessage());
	}

	@Test
	void testGetNumNonMembersInGroup_NoSearchTerm() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		instance.addUserToGroup(adminUser, "gid", "adminid", "NATIVE", endDate);

		UnitTestSecurityAuthUtils.createUser("b2", false);
		UnitTestSecurityAuthUtils.createUser("c3", false);
		UnitTestSecurityAuthUtils.createUser("d4", false);

		long nonGroupMembers = instance.getNumNonMembersInGroup("gid", null);

		assertEquals(3, nonGroupMembers);
	}

	@Test
	void testGetNumNonMembersInGroup_search() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		instance.addUserToGroup(adminUser, "gid", "adminid", "NATIVE", endDate);

		UnitTestSecurityAuthUtils.createUser("b2", false);
		UnitTestSecurityAuthUtils.createUser("c3", false);
		UnitTestSecurityAuthUtils.createUser("d4", false);

		long nonMembers = instance.getNumNonMembersInGroup("gid", "b2");

		assertEquals(1, nonMembers);
	}

	///
	/// addGroupProjectPermission
	///

	@Test
	void testAddGroupProjectPermission_groupDoesNotExist() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> instance.addGroupProjectPermission(adminUser, "nope", "CUSTOM", "pid1", 1, null));
		assertEquals("Group nope with type CUSTOM does not exist", e.getMessage());
	}

	@Test
	void testAddGroupProjectPermission_GroupAlreadyHasPermission() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", adminUser);
		instance.addGroupProjectPermission(adminUser, "gid", "CUSTOM", "pid1", 1, null);

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> instance.addGroupProjectPermission(adminUser, "gid", "CUSTOM", "pid1", 1, null));
		assertEquals("Group gid already has access to project pid1 with permission = OWNER", e.getMessage());
	}

	@Test
	void testAddGroupProjectPermission_Successful() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", adminUser);
		instance.addGroupProjectPermission(adminUser, "gid", "CUSTOM", "pid1", 1, null);

		assertEquals(1, instance.groupProjectPermission("gid", "CUSTOM", "pid1"));
	}

	///
	/// editGroupProjectPermission
	///

	@Test
	void testEditGroupProjectPermission_groupDoesNotExist() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> instance.editGroupProjectPermission(adminUser, "nope", "CUSTOM", "pid1", 1, null));
		assertEquals("Group nope does not currently have access to project pid1 to edit", e.getMessage());
	}

	@Test
	void testEditGroupProjectPermission_samePermissionAsBefore() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", adminUser);
		instance.addGroupProjectPermission(adminUser, "gid", "CUSTOM", "pid1", 1, null);

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> instance.editGroupProjectPermission(adminUser, "gid", "CUSTOM", "pid1", 1, null));
		assertEquals("Group gid already has permission level OWNER to project pid1", e.getMessage());
	}

	@Test
	void testEditGroupProjectPermission_Successful() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", adminUser);
		instance.addGroupProjectPermission(adminUser, "gid", "CUSTOM", "pid1", 2, null);
		assertEquals(2, instance.groupProjectPermission("gid", "CUSTOM", "pid1"));

		instance.editGroupProjectPermission(adminUser, "gid", "CUSTOM", "pid1", 1, null);
		assertEquals(1, instance.groupProjectPermission("gid", "CUSTOM", "pid1"));
	}

	///
	/// removeGroupProjectPermission
	///

	@Test
	void testRemoveGroupProjectPermission_groupDoesNotExist() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> instance.removeGroupProjectPermission(adminUser, "nope", "CUSTOM", "pid1"));
		assertEquals("Group nope does not currently have access to project pid1 to remove", e.getMessage());
	}

	@Test
	void testRemoveGroupProjectPermission_Successful() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", adminUser);
		instance.addGroupProjectPermission(adminUser, "gid", "CUSTOM", "pid1", 1, null);

		instance.removeGroupProjectPermission(adminUser, "gid", "CUSTOM", "pid1");
		assertEquals(-1, instance.groupProjectPermission("gid", "CUSTOM", "pid1"));
	}

	///
	/// getProjectsForGroup
	///

	@Test
	void testGetProjectsForGroup_groupDoesNotExist() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> instance.getProjectsForGroup("nope", "CUSTOM", null, 0, 0, false));
		assertEquals("Group nope with type CUSTOM does not exist", e.getMessage());
	}

	@Test
	void testGetProjectsForGroup_searchLimitAndOffset() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");
		instance.addGroup(adminUser, "gid2", "CUSTOM", "desc");

		UnitTestSecurityAuthUtils.createProject("pid", "pname", adminUser);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", adminUser);
		UnitTestSecurityAuthUtils.createProject("projectid1", "projectname1", adminUser);
		UnitTestSecurityAuthUtils.createProject("pid2", "pname2", adminUser);
		UnitTestSecurityAuthUtils.createProject("pid3", "pname3", adminUser);

		instance.addGroupProjectPermission(adminUser, "gid2", "CUSTOM", "pid", 1, null);
		instance.addGroupProjectPermission(adminUser, "gid", "CUSTOM", "pid1", 1, null);
		instance.addGroupProjectPermission(adminUser, "gid", "CUSTOM", "projectid1", 1, null);
		instance.addGroupProjectPermission(adminUser, "gid", "CUSTOM", "pid2", 1, null);
		instance.addGroupProjectPermission(adminUser, "gid", "CUSTOM", "pid3", 1, null);

		List<Map<String, Object>> values = instance.getProjectsForGroup("gid", "CUSTOM", "pid", 1, 1, false);

		// make sure that the only value we get back is the one for the group id, that
		// starts with pid,
		// that is offset by 1
		assertEquals(1, values.size());
		Map<String, Object> map = values.getFirst();
		assertEquals("pid2", map.get("project_id"));
	}

	@Test
	void testGetProjectsForGroup_onlyApps() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");

		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", adminUser);

		instance.addGroupProjectPermission(adminUser, "gid", "CUSTOM", "pid1", 1, null);

		List<Map<String, Object>> values = instance.getProjectsForGroup("gid", "CUSTOM", null, 0, 0, true);

		// only apps being true filters out only value
		assertEquals(0, values.size());
	}

	///
	/// getNumProjectsFOrGroup
	///

	@Test
	void testGetNumProjectsForGroup_groupDoesNotExist() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> instance.getNumProjectsForGroup("nope", "CUSTOM", null, false));
		assertEquals("Group nope with type CUSTOM does not exist", e.getMessage());
	}

	@Test
	void testGetNumProjectsForGroup_searchLimitAndOffset() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");
		instance.addGroup(adminUser, "gid2", "CUSTOM", "desc");

		UnitTestSecurityAuthUtils.createProject("pid", "pname", adminUser);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", adminUser);
		UnitTestSecurityAuthUtils.createProject("projectid1", "projectname1", adminUser);
		UnitTestSecurityAuthUtils.createProject("pid2", "pname2", adminUser);
		UnitTestSecurityAuthUtils.createProject("pid3", "pname3", adminUser);

		instance.addGroupProjectPermission(adminUser, "gid2", "CUSTOM", "pid", 1, null);
		instance.addGroupProjectPermission(adminUser, "gid", "CUSTOM", "pid1", 1, null);
		instance.addGroupProjectPermission(adminUser, "gid", "CUSTOM", "projectid1", 1, null);
		instance.addGroupProjectPermission(adminUser, "gid", "CUSTOM", "pid2", 1, null);
		instance.addGroupProjectPermission(adminUser, "gid", "CUSTOM", "pid3", 1, null);

		long count = instance.getNumProjectsForGroup("gid", "CUSTOM", "pid", false);

		// make sure its 3, pid1, pid2, pid3 are correct
		assertEquals(3, count);
	}

	@Test
	void testGetNumProjectsForGroup_onlyApps() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");

		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", adminUser);

		instance.addGroupProjectPermission(adminUser, "gid", "CUSTOM", "pid1", 1, null);

		long count = instance.getNumProjectsForGroup("gid", "CUSTOM", null, true);

		// only apps being true filters out only value
		assertEquals(0, count);
	}

	///
	/// getAvailableProjectsForGroup
	///
	@Test
	void testGetAvailableProjectsFroGroup_groupDoesNotExist() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> instance.getAvailableProjectsForGroup("nope", "CUSTOM", null, 0, 0, false));
		assertEquals("Group nope with type CUSTOM does not exist", e.getMessage());
	}

	@Test
	void testGetAvailableProjectsForGroup_searchLimitAndOffset() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");
		instance.addGroup(adminUser, "gid2", "CUSTOM", "desc");

		UnitTestSecurityAuthUtils.createProject("pid", "pname", adminUser);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", adminUser);
		UnitTestSecurityAuthUtils.createProject("projectid1", "projectname1", adminUser);
		UnitTestSecurityAuthUtils.createProject("pid2", "pname2", adminUser);
		UnitTestSecurityAuthUtils.createProject("pid3", "pname3", adminUser);

		instance.addGroupProjectPermission(adminUser, "gid2", "CUSTOM", "pid", 1, null);
		instance.addGroupProjectPermission(adminUser, "gid", "CUSTOM", "pid1", 1, null);

		List<Map<String, Object>> values = instance.getAvailableProjectsForGroup("gid", "CUSTOM", "pid", 0, 0, false);

		// filter out pid1 and projectid1, limit to one and offset to 1 to pid3.
		assertEquals(3, values.size());
		Map<String, Object> map = values.getFirst();
		assertEquals("pid", map.get("project_id"));
		Map<String, Object> map2 = values.get(1);
		assertEquals("pid2", map2.get("project_id"));
		Map<String, Object> map3 = values.get(2);
		assertEquals("pid3", map3.get("project_id"));

		// limit and offset check
		values = instance.getAvailableProjectsForGroup("gid", "CUSTOM", "pid", 1, 1, false);
		assertEquals(1, values.size());
		map = values.getFirst();
		assertEquals("pid2", map.get("project_id"));
	}

	@Test
	void testGetAvailableProjectsForGroup_onlyApps() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");

		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", adminUser);
		UnitTestSecurityAuthUtils.createProject("pid2", "pname2", adminUser, true);

		List<Map<String, Object>> values = instance.getAvailableProjectsForGroup("gid", "CUSTOM", null, 0, 0, true);

		// only apps being true filters out only value
		assertEquals(1, values.size());
		Map<String, Object> map = values.getFirst();
		assertEquals("pid2", map.get("project_id"));
	}

	///
	/// getNumAvailableProjectsForGroup
	///

	@Test
	void testGetNumAvailableProjectsFroGroup_groupDoesNotExist() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> instance.getNumAvailableProjectsForGroup("nope", "CUSTOM", null, false));
		assertEquals("Group nope with type CUSTOM does not exist", e.getMessage());
	}

	@Test
	void testNumGetAvailableProjectsForGroup_searchLimitAndOffset() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");
		instance.addGroup(adminUser, "gid2", "CUSTOM", "desc");

		UnitTestSecurityAuthUtils.createProject("pid", "pname", adminUser);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", adminUser);
		UnitTestSecurityAuthUtils.createProject("projectid1", "projectname1", adminUser);
		UnitTestSecurityAuthUtils.createProject("pid2", "pname2", adminUser);
		UnitTestSecurityAuthUtils.createProject("pid3", "pname3", adminUser);

		instance.addGroupProjectPermission(adminUser, "gid2", "CUSTOM", "pid", 1, null);
		instance.addGroupProjectPermission(adminUser, "gid", "CUSTOM", "pid1", 1, null);

		long count = instance.getNumAvailableProjectsForGroup("gid", "CUSTOM", "pid", false);

		// filter out pid1 and projectid1
		assertEquals(3, count);
	}

	@Test
	void testGetNumAvailableProjectsForGroup_onlyApps() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");

		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", adminUser);
		UnitTestSecurityAuthUtils.createProject("pid2", "pname2", adminUser, true);

		long count = instance.getNumAvailableProjectsForGroup("gid", "CUSTOM", null, true);

		// only apps being true filters out only value
		assertEquals(1, count);
	}

	///
	/// addGroupEnginePermission
	///

	@Test
	void testAddGroupEnginePermission_groupDoesNotExist() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> instance.addGroupEnginePermission(adminUser, "nope", "CUSTOM", "pid1", 1, null));
		assertEquals("Group nope with type CUSTOM does not exist", e.getMessage());
	}

	@Test
	void testAddGroupEnginePermission_GroupAlreadyHasPermission() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", adminUser);
		instance.addGroupEnginePermission(adminUser, "gid", "CUSTOM", "eid1", 1, null);

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> instance.addGroupEnginePermission(adminUser, "gid", "CUSTOM", "eid1", 1, null));
		assertEquals("Group gid already has access to engine eid1 with permission = OWNER", e.getMessage());
	}

	@Test
	void testAddGroupEnginePermission_Successful() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", adminUser);
		instance.addGroupEnginePermission(adminUser, "gid", "CUSTOM", "eid1", 1, null);

		assertEquals(1, instance.groupEnginePermission("gid", "CUSTOM", "eid1"));
	}

	///
	/// editGroupEnginePermission
	///

	@Test
	void testEditGroupEnginePermission_groupDoesNotExist() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> instance.editGroupEnginePermission(adminUser, "nope", "CUSTOM", "eid1", 1, null));
		assertEquals("Group nope does not currently have access to engine eid1 to edit", e.getMessage());
	}

	@Test
	void testEditGroupEnginePermission_samePermissionAsBefore() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", adminUser);
		instance.addGroupEnginePermission(adminUser, "gid", "CUSTOM", "eid1", 1, null);

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> instance.editGroupEnginePermission(adminUser, "gid", "CUSTOM", "eid1", 1, null));
		assertEquals("Group gid already has permission level OWNER to engine eid1", e.getMessage());
	}

	@Test
	void testEditGroupEnginePermission_Successful() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", adminUser);
		instance.addGroupEnginePermission(adminUser, "gid", "CUSTOM", "eid1", 2, null);
		assertEquals(2, instance.groupEnginePermission("gid", "CUSTOM", "eid1"));

		instance.editGroupEnginePermission(adminUser, "gid", "CUSTOM", "eid1", 1, null);
		assertEquals(1, instance.groupEnginePermission("gid", "CUSTOM", "eid1"));
	}

	///
	/// removeGroupEnginePermission
	///

	@Test
	void testRemoveGroupEnginePermission_groupDoesNotExist() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> instance.removeGroupEnginePermission(adminUser, "nope", "CUSTOM", "eid1"));
		assertEquals("Group nope does not currently have access to engine eid1 to remove", e.getMessage());
	}

	@Test
	void testRemoveGroupEnginePermission_Successful() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", adminUser);
		instance.addGroupEnginePermission(adminUser, "gid", "CUSTOM", "eid1", 1, null);

		instance.removeGroupEnginePermission(adminUser, "gid", "CUSTOM", "eid1");
		assertEquals(-1, instance.groupEnginePermission("gid", "CUSTOM", "eid1"));
	}

	///
	/// getEnginesForGroup
	///

	@Test
	void testGetEnginesForGroup_groupDoesNotExist() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> instance.getEnginesForGroup("nope", "CUSTOM", null, 0, 0));
		assertEquals("Group nope with type CUSTOM does not exist", e.getMessage());
	}

	@Test
	void testGetEnginesForGroup_searchLimitAndOffset() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");
		instance.addGroup(adminUser, "gid2", "CUSTOM", "desc");

		UnitTestSecurityAuthUtils.createEngine("eid", "ename", adminUser);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", adminUser);
		UnitTestSecurityAuthUtils.createEngine("foobar", "foobarname", adminUser);
		UnitTestSecurityAuthUtils.createEngine("eid2", "ename2", adminUser);
		UnitTestSecurityAuthUtils.createEngine("eid3", "ename3", adminUser);

		instance.addGroupEnginePermission(adminUser, "gid2", "CUSTOM", "eid", 1, null);
		instance.addGroupEnginePermission(adminUser, "gid", "CUSTOM", "eid1", 1, null);
		instance.addGroupEnginePermission(adminUser, "gid", "CUSTOM", "foobar", 1, null);
		instance.addGroupEnginePermission(adminUser, "gid", "CUSTOM", "eid2", 1, null);
		instance.addGroupEnginePermission(adminUser, "gid", "CUSTOM", "eid3", 1, null);

		List<Map<String, Object>> values = instance.getEnginesForGroup("gid", "CUSTOM", "eid", 0, 0);

		// make sure that the only value we get back is the one for the group id, that
		// starts with pid,
		// that is offset by 1
		assertEquals(3, values.size());
		Map<String, Object> map = values.getFirst();
		assertEquals("eid1", map.get("engine_id"));
		Map<String, Object> map2 = values.get(1);
		assertEquals("eid2", map2.get("engine_id"));
		Map<String, Object> map3 = values.get(2);
		assertEquals("eid3", map3.get("engine_id"));

		// use limit and offset
		values = instance.getEnginesForGroup("gid", "CUSTOM", "eid", 1, 1);
		assertEquals(1, values.size());
		map = values.getFirst();
		assertEquals("eid2", map.get("engine_id"));
	}

	///
	/// getNumEnginesForGroup
	///

	@Test
	void testGetNumEnginesForGroup_groupDoesNotExist() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> instance.getNumEnginesForGroup("nope", "CUSTOM", null));
		assertEquals("Group nope with type CUSTOM does not exist", e.getMessage());
	}

	@Test
	void testGetNumEnginesForGroup_searchLimitAndOffset() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");
		instance.addGroup(adminUser, "gid2", "CUSTOM", "desc");

		UnitTestSecurityAuthUtils.createEngine("eid", "ename", adminUser);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", adminUser);
		UnitTestSecurityAuthUtils.createEngine("foobar", "foobarname1", adminUser);
		UnitTestSecurityAuthUtils.createEngine("eid2", "ename2", adminUser);
		UnitTestSecurityAuthUtils.createEngine("eid3", "ename3", adminUser);

		instance.addGroupEnginePermission(adminUser, "gid2", "CUSTOM", "eid", 1, null);
		instance.addGroupEnginePermission(adminUser, "gid", "CUSTOM", "eid1", 1, null);
		instance.addGroupEnginePermission(adminUser, "gid", "CUSTOM", "foobar", 1, null);
		instance.addGroupEnginePermission(adminUser, "gid", "CUSTOM", "eid2", 1, null);
		instance.addGroupEnginePermission(adminUser, "gid", "CUSTOM", "eid3", 1, null);

		long count = instance.getNumEnginesForGroup("gid", "CUSTOM", "eid");

		// make sure its 3, pid1, pid2, pid3 are correct
		assertEquals(3, count);
	}

	///
	/// getAvailableEnginesForGroup
	///
	@Test
	void testGetAvailableEnginesForGroup_groupDoesNotExist() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> instance.getAvailableEnginesForGroup("nope", "CUSTOM", null, 0, 0));
		assertEquals("Group nope with type CUSTOM does not exist", e.getMessage());
	}

	@Test
	void testGetAvailableEnginesForGroup_searchLimitAndOffset() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");
		instance.addGroup(adminUser, "gid2", "CUSTOM", "desc");

		UnitTestSecurityAuthUtils.createEngine("eid", "ename", adminUser);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", adminUser);
		UnitTestSecurityAuthUtils.createEngine("foobar", "foobarname1", adminUser);
		UnitTestSecurityAuthUtils.createEngine("eid2", "ename2", adminUser);
		UnitTestSecurityAuthUtils.createEngine("eid3", "ename3", adminUser);

		instance.addGroupEnginePermission(adminUser, "gid2", "CUSTOM", "eid", 1, null);
		instance.addGroupEnginePermission(adminUser, "gid", "CUSTOM", "eid1", 1, null);

		List<Map<String, Object>> values = instance.getAvailableEnginesForGroup("gid", "CUSTOM", "eid", 0, 0);

		// filter out pid1 and projectid1, limit to one and offset to 1 to pid3.
		assertEquals(3, values.size());
		Map<String, Object> map = values.getFirst();
		assertEquals("eid", map.get("engine_id"));
		Map<String, Object> map2 = values.get(1);
		assertEquals("eid2", map2.get("engine_id"));
		Map<String, Object> map3 = values.get(2);
		assertEquals("eid3", map3.get("engine_id"));

		// limit and offset check
		values = instance.getAvailableEnginesForGroup("gid", "CUSTOM", "eid", 1, 1);
		assertEquals(1, values.size());
		map = values.getFirst();
		assertEquals("eid2", map.get("engine_id"));
	}

	///
	/// getNumAvailableEnginesForGroup
	///

	@Test
	void testGetNumAvailableEnginesForGroup_groupDoesNotExist() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> instance.getNumAvailableEnginesForGroup("nope", "CUSTOM", null));
		assertEquals("Group nope with type CUSTOM does not exist", e.getMessage());
	}

	@Test
	void testNumGetAvailableEnginesForGroup_searchLimitAndOffset() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");
		instance.addGroup(adminUser, "gid2", "CUSTOM", "desc");

		UnitTestSecurityAuthUtils.createEngine("eid", "ename", adminUser);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", adminUser);
		UnitTestSecurityAuthUtils.createEngine("foobar", "foobarname1", adminUser);
		UnitTestSecurityAuthUtils.createEngine("eid2", "ename2", adminUser);
		UnitTestSecurityAuthUtils.createEngine("eid3", "ename3", adminUser);

		instance.addGroupEnginePermission(adminUser, "gid2", "CUSTOM", "eid", 1, null);
		instance.addGroupEnginePermission(adminUser, "gid", "CUSTOM", "eid1", 1, null);

		long count = instance.getNumAvailableEnginesForGroup("gid", "CUSTOM", "eid");

		// filter out eid1 and engineid1
		assertEquals(3, count);
	}

	///
	/// getUserCustomGroups
	///

	@Test
	void testGetUserCustomGroups_multiple() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");
		instance.addGroup(adminUser, "gid2", "CUSTOM", "desc");
		instance.addGroup(adminUser, "gid3", "CUSTOM", "desc");

		UnitTestSecurityAuthUtils.addUserToGroupAsUser(adminUser, "gid");
		UnitTestSecurityAuthUtils.addUserToGroupAsUser(adminUser, "gid2");

		List<String> userCustomGroups = AdminSecurityGroupUtils.getUserCustomGroups(adminUser.getPrimaryLoginToken());

		assertEquals(2, userCustomGroups.size());
		assertEquals("gid", userCustomGroups.getFirst());
		assertEquals("gid2", userCustomGroups.get(1));
	}

	///
	/// userInCustomGroup
	///

	@Test
	void testUserInCustomGroup_notIn() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");

		assertFalse(instance.userInCustomGroup("gid", adminUser.getPrimaryLoginToken().getId(),
				adminUser.getPrimaryLogin().getLabel()));
	}

	@Test
	void testUserInCustomGroup_In() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");

		UnitTestSecurityAuthUtils.addUserToGroupAsUser(adminUser, "gid");

		assertTrue(instance.userInCustomGroup("gid", adminUser.getPrimaryLoginToken().getId(),
				adminUser.getPrimaryLogin().getLabel()));
	}

	///
	/// groupExists
	///

	@Test
	void testGroupExists_doesNotExist() {
		assertFalse(instance.groupExists("gid", "CUSTOM"));
	}

	@Test
	void testGroupExists_exists() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");
		assertTrue(instance.groupExists("gid", "CUSTOM"));
	}

	///
	/// isCustomGroup
	///

	@Test
	void testIsCustom_no() throws Exception {
		instance.addGroup(adminUser, "gid", "not", "desc");
		assertFalse(instance.isCustomGroup("gid"));
	}

	@Test
	void testIsCustom_yes() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");
		assertTrue(instance.isCustomGroup("gid"));
	}

	///
	/// userExists
	///

	@Test
	void testUserExists_no() {
		assertFalse(instance.userExists("foo", "bar"));
	}

	@Test
	void testUserExists_yes() {
		assertTrue(
				instance.userExists(adminUser.getPrimaryLoginToken().getId(), adminUser.getPrimaryLogin().getLabel()));
	}

	///
	/// getGroupProjectPermission
	///

	@Test
	void testGetGroupProjectPermission_readonly() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");

		UnitTestSecurityAuthUtils.createProject("foobar", "foobarname1", adminUser);

		instance.addGroupProjectPermission(adminUser, "gid", "CUSTOM", "foobar", 3, null);

		assertEquals(3, instance.groupProjectPermission("gid", "CUSTOM", "foobar"));
	}

	@Test
	void testGetGroupProjectPermission_none() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");

		UnitTestSecurityAuthUtils.createProject("foobar", "foobarname1", adminUser);

		assertEquals(-1, instance.groupProjectPermission("gid", "CUSTOM", "foobar"));
	}

	///
	/// getGroupEnginePermission
	///

	@Test
	void testGetGroupEnginePermission_editor() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");

		UnitTestSecurityAuthUtils.createEngine("foobar", "foobarname1", adminUser);

		instance.addGroupEnginePermission(adminUser, "gid", "CUSTOM", "foobar", 3, null);

		assertEquals(3, instance.groupEnginePermission("gid", "CUSTOM", "foobar"));
	}

	@Test
	void testGetGroupEnginePermission_none() throws Exception {
		instance.addGroup(adminUser, "gid", "CUSTOM", "desc");

		UnitTestSecurityAuthUtils.createEngine("foobar", "foobarname1", adminUser);

		assertEquals(-1, instance.groupEnginePermission("gid", "CUSTOM", "foobar"));
	}
}
