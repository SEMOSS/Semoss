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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockStatic;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.access.ModelUsageLimit;
import prerna.auth.access.ModelUsageLimitKey;
import prerna.auth.access.ModelUsageLimitRule;
import prerna.auth.access.PermissionBatch;
import prerna.auth.access.PermissionChange;
import prerna.auth.access.PermissionGrant;
import prerna.auth.access.PermissionSubject;
import prerna.auth.access.PermissionTargetResource;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.io.connector.ms.MicrosoftGraphUserLookup;
import prerna.util.SystemEngineRegistry;

/**
 * Runs permission batches against a disposable security database: who may make
 * each change, that a batch is saved whole or not at all, and the reads Bulk
 * Access Management copies and batches from. The Microsoft directory is
 * replaced with a static mock so no test reaches it.
 */
class PermissionBatchUtilsUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private static final String CUSTOM = SecurityGroupManagerUtils.CUSTOM_GROUP_TYPE;
	private static final String NATIVE = AuthProvider.NATIVE.getLabel();
	private static final String MICROSOFT = AuthProvider.MICROSOFT.getLabel();

	private static final String PROJECT_ID = "plannedproject";
	private static final String OTHER_PROJECT_ID = "otherproject";
	private static final String ENGINE_ID = "plannedengine";
	private static final String GROUP_ID = "team";
	private static final String NON_CUSTOM_GROUP_ID = "msgroup";

	private IRDBMSEngine securityDb;
	private List<String> tables = new ArrayList<>();
	private MockedStatic<MicrosoftGraphUserLookup> directory;

	private User admin;
	private User owner;
	private User editor;
	private User reader;
	private User stranger;
	private User manager;
	private AdminSecurityGroupUtils groups;

	@BeforeEach
	void setup() throws Exception {
		directory = mockStatic(MicrosoftGraphUserLookup.class);
		securityDb = SystemEngineRegistry.getSecurityDb();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));

		admin = UnitTestSecurityAuthUtils.createUser("admin", true);
		owner = UnitTestSecurityAuthUtils.createUser("owner", false);
		editor = UnitTestSecurityAuthUtils.createUser("editor", false);
		reader = UnitTestSecurityAuthUtils.createUser("reader", false);
		stranger = UnitTestSecurityAuthUtils.createUser("stranger", false);
		manager = UnitTestSecurityAuthUtils.createUser("manager", false);
		groups = AdminSecurityGroupUtils.getInstance(admin);

		UnitTestSecurityAuthUtils.createProject(PROJECT_ID, "changed project", admin);
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(admin, PROJECT_ID, "ownerid", "OWNER");
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(admin, PROJECT_ID, "editorid", "EDIT");
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(admin, PROJECT_ID, "readerid", "READ_ONLY");

		groups.addGroup(admin, GROUP_ID, CUSTOM, "the team");
		groups.addGroup(admin, NON_CUSTOM_GROUP_ID, MICROSOFT, "a login group");
		UnitTestGroupTables.insertManager(securityDb, GROUP_ID, "managerid", NATIVE);
		// making the team made the admin its first manager; the tests need an
		// admin who does not manage it
		SecurityGroupManagerUtils.removeGroupManager(groups, GROUP_ID, "adminid", NATIVE);
	}

	@AfterEach
	void cleanup() throws Exception {
		directory.close();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		tables = UnitTestSecurityAuthUtils.clearSecurityDB(securityDb, tables);
	}

	///
	/// levelId and toIsoUtc
	///

	@ParameterizedTest
	@CsvSource({ "OWNER,1", "edit,2", " READ_ONLY ,3" })
	void levelId_readsTheThreeLevels(String level, int id) {
		assertEquals(id, PermissionBatchUtils.levelId(level));
	}

	@ParameterizedTest
	@NullAndEmptySource
	@ValueSource(strings = { "DISCOVERABLE", "4", "author" })
	void levelId_nullForAnythingElse(String level) {
		assertNull(PermissionBatchUtils.levelId(level));
	}

	@Test
	void toIsoUtc_writesStoredDatesAsUtc() {
		Timestamp stored = Timestamp.valueOf(LocalDateTime.of(2026, 12, 31, 0, 0));

		assertEquals("2026-12-31T00:00Z", PermissionBatchUtils.toIsoUtc(stored));
		assertEquals("2026-12-31T00:00Z", PermissionBatchUtils.toIsoUtc("2026-12-31 00:00:00"));
		assertNull(PermissionBatchUtils.toIsoUtc(null));
		assertNull(PermissionBatchUtils.toIsoUtc(" "));
		assertNull(PermissionBatchUtils.toIsoUtc("not a date"));
	}

	///
	/// readBatches
	///

	@Test
	void readPlans_readsTheJsonTheFrontEndSends() {
		List<PermissionBatch> batches = SecurityAccessPermissionBatchUtils
				.readBatches("[{\"target\":{\"resourceType\":\"PROJECT\",\"id\":\"p1\"},"
						+ "\"changes\":[{\"action\":\"ADD\",\"subject\":{\"category\":\"USER\",\"id\":\"ada\",\"type\":\"NATIVE\"},"
						+ "\"to\":{\"level\":\"EDIT\",\"endDate\":\"2030-01-01T00:00:00Z\"}}]}]");

		assertEquals(1, batches.size());
		assertEquals(PermissionTargetResource.TargetResourceType.PROJECT, batches.get(0).getTarget().getResourceType());
		PermissionChange change = batches.get(0).getChanges().get(0);
		assertEquals(PermissionChange.Action.ADD, change.getAction());
		assertEquals("ada", change.getSubject().getId());
		assertEquals("EDIT", change.getTo().getLevel());
		assertEquals("2030-01-01T00:00:00Z", change.getTo().getEndDate());
	}

	@ParameterizedTest
	@NullAndEmptySource
	@ValueSource(strings = { "[]", "{\"target\":1}", "not json" })
	void readPlans_rejectsAnythingButAListOfBatches(String json) {
		assertThrows(IllegalArgumentException.class, () -> SecurityAccessPermissionBatchUtils.readBatches(json));
	}

	///
	/// validateBatches
	///

	@Test
	void validateBatches_editorsChangePeopleButOnlyOwnersGiveAuthorAccess() {
		PermissionBatch batch = projectBatch(add("strangerid", "READ_ONLY"), add("managerid", "OWNER"),
				change("readerid", "EDIT"), remove("ownerid"));

		List<Map<String, Object>> results = SecurityAccessPermissionBatchUtils.validateBatches(editor, List.of(batch));

		List<Map<String, Object>> changes = changes(results.get(0));
		assertAllowed(changes.get(0));
		assertBlocked(changes.get(1), "Only owners can give Author access");
		assertAllowed(changes.get(2));
		assertBlocked(changes.get(3), "Only owners can remove an Author's access");
	}

	@Test
	void validateBatches_ownersCanGiveAndTakeAwayAuthorAccess() {
		PermissionBatch batch = projectBatch(add("strangerid", "OWNER"), change("editorid", "OWNER"),
				remove("adminid"));

		for (Map<String, Object> change : changes(
				SecurityAccessPermissionBatchUtils.validateBatches(owner, List.of(batch)).get(0))) {
			assertAllowed(change);
		}
	}

	@Test
	void validateBatches_readersAndStrangersCannotChangeAnything() {
		PermissionBatch batch = projectBatch(add("strangerid", "READ_ONLY"));

		assertBlocked(changes(SecurityAccessPermissionBatchUtils.validateBatches(reader, List.of(batch)).get(0)).get(0),
				"Only this project's editors and owners can change who can use it");
		assertBlocked(
				changes(SecurityAccessPermissionBatchUtils.validateBatches(stranger, List.of(batch)).get(0)).get(0),
				"Only this project's editors and owners can change who can use it");
	}

	@Test
	void validateBatches_someoneWithNoRightsIsToldOnlyThatOnEachChange() {
		PermissionChange badLevel = new PermissionChange(PermissionChange.Action.ADD, user("managerid"),
				new PermissionGrant("DISCOVERABLE", null));
		PermissionBatch batch = projectBatch(badLevel, addTeam(GROUP_ID, "READ_ONLY"));

		List<Map<String, Object>> changes = changes(
				SecurityAccessPermissionBatchUtils.validateBatches(stranger, List.of(batch)).get(0));

		// the bad level is not read, so it gets the same answer as any other change
		assertBlocked(changes.get(0), "Only this project's editors and owners can change who can use it");
		assertBlocked(changes.get(1), "Only this project's owners can change which teams can use it");
		assertEquals("Only this project's editors and owners can change who can use it",
				SecurityAccessPermissionBatchUtils.validateBatches(stranger, List.of(projectBatch())).get(0)
						.get("reason"));
	}

	@Test
	void validateBatches_checksEachChangeAgainstTheCurrentAccess() {
		PermissionBatch batch = projectBatch(add("readerid", "EDIT"), change("strangerid", "EDIT"),
				remove("managerid"));

		List<Map<String, Object>> changes = changes(
				SecurityAccessPermissionBatchUtils.validateBatches(owner, List.of(batch)).get(0));

		assertBlocked(changes.get(0), "user readerid already has access");
		assertBlocked(changes.get(1), "user strangerid no longer has access to change");
		assertBlocked(changes.get(2), "user managerid no longer has access to remove");
	}

	@Test
	void validateBatches_blocksBadInput() {
		PermissionChange badDate = new PermissionChange(PermissionChange.Action.ADD, user("strangerid"),
				new PermissionGrant("READ_ONLY", "nope"));
		PermissionChange badLevel = new PermissionChange(PermissionChange.Action.ADD, user("managerid"),
				new PermissionGrant("DISCOVERABLE", null));
		UnitTestSecurityAuthUtils.createUser("newcomer", false);
		PermissionChange noType = new PermissionChange(PermissionChange.Action.ADD,
				new PermissionSubject(PermissionSubject.SubjectCategory.USER, "newcomerid", null),
				new PermissionGrant("READ_ONLY", null));
		PermissionBatch batch = projectBatch(badDate, badLevel, noType, remove("readerid"), change("readerid", "EDIT"));

		List<Map<String, Object>> changes = changes(
				SecurityAccessPermissionBatchUtils.validateBatches(owner, List.of(batch)).get(0));

		assertBlocked(changes.get(0), "The end date nope for user strangerid is not a valid date");
		assertBlocked(changes.get(1), "The change for user managerid does not say which access level to give");
		// a project keeps only the id, so a person needs no login type there
		assertAllowed(changes.get(2));
		assertAllowed(changes.get(3));
		assertBlocked(changes.get(4), "user readerid is in the batch more than once");
	}

	@Test
	void validateBatches_aTeamMemberNeedsALoginType() {
		PermissionChange noType = new PermissionChange(PermissionChange.Action.ADD,
				new PermissionSubject(PermissionSubject.SubjectCategory.USER, "strangerid", null), null);
		PermissionChange nobody = new PermissionChange(PermissionChange.Action.ADD,
				new PermissionSubject(PermissionSubject.SubjectCategory.USER, "nobody", null),
				new PermissionGrant("READ_ONLY", null));

		assertBlocked(
				changes(SecurityAccessPermissionBatchUtils
						.validateBatches(manager, List.of(teamBatch(GROUP_ID, noType))).get(0)).get(0),
				"The change for user strangerid does not say how they sign in");
		assertBlocked(
				changes(SecurityAccessPermissionBatchUtils.validateBatches(owner, List.of(projectBatch(nobody))).get(0))
						.get(0),
				"user nobody does not exist");
	}

	@Test
	void validateBatches_adminModeReportsATargetThatDoesNotExist() {
		PermissionBatch batch = new PermissionBatch(
				new PermissionTargetResource(PermissionTargetResource.TargetResourceType.PROJECT, OTHER_PROJECT_ID,
						null),
				List.of(add("strangerid", "READ_ONLY")));

		assertBlocked(
				changes(SecurityAccessPermissionBatchUtils
						.validateBatches(SecurityAdminUtils.getInstance(admin), admin, List.of(batch)).get(0)).get(0),
				"Project " + OTHER_PROJECT_ID + " does not exist");
	}

	@Test
	void validateBatches_aTargetCanBeInOnlyOneBatch() {
		List<Map<String, Object>> results = SecurityAccessPermissionBatchUtils.validateBatches(owner,
				List.of(projectBatch(add("strangerid", "READ_ONLY")), projectBatch(add("managerid", "READ_ONLY"))));

		assertAllowed(changes(results.get(0)).get(0));
		assertBlocked(changes(results.get(1)).get(0), "This project is in more than one batch");
	}

	@Test
	void validateBatches_reportsAnEngineThatDoesNotExistOnItsBatch() {
		PermissionBatch batch = new PermissionBatch(
				new PermissionTargetResource(PermissionTargetResource.TargetResourceType.ENGINE, "missingengine", null),
				List.of(add("strangerid", "READ_ONLY")));

		assertBlocked(changes(SecurityAccessPermissionBatchUtils.validateBatches(editor, List.of(batch)).get(0)).get(0),
				"Only this engine's editors and owners can change who can use it");
	}

	@Test
	void validateBatches_onlyOwnersChangeWhichTeamsHaveAccess() {
		PermissionBatch batch = projectBatch(addTeam(GROUP_ID, "READ_ONLY"));

		assertBlocked(changes(SecurityAccessPermissionBatchUtils.validateBatches(editor, List.of(batch)).get(0)).get(0),
				"Only this project's owners can change which teams can use it");
		assertAllowed(changes(SecurityAccessPermissionBatchUtils.validateBatches(owner, List.of(batch)).get(0)).get(0));
	}

	@Test
	void validateBatches_onlyManagersChangeACustomTeamsMembers() throws Exception {
		PermissionBatch batch = teamBatch(GROUP_ID, addMember("strangerid"));

		assertAllowed(
				changes(SecurityAccessPermissionBatchUtils.validateBatches(manager, List.of(batch)).get(0)).get(0));
		assertBlocked(changes(SecurityAccessPermissionBatchUtils.validateBatches(owner, List.of(batch)).get(0)).get(0),
				"Only this team's managers can change its members");
		// only admin mode is told that a team is not a custom team
		PermissionBatch notCustom = teamBatch(NON_CUSTOM_GROUP_ID, addMember("strangerid"));
		assertBlocked(
				changes(SecurityAccessPermissionBatchUtils.validateBatches(admin, List.of(notCustom)).get(0)).get(0),
				"Only this team's managers can change its members");
		assertBlocked(
				changes(SecurityAccessPermissionBatchUtils
						.validateBatches(SecurityAdminUtils.getInstance(admin), admin, List.of(notCustom)).get(0))
						.get(0),
				"Team " + NON_CUSTOM_GROUP_ID + " is not a custom team, so its members come from the login provider");
	}

	@Test
	void validateBatches_someoneWithNoPermissionCannotTellWhatTypeOfTeamItIs() throws Exception {
		String reason = "Only this team's managers can change its members";
		for (String teamId : List.of(GROUP_ID, NON_CUSTOM_GROUP_ID, "missingteam")) {
			assertBlocked(
					changes(SecurityAccessPermissionBatchUtils
							.validateBatches(stranger, List.of(teamBatch(teamId, addMember("ownerid")))).get(0)).get(0),
					reason);
			IllegalAccessException e = assertThrows(IllegalAccessException.class,
					() -> SecurityAccessPermissionBatchUtils.getPermissions(stranger,
							PermissionTargetResource.team(teamId)));
			assertEquals(
					"Only this team's managers, and the owners and editors of what it can use, can see its members",
					e.getMessage());
		}
	}

	@Test
	void validateBatches_onlyManagersChangeACustomTeamsManagers() {
		PermissionBatch batch = managersBatch(GROUP_ID, addMember("strangerid"));

		assertAllowed(
				changes(SecurityAccessPermissionBatchUtils.validateBatches(manager, List.of(batch)).get(0)).get(0));
		assertBlocked(changes(SecurityAccessPermissionBatchUtils.validateBatches(owner, List.of(batch)).get(0)).get(0),
				"Only this team's managers can change who manages it");
		assertBlocked(changes(SecurityAccessPermissionBatchUtils.validateBatches(admin, List.of(batch)).get(0)).get(0),
				"Only this team's managers can change who manages it");
		assertAllowed(changes(SecurityAccessPermissionBatchUtils
				.validateBatches(SecurityAdminUtils.getInstance(admin), admin, List.of(batch)).get(0)).get(0));
		// a manager of another team is told nothing about this one; admin mode is
		// told it is not a custom team
		PermissionBatch notCustom = managersBatch(NON_CUSTOM_GROUP_ID, addMember("strangerid"));
		assertBlocked(
				changes(SecurityAccessPermissionBatchUtils.validateBatches(manager, List.of(notCustom)).get(0)).get(0),
				"Only this team's managers can change who manages it");
		assertBlocked(changes(SecurityAccessPermissionBatchUtils
				.validateBatches(SecurityAdminUtils.getInstance(admin), admin, List.of(notCustom)).get(0)).get(0),
				"Team " + NON_CUSTOM_GROUP_ID + " is not a custom team, so it has no managers");
	}

	@Test
	void validateBatches_checksManagerChangesAgainstTheCurrentManagers() {
		PermissionChange endDated = new PermissionChange(PermissionChange.Action.ADD, user("strangerid"),
				new PermissionGrant(null, ZonedDateTime.now().plusDays(5).toString()));
		PermissionBatch batch = managersBatch(GROUP_ID, addMember("managerid"), remove("readerid"),
				new PermissionChange(PermissionChange.Action.CHANGE, user("editorid"),
						new PermissionGrant("EDIT", null)),
				endDated);

		List<Map<String, Object>> results = changes(
				SecurityAccessPermissionBatchUtils.validateBatches(manager, List.of(batch)).get(0));

		assertBlocked(results.get(0), "user managerid already manages this team");
		assertBlocked(results.get(1), "user readerid no longer manages this team");
		assertBlocked(results.get(2), "A team's managers have no access level to change");
		assertBlocked(results.get(3), "A team's managers have no end date, so user strangerid cannot be given one");
	}

	@Test
	void validateBatches_anAdminIsAnOrdinaryUserOutsideAdminMode() {
		User otherAdmin = UnitTestSecurityAuthUtils.createUser("otheradmin", true);
		PermissionBatch batch = projectBatch(add("strangerid", "OWNER"), addTeam(GROUP_ID, "READ_ONLY"));

		List<Map<String, Object>> asUser = changes(
				SecurityAccessPermissionBatchUtils.validateBatches(otherAdmin, List.of(batch)).get(0));
		assertBlocked(asUser.get(0), "Only this project's editors and owners can change who can use it");
		assertBlocked(asUser.get(1), "Only this project's owners can change which teams can use it");

		List<Map<String, Object>> asAdmin = changes(SecurityAccessPermissionBatchUtils
				.validateBatches(SecurityAdminUtils.getInstance(otherAdmin), otherAdmin, List.of(batch)).get(0));
		assertAllowed(asAdmin.get(0));
		assertAllowed(asAdmin.get(1));
	}

	@Test
	void validateBatches_anAuthorThroughATeamHasEveryRight() throws Exception {
		groups.addGroupProjectPermission(admin, GROUP_ID, CUSTOM, PROJECT_ID, 1, null);
		groups.addGroup(admin, "otherteam", CUSTOM, "another team");
		UnitTestGroupTables.insertMember(securityDb, GROUP_ID, "strangerid", NATIVE);
		PermissionBatch batch = projectBatch(add("managerid", "OWNER"), change("ownerid", "EDIT"),
				addTeam("otherteam", "READ_ONLY"));

		for (Map<String, Object> change : changes(
				SecurityAccessPermissionBatchUtils.validateBatches(stranger, List.of(batch)).get(0))) {
			assertAllowed(change);
		}
	}

	@Test
	void validateBatches_anEditorThroughATeamChangesPeopleButNotAuthors() throws Exception {
		groups.addGroupProjectPermission(admin, GROUP_ID, CUSTOM, PROJECT_ID, 2, null);
		groups.addGroup(admin, "otherteam", CUSTOM, "another team");
		UnitTestGroupTables.insertMember(securityDb, GROUP_ID, "strangerid", NATIVE);
		PermissionBatch batch = projectBatch(add("managerid", "READ_ONLY"), change("ownerid", "EDIT"),
				addTeam("otherteam", "READ_ONLY"));

		List<Map<String, Object>> changes = changes(
				SecurityAccessPermissionBatchUtils.validateBatches(stranger, List.of(batch)).get(0));

		assertAllowed(changes.get(0));
		assertBlocked(changes.get(1), "Only owners can change an Author's access");
		assertBlocked(changes.get(2), "Only this project's owners can change which teams can use it");
	}

	@Test
	void validateBatches_anEndedTeamGrantGivesNoRights() throws Exception {
		groups.addGroupProjectPermission(admin, GROUP_ID, CUSTOM, PROJECT_ID, 1,
				ZonedDateTime.now().minusDays(1).toString());
		UnitTestGroupTables.insertMember(securityDb, GROUP_ID, "strangerid", NATIVE);

		assertBlocked(
				changes(SecurityAccessPermissionBatchUtils
						.validateBatches(stranger, List.of(projectBatch(add("managerid", "READ_ONLY")))).get(0)).get(0),
				"Only this project's editors and owners can change who can use it");
	}

	@Test
	void saveBatches_rightsComeFromTheRowsBeforeTheSaveNotFromTheBatches() throws Exception {
		groups.addGroupProjectPermission(admin, GROUP_ID, CUSTOM, PROJECT_ID, 1, null);
		// the manager manages the team but is not in it, so has no access to the
		// project; joining the team in the same save gives them none either
		PermissionBatch joinTeam = teamBatch(GROUP_ID, addMember("managerid"));
		PermissionBatch addPerson = projectBatch(add("strangerid", "READ_ONLY"));

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> SecurityAccessPermissionBatchUtils.saveBatches(manager, List.of(joinTeam, addPerson)));

		assertTrue(e.getMessage().contains("Only this project's editors and owners can change who can use it"));
		assertEquals(List.of(), UnitTestGroupTables.members(securityDb, GROUP_ID));
		assertNull(SecurityProjectUtils.getUserProjectPermission("strangerid", PROJECT_ID));
	}

	///
	/// changing your own access
	///

	@Test
	void validateBatches_anEditorCannotChangeTheirOwnAccessButCanLeave() {
		PermissionBatch extend = projectBatch(change("editorid", "EDIT"));
		assertBlocked(
				changes(SecurityAccessPermissionBatchUtils.validateBatches(editor, List.of(extend)).get(0)).get(0),
				"Only this project's owners can change their own access");

		PermissionBatch leave = projectBatch(remove("editorid"));
		assertAllowed(
				changes(SecurityAccessPermissionBatchUtils.validateBatches(editor, List.of(leave)).get(0)).get(0));
	}

	@Test
	void validateBatches_anEditorThroughATeamCannotGiveThemselvesADirectGrant() throws Exception {
		groups.addGroupProjectPermission(admin, GROUP_ID, CUSTOM, PROJECT_ID, 2, null);
		UnitTestGroupTables.insertMember(securityDb, GROUP_ID, "strangerid", NATIVE);

		assertBlocked(
				changes(SecurityAccessPermissionBatchUtils
						.validateBatches(stranger, List.of(projectBatch(add("strangerid", "EDIT")))).get(0)).get(0),
				"Only this project's owners can change their own access");
	}

	///
	/// denying access requests: not a batch, but the same permissions
	///

	@Test
	void denyRequests_anAuthorThroughATeamCanDeny() throws Exception {
		groups.addGroupProjectPermission(admin, GROUP_ID, CUSTOM, PROJECT_ID, 1, null);
		UnitTestGroupTables.insertMember(securityDb, GROUP_ID, "strangerid", NATIVE);
		SecurityProjectUtils.setUserAccessRequest("managerid", NATIVE, PROJECT_ID, "please", 3, manager);
		String requestId = String.valueOf(UnitTestGroupTables.queryValue(securityDb,
				"SELECT ID FROM PROJECTACCESSREQUEST WHERE REQUEST_USERID = ?", "managerid"));

		SecurityProjectUtils.denyProjectUserAccessRequests(stranger, PROJECT_ID, List.of(requestId));

		assertEquals("DENIED", UnitTestGroupTables.queryValue(securityDb,
				"SELECT APPROVER_DECISION FROM PROJECTACCESSREQUEST WHERE ID = ?", requestId));
		assertEquals("strangerid", UnitTestGroupTables.queryValue(securityDb,
				"SELECT APPROVER_USERID FROM PROJECTACCESSREQUEST WHERE ID = ?", requestId));
	}

	@Test
	void denyRequests_onlyOwnersCanDeny() {
		IllegalAccessException notOwner = assertThrows(IllegalAccessException.class,
				() -> SecurityProjectUtils.denyProjectUserAccessRequests(editor, PROJECT_ID, List.of("anyrequest")));
		assertEquals("Insufficient privileges to deny user access requests.", notOwner.getMessage());

		IllegalAccessException noRights = assertThrows(IllegalAccessException.class,
				() -> SecurityProjectUtils.denyProjectUserAccessRequests(reader, PROJECT_ID, List.of("anyrequest")));
		assertEquals("Insufficient privileges to modify this project's permissions.", noRights.getMessage());
	}

	@Test
	void validateBatches_onlyAdminModeLetsAnAdminChangeAnotherTeamsMembers() {
		PermissionBatch batch = teamBatch(GROUP_ID, addMember("strangerid"));

		assertBlocked(changes(SecurityAccessPermissionBatchUtils.validateBatches(admin, List.of(batch)).get(0)).get(0),
				"Only this team's managers can change its members");
		assertAllowed(changes(SecurityAccessPermissionBatchUtils
				.validateBatches(SecurityAdminUtils.getInstance(admin), admin, List.of(batch)).get(0)).get(0));
	}

	@Test
	void validateBatches_adminModeNeedsTheAdminUtilities() {
		assertThrows(IllegalArgumentException.class, () -> SecurityAccessPermissionBatchUtils.validateBatches(null,
				admin, List.of(projectBatch(add("strangerid", "EDIT")))));
	}

	///
	/// saveBatches
	///

	@Test
	void saveBatches_savesEveryChangeTogether() throws Exception {
		Map<String, Object> result = SecurityAccessPermissionBatchUtils.saveBatches(owner,
				List.of(projectBatch(add("strangerid", "EDIT"), change("readerid", "OWNER"), remove("editorid"),
						addTeam(GROUP_ID, "READ_ONLY"))));

		assertEquals(1, result.get("targets"));
		assertEquals(4, result.get("changes"));
		Map<String, Integer> levels = SecurityProjectUtils
				.getUserProjectPermissions(List.of("strangerid", "readerid", "editorid"), PROJECT_ID);
		assertEquals(2, levels.get("strangerid"));
		assertEquals(1, levels.get("readerid"));
		assertFalse(levels.containsKey("editorid"));
		assertEquals(3, SecurityGroupProjectUtils.getGroupProjectPermission(GROUP_ID, CUSTOM, PROJECT_ID));
	}

	@Test
	void saveBatches_savesNothingWhenAnyChangeIsBlocked() {
		PermissionBatch good = projectBatch(add("strangerid", "EDIT"));
		PermissionBatch bad = new PermissionBatch(
				new PermissionTargetResource(PermissionTargetResource.TargetResourceType.PROJECT, OTHER_PROJECT_ID,
						null),
				List.of(add("managerid", "READ_ONLY")));

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> SecurityAccessPermissionBatchUtils.saveBatches(editor, List.of(good, bad)));

		assertTrue(e.getMessage().startsWith("Some changes cannot be made: project " + OTHER_PROJECT_ID));
		assertFalse(SecurityProjectUtils.getUserProjectPermissions(List.of("strangerid"), PROJECT_ID)
				.containsKey("strangerid"));
	}

	@Test
	void saveBatches_savesAModelsEndDateAndUsageLimit() throws Exception {
		UnitTestSecurityAuthUtils.createEngine(ENGINE_ID, "changed engine", IEngine.CATALOG_TYPE.MODEL, admin);
		PermissionGrant grant = new PermissionGrant("READ_ONLY", ZonedDateTime.now().plusDays(5).toString())
				.withUsageLimit(tokensAWeek(1000L));
		PermissionBatch batch = new PermissionBatch(
				new PermissionTargetResource(PermissionTargetResource.TargetResourceType.ENGINE, ENGINE_ID, null),
				List.of(new PermissionChange(PermissionChange.Action.ADD, user("strangerid"), grant)));

		SecurityAccessPermissionBatchUtils.saveBatches(admin, List.of(batch));

		ModelUsageLimit saved = SecurityModelUsageLimitUtils
				.getLimit(ModelUsageLimitKey.direct(ENGINE_ID, "strangerid", null));
		assertEquals(1, saved.getRules().size());
		assertEquals(ModelUsageLimitRule.Metric.TOKENS, saved.getRules().get(0).getMetric());
		assertEquals(1000L, saved.getRules().get(0).getAmount());
		assertEquals(ModelUsageLimitRule.Window.WEEK, saved.getRules().get(0).getWindow());
		assertTrue(UnitTestGroupTables.queryValue(securityDb,
				"SELECT ENDDATE FROM ENGINEPERMISSION WHERE USERID=? AND ENGINEID=?", "strangerid", ENGINE_ID) != null);
	}

	@Test
	void saveBatches_refusesALimitOnAnEngineThatIsNotAModel() {
		UnitTestSecurityAuthUtils.createEngine(ENGINE_ID, "changed engine", admin);
		PermissionGrant grant = new PermissionGrant("READ_ONLY", null).withUsageLimit(tokensAWeek(1000L));
		PermissionBatch batch = new PermissionBatch(PermissionTargetResource.engine(ENGINE_ID),
				List.of(PermissionChange.add(user("strangerid"), grant)));

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> SecurityAccessPermissionBatchUtils.saveBatches(admin, List.of(batch)));

		assertTrue(e.getMessage().contains("Only a model's grants can have usage limits"));
		assertNull(
				SecurityUserEngineUtils.getUserEnginePermissions(List.of("strangerid"), ENGINE_ID).get("strangerid"));
	}

	@Test
	void saveBatches_changesACustomTeamsMembers() throws Exception {
		UnitTestGroupTables.insertMember(securityDb, GROUP_ID, "readerid", NATIVE);

		SecurityAccessPermissionBatchUtils.saveBatches(manager, List.of(teamBatch(GROUP_ID, addMember("strangerid"),
				new PermissionChange(PermissionChange.Action.REMOVE, user("readerid"), null))));

		assertEquals(List.of("strangerid:" + NATIVE), UnitTestGroupTables.members(securityDb, GROUP_ID));
	}

	@Test
	void saveBatches_changesATeamsMembersAndManagersTogether() throws Exception {
		SecurityAccessPermissionBatchUtils.saveBatches(manager, List.of(teamBatch(GROUP_ID, addMember("strangerid")),
				managersBatch(GROUP_ID, addMember("editorid"), remove("managerid"))));

		assertEquals(List.of("strangerid:" + NATIVE), UnitTestGroupTables.members(securityDb, GROUP_ID));
		assertEquals(List.of("editorid:" + NATIVE), UnitTestGroupTables.managers(securityDb, GROUP_ID));
	}

	@Test
	void saveBatches_addsPeoplePickedFromTheDirectoryFirst() throws Exception {
		directory.when(MicrosoftGraphUserLookup::isEnabled).thenReturn(true);
		directory.when(() -> MicrosoftGraphUserLookup.addMissingUser(admin, "directory-user")).thenAnswer(call -> {
			UnitTestGroupTables.insertUser(securityDb, "directory-user", MICROSOFT);
			return true;
		});
		PermissionChange add = new PermissionChange(PermissionChange.Action.ADD,
				new PermissionSubject(PermissionSubject.SubjectCategory.USER, "directory-user", MICROSOFT),
				new PermissionGrant("READ_ONLY", null));

		SecurityAccessPermissionBatchUtils.saveBatches(admin, List.of(projectBatch(add)));

		directory.verify(() -> MicrosoftGraphUserLookup.addMissingUser(admin, "directory-user"));
		assertEquals(3, SecurityProjectUtils.getUserProjectPermissions(List.of("directory-user"), PROJECT_ID)
				.get("directory-user"));
	}

	///
	/// getPermissions
	///

	@Test
	void getAccess_readsPeopleTeamsAndRights() throws Exception {
		groups.addGroupProjectPermission(admin, GROUP_ID, CUSTOM, PROJECT_ID, 3,
				ZonedDateTime.now().plusDays(3).toString());

		Map<String, Object> access = SecurityAccessPermissionBatchUtils.getPermissions(editor,
				new PermissionTargetResource(PermissionTargetResource.TargetResourceType.PROJECT, PROJECT_ID, null));

		Map<String, Object> readerRow = rowFor(list(access.get("people")), "readerid");
		assertEquals("READ_ONLY", map(readerRow.get("access")).get("level"));
		// the test helper gives the people it adds an end date, which comes back in ISO
		assertTrue(String.valueOf(map(readerRow.get("access")).get("endDate")).endsWith("Z"));
		List<Map<String, Object>> teams = list(access.get("teams"));
		assertEquals(GROUP_ID, map(teams.get(0).get("subject")).get("id"));
		assertEquals("READ_ONLY", map(teams.get(0).get("access")).get("level"));
		// a team's end date comes back too, so changing its level keeps it
		assertTrue(String.valueOf(map(teams.get(0).get("access")).get("endDate")).endsWith("Z"));
		Map<String, Object> allowed = map(access.get("allowedPermissionChanges"));
		assertEquals(true, allowed.get("canChangePeople"));
		assertEquals(false, allowed.get("canGiveAuthorAccess"));
		assertEquals(false, allowed.get("canChangeTeams"));
	}

	@Test
	void getAccess_readsATeamsMembersForItsManagers() throws Exception {
		UnitTestGroupTables.insertMember(securityDb, GROUP_ID, "readerid", NATIVE);
		PermissionTargetResource team = new PermissionTargetResource(PermissionTargetResource.TargetResourceType.TEAM,
				GROUP_ID, null);

		Map<String, Object> access = SecurityAccessPermissionBatchUtils.getPermissions(manager, team);

		List<Map<String, Object>> people = list(access.get("people"));
		assertEquals("readerid", map(people.get(0).get("subject")).get("id"));
		assertNull(people.get(0).get("access"));
		assertEquals(true, map(access.get("allowedPermissionChanges")).get("canChangePeople"));
		assertThrows(IllegalAccessException.class,
				() -> SecurityAccessPermissionBatchUtils.getPermissions(stranger, team));
	}

	@Test
	void getAccess_readsATeamsManagersForItsManagers() throws Exception {
		PermissionTargetResource managers = new PermissionTargetResource(
				PermissionTargetResource.TargetResourceType.TEAM_MANAGERS, GROUP_ID, null);

		Map<String, Object> access = SecurityAccessPermissionBatchUtils.getPermissions(manager, managers);

		List<Map<String, Object>> people = list(access.get("people"));
		assertEquals("managerid", map(people.get(0).get("subject")).get("id"));
		assertNull(people.get(0).get("access"));
		assertTrue(list(access.get("teams")).isEmpty());
		assertEquals(true, map(access.get("allowedPermissionChanges")).get("canChangePeople"));
		assertThrows(IllegalAccessException.class,
				() -> SecurityAccessPermissionBatchUtils.getPermissions(stranger, managers));
		assertThrows(IllegalAccessException.class,
				() -> SecurityAccessPermissionBatchUtils.getPermissions(admin, managers));
		assertEquals(1, list(SecurityAccessPermissionBatchUtils
				.getPermissions(SecurityAdminUtils.getInstance(admin), admin, managers).get("people")).size());
	}

	@Test
	void getAccess_readsAnyTargetInAdminModeOnly() throws Exception {
		User otherAdmin = UnitTestSecurityAuthUtils.createUser("otheradmin", true);
		PermissionTargetResource project = new PermissionTargetResource(
				PermissionTargetResource.TargetResourceType.PROJECT, PROJECT_ID, null);

		assertThrows(IllegalAccessException.class,
				() -> SecurityAccessPermissionBatchUtils.getPermissions(otherAdmin, project));

		Map<String, Object> access = SecurityAccessPermissionBatchUtils
				.getPermissions(SecurityAdminUtils.getInstance(otherAdmin), otherAdmin, project);
		assertEquals("READ_ONLY", map(rowFor(list(access.get("people")), "readerid").get("access")).get("level"));
		Map<String, Object> allowed = map(access.get("allowedPermissionChanges"));
		assertEquals(true, allowed.get("canChangePeople"));
		assertEquals(true, allowed.get("canGiveAuthorAccess"));
		assertEquals(true, allowed.get("canChangeTeams"));
	}

	///
	/// getPermissionsToCopy
	///

	@Test
	void getAccessToCopy_copiesAProjectsDirectAccess() throws Exception {
		UnitTestSecurityAuthUtils.createProject(OTHER_PROJECT_ID, "other project", admin);
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(admin, OTHER_PROJECT_ID, "ownerid", "OWNER");
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(admin, OTHER_PROJECT_ID, "strangerid", "EDIT");

		List<Map<String, Object>> rows = SecurityAccessPermissionBatchUtils.getPermissionsToCopy(owner,
				new PermissionTargetResource(PermissionTargetResource.TargetResourceType.PROJECT, OTHER_PROJECT_ID,
						null),
				new PermissionTargetResource(PermissionTargetResource.TargetResourceType.PROJECT, PROJECT_ID, null));

		Map<String, Object> strangerRow = rowFor(rows, "strangerid");
		assertEquals("EDIT", map(strangerRow.get("source")).get("level"));
		assertNull(strangerRow.get("target"));
		assertEquals(false, strangerRow.get("inTarget"));
		Map<String, Object> ownerRow = rowFor(rows, "ownerid");
		assertEquals(true, ownerRow.get("inTarget"));
		assertEquals("OWNER", map(ownerRow.get("target")).get("level"));
	}

	@Test
	void getAccessToCopy_aTeamsMembersNeedItsManagersTheTargetsOwnersOrAccessToTheTarget() throws Exception {
		UnitTestGroupTables.insertMember(securityDb, GROUP_ID, "strangerid", NATIVE);
		PermissionTargetResource team = new PermissionTargetResource(PermissionTargetResource.TargetResourceType.TEAM,
				GROUP_ID, null);
		PermissionTargetResource project = new PermissionTargetResource(
				PermissionTargetResource.TargetResourceType.PROJECT, PROJECT_ID, null);

		// the project's owners see whom they would bring in; its editors once the
		// team can use it
		assertNull(rowFor(SecurityAccessPermissionBatchUtils.getPermissionsToCopy(owner, team, project), "strangerid")
				.get("source"));
		assertThrows(IllegalAccessException.class,
				() -> SecurityAccessPermissionBatchUtils.getPermissionsToCopy(editor, team, project));

		groups.addGroupProjectPermission(admin, GROUP_ID, CUSTOM, PROJECT_ID, 3, null);
		assertNull(rowFor(SecurityAccessPermissionBatchUtils.getPermissionsToCopy(editor, team, project), "strangerid")
				.get("source"));
	}

	@Test
	void getAccessToCopy_copiesManagersAndMembersBetweenATeamsLists() throws Exception {
		groups.addGroup(admin, "otherteam", CUSTOM, "another team");
		UnitTestGroupTables.insertManager(securityDb, "otherteam", "managerid", NATIVE);
		UnitTestGroupTables.insertManager(securityDb, "otherteam", "editorid", NATIVE);
		PermissionTargetResource from = new PermissionTargetResource(
				PermissionTargetResource.TargetResourceType.TEAM_MANAGERS, "otherteam", null);
		PermissionTargetResource to = new PermissionTargetResource(
				PermissionTargetResource.TargetResourceType.TEAM_MANAGERS, GROUP_ID, null);
		PermissionTargetResource members = new PermissionTargetResource(
				PermissionTargetResource.TargetResourceType.TEAM, GROUP_ID, null);

		List<Map<String, Object>> rows = SecurityAccessPermissionBatchUtils.getPermissionsToCopy(manager, from, to);

		assertEquals(false, rowFor(rows, "editorid").get("inTarget"));
		assertEquals(true, rowFor(rows, "managerid").get("inTarget"));
		assertNull(rowFor(rows, "editorid").get("source"));
		assertThrows(IllegalAccessException.class,
				() -> SecurityAccessPermissionBatchUtils.getPermissionsToCopy(stranger, from, to));

		// another team's managers, and a team's own managers, can become its members
		assertEquals(false,
				rowFor(SecurityAccessPermissionBatchUtils.getPermissionsToCopy(manager, from, members), "editorid")
						.get("inTarget"));
		assertEquals(false,
				rowFor(SecurityAccessPermissionBatchUtils.getPermissionsToCopy(manager, to, members), "managerid")
						.get("inTarget"));

		// managers are never copied to or from a project or an engine
		PermissionTargetResource project = new PermissionTargetResource(
				PermissionTargetResource.TargetResourceType.PROJECT, PROJECT_ID, null);
		assertThrows(IllegalArgumentException.class,
				() -> SecurityAccessPermissionBatchUtils.getPermissionsToCopy(manager, to, project));
		assertThrows(IllegalArgumentException.class,
				() -> SecurityAccessPermissionBatchUtils.getPermissionsToCopy(owner, project, to));
	}

	@Test
	void getAccessToCopy_rejectsCopyingATargetIntoItself() {
		PermissionTargetResource project = new PermissionTargetResource(
				PermissionTargetResource.TargetResourceType.PROJECT, PROJECT_ID, null);

		assertThrows(IllegalArgumentException.class,
				() -> SecurityAccessPermissionBatchUtils.getPermissionsToCopy(owner, project, project));
	}

	///
	/// helpers
	///

	private static PermissionSubject user(String id) {
		return new PermissionSubject(PermissionSubject.SubjectCategory.USER, id, NATIVE);
	}

	private static PermissionChange add(String userId, String level) {
		return new PermissionChange(PermissionChange.Action.ADD, user(userId), new PermissionGrant(level, null));
	}

	private static PermissionChange change(String userId, String level) {
		return new PermissionChange(PermissionChange.Action.CHANGE, user(userId), new PermissionGrant(level, null));
	}

	private static PermissionChange remove(String userId) {
		return new PermissionChange(PermissionChange.Action.REMOVE, user(userId), null);
	}

	private static PermissionChange addTeam(String groupId, String level) {
		return new PermissionChange(PermissionChange.Action.ADD,
				new PermissionSubject(PermissionSubject.SubjectCategory.TEAM, groupId, CUSTOM),
				new PermissionGrant(level, null));
	}

	private static PermissionChange addMember(String userId) {
		return new PermissionChange(PermissionChange.Action.ADD, user(userId), null);
	}

	private static ModelUsageLimit tokensAWeek(long amount) {
		return ModelUsageLimit.of(List.of(new ModelUsageLimitRule(ModelUsageLimitRule.Metric.TOKENS, amount,
				ModelUsageLimitRule.Window.WEEK, ModelUsageLimitRule.Counts.EACH_PERSON)));
	}

	private static PermissionBatch projectBatch(PermissionChange... changes) {
		return new PermissionBatch(
				new PermissionTargetResource(PermissionTargetResource.TargetResourceType.PROJECT, PROJECT_ID, null),
				List.of(changes));
	}

	private static PermissionBatch teamBatch(String groupId, PermissionChange... changes) {
		return new PermissionBatch(
				new PermissionTargetResource(PermissionTargetResource.TargetResourceType.TEAM, groupId, null),
				List.of(changes));
	}

	private static PermissionBatch managersBatch(String groupId, PermissionChange... changes) {
		return new PermissionBatch(
				new PermissionTargetResource(PermissionTargetResource.TargetResourceType.TEAM_MANAGERS, groupId, null),
				List.of(changes));
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> map(Object value) {
		return (Map<String, Object>) value;
	}

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> list(Object value) {
		return (List<Map<String, Object>>) value;
	}

	private static List<Map<String, Object>> changes(Map<String, Object> result) {
		return list(result.get("changes"));
	}

	private static Map<String, Object> rowFor(List<Map<String, Object>> rows, String userId) {
		return rows.stream().filter(row -> userId.equals(map(row.get("subject")).get("id"))).findFirst().orElseThrow();
	}

	private static void assertAllowed(Map<String, Object> change) {
		assertEquals(true, change.get("allowed"), String.valueOf(change.get("reason")));
	}

	private static void assertBlocked(Map<String, Object> change, String reason) {
		assertEquals(false, change.get("allowed"));
		assertEquals(reason, change.get("reason"));
	}
}
