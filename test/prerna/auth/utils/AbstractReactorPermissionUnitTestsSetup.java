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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.SystemEngineRegistry;

/**
 * A disposable security database for tests of who a reactor works for. It
 * holds an admin with no permission on anything, and one database, one project
 * and one insight with an owner, an editor and a user who has asked for access.
 * A test in any package extends it and runs a reactor as one of those users.
 */
public abstract class AbstractReactorPermissionUnitTestsSetup extends AbstractSecurityUtilsUnitTestsSetup {

	protected static final String ENGINE = "engine-1";
	protected static final String PROJECT = "project-1";
	protected static final String INSIGHT = "insight-1";
	protected static final String REQUESTER_ID = "requesterid";
	protected static final String NOT_AN_ADMIN = "User must be an admin to perform this function";
	protected static final String NO_PERMISSION = "does not exist or user does not have";
	protected static final String LIMITED_TO_ADMINS = "This functionality is limited to only admins";

	private static final String EDITOR_ID = "editorid";
	private static final String LOGIN_TYPE = "NATIVE";
	private static final String EDIT = "EDIT";
	private static final String OWNER = "OWNER";
	private static final int READ_ONLY_LEVEL = 3;

	protected User admin;
	protected User owner;
	protected User editor;
	protected User requester;

	private boolean wasAdminOnlyDatabaseDelete;
	private boolean wasAdminOnlyProjectDelete;
	private boolean wasAdminOnlyDatabaseAdd;
	private IRDBMSEngine securityDb;
	private List<String> tables = new ArrayList<>();

	/**
	 * Creates the users, then the database, project and insight the owner owns and
	 * the editor can edit.
	 *
	 * @throws Exception when the security database cannot be written
	 */
	@BeforeEach
	public void createUsersAndResources() throws Exception {
		this.wasAdminOnlyDatabaseDelete = AbstractSecurityUtils.adminOnlyDatabaseDelete;
		this.wasAdminOnlyProjectDelete = AbstractSecurityUtils.adminOnlyProjectDelete;
		this.wasAdminOnlyDatabaseAdd = AbstractSecurityUtils.adminOnlyDatabaseAdd;
		this.securityDb = SystemEngineRegistry.getSecurityDb();
		assertTrue(this.securityDb.getOwlFilePath().contains("junit"));
		this.admin = UnitTestSecurityAuthUtils.createUser("admin", true);
		this.owner = UnitTestSecurityAuthUtils.createUser("owner", false);
		this.editor = UnitTestSecurityAuthUtils.createUser("editor", false);
		this.requester = UnitTestSecurityAuthUtils.createUser("requester", false);

		UnitTestSecurityAuthUtils.createEngine(ENGINE, ENGINE, this.owner);
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(this.owner, EDITOR_ID, ENGINE, EDIT);
		UnitTestSecurityAuthUtils.createProject(PROJECT, PROJECT, this.owner);
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(this.owner, PROJECT, EDITOR_ID, EDIT);
		UnitTestSecurityAuthUtils.createInsight(PROJECT, INSIGHT, "an insight", "grid");
		SecurityInsightUtils.addInsightUser(this.owner, EDITOR_ID, PROJECT, INSIGHT, EDIT, null);
	}

	/**
	 * Signs the thread's user out, puts the admin only settings back as they were
	 * before the test and empties the security database.
	 *
	 * @throws Exception when the security database cannot be emptied
	 */
	@AfterEach
	public void clearUsersAndResources() throws Exception {
		ThreadStore.remove();
		AbstractSecurityUtils.adminOnlyDatabaseDelete = this.wasAdminOnlyDatabaseDelete;
		AbstractSecurityUtils.adminOnlyProjectDelete = this.wasAdminOnlyProjectDelete;
		AbstractSecurityUtils.adminOnlyDatabaseAdd = this.wasAdminOnlyDatabaseAdd;
		assertTrue(this.securityDb.getOwlFilePath().contains("junit"));
		this.tables = UnitTestSecurityAuthUtils.clearSecurityDB(this.securityDb, this.tables);
	}

	/**
	 * The admin, checked to be an admin who holds no permission on the database,
	 * the project or the insight.
	 *
	 * @return the admin
	 */
	protected User anAdminWhoHoldsNothing() {
		assertTrue(SecurityAdminUtils.userIsAdmin(this.admin));
		assertFalse(SecurityEngineUtils.userCanViewEngine(this.admin, ENGINE));
		assertFalse(SecurityProjectUtils.userCanViewProject(this.admin, PROJECT));
		assertFalse(SecurityInsightUtils.userCanEditInsight(this.admin, PROJECT, INSIGHT));
		return this.admin;
	}

	/**
	 * The admin, given ownership of the database and the project, and checked to
	 * own both.
	 *
	 * @return the admin
	 * @throws Exception when the permissions cannot be given
	 */
	protected User anAdminWhoOwnsTheDatabaseAndTheProject() throws Exception {
		String adminId = User.getPrimaryUserIdAndTypePair(this.admin).getValue0();
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(this.owner, adminId, ENGINE, OWNER);
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(this.owner, PROJECT, adminId, OWNER);
		assertTrue(SecurityAdminUtils.userIsAdmin(this.admin));
		assertTrue(SecurityEngineUtils.userIsOwner(this.admin, ENGINE));
		assertTrue(SecurityProjectUtils.userIsOwner(this.admin, PROJECT));
		return this.admin;
	}

	/**
	 * The owner, checked to own the database and the project without being an
	 * admin.
	 *
	 * @return the owner
	 */
	protected User anOwnerWhoIsNotAnAdmin() {
		assertFalse(SecurityAdminUtils.userIsAdmin(this.owner));
		assertTrue(SecurityEngineUtils.userIsOwner(this.owner, ENGINE));
		assertTrue(SecurityProjectUtils.userIsOwner(this.owner, PROJECT));
		return this.owner;
	}

	/**
	 * The editor, checked to be able to edit the database, the project and the
	 * insight without owning them or being an admin.
	 *
	 * @return the editor
	 */
	protected User anEditorWhoIsNotAnAdmin() {
		assertFalse(SecurityAdminUtils.userIsAdmin(this.editor));
		assertTrue(SecurityEngineUtils.userCanEditEngine(this.editor, ENGINE));
		assertFalse(SecurityEngineUtils.userIsOwner(this.editor, ENGINE));
		assertTrue(SecurityProjectUtils.userCanEditProject(this.editor, PROJECT));
		assertFalse(SecurityProjectUtils.userIsOwner(this.editor, PROJECT));
		assertTrue(SecurityInsightUtils.userCanEditInsight(this.editor, PROJECT, INSIGHT));
		return this.editor;
	}

	/**
	 * Turns on the settings that leave deleting a database or a project to admins,
	 * until the test ends.
	 */
	protected void onlyAdminsDelete() {
		AbstractSecurityUtils.adminOnlyDatabaseDelete = true;
		AbstractSecurityUtils.adminOnlyProjectDelete = true;
	}

	/**
	 * Turns on the setting that leaves adding a database to admins, until the test
	 * ends.
	 */
	protected void onlyAdminsAddDatabases() {
		AbstractSecurityUtils.adminOnlyDatabaseAdd = true;
	}

	/**
	 * Records that the requester asks for Read-Only access to the database.
	 */
	protected void theRequesterAsksForTheEngine() {
		SecurityEngineUtils.setUserAccessRequest(REQUESTER_ID, LOGIN_TYPE, ENGINE, "to read it", READ_ONLY_LEVEL,
				this.requester);
	}

	/**
	 * Records that the requester asks for Read-Only access to the project.
	 */
	protected void theRequesterAsksForTheProject() {
		SecurityProjectUtils.setUserAccessRequest(REQUESTER_ID, LOGIN_TYPE, PROJECT, "to read it", READ_ONLY_LEVEL,
				this.requester);
	}

	/**
	 * Records that the requester asks for Read-Only access to the insight.
	 */
	protected void theRequesterAsksForTheInsight() {
		SecurityInsightUtils.setUserAccessRequest(REQUESTER_ID, LOGIN_TYPE, PROJECT, "to read it", INSIGHT,
				READ_ONLY_LEVEL, this.requester);
	}

	/**
	 * Runs a reactor for a signed in user: the user of the reactor's insight and of
	 * the thread, as on a request.
	 *
	 * @param reactor   the reactor to run
	 * @param user      the signed in user
	 * @param arguments the reactor's inputs by key; a list gives the key several
	 *                  values
	 * @return what the reactor returns
	 */
	protected NounMetadata run(AbstractReactor reactor, User user, Map<String, Object> arguments) {
		Insight insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(user);
		reactor.setInsight(insight);
		NounStore nouns = new NounStore(reactor.getClass().getSimpleName());
		arguments.forEach((key, value) -> {
			GenRowStruct values = nouns.makeGenRowStruct(key);
			for (Object each : value instanceof List<?> list ? list : List.of(value)) {
				values.add(each, each instanceof Boolean ? PixelDataType.BOOLEAN : PixelDataType.CONST_STRING);
			}
		});
		reactor.setNounStore(nouns);
		ThreadStore.setUser(user);
		return reactor.execute();
	}

	/**
	 * Reads the rows a reactor returned as a list of maps.
	 *
	 * @param result what the reactor returned
	 * @return its rows, each a map of column name to value
	 */
	protected static List<Map<String, Object>> rowsOf(NounMetadata result) {
		List<Map<String, Object>> rows = new ArrayList<>();
		for (Object row : (List<?>) result.getValue()) {
			Map<String, Object> cells = new LinkedHashMap<>();
			((Map<?, ?>) row).forEach((key, cell) -> cells.put(String.valueOf(key), cell));
			rows.add(cells);
		}
		return rows;
	}

}
