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
package prerna.reactor.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.auth.utils.AbstractReactorPermissionUnitTestsSetup;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Who CloneEngine and GetEngineUserAccessRequest work for: the owner of the
 * engine clones it, and its editors read its access requests. An admin holds
 * their own permissions in them, like anyone else, and works on someone else's
 * engine through the admin reactors. The rule is the section "Admin
 * operations" of docs/platform_services/engine_access.md.
 */
class EngineReactorPermissionsUnitTests extends AbstractReactorPermissionUnitTestsSetup {

	private static final String NO_ACCESS_TO_REQUESTS = "User does not have permission to view access requests for "
			+ "this engine";

	/**
	 * Being an admin does not stand in for owning the engine.
	 */
	@Test
	void cloneEngineRefusesAnAdminWhoHoldsNoPermissionOnTheEngine() {
		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> run(new CloneEngineReactor(), anAdminWhoHoldsNothing(), Map.of("engineId", ENGINE)));

		assertTrue(refusal.getMessage().contains(NO_PERMISSION), refusal.getMessage());
	}

	/**
	 * A clone is a new engine, so with adding databases left to admins, owning the
	 * source is not enough for a user who is not an admin.
	 */
	@Test
	void cloneEngineRefusesAnOwnerWhoIsNotAnAdminWhileOnlyAdminsAddDatabases() {
		onlyAdminsAddDatabases();

		SemossPixelException refusal = assertThrows(SemossPixelException.class,
				() -> run(new CloneEngineReactor(), anOwnerWhoIsNotAnAdmin(), Map.of("engineId", ENGINE)));

		assertEquals(LIMITED_TO_ADMINS, refusal.getMessage());
	}

	/**
	 * With adding databases left to admins, an admin passes that setting and is
	 * still held to owning the source.
	 */
	@Test
	void cloneEngineRefusesAnAdminWhoHoldsNoPermissionWhileOnlyAdminsAddDatabases() {
		onlyAdminsAddDatabases();

		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> run(new CloneEngineReactor(), anAdminWhoHoldsNothing(), Map.of("engineId", ENGINE)));

		assertTrue(refusal.getMessage().contains(NO_PERMISSION), refusal.getMessage());
	}

	/**
	 * Being an admin does not stand in for being able to edit the engine.
	 */
	@Test
	void getEngineUserAccessRequestRefusesAnAdminWhoHoldsNoPermissionOnTheEngine() {
		theRequesterAsksForTheEngine();

		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> run(new GetEngineUserAccessRequestReactor(), anAdminWhoHoldsNothing(),
						Map.of("engine", ENGINE)));

		assertEquals(NO_ACCESS_TO_REQUESTS, refusal.getMessage());
	}

	/**
	 * Reading an engine's access requests touches the security database only, so
	 * the whole reactor runs here for an editor.
	 */
	@Test
	void getEngineUserAccessRequestReturnsTheRequestsForAnEditor() {
		theRequesterAsksForTheEngine();

		NounMetadata result = run(new GetEngineUserAccessRequestReactor(), anEditorWhoIsNotAnAdmin(),
				Map.of("engine", ENGINE));

		assertEquals(List.of(REQUESTER_ID),
				rowsOf(result).stream().map(row -> row.get("REQUEST_USERID")).toList());
	}

	/**
	 * Someone who asked for access is not yet someone who can read who else did.
	 */
	@Test
	void getEngineUserAccessRequestRefusesAUserWhoCannotEditTheEngine() {
		theRequesterAsksForTheEngine();

		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> run(new GetEngineUserAccessRequestReactor(), this.requester, Map.of("engine", ENGINE)));

		assertEquals(NO_ACCESS_TO_REQUESTS, refusal.getMessage());
	}

}
