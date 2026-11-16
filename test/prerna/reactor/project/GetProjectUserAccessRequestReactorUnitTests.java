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
package prerna.reactor.project;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.auth.utils.AbstractReactorPermissionUnitTestsSetup;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Who GetProjectUserAccessRequest works for: the editors of the project. An
 * admin holds their own permissions in it, like anyone else, and reads the
 * requests of someone else's project through AdminGetProjectUserAccessRequest.
 * The rule is the section "Admin operations" of
 * docs/platform_services/engine_access.md.
 */
class GetProjectUserAccessRequestReactorUnitTests extends AbstractReactorPermissionUnitTestsSetup {

	private static final String NO_ACCESS_TO_REQUESTS = "User does not have permission to view access requests for "
			+ "this project";

	/**
	 * Being an admin does not stand in for being able to edit the project.
	 */
	@Test
	void anAdminWhoHoldsNoPermissionOnTheProjectIsRefused() {
		theRequesterAsksForTheProject();

		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> run(new GetProjectUserAccessRequestReactor(), anAdminWhoHoldsNothing(),
						Map.of("project", PROJECT)));

		assertEquals(NO_ACCESS_TO_REQUESTS, refusal.getMessage());
	}

	/**
	 * Reading a project's access requests touches the security database only, so
	 * the whole reactor runs here for an editor.
	 */
	@Test
	void anEditorGetsTheRequests() {
		theRequesterAsksForTheProject();

		NounMetadata result = run(new GetProjectUserAccessRequestReactor(), anEditorWhoIsNotAnAdmin(),
				Map.of("project", PROJECT));

		assertEquals(List.of(REQUESTER_ID),
				rowsOf(result).stream().map(row -> row.get("REQUEST_USERID")).toList());
	}

	/**
	 * Someone who asked for access is not yet someone who can read who else did.
	 */
	@Test
	void aUserWhoCannotEditTheProjectIsRefused() {
		theRequesterAsksForTheProject();

		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> run(new GetProjectUserAccessRequestReactor(), this.requester, Map.of("project", PROJECT)));

		assertEquals(NO_ACCESS_TO_REQUESTS, refusal.getMessage());
	}

}
