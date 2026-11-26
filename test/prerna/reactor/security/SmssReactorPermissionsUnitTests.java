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
package prerna.reactor.security;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.auth.utils.AbstractReactorPermissionUnitTestsSetup;

/**
 * Who GetDatabaseSMSS and GetProjectSMSS work for: the owner of the database
 * or project. An admin holds their own permissions in them, like anyone else,
 * and reads the smss of someone else's with AdminGetEngineSMSS and
 * AdminGetProjectSMSS. The rule is the section "Admin operations" of
 * docs/platform_services/engine_access.md.
 */
class SmssReactorPermissionsUnitTests extends AbstractReactorPermissionUnitTestsSetup {

	/**
	 * Being an admin does not stand in for owning the database.
	 */
	@Test
	void getDatabaseSmssRefusesAnAdminWhoHoldsNoPermissionOnTheDatabase() {
		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> run(new GetDatabaseSMSSReactor(), anAdminWhoHoldsNothing(), Map.of("database", ENGINE)));

		assertTrue(refusal.getMessage().contains(NO_PERMISSION), refusal.getMessage());
	}

	/**
	 * Being an admin does not stand in for owning the project.
	 */
	@Test
	void getProjectSmssRefusesAnAdminWhoHoldsNoPermissionOnTheProject() {
		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> run(new GetProjectSMSSReactor(), anAdminWhoHoldsNothing(), Map.of("project", PROJECT)));

		assertTrue(refusal.getMessage().contains(NO_PERMISSION), refusal.getMessage());
	}

	/**
	 * An editor is one level short of the owner these reactors serve.
	 */
	@Test
	void getDatabaseSmssRefusesAnEditor() {
		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> run(new GetDatabaseSMSSReactor(), anEditorWhoIsNotAnAdmin(), Map.of("database", ENGINE)));

		assertTrue(refusal.getMessage().contains(NO_PERMISSION), refusal.getMessage());
	}

	/**
	 * An editor is one level short of the owner these reactors serve.
	 */
	@Test
	void getProjectSmssRefusesAnEditor() {
		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> run(new GetProjectSMSSReactor(), anEditorWhoIsNotAnAdmin(), Map.of("project", PROJECT)));

		assertTrue(refusal.getMessage().contains(NO_PERMISSION), refusal.getMessage());
	}

}
