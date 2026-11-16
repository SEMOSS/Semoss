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
package prerna.reactor.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.auth.utils.AbstractReactorPermissionUnitTestsSetup;

/**
 * Who ReloadDatabase and RdbmsReconnect work for: the editors of the database.
 * An admin holds their own permissions in them, like anyone else, and works on
 * someone else's database through the admin reactors. The rule is the section
 * "Admin operations" of docs/platform_services/engine_access.md.
 */
class DatabaseReactorPermissionsUnitTests extends AbstractReactorPermissionUnitTestsSetup {

	/**
	 * Being an admin does not stand in for being able to edit the database.
	 */
	@Test
	void reloadDatabaseRefusesAnAdminWhoHoldsNoPermissionOnTheDatabase() {
		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> run(new ReloadDatabaseReactor(), anAdminWhoHoldsNothing(), Map.of("database", ENGINE)));

		assertEquals("User does not have permission to reload the database", refusal.getMessage());
	}

	/**
	 * Being an admin does not stand in for being able to edit the database.
	 */
	@Test
	void rdbmsReconnectRefusesAnAdminWhoHoldsNoPermissionOnTheDatabase() {
		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> run(new RdbmsReconnectReactor(), anAdminWhoHoldsNothing(), Map.of("database", ENGINE)));

		assertEquals("User does not have permission to re-establish the connection for this database",
				refusal.getMessage());
	}

}
