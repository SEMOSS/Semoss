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
package prerna.auth.utils.reactors.admin;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import prerna.auth.User;
import prerna.auth.utils.AbstractReactorPermissionUnitTestsSetup;
import prerna.reactor.AbstractReactor;
import prerna.util.SemossClassLoader;

/**
 * The reactors only an admin runs that have no counterpart for other users.
 * Each is named for an admin, so project code cannot load it and the engine
 * access check counts it as an admin operation, says so in its description,
 * and refuses anyone who is not an admin.
 */
class AdminOnlyReactorsUnitTests extends AbstractReactorPermissionUnitTestsSetup {

	static Stream<Arguments> adminOnlyReactors() throws Exception {
		return Stream.of(new AdminAddBlockReactor(), new AdminDeleteBlockReactor(),
				new AdminSetDatabaseMetakeyOptionsReactor(), new AdminSetProjectMetakeyOptionsReactor(),
				new AdminSetInsightMetakeyOptionsReactor(), new AdminUnlockEngineReactor(),
				new AdminUnlockProjectsReactor(), new AdminLiveKitListRoomsReactor(),
				new AdminSyncModelMetadataReactor(), new AdminMigrateImagesToCouchReactor(),
				new AdminSchedulerStatsReactor())
				.map(reactor -> Arguments.of(Named.of(reactor.getClass().getSimpleName(), reactor)));
	}

	/**
	 * The sandbox refuses the class to project code, and the engine access check
	 * lets an admin through while it runs.
	 */
	@ParameterizedTest
	@MethodSource("adminOnlyReactors")
	void anAdminOnlyReactorIsNamedForAdmins(AbstractReactor reactor) {
		assertTrue(SemossClassLoader.requiresAdmin(reactor.getClass().getName()), reactor.getClass().getName());
	}

	/**
	 * The description starts with "Admin Only: ", as every admin reactor's does.
	 */
	@ParameterizedTest
	@MethodSource("adminOnlyReactors")
	void anAdminOnlyReactorSaysItIsForAdminsOnly(AbstractReactor reactor) {
		String description = reactor.getReactorDefinition().getDescription();

		assertTrue(description.startsWith("Admin Only: "), description);
	}

	/**
	 * Owning the engine and the project gives nothing in a reactor only admins
	 * run.
	 */
	@ParameterizedTest
	@MethodSource("adminOnlyReactors")
	void anAdminOnlyReactorRefusesAnOwnerWhoIsNotAnAdmin(AbstractReactor reactor) {
		User owner = anOwnerWhoIsNotAnAdmin();

		RuntimeException refusal = assertThrows(RuntimeException.class, () -> run(reactor, owner, Map.of()));

		assertTrue(refusal.getMessage().toLowerCase().contains("admin"), refusal.getMessage());
	}

}
