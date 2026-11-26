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
package prerna.reactor.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import prerna.auth.User;
import prerna.auth.utils.AbstractReactorPermissionUnitTestsSetup;
import prerna.reactor.AbstractReactor;
import prerna.reactor.app.ExportAppReactor;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Who the reactors that delete, close, export and back up an engine or project
 * work for: its owner. An admin holds their own permissions in them, like
 * anyone else, and works on someone else's engine or project through the admin
 * reactors. The rule is the section "Admin operations" of
 * docs/platform_services/engine_access.md.
 */
class EngineAndProjectReactorPermissionsUnitTests extends AbstractReactorPermissionUnitTestsSetup {

	/**
	 * Every reactor that serves an owner, with inputs that name the database or
	 * project of the test. ExportDatabase and ExportApp are here because they run
	 * the check of the reactor they extend.
	 *
	 * @return one case for each reactor
	 */
	static Stream<Arguments> ownerReactors() {
		return Stream.of(reactor(new DeleteEngineReactor(), Map.of("engine", ENGINE)),
				reactor(new DeleteDatabaseReactor(), Map.of("database", ENGINE)),
				reactor(new DeleteProjectReactor(), Map.of("project", PROJECT)),
				reactor(new CloseEngineReactor(), Map.of("engine", ENGINE)),
				reactor(new ExportEngineReactor(), Map.of("engine", ENGINE)),
				reactor(new ExportDatabaseReactor(), Map.of("database", ENGINE)),
				reactor(new ExportProjectReactor(), Map.of("project", PROJECT)),
				reactor(new ExportProjectAppReactor(), Map.of("project", PROJECT)),
				reactor(new ExportAppReactor(), Map.of("project", PROJECT)),
				reactor(new BackupDatabaseReactor(), Map.of("database", ENGINE)));
	}

	/**
	 * The reactors that delete, which a setting can leave to admins, with inputs
	 * that name the database or project of the test.
	 *
	 * @return one case for each reactor
	 */
	static Stream<Arguments> deleteReactors() {
		return Stream.of(reactor(new DeleteEngineReactor(), Map.of("engine", ENGINE)),
				reactor(new DeleteDatabaseReactor(), Map.of("database", ENGINE)),
				reactor(new DeleteProjectReactor(), Map.of("project", PROJECT)));
	}

	/**
	 * Being an admin does not stand in for owning the engine or project.
	 */
	@ParameterizedTest
	@MethodSource("ownerReactors")
	void theReactorRefusesAnAdminWhoHoldsNoPermissionOnTheResource(AbstractReactor reactor,
			Map<String, Object> arguments) {
		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> run(reactor, anAdminWhoHoldsNothing(), arguments));

		assertTrue(refusal.getMessage().contains(NO_PERMISSION), refusal.getMessage());
	}

	/**
	 * An editor is one level short of the owner these reactors serve.
	 */
	@ParameterizedTest
	@MethodSource("ownerReactors")
	void theReactorRefusesAnEditor(AbstractReactor reactor, Map<String, Object> arguments) {
		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> run(reactor, anEditorWhoIsNotAnAdmin(), arguments));

		assertTrue(refusal.getMessage().contains(NO_PERMISSION), refusal.getMessage());
	}

	/**
	 * With deleting left to admins, owning the engine or project is not enough for
	 * a user who is not an admin.
	 */
	@ParameterizedTest
	@MethodSource("deleteReactors")
	void aDeleteReactorRefusesAnOwnerWhoIsNotAnAdminWhileOnlyAdminsDelete(AbstractReactor reactor,
			Map<String, Object> arguments) {
		onlyAdminsDelete();

		SemossPixelException refusal = assertThrows(SemossPixelException.class,
				() -> run(reactor, anOwnerWhoIsNotAnAdmin(), arguments));

		assertEquals(LIMITED_TO_ADMINS, refusal.getMessage());
	}

	/**
	 * With deleting left to admins, the reactor every user runs refuses an admin
	 * too, even one who owns the engine or project: an admin deletes through the
	 * admin counterpart.
	 */
	@ParameterizedTest
	@MethodSource("deleteReactors")
	void aDeleteReactorRefusesAnAdminWhoOwnsTheResourceWhileOnlyAdminsDelete(AbstractReactor reactor,
			Map<String, Object> arguments) throws Exception {
		onlyAdminsDelete();
		User adminOwner = anAdminWhoOwnsTheDatabaseAndTheProject();

		SemossPixelException refusal = assertThrows(SemossPixelException.class,
				() -> run(reactor, adminOwner, arguments));

		assertEquals(LIMITED_TO_ADMINS, refusal.getMessage());
	}

	/**
	 * The engine of the test is not open on this node, so closing it runs the whole
	 * reactor without engine files.
	 */
	@Test
	void closeEngineClosesAnEngineForItsOwner() {
		NounMetadata result = run(new CloseEngineReactor(), anOwnerWhoIsNotAnAdmin(), Map.of("engine", ENGINE));

		assertEquals(true, result.getValue());
	}

	/**
	 * One case of a method source, shown in the test report by the name of the
	 * reactor.
	 *
	 * @param reactor   the reactor
	 * @param arguments inputs that name the resource of the test
	 * @return the case
	 */
	private static Arguments reactor(AbstractReactor reactor, Map<String, Object> arguments) {
		return Arguments.of(Named.of(reactor.getClass().getSimpleName(), reactor), arguments);
	}

}
