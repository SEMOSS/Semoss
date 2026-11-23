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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.auth.utils.AbstractReactorPermissionUnitTestsSetup;
import prerna.auth.utils.CatalogTypes;

/**
 * Who SyncCatalogDependencies works for: a user who can edit the engine or
 * project, as SetCatalogDependencies.
 */
class SyncCatalogDependenciesReactorUnitTests extends AbstractReactorPermissionUnitTestsSetup {

	private static final String CANNOT_EDIT = "does not have access to edit";

	/**
	 * Being an admin does not stand in for being able to edit the engine.
	 */
	@Test
	void refusesAnAdminWhoHoldsNoPermissionOnTheEngine() {
		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> run(new SyncCatalogDependenciesReactor(), anAdminWhoHoldsNothing(), Map.of("catalogId", ENGINE)));

		assertTrue(refusal.getMessage().contains(CANNOT_EDIT), refusal.getMessage());
	}

	/**
	 * A user with no permission on the project cannot sync it.
	 */
	@Test
	void refusesAUserWithNoPermissionOnTheProject() {
		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> run(new SyncCatalogDependenciesReactor(), this.requester, Map.of("catalogId", PROJECT)));

		assertTrue(refusal.getMessage().contains(CANNOT_EDIT), refusal.getMessage());
	}

	/**
	 * An editor can sync; a project type with nothing to derive is left as it is.
	 */
	@Test
	@SuppressWarnings("unchecked")
	void syncsForAnEditorAndCountsTheAutomaticDependencies() {
		Map<String, Object> result = (Map<String, Object>) run(new SyncCatalogDependenciesReactor(),
				anEditorWhoIsNotAnAdmin(), Map.of("catalogId", PROJECT)).getValue();

		assertEquals(PROJECT, result.get("catalogId"));
		assertEquals(CatalogTypes.resolve(PROJECT), result.get("catalogType"));
		assertEquals(0, result.get("automatic"));
	}

	/**
	 * A type that is not the item's is refused before anything is read.
	 */
	@Test
	void refusesATypeThatIsNotTheItems() {
		assertThrows(IllegalArgumentException.class, () -> run(new SyncCatalogDependenciesReactor(),
				anOwnerWhoIsNotAnAdmin(), Map.of("catalogId", ENGINE, "catalogType", "MODEL")));
	}
}
