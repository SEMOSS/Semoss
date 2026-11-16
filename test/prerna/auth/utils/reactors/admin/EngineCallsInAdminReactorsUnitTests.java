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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import prerna.auth.utils.AbstractReactorPermissionUnitTestsSetup;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.pipeline.StubEngines;
import prerna.om.ThreadStore;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * Admin reactors calling a loaded engine for an admin who holds nothing on it.
 * The engine is a stand-in kept where loaded engines are kept, so
 * {@code Utility.getDatabase} hands it out behind the real engine proxy, and
 * each call on it runs the real access check. The admin passes that check only
 * while an admin reactor is acting, so this class's name does not start with
 * {@code Admin}: it must not be what lets the calls through.
 */
class EngineCallsInAdminReactorsUnitTests extends AbstractReactorPermissionUnitTestsSetup {

	/**
	 * Takes the stand-in engine out of the loaded engines.
	 */
	@AfterEach
	void unloadTheEngine() {
		DIHelper.getInstance().removeEngineProperty(ENGINE);
	}

	@Test
	void outsideAnAdminReactorTheAdminCannotCallTheEngine() {
		DIHelper.getInstance().setEngineProperty(ENGINE, StubEngines.unguardedDatabase(ENGINE, Map.of()));
		ThreadStore.setUser(anAdminWhoHoldsNothing());

		assertThrows(SemossPixelException.class, () -> Utility.getDatabase(ENGINE).getSmssFilePath());
	}

	@Test
	void adminReloadDatabaseReloadsADatabaseForAnAdminWhoHoldsNothing() {
		List<String> calls = new ArrayList<>();
		DIHelper.getInstance().setEngineProperty(ENGINE,
				StubEngines.unguardedDatabase(ENGINE, Map.of("getSmssFilePath", () -> "db.smss", "close",
						() -> calls.add("close"), "open", () -> calls.add("open"))));

		NounMetadata result = run(new AdminReloadDatabaseReactor(), anAdminWhoHoldsNothing(),
				Map.of("database", ENGINE));

		assertEquals(true, result.getValue());
		assertEquals(List.of("close", "open"), calls);
	}

	@Test
	void adminRdbmsReconnectClosesTheConnectionsForAnAdminWhoHoldsNothing() {
		List<String> calls = new ArrayList<>();
		IRDBMSEngine database = StubEngines.unguardedDatabase(ENGINE,
				Map.of("isConnectionPooling", () -> true, "closeDataSource", () -> calls.add("closeDataSource")));
		DIHelper.getInstance().setEngineProperty(ENGINE, database);

		NounMetadata result = run(new AdminRdbmsReconnectReactor(), anAdminWhoHoldsNothing(),
				Map.of("database", ENGINE));

		assertEquals(true, result.getValue());
		assertEquals(List.of("closeDataSource"), calls);
	}

}
