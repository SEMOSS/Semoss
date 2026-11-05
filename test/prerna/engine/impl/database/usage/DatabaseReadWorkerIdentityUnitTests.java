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
package prerna.engine.impl.database.usage;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.json.JSONArray;
import org.junit.jupiter.api.Test;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.ThreadStore;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.tcp.PayloadStruct;
import prerna.tcp.client.workers.NativePyEngineWorker;
import prerna.util.Utility;

class DatabaseReadWorkerIdentityUnitTests {
	private static PayloadStruct request() {
		var payload = new PayloadStruct();
		payload.objId = "db";
		payload.engineType = "DATABASE";
		payload.methodName = "execquery";
		payload.payload = new Object[] { "SELECT 1", "json" };
		return payload;
	}

	@Test
	void workerUsesItsAuthenticatedActorDuringExecutionAndCleanupThenRestoresTheThread() throws Exception {
		var actor = mock(User.class);
		var previous = ThreadStore.getUser();
		var engine = mock(IDatabaseEngine.class);
		var wrapper = mock(IRawSelectWrapper.class);
		var manager = mock(WrapperManager.class);
		try (var security = mockStatic(SecurityEngineUtils.class);
				var utility = mockStatic(Utility.class);
				var wrappers = mockStatic(WrapperManager.class)) {
			security.when(() -> SecurityEngineUtils.userCanViewEngine(actor, "db")).thenAnswer(call -> {
				assertSame(actor, ThreadStore.getUser());
				return true;
			});
			utility.when(() -> Utility.getDatabase("db")).thenReturn(engine);
			wrappers.when(WrapperManager::getInstance).thenReturn(manager);
			when(manager.getRawWrapper(engine, "SELECT 1")).thenAnswer(call -> {
				assertSame(actor, ThreadStore.getUser());
				return wrapper;
			});
			utility.when(() -> Utility.writeResultToJsonObject(eq(wrapper), isNull(), any()))
					.thenReturn(new JSONArray());
			doAnswer(call -> {
				assertSame(actor, ThreadStore.getUser());
				return null;
			}).when(wrapper).close();
			var worker = new NativePyEngineWorker(actor, request());
			worker.run();
			assertNull(worker.getOutput().ex);
			assertTrue(worker.getOutput().response);
			verify(wrapper).close();
			assertSame(previous, ThreadStore.getUser());
		} finally {
			ThreadStore.setUser(previous);
		}
	}

	@Test
	void deniedWorkerAlsoRestoresThePreviousIdentity() {
		var actor = mock(User.class);
		var previous = ThreadStore.getUser();
		try (var security = mockStatic(SecurityEngineUtils.class)) {
			security.when(() -> SecurityEngineUtils.userCanViewEngine(actor, "db")).thenAnswer(call -> {
				assertSame(actor, ThreadStore.getUser());
				return false;
			});
			var worker = new NativePyEngineWorker(actor, request());
			worker.run();
			assertNotNull(worker.getOutput().ex);
			assertTrue(worker.getOutput().response);
			assertSame(previous, ThreadStore.getUser());
		} finally {
			ThreadStore.setUser(previous);
		}
	}
}
