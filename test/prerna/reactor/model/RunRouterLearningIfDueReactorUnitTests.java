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
package prerna.reactor.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import prerna.engine.api.IModelRouterEngine;
import prerna.engine.impl.model.ModelRouterEngine.ReplaySettings;
import prerna.om.Insight;

/** The learning job starts only after its configured volume trigger and safe task-set checks. */
class RunRouterLearningIfDueReactorUnitTests {

	@TempDir
	Path folder;

	private boolean running;

	private boolean enough = true;

	private Map<?, ?> check(String improvement, long newTasks) throws Exception {
		IModelRouterEngine engine = mock(IModelRouterEngine.class);
		when(engine.readConfigJson()).thenReturn(ApplyRouterCandidateReactorUnitTests.config("v1", improvement));
		try (MockedStatic<UpdateModelRouterConfigReactor> updater = ApplyRouterCandidateReactorUnitTests.stubUpdater(Map.of("rt", engine), folder,
				new ArrayList<>())) {
			return (Map<?, ?>) ApplyRouterCandidateReactorUnitTests.with(new RunRouterLearningIfDueReactor() {
				@Override
				long newTasks(String engineId) {
					return newTasks;
				}

				@Override
				boolean attemptRunning(String engineId) {
					return running;
				}

				@Override
				RouterLearningExamples.Built taskSets(String engineId, ReplaySettings settings) {
					return enough ? new RouterLearningExamples.Built("train", "val", 0) : null;
				}

				@Override
				boolean start(String engineId, String optimizerModel, RouterLearningExamples.Built sets, ReplaySettings settings) {
					return true;
				}

			}, mock(Insight.class), "engine", "rt").execute().getValue();
		}
	}

	private static final String ON = "\"improvement\":{\"enabled\":true,\"optimizer_model\":\"judge\",\"min_new_tasks\":100}";

	@Test
	void theConfiguredVolumeThresholdAndUsableTaskSetsGateOneAttempt() throws Exception {
		assertEquals(Map.of("due", false, "new_tasks", 99L, "reason", "new tasks have not reached min_new_tasks"), check(ON, 99));
		assertEquals(Map.of("due", true, "new_tasks", 100L), check(ON, 100));
		running = true;
		assertEquals(Map.of("due", false, "new_tasks", 500L, "reason", "a learning attempt is still running"), check(ON, 500));
		running = false;
		enough = false;
		assertEquals(Map.of("due", false, "new_tasks", 500L, "reason", "not enough usable recorded tasks yet"), check(ON, 500));
	}

	@Test
	void theStartLockRechecksForAnAttemptStartedAfterTheInitialCheck() throws Exception {
		IModelRouterEngine engine = mock(IModelRouterEngine.class);
		when(engine.readConfigJson()).thenReturn(ApplyRouterCandidateReactorUnitTests.config("v1", ON));
		Insight insight = mock(Insight.class);
		try (MockedStatic<UpdateModelRouterConfigReactor> updater = ApplyRouterCandidateReactorUnitTests.stubUpdater(Map.of("rt", engine), folder,
				new ArrayList<>())) {
			int[] checks = { 0 };
			Map<?, ?> result = (Map<?, ?>) ApplyRouterCandidateReactorUnitTests.with(new RunRouterLearningIfDueReactor() {
				@Override
				long newTasks(String engineId) {
					return 100;
				}

				@Override
				boolean attemptRunning(String engineId) {
					return ++checks[0] > 1;
				}

				@Override
				RouterLearningExamples.Built taskSets(String engineId, ReplaySettings settings) {
					return new RouterLearningExamples.Built("train", "val", 0);
				}
			}, insight, "engine", "rt").execute().getValue();
			assertEquals(Map.of("due", false, "new_tasks", 100L, "reason", "a learning attempt is still running"), result);
			assertEquals(2, checks[0]);
			verify(insight).getRoomId();
			verify(insight, never()).runPixel(anyString());
		}
	}

	@Test
	void theLearningRoomFitsTheRoomIdColumnForAnEngineUuid() {
		assertTrue(RunRouterLearningIfDueReactor.newLearningRoom().length() <= 50);
	}

	@Test
	void doesNothingWhileLearningIsOffOrPausedAndRefusesAnAgentRun() throws Exception {
		assertEquals(false, check("\"improvement\":{\"enabled\":false}", 1000).get("due"));
		assertEquals(false, check("\"improvement\":{\"enabled\":true,\"paused\":true,\"optimizer_model\":\"judge\"}", 1000).get("due"));
		assertEquals(false, check(null, 1000).get("due"));

		Insight agentRun = mock(Insight.class);
		when(agentRun.getRoomId()).thenReturn("room-1");
		assertThrows(SecurityException.class, () -> ApplyRouterCandidateReactorUnitTests.with(new RunRouterLearningIfDueReactor(), agentRun).execute());
	}
}
