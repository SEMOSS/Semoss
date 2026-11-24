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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.engine.api.IModelRouterEngine;
import prerna.om.Insight;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** A rollback restores a committed version as a paused new commit (resumed when it undoes a rollback) and refuses what it cannot restore. */
class RollbackRouterConfigReactorUnitTests {

	@TempDir
	Path folder;

	private static final String IMPROVEMENT = "\"improvement\":{\"enabled\":true,\"guardrails\":{\"min_tasks\":7}}";
	private final IModelRouterEngine engine = mock(IModelRouterEngine.class);
	private final List<String[]> saves = new ArrayList<>();
	private final Insight insight = mock(Insight.class);
	private MockedStatic<UpdateModelRouterConfigReactor> updater;

	@BeforeEach
	void setUp() {
		updater = ApplyRouterCandidateReactorUnitTests.stubUpdater(Map.of("rt", engine), folder, saves);
	}

	@AfterEach
	void tearDown() {
		updater.close();
	}

	private RollbackRouterConfigReactor reactor(String version) {
		return ApplyRouterCandidateReactorUnitTests.with(new RollbackRouterConfigReactor(), insight, "engine", "rt", "version", version);
	}

	@Test
	void restoresTheOldVersionWithTheLiveImprovementPausedAndRefusesUnknownOrLiveVersions() throws Exception {
		String live = ApplyRouterCandidateReactorUnitTests.config("v2", IMPROVEMENT);
		ApplyRouterCandidateReactorUnitTests.commits(folder, "one", ApplyRouterCandidateReactorUnitTests.config("v1", null), "two", live);
		when(engine.readConfigJson()).thenReturn(live);
		assertTrue(assertThrows(IllegalArgumentException.class, () -> reactor("v9").execute()).getMessage().contains("no committed version v9"));
		assertTrue(assertThrows(IllegalArgumentException.class, () -> reactor("v2").execute()).getMessage().contains("already live"));
		assertTrue(saves.isEmpty());

		NounMetadata result = reactor("v1").execute();
		assertEquals("rolled_back", ((Map<?, ?>) result.getValue()).get("status"));
		JsonObject restored = JsonParser.parseString(saves.get(0)[0]).getAsJsonObject();
		assertEquals("v1", restored.get("version").getAsString());
		assertTrue(restored.getAsJsonObject("improvement").get("paused").getAsBoolean());
		assertEquals(7, restored.getAsJsonObject("improvement").getAsJsonObject("guardrails").get("min_tasks").getAsInt());
		assertTrue(saves.get(0)[1].contains("rolled back v2 to v1"));
	}

	@Test
	void rollingForwardToTheVersionLastRolledBackFromResumesLearning() throws Exception {
		String v2 = ApplyRouterCandidateReactorUnitTests.config("v2", IMPROVEMENT);
		String v1 = ApplyRouterCandidateReactorUnitTests.config("v1", "\"improvement\":{\"enabled\":true,\"paused\":true}");
		ApplyRouterCandidateReactorUnitTests.commits(folder, "one", v2, "RollbackRouterConfig: rolled back v2 to v1 by request", v1);
		when(engine.readConfigJson()).thenReturn(v1);
		NounMetadata result = reactor("v2").execute();

		assertEquals(false, ((Map<?, ?>) result.getValue()).get("paused"));
		assertEquals(false, JsonParser.parseString(saves.get(0)[0]).getAsJsonObject().getAsJsonObject("improvement").get("paused").getAsBoolean());
	}

	@Test
	void rollingBackAgainAfterARollForwardPausesLearningAgain() throws Exception {
		String v1 = ApplyRouterCandidateReactorUnitTests.config("v1", IMPROVEMENT);
		String v2 = ApplyRouterCandidateReactorUnitTests.config("v2", IMPROVEMENT);
		ApplyRouterCandidateReactorUnitTests.commits(folder, "one", v1, "RollbackRouterConfig: rolled back v1 to v2 by request", v2);
		when(engine.readConfigJson()).thenReturn(v2);
		NounMetadata result = reactor("v1").execute();

		assertEquals(true, ((Map<?, ?>) result.getValue()).get("paused"));
	}

	@Test
	void anAgentRunCannotRollBack() {
		Insight agentRun = mock(Insight.class);
		when(agentRun.getRoomId()).thenReturn("room-1");
		assertThrows(SecurityException.class, () -> ApplyRouterCandidateReactorUnitTests.with(new RollbackRouterConfigReactor(), agentRun).execute());
	}
}
