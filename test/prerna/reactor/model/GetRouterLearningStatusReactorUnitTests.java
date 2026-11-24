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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import prerna.engine.api.IModelRouterEngine;
import prerna.om.Insight;

/** The status read: config facts, git versions, the schedule of this engine's learning job and the cost per version. */
class GetRouterLearningStatusReactorUnitTests {

	@TempDir
	Path folder;

	@SuppressWarnings("unchecked")
	@Test
	void reportsTheSwitchOptimizerModelVersionsScheduleAndCost() throws Exception {
		String live = ApplyRouterCandidateReactorUnitTests.config("v2",
				"\"improvement\":{\"enabled\":true,\"paused\":true,\"optimizer_model\":\"judge-1\",\"max_quality_loss\":0.07,\"scheduler_job_id\":\"job-42\"}");
		ApplyRouterCandidateReactorUnitTests.commits(folder, "one", ApplyRouterCandidateReactorUnitTests.config("v1", null), "two", live);
		IModelRouterEngine engine = mock(IModelRouterEngine.class);
		when(engine.readConfigJson()).thenReturn(live);
		try (MockedStatic<UpdateModelRouterConfigReactor> updater = ApplyRouterCandidateReactorUnitTests.stubUpdater(Map.of("rt", engine), folder,
				new ArrayList<>()); MockedStatic<SetRouterLearningReactor> scheduler = mockStatic(SetRouterLearningReactor.class);
				MockedStatic<GetRouterEvidenceReactor> evidence = mockStatic(GetRouterEvidenceReactor.class)) {
			scheduler.when(() -> SetRouterLearningReactor.findBoundJob("rt", "job-42"))
					.thenReturn(new SetRouterLearningReactor.BoundJob("0 0 3 ? * SUN"));
			evidence.when(() -> GetRouterEvidenceReactor.collect(any(), any(), anyInt(), anyInt()))
					.thenReturn(Map.of("by_version", List.of(Map.of("config_version", "v1"))));
			evidence.when(() -> GetRouterEvidenceReactor.newTasksSinceLastAttempt(any(), any())).thenReturn(42L);
			Map<String, Object> status = (Map<String, Object>) ApplyRouterCandidateReactorUnitTests
					.with(new GetRouterLearningStatusReactor(), mock(Insight.class), "engine", "rt").execute().getValue();

			assertEquals(true, status.get("enabled"));
			assertEquals(true, status.get("paused"));
			assertEquals("judge-1", status.get("optimizer_model"));
			assertEquals("v2", status.get("current_version"));
			assertEquals("subtask", status.get("mode"));
			assertEquals(0.07, (double) status.get("max_quality_loss"), 1e-9);
			assertEquals("weekly", status.get("frequency"));
			assertEquals("job-42", status.get("scheduler_job_id"));
			assertEquals("bound", status.get("scheduler_job_binding"));
			assertEquals(List.of("v2", "v1"), ((List<Map<String, Object>>) status.get("versions")).stream().map(v -> v.get("version")).toList());
			assertTrue(((List<?>) status.get("cost_by_version")).size() == 1);
			assertEquals(100, status.get("min_new_tasks"));
			assertEquals(42L, status.get("new_tasks_since_last_attempt"));
			assertEquals(List.of("initial setup"), ((List<Map<String, Object>>) status.get("versions")).get(1).get("changes"));
		}
	}
}
