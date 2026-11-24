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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

import org.eclipse.jgit.api.Git;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import com.google.gson.JsonParser;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelRouterEngine;
import prerna.engine.impl.model.ModelRouterEngine;
import prerna.om.Insight;
import prerna.sablecc2.om.PixelDataType;
import prerna.usertracking.AuditEventType;
import prerna.usertracking.AuditTargetType;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.util.Utility;

class UpdateModelRouterConfigReactorUnitTests {

	@TempDir
	Path versionFolder;

	private static final String JSON = "{\"mode\":\"subtask\",\"version\":\"v7\"}";

	@Test
	void aSubtaskConfigIsCommittedAndAuditedWithItsHashAndVersionOtherModesAreNot() throws Exception {
		Files.createDirectories(versionFolder.resolve("assets"));
		Files.writeString(versionFolder.resolve("assets/router.json"), JSON);
		Insight insight = mock(Insight.class);
		User user = mock(User.class);
		when(insight.getUser()).thenReturn(user);
		IModelRouterEngine model = mock(IModelRouterEngine.class);
		when(model.getSmssProp()).thenReturn(new Properties());

		try (MockedStatic<UserAuditTrailUtils> audit = mockStatic(UserAuditTrailUtils.class);
				MockedStatic<UpdateModelRouterConfigReactor> folder = mockStatic(UpdateModelRouterConfigReactor.class,
						i -> i.getMethod().getName().equals("versionFolder") ? versionFolder.toString() : i.callRealMethod());
				MockedStatic<SecurityEngineUtils> security = mockStatic(SecurityEngineUtils.class)) {
			security.when(() -> SecurityEngineUtils.getEngineDisplayNameForId("e1")).thenReturn("Router");
			UpdateModelRouterConfigReactor.save(insight, "e1", model, JSON, "a note");

			ArgumentCaptor<Object> details = ArgumentCaptor.forClass(Object.class);
			audit.verify(() -> UserAuditTrailUtils.recordEngineLifecycle(eq(user), eq(AuditEventType.ENGINE_UPDATE), eq(AuditTargetType.MODEL),
					eq("e1"), eq("Router"), details.capture()));
			Map<?, ?> recorded = (Map<?, ?>) details.getValue();
			assertEquals(ModelRouterEngine.sha256(JSON), recorded.get("sha256"));
			assertEquals("v7", recorded.get("version").toString().replace("\"", ""));
			assertEquals("a note", recorded.get("note"));
			assertEquals(true, recorded.get("committed"));

			// a keyword router is written but not versioned
			audit.clearInvocations();
			UpdateModelRouterConfigReactor.save(insight, "e1", model, "{\"mode\":\"keyword\"}", null);
			audit.verifyNoInteractions();
		}
		// the engine had no repository: one was created and the file committed
		try (Git git = Git.open(versionFolder.toFile())) {
			assertTrue(git.log().call().iterator().next().getFullMessage().contains("Router config"));
			assertNotNull(git.getRepository().resolve("HEAD:assets/router.json"));
		}
	}

	@Test
	void configEditsCannotReplaceOrAccidentallyDropTheSavedJobId() {
		var live = JsonParser.parseString("{\"improvement\":{\"scheduler_job_id\":\"job-1\"}}").getAsJsonObject();
		var edit = JsonParser.parseString("{\"improvement\":{\"min_new_tasks\":120}}").getAsJsonObject();
		UpdateModelRouterConfigReactor.preserveManagedLearningSettings(live, edit);
		assertEquals("job-1", edit.getAsJsonObject("improvement").get("scheduler_job_id").getAsString());
		edit.getAsJsonObject("improvement").addProperty("scheduler_job_id", "job-2");
		assertThrows(IllegalArgumentException.class, () -> UpdateModelRouterConfigReactor.preserveManagedLearningSettings(live, edit));
	}

	@Test
	void anAgentRunCannotWriteARouterWithLearningOn() throws Exception {
		IModelRouterEngine on = mock(IModelRouterEngine.class);
		when(on.readConfigJson()).thenReturn(ApplyRouterCandidateReactorUnitTests.config("v1", "\"improvement\":{\"enabled\":true}"));
		Insight agentRun = mock(Insight.class);
		when(agentRun.getRoomId()).thenReturn("room-1");

		try (MockedStatic<SecurityEngineUtils> security = mockStatic(SecurityEngineUtils.class);
				MockedStatic<Utility> utility = mockStatic(Utility.class)) {
			security.when(() -> SecurityEngineUtils.userCanEditEngine(any(), any())).thenReturn(true);
			utility.when(() -> Utility.getModel("rt")).thenReturn(on);
			UpdateModelRouterConfigReactor refused = ApplyRouterCandidateReactorUnitTests.with(new UpdateModelRouterConfigReactor(), agentRun,
					"engine", "rt");
			refused.getNounStore().makeNoun("map").add(Map.of("mode", "subtask"), PixelDataType.MAP);
			assertThrows(SecurityException.class, refused::execute);
		}
	}
}
