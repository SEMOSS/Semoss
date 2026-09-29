/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	you may obtain a copy of the License at
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
package prerna.engine.impl.model.inferencetracking.reactors.workspaces;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.om.Insight;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

class EditOrchestratorRosterReactorUnitTests {

	@Test
	void editorCanReplaceOnlyTheOrchestratorRoster() {
		EditWorkspaceReactor reactor = new EditWorkspaceReactor();
		Insight insight = mock(Insight.class);
		User user = mock(User.class);
		when(insight.getUser()).thenReturn(user);
		reactor.setInsight(insight);

		NounStore store = new NounStore("test");
		add(store, "workspaceId", Constants.AGENT_ORCHESTRATOR, PixelDataType.CONST_STRING);
		add(store, "name", "Orchestrator Agent", PixelDataType.CONST_STRING);
		add(store, "subagents", Map.of("workspaceId", Constants.AGENT_PPTX), PixelDataType.MAP);
		reactor.setNounStore(store);

		JSONObject existingConfig = new JSONObject().put("system_prompt", "managed prompt");
		try (var security = mockStatic(SecurityProjectUtils.class);
				var workspaces = mockStatic(ModelInferenceLogsUtils.class)) {
			security.when(() -> SecurityProjectUtils.userCanEditProject(user, Constants.AGENT_ORCHESTRATOR))
					.thenReturn(true);
			security.when(() -> SecurityProjectUtils.userCanViewProject(user, Constants.AGENT_PPTX)).thenReturn(true);
			workspaces.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(Constants.AGENT_ORCHESTRATOR))
					.thenReturn(Map.of("name", "Orchestrator Agent", "is_active", true));
			workspaces.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(Constants.AGENT_PPTX))
					.thenReturn(Map.of("name", "PPTX Agent", "is_active", true));
			workspaces.when(() -> ModelInferenceLogsUtils.getWorkspaceConfigJson(Constants.AGENT_ORCHESTRATOR))
					.thenReturn(existingConfig);

			NounMetadata result = reactor.execute();

			assertEquals(Boolean.TRUE, result.getValue());
			var captor = ArgumentCaptor.forClass(JSONObject.class);
			workspaces.verify(() -> ModelInferenceLogsUtils.updateWorkspaceConfigJson(
					org.mockito.ArgumentMatchers.eq(Constants.AGENT_ORCHESTRATOR), captor.capture()));
			JSONObject updated = captor.getValue();
			assertEquals("managed prompt", updated.getString("system_prompt"));
			assertEquals(Constants.AGENT_PPTX,
					updated.getJSONArray("subagents").getJSONObject(0).getString("workspaceId"));
			assertTrue(updated.getJSONArray("subagents").length() == 1);
		}
	}

	private static void add(NounStore store, String key, Object value, PixelDataType type) {
		GenRowStruct row = new GenRowStruct();
		row.add(new NounMetadata(value, type));
		store.addNoun(key, row);
	}
}
