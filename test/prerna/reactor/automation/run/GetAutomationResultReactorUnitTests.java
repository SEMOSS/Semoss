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
package prerna.reactor.automation.run;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.auth.User;
import prerna.project.api.IProject;
import prerna.om.Insight;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.project.AutomationProjectService;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

class GetAutomationResultReactorUnitTests {

	@Test
	void returnsBoundedStableTablePageAfterAuthorization() throws Exception {
		String payload = "[[1,\"a\"],[2,\"b\"],[3,\"c\"]]";
		Map<String, Object> result = Map.of(AutomationConstants.RESULT_ID, "result",
				AutomationConstants.PROJECT_ID, "project", AutomationConstants.RESULT_KIND,
				AutomationConstants.RESULT_KIND_TABLE, AutomationConstants.PAYLOAD_VALUE,
				"[{\"name\":\"ID\",\"type\":\"INT\"},{\"name\":\"VALUE\",\"type\":\"STRING\"}]",
				AutomationConstants.ROW_COUNT, 3L, AutomationConstants.BYTE_COUNT,
				(long) payload.getBytes(StandardCharsets.UTF_8).length, AutomationConstants.CHECKSUM,
				sha256(payload));
		Map<String, Object> metadata = Map.of(AutomationConstants.CHUNK_INDEX, 0,
				AutomationConstants.FIRST_ROW_INDEX, 0L, AutomationConstants.CHUNK_ROW_COUNT, 3L);
		Map<String, Object> chunk = Map.of(AutomationConstants.CHUNK_VALUE, payload,
				AutomationConstants.BYTE_COUNT, (long) payload.getBytes(StandardCharsets.UTF_8).length,
				AutomationConstants.CHECKSUM, sha256(payload));

		GetAutomationResultReactor reactor = reactor(Map.of("project", "project", "resultId", "result",
				"offset", "1", "limit", "1"));
		IProject project = mock(IProject.class);
		when(project.getProjectId()).thenReturn("project");
		try (var projects = mockStatic(AutomationProjectService.class);
				var store = mockStatic(AutomationRunStore.class)) {
			projects.when(() -> AutomationProjectService.getViewableAutomationProject(
					org.mockito.ArgumentMatchers.any(User.class), org.mockito.ArgumentMatchers.eq("project")))
					.thenReturn(project);
			store.when(() -> AutomationRunStore.getSavedResult("result")).thenReturn(result);
			store.when(() -> AutomationRunStore.getSavedResultChunkMetadata("result")).thenReturn(List.of(metadata));
			store.when(() -> AutomationRunStore.getSavedResultChunk("result", 0)).thenReturn(chunk);

			@SuppressWarnings("unchecked")
			Map<String, Object> response = (Map<String, Object>) reactor.execute().getValue();
			assertEquals(List.of(List.of(2.0, "b")), response.get("rows"));
			assertEquals(2L, response.get("nextOffset"));
			assertEquals(true, response.get("hasMore"));
		}
	}

	@Test
	void rejectsResultFromAnotherProject() {
		GetAutomationResultReactor reactor = reactor(Map.of("project", "project", "resultId", "result"));
		IProject project = mock(IProject.class);
		when(project.getProjectId()).thenReturn("project");
		try (var projects = mockStatic(AutomationProjectService.class);
				var store = mockStatic(AutomationRunStore.class)) {
			projects.when(() -> AutomationProjectService.getViewableAutomationProject(
					org.mockito.ArgumentMatchers.any(User.class), org.mockito.ArgumentMatchers.eq("project")))
					.thenReturn(project);
			store.when(() -> AutomationRunStore.getSavedResult("result"))
					.thenReturn(Map.of(AutomationConstants.PROJECT_ID, "other"));
			assertThrows(IllegalArgumentException.class, reactor::execute);
		}
	}

	private static GetAutomationResultReactor reactor(Map<String, String> inputs) {
		GetAutomationResultReactor reactor = new GetAutomationResultReactor();
		Insight insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(mock(User.class));
		NounStore store = mock(NounStore.class);
		when(store.size()).thenReturn(inputs.size());
		for (Map.Entry<String, String> input : inputs.entrySet()) {
			GenRowStruct row = new GenRowStruct();
			row.add(new NounMetadata(input.getValue(), PixelDataType.CONST_STRING));
			when(store.getGenRowStruct(input.getKey())).thenReturn(row);
		}
		reactor.setInsight(insight);
		reactor.setNounStore(store);
		return reactor;
	}

	private static String sha256(String value) throws Exception {
		return HexFormat.of().formatHex(
				MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
	}
}
