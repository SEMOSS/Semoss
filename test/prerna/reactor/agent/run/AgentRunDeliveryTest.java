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
package prerna.reactor.agent.run;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import prerna.om.Insight;

class AgentRunDeliveryTest {

	@Test
	void persistedDeliveryWarningAndArtifactReachThePollingSnapshotWithoutChangingReviewStatus() throws Exception {
		Map<String, Object> artifact = Map.of("status", "available", "filePath", "deck.pptx", "sourceHash",
				"validated-hash", "slideCount", 5, "structuralValidation", "passed");
		Map<String, Object> review = Map.of("status", "incomplete", "verdict", "inconclusive", "consistencyReview",
				"inconclusive");
		String warning = "Overview inconclusive: Images are placeholders";
		Map<String, Object> workflow = Map.of("phase", "delivered_with_warnings", "artifact", artifact, "reviewOutcome",
				review, "warning", warning);
		Map<String, Object> run = readRow("COMPLETED", new JSONObject(Map.of("workflow", workflow)).toString());
		Insight insight = mock(Insight.class);
		try (var store = mockStatic(AgentRunStore.class)) {
			store.when(() -> AgentRunStore.getRunMap("test-run", insight)).thenReturn(run);
			Map<String, Object> snapshot = AgentRunService.get().getRunSnapshot("test-run", insight);
			assertEquals("COMPLETED", snapshot.get("status"), "The existing app's import gate remains compatible");
			assertEquals(List.of(warning), snapshot.get("warnings"));
			assertEquals(review, snapshot.get("reviewOutcome"));
			var saved = (Map<?, ?>) ((List<?>) snapshot.get("artifacts")).getFirst();
			assertEquals("deck.pptx", saved.get("filePath"));
			assertEquals("validated-hash", saved.get("sourceHash"));
			assertNull(snapshot.get("errorMessage"));
		}
	}

	@Test
	void failedBuildMayExposeARecoveryArtifactButDoesNotBecomeCompleted() throws Exception {
		var run = readRow("FAILED", "{\"workflow\":{\"artifact\":{\"status\":\"available\",\"filePath\":\"deck.pptx\"},"
				+ "\"reviewOutcome\":{\"status\":\"incomplete\",\"verdict\":\"inconclusive\"}}}");
		assertEquals("FAILED", run.get("status"));
		assertEquals(1, ((List<?>) run.get("artifacts")).size());
		assertFalse(run.containsKey("warnings"));
	}

	@Test
	void legacyAndUnverifiedRunsDoNotInventDeliverablesOrReviewResults() throws Exception {
		for (String progress : new String[] { null, "{}",
				"{\"workflow\":{\"artifact\":{\"status\":\"unavailable\",\"filePath\":\"deck.pptx\"}}}" }) {
			var run = readRow("FAILED", progress);
			assertTrue(((List<?>) run.get("artifacts")).isEmpty());
			assertFalse(run.containsKey("reviewOutcome"));
			assertFalse(run.containsKey("warnings"));
		}
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> readRow(String status, String progress) throws Exception {
		ResultSet row = mock(ResultSet.class);
		when(row.getString("RUN_ID")).thenReturn("test-run");
		when(row.getString("ROOM_ID")).thenReturn("test-room");
		when(row.getString("STATUS")).thenReturn(status);
		when(row.getString("PROGRESS_JSON")).thenReturn(progress);
		var method = AgentRunStore.class.getDeclaredMethod("runMapFromRow", ResultSet.class);
		method.setAccessible(true);
		return (Map<String, Object>) method.invoke(null, row);
	}
}
