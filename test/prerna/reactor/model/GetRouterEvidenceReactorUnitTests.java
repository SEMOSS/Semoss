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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;


/** The evidence tallies the caller's routed steps per (subtask, model) and per config version, from their own runs only. */
class GetRouterEvidenceReactorUnitTests {

	private static Map<String, Object> step(String router, String tx, Map<String, Object> trace, Boolean rating) {
		Map<String, Object> message = new HashMap<>();
		message.put("transactionId", tx);
		message.put("ornaments", Map.of("routing", Map.of("router_engine_id", router, "trace", trace)));
		if (rating != null) {
			message.put("feedback", Map.of("rating", rating));
		}
		return message;
	}

	@SafeVarargs
	private static Map<String, Object> runOf(String id, String status, Map<String, Object>... steps) {
		return Map.of("runId", id, "roomId", "room-" + id, "status", status, "messages", List.of(steps));
	}

	private static Map<String, Object> row(String tx, Long cost) {
		Map<String, Object> row = new HashMap<>();
		row.put("TRANSACTION_ID", tx);
		row.put("COST_MICROS", cost);
		return row;
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> find(Map<String, Object> evidence, String list, String key, String value) {
		return ((List<Map<String, Object>>) evidence.get(list)).stream().filter(m -> value.equals(m.get(key))).findFirst()
				.orElseThrow();
	}

	@Test
	void tallyOfCostRunOutcomesAndThumbsPerCellAndVersionIgnoringOtherRoutersAndOldTraces() {
		Map<String, Object> trace = Map.of("subtask_id", "edit", "served_engine_id", "cheap", "config_version", "v2");
		var done = runOf("r1", "COMPLETED", step("rt", "t1", trace, true), step("rt", "t2", trace, null),
				step("other", "t9", trace, null), step("rt", "t4", new HashMap<>(), null));
		var failed = runOf("r2", "FAILED", step("rt", "t3", trace, false));
		Map<String, List<Map<String, Object>>> rows = Map.of("room-r1",
				List.of(row("t1", null), row("t1", 300L), row("t2", 200L)), "room-r2", List.of(row("t3", null)));

		Map<String, Object> evidence = GetRouterEvidenceReactor.aggregate("rt", List.of(done, failed), 0, rows::get);

		assertEquals(4, evidence.get("routed_steps"));
		Map<String, Object> cell = find(evidence, "cells", "subtask_id", "edit");
		assertEquals(3, cell.get("steps"));
		assertEquals(250.0, cell.get("avg_cost_micros_per_priced_step"));
		assertEquals(2, cell.get("runs"));
		assertEquals(0.5, cell.get("failed_run_share"));
		assertEquals(1, cell.get("thumbs_down"));
		assertEquals(1L, cell.get("failed_runs"));
		// the old trace has no subtask or version and no model: it lands in (none) and unknown
		assertEquals(1, find(evidence, "cells", "served_engine_id", "unknown").get("steps"));
		assertEquals("(none)", find(evidence, "cells", "served_engine_id", "unknown").get("subtask_id"));
		Map<String, Object> version = find(evidence, "by_version", "config_version", "v2");
		assertEquals(1, version.get("thumbs_down"));
		assertEquals(1, find(evidence, "by_version", "config_version", "(none)").get("steps"));
	}

	@Test
	@SuppressWarnings("unchecked")
	void stepSampleReturnsAtMostNStepsPerSubtaskWithWhatTheClassifierSawAndIsAbsentByDefault() {
		Map<String, Object> inputs = Map.of("goal", "g".repeat(300), "previous_subtask", "inspect", "recent_steps", List.of("read file", "list sheets"));
		Map<String, Object> trace = Map.of("subtask_id", "edit", "served_engine_id", "cheap", "config_version", "v2", "classifier_inputs", inputs);
		var run = runOf("r1", "COMPLETED", step("rt", "t1", trace, null), step("rt", "t2", trace, null), step("rt", "t3", trace, null));
		Map<String, List<Map<String, Object>>> rows = Map.of("room-r1", List.of(row("t1", 100L)));

		assertTrue(!GetRouterEvidenceReactor.aggregate("rt", List.of(run), 0, rows::get).containsKey("step_sample"));
		List<Map<String, Object>> sample = (List<Map<String, Object>>) GetRouterEvidenceReactor.aggregate("rt", List.of(run), 2, rows::get)
				.get("step_sample");
		assertEquals(2, sample.size());
		assertEquals("edit", sample.get(0).get("subtask_id"));
		assertEquals("inspect", sample.get(0).get("previous_subtask"));
		assertEquals(List.of("read file", "list sheets"), sample.get(0).get("recent_steps"));
		assertTrue(((String) sample.get(0).get("goal")).length() <= 243);
		assertEquals(100L, sample.get(0).get("cost_micros"));
	}
}
