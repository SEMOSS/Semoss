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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static prerna.reactor.automation.run.AutomationGraphFixtures.document;
import static prerna.reactor.automation.run.AutomationGraphFixtures.edge;
import static prerna.reactor.automation.run.AutomationGraphFixtures.node;
import static prerna.reactor.automation.run.AutomationGraphFixtures.python;
import static prerna.reactor.automation.run.AutomationGraphFixtures.transform;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.definition.AutomationDefinitionValidator;
import prerna.reactor.automation.definition.AutomationDefinitionValidator.ValidatedDefinition;

/** Covers validation, graph patching, and ordering of a node re-run, which touch no state. */
class AutomationNodeRerunPlanUnitTests {

	/** start -> a -> b -> c, where every node's graph entry is built by the caller. */
	private static String chain(Map<String, Object> a, Map<String, Object> b, Map<String, Object> c) {
		List<Map<String, Object>> nodes = new ArrayList<>();
		nodes.add(node("start", "Start", AutomationConstants.NODE_START, Map.of()));
		nodes.add(a);
		if (b != null) {
			nodes.add(b);
		}
		if (c != null) {
			nodes.add(c);
		}
		List<Map<String, Object>> edges = new ArrayList<>();
		edges.add(edge("start", "a"));
		if (b != null) {
			edges.add(edge("a", "b"));
		}
		if (c != null) {
			edges.add(edge("b", "c"));
		}
		return document(nodes, edges);
	}

	private static String recorded() {
		return AutomationDefinitionValidator
				.parseAndValidate(chain(transform("a", "A", "a-old"), transform("b", "B", "b-old"),
						transform("c", "C", "c-old")))
				.snapshot();
	}

	private static ValidatedDefinition current() {
		return AutomationDefinitionValidator.parseAndValidate(chain(transform("a", "A edited", "a-new"),
				transform("b", "B edited", "b-new"), transform("c", "C edited", "c-new")));
	}

	@SuppressWarnings("unchecked")
	private static String messageOf(ValidatedDefinition definition, String nodeId) {
		for (Map<String, Object> node : definition.nodes()) {
			if (nodeId.equals(node.get(AutomationConstants.NODE_FIELD_ID))) {
				Map<String, Object> config = (Map<String, Object>) node.get(AutomationConstants.NODE_FIELD_CONFIG);
				Map<String, Object> field = ((List<Map<String, Object>>) config.get("fields")).get(0);
				return (String) ((Map<String, Object>) field.get("node")).get("value");
			}
		}
		throw new AssertionError("no node " + nodeId);
	}

	@Test
	void onlySelectedNodesTakeTheirCurrentSettings() {
		AutomationRunExecutionService.NodeRerunPlan plan = AutomationRunExecutionService.planNodeRerun(recorded(),
				List.of("b"), current(), Map.of());

		assertEquals("a-old", messageOf(plan.definition(), "a"));
		assertEquals("b-new", messageOf(plan.definition(), "b"));
		assertEquals("c-old", messageOf(plan.definition(), "c"));
		assertEquals(List.of("b"), plan.nodeIds());
	}

	@Test
	void theRecordedGraphStaysValidAndCarriesANewHash() {
		AutomationRunExecutionService.NodeRerunPlan plan = AutomationRunExecutionService.planNodeRerun(recorded(),
				List.of("b"), current(), Map.of());

		assertEquals(plan.definition().hash(),
				AutomationDefinitionValidator.parseAndValidate(plan.definition().snapshot()).hash());
		assertTrue(!plan.definition().hash()
				.equals(AutomationDefinitionValidator.parseAndValidate(recorded()).hash()));
	}

	@Test
	void selectedNodesRunInRunOrderWhateverTheRequestOrder() {
		AutomationRunExecutionService.NodeRerunPlan plan = AutomationRunExecutionService.planNodeRerun(recorded(),
				List.of("c", "a"), current(), Map.of());

		assertEquals(List.of("a", "c"), plan.nodeIds());
		assertEquals(List.of("a", "c"),
				plan.nodes().stream().map(n -> n.get(AutomationConstants.NODE_FIELD_ID)).toList());
	}

	@Test
	void aRepeatedNodeIsRunOnce() {
		AutomationRunExecutionService.NodeRerunPlan plan = AutomationRunExecutionService.planNodeRerun(recorded(),
				List.of("b", "b"), current(), Map.of());

		assertEquals(List.of("b"), plan.nodeIds());
	}

	@Test
	void historyRecordsTheCurrentLabelOfEachSelectedNode() {
		AutomationRunExecutionService.NodeRerunPlan plan = AutomationRunExecutionService.planNodeRerun(recorded(),
				List.of("a", "b"), current(), Map.of());

		assertEquals(List.of("A edited", "B edited"),
				plan.updates().stream().map(AutomationRunStore.NodeRerun::label).toList());
		assertEquals(Set.of("b"), plan.updates().get(1).replacedSourceIds());
	}

	@Test
	void aSelectedPythonNodeRecordsItsCurrentSource() {
		String recordedGraph = AutomationDefinitionValidator
				.parseAndValidate(chain(transform("a", "A", "a-old"), python("b", "B"), null)).snapshot();
		ValidatedDefinition now = AutomationDefinitionValidator
				.parseAndValidate(chain(transform("a", "A", "a-old"), python("b", "B edited"), null));

		AutomationRunExecutionService.NodeRerunPlan plan = AutomationRunExecutionService.planNodeRerun(recordedGraph,
				List.of("b"), now, Map.of("b", "def run(scope):\n    return 2\n"));

		assertEquals(Map.of("b", "def run(scope):\n    return 2\n"), plan.updates().get(0).sources());
	}

	@Test
	void aSelectedPythonNodeWithoutCurrentSourceIsRefused() {
		String recordedGraph = AutomationDefinitionValidator
				.parseAndValidate(chain(transform("a", "A", "a-old"), python("b", "B"), null)).snapshot();
		ValidatedDefinition now = AutomationDefinitionValidator
				.parseAndValidate(chain(transform("a", "A", "a-old"), python("b", "B"), null));

		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> AutomationRunExecutionService.planNodeRerun(recordedGraph, List.of("b"), now, Map.of()));

		assertTrue(error.getMessage().contains("has no Python source"), error.getMessage());
	}

	@Test
	void anEmptySelectionIsRefused() {
		assertThrows(IllegalArgumentException.class,
				() -> AutomationRunExecutionService.planNodeRerun(recorded(), List.of(), current(), Map.of()));
		assertThrows(IllegalArgumentException.class,
				() -> AutomationRunExecutionService.planNodeRerun(recorded(), null, current(), Map.of()));
	}

	@Test
	void aNodeThatIsNotPartOfTheRunIsRefused() {
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> AutomationRunExecutionService.planNodeRerun(recorded(), List.of("nope"), current(), Map.of()));

		assertTrue(error.getMessage().contains("not part of this run"), error.getMessage());
	}

	@Test
	void aNodeThatNoLongerExistsInTheCurrentAutomationIsRefused() {
		ValidatedDefinition withoutC = AutomationDefinitionValidator
				.parseAndValidate(chain(transform("a", "A", "a-new"), transform("b", "B", "b-new"), null));

		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> AutomationRunExecutionService.planNodeRerun(recorded(), List.of("c"), withoutC, Map.of()));

		assertTrue(error.getMessage().contains("no longer exists in the current automation"), error.getMessage());
	}

	@Test
	void theTriggerCannotBeRerun() {
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> AutomationRunExecutionService.planNodeRerun(recorded(), List.of("start"), current(), Map.of()));

		assertTrue(error.getMessage().contains("cannot be re-run"), error.getMessage());
	}
}
