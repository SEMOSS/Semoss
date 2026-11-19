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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static prerna.reactor.automation.run.AutomationGraphFixtures.document;
import static prerna.reactor.automation.run.AutomationGraphFixtures.edge;
import static prerna.reactor.automation.run.AutomationGraphFixtures.forEachLoop;
import static prerna.reactor.automation.run.AutomationGraphFixtures.node;
import static prerna.reactor.automation.run.AutomationGraphFixtures.python;
import static prerna.reactor.automation.run.AutomationGraphFixtures.transform;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.definition.AutomationDefinitionValidator;
import prerna.reactor.automation.definition.AutomationDefinitionValidator.ValidatedDefinition;

/** Covers re-running body nodes of one loop iteration: planning, and the scope an iteration starts with. */
class AutomationLoopIterationRerunUnitTests {

	/** start -> loop(items 1..3; body a -> b), with each body node's value chosen by the caller. */
	private static String automation(String aValue, String bValue) {
		Map<String, Object> loop = forEachLoop("loop", "Loop", List.of(1, 2, 3),
				List.of(transform("a", "A", aValue), transform("b", "B", bValue)), List.of(edge("a", "b")));
		return document(List.of(node("start", "Start", AutomationConstants.NODE_START, Map.of()), loop),
				List.of(edge("start", "loop")));
	}

	private static String recorded() {
		return AutomationDefinitionValidator.parseAndValidate(automation("a-old", "b-old")).snapshot();
	}

	private static ValidatedDefinition current() {
		return AutomationDefinitionValidator.parseAndValidate(automation("a-new", "b-new"));
	}

	@SuppressWarnings("unchecked")
	private static String valueOf(AutomationRunExecutionService.LoopIterationRerunPlan plan, String bodyNodeId) {
		for (Map<String, Object> node : plan.bodyNodes()) {
			if (bodyNodeId.equals(node.get(AutomationConstants.NODE_FIELD_ID))) {
				Map<String, Object> config = (Map<String, Object>) node.get(AutomationConstants.NODE_FIELD_CONFIG);
				Map<String, Object> field = ((List<Map<String, Object>>) config.get("fields")).get(0);
				return (String) ((Map<String, Object>) field.get("node")).get("value");
			}
		}
		throw new AssertionError("no body node " + bodyNodeId);
	}

	@Test
	void onlySelectedBodyNodesTakeTheirCurrentSettings() {
		AutomationRunExecutionService.LoopIterationRerunPlan plan = AutomationRunExecutionService
				.planLoopIterationRerun(recorded(), "loop", List.of("b"), current(), Map.of());

		assertEquals("a-old", valueOf(plan, "a"));
		assertEquals("b-new", valueOf(plan, "b"));
		assertEquals(List.of("b"), plan.nodeIds());
	}

	@Test
	void theRecordedGraphStaysValidAndCarriesANewHash() {
		AutomationRunExecutionService.LoopIterationRerunPlan plan = AutomationRunExecutionService
				.planLoopIterationRerun(recorded(), "loop", List.of("b"), current(), Map.of());

		assertEquals(plan.definition().hash(),
				AutomationDefinitionValidator.parseAndValidate(plan.definition().snapshot()).hash());
		assertFalse(plan.definition().hash()
				.equals(AutomationDefinitionValidator.parseAndValidate(recorded()).hash()));
	}

	@Test
	void selectedNodesRunInBodyOrderWhateverTheRequestOrder() {
		AutomationRunExecutionService.LoopIterationRerunPlan plan = AutomationRunExecutionService
				.planLoopIterationRerun(recorded(), "loop", List.of("b", "a", "b"), current(), Map.of());

		assertEquals(List.of("a", "b"), plan.nodeIds());
	}

	@Test
	void theWholeBodyIsAvailableSoUnchosenNodesCanBeReplayedIntoScope() {
		AutomationRunExecutionService.LoopIterationRerunPlan plan = AutomationRunExecutionService
				.planLoopIterationRerun(recorded(), "loop", List.of("b"), current(), Map.of());

		assertEquals(List.of("a", "b"),
				plan.bodyNodes().stream().map(n -> n.get(AutomationConstants.NODE_FIELD_ID)).toList());
	}

	@Test
	void aSelectedPythonBodyNodeRecordsItsCurrentSource() {
		String withPython = document(List.of(node("start", "Start", AutomationConstants.NODE_START, Map.of()),
				forEachLoop("loop", "Loop", List.of(1, 2), List.of(python("p", "P")), List.of())),
				List.of(edge("start", "loop")));
		ValidatedDefinition now = AutomationDefinitionValidator.parseAndValidate(withPython);

		AutomationRunExecutionService.LoopIterationRerunPlan plan = AutomationRunExecutionService
				.planLoopIterationRerun(now.snapshot(), "loop", List.of("p"), now,
						Map.of("p", "def run(scope):\n    return 1\n"));

		assertEquals(Map.of("p", "def run(scope):\n    return 1\n"), plan.sources());
		IllegalArgumentException missing = assertThrows(IllegalArgumentException.class,
				() -> AutomationRunExecutionService.planLoopIterationRerun(now.snapshot(), "loop", List.of("p"),
						now, Map.of()));
		assertTrue(missing.getMessage().contains("has no Python source"), missing.getMessage());
	}

	@Test
	void anEmptySelectionIsRefused() {
		assertThrows(IllegalArgumentException.class, () -> AutomationRunExecutionService
				.planLoopIterationRerun(recorded(), "loop", List.of(), current(), Map.of()));
		assertThrows(IllegalArgumentException.class, () -> AutomationRunExecutionService
				.planLoopIterationRerun(recorded(), "loop", null, current(), Map.of()));
	}

	@Test
	void aNodeThatIsNotInTheLoopBodyIsRefused() {
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> AutomationRunExecutionService.planLoopIterationRerun(recorded(), "loop", List.of("nope"),
						current(), Map.of()));

		assertTrue(error.getMessage().contains("not part of loop"), error.getMessage());
	}

	@Test
	void aNodeThatIsNotALoopIsRefused() {
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> AutomationRunExecutionService.planLoopIterationRerun(recorded(), "start", List.of("a"),
						current(), Map.of()));

		assertTrue(error.getMessage().contains("is not a loop"), error.getMessage());
	}

	@Test
	void aBodyNodeRemovedFromTheCurrentLoopIsRefused() {
		String withoutB = document(List.of(node("start", "Start", AutomationConstants.NODE_START, Map.of()),
				forEachLoop("loop", "Loop", List.of(1, 2, 3), List.of(transform("a", "A", "a-new")), List.of())),
				List.of(edge("start", "loop")));
		ValidatedDefinition now = AutomationDefinitionValidator.parseAndValidate(withoutB);

		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> AutomationRunExecutionService.planLoopIterationRerun(recorded(), "loop", List.of("b"), now,
						Map.of()));

		assertTrue(error.getMessage().contains("no longer exists in the current loop"), error.getMessage());
	}

	@Test
	void aLoopRemovedFromTheCurrentAutomationIsRefused() {
		ValidatedDefinition withoutLoop = AutomationDefinitionValidator.parseAndValidate(
				document(List.of(node("start", "Start", AutomationConstants.NODE_START, Map.of())), List.of()));

		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> AutomationRunExecutionService.planLoopIterationRerun(recorded(), "loop", List.of("a"),
						withoutLoop, Map.of()));

		assertTrue(error.getMessage().contains("no longer exists in the current automation"), error.getMessage());
	}

	// --- the scope an iteration starts with

	private static Map<String, Object> loopNode(String mode, Map<String, Object> extraConfig) {
		Map<String, Object> config = new LinkedHashMap<>(extraConfig);
		config.put(AutomationConstants.CONFIG_LOOP_MODE, mode);
		config.put(AutomationConstants.CONFIG_LOOP_MAX_ITERATIONS, 10);
		Map<String, Object> node = new LinkedHashMap<>();
		node.put(AutomationConstants.NODE_FIELD_ID, "loop");
		node.put(AutomationConstants.NODE_FIELD_OUTPUT_VAR, "loop_out");
		node.put(AutomationConstants.NODE_FIELD_CONFIG, config);
		return node;
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> loopVariable(Map<String, Object> overrides) {
		return (Map<String, Object>) overrides.get("loop_out");
	}

	@Test
	void aForEachIterationStartsWithItsOwnBatchAndItem() {
		AutomationRunExecutionService.LoopPlan plan = AutomationRunExecutionService.planLoop(
				loopNode(AutomationConstants.LOOP_MODE_FOR_EACH,
						Map.of(AutomationConstants.CONFIG_LOOP_ITEMS, List.of(10, 20, 30),
								AutomationConstants.CONFIG_LOOP_BATCH_SIZE, 2)),
				Map.of());

		assertEquals(2, plan.iterationLimit());
		Map<String, Object> first = loopVariable(plan.iterationOverrides(0, Map.of()));
		assertEquals(List.of(10, 20), first.get("batch"));
		assertFalse(first.containsKey("item"));
		assertEquals(true, first.get("isFirst"));
		Map<String, Object> last = loopVariable(plan.iterationOverrides(1, Map.of()));
		assertEquals(List.of(30), last.get("batch"));
		assertEquals(30, last.get("item"));
		assertEquals(true, last.get("isLast"));
		assertEquals(2, last.get("number"));
		assertEquals(2, last.get("total"));
	}

	@Test
	void aRepeatIterationKnowsItsPositionOnly() {
		AutomationRunExecutionService.LoopPlan plan = AutomationRunExecutionService.planLoop(
				loopNode(AutomationConstants.LOOP_MODE_REPEAT, Map.of(AutomationConstants.CONFIG_LOOP_COUNT, 3)),
				Map.of());

		Map<String, Object> middle = loopVariable(plan.iterationOverrides(1, Map.of()));

		assertEquals(1, middle.get("index"));
		assertEquals(false, middle.get("isLast"));
		assertNull(middle.get("item"));
	}

	@Test
	void aWhileIterationStartsWithThePreviousIterationsOutputs() {
		AutomationRunExecutionService.LoopPlan plan = AutomationRunExecutionService.planLoop(
				loopNode(AutomationConstants.LOOP_MODE_WHILE,
						Map.of(AutomationConstants.CONFIG_LOOP_CONDITION, "${x} < 3")),
				Map.of());
		Map<String, Object> carried = new LinkedHashMap<>();
		carried.put("x", 2);

		Map<String, Object> overrides = plan.iterationOverrides(2, carried);

		assertEquals(2, overrides.get("x"));
		assertEquals(carried, loopVariable(overrides).get("previous"));
		assertEquals(10, loopVariable(overrides).get("maximum"));
	}

	@Test
	void aLoopWhoseItemsExceedItsMaximumIsRefused() {
		assertThrows(IllegalArgumentException.class, () -> AutomationRunExecutionService.planLoop(
				loopNode(AutomationConstants.LOOP_MODE_FOR_EACH,
						Map.of(AutomationConstants.CONFIG_LOOP_ITEMS, List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
								AutomationConstants.CONFIG_LOOP_BATCH_SIZE, 1)),
				Map.of()));
	}

	@Test
	void theLoopVariableUsesTheLoopsOutputName() {
		AutomationRunExecutionService.LoopPlan plan = AutomationRunExecutionService.planLoop(
				loopNode(AutomationConstants.LOOP_MODE_REPEAT, Map.of(AutomationConstants.CONFIG_LOOP_COUNT, 1)),
				Map.of());

		assertEquals(Set.of("loop_out"), plan.iterationOverrides(0, Map.of()).keySet());
	}
}
