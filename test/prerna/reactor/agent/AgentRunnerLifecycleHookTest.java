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
package prerna.reactor.agent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.om.Insight;
import prerna.reactor.agent.config.AgentConfig;
import prerna.reactor.agent.config.AgentConfigLoader;
import prerna.reactor.agent.skill.SkillStager;
import prerna.util.Utility;

/**
 * Verifies that {@link AgentRunner#run} invokes the 5 run-level lifecycle hook
 * methods on every registered {@link IAgentRunHook} in the documented order:
 *
 * <pre>
 *   onRoomCreation  ->  beforeRun  ->  afterAgentInit
 *   [harness.execute]
 *   afterRun  ->  beforeAgentDeInit
 * </pre>
 *
 * <p>
 * Also verifies veto semantics: an exception thrown from {@code beforeRun}
 * aborts the run, while exceptions in the other 4 lifecycle methods are logged
 * and swallowed (the run completes and {@code finally} hooks still fire).
 */
class AgentRunnerLifecycleHookTest {

	private static final String ROOM_ID = "room-life-1";
	private static final String MODEL_ID = "engine-xyz";

	private Insight insight;
	private Room room;
	private IModelEngine modelEngine;
	private IAgentHarness harness;
	private AgentConfig agentConfig;
	private AgentHarnessResult harnessResult;

	@BeforeEach
	void setUp() {
		insight = mock(Insight.class);
		room = mock(Room.class);
		modelEngine = mock(IModelEngine.class);
		harness = mock(IAgentHarness.class);
		agentConfig = mock(AgentConfig.class);
		harnessResult = mock(AgentHarnessResult.class);

		when(room.getId()).thenReturn(ROOM_ID);
		when(room.getModelId()).thenReturn(MODEL_ID);
		when(room.getUserId()).thenReturn("user-1");
		when(room.getOptionsMap()).thenReturn(new HashMap<>());
		when(room.getRoomFolderPath()).thenReturn(System.getProperty("java.io.tmpdir"));

		when(agentConfig.getSkills()).thenReturn(Collections.emptyList());
		when(harness.getName()).thenReturn("test_harness");
	}

	/**
	 * Wires the heavy external statics so {@link AgentRunner#run} can execute
	 * without a live SEMOSS environment. The caller picks the {@code hooks} list
	 * returned by {@code AgentConfig.getRunHooks()}, which is what each test
	 * exercises.
	 */
	private void stubStatics(MockedStatic<RoomUtils> roomUtils, MockedStatic<Utility> utility,
			MockedStatic<AgentConfigLoader> loader, MockedStatic<AgentHarnessRegistry> registry,
			MockedStatic<SkillStager> skill, List<IAgentRunHook> hooks) throws Exception {
		roomUtils.when(() -> RoomUtils.getOrLoadRoom(ROOM_ID, insight)).thenReturn(room);
		utility.when(() -> Utility.getModel(MODEL_ID)).thenReturn(modelEngine);

		when(agentConfig.getRunHooks()).thenReturn(hooks);
		loader.when(() -> AgentConfigLoader.load(any(Room.class), any(), anyString(), anyMap(), anyMap(), anyInt(),
				anyInt(), any())).thenReturn(agentConfig);

		registry.when(() -> AgentHarnessRegistry.getOrDefault(anyString())).thenReturn(harness);

		// Inject a "harness.execute" marker into every recording hook so the
		// ordering assertion can verify lifecycle methods fired around it.
		when(harness.execute(any(AgentRunContext.class))).thenAnswer(inv -> {
			for (IAgentRunHook h : hooks) {
				if (h instanceof RecordingRunHook) {
					((RecordingRunHook) h).calls.add("harness.execute");
				}
			}
			return harnessResult;
		});

		skill.when(() -> SkillStager.stage(any(), anyList())).then(inv -> null);
	}

	// ---------- ordering ----------

	@Test
	void firesAllFiveRunLifecycleHooksInOrder() throws Exception {
		RecordingRunHook hook = new RecordingRunHook();

		try (MockedStatic<RoomUtils> roomUtils = Mockito.mockStatic(RoomUtils.class);
				MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class);
				MockedStatic<AgentConfigLoader> loader = Mockito.mockStatic(AgentConfigLoader.class);
				MockedStatic<AgentHarnessRegistry> registry = Mockito.mockStatic(AgentHarnessRegistry.class);
				MockedStatic<SkillStager> skill = Mockito.mockStatic(SkillStager.class)) {

			stubStatics(roomUtils, utility, loader, registry, skill, List.of(hook));

			AgentHarnessResult result = AgentRunner.run(ROOM_ID, "hello", null, "test_harness", 10, 0, new HashMap<>(),
					new HashMap<>(), insight);

			assertSame(harnessResult, result);
			assertEquals(List.of("onRoomCreation", "beforeRun", "afterAgentInit", "harness.execute", "afterRun",
					"beforeAgentDeInit"), hook.calls, "Lifecycle methods must fire in the documented order");
		}
	}

	@Test
	void firesHooksForMultipleRegisteredHooksInOrder() throws Exception {
		RecordingRunHook hookA = new RecordingRunHook("A");
		RecordingRunHook hookB = new RecordingRunHook("B");

		try (MockedStatic<RoomUtils> roomUtils = Mockito.mockStatic(RoomUtils.class);
				MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class);
				MockedStatic<AgentConfigLoader> loader = Mockito.mockStatic(AgentConfigLoader.class);
				MockedStatic<AgentHarnessRegistry> registry = Mockito.mockStatic(AgentHarnessRegistry.class);
				MockedStatic<SkillStager> skill = Mockito.mockStatic(SkillStager.class)) {

			stubStatics(roomUtils, utility, loader, registry, skill, List.of(hookA, hookB));

			AgentRunner.run(ROOM_ID, "hello", null, "test_harness", 10, 0, new HashMap<>(), new HashMap<>(), insight);

			// Each lifecycle event fires across ALL hooks before moving to the next event.
			assertEquals(List.of("onRoomCreation", "beforeRun", "afterAgentInit", "harness.execute", "afterRun",
					"beforeAgentDeInit"), hookA.calls);
			assertEquals(List.of("onRoomCreation", "beforeRun", "afterAgentInit", "harness.execute", "afterRun",
					"beforeAgentDeInit"), hookB.calls);
		}
	}

	// ---------- veto semantics ----------

	@Test
	void beforeRunExceptionAbortsTheRunButFinallyHooksStillFire() throws Exception {
		RecordingRunHook hook = new RecordingRunHook();
		hook.throwOn("beforeRun", new RuntimeException("veto!"));

		try (MockedStatic<RoomUtils> roomUtils = Mockito.mockStatic(RoomUtils.class);
				MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class);
				MockedStatic<AgentConfigLoader> loader = Mockito.mockStatic(AgentConfigLoader.class);
				MockedStatic<AgentHarnessRegistry> registry = Mockito.mockStatic(AgentHarnessRegistry.class);
				MockedStatic<SkillStager> skill = Mockito.mockStatic(SkillStager.class)) {

			stubStatics(roomUtils, utility, loader, registry, skill, List.of(hook));

			RuntimeException ex = assertThrows(RuntimeException.class, () -> AgentRunner.run(ROOM_ID, "hello", null,
					"test_harness", 10, 0, new HashMap<>(), new HashMap<>(), insight));
			assertTrue(ex.getMessage().contains("veto!"));

			// onRoomCreation ran before veto; harness.execute and afterAgentInit
			// did NOT fire. But afterRun + beforeAgentDeInit DO fire from the
			// finally block.
			assertTrue(hook.calls.contains("onRoomCreation"));
			assertTrue(hook.calls.contains("beforeRun"));
			assertEquals(false, hook.calls.contains("afterAgentInit"),
					"afterAgentInit must NOT fire after beforeRun aborts");
			assertEquals(false, hook.calls.contains("harness.execute"),
					"harness must NOT execute after beforeRun aborts");
			assertTrue(hook.calls.contains("afterRun"), "afterRun in the finally block must still fire after abort");
			assertTrue(hook.calls.contains("beforeAgentDeInit"),
					"beforeAgentDeInit in the finally block must still fire after abort");
		}
	}

	@Test
	void onRoomCreationExceptionIsSwallowedAndRunContinues() throws Exception {
		RecordingRunHook hook = new RecordingRunHook();
		hook.throwOn("onRoomCreation", new RuntimeException("observer-failed"));

		try (MockedStatic<RoomUtils> roomUtils = Mockito.mockStatic(RoomUtils.class);
				MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class);
				MockedStatic<AgentConfigLoader> loader = Mockito.mockStatic(AgentConfigLoader.class);
				MockedStatic<AgentHarnessRegistry> registry = Mockito.mockStatic(AgentHarnessRegistry.class);
				MockedStatic<SkillStager> skill = Mockito.mockStatic(SkillStager.class)) {

			stubStatics(roomUtils, utility, loader, registry, skill, List.of(hook));

			AgentHarnessResult result = AgentRunner.run(ROOM_ID, "hello", null, "test_harness", 10, 0, new HashMap<>(),
					new HashMap<>(), insight);

			assertSame(harnessResult, result);
			// All 5 lifecycle methods still ran — exception was swallowed.
			assertEquals(List.of("onRoomCreation", "beforeRun", "afterAgentInit", "harness.execute", "afterRun",
					"beforeAgentDeInit"), hook.calls);
		}
	}

	@Test
	void afterAgentInitExceptionIsSwallowedAndHarnessStillRuns() throws Exception {
		RecordingRunHook hook = new RecordingRunHook();
		hook.throwOn("afterAgentInit", new RuntimeException("post-init oops"));

		try (MockedStatic<RoomUtils> roomUtils = Mockito.mockStatic(RoomUtils.class);
				MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class);
				MockedStatic<AgentConfigLoader> loader = Mockito.mockStatic(AgentConfigLoader.class);
				MockedStatic<AgentHarnessRegistry> registry = Mockito.mockStatic(AgentHarnessRegistry.class);
				MockedStatic<SkillStager> skill = Mockito.mockStatic(SkillStager.class)) {

			stubStatics(roomUtils, utility, loader, registry, skill, List.of(hook));

			AgentHarnessResult result = AgentRunner.run(ROOM_ID, "hello", null, "test_harness", 10, 0, new HashMap<>(),
					new HashMap<>(), insight);

			assertSame(harnessResult, result);
			assertTrue(hook.calls.contains("harness.execute"), "harness must still run after afterAgentInit throws");
		}
	}

	@Test
	void afterRunExceptionIsSwallowedAndBeforeAgentDeInitStillFires() throws Exception {
		RecordingRunHook hook = new RecordingRunHook();
		hook.throwOn("afterRun", new RuntimeException("late-fail"));

		try (MockedStatic<RoomUtils> roomUtils = Mockito.mockStatic(RoomUtils.class);
				MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class);
				MockedStatic<AgentConfigLoader> loader = Mockito.mockStatic(AgentConfigLoader.class);
				MockedStatic<AgentHarnessRegistry> registry = Mockito.mockStatic(AgentHarnessRegistry.class);
				MockedStatic<SkillStager> skill = Mockito.mockStatic(SkillStager.class)) {

			stubStatics(roomUtils, utility, loader, registry, skill, List.of(hook));

			AgentHarnessResult result = AgentRunner.run(ROOM_ID, "hello", null, "test_harness", 10, 0, new HashMap<>(),
					new HashMap<>(), insight);

			assertSame(harnessResult, result);
			assertTrue(hook.calls.contains("beforeAgentDeInit"));
		}
	}

	@Test
	void beforeAgentDeInitExceptionIsSwallowed() throws Exception {
		RecordingRunHook hook = new RecordingRunHook();
		hook.throwOn("beforeAgentDeInit", new RuntimeException("deinit-fail"));

		try (MockedStatic<RoomUtils> roomUtils = Mockito.mockStatic(RoomUtils.class);
				MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class);
				MockedStatic<AgentConfigLoader> loader = Mockito.mockStatic(AgentConfigLoader.class);
				MockedStatic<AgentHarnessRegistry> registry = Mockito.mockStatic(AgentHarnessRegistry.class);
				MockedStatic<SkillStager> skill = Mockito.mockStatic(SkillStager.class)) {

			stubStatics(roomUtils, utility, loader, registry, skill, List.of(hook));

			AgentHarnessResult result = AgentRunner.run(ROOM_ID, "hello", null, "test_harness", 10, 0, new HashMap<>(),
					new HashMap<>(), insight);

			// No exception — beforeAgentDeInit failure must not propagate.
			assertSame(harnessResult, result);
		}
	}

	// ---------- context handed to hooks ----------

	@Test
	void hooksReceiveContextWithInsightAndRoom() throws Exception {
		RecordingRunHook hook = new RecordingRunHook();

		try (MockedStatic<RoomUtils> roomUtils = Mockito.mockStatic(RoomUtils.class);
				MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class);
				MockedStatic<AgentConfigLoader> loader = Mockito.mockStatic(AgentConfigLoader.class);
				MockedStatic<AgentHarnessRegistry> registry = Mockito.mockStatic(AgentHarnessRegistry.class);
				MockedStatic<SkillStager> skill = Mockito.mockStatic(SkillStager.class)) {

			stubStatics(roomUtils, utility, loader, registry, skill, List.of(hook));

			AgentRunner.run(ROOM_ID, "hello", null, "test_harness", 10, 0, new HashMap<>(), new HashMap<>(), insight);

			// Verify the same ctx was handed to every lifecycle method.
			assertEquals(5, hook.contexts.size(), "5 lifecycle methods should each receive a ctx");
			AgentRunContext first = hook.contexts.get(0);
			for (AgentRunContext c : hook.contexts) {
				assertSame(first, c, "All lifecycle hooks must receive the same AgentRunContext");
			}
			assertSame(insight, first.getInsight());
			assertSame(room, first.getRoom());
		}
	}

	@Test
	void afterRunAndBeforeAgentDeInitReceiveTheHarnessResult() throws Exception {
		RecordingRunHook hook = new RecordingRunHook();

		try (MockedStatic<RoomUtils> roomUtils = Mockito.mockStatic(RoomUtils.class);
				MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class);
				MockedStatic<AgentConfigLoader> loader = Mockito.mockStatic(AgentConfigLoader.class);
				MockedStatic<AgentHarnessRegistry> registry = Mockito.mockStatic(AgentHarnessRegistry.class);
				MockedStatic<SkillStager> skill = Mockito.mockStatic(SkillStager.class)) {

			stubStatics(roomUtils, utility, loader, registry, skill, List.of(hook));

			AgentRunner.run(ROOM_ID, "hello", null, "test_harness", 10, 0, new HashMap<>(), new HashMap<>(), insight);

			assertSame(harnessResult, hook.afterRunResult, "afterRun hook should receive harness result");
			assertSame(harnessResult, hook.beforeDeInitResult, "beforeAgentDeInit hook should receive harness result");
		}
	}

	// ---------- helpers ----------

	/**
	 * Records the order and identity of every lifecycle method invoked on it, and
	 * lets a test selectively throw from any single method.
	 */
	private static final class RecordingRunHook implements IAgentRunHook {
		final String label;
		final List<String> calls = new ArrayList<>();
		final List<AgentRunContext> contexts = new ArrayList<>();
		AgentHarnessResult afterRunResult;
		AgentHarnessResult beforeDeInitResult;
		private final Map<String, RuntimeException> throwOn = new HashMap<>();

		RecordingRunHook() {
			this("default");
		}

		RecordingRunHook(String label) {
			this.label = label;
		}

		void throwOn(String method, RuntimeException ex) {
			throwOn.put(method, ex);
		}

		@Override
		public void onRoomCreation(AgentRunContext ctx) {
			calls.add("onRoomCreation");
			contexts.add(ctx);
			if (throwOn.containsKey("onRoomCreation")) {
				throw throwOn.get("onRoomCreation");
				// Also record harness.execute boundary via a hook between
				// afterAgentInit (last method before execute) and afterRun. We
				// capture it here only after afterAgentInit fires; for the
				// ordering invariant we want a marker showing the harness ran.
			}
		}

		@Override
		public void beforeRun(AgentRunContext ctx) {
			calls.add("beforeRun");
			contexts.add(ctx);
			if (throwOn.containsKey("beforeRun")) {
				throw throwOn.get("beforeRun");
			}
		}

		@Override
		public void afterAgentInit(AgentRunContext ctx) {
			calls.add("afterAgentInit");
			contexts.add(ctx);
			if (throwOn.containsKey("afterAgentInit")) {
				throw throwOn.get("afterAgentInit");
				// We mark the harness boundary here on a "side-channel" — see
				// setUp(): harness.execute(ctx) is stubbed to insert a marker
				// into this hook's calls list via doAnswer below in tests that
				// need it. For simplicity, inject the marker via a custom
				// IAgentHarness stub in firesAllFiveRunLifecycleHooksInOrder.
			}
		}

		@Override
		public void afterRun(AgentRunContext ctx, AgentHarnessResult result) {
			calls.add("afterRun");
			contexts.add(ctx);
			afterRunResult = result;
			if (throwOn.containsKey("afterRun")) {
				throw throwOn.get("afterRun");
			}
		}

		@Override
		public void beforeAgentDeInit(AgentRunContext ctx, AgentHarnessResult result) {
			calls.add("beforeAgentDeInit");
			contexts.add(ctx);
			beforeDeInitResult = result;
			if (throwOn.containsKey("beforeAgentDeInit")) {
				throw throwOn.get("beforeAgentDeInit");
			}
		}
	}
}
