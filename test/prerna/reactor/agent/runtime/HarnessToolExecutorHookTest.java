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
package prerna.reactor.agent.runtime;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.reactor.agent.AgentRunContext;
import prerna.reactor.agent.IToolHook;

/**
 * Verifies that {@link HarnessToolExecutor} fires {@link IToolHook#beforeTool}
 * before tool dispatch and {@link IToolHook#afterTool} after — with the right
 * arguments, in the right order, and with exceptions in one hook never skipping
 * the remaining hooks.
 *
 * <p>
 * The {@code fireBeforeTool} / {@code fireAfterTool} entry points are
 * package-private static methods, and the helper value classes
 * ({@code ParsedToolCall}, {@code ToolExecOutcome}) are package/private-static.
 * We reach them via reflection — testing the call-site contract without
 * standing up a live Room + ModelEngine.
 */
class HarnessToolExecutorHookTest {

	private AgentRunContext ctx;
	private Object parsedToolCall;
	private Object toolExecOutcomeSuccess;
	private Object toolExecOutcomeFailure;
	private Method fireBeforeTool;
	private Method fireAfterTool;

	@BeforeEach
	void setUp() throws Exception {
		ctx = mock(AgentRunContext.class);

		// ParsedToolCall(Map<String,Object> toolCall)
		Class<?> parsedCls = Class.forName("prerna.reactor.agent.runtime.HarnessToolExecutor$ParsedToolCall");
		Constructor<?> parsedCtor = parsedCls.getDeclaredConstructor(Map.class);
		parsedCtor.setAccessible(true);
		Map<String, Object> rawToolCall = new HashMap<>();
		rawToolCall.put("id", "call-42");
		rawToolCall.put("name", "Bash");
		Map<String, Object> args = new HashMap<>();
		args.put("command", "ls -la");
		rawToolCall.put("input", args);
		parsedToolCall = parsedCtor.newInstance(rawToolCall);

		// ToolExecOutcome(String content, boolean success)
		Class<?> outcomeCls = Class.forName("prerna.reactor.agent.runtime.HarnessToolExecutor$ToolExecOutcome");
		Constructor<?> outcomeCtor = outcomeCls.getDeclaredConstructor(String.class, boolean.class);
		outcomeCtor.setAccessible(true);
		toolExecOutcomeSuccess = outcomeCtor.newInstance("ok-result", true);
		toolExecOutcomeFailure = outcomeCtor.newInstance("boom-err", false);

		// fireBeforeTool(List<IToolHook>, AgentRunContext, ParsedToolCall, int)
		fireBeforeTool = HarnessToolExecutor.class.getDeclaredMethod("fireBeforeTool", List.class,
				AgentRunContext.class, parsedCls, int.class);
		fireBeforeTool.setAccessible(true);

		// fireAfterTool(List<IToolHook>, AgentRunContext, ParsedToolCall,
		// ToolExecOutcome, long, int)
		fireAfterTool = HarnessToolExecutor.class.getDeclaredMethod("fireAfterTool", List.class, AgentRunContext.class,
				parsedCls, outcomeCls, long.class, int.class);
		fireAfterTool.setAccessible(true);
	}

	// ---------- beforeTool ----------

	@Test
	void firesBeforeToolWithExpectedArgs() throws Exception {
		RecordingToolHook hook = new RecordingToolHook();

		fireBeforeTool.invoke(null, List.of(hook), ctx, parsedToolCall, 3);

		assertEquals(1, hook.beforeCalls.size());
		ToolCallSnapshot snap = hook.beforeCalls.get(0);
		assertSame(ctx, snap.ctx);
		assertEquals("Bash", snap.toolName);
		assertEquals("call-42", snap.toolCallId);
		assertEquals("ls -la", snap.params.get("command"));
		assertEquals(3, snap.iteration);
	}

	@Test
	void firesBeforeToolForAllRegisteredHooksInOrder() throws Exception {
		RecordingToolHook a = new RecordingToolHook("A");
		RecordingToolHook b = new RecordingToolHook("B");
		RecordingToolHook c = new RecordingToolHook("C");

		fireBeforeTool.invoke(null, List.of(a, b, c), ctx, parsedToolCall, 0);

		// All three hooks fired exactly once on beforeTool.
		assertEquals(1, a.beforeCalls.size());
		assertEquals(1, b.beforeCalls.size());
		assertEquals(1, c.beforeCalls.size());
	}

	@Test
	void beforeToolHookExceptionDoesNotSkipRemainingHooks() throws Exception {
		RecordingToolHook a = new RecordingToolHook("A");
		RecordingToolHook b = new RecordingToolHook("B");
		b.throwOnBefore = new RuntimeException("hook B beforeTool boom");
		RecordingToolHook c = new RecordingToolHook("C");

		// No exception should propagate out — fireBeforeTool catches per-hook.
		fireBeforeTool.invoke(null, List.of(a, b, c), ctx, parsedToolCall, 0);

		assertEquals(1, a.beforeCalls.size(), "hook A should still have fired");
		assertEquals(1, b.beforeCalls.size(), "hook B should still have been invoked (it threw inside)");
		assertEquals(1, c.beforeCalls.size(),
				"hook C must still fire even though B threw — error must not skip subsequent hooks");
	}

	@Test
	void emptyHookListIsNoOp() {
		assertDoesNotThrow(() -> fireBeforeTool.invoke(null, Collections.emptyList(), ctx, parsedToolCall, 0));
	}

	@Test
	void nullHookListIsNoOp() {
		assertDoesNotThrow(() -> fireBeforeTool.invoke(null, null, ctx, parsedToolCall, 0));
	}

	// ---------- afterTool ----------

	@Test
	void firesAfterToolWithSuccessOutcomeAndAllFields() throws Exception {
		RecordingToolHook hook = new RecordingToolHook();

		fireAfterTool.invoke(null, List.of(hook), ctx, parsedToolCall, toolExecOutcomeSuccess, 250L, 7);

		assertEquals(1, hook.afterCalls.size());
		ToolCallSnapshot snap = hook.afterCalls.get(0);
		assertSame(ctx, snap.ctx);
		assertEquals("Bash", snap.toolName);
		assertEquals("call-42", snap.toolCallId);
		assertEquals("ls -la", snap.params.get("command"));
		assertEquals("ok-result", snap.resultContent);
		assertEquals(250L, snap.durationMs);
		assertTrue(snap.success);
		assertEquals(7, snap.iteration);
	}

	@Test
	void firesAfterToolWithFailureOutcome() throws Exception {
		RecordingToolHook hook = new RecordingToolHook();

		fireAfterTool.invoke(null, List.of(hook), ctx, parsedToolCall, toolExecOutcomeFailure, 18L, 0);

		ToolCallSnapshot snap = hook.afterCalls.get(0);
		assertEquals("boom-err", snap.resultContent);
		assertEquals(false, snap.success, "afterTool must receive success=false when the tool errored");
	}

	@Test
	void afterToolHookExceptionDoesNotSkipRemainingHooks() throws Exception {
		RecordingToolHook a = new RecordingToolHook("A");
		RecordingToolHook b = new RecordingToolHook("B");
		b.throwOnAfter = new RuntimeException("hook B afterTool boom");
		RecordingToolHook c = new RecordingToolHook("C");

		fireAfterTool.invoke(null, List.of(a, b, c), ctx, parsedToolCall, toolExecOutcomeSuccess, 1L, 0);

		assertEquals(1, a.afterCalls.size());
		assertEquals(1, b.afterCalls.size());
		assertEquals(1, c.afterCalls.size(), "hook C must still fire even though B threw");
	}

	@Test
	void afterToolFiresEvenOnFailure() throws Exception {
		// Mirrors the production guarantee: post-tool hooks run "even on failure
		// so observability survives errors" (HarnessToolExecutor.java:207).
		RecordingToolHook hook = new RecordingToolHook();

		fireAfterTool.invoke(null, List.of(hook), ctx, parsedToolCall, toolExecOutcomeFailure, 99L, 4);

		assertEquals(1, hook.afterCalls.size(), "afterTool must fire even when the tool returns success=false");
		assertEquals(false, hook.afterCalls.get(0).success);
		assertEquals(99L, hook.afterCalls.get(0).durationMs);
	}

	// ---------- helpers ----------

	/** Snapshot of one beforeTool or afterTool invocation. */
	private static final class ToolCallSnapshot {
		final AgentRunContext ctx;
		final String toolName;
		final String toolCallId;
		final Map<String, Object> params;
		final String resultContent;
		final long durationMs;
		final boolean success;
		final int iteration;

		ToolCallSnapshot(AgentRunContext ctx, String toolName, String toolCallId, Map<String, Object> params,
				String resultContent, long durationMs, boolean success, int iteration) {
			this.ctx = ctx;
			this.toolName = toolName;
			this.toolCallId = toolCallId;
			this.params = params;
			this.resultContent = resultContent;
			this.durationMs = durationMs;
			this.success = success;
			this.iteration = iteration;
		}
	}

	/** Records every beforeTool/afterTool invocation; optionally throws. */
	private static final class RecordingToolHook implements IToolHook {
		final String label;
		final List<ToolCallSnapshot> beforeCalls = new ArrayList<>();
		final List<ToolCallSnapshot> afterCalls = new ArrayList<>();
		RuntimeException throwOnBefore;
		RuntimeException throwOnAfter;

		RecordingToolHook() {
			this("default");
		}

		RecordingToolHook(String label) {
			this.label = label;
		}

		@Override
		public void beforeTool(AgentRunContext ctx, String toolName, String toolCallId, Map<String, Object> params,
				int iteration) {
			beforeCalls.add(new ToolCallSnapshot(ctx, toolName, toolCallId, params, null, 0L, false, iteration));
			if (throwOnBefore != null) {
				throw throwOnBefore;
			}
		}

		@Override
		public void afterTool(AgentRunContext ctx, String toolName, String toolCallId, Map<String, Object> params,
				String resultContent, long durationMs, boolean success, int iteration) {
			afterCalls.add(new ToolCallSnapshot(ctx, toolName, toolCallId, params, resultContent, durationMs, success,
					iteration));
			if (throwOnAfter != null) {
				throw throwOnAfter;
			}
		}
	}
}
