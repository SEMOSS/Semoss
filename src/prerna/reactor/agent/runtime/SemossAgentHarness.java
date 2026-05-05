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

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageType;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.om.Insight;
import prerna.reactor.agent.AgentHarnessResult;
import prerna.reactor.agent.AgentMaxTurnsException;
import prerna.reactor.agent.AgentRunContext;
import prerna.reactor.agent.IAgentHarness;
import prerna.reactor.agent.runtime.AgentBudgetException.BudgetKind;

/**
 * SEMOSS-native agent harness - the canonical replacement for
 * {@link prerna.reactor.agent.RoomAgentHarness}.
 *
 * <p>
 * Implements a single unified loop with explicit {@link AgentLoopState} so
 * future additions (hooks, compaction, memory, observability) have clear
 * injection points without restructuring the loop itself.
 *
 * <p>
 * Current capabilities (v1 - this PR):
 * <ul>
 * <li>Multi-turn tool loop with parallel tool execution
 * <li>Configurable reflection rounds
 * <li>Cooperative cancellation via {@code Thread.isInterrupted()}
 * <li>Run-time budget via {@code max_seconds} paramMap key (0 = unlimited)
 * <li>Turn cap from {@link AgentRunContext#getMaxTurns()}
 * </ul>
 *
 * <p>
 * Planned (separate PRs):
 * <ul>
 * <li>Hooks / HookBus (pre/post lifecycle interceptors)
 * <li>Context compaction
 * <li>Memory and plan layer
 * <li>Observability spans
 * <li>WorkspaceConfigV2 loading
 * </ul>
 *
 * <p>
 * Register name: {@value #NAME}. Activated by passing {@code harness="semoss"}
 * to {@code RunAgent()} or when a workspace has {@code CONFIG_VERSION >= 2}.
 */
public class SemossAgentHarness implements IAgentHarness {

	private static final Logger logger = LogManager.getLogger(SemossAgentHarness.class);

	/** Registry key {@value}. */
	public static final String NAME = "semoss";

	/** paramMap key for an optional run-time limit in seconds (0 = no limit). */
	public static final String PARAM_MAX_SECONDS = "max_seconds";

	private static final String PARAM_FILE_PATH = "file_path";
	private static final String PARAM_FILE_PATH_CAMEL = "filePath";
	private static final String PARAM_PERMISSION_MODE = "permissionMode";
	private static final String PARAM_PERMISSION_MODE_SNAKE = "permission_mode";

	private static final String REFLECTION_PROMPT = "Review the analysis you just produced. Are there important aspects you have not yet "
			+ "examined, or tool calls that would meaningfully improve the completeness or accuracy "
			+ "of your answer? If yes, make those tool calls now and incorporate the new findings "
			+ "into your answer. If the analysis is already thorough and complete, respond with "
			+ "your final consolidated answer.";

	@Override
	public String getName() {
		return NAME;
	}

	@Override
	public AgentHarnessResult execute(AgentRunContext ctx) throws Exception {
		Room room = ctx.getRoom();
		Map<String, Object> paramMap = new HashMap<>(ctx.getParamMap());
		int maxSeconds = resolveMaxSeconds(paramMap);
		stripHarnessOnlyParams(paramMap);
		activateFileSpace(ctx.getInsight(), ctx.getFilePath());
		SemossAgentStream.userPrompt(room.getId(), ctx.getInput());

		// Overlay AGENTS.md / CLAUDE.md onto room.options.instructions for the duration
		// of this run. Putting it on the room (rather than just on the first InputMessage)
		// makes it visible to every Room.getEffectiveSystemPrompt() call — including the
		// synthetic tool-result InputMessage built inside Room.addToolExecutionResult,
		// which would otherwise drop AGENTS.md after the first tool call.
		// Restored in the finally block below. In-memory mutation only — no DB write.
		String agentsMd = AgentsMdLoader.discover(ctx.getFilePath());
		Map<String, Object> opts = room.getOptionsMap();
		boolean hadInstructions = opts.containsKey("instructions");
		Object originalInstructions = hadInstructions ? opts.get("instructions") : null;
		boolean optionsMutated = false;
		if (agentsMd != null) {
			String origStr = originalInstructions instanceof String ? (String) originalInstructions : null;
			String augmented = (origStr != null && !origStr.isEmpty())
					? agentsMd + "\n\n" + origStr
					: agentsMd;
			opts.put("instructions", augmented);
			room.setOptionsMap(opts);
			optionsMutated = true;
			logger.info(
					"SemossAgentHarness: overlaid {} chars of AGENTS.md onto room.options.instructions room={}",
					agentsMd.length(), room.getId());
		}

		try {
			String systemPrompt = room.getEffectiveSystemPrompt();

			// Start the clock BEFORE the first model call so it counts against max_seconds.
			AgentLoopState state = new AgentLoopState();

			InputMessage firstMsg = InputMessage.builder(room).withSystemPrompt(systemPrompt).withText(ctx.getInput())
					.withModelType(ctx.getModelEngine().getModelType()).withParamMap(paramMap).build();

			logger.info("SemossAgentHarness: initial ask room={} model={} inputLen={}", room.getId(),
					ctx.getModelEngine().getEngineId(), ctx.getInput().length());

			ResponseMessage response = room.ask(firstMsg, ctx.getModelEngine(), null);

			// Honor cancellation that arrived during the initial ask before doing any tool
			// work.
			if (Thread.currentThread().isInterrupted()) {
				throw new AgentCancelledException("Agent run cancelled during initial model call");
			}
			if (maxSeconds > 0 && state.getElapsedMs() > maxSeconds * 1000L) {
				throw new AgentBudgetException(BudgetKind.RUN_TIME, "Run-time budget of " + maxSeconds
						+ "s exceeded during initial model call (" + state.getElapsedMs() + "ms elapsed)");
			}

			while (!state.isTerminal()) {

				if (Thread.currentThread().isInterrupted()) {
					logger.info("SemossAgentHarness: cancellation detected at iteration={} room={}",
							state.getIterations(), room.getId());
					throw new AgentCancelledException(
							"Agent run cancelled after " + state.getIterations() + " iterations");
				}

				if (state.getIterations() >= ctx.getMaxTurns()) {
					logger.warn("SemossAgentHarness: maxTurns ({}) reached room={}", ctx.getMaxTurns(),
							room.getId());
					throw new AgentMaxTurnsException(ctx.getMaxTurns());
				}

				if (maxSeconds > 0 && state.getElapsedMs() > maxSeconds * 1000L) {
					throw new AgentBudgetException(BudgetKind.RUN_TIME,
							"Run-time budget of " + maxSeconds + "s exceeded after " + state.getIterations()
									+ " iterations (" + state.getElapsedMs() + "ms elapsed)");
				}

				if (response == null) {
					logger.warn("SemossAgentHarness: null response from model at iteration={} - treating as terminal",
							state.getIterations());
					state.setTerminal(true);
					break;
				}

				MessageType msgType = response.getMessageType();

				if (msgType == MessageType.RESPONSE_TEXT) {

					if (state.getReflectionsUsed() < ctx.getMaxReflections()) {
						state.incrementReflections();
						logger.info("SemossAgentHarness: reflection {}/{} room={}", state.getReflectionsUsed(),
								ctx.getMaxReflections(), room.getId());

						InputMessage reflectionMsg = InputMessage.builder(room).withSystemPrompt(systemPrompt)
								.withText(REFLECTION_PROMPT).withModelType(ctx.getModelEngine().getModelType())
								.withParamMap(new HashMap<>(paramMap)).build();
						response = room.ask(reflectionMsg, ctx.getModelEngine(), null);

					} else {
						state.setFinalText(response.getContent());
						SemossAgentStream.assistantText(SemossAgentStream.ASSISTANT_CONTENT_EVENT_ID,
								response.getContent());
						state.setTerminal(true);
					}

				} else if (msgType == MessageType.RESPONSE_TOOL) {

					ResponseMessage next = HarnessToolExecutor.executeToolBatch(response, state, paramMap, ctx);
					state.incrementIterations();

					if (next != null) {
						response = next;
					} else {
						logger.warn(
								"SemossAgentHarness: no model response after tool batch at iteration={} - treating as terminal",
								state.getIterations());
						state.setTerminal(true);
					}

				} else {
					logger.warn("SemossAgentHarness: unexpected MessageType {} at iteration={} - treating as terminal",
							msgType, state.getIterations());
					state.setFinalText(response.getContent());
					SemossAgentStream.assistantText(SemossAgentStream.ASSISTANT_CONTENT_EVENT_ID,
							response.getContent());
					state.setTerminal(true);
				}
			}

			logger.info("SemossAgentHarness: done room={} iterations={} reflections={} elapsedMs={}", room.getId(),
					state.getIterations(), state.getReflectionsUsed(), state.getElapsedMs());
			SemossAgentStream.agentResult(room.getId(), state.getIterations(), state.getReflectionsUsed(),
					state.getToolCallRecordsSnapshot().size());

			return new AgentHarnessResult(state.getFinalText(), state.getIterations(),
					state.getToolCallRecordsSnapshot(), state.getReflectionsUsed());
		} finally {
			if (optionsMutated) {
				if (hadInstructions) {
					opts.put("instructions", originalInstructions);
				} else {
					opts.remove("instructions");
				}
				room.setOptionsMap(opts);
			}
		}
	}

	private static void activateFileSpace(Insight insight, String filePath) {
		if (insight == null || filePath == null || filePath.trim().isEmpty()) {
			return;
		}
		insight.setInsightFolder(filePath.trim());
	}

	private static void stripHarnessOnlyParams(Map<String, Object> paramMap) {
		// These values steer the agent runner/tool harness, not the provider model API.
		paramMap.remove(PARAM_MAX_SECONDS);
		paramMap.remove(PARAM_FILE_PATH);
		paramMap.remove(PARAM_FILE_PATH_CAMEL);
		paramMap.remove(PARAM_PERMISSION_MODE);
		paramMap.remove(PARAM_PERMISSION_MODE_SNAKE);
	}

	private static int resolveMaxSeconds(Map<String, Object> paramMap) {
		Object val = paramMap.get(PARAM_MAX_SECONDS);
		if (val == null)
			return 0;
		if (val instanceof Number)
			return ((Number) val).intValue();
		if (val instanceof String) {
			try {
				return Integer.parseInt(((String) val).trim());
			} catch (NumberFormatException ignored) {
			}
		}
		return 0;
	}
}
