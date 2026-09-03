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

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import com.google.gson.Gson;

import prerna.auth.User;
import prerna.engine.api.ToolExecutionResult;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.om.ThreadStore;
import prerna.reactor.agent.AgentHarnessResult;
import prerna.reactor.agent.AgentRunContext;
import prerna.reactor.agent.AgentRunTarget;
import prerna.reactor.agent.IToolHook;
import prerna.reactor.agent.config.SubAgentSpec;
import prerna.reactor.agent.exceptions.AgentCancelledException;
import prerna.reactor.agent.exceptions.AgentInputRequiredException;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.reactor.agent.mcp.RunMCPToolReactor;
import prerna.reactor.agent.stream.AgentRunStreamService;
import prerna.reactor.agent.stream.AgentStreamItems;
import prerna.reactor.agent.subagent.SubAgentDispatcher;
import prerna.reactor.agent.subagent.SubAgentToolSynthesizer;
import prerna.sablecc2.comm.PixelJobManager;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Tool dispatch utilities for {@link SemossAgentHarness}.
 *
 * <p>
 * Handles parallel execution of one or more tool calls from a single model
 * response, submits results back to the Room, and returns the next model
 * {@link ResponseMessage}.
 */
final class HarnessToolExecutor {

	private static final Logger logger = LogManager.getLogger(HarnessToolExecutor.class);

	private static final String TOOL_STATUS_SUCCESS = "success";
	private static final String TOOL_STATUS_ERROR = "error";

	private static final Gson GSON = new Gson();
	static final int MAX_LIVE_TOOL_RESULT_CHARS = 12_000;

	/** How often the parallel-batch wait polls for cancellation. */
	private static final long CANCEL_POLL_MS = 100L;

	private HarnessToolExecutor() {
	}

	/**
	 * Executes all tool calls in {@code toolResponse} - single-threaded when there
	 * is one, parallel otherwise - records results on {@code state}, and returns
	 * the next {@link ResponseMessage} from the model, or {@code null} if the Room
	 * produced no follow-up.
	 */
	@SuppressWarnings("unchecked")
	static ResponseMessage executeToolBatch(ResponseMessage toolResponse, AgentLoopState state,
			Map<String, Object> paramMap, AgentRunContext ctx) {

		Room room = ctx.getRoom();
		String parentMsgId = toolResponse.getMessageId();
		List<Map<String, Object>> allToolCalls = toolResponse.getToolResponses();
		List<Map<String, Object>> toolCalls = new ArrayList<>();
		for (Map<String, Object> toolCall : allToolCalls) {
			if (MessageUtils.isServerToolCall(toolCall)) {
				continue;
			}
			toolCalls.add(toolCall);
		}
		if (toolCalls.size() < allToolCalls.size()) {
			logger.info("HarnessToolExecutor: skipping {} provider-executed server tool call(s) iter={} room={}",
					allToolCalls.size() - toolCalls.size(), state.getIterations(), room.getId());
		}
		if (toolCalls.isEmpty()) {
			return toolResponse;
		}
		String jobId = ThreadStore.getJobId();
		AskModelEngineResponse<?> nextModelResp = null;

		// Per-turn spawn cap - shared across the batch. Only spawn-kind calls
		// decrement.
		int spawnsPerTurnCap = ctx.getAgentConfig().getSpawnPolicy().getMaxSpawnsPerTurn();
		AtomicInteger spawnsRemainingInBatch = new AtomicInteger(spawnsPerTurnCap);

		// --- Human-in-the-loop pause: split SMSS_MCP_EXECUTION=ask tools ---
		// Non-ask tools still execute immediately and write their tool results to the
		// room. Only ask tools become AGENT_RUN_ACTION rows and pause the run.
		List<Map<String, Object>> askToolCalls = getAskToolCalls(toolCalls);
		if (!askToolCalls.isEmpty()) {
			List<Map<String, Object>> autoToolCalls = new ArrayList<>();
			for (Map<String, Object> toolCall : toolCalls) {
				if (!isAskTool(toolCall)) {
					autoToolCalls.add(toolCall);
				}
			}
			if (!autoToolCalls.isEmpty()) {
				logger.info("HarnessToolExecutor: executing {} non-ask tool(s) before pausing for {} ask tool(s) iter={} room={}",
						autoToolCalls.size(), askToolCalls.size(), state.getIterations(), room.getId());
				executeToolCalls(autoToolCalls, state, paramMap, parentMsgId, ctx, jobId, spawnsRemainingInBatch);
			}
			throw new AgentInputRequiredException(parentMsgId, askToolCalls);
		}

		nextModelResp = executeToolCalls(toolCalls, state, paramMap, parentMsgId, ctx, jobId,
				spawnsRemainingInBatch);

		if (nextModelResp == null) {
			return null;
		}
		Object lastMsg = room.getMessages().getLast();
		if (lastMsg instanceof ResponseMessage) {
			return (ResponseMessage) lastMsg;
		}
		logger.warn("HarnessToolExecutor: last message after tool batch is not ResponseMessage: {}",
				lastMsg == null ? "null" : lastMsg.getClass().getName());
		return null;
	}

	@SuppressWarnings("unchecked")
	private static AskModelEngineResponse<?> executeToolCalls(List<Map<String, Object>> toolCalls, AgentLoopState state,
			Map<String, Object> paramMap, String parentMsgId, AgentRunContext ctx, String jobId,
			AtomicInteger spawnsRemainingInBatch) {
		AskModelEngineResponse<?> nextModelResp = null;
		if (toolCalls.size() == 1) {
			ParsedToolCall tc = new ParsedToolCall(toolCalls.get(0));
			ToolExecResult r = executeOneTool(tc, state.getIterations(), paramMap, parentMsgId, ctx, jobId,
					spawnsRemainingInBatch);
			state.addToolCallRecord(r.record);
			nextModelResp = r.modelResponse;

		} else {
			logger.info("HarnessToolExecutor: {} tools in parallel iter={} room={}", toolCalls.size(),
					state.getIterations(), ctx.getRoom().getId());
			AgentThreadContext parentContext = AgentThreadContext.capture();

			try (ExecutorService pool = Executors.newFixedThreadPool(toolCalls.size())) {
				CompletableFuture<ToolExecResult>[] futures = new CompletableFuture[toolCalls.size()];
				for (int i = 0; i < toolCalls.size(); i++) {
					final ParsedToolCall tc = new ParsedToolCall(toolCalls.get(i));
					futures[i] = CompletableFuture.supplyAsync(
							() -> parentContext.call(() -> executeOneTool(tc, state.getIterations(), paramMap,
										parentMsgId, ctx, jobId, spawnsRemainingInBatch)),
							pool);
				}
				// Poll instead of allOf().join() so a cancel signal aborts the batch promptly.
				CompletableFuture<Void> all = CompletableFuture.allOf(futures);
				while (!all.isDone()) {
					if (Thread.currentThread().isInterrupted()) {
						for (CompletableFuture<ToolExecResult> f : futures) {
							f.cancel(true);
						}
						throw new AgentCancelledException(
								"Agent run cancelled during parallel tool batch (iter=" + state.getIterations() + ")");
					}
					try {
						all.get(CANCEL_POLL_MS, TimeUnit.MILLISECONDS);
					} catch (TimeoutException ignored) {
						// keep polling
					} catch (InterruptedException ie) {
						Thread.currentThread().interrupt();
						// loop back to the isInterrupted() branch, which cancels and throws.
					} catch (ExecutionException ignored) {
						// per-future failures are surfaced in the f.get() loop below.
					}
				}
				for (CompletableFuture<ToolExecResult> f : futures) {
					if (f.isCancelled()) {
						continue;
					}
					try {
						ToolExecResult r = f.get();
						state.addToolCallRecord(r.record);
						if (r.modelResponse != null) {
							nextModelResp = r.modelResponse;
						}
					} catch (InterruptedException ie) {
						Thread.currentThread().interrupt();
						logger.warn("HarnessToolExecutor: interrupted waiting for tool future");
					} catch (ExecutionException e) {
						logger.error("HarnessToolExecutor: tool future threw unexpectedly", e.getCause());
					}
				}
			}
		}
		return nextModelResp;
	}

	/**
	 * Executes one tool call and submits the result to the Room. Safe to call
	 * concurrently - Room.addToolExecutionResult() is synchronized.
	 */
	private static ToolExecResult executeOneTool(ParsedToolCall tc, int currentIter, Map<String, Object> paramMap,
			String parentMsgId, AgentRunContext ctx, String jobId, AtomicInteger spawnsRemainingInBatch) {

		logger.info("HarnessToolExecutor: tool start name={} callId={} iter={}", tc.rawToolName, tc.toolCallId,
				currentIter);

		// Pre-tool hooks - observer only; exceptions logged + swallowed.
		List<IToolHook> toolHooks = ctx.getAgentConfig().getToolHooks();
		fireBeforeTool(toolHooks, ctx, tc, currentIter);

		Map<String, Object> runningPatch = new LinkedHashMap<>();
		runningPatch.put("status", AgentStreamItems.TOOL_RUNNING);
		AgentRunStreamService.get().publishToolUpdated(jobId, tc.toolCallId, runningPatch);

		long startMs = System.currentTimeMillis();
		// jobId is captured on the caller's thread (where ThreadStore is valid) and
		// forwarded so subagent dispatch can address the parent's stream queue even
		// when this method runs on a worker thread from the parallel-tool pool.
		ToolExecOutcome outcome;
		try {
			outcome = executeToolSafely(tc, ctx, jobId, spawnsRemainingInBatch);
		} catch (AgentCancelledException cancelEx) {
			publishToolItemTerminal(jobId, tc, AgentStreamItems.TOOL_CANCELLED, null, cancelEx.getMessage(),
					System.currentTimeMillis() - startMs);
			throw cancelEx;
		}
		long durMs = System.currentTimeMillis() - startMs;

		// Post-tool hooks - fired even on failure so observability survives errors.
		fireAfterTool(toolHooks, ctx, tc, outcome, durMs, currentIter);
		publishToolResult(jobId, tc.toolCallId, tc.rawToolName, outcome.content, durMs, outcome.success);
		publishToolItemTerminal(jobId, tc,
				outcome.success ? AgentStreamItems.TOOL_COMPLETED : AgentStreamItems.TOOL_FAILED,
				outcome.success ? outcome.content : null, outcome.success ? null : outcome.content, durMs);

		logger.info("HarnessToolExecutor: tool end name={} durationMs={} success={}", tc.rawToolName, durMs,
				outcome.success);

		AgentHarnessResult.ToolCallRecord record = new AgentHarnessResult.ToolCallRecord(tc.rawToolName, tc.toolCallId,
				outcome.content, durMs, outcome.success);

		// Pass a fresh copy - Room.appendToolsToParams() mutates the map.
		AskModelEngineResponse<?> modelResp = ctx.getRoom().addToolExecutionResult(tc.toolCallId, tc.rawToolName,
				outcome.content, tc.toolParams, new HashMap<>(paramMap), parentMsgId, ctx.getModelEngine(),
				ctx.getInsight(), outcome.success ? TOOL_STATUS_SUCCESS : TOOL_STATUS_ERROR);

		return new ToolExecResult(record, modelResp);
	}

	/**
	 * Return only the tool calls in the batch with
	 * {@code SMSS_MCP_EXECUTION=ask}.
	 * The enriched {@code _meta} is attached by
	 * {@code Room.updateToolResponseMeta()} before this method is called.
	 */
	private static List<Map<String, Object>> getAskToolCalls(List<Map<String, Object>> toolCalls) {
		List<Map<String, Object>> askToolCalls = new ArrayList<>();
		if (toolCalls == null || toolCalls.isEmpty()) {
			return askToolCalls;
		}
		for (Map<String, Object> toolCall : toolCalls) {
			if (isAskTool(toolCall)) {
				askToolCalls.add(toolCall);
			}
		}
		return askToolCalls;
	}

	@SuppressWarnings("unchecked")
	private static boolean isAskTool(Map<String, Object> toolCall) {
		if (toolCall == null) {
			return false;
		}
		Object metaObj = toolCall.get("_meta");
		if (!(metaObj instanceof Map)) {
			return false;
		}
		Map<String, Object> meta = (Map<String, Object>) metaObj;
		Object execValue = meta.get(MCPUtility.SMSS_MCP_EXECUTION);
		return "ask".equalsIgnoreCase(String.valueOf(execValue));
	}

	private static void publishToolResult(String jobId, String toolCallId, String toolName, String output,
			long durationMs, boolean success) {
		if (jobId == null || jobId.isBlank() || toolCallId == null || toolCallId.isBlank()) {
			return;
		}
		Map<String, Object> data = new LinkedHashMap<>();
		data.put("kind", "tool-result");
		data.put("eventId", "semoss-tool-result-" + toolCallId);
		data.put("toolUseId", toolCallId);
		if (toolName != null && !toolName.isBlank()) {
			data.put("toolName", toolName);
		}
		data.put("status", success ? "completed" : "error");
		data.put("isPartial", false);
		data.put("durationMs", durationMs);
		String content = truncate(output, MAX_LIVE_TOOL_RESULT_CHARS);
		if (content != null && !content.isBlank()) {
			data.put("content", content);
		}
		data.put("timestamp", Instant.now().toString());

		Map<String, Object> envelope = new LinkedHashMap<>();
		envelope.put("stream_type", "tool");
		envelope.put("data", data);
		PixelJobManager.getManager().addStreamOut(jobId, envelope);
	}

	@SuppressWarnings("unchecked")
	private static void publishToolItemTerminal(String runId, ParsedToolCall tc, String status, String output,
			String error, long durationMs) {
		if (runId == null || runId.isBlank() || tc.toolCallId == null || tc.toolCallId.isBlank()) {
			return;
		}
		Object metaObj = tc.toolCall.get("_meta");
		Map<String, Object> meta = metaObj instanceof Map ? (Map<String, Object>) metaObj : null;
		Object title = tc.toolCall.get("title");
		Map<String, Object> item = AgentStreamItems.toolItem(tc.toolCallId, tc.rawToolName,
				title != null ? title.toString() : null, tc.toolParams, meta, status);
		String boundedOutput = truncate(output, MAX_LIVE_TOOL_RESULT_CHARS);
		if (boundedOutput != null && !boundedOutput.isBlank()) {
			item.put("output", boundedOutput);
		}
		String boundedError = truncate(error, MAX_LIVE_TOOL_RESULT_CHARS);
		if (boundedError != null && !boundedError.isBlank()) {
			item.put("error", boundedError);
		}
		item.put("durationMs", durationMs);
		AgentRunStreamService.get().publishToolCompleted(runId, item);
	}

	private static String truncate(String value, int maxChars) {
		if (value == null || value.length() <= maxChars) {
			return value;
		}
		return value.substring(0, maxChars) + "\n... [truncated for live stream]";
	}

	// Fire all configured pre-tool hooks. Each hook is isolated so a thrower does
	// not
	// skip the remaining hooks or abort the tool dispatch.
	private static void fireBeforeTool(List<IToolHook> hooks, AgentRunContext ctx, ParsedToolCall tc, int currentIter) {
		if (hooks == null || hooks.isEmpty()) {
			return;
		}
		for (IToolHook hook : hooks) {
			try {
				hook.beforeTool(ctx, tc.rawToolName, tc.toolCallId, new HashMap<>(tc.toolParams), currentIter);
			} catch (Throwable t) {
				logger.warn("HarnessToolExecutor: tool hook {}#beforeTool threw for tool '{}': {}",
						hook.getClass().getSimpleName(), tc.rawToolName, t.toString());
			}
		}
	}

	// Mirror of fireBeforeTool for the post-dispatch phase.
	private static void fireAfterTool(List<IToolHook> hooks, AgentRunContext ctx, ParsedToolCall tc,
			ToolExecOutcome outcome, long durMs, int currentIter) {
		if (hooks == null || hooks.isEmpty()) {
			return;
		}
		for (IToolHook hook : hooks) {
			try {
				hook.afterTool(ctx, tc.rawToolName, tc.toolCallId, new HashMap<>(tc.toolParams), outcome.content, durMs,
						outcome.success, currentIter);
			} catch (Throwable t) {
				logger.warn("HarnessToolExecutor: tool hook {}#afterTool threw for tool '{}': {}",
						hook.getClass().getSimpleName(), tc.rawToolName, t.toString());
			}
		}
	}

	private static ToolExecOutcome executeToolSafely(ParsedToolCall tc, AgentRunContext ctx, String parentJobId,
			AtomicInteger spawnsRemainingInBatch) {

		// 1. Subagent tools - named alias OR built-in spawn/check/wait - short-circuit
		// the MCP pipeline. The dispatcher returns a JSON string suitable for handing
		// straight back to the model.
		java.util.List<SubAgentSpec> specs = ctx.getAgentConfig().getSubagents();
		if (SubAgentToolSynthesizer.isSubAgentTool(tc.rawToolName, specs)) {
			try {
				String result = dispatchSubAgentTool(tc.rawToolName, tc.toolParams, ctx, specs, parentJobId,
						spawnsRemainingInBatch);
				return new ToolExecOutcome(result, true);
			} catch (AgentCancelledException e) {
				throw e;
			} catch (Exception e) {
				String msg = "Tool execution error: " + e.getMessage();
				logger.warn("HarnessToolExecutor: subagent tool '{}' failed: {}", tc.rawToolName, e.getMessage(), e);
				return new ToolExecOutcome(msg, false);
			}
		}

		ToolExecutionResult policyDenial = PlatformAgentTools.policyDenialResult(tc.rawToolName, ctx);
		if (policyDenial != null) {
			logger.warn("HarnessToolExecutor: denied default tool workspaceId={} roomId={} callId={} toolName={}",
					ctx.getAgentConfig().getWorkspaceId(), ctx.getRoom().getId(), tc.toolCallId, tc.rawToolName);
			String error = policyDenial.getError();
			return new ToolExecOutcome(error != null ? error : String.valueOf(policyDenial.getOutput()), false);
		}

		// 2. Platform default agent tools. These are not backed by room/workspace MCP
		// metadata, so resolve them by the default tool registry before the MCP path.
		if (PlatformAgentTools.isDefaultTool(tc.rawToolName)) {
			try {
				ToolExecutionResult result = PlatformAgentTools.executeDefaultToolResult(tc.rawToolName, tc.toolParams,
						ctx);
				String output = result.getOutput() != null ? result.getOutput().toString() : "";
				if (!result.isSuccess() && result.getError() != null && !result.getError().isBlank()) {
					output = result.getError();
				}
				return new ToolExecOutcome(output, result.isSuccess());
			} catch (AgentCancelledException e) {
				throw e;
			} catch (Exception e) {
				String msg = "Tool execution error: " + e.getMessage();
				logger.warn("HarnessToolExecutor: platform default tool '{}' failed: {}", tc.rawToolName,
						e.getMessage(), e);
				return new ToolExecOutcome(msg, false);
			}
		}

		// 3. Normal MCP tool path. Prefer Room-enriched metadata so shortened
		// provider-facing names still resolve; fall back to legacy UUID prefixes.
		ResolvedMcpTool resolved = resolveMcpTool(tc);
		if (resolved == null) {
			String msg = "Tool execution error: cannot resolve engine/project id from tool name '" + tc.rawToolName
					+ "'";
			logger.warn("HarnessToolExecutor: {}", msg);
			return new ToolExecOutcome(msg, false);
		}
		try {
			String result = callMcpToolViaReactor(resolved.toolName, resolved.engineId, tc.toolParams, ctx);
			boolean success = result == null || !result.startsWith("Tool execution error:");
			return new ToolExecOutcome(result, success);
		} catch (Exception e) {
			String msg = "Tool execution error: " + e.getMessage();
			logger.warn("HarnessToolExecutor: uncaught exception from tool '{}': {}", tc.rawToolName, e.getMessage(),
					e);
			return new ToolExecOutcome(msg, false);
		}
	}

	@SuppressWarnings("unchecked")
	private static ResolvedMcpTool resolveMcpTool(ParsedToolCall tc) {
		Map<String, Object> meta = null;
		Object metaObj = tc.toolCall.get("_meta");
		if (metaObj instanceof Map) {
			meta = (Map<String, Object>) metaObj;
		}

		String engineId = getString(meta, MCPUtility.SMSS_ENGINE_ID);
		if (engineId == null) {
			engineId = getString(meta, MCPUtility.SMSS_PROJECT_ID);
		}
		String[] parsed = MCPUtility.parseEngineIdFromFunctionName(tc.rawToolName);
		if (engineId == null && parsed != null) {
			engineId = parsed[0];
		}
		if (engineId == null) {
			return null;
		}

		String toolName = getString(meta, MCPUtility.SMSS_ORIGINAL_TOOL_NAME);
		if (toolName == null) {
			toolName = getString(tc.toolCall, "original_name");
		}
		if (toolName == null) {
			toolName = parsed != null ? parsed[1] : tc.rawToolName;
		}

		return new ResolvedMcpTool(engineId, toolName);
	}

	private static String getString(Map<String, Object> map, String key) {
		if (map == null || key == null) {
			return null;
		}
		Object value = map.get(key);
		if (value == null) {
			return null;
		}
		String str = value.toString().trim();
		return str.isEmpty() ? null : str;
	}

	/**
	 * Route a synthesized subagent tool call to {@link SubAgentDispatcher}.
	 * Built-in tools are matched by name; named subagent tools resolve to the
	 * matching {@link SubAgentSpec}.
	 */
	private static String dispatchSubAgentTool(String rawToolName, Map<String, Object> params, AgentRunContext ctx,
			java.util.List<SubAgentSpec> specs, String parentJobId, AtomicInteger spawnsRemainingInBatch) {
		Room parentRoom = ctx.getRoom();
		AgentRunTarget parentTarget = ctx.getAgentTarget();
		// Do not read instructions back from the live room here. SemossAgentHarness
		// temporarily replaces them with its fully composed runtime prompt while tools run.
		String parentAuthoredSystemPrompt = ctx.getAgentConfig().getAuthoredPrompt();
		if (SubAgentToolSynthesizer.TOOL_SPAWN_SUBAGENT.equals(rawToolName)) {
			if (!claimSpawnSlot(spawnsRemainingInBatch, ctx, rawToolName)) {
				return perTurnRejectedJson(ctx);
			}
			return SubAgentDispatcher.spawnAnonymous(params, parentRoom, ctx.getInsight(), parentJobId,
					parentAuthoredSystemPrompt, parentTarget);
		}
		if (SubAgentToolSynthesizer.TOOL_CHECK_SUBAGENT.equals(rawToolName)) {
			Object jobIdObj = params == null ? null : params.get("jobId");
			return SubAgentDispatcher.check(jobIdObj == null ? null : String.valueOf(jobIdObj), ctx.getInsight());
		}
		if (SubAgentToolSynthesizer.TOOL_WAIT_SUBAGENT.equals(rawToolName)) {
			Object jobIdObj = params == null ? null : params.get("jobId");
			Object timeoutObj = params == null ? null : params.get("timeoutSec");
			int timeoutSec = SubAgentDispatcher.DEFAULT_WAIT_TIMEOUT_SEC;
			if (timeoutObj instanceof Number) {
				timeoutSec = ((Number) timeoutObj).intValue();
			} else if (timeoutObj instanceof String) {
				try {
					timeoutSec = Integer.parseInt(((String) timeoutObj).trim());
				} catch (NumberFormatException ignored) {
					/* keep default */ }
			}
			return SubAgentDispatcher.wait(jobIdObj == null ? null : String.valueOf(jobIdObj), ctx.getInsight(),
					timeoutSec);
		}
		// Named subagent tool - look up the spec and spawn.
		SubAgentSpec spec = SubAgentToolSynthesizer.findSpec(specs, rawToolName);
		if (spec == null) {
			throw new IllegalStateException("Tool '" + rawToolName + "' classified as subagent but no matching spec");
		}
		if (!claimSpawnSlot(spawnsRemainingInBatch, ctx, rawToolName)) {
			return perTurnRejectedJson(ctx);
		}
		return SubAgentDispatcher.spawnNamed(spec, params, parentRoom, ctx.getInsight(), parentJobId,
					parentAuthoredSystemPrompt, parentTarget);
	}

	/** Atomic claim against the per-turn spawn budget; restores on miss. */
	private static boolean claimSpawnSlot(AtomicInteger budget, AgentRunContext ctx, String toolName) {
		if (budget == null) {
			return true;
		}
		if (budget.decrementAndGet() < 0) {
			budget.incrementAndGet();
			logger.warn("HarnessToolExecutor: per-turn spawn cap reached ({}). Rejecting subagent tool '{}'.",
					ctx.getAgentConfig().getSpawnPolicy().getMaxSpawnsPerTurn(), toolName);
			return false;
		}
		return true;
	}

	private static String perTurnRejectedJson(AgentRunContext ctx) {
		int cap = ctx.getAgentConfig().getSpawnPolicy().getMaxSpawnsPerTurn();
		Map<String, Object> err = new HashMap<>();
		err.put("error", "spawn_rejected_per_turn_cap");
		err.put("message",
				"Spawn rejected: this turn already has " + cap + " subagent spawn(s) in flight, "
						+ "which matches the configured maxSpawnsPerTurn. Wait for the current children to "
						+ "complete (call WaitForSubAgent) before spawning more, or split the work across "
						+ "multiple turns.");
		err.put("maxSpawnsPerTurn", cap);
		return GSON.toJson(err);
	}

	private static String callMcpToolViaReactor(String rawToolName, String engineId, Map<String, Object> params,
			AgentRunContext ctx) {
		try {
			RunMCPToolReactor reactor = new RunMCPToolReactor();
			reactor.In();
			reactor.setInsight(ctx.getInsight());

			GenRowStruct engineGrs = new GenRowStruct();
			engineGrs.add(new NounMetadata(engineId, PixelDataType.CONST_STRING));
			reactor.getNounStore().addNoun(ReactorKeysEnum.ENGINE.getKey(), engineGrs);

			GenRowStruct functionGrs = new GenRowStruct();
			functionGrs.add(new NounMetadata(rawToolName, PixelDataType.CONST_STRING));
			reactor.getNounStore().addNoun(ReactorKeysEnum.FUNCTION.getKey(), functionGrs);

			// Always attach paramValues — RunMCPToolReactor declares it required, so
			// no-arg
			// tools (ListSkill, TodoRead, …) would otherwise blow up with "Required
			// input(s)
			// missing: paramValues". Pass an empty map when the model sent no args.
			GenRowStruct paramGrs = new GenRowStruct();
			paramGrs.add(
					new NounMetadata(params != null ? params : java.util.Collections.emptyMap(), PixelDataType.MAP));
			reactor.getNounStore().addNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey(), paramGrs);

			NounMetadata result = reactor.execute();
			return result != null && result.getValue() != null ? result.getValue().toString() : "";
		} catch (Exception e) {
			return "Tool execution error: " + e.getMessage();
		}
	}

	// Internal value types

	/**
	 * Carries request-scoped state into parallel tool threads. The final tool
	 * result can synchronously trigger the next model ask, so both Log4j MDC and
	 * ThreadStore must be present on that thread.
	 */
	private static final class AgentThreadContext {
		private final Map<String, String> log4jContext;
		private final String insightId;
		private final String sessionId;
		private final String routeId;
		private final String jobId;
		private final User user;
		private final String localHostname;
		private final String localProtocol;
		private final Integer localPort;

		private AgentThreadContext(Map<String, String> log4jContext, String insightId, String sessionId, String routeId,
				String jobId, User user, String localHostname, String localProtocol, Integer localPort) {
			this.log4jContext = log4jContext;
			this.insightId = insightId;
			this.sessionId = sessionId;
			this.routeId = routeId;
			this.jobId = jobId;
			this.user = user;
			this.localHostname = localHostname;
			this.localProtocol = localProtocol;
			this.localPort = localPort;
		}

		private static AgentThreadContext capture() {
			return new AgentThreadContext(new HashMap<>(ThreadContext.getImmutableContext()), ThreadStore.getInsightId(),
					ThreadStore.getSessionId(), ThreadStore.getRouteId(), ThreadStore.getJobId(), ThreadStore.getUser(),
					ThreadStore.getLocalHostname(), ThreadStore.getLocalProtocol(), ThreadStore.getLocalPort());
		}

		private <T> T call(Supplier<T> task) {
			try (var ignored = CloseableThreadContext.putAll(log4jContext)) {
				ThreadStore.setInsightId(insightId);
				ThreadStore.setSessionId(sessionId);
				ThreadStore.setRouteId(routeId);
				ThreadStore.setJobId(jobId);
				ThreadStore.setUser(user);
				ThreadStore.setLocalHostname(localHostname);
				ThreadStore.setLocalProtocol(localProtocol);
				ThreadStore.setLocalPort(localPort);
				return task.get();
			} finally {
				ThreadStore.remove();
			}
		}
	}

	@SuppressWarnings("unchecked")
	static final class ParsedToolCall {
		final String toolCallId;
		final String rawToolName;
		final Map<String, Object> toolParams;
		final Map<String, Object> toolCall;

		ParsedToolCall(Map<String, Object> toolCall) {
			this.toolCall = toolCall != null ? new HashMap<>(toolCall) : new HashMap<>();
			this.toolCallId = String.valueOf(toolCall.get("id"));
			this.rawToolName = String.valueOf(toolCall.get("name"));
			// Providers vary: Anthropic uses "input" (Map), OpenAI/Responses-style uses
			// "arguments"
			// which can arrive as a Map OR as a JSON-encoded String. Mirror the parsing in
			// AskToolModelEngineResponse so we don't drop string-encoded payloads.
			Object argsObj = toolCall.get("arguments");
			if (argsObj == null) {
				argsObj = toolCall.get("input");
			}
			Map<String, Object> parsed = null;
			if (argsObj == null) {
				parsed = new HashMap<>();
			} else if (argsObj instanceof Map) {
				parsed = (Map<String, Object>) argsObj;
			} else {
				String json = argsObj.toString();
				try {
					parsed = GSON.fromJson(json, Map.class);
				} catch (Exception e) {
					logger.warn("HarnessToolExecutor: failed to parse tool arguments JSON for tool '{}': {}",
							this.rawToolName, e.getMessage());
				}
			}
			this.toolParams = parsed != null ? parsed : new HashMap<>();
		}
	}

	static final class ToolExecResult {
		final AgentHarnessResult.ToolCallRecord record;
		final AskModelEngineResponse<?> modelResponse;

		ToolExecResult(AgentHarnessResult.ToolCallRecord record, AskModelEngineResponse<?> modelResponse) {
			this.record = record;
			this.modelResponse = modelResponse;
		}
	}

	private static final class ToolExecOutcome {
		final String content;
		final boolean success;

		ToolExecOutcome(String content, boolean success) {
			this.content = content;
			this.success = success;
		}
	}

	private static final class ResolvedMcpTool {
		final String engineId;
		final String toolName;

		ResolvedMcpTool(String engineId, String toolName) {
			this.engineId = engineId;
			this.toolName = toolName;
		}
	}
}
