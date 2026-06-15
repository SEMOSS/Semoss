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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomMessageStore;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageType;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.reactor.agent.AgentHarnessResult;
import prerna.reactor.agent.AgentRunContext;
import prerna.reactor.agent.IAgentHarness;
import prerna.reactor.agent.config.AgentConfig;
import prerna.reactor.agent.config.SubAgentSpec;
import prerna.reactor.agent.exceptions.AgentBudgetException;
import prerna.reactor.agent.exceptions.AgentBudgetException.BudgetKind;
import prerna.reactor.agent.exceptions.AgentCancelledException;
import prerna.reactor.agent.exceptions.AgentMaxTurnsException;
import prerna.reactor.agent.skill.SkillScanner;
import prerna.reactor.agent.skill.SkillScanner.DiscoveredSkill;
import prerna.reactor.agent.subagent.AgentSubAgentRegistry;
import prerna.reactor.agent.subagent.SubAgentToolSynthesizer;

/**
 * SEMOSS-native agent harness - the canonical replacement for
 * {@link prerna.reactor.agent.RoomAgentHarness}.
 *
 * <p>Capabilities:
 * <ul>
 * <li>Multi-turn tool loop with parallel tool execution
 * <li>Configurable reflection rounds
 * <li>Cooperative cancellation via {@code Thread.isInterrupted()}
 * <li>Run-time budget via {@code max_seconds} paramMap key (0 = unlimited)
 * <li>Turn cap, reflection cap, and spawn depth from {@link AgentRunContext#getAgentConfig()}
 * <li>Subagent spawning with per-run and per-turn spawn caps
 * <li>Pre/post tool hooks and run lifecycle hooks via {@link prerna.reactor.agent.IAgentRunHook}
 * </ul>
 *
 * <p>Register name: {@value #NAME}. Activated by passing {@code harness="semoss"}
 * to {@code RunAgent()}.
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
	private static final String PARAM_PROJECT = "project";
	private static final String PARAM_SUBDIR = "subdir";
	private static final String PARAM_WORKSPACE_ID = "workspace_id";
	private static final String PARAM_WORKSPACE_ID_CAMEL = "workspaceId";
	private static final String ORNAMENT_AGENT_RUN_ID = "agentRunId";
	private static final String ORNAMENT_AGENT_RUN_ROLE = "agentRunRole";
	private static final String RUN_ROLE_INPUT = "input";
	private static final String RUN_ROLE_REFLECTION_INPUT = "reflection_input";
	private static final String RUN_ROLE_ASSISTANT = "assistant";
	private static final String RUN_ROLE_ASSISTANT_TOOL = "assistant_tool";
	private static final String RUN_ROLE_TOOL_RESULT = "tool_result";
	private static final String RUN_ROLE_FINAL_OUTPUT = "final_output";

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
		paramMap.put("stream", true);
		activateFileSpace(ctx.getInsight(), ctx.getFilePath());

		// Spawn tools are shown when this run is below the configured depth cap.
		// max_subagent_depth=0 disables spawning entirely; =1 = root only; =2 = root + one level; etc.
		AgentConfig agentConfig = ctx.getAgentConfig();
		AgentConfig.SubAgentSpawnPolicy policy = agentConfig.getSpawnPolicy();
		List<SubAgentSpec> subAgentSpecs = agentConfig.getSubagents();
		boolean canSpawn = ctx.getSpawnDepth() < policy.getMaxSubagentDepth();
		List<Map<String, Object>> subAgentTools = new ArrayList<>();
		if (canSpawn) {
			subAgentTools.addAll(SubAgentToolSynthesizer.allTools(subAgentSpecs));
		}
		injectSubAgentTools(paramMap, subAgentTools);

		// Register on root only; descendants look up the shared per-tree budget. Released in finally.
		String rootJobIdRegistered = null;
		if (ctx.getSpawnDepth() == AgentRunContext.ROOT_SPAWN_DEPTH) {
			String runJobId = ThreadStore.getJobId();
			if (runJobId != null && !runJobId.isBlank()) {
				if (AgentSubAgentRegistry.getManager().registerRoot(runJobId, policy)) {
					rootJobIdRegistered = runJobId;
				}
				logger.info(
						"SemossAgentHarness: root spawn policy active jobId={} maxDepth={} maxPerRun={} maxPerTurn={}",
						runJobId, policy.getMaxSubagentDepth(), policy.getMaxSubagentsPerRun(),
						policy.getMaxSpawnsPerTurn());
			}
		}

		String agentSidePrompt = agentConfig.getComposedAgentPrompt();

		Map<String, Object> opts = room.getOptionsMap();
		boolean hadInstructions = opts.containsKey("instructions");
		Object originalInstructions = hadInstructions ? opts.get("instructions") : null;

		StringBuilder composed = new StringBuilder(SemossHarnessPrompts.SYSTEM_PROMPT);
		// Prompt block matches the tools exposed to this run.
		if (canSpawn) {
			composed.append("\n\n").append(buildSubAgentPromptBlock(subAgentSpecs));
		}
		// Advertise skills materialized into the working dir (by SkillStager, earlier in the run) so
		// the model knows what it can pull in via LoadSkill. Empty when no skills are present.
		String availableSkillsBlock = buildAvailableSkillsPromptBlock(agentConfig.getWorkingDir());
		if (!availableSkillsBlock.isEmpty()) {
			composed.append("\n\n").append(availableSkillsBlock);
		}
		if (agentSidePrompt != null && !agentSidePrompt.isEmpty()) {
			composed.append("\n\n").append(agentSidePrompt);
		}
		opts.put("instructions", composed.toString());
		room.setOptionsMap(opts);

		logger.info(
				"SemossAgentHarness: composed system prompt room={} workspaceId={} harnessChars={} agentSideChars={} (agentAgentsMd={} workdirAgentsMd={} authored={})",
				room.getId(), agentConfig.getWorkspaceId(),
				SemossHarnessPrompts.SYSTEM_PROMPT.length(),
				agentSidePrompt != null ? agentSidePrompt.length() : 0,
				lengthOrZero(agentConfig.getAgentAgentsMd()),
				lengthOrZero(agentConfig.getWorkdirAgentsMd()),
				lengthOrZero(agentConfig.getAuthoredPrompt()));

		try {
			String systemPrompt = room.getEffectiveSystemPrompt();

			// Start the clock BEFORE the first model call so it counts against max_seconds.
			AgentLoopState state = new AgentLoopState();
			String inputMessageId = null;
			String finalOutputMessageId = null;
			int runMessageStartIndex = room.getMessages().size();

			InputMessage firstMsg = InputMessage.builder(room).withSystemPrompt(systemPrompt).withText(ctx.getInput())
					.withModelType(ctx.getModelEngine().getModelType()).withParamMap(paramMap).build();
			tagAgentRun(firstMsg, ctx.getRunId(), RUN_ROLE_INPUT);
			inputMessageId = firstMsg.getMessageId();

			logger.info("SemossAgentHarness: initial ask room={} model={} inputLen={}", room.getId(),
					ctx.getModelEngine().getEngineId(), ctx.getInput().length());

			ResponseMessage response = room.ask(firstMsg, ctx.getModelEngine(), null);
			tagAgentRun(response, ctx.getRunId(), roleForAssistant(response));
			if (Thread.currentThread().isInterrupted()) {
				throw new AgentCancelledException("Agent run cancelled during initial model call");
			}
			if (maxSeconds > 0 && state.getElapsedMs() > maxSeconds * 1000L) {
				throw new AgentBudgetException(BudgetKind.RUN_TIME, "Run-time budget of " + maxSeconds
						+ "s exceeded during initial model call (" + state.getElapsedMs() + "ms elapsed)");
			}

			while (!state.isTerminal()) {

				if (Thread.currentThread().isInterrupted()) {
					logger.info("SemossAgentHarness: cancelled room={} after iterations={}", room.getId(),
							state.getIterations());
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

						Map<String, Object> reflectionParams = new HashMap<>(paramMap);
						injectSubAgentTools(reflectionParams, subAgentTools);
						InputMessage reflectionMsg = InputMessage.builder(room).withSystemPrompt(systemPrompt)
								.withText(SemossHarnessPrompts.REFLECTION_PROMPT).withModelType(ctx.getModelEngine().getModelType())
								.withParamMap(reflectionParams).build();
						tagAgentRun(reflectionMsg, ctx.getRunId(), RUN_ROLE_REFLECTION_INPUT);
						response = room.ask(reflectionMsg, ctx.getModelEngine(), null);
						tagAgentRun(response, ctx.getRunId(), roleForAssistant(response));

					} else {
						state.setFinalText(response.getContent());
						finalOutputMessageId = response.getMessageId();
						tagAgentRun(response, ctx.getRunId(), RUN_ROLE_FINAL_OUTPUT);
						state.setTerminal(true);
					}

				} else if (msgType == MessageType.RESPONSE_TOOL) {

					room.updateToolResponseMeta(response);
					tagAgentRun(response, ctx.getRunId(), RUN_ROLE_ASSISTANT_TOOL);
					// Re-inject synthesized tools so the tool-result follow-up call sees a fresh
					// list (Room.appendToolsToParams mutates the existing 'tools' value in place).
					injectSubAgentTools(paramMap, subAgentTools);
					ResponseMessage next = HarnessToolExecutor.executeToolBatch(response, state, paramMap, ctx);
					tagAgentRunMessagesFrom(room, runMessageStartIndex, ctx.getRunId());
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
					finalOutputMessageId = response.getMessageId();
					tagAgentRun(response, ctx.getRunId(), RUN_ROLE_FINAL_OUTPUT);
					state.setTerminal(true);
				}
			}
			persistAgentRunTags(room, ctx);

			logger.info("SemossAgentHarness: done room={} iterations={} reflections={} elapsedMs={}", room.getId(),
					state.getIterations(), state.getReflectionsUsed(), state.getElapsedMs());
			if (state.getFinalText() != null) {
				AgentSubAgentRegistry.getManager().emitSubAgentCompleted(ThreadStore.getJobId(), state.getFinalText());
			}

			return new AgentHarnessResult(state.getFinalText(), state.getIterations(),
					state.getToolCallRecordsSnapshot(), state.getReflectionsUsed(), inputMessageId,
					finalOutputMessageId);
		} finally {
			// Always restore -- we always mutated options.instructions above.
			if (hadInstructions) {
				opts.put("instructions", originalInstructions);
			} else {
				opts.remove("instructions");
			}
			room.setOptionsMap(opts);
			if (rootJobIdRegistered != null) {
				AgentSubAgentRegistry.getManager().unregisterRoot(rootJobIdRegistered);
			}
		}
	}

	private static void activateFileSpace(Insight insight, String filePath) {
		if (insight == null || filePath == null || filePath.trim().isEmpty()) {
			return;
		}
		insight.setInsightFolder(filePath.trim());
	}

	private static void tagAgentRun(AbstractMessage message, String runId, String role) {
		if (message == null || runId == null || runId.trim().isEmpty()) {
			return;
		}
		message.setOrnament(ORNAMENT_AGENT_RUN_ID, runId);
		if (role != null && !role.trim().isEmpty()) {
			message.setOrnament(ORNAMENT_AGENT_RUN_ROLE, role);
		}
	}

	private static void tagAgentRunMessagesFrom(Room room, int startIndex, String runId) {
		if (room == null || runId == null || runId.trim().isEmpty()) {
			return;
		}
		List<AbstractMessage> messages = room.getMessages();
		int from = Math.max(0, startIndex);
		for (int i = from; i < messages.size(); i++) {
			AbstractMessage message = messages.get(i);
			if (message == null) {
				continue;
			}
			Object existingRole = message.getOrnament(ORNAMENT_AGENT_RUN_ROLE);
			String role = existingRole == null ? roleForMessage(message) : String.valueOf(existingRole);
			tagAgentRun(message, runId, role);
		}
	}

	private static String roleForMessage(AbstractMessage message) {
		if (message instanceof InputMessage && message.hasToolResultPart()) {
			return RUN_ROLE_TOOL_RESULT;
		}
		if (message instanceof InputMessage) {
			return RUN_ROLE_INPUT;
		}
		if (message instanceof ResponseMessage) {
			return roleForAssistant((ResponseMessage) message);
		}
		return null;
	}

	private static String roleForAssistant(ResponseMessage message) {
		if (message != null && message.getMessageType() == MessageType.RESPONSE_TOOL) {
			return RUN_ROLE_ASSISTANT_TOOL;
		}
		return RUN_ROLE_ASSISTANT;
	}

	private static void persistAgentRunTags(Room room, AgentRunContext ctx) {
		if (room == null || ctx == null || ctx.getRunId() == null || ctx.getRunId().trim().isEmpty()) {
			return;
		}
		String userId = ctx.getUserId();
		if (userId == null || userId.trim().isEmpty()) {
			userId = room.getUserId();
		}
		if (userId == null || userId.trim().isEmpty()) {
			return;
		}
		RoomMessageStore.persist(room, userId);
	}

	private static void stripHarnessOnlyParams(Map<String, Object> paramMap) {
		// These values steer the agent runner/tool harness, not the provider model API.
		// TODO(harness-params-refactor): this strip-list is the Java half of the same
		// concern handled in genai_client/message_builders/<provider>/SEMOSS_RUNTIME_PARAM_KEYS.
		// Both lists have to be kept in sync by hand today. See AGENTS_TASKS/harness-build-progress.md
		// "Agent-runtime paramMap key handling" for the design discussion (prefix
		// convention vs. moving runtime info onto AgentRunContext as typed fields).
		paramMap.remove(PARAM_MAX_SECONDS);
		paramMap.remove(PARAM_FILE_PATH);
		paramMap.remove(PARAM_FILE_PATH_CAMEL);
		paramMap.remove(PARAM_PERMISSION_MODE);
		paramMap.remove(PARAM_PERMISSION_MODE_SNAKE);
		paramMap.remove(PARAM_PROJECT);
		paramMap.remove(PARAM_SUBDIR);
		paramMap.remove(PARAM_WORKSPACE_ID);
		paramMap.remove(PARAM_WORKSPACE_ID_CAMEL);
	}

	private static int lengthOrZero(String s) {
		return s == null ? 0 : s.length();
	}

	/**
	 * Stuff a fresh copy of the synthesized subagent tool list into {@code paramMap.tools}.
	 * No-op when {@code subAgentTools} is empty. We always replace (rather than merge) so
	 * the in-place mutation done by {@code Room.appendToolsToParams} on the previous call
	 * doesn't carry stale entries forward.
	 */
	private static void injectSubAgentTools(Map<String, Object> paramMap, List<Map<String, Object>> subAgentTools) {
		if (paramMap == null || subAgentTools == null || subAgentTools.isEmpty()) {
			return;
		}
		paramMap.put("tools", new ArrayList<>(subAgentTools));
	}

	/**
	 * Inline guidance the LLM needs to use the spawn/wait/check tools correctly.
	 * Always appended to the semoss-harness system prompt. When {@code specs} is
	 * non-empty, the block also lists named specialist subagents the LLM can
	 * delegate to; when empty, only the anonymous-spawn guidance is included.
	 */
	private static String buildSubAgentPromptBlock(List<SubAgentSpec> specs) {
		StringBuilder sb = new StringBuilder();
		if (specs != null && !specs.isEmpty()) {
			sb.append("You can delegate work to specialist subagents via these tools: ");
			boolean first = true;
			for (SubAgentSpec spec : specs) {
				if (!first) sb.append(", ");
				sb.append(spec.getAlias());
				first = false;
			}
			sb.append(".\n");
			sb.append("You can also spawn anonymous subagents (clones of yourself) via `SpawnSubAgent`.\n\n");
		} else {
			sb.append("You can spawn anonymous subagents (clones of yourself) via `SpawnSubAgent` ")
			  .append("to delegate independent pieces of work in parallel.\n\n");
		}
		sb.append("Each spawn tool returns IMMEDIATELY with a `jobId` handle -- NOT the final answer.\n");
		sb.append("- To get a subagent's answer, call `WaitForSubAgent(jobId=<handle>)`. This blocks ")
		  .append("until the subagent completes or your timeoutSec elapses.\n");
		sb.append("- To check progress without blocking, call `CheckSubAgentStatus(jobId=<handle>)`.\n");
		sb.append("- You may fire multiple subagents BEFORE waiting on any -- they run in parallel.\n\n");
		sb.append("Subagents have separate room transcripts. They may share your workdir only when ")
		  .append("you explicitly set `inherit_parent_workdir=true` while spawning them.\n\n");

		sb.append("## Two patterns: blocking vs deferred\n\n");
		sb.append("**Pattern A -- blocking (default for quick subagent work, <30s expected):**\n");
		sb.append("  spawn -> spawn -> wait -> wait -> reply with combined results. ");
		sb.append("User waits until you're done. Simple, but ties up the conversation.\n\n");
		sb.append("**Pattern B -- deferred (use when subagents are expected to be slow, ");
		sb.append("the user might want to keep talking, or you've spawned 3+ children):**\n");
		sb.append("  spawn -> spawn -> reply to the user IMMEDIATELY with the jobIds and a note ");
		sb.append("that you've kicked them off (do NOT call WaitForSubAgent yet). End your turn.\n");
		sb.append("  Subagents continue running in the background between your turns -- they don't ");
		sb.append("pause when you end your turn.\n\n");

		sb.append("### How to handle follow-ups in Pattern B\n\n");

		sb.append("**Rule 0 -- standing orders persist across turns.** ");
		sb.append("If the user's earlier prompt authorized a downstream action that depends on ");
		sb.append("subagent completion (e.g. \"spawn 2 subagents to plan trips and then write the ");
		sb.append("md files\" -- the writing is a standing order), then on ANY later turn where you ");
		sb.append("observe the subagents are terminal, immediately collect their output via ");
		sb.append("`WaitForSubAgent` AND execute the standing-order action in the same turn -- EVEN if ");
		sb.append("the user's immediate message only asked for status. The user has already ");
		sb.append("authorized the downstream work; asking for permission again is rude and wastes ");
		sb.append("their time. The ONLY exception is if the user explicitly restricts you to ");
		sb.append("status-only (\"just tell me if they're done, don't do anything else\").\n\n");

		sb.append("Concrete example of Rule 0 in action:\n");
		sb.append("```\n");
		sb.append("turn 1 user: \"spawn 2 subagents to plan trips. Don't wait. Then write md files.\"\n");
		sb.append("turn 1 agent: spawn x 2, reply with jobIds, end turn.\n");
		sb.append("turn 2 user: \"random unrelated question\"\n");
		sb.append("turn 2 agent: answer normally.\n");
		sb.append("turn 3 user: \"are they done?\"\n");
		sb.append("turn 3 agent: check x 2 -> terminal observed -> IMMEDIATELY wait x 2 ->\n");
		sb.append("              WriteFile x 2 -> reply: \"yes, done; here are the filenames\".\n");
		sb.append("              (The 'write md files' standing order from turn 1 fires now.)\n");
		sb.append("```\n\n");

		sb.append("**Rule 1 -- user intent maps to a tool.** When there's no standing order yet ");
		sb.append("(or you genuinely can't tell from context whether the user wants output):\n");
		sb.append("- \"are they done?\" / \"what's the status?\" / \"check on them\" -> call ");
		sb.append("`CheckSubAgentStatus(jobId)` (non-blocking) and report status only.\n");
		sb.append("- \"what did they say?\" / \"give me the results\" / \"did you finish the task?\" / ");
		sb.append("\"did you write the file?\" / any request for the actual output -> call ");
		sb.append("`WaitForSubAgent(jobId)` directly. If the subagent is done, it returns immediately ");
		sb.append("with the text. If still running, it blocks briefly. DO NOT call CheckSubAgentStatus ");
		sb.append("first in this case -- just go straight to WaitForSubAgent.\n\n");

		sb.append("**Reporting status -- required format.** After calling CheckSubAgentStatus, your reply ");
		sb.append("must begin with a direct binary answer before any narration:\n");
		sb.append("- any job non-terminal -> start with \"No, not yet.\" then list each job's status.\n");
		sb.append("- all jobs terminal -> start with \"Yes\" then list each.\n");
		sb.append("- mixed -> start with \"Partially:\" then list which are done vs running.\n");
		sb.append("Never make the user infer completion state from process narration like \"I'm ");
		sb.append("checking...\" or \"if they're done, I'll...\". Those describe what you did; they ");
		sb.append("don't answer the question. Answer first, narrate second.\n\n");

		sb.append("**Rule 2 -- never ask permission for the obvious next step.** When ");
		sb.append("CheckSubAgentStatus returns terminal AND there's any reasonable next action ");
		sb.append("(collect output, run the standing-order downstream work, summarize), just do ");
		sb.append("it in the SAME turn. Don't say \"want me to collect the results?\" -- the user ");
		sb.append("said what they want, just do it.\n\n");

		sb.append("Prefer Pattern B when: the user's request is open-ended planning/research that ");
		sb.append("may take a while, or when blocking would prevent the user from following up. ");
		sb.append("Prefer Pattern A when: the user explicitly asked for a single combined answer ");
		sb.append("and the work is expected to be fast.");
		return sb.toString();
	}

	/**
	 * Renders the skills discovered in {@code workingDir} as an {@code <available_skills>} block
	 * for the system prompt, mirroring {@link #buildSubAgentPromptBlock}. Returns an empty string
	 * when the working dir is blank or no skills are present, so the caller can skip appending an
	 * empty block.
	 *
	 * <p>Discovery is delegated to {@link SkillScanner#scan(String)} -- the same logic the
	 * {@code ListSkill} tool uses -- so the prompt and the tool agree on what's available. The
	 * {@code <location>} of each skill is its working-dir-relative folder (e.g.
	 * {@code .claude/skills/pdf}).
	 */
	private static String buildAvailableSkillsPromptBlock(String workingDir) {
		List<DiscoveredSkill> skills = SkillScanner.scan(workingDir);
		logger.info("SemossAgentHarness: discovered {} skill(s) for available_skills block workingDir={}",
				skills.size(), workingDir);
		if (skills.isEmpty()) {
			return "";
		}
		StringBuilder sb = new StringBuilder();
		sb.append("<available_skills>\n");
		for (DiscoveredSkill skill : skills) {
			sb.append("  <skill>\n");
			sb.append("    <name>").append(xmlEscape(skill.getName())).append("</name>\n");
			sb.append("    <description>\n");
			sb.append("      ").append(xmlEscape(skill.getDescription())).append("\n");
			sb.append("    </description>\n");
			sb.append("    <location>").append(xmlEscape(skill.getDirectory())).append("</location>\n");
			sb.append("  </skill>\n");
		}
		sb.append("</available_skills>");
		return sb.toString();
	}

	/** Minimal XML escaping for values interpolated into the {@code <available_skills>} block. */
	private static String xmlEscape(String s) {
		if (s == null || s.isEmpty()) {
			return "";
		}
		StringBuilder sb = new StringBuilder(s.length());
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			switch (c) {
				case '&': sb.append("&amp;"); break;
				case '<': sb.append("&lt;");  break;
				case '>': sb.append("&gt;");  break;
				default:  sb.append(c);
			}
		}
		return sb.toString();
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
