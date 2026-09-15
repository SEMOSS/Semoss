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

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.model.Room;
import prerna.om.ThreadStore;
import prerna.reactor.agent.AgentHarnessResult;
import prerna.reactor.agent.AgentRunner;
import prerna.reactor.agent.exceptions.AgentCancelledException;
import prerna.reactor.agent.exceptions.AgentInputRequiredException;
import prerna.reactor.agent.stream.AgentRunStreamService;
import prerna.reactor.agent.stream.AgentStreamItems;
import prerna.reactor.agent.subagent.AgentSubAgentRegistry;
import prerna.reactor.agent.subagent.SubAgentMeta;

/**
 * Runs one agent run to completion and records how it ended.
 *
 * <p>
 * {@link AgentRunQueueLoop} decides which runs start and on what thread; this
 * class is what that thread does. It seeds the run's identity, invokes the
 * harness, and translates the outcome into a durable status plus the stream
 * events a parent run needs.
 *
 * <h3>How an outcome is classified</h3> The harness signals its result by
 * throwing, so the catch block is the real decision point:
 * <ul>
 * <li>a cancel, direct or via the thread's interrupt flag, settles the run as
 * {@code CANCELLED};</li>
 * <li>{@link AgentInputRequiredException} means the harness paused on a tool
 * needing approval and is not terminal, so the run becomes
 * {@code INPUT_REQUIRED} and no terminal stream event is published;</li>
 * <li>anything else is {@code FAILED}.</li>
 * </ul>
 */
final class AgentRunExecutor {

	private static final Logger logger = LogManager.getLogger(AgentRunExecutor.class);

	/** Cap on what gets written to the {@code ERROR_MESSAGE} column. */
	private static final int MAX_ERROR_LENGTH = 8000;

	private AgentRunExecutor() {

	}

	/**
	 * Runs the harness on the calling thread and records the outcome.
	 *
	 * <p>
	 * {@code activeRun} is passed rather than looked up by id because a cancel
	 * unregisters the run as it releases the room; the object outlives the registry
	 * entry and is what still carries the cancel flag.
	 */
	static void execute(AgentRunRecord record, InsightHandle insightHandle, AgentRunRegistry.ActiveRun activeRun,
			Room automationResumeRoom) {
		String runId = record.runId();
		String jobId = runId;
		String parentRunId = record.request() != null ? record.request().getParentRunId() : null;
		try {
			insightHandle.seedThreadStore(runId);
			// A cancel can land after the room is claimed but before this thread starts,
			// and interrupting an unstarted thread does nothing. The flag is the only
			// record of it, so check before announcing RUNNING or doing any work.
			if (activeRun.isCancelRequested()) {
				throw new AgentCancelledException();
			}
			publishSubagentPatch(parentRunId, runId, AgentRunStatus.RUNNING);
			AgentRunRequest request = record.request();
			// Detect resume: the persisted request always has resumeMode=false on initial
			// submission, so fall back to checking for existing AGENT_RUN_ACTION rows.
			boolean resumeMode = request.isResumeMode() || AgentRunActionStore.hasAnyActions(runId);
			AgentHarnessResult result = automationResumeRoom == null
					? AgentRunner.run(request.getRoomId(), request.getInput(), request.getEngineIdFallback(),
							request.getHarnessType(), request.getMaxTurns(), request.getMaxReflections(),
							request.getParamMap(), request.getAgentParamMap(), request.getMediaInputPaths(),
							request.getMediaUrls(), runId, insightHandle.insight(), resumeMode)
					: AgentRunner.resumeAutomationRun(request.getRoomId(), request.getInput(),
							request.getEngineIdFallback(), request.getHarnessType(), request.getMaxTurns(),
							request.getMaxReflections(), request.getParamMap(), request.getAgentParamMap(),
							request.getMediaInputPaths(), request.getMediaUrls(), runId, insightHandle.insight(),
							automationResumeRoom);
			if (result != null) {
				AgentRunStore.markInputMessage(runId, result.getInputMessageId());
			}

			jobId = firstNonBlank(ThreadStore.getJobId(), jobId);
			if (result != null) {
				AgentRunStore.markFinalOutputMessage(runId, result.getFinalOutputMessageId());
			}
			if (Thread.currentThread().isInterrupted()) {
				throw new AgentCancelledException();
			}
			AgentRunStore.markCompleted(runId, jobId, result != null ? result.getFinalText() : null);
			AgentRunStreamService.get().markTerminal(runId);
			publishSubagentTerminal(parentRunId, record, runId, AgentRunStatus.COMPLETED,
					result != null ? result.getFinalText() : null, null);
		} catch (Exception e) {
			jobId = firstNonBlank(ThreadStore.getJobId(), jobId);
			if (isCancelled(e)) {
				AgentRunStore.markCancelled(runId, jobId, boundedError(e));
				AgentRunStreamService.get().markTerminal(runId);
				publishSubagentTerminal(parentRunId, record, runId, AgentRunStatus.CANCELLED, null, boundedError(e));
			} else if (e instanceof AgentInputRequiredException) {
				// The harness already persisted the AGENT_RUN_ACTION rows; only
				// transition the durable run status here.
				AgentRunStore.markInputRequired(runId, jobId);
				publishSubagentPatch(parentRunId, runId, AgentRunStatus.INPUT_REQUIRED);
				logger.info("AgentRunExecutor: runId={} paused for user input", runId);
			} else {
				AgentRunStore.markFailed(runId, jobId, boundedError(e));
				AgentRunStreamService.get().markTerminal(runId);
				publishSubagentTerminal(parentRunId, record, runId, AgentRunStatus.FAILED, null, boundedError(e));
			}
			logger.warn("AgentRunExecutor: runId={} failed: {}", runId, e.getMessage(), e);
		} finally {
			ThreadStore.remove();
		}
	}

	private static void publishSubagentPatch(String parentRunId, String childRunId, AgentRunStatus status) {
		if (parentRunId == null || parentRunId.isBlank()) {
			return;
		}
		Map<String, Object> patch = new HashMap<>();
		patch.put("status", status.name());
		AgentRunStreamService.get().publishSubagentUpdated(parentRunId, childRunId, patch);
	}

	private static void publishSubagentTerminal(String parentRunId, AgentRunRecord record, String runId,
			AgentRunStatus status, String resultPreview, String error) {
		if (parentRunId == null || parentRunId.isBlank()) {
			return;
		}
		String alias = null;
		String workspaceId = record.request() != null ? record.request().getWorkspaceId() : null;
		SubAgentMeta meta = AgentSubAgentRegistry.getManager().lookup(runId);
		if (meta != null) {
			alias = meta.getAlias();
			if (meta.getWorkspaceId() != null) {
				workspaceId = meta.getWorkspaceId();
			}
		}
		Map<String, Object> item = AgentStreamItems.subagentItem(runId, alias, record.roomId(), workspaceId,
				status.name());
		if (resultPreview != null && !resultPreview.isBlank()) {
			item.put("resultPreview",
					AgentStreamItems.truncate(resultPreview, AgentStreamItems.MAX_RESULT_PREVIEW_CHARS));
		}
		if (error != null && !error.isBlank()) {
			item.put("error", AgentStreamItems.truncate(error, AgentStreamItems.MAX_RESULT_PREVIEW_CHARS));
		}
		AgentRunStreamService.get().publishSubagentCompleted(parentRunId, item);
	}

	/**
	 * Whether {@code t} represents a cancel rather than a failure, so the run
	 * settles as {@code CANCELLED}. The interrupt flag counts because a cancel
	 * interrupts the run's thread, and whatever that unblocks may surface as an
	 * unrelated exception type.
	 */
	private static boolean isCancelled(Throwable t) {
		Throwable cur = t;
		while (cur != null) {
			if (cur instanceof AgentCancelledException) {
				return true;
			}
			cur = cur.getCause();
		}
		return Thread.currentThread().isInterrupted();
	}

	/**
	 * Message for the {@code ERROR_MESSAGE} column, never null and never oversized.
	 */
	private static String boundedError(Throwable t) {
		String message = t == null ? null : t.getMessage();
		if (message == null || message.trim().isEmpty()) {
			message = t == null ? "Unknown agent run failure" : t.getClass().getName();
		}
		if (message.length() <= MAX_ERROR_LENGTH) {
			return message;
		}
		return message.substring(0, MAX_ERROR_LENGTH);
	}

	private static String firstNonBlank(String first, String second) {
		if (first != null && !first.trim().isEmpty()) {
			return first;
		}
		return second;
	}
}
