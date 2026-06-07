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

import com.github.f4b6a3.uuid.alt.GUID;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.reactor.agent.AgentHarnessResult;
import prerna.reactor.agent.AgentRunner;
import prerna.reactor.agent.exceptions.AgentCancelledException;

public final class AgentRuntimeManager {

	private static final int MAX_ERROR_LENGTH = 8000;
	private static final AgentRuntimeManager INSTANCE = new AgentRuntimeManager(new AgentRunStore());

	private final AgentRunStore store;

	public static AgentRuntimeManager get() {
		return INSTANCE;
	}

	AgentRuntimeManager(AgentRunStore store) {
		this.store = store;
	}

	public RunAgentResult run(RunAgentRequest request) throws Exception {
		String runId = GUID.v7().toUUID().toString();
		String userId = resolveUserId(request.getInsight());
		store.insertCreated(runId, request, userId);

		String jobId = ThreadStore.getJobId();
		store.markRunning(runId, jobId);
		try {
			AgentHarnessResult result = AgentRunner.run(
					request.getRoomId(),
					request.getInput(),
					request.getEngineIdFallback(),
					request.getHarnessType(),
					request.getMaxTurns(),
					request.getMaxReflections(),
					request.getParamMap(),
					request.getAgentParamMap(),
					runId,
					request.getInsight());

			jobId = firstNonBlank(ThreadStore.getJobId(), jobId);
			if (result != null) {
				store.markInputMessage(runId, result.getInputMessageId());
				store.markFinalOutputMessage(runId, result.getFinalOutputMessageId());
			}
			store.markCompleted(runId, jobId, result != null ? result.getFinalText() : null);
			return new RunAgentResult(runId, AgentRunStatus.COMPLETED, result);
		} catch (Exception e) {
			jobId = firstNonBlank(ThreadStore.getJobId(), jobId);
			if (isCancelled(e)) {
				store.markCancelled(runId, jobId, boundedError(e));
			} else {
				store.markFailed(runId, jobId, boundedError(e));
			}
			throw e;
		}
	}

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

	private static String resolveUserId(Insight insight) {
		if (insight == null) {
			return null;
		}
		User user = insight.getUser();
		if (user == null || user.getPrimaryLoginToken() == null) {
			return null;
		}
		return user.getPrimaryLoginToken().getId();
	}

	private static String firstNonBlank(String first, String second) {
		if (first != null && !first.trim().isEmpty()) {
			return first;
		}
		return second;
	}
}
