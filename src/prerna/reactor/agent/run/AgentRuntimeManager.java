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

import java.util.Map;

import com.github.f4b6a3.uuid.alt.GUID;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.reactor.agent.exceptions.AgentCancelledException;

public final class AgentRuntimeManager {

	private static final int MAX_ERROR_LENGTH = 8000;
	private static final AgentRuntimeManager INSTANCE = new AgentRuntimeManager(new AgentRunStore());

	private final AgentRunStore store;
	private final AgentRunQueueCoordinator queueCoordinator;
	private final AgentRunWorker worker;

	public static AgentRuntimeManager get() {
		return INSTANCE;
	}

	AgentRuntimeManager(AgentRunStore store) {
		this.store = store;
		this.queueCoordinator = new AgentRunQueueCoordinator(store);
		this.worker = new AgentRunWorker(this, store, queueCoordinator);
	}

	public RunAgentResult run(RunAgentRequest request) {
		String runId = GUID.v7().toUUID().toString();
		String userId = resolveUserId(request.getInsight());
		store.insertQueued(runId, request, userId);
		worker.rememberInsight(runId, request.getInsight());
		worker.signal();
		return new RunAgentResult(runId, request.getRoomId(), AgentRunStatus.QUEUED, null);
	}

	public Map<String, Object> getRun(String runId, Insight insight) {
		if (runId == null || runId.trim().isEmpty()) {
			throw new IllegalArgumentException("runId is required");
		}
		Map<String, Object> run = store.getRunMap(runId, insight);
		if (run == null) {
			throw new IllegalArgumentException("No AGENT_RUN found for runId=" + runId);
		}
		return run;
	}

	boolean isCancelled(Throwable t) {
		Throwable cur = t;
		while (cur != null) {
			if (cur instanceof AgentCancelledException) {
				return true;
			}
			cur = cur.getCause();
		}
		return Thread.currentThread().isInterrupted();
	}

	String boundedError(Throwable t) {
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

	String firstNonBlank(String first, String second) {
		if (first != null && !first.trim().isEmpty()) {
			return first;
		}
		return second;
	}

}
