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

import org.apache.logging.log4j.ThreadContext;

import prerna.auth.User;
import prerna.logging.SemossLogUtils;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.om.ThreadStore;

/**
 * A snapshot of the request context an agent run was submitted from, taken so
 * the run can be executed later on a background thread.
 *
 * <p>
 * An agent run outlives the HTTP request that asked for it. By the time
 * {@link AgentRunExecutor} starts, the submitting thread is gone along with its
 * {@link ThreadStore} and log4j context, so identity has to be captured up
 * front and replayed. {@link #capture} takes the snapshot on the request thread
 * and {@link #seedThreadStore} replays it on the run's thread.
 *
 * <p>
 * The {@link Insight} here is a copy, not the caller's, and it is registered in
 * the {@link InsightStore} for the duration of the run. {@link #release} takes
 * it back out, and skipping that leaks an entry per run.
 *
 * @param insight         copy of the submitter's insight, carrying user and
 *                        project
 * @param insightId       id the copy is registered under
 * @param sessionId       submitting session
 * @param routeId         submitting route
 * @param localHostname   node that received the submission
 * @param localProtocol   protocol the submission arrived on
 * @param localPort       port the submission arrived on
 * @param log4jContextMap thread context replayed around the run so its logs
 *                        carry the submitter's request, session, and user
 */
record InsightHandle(Insight insight, String insightId, String sessionId, String routeId, String localHostname,
		String localProtocol, Integer localPort, Map<String, String> log4jContextMap) {

	/**
	 * Snapshots the caller's context. Must be called on the submitting thread,
	 * since it reads the ambient {@link ThreadStore} and log4j context.
	 */
	static InsightHandle capture(String runId, Insight source) {
		Insight clone = new Insight();
		User user = source.getUser();
		if (user == null) {
			user = ThreadStore.getUser();
		}
		clone.setUser(user);
		clone.setBaseURL(source.getBaseURL());
		clone.setProjectId(source.getProjectId());
		clone.setContextProjectId(source.getContextProjectId());
		String insightId = InsightStore.getInstance().put(clone);
		Map<String, String> log4jContextMap = captureLog4jContext(runId, user);
		String sessionId = ThreadStore.getSessionId();
		if (sessionId == null || sessionId.trim().isEmpty()) {
			sessionId = log4jContextMap.get(SemossLogUtils.SESSION_ID);
		}
		return new InsightHandle(clone, insightId, sessionId, ThreadStore.getRouteId(), ThreadStore.getLocalHostname(),
				ThreadStore.getLocalProtocol(), ThreadStore.getLocalPort(), log4jContextMap);
	}

	/** Replays the snapshot onto the calling thread, which is the run's thread. */
	void seedThreadStore(String runId) {
		ThreadStore.setJobId(runId);
		ThreadStore.setInsightId(insightId);
		if (insight != null) {
			ThreadStore.setUser(insight.getUser());
		}
		ThreadStore.setSessionId(sessionId);
		ThreadStore.setRouteId(routeId);
		ThreadStore.setLocalHostname(localHostname);
		ThreadStore.setLocalProtocol(localProtocol);
		ThreadStore.setLocalPort(localPort);
	}

	/** Unregisters the insight copy. Safe to call more than once. */
	void release() {
		if (insightId != null) {
			InsightStore.getInstance().remove(insightId);
		}
	}

	private static Map<String, String> captureLog4jContext(String runId, User user) {
		Map<String, String> context = new HashMap<>(ThreadContext.getImmutableContext());
		putIfBlank(context, SemossLogUtils.REQUEST_ID, runId);
		putIfBlank(context, SemossLogUtils.SESSION_ID, ThreadStore.getSessionId());

		if (user != null && user.getPrimaryLoginToken() != null) {
			var token = user.getPrimaryLoginToken();
			putIfBlank(context, SemossLogUtils.USER_ID, token.getId());
			String userName = token.getResolvedDisplayName();
			if (userName == null || userName.trim().isEmpty()) {
				userName = token.getName();
			}
			if (userName == null || userName.trim().isEmpty()) {
				userName = token.getUsername();
			}
			if (userName == null || userName.trim().isEmpty()) {
				userName = token.getEmail();
			}
			putIfBlank(context, SemossLogUtils.USER_NAME, userName);
			if (token.getProvider() != null) {
				putIfBlank(context, SemossLogUtils.USER_TYPE, token.getProvider().getLabel());
			}
		}
		return context;
	}

	private static void putIfBlank(Map<String, String> context, String key, String value) {
		if (value == null || value.trim().isEmpty()) {
			return;
		}
		String existing = context.get(key);
		if (existing == null || existing.trim().isEmpty()) {
			context.put(key, value);
		}
	}
}
