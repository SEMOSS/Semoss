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
package prerna.engine.impl.pipeline;

import prerna.auth.access.EngineGrantee;
import prerna.auth.utils.SecurityEngineAccessUtils;
import prerna.util.SandboxedClassLoading;

/**
 * An agent's run, which the platform does for the person using the agent.
 * Inside it, the platform's calls on this thread can use the permissions
 * engines and projects give the agent, the way an app's custom reactors use the
 * permissions given to the app: a call the person's own access does not cover
 * goes through when the engine or project gives the agent a permission that
 * covers it. A model called this way knows the agent, so the agent's App Limit
 * counts the call.
 *
 * <pre>
 * try (AgentRunOperation run = AgentRunOperation.begin(agentId)) {
 * 	stageSkills();
 * 	harness.execute();
 * }
 * </pre>
 *
 * <p>
 * Only the platform's Java code on the run's own thread uses it:
 * <ul>
 * <li>a project's custom code cannot load this class, and a call a project's
 * custom code makes is checked as that project's, never as the agent's;</li>
 * <li>{@link #begin(String)} opens nothing while a class of a project is on the
 * call stack;</li>
 * <li>a thread the run starts does not carry it, except the run's own parallel
 * tool calls, which begin it again for the same agent; Python reaches the
 * platform on threads of its own and never carries it, so whatever Python does
 * is checked as the person.</li>
 * </ul>
 *
 * <p>
 * A run begun inside another, such as a sub-agent's, uses its own agent's
 * permissions until it closes, then the outer run's again.
 */
public final class AgentRunOperation implements AutoCloseable {

	private static final ThreadLocal<AgentRunOperation> RUNNING = new ThreadLocal<>();

	private final String agentId;
	private final Thread thread;
	private final AgentRunOperation outer;
	private boolean closed;

	private AgentRunOperation(String agentId, AgentRunOperation outer) {
		this.agentId = agentId;
		this.thread = Thread.currentThread();
		this.outer = outer;
	}

	/**
	 * Begins an agent's run on this thread. Close it when the run ends; a
	 * try-with-resources statement does.
	 *
	 * @param agentId the agent, a project of type WORKSPACE
	 * @return the run to close; it gives nothing when a project's custom code is on
	 *         the call stack
	 * @throws IllegalArgumentException when the agent id is empty
	 */
	public static AgentRunOperation begin(String agentId) {
		if (agentId == null || agentId.isBlank()) {
			throw new IllegalArgumentException("An agent run needs the agent's id");
		}
		AgentRunOperation run = new AgentRunOperation(agentId.trim(), RUNNING.get());
		if (!SandboxedClassLoading.projectCodeIsOnTheStack()) {
			RUNNING.set(run);
		}
		return run;
	}

	/**
	 * Ends the run and puts back the one it began inside, if any. Closing it again,
	 * or from another thread, changes nothing.
	 */
	@Override
	public void close() {
		if (this.closed || Thread.currentThread() != this.thread) {
			return;
		}
		this.closed = true;
		if (RUNNING.get() != this) {
			return;
		}
		AgentRunOperation stillRunning = this.outer;
		while (stillRunning != null && stillRunning.closed) {
			stillRunning = stillRunning.outer;
		}
		if (stillRunning == null) {
			RUNNING.remove();
		} else {
			RUNNING.set(stillRunning);
		}
	}

	/**
	 * The agent the run is for.
	 *
	 * @return the id given to {@link #begin(String)}
	 */
	public String getAgentId() {
		return this.agentId;
	}

	/**
	 * Whether the agent whose run is going on this thread was given a permission on
	 * an engine or a project, for the checks a run makes before it calls one, such
	 * as whether its model may be used.
	 *
	 * @param targetId the engine or project
	 * @param project  whether it is a project
	 * @return whether a run is going and the engine or project gives its agent a
	 *         permission that is in effect
	 */
	public static boolean runningAgentCanUse(String targetId, boolean project) {
		String agentId = runningAgent();
		return agentId != null && targetId != null && SecurityEngineAccessUtils
				.getGranteePermission(EngineGrantee.ofApp(agentId), targetId, project) != null;
	}

	/**
	 * The agent whose run the platform is doing on this thread now.
	 *
	 * @return the agent's id, or null when no run is going on this thread or a
	 *         project's custom code is on the call stack
	 */
	public static String runningAgent() {
		AgentRunOperation run = RUNNING.get();
		if (run == null || SandboxedClassLoading.projectCodeIsOnTheStack()) {
			return null;
		}
		return run.agentId;
	}

}
