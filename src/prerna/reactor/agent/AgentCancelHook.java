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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.agent.subagent.AgentSubAgentRegistry;
import prerna.tcp.client.SocketClient;

/**
 * Single entry point for agent-aware cancel extras. Called from
 * {@code StopPixelExecutionReactor} after the generic Thread.interrupt + pySocket
 * interrupt paths. Every operation here is a safe no-op when the jobId is not
 * an agent job (registries return empty / null).
 */
public final class AgentCancelHook {

	private static final Logger logger = LogManager.getLogger(AgentCancelHook.class);

	private AgentCancelHook() {}

	/** Cascade cancel to subagent children and interrupt any CLI sidecar socket bound to this jobId. */
	public static void onStop(String jobId) {
		if (jobId == null || jobId.isBlank()) return;

		// subagent cascade (no-op for non-agent jobs)
		try {
			int cascaded = AgentSubAgentRegistry.getManager().cascadeCancel(jobId);
			if (cascaded > 0) {
				logger.info("AgentCancelHook: cascaded cancel to {} subagent job(s) under parentJobId={}", cascaded, jobId);
			}
		} catch (Exception e) {
			logger.warn("AgentCancelHook: cascadeCancel(parentJobId={}) failed: {}", jobId, e.toString());
		}

		// CLI harness sidecar interrupt (no-op when no CLI socket registered for this jobId)
		SocketClient cliSocket = AgentCliSocketRegistry.lookup(jobId);
		if (cliSocket != null) {
			try {
				cliSocket.interruptInsightJob(null, jobId);
				logger.info("AgentCancelHook: routed interrupt to CLI sidecar for jobId={}", jobId);
			} catch (Exception e) {
				logger.warn("AgentCancelHook: CLI sidecar interrupt failed jobId={}: {}", jobId, e.toString());
			}
		}
	}
}
