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

import java.util.concurrent.ConcurrentHashMap;

import prerna.tcp.client.SocketClient;

/**
 * Per-jobId registry of CLI-harness Python sidecar sockets. Populated by
 * {@code ClaudeCodeManager} / {@code GitHubCopilotPyManager} around their
 * blocking {@code runDirectPy} call; consulted by {@code StopPixelExecutionReactor}
 * so cancel routes the interrupt opcode to the right socket (the CLI harness's
 * private CPW, not the user's GAAS socket).
 */
public final class AgentCliSocketRegistry {

	private static final ConcurrentHashMap<String, SocketClient> BY_JOB_ID = new ConcurrentHashMap<>();

	private AgentCliSocketRegistry() {}

	public static void register(String jobId, SocketClient sc) {
		if (jobId == null || jobId.isBlank() || sc == null) return;
		BY_JOB_ID.put(jobId, sc);
	}

	public static void unregister(String jobId) {
		if (jobId == null || jobId.isBlank()) return;
		BY_JOB_ID.remove(jobId);
	}

	public static SocketClient lookup(String jobId) {
		if (jobId == null || jobId.isBlank()) return null;
		return BY_JOB_ID.get(jobId);
	}
}
