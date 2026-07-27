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

// Async submit handle returned by AgentRuntimeManager.run/runWithId. The run executes
// on a background worker; terminal results are read from the durable AGENT_RUN record
// (via waitForRun/getRun), not from this handle.
public final class RunAgentResult {

	private final String runId;
	private final AgentRunStatus status;
	private final String roomId;

	public RunAgentResult(String runId, AgentRunStatus status) {
		this(runId, null, status);
	}

	public RunAgentResult(String runId, String roomId, AgentRunStatus status) {
		this.runId = runId;
		this.roomId = roomId;
		this.status = status;
	}

	public String getRunId() {
		return runId;
	}

	public String getRoomId() {
		return roomId;
	}

	public AgentRunStatus getStatus() {
		return status;
	}

	public Map<String, Object> toMap() {
		Map<String, Object> map = new HashMap<>();
		map.put("runId", runId);
		map.put("roomId", roomId);
		map.put("status", status == null ? null : status.name());
		return map;
	}
}
