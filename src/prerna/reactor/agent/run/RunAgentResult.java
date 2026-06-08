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

import prerna.reactor.agent.AgentHarnessResult;

public final class RunAgentResult {

	private final String runId;
	private final AgentRunStatus status;
	private final AgentHarnessResult result;
	private final String roomId;

	public RunAgentResult(String runId, AgentRunStatus status, AgentHarnessResult result) {
		this(runId, null, status, result);
	}

	public RunAgentResult(String runId, String roomId, AgentRunStatus status, AgentHarnessResult result) {
		this.runId = runId;
		this.roomId = roomId;
		this.status = status;
		this.result = result;
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

	public AgentHarnessResult getResult() {
		return result;
	}

	public Map<String, Object> toMap() {
		Map<String, Object> map = new HashMap<>();
		map.put("runId", runId);
		map.put("roomId", roomId);
		map.put("status", status == null ? null : status.name());
		map.put("finalText", result == null ? null : result.getFinalText());
		map.put("inputMessageId", result == null ? null : result.getInputMessageId());
		map.put("finalOutputMessageId", result == null ? null : result.getFinalOutputMessageId());
		return map;
	}
}
