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

/**
 * The receipt for a submitted agent run: its id, its room, and the status at
 * submission time.
 *
 * <p>
 * Submission is asynchronous, so this carries no output. The run executes on a
 * background worker and its result is read from the durable {@code AGENT_RUN}
 * record through {@link AgentRunService#getRun} or
 * {@link AgentRunService#waitForRun}. Use the {@code runId} here to make those
 * calls.
 *
 * @param runId  durable id of the submitted run
 * @param roomId room the run was submitted to
 * @param status status at submission
 */
public record AgentRunHandle(String runId, String roomId, AgentRunStatus status) {

	/** Null-tolerant, so the map allows null {@code roomId} and {@code status}. */
	public Map<String, Object> toMap() {
		Map<String, Object> map = new HashMap<>();
		map.put("runId", runId);
		map.put("roomId", roomId);
		map.put("status", status == null ? null : status.name());
		return map;
	}
}
