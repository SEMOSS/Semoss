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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.json.JSONObject;

/**
 * Immutable websocket envelope for GitHub Copilot live events.
 */
public final class GitHubCopilotLiveEvent {

	private final String roomId;
	private final String runId;
	private final long sequence;
	private final String event;
	private final String timestamp;
	private final boolean ephemeral;
	private final Map<String, Object> data;

	public GitHubCopilotLiveEvent(String roomId, String runId, long sequence, String event, String timestamp,
			boolean ephemeral, Map<String, Object> data) {
		this.roomId = roomId;
		this.runId = runId;
		this.sequence = sequence;
		this.event = event;
		this.timestamp = timestamp;
		this.ephemeral = ephemeral;
		this.data = data == null ? Collections.emptyMap()
				: Collections.unmodifiableMap(new LinkedHashMap<>(data));
	}

	public String getRoomId() {
		return roomId;
	}

	public String getRunId() {
		return runId;
	}

	public long getSequence() {
		return sequence;
	}

	public String getEvent() {
		return event;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public boolean isEphemeral() {
		return ephemeral;
	}

	public Map<String, Object> getData() {
		return data;
	}

	public JSONObject toJson() {
		JSONObject payload = new JSONObject();
		payload.put("type", "github_copilot");
		payload.put("roomId", roomId);
		payload.put("runId", runId);
		payload.put("sequence", sequence);
		payload.put("event", event);
		payload.put("timestamp", timestamp);
		payload.put("ephemeral", ephemeral);
		payload.put("data", new JSONObject(data));
		return payload;
	}
}
