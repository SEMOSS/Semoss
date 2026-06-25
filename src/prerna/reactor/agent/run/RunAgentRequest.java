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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.om.Insight;
import prerna.reactor.agent.AgentRunContext;
import prerna.reactor.agent.AgentRunner;

public final class RunAgentRequest {

	private final String roomId;
	private final String input;
	private final String engineIdFallback;
	private final String harnessType;
	private final String workspaceId;
	private final int maxTurns;
	private final int maxReflections;
	private final Map<String, Object> paramMap;
	private final Map<String, Object> agentParamMap;
	private final List<String> mediaInputPaths;
	private final List<String> mediaUrls;
	private final Insight insight;

	public RunAgentRequest(String roomId, String input, String engineIdFallback, String harnessType,
			String workspaceId, int maxTurns, int maxReflections, Map<String, Object> paramMap,
			Map<String, Object> agentParamMap, List<String> mediaInputPaths, List<String> mediaUrls, Insight insight) {
		this.roomId = roomId;
		this.input = input;
		this.engineIdFallback = engineIdFallback;
		this.harnessType = harnessType;
		this.workspaceId = workspaceId;
		this.maxTurns = maxTurns;
		this.maxReflections = maxReflections;
		this.paramMap = paramMap != null ? new HashMap<>(paramMap) : new HashMap<>();
		this.agentParamMap = agentParamMap != null ? new HashMap<>(agentParamMap) : new HashMap<>();
		this.mediaInputPaths = immutableStringList(mediaInputPaths);
		this.mediaUrls = immutableStringList(mediaUrls);
		if (workspaceId != null && !workspaceId.trim().isEmpty()) {
			this.paramMap.put(AgentRunner.PARAM_WORKSPACE_ID, workspaceId);
		}
		this.insight = insight;
	}

	public RunAgentRequest(String roomId, String input, String engineIdFallback, String harnessType,
			String workspaceId, int maxTurns, int maxReflections, Map<String, Object> paramMap,
			Map<String, Object> agentParamMap, Insight insight) {
		this(roomId, input, engineIdFallback, harnessType, workspaceId, maxTurns, maxReflections, paramMap,
				agentParamMap, null, null, insight);
	}

	public String getRoomId() {
		return roomId;
	}

	public String getInput() {
		return input;
	}

	public String getEngineIdFallback() {
		return engineIdFallback;
	}

	public String getHarnessType() {
		return harnessType;
	}

	public String getWorkspaceId() {
		return workspaceId;
	}

	public int getMaxTurns() {
		return maxTurns;
	}

	public int getMaxReflections() {
		return maxReflections;
	}

	public Map<String, Object> getParamMap() {
		return new HashMap<>(paramMap);
	}

	public Map<String, Object> getAgentParamMap() {
		return new HashMap<>(agentParamMap);
	}

	public List<String> getMediaInputPaths() {
		return mediaInputPaths;
	}

	public List<String> getMediaUrls() {
		return mediaUrls;
	}

	public Insight getInsight() {
		return insight;
	}

	public Map<String, Object> toPersistedMap() {
		Map<String, Object> map = new HashMap<>();
		map.put("roomId", roomId);
		map.put("input", input);
		map.put("engineIdFallback", engineIdFallback);
		map.put("harnessType", harnessType);
		map.put("workspaceId", workspaceId);
		map.put("maxTurns", maxTurns);
		map.put("maxReflections", maxReflections);
		map.put("paramMap", getParamMap());
		map.put("agentParamMap", getAgentParamMap());
		map.put("mediaInputPaths", getMediaInputPaths());
		map.put("mediaUrls", getMediaUrls());
		return map;
	}

	@SuppressWarnings("unchecked")
	public static RunAgentRequest fromPersistedMap(Map<String, Object> map, Insight insight) {
		if (map == null) {
			return null;
		}
		return new RunAgentRequest(
				stringValue(map.get("roomId")),
				stringValue(map.get("input")),
				stringValue(map.get("engineIdFallback")),
				stringValue(map.get("harnessType")),
				stringValue(map.get("workspaceId")),
				intValue(map.get("maxTurns"), AgentRunContext.DEFAULT_MAX_TURNS),
				intValue(map.get("maxReflections"), AgentRunContext.DEFAULT_MAX_REFLECTIONS),
				map.get("paramMap") instanceof Map ? (Map<String, Object>) map.get("paramMap") : null,
				map.get("agentParamMap") instanceof Map ? (Map<String, Object>) map.get("agentParamMap") : null,
				listValue(map.get("mediaInputPaths")),
				listValue(map.get("mediaUrls")),
				insight);
	}

	private static List<String> immutableStringList(List<String> values) {
		if (values == null || values.isEmpty()) {
			return Collections.emptyList();
		}
		List<String> copy = new ArrayList<>();
		for (String value : values) {
			if (value != null && !value.trim().isEmpty()) {
				copy.add(value);
			}
		}
		return copy.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(copy);
	}

	private static List<String> listValue(Object value) {
		if (!(value instanceof List<?>)) {
			return Collections.emptyList();
		}
		List<String> result = new ArrayList<>();
		for (Object item : (List<?>) value) {
			if (item != null) {
				String str = String.valueOf(item);
				if (!str.trim().isEmpty()) {
					result.add(str);
				}
			}
		}
		return result;
	}

	private static String stringValue(Object value) {
		if (value == null) {
			return null;
		}
		String str = String.valueOf(value);
		return str.trim().isEmpty() ? null : str;
	}

	private static int intValue(Object value, int defaultValue) {
		if (value instanceof Number) {
			return ((Number) value).intValue();
		}
		if (value != null) {
			try {
				return Integer.parseInt(String.valueOf(value));
			} catch (NumberFormatException ignored) {
				return defaultValue;
			}
		}
		return defaultValue;
	}
}
