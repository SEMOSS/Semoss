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
package prerna.reactor.agent.transfer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.agent.config.AgentConfig;
import prerna.reactor.agent.config.SubAgentSpec;

/** Resolves and validates the agents participating in one room. */
public final class RoomAgentRoster {

	public static final String ROOM_OPTION_AGENTS = "agents";

	private RoomAgentRoster() {
	}

	/** A transfer target with a stable provider-safe tool name. */
	public record Target(String workspaceId, String name, String description, String toolName) {
	}

	/**
	 * Transfer is intentionally root-owner only for the POC. A transferred
	 * specialist can still use its own true subagents, but cannot transfer the room
	 * again.
	 */
	public static List<Target> transferTargets(Room room, AgentConfig currentAgent) {
		if (room == null || currentAgent == null) {
			return List.of();
		}
		String defaultAgentId = defaultAgentId(room.getOptionsMap());
		if (defaultAgentId == null || !defaultAgentId.equals(currentAgent.getWorkspaceId())) {
			return List.of();
		}

		List<String> ids = participantIds(room.getOptionsMap());
		Map<String, String> fallbackDescriptions = new LinkedHashMap<>();
		boolean hasRoomRoster = room.getOptionsMap() != null
				&& room.getOptionsMap().containsKey(ROOM_OPTION_AGENTS);
		if (!hasRoomRoster) {
			for (SubAgentSpec spec : currentAgent.getSubagents()) {
				ids.add(spec.getWorkspaceId());
				fallbackDescriptions.put(spec.getWorkspaceId(), spec.getDescription());
			}
		}

		User user = room.getInsight() == null ? null : room.getInsight().getUser();
		List<Target> targets = new ArrayList<>();
		Set<String> usedToolNames = new HashSet<>();
		for (String workspaceId : ids) {
			if (workspaceId.equals(defaultAgentId) || !isAvailable(user, workspaceId)) {
				continue;
			}
			Map<String, Object> workspace = ModelInferenceLogsUtils.getWorkspaceEntry(workspaceId);
			String name = stringValue(workspace == null ? null : workspace.get("name"));
			if (name == null) {
				name = workspaceId;
			}
			String description = stringValue(workspace == null ? null : workspace.get("description"));
			if (description == null) {
				description = fallbackDescriptions.get(workspaceId);
			}
			targets.add(new Target(workspaceId, name, description,
					uniqueToolName(name, workspaceId, usedToolNames)));
		}
		return targets;
	}

	/** Validate and normalize a client-authored room roster before persistence. */
	public static List<Map<String, Object>> normalize(User user, Object rawAgents, String defaultAgentId) {
		if (rawAgents == null) {
			return null;
		}
		if (!(rawAgents instanceof List<?> entries)) {
			throw new IllegalArgumentException("Room agents must be a list");
		}
		List<Map<String, Object>> normalized = new ArrayList<>();
		Set<String> seen = new HashSet<>();
		for (Object rawEntry : entries) {
			if (!(rawEntry instanceof Map<?, ?> entry)) {
				throw new IllegalArgumentException("Each room agent must contain a workspaceId");
			}
			String workspaceId = stringValue(entry.get("workspaceId"));
			if (workspaceId == null) {
				throw new IllegalArgumentException("Each room agent must contain a workspaceId");
			}
			if (workspaceId.equals(defaultAgentId)) {
				throw new IllegalArgumentException("The room's default agent cannot also be a transfer target");
			}
			if (!seen.add(workspaceId)) {
				throw new IllegalArgumentException("Duplicate room agent: " + workspaceId);
			}
			if (!isAvailable(user, workspaceId)) {
				throw new IllegalArgumentException(
						"Agent " + workspaceId + " does not exist, is inactive, or is not accessible");
			}
			normalized.add(Map.of("workspaceId", workspaceId));
		}
		return normalized;
	}

	public static String defaultAgentId(Map<String, Object> options) {
		if (options == null) {
			return null;
		}
		Object workspace = options.get("workspace");
		if (workspace instanceof String value) {
			return normalizeId(value);
		}
		if (workspace instanceof Map<?, ?> value) {
			return normalizeId(value.get("workspace_id"));
		}
		return null;
	}

	private static List<String> participantIds(Map<String, Object> options) {
		List<String> ids = new ArrayList<>();
		if (options == null || !(options.get(ROOM_OPTION_AGENTS) instanceof List<?> entries)) {
			return ids;
		}
		Set<String> seen = new HashSet<>();
		for (Object rawEntry : entries) {
			if (!(rawEntry instanceof Map<?, ?> entry)) {
				continue;
			}
			String id = normalizeId(entry.get("workspaceId"));
			if (id != null && seen.add(id)) {
				ids.add(id);
			}
		}
		return ids;
	}

	private static boolean isAvailable(User user, String workspaceId) {
		if (user == null || workspaceId == null || !SecurityProjectUtils.userCanViewProject(user, workspaceId)) {
			return false;
		}
		Map<String, Object> workspace = ModelInferenceLogsUtils.getWorkspaceEntry(workspaceId);
		return workspace != null && Boolean.TRUE.equals(workspace.get("is_active"));
	}

	private static String uniqueToolName(String name, String workspaceId, Set<String> used) {
		String slug = name.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_-]+", "_").replaceAll("_+", "_")
				.replaceAll("^[_-]+|[_-]+$", "");
		if (slug.isEmpty()) {
			slug = "agent";
		}
		String base = truncate("transfer_to_" + slug, 64);
		if (used.add(base)) {
			return base;
		}
		String suffix = "_" + Integer.toUnsignedString(workspaceId.hashCode(), 16);
		String candidate = truncate(base, 64 - suffix.length()) + suffix;
		used.add(candidate);
		return candidate;
	}

	private static String truncate(String value, int maxLength) {
		return value.length() <= maxLength ? value : value.substring(0, maxLength);
	}

	private static String normalizeId(Object value) {
		return value == null || String.valueOf(value).isBlank() ? null : String.valueOf(value).trim();
	}

	private static String stringValue(Object value) {
		return normalizeId(value);
	}
}
