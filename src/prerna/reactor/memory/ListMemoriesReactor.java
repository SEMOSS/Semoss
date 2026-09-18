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
package prerna.reactor.memory;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.MemoryUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Lists memories visible to the current user: memories they own, or (when
 * workspaceId is supplied) memories shared to that workspace, with optional
 * eventType (multi-value) and free-text search filters. The response is
 * paginated: {@code {memories, total_count, has_more}}.
 */
public class ListMemoriesReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ListMemoriesReactor.class);

	public ListMemoriesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.WORKSPACE_ID.getKey(), ReactorKeysEnum.ROOM_ID.getKey(),
				ReactorKeysEnum.AGENT_ID.getKey(), ReactorKeysEnum.MEMORY_PROJECT_ID.getKey(),
				ReactorKeysEnum.EVENT_TYPE.getKey(), ReactorKeysEnum.SEARCH.getKey(),
				ReactorKeysEnum.META_FILTERS.getKey(), ReactorKeysEnum.INCLUDE_SUPERSEDED.getKey(),
				ReactorKeysEnum.EMBEDDING_ENGINE_ID.getKey(), ReactorKeysEnum.LIMIT.getKey(),
				ReactorKeysEnum.OFFSET.getKey() };
		this.keyRequired = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public String getReactorDescription() {
		return """
				Lists memories the current user can see: memories they own, or (when \
				workspaceId is supplied and the user has view access to that workspace) \
				memories shared to that workspace. Results are capped and ranked, not a \
				full dump: pass search with a specific question/topic to get a small, \
				semantically-ranked set of the most relevant memories (uses the same \
				vector engine memories are deduped against - not just a literal keyword \
				match), rather than calling with no search term and relying on recency \
				order alone. IMPORTANT: leave roomId empty when trying to recall what \
				the user has told you in past conversations - an omitted roomId searches \
				across every room/conversation you've had with this same agent, which is \
				what "do you remember..." questions need. Only pass roomId when you \
				specifically want to restrict to the current conversation. When agentId \
				is also omitted and the call runs inside a room, agentId defaults to \
				that room's attached agent (see SetRoomWorkspaceReactor - not the room's \
				own project, which may be a different app the agent is embedded in), so \
				an agent naturally recalls everything the caller has personally told it \
				across every room/app they've used it from - this never bypasses \
				per-user ownership, unlike workspaceId's deliberate cross-user sharing. \
				Memories already folded into a compacted summary are hidden by default \
				(includeSuperseded=true to see them anyway). Supports filtering by \
				project and event type, with paginated results.\
				""";
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}
		String userId = user.getPrimaryLoginToken().getId();

		String workspaceId = this.keyValue.get(ReactorKeysEnum.WORKSPACE_ID.getKey());
		if (workspaceId != null && !workspaceId.isBlank()
				&& !SecurityProjectUtils.userCanViewProject(user, workspaceId)) {
			throw new IllegalArgumentException(
					"Workspace " + workspaceId + " does not exist or user does not have access to the workspace");
		}
		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		String agentId = this.keyValue.get(ReactorKeysEnum.AGENT_ID.getKey());
		if ((agentId == null || agentId.isBlank()) && (workspaceId == null || workspaceId.isBlank())
				&& this.insight.getRoomId() != null && !this.insight.getRoomId().isBlank()) {
			// The agent/persona actually in effect for the current room right now -
			// lets an agent recall everything the caller has told it, regardless of
			// which room/app it was said in, without requiring the LLM to know or
			// pass the agent's id itself. See
			// ModelInferenceLogsUtils#resolveEffectiveWorkspaceId for why this isn't
			// just the persisted WORKSPACE_ID column (in-flight RunAgent overrides
			// and inherited sub-agent personas live only in the room's live options,
			// not that column). Best-effort: a bad/stale roomId should never block
			// listing memories.
			try {
				Room room = RoomUtils.getOrLoadRoom(this.insight.getRoomId(), this.insight);
				agentId = ModelInferenceLogsUtils.resolveEffectiveWorkspaceId(room, userId);
			} catch (Exception e) {
				classLogger.warn(
						"Failed to resolve live agent id for room '{}'; falling back to the persisted workspace column.",
						this.insight.getRoomId(), e);
				agentId = ModelInferenceLogsUtils.getRoomWorkspaceId(this.insight.getRoomId(), userId);
			}
		}
		String projectId = this.keyValue.get(ReactorKeysEnum.MEMORY_PROJECT_ID.getKey());
		List<String> eventTypes = getListStringFromKeyOrCurRow(ReactorKeysEnum.EVENT_TYPE.getKey());
		String search = this.keyValue.get(ReactorKeysEnum.SEARCH.getKey());
		Map<String, Object> metaFilters = getMapFromKeyOrCurRow(ReactorKeysEnum.META_FILTERS.getKey());
		Boolean includeSuperseded = parseBooleanOrNull(this.keyValue.get(ReactorKeysEnum.INCLUDE_SUPERSEDED.getKey()));
		String vectorEngineId = MemoryUtils
				.resolveVectorEngineId(this.keyValue.get(ReactorKeysEnum.EMBEDDING_ENGINE_ID.getKey()), userId);
		Integer limit = parseIntOrNull(this.keyValue.get(ReactorKeysEnum.LIMIT.getKey()));
		Integer offset = parseIntOrNull(this.keyValue.get(ReactorKeysEnum.OFFSET.getKey()));

		Map<String, Object> result = MemoryUtils.listMemories(userId, workspaceId, roomId, agentId, eventTypes, search,
				metaFilters, projectId, includeSuperseded, vectorEngineId, this.insight, limit, offset);
		return new NounMetadata(result, PixelDataType.MAP);
	}

	private Integer parseIntOrNull(String value) {
		if (value == null || value.isBlank()) {
			return null;
		}
		try {
			return Integer.parseInt(value);
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Invalid numeric value: " + value);
		}
	}

	private Boolean parseBooleanOrNull(String value) {
		if (value == null || value.isBlank()) {
			return null;
		}
		return Boolean.parseBoolean(value);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.WORKSPACE_ID.getKey())) {
			return "Optional workspace to list shared memories from";
		}
		if (key.equals(ReactorKeysEnum.ROOM_ID.getKey())) {
			return "Optional room to restrict results to. Leave this empty to recall memories across every room/conversation with this agent - only set it to narrow to just the current conversation.";
		}
		if (key.equals(ReactorKeysEnum.AGENT_ID.getKey())) {
			return "Optional agent/workspace id to restrict results to (defaults to the current room's attached agent, i.e. everything the caller has told that agent across every room)";
		}
		if (key.equals(ReactorKeysEnum.MEMORY_PROJECT_ID.getKey())) {
			return "Optional project/app id to restrict results to (informational: which app the memory was captured in, separate from which agent was attached)";
		}
		if (key.equals(ReactorKeysEnum.EVENT_TYPE.getKey())) {
			return "Optional event type(s) to filter by (single value or list)";
		}
		if (key.equals(ReactorKeysEnum.SEARCH.getKey())) {
			return "A specific question or topic to semantically rank memories against - not a keyword filter. Prefer this over leaving it blank when you're recalling something specific, so you get a small set of the most relevant memories instead of a plain recency-ordered dump.";
		}
		if (key.equals(ReactorKeysEnum.META_FILTERS.getKey())) {
			return "Optional map of custom metakey -> value(s) to filter memories by (e.g. {\"topic\": \"billing\"})";
		}
		if (key.equals(ReactorKeysEnum.INCLUDE_SUPERSEDED.getKey())) {
			return "Whether to include memories already folded into a CompactMemories summary (default false)";
		}
		if (key.equals(ReactorKeysEnum.EMBEDDING_ENGINE_ID.getKey())) {
			return "Vector engine id used to semantically rank the search term (defaults to the platform/user default vector engine)";
		}
		return super.getDescriptionForKey(key);
	}

}
