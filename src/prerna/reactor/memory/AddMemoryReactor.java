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

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.MemoryUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.inferencetracking.workers.MemoryClassificationWorker;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Adds a new memory owned by the current user.
 *
 * <p>
 * When {@code eventType} is omitted and {@code reasoningEngineId} is supplied,
 * the memory is inserted immediately with a default "memory" type and
 * classified in a background thread afterward (see
 * {@link MemoryClassificationWorker}), so the reasoning-model call never blocks
 * the reactor's return. Vector-based dedup stays synchronous - it has to run
 * before we decide whether to insert at all.
 *
 * <p>
 * {@code roomId} defaults to the calling insight's current room (see
 * {@code Insight#getRoomId()}, populated automatically for every agent tool
 * call) when not explicitly supplied, so a memory always records where it was
 * captured even if the LLM never thinks to pass it.
 *
 * <p>
 * {@code agentId} auto-defaults to the current room's attached agent/persona
 * ({@code ROOM.WORKSPACE_ID}, see {@code SetRoomWorkspaceReactor}) - not the
 * room's own project (which may be a different app the agent is embedded in)
 * and not the underlying LLM model engine. This lets a memory be recalled later
 * by "the same agent" regardless of which room/app it was captured in, while
 * remaining strictly personal: it never bypasses the {@code USER_ID} ownership
 * check the way {@code workspaceId} (deliberate, cross-user sharing via
 * {@link prerna.reactor.memory.PromoteMemoryToWorkspaceReactor}) does.
 *
 * <p>
 * When a vector engine is available (defaults to
 * {@link MemoryUtils#DEFAULT_VECTOR_ENGINE_ID}, a FAISS index), the content is
 * embedded and searched against the caller's existing memories in the same
 * scope via real nearest-neighbor search; if a close-enough duplicate is found,
 * its id is returned instead of inserting a new row (mirrors the memory_mcp
 * app's dedup behavior, now backed by a real vector engine instead of a JSON
 * blob + brute-force comparison).
 */
public class AddMemoryReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AddMemoryReactor.class);

	public AddMemoryReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.CONTENT.getKey(), ReactorKeysEnum.EVENT_TYPE.getKey(),
				ReactorKeysEnum.ROOM_ID.getKey(), ReactorKeysEnum.WORKSPACE_ID.getKey(),
				ReactorKeysEnum.METADATA.getKey(), ReactorKeysEnum.EMBEDDING_ENGINE_ID.getKey(),
				ReactorKeysEnum.REASONING_ENGINE_ID.getKey(), ReactorKeysEnum.META_FILTERS.getKey() };
		this.keyRequired = new int[] { 1, 0, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}
		String userId = user.getPrimaryLoginToken().getId();

		String content = this.keyValue.get(ReactorKeysEnum.CONTENT.getKey());
		String eventType = this.keyValue.get(ReactorKeysEnum.EVENT_TYPE.getKey());
		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		if (roomId == null || roomId.isBlank()) {
			// Every agent tool call runs on a room-bound insight (see
			// RunMCPToolReactor/AgentToolDecisionHandler#setRoomForInsight), so this
			// captures the room even when the LLM's tool call omits it.
			roomId = this.insight.getRoomId();
		}
		String workspaceId = this.keyValue.get(ReactorKeysEnum.WORKSPACE_ID.getKey());
		if (workspaceId != null && !workspaceId.isBlank()
				&& !SecurityProjectUtils.userCanViewProject(user, workspaceId)) {
			throw new IllegalArgumentException(
					"Workspace " + workspaceId + " does not exist or user does not have access to the workspace");
		}
		String projectId = this.insight.getContextProjectId();
		Map<String, Object> metadata = getMapFromKeyOrCurRow(ReactorKeysEnum.METADATA.getKey());
		Map<String, Object> metaFilters = getMapFromKeyOrCurRow(ReactorKeysEnum.META_FILTERS.getKey());

		String agentId = null;
		if (roomId != null && !roomId.isBlank()) {
			// The agent/persona actually in effect for this room right now -
			// independent of PROJECT_ID (the app/project the room lives under,
			// which may be a different project entirely if this agent is embedded
			// inside another app) and not just the persisted WORKSPACE_ID column,
			// which an in-flight RunAgent(workspaceId=...) override or an inherited
			// sub-agent persona would not be reflected in - see
			// ModelInferenceLogsUtils#resolveEffectiveWorkspaceId. Best-effort: a
			// bad/stale roomId should never block capturing the memory itself.
			try {
				Room room = RoomUtils.getOrLoadRoom(roomId, this.insight);
				agentId = ModelInferenceLogsUtils.resolveEffectiveWorkspaceId(room, userId);
			} catch (Exception e) {
				classLogger.warn(
						"Failed to resolve live agent id for room '{}'; falling back to the persisted workspace column.",
						roomId, e);
				agentId = ModelInferenceLogsUtils.getRoomWorkspaceId(roomId, userId);
			}
		}

		String reasoningEngineId = this.keyValue.get(ReactorKeysEnum.REASONING_ENGINE_ID.getKey());
		boolean needsAsyncClassification = (eventType == null || eventType.isBlank()) && reasoningEngineId != null
				&& !reasoningEngineId.isBlank();
		if (eventType == null || eventType.isBlank()) {
			// Insert with the default type now; classification (if requested) is
			// backfilled asynchronously below so it never blocks this call.
			eventType = "memory";
		}

		String vectorEngineId = MemoryUtils
				.resolveVectorEngineId(this.keyValue.get(ReactorKeysEnum.EMBEDDING_ENGINE_ID.getKey()), userId);

		Map.Entry<String, Double> duplicate = MemoryUtils.findDuplicateMemoryViaVector(vectorEngineId, this.insight,
				userId, workspaceId, agentId, eventType, content);
		if (duplicate != null) {
			// A near-duplicate already exists in scope - return it rather than inserting
			// a redundant row. No classification/indexing needed since we're not
			// creating a new row.
			return new NounMetadata(duplicate.getKey(), PixelDataType.CONST_STRING);
		}

		String memoryId = MemoryUtils.addMemory(userId, content, eventType, roomId, workspaceId, projectId, agentId,
				metadata, null, metaFilters);
		MemoryUtils.indexMemoryEmbedding(vectorEngineId, this.insight, memoryId, content);

		if (needsAsyncClassification) {
			Thread classifier = new Thread(
					new MemoryClassificationWorker(memoryId, reasoningEngineId, this.insight, content));
			classifier.setDaemon(true);
			classifier.start();
		}

		return new NounMetadata(memoryId, PixelDataType.CONST_STRING);
	}

	@Override
	public String getReactorDescription() {
		return """
				Stores a new memory for the current user. When eventType is omitted and \
				reasoningEngineId is supplied, the memory is classified in the background \
				after being saved. roomId defaults to the current room when omitted. When a \
				vector engine is available (defaults to the platform default FAISS index), \
				near-duplicate content in the same scope is detected via nearest-neighbor \
				search and the existing memory's id is returned instead of inserting a new \
				row.\
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.CONTENT.getKey())) {
			return "The memory text to store";
		}
		if (key.equals(ReactorKeysEnum.EVENT_TYPE.getKey())) {
			return "Memory event type, e.g. memory/decision/lesson/error/task (auto-classified in the background via reasoningEngineId when omitted, else defaults to \"memory\")";
		}
		if (key.equals(ReactorKeysEnum.ROOM_ID.getKey())) {
			return "Optional room/conversation this memory was captured in (defaults to the current room)";
		}
		if (key.equals(ReactorKeysEnum.WORKSPACE_ID.getKey())) {
			return "Optional workspace to share this memory with";
		}
		if (key.equals(ReactorKeysEnum.EMBEDDING_ENGINE_ID.getKey())) {
			return "Vector engine id used for duplicate detection (defaults to the platform default vector engine)";
		}
		if (key.equals(ReactorKeysEnum.REASONING_ENGINE_ID.getKey())) {
			return "Engine id used to auto-classify eventType in the background when omitted (skipped if not supplied)";
		}
		if (key.equals(ReactorKeysEnum.META_FILTERS.getKey())) {
			return "Optional map of custom metakey -> value(s) to attach to this memory as filterable metadata (e.g. {\"topic\": \"billing\"})";
		}
		return super.getDescriptionForKey(key);
	}

}
