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

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.om.Insight;
import prerna.util.Utility;

/**
 * High-level orchestrator for the generic agent loop.
 *
 * <p>Accepts minimal inputs, resolves the Room and model engine, selects a harness from
 * {@link AgentHarnessRegistry}, and delegates execution.
 *
 * <h3>Resolution steps</h3>
 * <ol>
 *   <li>Load {@link Room} via {@link RoomUtils#getOrLoadRoom(String, Insight)}
 *   <li>Resolve {@link IModelEngine} via {@code Utility.getModel(room.getModelId())}
 *   <li>Determine harness key: {@code harnessType} if provided, otherwise {@code "room_loop"}
 *   <li>Add {@code filePath} to {@code paramMap} under key {@code "file_path"} if non-null
 *   <li>Build {@link GenericAgentContext} and call {@link IAgentHarness#execute(GenericAgentContext)}
 * </ol>
 */
public final class GenericAgent {

    private static final Logger logger = LogManager.getLogger(GenericAgent.class);

    /** Key under which {@code filePath} is injected into {@code paramMap}. */
    public static final String FILE_PATH_PARAM_KEY = "file_path";

    private GenericAgent() { /* static utility */ }

    /**
     * Run the agent loop.
     *
     * @param roomId      Required. ROOM table ID that provides model, history, and tools.
     * @param input       Required. Initial user message.
     * @param harnessType Optional. Registry key for the harness; defaults to {@code "room_loop"}.
     * @param filePath    Optional. Working directory or project ID for file-system tools.
     * @param paramMap    Optional. Extra model parameters (temperature, max_tokens, etc.).
     * @param insight     Required. Current insight context (user, project, etc.).
     * @return Rich result containing final text, iteration count, and tool-call trace.
     * @throws Exception on unrecoverable errors during execution.
     */
    public static AgentHarnessResult run(
            String roomId,
            String input,
            String harnessType,
            String filePath,
            Map<String, Object> paramMap,
            Insight insight) throws Exception {

        if (roomId == null || roomId.trim().isEmpty()) {
            throw new IllegalArgumentException("roomId is required");
        }
        if (input == null || input.trim().isEmpty()) {
            throw new IllegalArgumentException("input is required");
        }

        // ── 1. Load Room ──────────────────────────────────────────────────────
        Room room = RoomUtils.getOrLoadRoom(roomId, insight);
        logger.info("GenericAgent: loaded room={} modelId={}", roomId, room.getModelId());

        // ── 2. Resolve model engine ───────────────────────────────────────────
        String modelId = room.getModelId();
        if (modelId == null || modelId.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Room '" + roomId + "' does not have a model engine configured");
        }
        IModelEngine modelEngine = Utility.getModel(modelId);
        if (modelEngine == null) {
            throw new IllegalArgumentException(
                    "Could not load model engine '" + modelId + "' for room '" + roomId + "'");
        }

        // ── 3. Build paramMap copy ────────────────────────────────────────────
        Map<String, Object> params = paramMap != null ? new HashMap<>(paramMap) : new HashMap<>();
        if (filePath != null && !filePath.trim().isEmpty()) {
            params.put(FILE_PATH_PARAM_KEY, filePath);
        }

        // ── 4. Build context ──────────────────────────────────────────────────
        GenericAgentContext ctx = GenericAgentContext.builder()
                .room(room)
                .modelEngine(modelEngine)
                .insight(insight)
                .userId(room.getUserId())
                .filePath(filePath)
                .input(input)
                .paramMap(params)
                .build();

        // ── 5. Select harness and execute ─────────────────────────────────────
        IAgentHarness harness = AgentHarnessRegistry.getOrDefault(harnessType);
        logger.info("GenericAgent: using harness '{}' for room={}", harness.getName(), roomId);
        return harness.execute(ctx);
    }
}
