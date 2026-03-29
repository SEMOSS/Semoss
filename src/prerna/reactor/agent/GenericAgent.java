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

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.om.Insight;
import prerna.util.AssetUtility;
import prerna.util.Utility;

/**
 * High-level orchestrator for the generic agent loop.
 *
 */
public final class GenericAgent {

    private static final Logger logger = LogManager.getLogger(GenericAgent.class);

    /** Key under which {@code filePath} is injected into {@code paramMap}. */
    public static final String FILE_PATH_PARAM_KEY = "file_path";

    /** Options-map keys checked (in order) when room.getModelId() is not set. */
    private static final String[] MODEL_ID_OPTION_KEYS = {"engine", "model", "modelId", "engineId"};

    private GenericAgent() { /* static utility */ }

    /**
     * Run the agent loop.
     *
     * @param roomId          Required. ROOM table ID that provides model, history, and tools.
     * @param input           Required. Initial user message.
     * @param engineIdFallback Optional. Engine/model ID to use if the room has no MODEL_ID set.
     * @param harnessType     Optional. Registry key for the harness; defaults to {@code "room_loop"}.
     * @param filePath        Optional. Working directory or project ID for file-system tools.
     * @param paramMap        Optional. Extra model parameters (temperature, max_tokens, etc.).
     * @param insight         Required. Current insight context (user, project, etc.).
     * @return Rich result containing final text, iteration count, and tool-call trace.
     * @throws Exception on unrecoverable errors during execution.
     */
    public static AgentHarnessResult run(
            String roomId,
            String input,
            String engineIdFallback,
            String harnessType,
            int maxReflections,
            Map<String, Object> paramMap,
            Insight insight
            ) throws Exception {

        if (roomId == null || roomId.trim().isEmpty()) {
            throw new IllegalArgumentException("roomId is required");
        }
        if (input == null || input.trim().isEmpty()) {
            throw new IllegalArgumentException("input is required");
        }

        Room room = RoomUtils.getOrLoadRoom(roomId, insight);

        String modelId = resolveModelId(room, engineIdFallback);
        if (modelId == null || modelId.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "No model engine found for room '" + roomId + "'. "
                    + "Set MODEL_ID on the room or pass engine= to the reactor.");
        }
        logger.debug("GenericAgent: room={} resolved modelId={}", roomId, modelId);

        IModelEngine modelEngine = Utility.getModel(modelId);
        if (modelEngine == null) {
            throw new IllegalArgumentException(
                    "Could not load model engine '" + modelId + "' for room '" + roomId + "'");
        }
        
        room.setModelId(modelId);

        insight.setRoomForInsight(room);

        String filePath = "";
        if (paramMap.containsKey("project")) {
        	String projectId = paramMap.remove("project").toString();
        	filePath = AssetUtility.getProjectAssetsFolder(projectId);
        	logger.info("Using project ID {} to set agent working directory..", projectId);
        } else if(paramMap.containsKey("filePath")){
        	filePath = paramMap.remove("filePath").toString();
        	insight.setInsightFolder(filePath.trim());
        } else {
            String roomFolderPath = room.getRoomFolderPath();
            File roomFolder = new File(roomFolderPath);
            if (!roomFolder.exists()) {
                roomFolder.mkdirs();
            }
            logger.info("GenericAgent: agentInsight folder set to room folder={}", roomFolderPath);
        }
        

        
        Map<String, Object> params = paramMap != null ? new HashMap<>(paramMap) : new HashMap<>();
        if (filePath != null && !filePath.trim().isEmpty()) {
            params.put(FILE_PATH_PARAM_KEY, filePath);
        }

        GenericAgentContext ctx = GenericAgentContext.builder()
                .room(room)
                .modelEngine(modelEngine)
                .insight(insight)
                .userId(room.getUserId())
                .filePath(filePath)
                .input(input)
                .paramMap(params)
                .maxReflections(maxReflections)
                .build();

        IAgentHarness harness = AgentHarnessRegistry.getOrDefault(harnessType);
        logger.info("GenericAgent: using harness '{}' for room={}", harness.getName(), roomId);
        return harness.execute(ctx);
    }

    /**
     * Resolves the model/engine ID using a three-tier priority:
     */
    @SuppressWarnings("unchecked")
    private static String resolveModelId(Room room, String fallback) {
        // Tier 1: direct column
        String modelId = room.getModelId();
        if (modelId != null && !modelId.trim().isEmpty()) {
            return modelId.trim();
        }

        // Tier 2: options map some older rooms stored it under "engine"
        Map<String, Object> opts = room.getOptionsMap();
        if (opts != null) {
            for (String key : MODEL_ID_OPTION_KEYS) {
                Object val = opts.get(key);
                if (val instanceof String && !((String) val).trim().isEmpty()) {
                    logger.info("GenericAgent: resolved modelId from options['{}']={}", key, val);
                    return ((String) val).trim();
                }
            }
        }

        // Tier 3: caller-supplied fallback (e.g. engine= param from reactor)
        if (fallback != null && !fallback.trim().isEmpty()) {
            logger.info("GenericAgent: using caller-supplied engine fallback={}", fallback);
            return fallback.trim();
        }

        return null;
    }
}
