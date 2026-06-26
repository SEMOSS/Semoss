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

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.exceptions.AgentMaxTurnsException;
import prerna.reactor.agent.run.AgentRuntimeManager;
import prerna.reactor.agent.run.RunAgentRequest;
import prerna.reactor.agent.run.RunAgentResult;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/** Starts a durable generic agent run and waits by default unless wait=false is passed. */
public class RunAgentReactor extends AbstractReactor {

    private static final Logger logger = LogManager.getLogger(RunAgentReactor.class);

    private static final String HARNESS_TYPE_KEY    = "harnessType";
    private static final String WORKSPACE_ID_KEY    = "workspaceId";
    private static final String MAX_TURNS_KEY       = "maxTurns";
    private static final String MAX_ITERATIONS_KEY  = "maxIterations";
    private static final String MAX_REFLECTIONS_KEY = "maxReflections";
    private static final String WAIT_KEY            = "wait";
    private static final String WAIT_TIMEOUT_MS_KEY = "waitTimeoutMs";

    public RunAgentReactor() {
        this.keysToGet = new String[] {
                ReactorKeysEnum.ROOM_ID.getKey(),
                ReactorKeysEnum.COMMAND.getKey(),
                ReactorKeysEnum.ENGINE.getKey(),
                HARNESS_TYPE_KEY,
                WORKSPACE_ID_KEY,
                MAX_TURNS_KEY,
                MAX_ITERATIONS_KEY,
                MAX_REFLECTIONS_KEY,
                WAIT_KEY,
                WAIT_TIMEOUT_MS_KEY,
                ReactorKeysEnum.PARAM_VALUES_MAP.getKey(),
                ReactorKeysEnum.AGENT_PARAMS.getKey(),
                ReactorKeysEnum.IMAGE.getKey(),
                ReactorKeysEnum.URL.getKey(),
        };
        this.keyRequired = new int[] { 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();

        String roomId           = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
        String input            = this.keyValue.get(ReactorKeysEnum.COMMAND.getKey());
        String engineIdFallback = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
        String harnessType      = this.keyValue.get(HARNESS_TYPE_KEY);
       

        // FE sends `command` URL-encoded (spaces as %20, etc.). Decode before
        // forwarding to the harness so the prompt reaches the model intact.
        if (input != null && input.indexOf('%') >= 0) {
            try {
                input = URLDecoder.decode(input, StandardCharsets.UTF_8);
            } catch (IllegalArgumentException e) {
                logger.warn("RunAgentReactor: command failed URL-decode, passing through raw: {}", e.getMessage());
            }
        }
        // workspaceId overrides room.options.workspace.workspace_id for this run.
        String explicitWorkspaceId = StringUtils.trimToNull(this.keyValue.get(WORKSPACE_ID_KEY));

        int maxTurns = parseIntAtLeast(
                StringUtils.firstNonBlank(
                        this.keyValue.get(MAX_TURNS_KEY),
                        this.keyValue.get(MAX_ITERATIONS_KEY)),
                AgentRunContext.DEFAULT_MAX_TURNS, 1);
        int maxReflections = parseIntAtLeast(
                this.keyValue.get(MAX_REFLECTIONS_KEY),
                AgentRunContext.DEFAULT_MAX_REFLECTIONS, 0);
        boolean wait = parseBoolean(this.keyValue.get(WAIT_KEY), true);
        long waitTimeoutMs = parseLongAtLeast(this.keyValue.get(WAIT_TIMEOUT_MS_KEY), 0L, 0L);
        Map<String, Object> paramMap = getMap("paramMap");
        Map<String, Object> agentParams = getMap("agentParams");
        List<String> inputImages = getListString(ReactorKeysEnum.IMAGE.getKey());
        List<String> inputImageURLs = getListString(ReactorKeysEnum.URL.getKey());

        if (roomId == null || roomId.trim().isEmpty()) {
            throw new IllegalArgumentException("roomId is required for RunAgent");
        }
        if (input == null || input.trim().isEmpty()) {
            throw new IllegalArgumentException("command (input) is required for RunAgent");
        }

        logger.info("RunAgentReactor: roomId={} engineFallback={} harnessType={} workspaceId={} maxTurns={} maxReflections={} wait={} waitTimeoutMs={} images={} urls={}",
                roomId, engineIdFallback, harnessType, explicitWorkspaceId, maxTurns, maxReflections, wait,
                waitTimeoutMs, sizeOf(inputImages), sizeOf(inputImageURLs));

        try {
            validateMediaSupported(harnessType, inputImages, inputImageURLs);
            List<String> copiedImages = stageMediaInputs(roomId, input, engineIdFallback, inputImages);
            RunAgentRequest request = new RunAgentRequest(
                    roomId,
                    input,
                    engineIdFallback,
                    harnessType,
                    explicitWorkspaceId,
                    maxTurns,
                    maxReflections,
                    paramMap,
                    agentParams,
                    copiedImages,
                    inputImageURLs,
                    this.insight);
            RunAgentResult handle = AgentRuntimeManager.get().run(request);
            Map<String, Object> output = handle.toMap();
            if (wait) {
                output = AgentRuntimeManager.get().waitForRun(handle.getRunId(), this.insight, waitTimeoutMs);
            }
            AgentHarnessResult result = handle.getResult();

            if (result != null) {
                logger.info("RunAgentReactor: completed runId={} iterations={} reflections={} tools={}",
                        handle.getRunId(), result.getIterations(), result.getReflectionsUsed(),
                        result.getToolCallRecords().size());
            } else {
                logger.info("RunAgentReactor: runId={} status={}", handle.getRunId(), handle.getStatus());
            }

            return new NounMetadata(output, PixelDataType.MAP,
                    PixelOperationType.OPERATION);

        } catch (AgentMaxTurnsException e) {
            throw new IllegalStateException(e.getMessage(), e);
        } catch (Exception e) {
            logger.error("RunAgentReactor: error running agent loop", e);
            throw new IllegalStateException("Agent execution failed: " + e.getMessage(), e);
        }
    }

    @Override
    public String getReactorDescription() {
        return "Start a durable generic agent loop using a pluggable harness. By default RunAgent waits for terminal state; pass wait=false for async submission. maxTurns applies to the SEMOSS harness tool loop; maxReflections controls optional self-critique rounds.";
    }

    // Helpers

    private List<String> stageMediaInputs(String roomId, String input, String engineIdFallback, List<String> inputImages) {
        if (inputImages == null || inputImages.isEmpty()) {
            return inputImages;
        }

        IModelEngine modelEngine = null;
        String runtimeModelId = engineIdFallback != null ? engineIdFallback.trim() : null;
        if (runtimeModelId != null && !runtimeModelId.isEmpty()) {
            modelEngine = Utility.getModel(runtimeModelId);
            if (modelEngine == null) {
                throw new IllegalArgumentException(
                        "Could not load model engine '" + runtimeModelId + "' for room '" + roomId + "'");
            }
        }

        Room room = modelEngine != null ? RoomUtils.createRoomIfNotExists(roomId, insight, modelEngine, input)
                : RoomUtils.getOrLoadRoom(roomId, insight);
        return RoomUtils.copyFilesToRoomFolder(inputImages, room, insight);
    }

    private void validateMediaSupported(String harnessType, List<String> inputImages, List<String> inputImageURLs) {
        if (sizeOf(inputImages) == 0 && sizeOf(inputImageURLs) == 0) {
            return;
        }
        IAgentHarness harness = AgentHarnessRegistry.getOrDefault(harnessType);
        if (!harness.supportsMediaInput()) {
            throw new IllegalArgumentException("RunAgent media input is not supported for harnessType='"
                    + harness.getName() + "'");
        }
    }

    @SuppressWarnings("unchecked")
	protected Map<String, Object> getMap(String identifier) {
    	String key = ReactorKeysEnum.PARAM_VALUES_MAP.getKey();
    	if ("agentParams".equals(identifier)){
    			key = ReactorKeysEnum.AGENT_PARAMS.getKey();
    	}
    	;
        GenRowStruct mapGrs = this.store.getGenRowStruct(key);
        if (mapGrs != null && !mapGrs.isEmpty()) {
            List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
            if (mapInputs != null && !mapInputs.isEmpty()) {
                return (Map<String, Object>) mapInputs.get(0).getValue();
            }
        }
        // Support the list-wrapped form: agentParams=[{...}]. The `[ ]` brackets make a
        // VECTOR noun (see VectorReactor); unwrap it and return the first map inside.
        if (mapGrs != null && !mapGrs.isEmpty()) {
            for (NounMetadata vecNoun : mapGrs.getNounsOfType(PixelDataType.VECTOR)) {
                Object vecVal = vecNoun.getValue();
                if (vecVal instanceof List) {
                    for (Object el : (List<?>) vecVal) {
                        Object inner = (el instanceof NounMetadata) ? ((NounMetadata) el).getValue() : el;
                        if (inner instanceof Map) {
                            return (Map<String, Object>) inner;
                        }
                    }
                }
            }
        }
        List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
        if (mapInputs != null && !mapInputs.isEmpty()) {
            return (Map<String, Object>) mapInputs.get(0).getValue();
        }
        return new HashMap<>();
    }
    
    /**
     * Parse {@code value} as an int, falling back to {@code defaultValue} when null,
     * blank, non-numeric, or below {@code minInclusive}.
     */
    private static int parseIntAtLeast(String value, int defaultValue, int minInclusive) {
        if (value == null || value.trim().isEmpty()) return defaultValue;
        try {
            int parsed = Integer.parseInt(value.trim());
            return parsed >= minInclusive ? parsed : defaultValue;
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    private static long parseLongAtLeast(String value, long defaultValue, long minInclusive) {
        if (value == null || value.trim().isEmpty()) return defaultValue;
        try {
            long parsed = Long.parseLong(value.trim());
            return parsed >= minInclusive ? parsed : defaultValue;
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    private static boolean parseBoolean(String value, boolean defaultValue) {
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        String normalized = value.trim().toLowerCase(java.util.Locale.ROOT);
        if ("false".equals(normalized) || "0".equals(normalized) || "no".equals(normalized)) {
            return false;
        }
        if ("true".equals(normalized) || "1".equals(normalized) || "yes".equals(normalized)) {
            return true;
        }
        return defaultValue;
    }

    private static int sizeOf(List<?> values) {
        return values == null ? 0 : values.size();
    }

    @Override
    protected MCP_KEY_TYPE getKeyTypeForMCP(String key) {
        if (key.equals(ReactorKeysEnum.IMAGE.getKey()) || key.equals(ReactorKeysEnum.URL.getKey())) {
            return MCP_KEY_TYPE.ARRAY;
        }
        if (key.equals(ReactorKeysEnum.PARAM_VALUES_MAP.getKey()) || key.equals(ReactorKeysEnum.AGENT_PARAMS.getKey())) {
            return MCP_KEY_TYPE.OBJECT;
        }
        return super.getKeyTypeForMCP(key);
    }

    @Override
    protected String getDescriptionForKey(String key) {
        if (key.equals(ReactorKeysEnum.IMAGE.getKey())) {
            return "Array of image file names already uploaded to the insight folder, or supported base64 image/PDF data URIs.";
        }
        if (key.equals(ReactorKeysEnum.URL.getKey())) {
            return "Array of image file URLs whose contents will be fetched when building the initial agent message.";
        }
        return super.getDescriptionForKey(key);
    }
}
