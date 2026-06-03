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

/** Runs the generic agent loop and returns the final text as {@code CONST_STRING}. */
public class RunAgentReactor extends AbstractReactor {

    private static final Logger logger = LogManager.getLogger(RunAgentReactor.class);

    private static final String HARNESS_TYPE_KEY    = "harnessType";
    private static final String WORKSPACE_ID_KEY    = "workspaceId";
    private static final String MAX_TURNS_KEY       = "maxTurns";
    private static final String MAX_ITERATIONS_KEY  = "maxIterations";
    private static final String MAX_REFLECTIONS_KEY = "maxReflections";

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
                ReactorKeysEnum.PARAM_VALUES_MAP.getKey()
        };
        this.keyRequired = new int[] { 1, 1, 0, 0, 0, 0, 0, 0, 0 };
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
        Map<String, Object> paramMap = getMap();

        if (roomId == null || roomId.trim().isEmpty()) {
            throw new IllegalArgumentException("roomId is required for RunAgent");
        }
        if (input == null || input.trim().isEmpty()) {
            throw new IllegalArgumentException("command (input) is required for RunAgent");
        }

        logger.info("RunAgentReactor: roomId={} engineFallback={} harnessType={} workspaceId={} maxTurns={} maxReflections={}",
                roomId, engineIdFallback, harnessType, explicitWorkspaceId, maxTurns, maxReflections);

        try {
            RunAgentRequest request = new RunAgentRequest(
                    roomId,
                    input,
                    engineIdFallback,
                    harnessType,
                    explicitWorkspaceId,
                    maxTurns,
                    maxReflections,
                    paramMap,
                    this.insight);
            RunAgentResult handle = AgentRuntimeManager.get().run(request);
            AgentHarnessResult result = handle.getResult();

            logger.info("RunAgentReactor: completed runId={} iterations={} reflections={} tools={}",
                    handle.getRunId(), result.getIterations(), result.getReflectionsUsed(),
                    result.getToolCallRecords().size());

            return new NounMetadata(result.getFinalText(), PixelDataType.CONST_STRING,
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
        return "Run a generic agent loop using a pluggable harness. maxTurns applies to the SEMOSS harness tool loop; maxReflections controls optional self-critique rounds.";
    }

    // Helpers

    @SuppressWarnings("unchecked")
    private Map<String, Object> getMap() {
        GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
        if (mapGrs != null && !mapGrs.isEmpty()) {
            List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
            if (mapInputs != null && !mapInputs.isEmpty()) {
                return (Map<String, Object>) mapInputs.get(0).getValue();
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
}
