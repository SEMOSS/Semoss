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
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Pixel reactor that invokes the generic agent loop.
 *
 * <h3>Pixel syntax</h3>
 * <pre>{@code
 * RunGenericAgent(
 *   roomId     = "<roomId>",
 *   command    = "<user prompt>",
 *   engine     = "<engineId>",
 *   harnessType = "room_loop",
 *   paramValues = {"key" : "val"}
 * )
 * }</pre>
 *
 * <p>Returns the final text as a {@code CONST_STRING NounMetadata}.
 */
public class RunAgentReactor extends AbstractReactor {

    private static final Logger logger = LogManager.getLogger(RunAgentReactor.class);

    private static final String HARNESS_TYPE_KEY    = "harnessType";
    private static final String AGENT_ID_KEY        = "agentId";
    private static final String MAX_REFLECTIONS_KEY = "maxReflections";

    public RunAgentReactor() {
        this.keysToGet = new String[] {
                ReactorKeysEnum.ROOM_ID.getKey(),             
                ReactorKeysEnum.COMMAND.getKey(),             
                ReactorKeysEnum.ENGINE.getKey(),              
                HARNESS_TYPE_KEY,                             
                AGENT_ID_KEY,                                 
                MAX_REFLECTIONS_KEY,                          
                ReactorKeysEnum.PARAM_VALUES_MAP.getKey()
        };
        this.keyRequired = new int[] { 1, 1, 0, 0, 0, 0, 0 };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();

        String roomId           = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
        String input            = this.keyValue.get(ReactorKeysEnum.COMMAND.getKey());
        String engineIdFallback = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
        String harnessType      = this.keyValue.get(HARNESS_TYPE_KEY);
        
        // agentId reserved for future agent-config lookup
        // String agentId       = this.keyValue.get(AGENT_ID_KEY);
        String maxReflectionsStr = this.keyValue.get(MAX_REFLECTIONS_KEY);
        int maxReflections = GenericAgentContext.DEFAULT_MAX_REFLECTIONS;
        if (maxReflectionsStr != null && !maxReflectionsStr.trim().isEmpty()) {
            try {
                maxReflections = Integer.parseInt(maxReflectionsStr.trim());
            } catch (NumberFormatException ignored) {
                // leave as default
            }
        }
        Map<String, Object> paramMap = getMap();

        if (roomId == null || roomId.trim().isEmpty()) {
            throw new IllegalArgumentException("roomId is required for RunGenericAgent");
        }
        if (input == null || input.trim().isEmpty()) {
            throw new IllegalArgumentException("command (input) is required for RunGenericAgent");
        }

        logger.info("RunGenericAgentReactor: roomId={} engineFallback={} harnessType={} maxReflections={}",
                roomId, engineIdFallback, harnessType, maxReflections);

        try {
            AgentHarnessResult result = GenericAgent.run(
                    roomId,
                    input,
                    engineIdFallback,
                    harnessType,
                    maxReflections,
                    paramMap,
                    this.insight);

            logger.info("RunGenericAgentReactor: completed iterations={} reflections={} tools={}",
                    result.getIterations(), result.getReflectionsUsed(), result.getToolCallRecords().size());

            return new NounMetadata(result.getFinalText(), PixelDataType.CONST_STRING,
                    PixelOperationType.OPERATION);

        } catch (AgentMaxIterationsException e) {
            throw new IllegalStateException(e.getMessage(), e);
        } catch (Exception e) {
            logger.error("RunGenericAgentReactor: error running agent loop", e);
            throw new IllegalStateException("Agent execution failed: " + e.getMessage(), e);
        }
    }

    @Override
    public String getReactorDescription() {
        return "Run a generic agent loop using a pluggable harness (room_loop or claude_code)";
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
}
