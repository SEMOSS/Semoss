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
 *   filePath   = "<optional project id or file path>",
 *   harnessType = "room_loop",
 *   paramValues = {"key" : "val"}
 * )
 * }</pre>
 *
 * <p>Returns the final text as a {@code CONST_STRING NounMetadata}.
 */
public class RunGenericAgentReactor extends AbstractReactor {

    private static final Logger logger = LogManager.getLogger(RunGenericAgentReactor.class);

    // ── Custom key constants (not in ReactorKeysEnum) ─────────────────────────
    private static final String HARNESS_TYPE_KEY    = "harnessType";
    private static final String AGENT_ID_KEY        = "agentId";
    private static final String MAX_REFLECTIONS_KEY = "maxReflections";

    public RunGenericAgentReactor() {
        this.keysToGet = new String[] {
                ReactorKeysEnum.ROOM_ID.getKey(),             // 0 — required
                ReactorKeysEnum.COMMAND.getKey(),             // 1 — required
                ReactorKeysEnum.ENGINE.getKey(),              // 2 — optional fallback engine
                ReactorKeysEnum.FILE_PATH.getKey(),           // 3 — optional
                HARNESS_TYPE_KEY,                             // 4 — optional
                AGENT_ID_KEY,                                 // 5 — optional
                MAX_REFLECTIONS_KEY,                          // 6 — optional
                ReactorKeysEnum.PARAM_VALUES_MAP.getKey()     // 7 — optional
        };
        this.keyRequired = new int[] { 1, 1, 0, 0, 0, 0, 0, 0 };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();

        String roomId           = getString(0);
        String input            = getString(1);
        String engineIdFallback = getString(2);
        String filePath         = getString(3);
        String harnessType      = getString(4);
        // agentId reserved for future agent-config lookup
        // String agentId       = getString(5);
        String maxReflectionsStr = getString(6);
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

        logger.info("RunGenericAgentReactor: roomId={} engineFallback={} harnessType={} filePath={} maxReflections={}",
                roomId, engineIdFallback, harnessType, filePath, maxReflections);

        try {
            AgentHarnessResult result = GenericAgent.run(
                    roomId,
                    input,
                    engineIdFallback,
                    harnessType,
                    filePath,
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

    // ── Helpers ───────────────────────────────────────────────────────────────

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
