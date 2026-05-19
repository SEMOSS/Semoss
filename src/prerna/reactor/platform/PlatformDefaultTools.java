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
package prerna.reactor.platform;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.om.Insight;
import prerna.reactor.AbstractReactor;
import prerna.reactor.database.CommandReactor;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Registry of platform-level reactors exposed as default tools to the LLM. The
 * active set is controlled by the admin theme ({@code playground.platformTools}).
 * LLM-facing names are prefixed with {@value #PLATFORM_PREFIX} to distinguish
 * them from MCP engine tools.
 */
public final class PlatformDefaultTools {

    private static final Logger classLogger = LogManager.getLogger(PlatformDefaultTools.class);

    public static final String PLATFORM_PREFIX = "platform__";
    public static final String SMSS_REACTOR_CLASS = "SMSS_REACTOR_CLASS";
    public static final String SMSS_IS_PLATFORM_TOOL = "SMSS_IS_PLATFORM_TOOL";

    private static final Map<String, Class<? extends AbstractReactor>> REGISTRY;

    static {
        Map<String, Class<? extends AbstractReactor>> m = new HashMap<>();
        m.put("Command", CommandReactor.class);
        REGISTRY = Collections.unmodifiableMap(m);
    }

    private PlatformDefaultTools() {
    }

    /** True if {@code llmFacingName} is a platform tool (carries the {@value #PLATFORM_PREFIX} prefix). */
    public static boolean isPlatformTool(String llmFacingName) {
        return llmFacingName != null && llmFacingName.startsWith(PLATFORM_PREFIX);
    }

    /** Builds the LLM-ready tool schema for a registered tool; empty if unknown. */
    public static Optional<Map<String, Object>> buildToolSchema(String name) {
        Class<? extends AbstractReactor> clazz = REGISTRY.get(name);
        if (clazz == null) {
            classLogger.warn("PlatformDefaultTools: unknown tool name '{}' — skipping", name);
            return Optional.empty();
        }
        try {
            AbstractReactor reactor = clazz.getDeclaredConstructor().newInstance();
            JSONObject schema = reactor.asMcpTool();
            schema.put("name", PLATFORM_PREFIX + name);

            JSONObject meta = new JSONObject();
            meta.put(MCPUtility.SMSS_MCP_EXECUTION, "auto");
            meta.put(MCPUtility.SMSS_FUNCTION_NAME, name);
            meta.put(SMSS_REACTOR_CLASS, clazz.getName());
            meta.put(SMSS_IS_PLATFORM_TOOL, true);
            JSONObject uiJson = new JSONObject();
            uiJson.put(MCPUtility.UI_DISPLAY_LOCATION, "inline");
            meta.put(MCPUtility.SMSS_MCP_UI, uiJson);
            schema.put("_meta", meta);

            return Optional.of(schema.toMap());
        } catch (Exception e) {
            classLogger.error("PlatformDefaultTools: failed to build schema for '{}': {}", name, e.getMessage(), e);
            return Optional.empty();
        }
    }

    /**
     * Instantiates the platform tool's reactor with {@code params} and runs it.
     * Preserves ERROR type from the inner reactor; throws if {@code llmFacingName}
     * is not in the registry.
     */
    public static NounMetadata execute(String llmFacingName, Map<String, Object> params, Insight insight) {
        String bareName = stripPrefix(llmFacingName);
        Class<? extends AbstractReactor> clazz = REGISTRY.get(bareName);
        if (clazz == null) {
            throw new IllegalArgumentException("No platform tool registered for name '" + llmFacingName + "'");
        }
        try {
            AbstractReactor reactor = clazz.getDeclaredConstructor().newInstance();
            reactor.In();
            reactor.setInsight(insight);

            if (params != null) {
                for (Map.Entry<String, Object> entry : params.entrySet()) {
                    GenRowStruct grs = new GenRowStruct();
                    Object val = entry.getValue();
                    String strVal = val != null ? val.toString() : "";
                    grs.add(new NounMetadata(strVal, PixelDataType.CONST_STRING));
                    reactor.getNounStore().addNoun(entry.getKey(), grs);
                }
            }

            NounMetadata result = reactor.execute();
            return result != null ? result : new NounMetadata("", PixelDataType.CONST_STRING);
        } catch (Exception e) {
            return NounMetadata.getErrorNounMessage("Tool execution error: " + e.getMessage());
        }
    }

    private static String stripPrefix(String llmFacingName) {
        if (llmFacingName != null && llmFacingName.startsWith(PLATFORM_PREFIX)) {
            return llmFacingName.substring(PLATFORM_PREFIX.length());
        }
        return llmFacingName;
    }
}
