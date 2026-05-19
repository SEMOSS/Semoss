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

import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.om.Insight;
import prerna.reactor.AbstractReactor;
import prerna.reactor.IReactor;
import prerna.reactor.ReactorFactory;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Exposes Pixel reactors to the LLM as platform default tools. The active set
 * is controlled entirely by the admin theme ({@code playground.platformTools});
 * any bare reactor name registered with {@link ReactorFactory#reactorHash}
 * (i.e. any reactor invocable from Pixel) that extends {@link AbstractReactor}
 * is a valid candidate. LLM-facing names are prefixed with {@value #PLATFORM_PREFIX}.
 *
 * <p>Because admins can list any Pixel reactor here, treat the theme as a
 * security boundary — exposing destructive or admin-only reactors will let the
 * LLM invoke them.
 */
public final class PlatformDefaultTools {

    private static final Logger classLogger = LogManager.getLogger(PlatformDefaultTools.class);

    public static final String PLATFORM_PREFIX = "platform__";
    public static final String SMSS_REACTOR_CLASS = "SMSS_REACTOR_CLASS";
    public static final String SMSS_IS_PLATFORM_TOOL = "SMSS_IS_PLATFORM_TOOL";

    private PlatformDefaultTools() {
    }

    /**
     * Resolves a bare reactor name from the theme to an instantiable
     * {@link AbstractReactor} subclass via {@link ReactorFactory#reactorHash}.
     * Returns {@code null} if unknown or not an {@code AbstractReactor}.
     */
    private static Class<? extends AbstractReactor> resolveReactorClass(String name) {
        Class<? extends IReactor> raw = ReactorFactory.reactorHash.get(name);
        if (raw == null) {
            return null;
        }
        if (!AbstractReactor.class.isAssignableFrom(raw)) {
            classLogger.warn(
                    "PlatformDefaultTools: reactor '{}' is not an AbstractReactor; cannot expose as platform tool",
                    name);
            return null;
        }
        return raw.asSubclass(AbstractReactor.class);
    }

    /** True if {@code llmFacingName} is a platform tool (carries the {@value #PLATFORM_PREFIX} prefix). */
    public static boolean isPlatformTool(String llmFacingName) {
        return llmFacingName != null && llmFacingName.startsWith(PLATFORM_PREFIX);
    }

    /** Builds the LLM-ready tool schema for {@code name}; empty if no such reactor. */
    public static Optional<Map<String, Object>> buildToolSchema(String name) {
        Class<? extends AbstractReactor> clazz = resolveReactorClass(name);
        if (clazz == null) {
            classLogger.warn(
                    "PlatformDefaultTools: theme references reactor '{}' but no AbstractReactor with that name is registered — skipping",
                    name);
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
     * Instantiates the reactor named by {@code llmFacingName} with {@code params}
     * and runs it. Preserves ERROR type from the inner reactor; throws if the
     * bare name doesn't resolve to an {@link AbstractReactor} in
     * {@link ReactorFactory#reactorHash}.
     */
    public static NounMetadata execute(String llmFacingName, Map<String, Object> params, Insight insight) {
        String bareName = stripPrefix(llmFacingName);
        Class<? extends AbstractReactor> clazz = resolveReactorClass(bareName);
        if (clazz == null) {
            throw new IllegalArgumentException(
                    "No AbstractReactor registered for platform tool name '" + llmFacingName + "'");
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
