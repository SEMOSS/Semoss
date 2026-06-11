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
package prerna.reactor.agent.hooks;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.engine.impl.model.Room;
import prerna.om.Insight;
import prerna.reactor.agent.AgentHarnessResult;
import prerna.reactor.agent.AgentRunContext;
import prerna.reactor.agent.IAgentRunHook;
import prerna.reactor.agent.IToolHook;

/**
 * Hook that executes a user-supplied Pixel expression at one or more
 * agent lifecycle events. Lets users wire custom behavior without writing
 * Java — they just register a hook entry in
 * {@code WORKSPACE.CONFIG_JSON.hooks[]} shaped:
 *
 * <pre>
 * { "kind": "pixel",
 *   "pixel": "MyReactor(arg='value');",
 *   "events": ["beforeRun", "afterTool"] }
 * </pre>
 *
 * If {@code events} is omitted or empty, the pixel fires on every
 * lifecycle event this hook gets called for (i.e. all five run-level
 * points + both tool-level points).
 *
 * <p>The pixel expression is executed via
 * {@link Insight#runPixel(String)} on the run's insight. Exceptions are
 * caught and logged so a misbehaving pixel cannot abort the run — the
 * same observer-style semantics as the other reference hooks.
 *
 * <p>No variable interpolation is performed today; the pixel string is
 * fired as-is. Interpolation (e.g. {@code ${toolName}}, {@code ${roomId}})
 * can be added in a follow-up if a use case emerges.
 */
public final class PixelReactorHook implements IAgentRunHook, IToolHook {

    private static final Logger logger = LogManager.getLogger(PixelReactorHook.class);

    /** Valid event names accepted in the {@code events} filter. */
    public static final String EVT_ON_ROOM_CREATION    = "onRoomCreation";
    public static final String EVT_BEFORE_RUN          = "beforeRun";
    public static final String EVT_AFTER_AGENT_INIT    = "afterAgentInit";
    public static final String EVT_BEFORE_TOOL         = "beforeTool";
    public static final String EVT_AFTER_TOOL          = "afterTool";
    public static final String EVT_AFTER_RUN           = "afterRun";
    public static final String EVT_BEFORE_AGENT_DEINIT = "beforeAgentDeInit";

    private static final Set<String> KNOWN_EVENTS = new HashSet<>();
    static {
        Collections.addAll(KNOWN_EVENTS,
                EVT_ON_ROOM_CREATION, EVT_BEFORE_RUN, EVT_AFTER_AGENT_INIT,
                EVT_BEFORE_TOOL, EVT_AFTER_TOOL,
                EVT_AFTER_RUN, EVT_BEFORE_AGENT_DEINIT);
    }

    private String pixel;
    /** Subset of {@link #KNOWN_EVENTS} to fire on; empty means fire on all. */
    private Set<String> eventFilter = Collections.emptySet();

    @Override
    public void configure(JSONObject spec) {
        String p = spec == null ? null : spec.optString("pixel", null);
        if (p == null || p.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "PixelReactorHook: required field 'pixel' is missing or empty");
        }
        this.pixel = p;

        if (spec.has("events")) {
            JSONArray arr = spec.optJSONArray("events");
            if (arr != null && arr.length() > 0) {
                Set<String> filter = new HashSet<>();
                for (int i = 0; i < arr.length(); i++) {
                    String evt = arr.optString(i, null);
                    if (evt == null) continue;
                    if (!KNOWN_EVENTS.contains(evt)) {
                        throw new IllegalArgumentException(
                                "PixelReactorHook: unknown event '" + evt
                                        + "' in events[]. Known events: " + KNOWN_EVENTS);
                    }
                    filter.add(evt);
                }
                this.eventFilter = filter;
            }
        }
    }

    // IAgentRunHook lifecycle

    @Override public void onRoomCreation(AgentRunContext ctx)                                  { fire(ctx, EVT_ON_ROOM_CREATION); }
    @Override public void beforeRun(AgentRunContext ctx)                                       { fire(ctx, EVT_BEFORE_RUN); }
    @Override public void afterAgentInit(AgentRunContext ctx)                                  { fire(ctx, EVT_AFTER_AGENT_INIT); }
    @Override public void afterRun(AgentRunContext ctx, AgentHarnessResult result)             { fire(ctx, EVT_AFTER_RUN); }
    @Override public void beforeAgentDeInit(AgentRunContext ctx, AgentHarnessResult result)    { fire(ctx, EVT_BEFORE_AGENT_DEINIT); }

    // IToolHook lifecycle

    @Override
    public void beforeTool(AgentRunContext ctx, String toolName, String toolCallId,
                           Map<String, Object> params, int iteration) {
        fire(ctx, EVT_BEFORE_TOOL);
    }

    @Override
    public void afterTool(AgentRunContext ctx, String toolName, String toolCallId,
                          Map<String, Object> params, String resultContent,
                          long durationMs, boolean success, int iteration) {
        fire(ctx, EVT_AFTER_TOOL);
    }

    // Internals

    private void fire(AgentRunContext ctx, String event) {
        if (!eventFilter.isEmpty() && !eventFilter.contains(event)) {
            return;
        }
        if (pixel == null) {
            // configure() was never called or failed — shouldn't happen in
            // the normal flow but guard against misuse.
            logger.warn("[pixel-hook] event={} skipped — no pixel configured", event);
            return;
        }
        Insight insight = ctx == null ? null : ctx.getInsight();
        if (insight == null) {
            logger.warn("[pixel-hook] event={} skipped — no insight on context", event);
            return;
        }
        Room room = ctx.getRoom();
        String roomId = room == null ? null : room.getId();
        try {
            logger.debug("[pixel-hook] event={} room={} firing pixel: {}", event, roomId, pixel);
            insight.runPixel(pixel);
        } catch (Exception e) {
            logger.warn("[pixel-hook] event={} room={} pixel threw — logging and continuing. cause: {}",
                    event, roomId, e.getMessage(), e);
        }
    }
}
