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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

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
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Hook that executes a user-supplied Pixel expression at one or more
 * agent lifecycle events. Lets users wire custom behavior without writing
 * Java — they just register a hook entry in
 * {@code WORKSPACE.CONFIG_JSON.hooks[]} shaped:
 *
 * <pre>
 * { "kind": "pixel",
 *   "pixel": "MyReactor(value=[hookOutput]);",
 *   "events": ["afterRun"],
 *   "bindings": { "hookOutput": "result.finalText" } }
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
 * <p>Optional {@code bindings} copy values from the lifecycle payload into
 * temporary Insight variables. The Pixel expression references them using
 * normal Pixel variable syntax (for example {@code [hookOutput]}). Values are
 * bound as typed nouns rather than interpolated into the Pixel source, so
 * arbitrary model output remains data. Any pre-existing variables with the
 * configured names are restored after execution.
 */
public final class PixelReactorHook implements IAgentRunHook, IToolHook {

    private static final Logger logger = LogManager.getLogger(PixelReactorHook.class);

    private static final Pattern VARIABLE_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Set<String> KNOWN_BINDING_SOURCES = Set.of(
            "event", "payload",
            "context", "context.runId", "context.roomId", "context.userId",
            "context.input", "context.spawnDepth",
            "result", "result.finalText", "result.iterations", "result.reflectionsUsed",
            "result.inputMessageId", "result.finalOutputMessageId", "result.toolCallRecords",
            "tool", "tool.name", "tool.callId", "tool.params", "tool.resultContent",
            "tool.durationMs", "tool.success", "tool.iteration");

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
    /** Insight variable name to lifecycle-payload path. */
    private Map<String, String> bindings = Collections.emptyMap();
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
        this.bindings = Collections.emptyMap();
        this.eventFilter = Collections.emptySet();

        JSONObject bindingsJson = spec.optJSONObject("bindings");
        if (spec.has("bindings") && bindingsJson == null) {
            throw new IllegalArgumentException("PixelReactorHook: 'bindings' must be an object");
        }
        if (bindingsJson != null) {
            Map<String, String> configuredBindings = new LinkedHashMap<>();
            for (String variableName : bindingsJson.keySet()) {
                if (!VARIABLE_NAME.matcher(variableName).matches()) {
                    throw new IllegalArgumentException("PixelReactorHook: invalid binding variable name '"
                            + variableName + "'");
                }
                Object sourceValue = bindingsJson.opt(variableName);
                if (!(sourceValue instanceof String) || ((String) sourceValue).trim().isEmpty()) {
                    throw new IllegalArgumentException("PixelReactorHook: binding '" + variableName
                            + "' must name a lifecycle payload field");
                }
                String source = ((String) sourceValue).trim();
                if (!KNOWN_BINDING_SOURCES.contains(source)) {
                    throw new IllegalArgumentException("PixelReactorHook: unknown binding source '" + source
                            + "'. Known sources: " + KNOWN_BINDING_SOURCES);
                }
                configuredBindings.put(variableName, source);
            }
            this.bindings = Collections.unmodifiableMap(configuredBindings);
        }

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

    @Override public void onRoomCreation(AgentRunContext ctx)                               { fire(ctx, EVT_ON_ROOM_CREATION, null, null); }
    @Override public void beforeRun(AgentRunContext ctx)                                    { fire(ctx, EVT_BEFORE_RUN, null, null); }
    @Override public void afterAgentInit(AgentRunContext ctx)                               { fire(ctx, EVT_AFTER_AGENT_INIT, null, null); }
    @Override public void afterRun(AgentRunContext ctx, AgentHarnessResult result)          { fire(ctx, EVT_AFTER_RUN, result, null); }
    @Override public void beforeAgentDeInit(AgentRunContext ctx, AgentHarnessResult result) { fire(ctx, EVT_BEFORE_AGENT_DEINIT, result, null); }

    // IToolHook lifecycle

    @Override
    public void beforeTool(AgentRunContext ctx, String toolName, String toolCallId,
                           Map<String, Object> params, int iteration) {
        fire(ctx, EVT_BEFORE_TOOL, null,
                toolPayload(toolName, toolCallId, params, null, null, null, iteration));
    }

    @Override
    public void afterTool(AgentRunContext ctx, String toolName, String toolCallId,
                          Map<String, Object> params, String resultContent,
                          long durationMs, boolean success, int iteration) {
        fire(ctx, EVT_AFTER_TOOL, null,
                toolPayload(toolName, toolCallId, params, resultContent, durationMs, success, iteration));
    }

    // Internals

    private void fire(AgentRunContext ctx, String event, AgentHarnessResult result,
                      Map<String, Object> tool) {
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
        Map<String, Object> payload = lifecyclePayload(ctx, roomId, event, result, tool);
        VarStore varStore = insight.getVarStore();
        Map<String, NounMetadata> previousValues = new LinkedHashMap<>();
        Set<String> absentVariables = new HashSet<>();
        try {
            for (Map.Entry<String, String> binding : bindings.entrySet()) {
                String variableName = binding.getKey();
                if (varStore.containsKey(variableName)) {
                    previousValues.put(variableName, varStore.get(variableName));
                } else {
                    absentVariables.add(variableName);
                }
                Object value = resolvePayloadValue(payload, binding.getValue());
                varStore.put(variableName, NounMetadata.predictNounMetadata(value));
            }
            logger.debug("[pixel-hook] event={} room={} firing pixel: {}", event, roomId, pixel);
            insight.runPixel(pixel);
        } catch (Exception e) {
            logger.warn("[pixel-hook] event={} room={} pixel threw — logging and continuing. cause: {}",
                    event, roomId, e.getMessage(), e);
        } finally {
            for (String variableName : absentVariables) {
                varStore.remove(variableName);
            }
            for (Map.Entry<String, NounMetadata> previous : previousValues.entrySet()) {
                varStore.put(previous.getKey(), previous.getValue());
            }
        }
    }

    private static Map<String, Object> lifecyclePayload(AgentRunContext ctx, String roomId, String event,
                                                         AgentHarnessResult result, Map<String, Object> tool) {
        Map<String, Object> context = new LinkedHashMap<>();
        context.put("runId", ctx.getRunId());
        context.put("roomId", roomId);
        context.put("userId", ctx.getUserId());
        context.put("input", ctx.getInput());
        context.put("spawnDepth", ctx.getSpawnDepth());

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("event", event);
        payload.put("context", context);
        payload.put("result", resultPayload(result));
        payload.put("tool", tool);
        return payload;
    }

    private static Map<String, Object> resultPayload(AgentHarnessResult result) {
        if (result == null) return null;
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("finalText", result.getFinalText());
        value.put("iterations", result.getIterations());
        value.put("reflectionsUsed", result.getReflectionsUsed());
        value.put("inputMessageId", result.getInputMessageId());
        value.put("finalOutputMessageId", result.getFinalOutputMessageId());
        List<Map<String, Object>> toolCalls = new ArrayList<>();
        List<AgentHarnessResult.ToolCallRecord> records = result.getToolCallRecords();
        if (records != null) {
            for (AgentHarnessResult.ToolCallRecord record : records) {
                Map<String, Object> call = new LinkedHashMap<>();
                call.put("toolName", record.getToolName());
                call.put("toolCallId", record.getToolCallId());
                call.put("result", record.getResult());
                call.put("durationMs", record.getDurationMs());
                call.put("success", record.isSuccess());
                toolCalls.add(call);
            }
        }
        value.put("toolCallRecords", toolCalls);
        return value;
    }

    private static Map<String, Object> toolPayload(String toolName, String toolCallId,
                                                    Map<String, Object> params, String resultContent,
                                                    Long durationMs, Boolean success, int iteration) {
        Map<String, Object> tool = new LinkedHashMap<>();
        tool.put("name", toolName);
        tool.put("callId", toolCallId);
        tool.put("params", params == null ? Collections.emptyMap() : new LinkedHashMap<>(params));
        tool.put("resultContent", resultContent);
        tool.put("durationMs", durationMs);
        tool.put("success", success);
        tool.put("iteration", iteration);
        return tool;
    }

    private static Object resolvePayloadValue(Map<String, Object> payload, String path) {
        // Binding the entire payload is useful for generic audit/storage reactors.
        if ("payload".equals(path)) return payload;
        Object current = payload;
        for (String segment : path.split("\\.")) {
            if (!(current instanceof Map)) return null;
            current = ((Map<?, ?>) current).get(segment);
        }
        return current;
    }
}
