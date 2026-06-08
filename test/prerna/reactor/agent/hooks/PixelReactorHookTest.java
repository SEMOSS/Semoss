/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *******************************************************************************/
package prerna.reactor.agent.hooks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.engine.impl.model.Room;
import prerna.om.Insight;
import prerna.reactor.agent.AgentHarnessResult;
import prerna.reactor.agent.AgentRunContext;

/**
 * Unit tests for {@link PixelReactorHook}: configure() validation,
 * event-filter behavior, exception swallowing, and null-insight guard.
 */
class PixelReactorHookTest {

    private PixelReactorHook hook;
    private AgentRunContext ctx;
    private Insight insight;
    private Room room;

    @BeforeEach
    void setUp() {
        hook = new PixelReactorHook();
        ctx = mock(AgentRunContext.class);
        insight = mock(Insight.class);
        room = mock(Room.class);
        when(ctx.getInsight()).thenReturn(insight);
        when(ctx.getRoom()).thenReturn(room);
        when(room.getId()).thenReturn("room-123");
    }

    // ---------- configure() validation ----------

    @Test
    void configureThrowsWhenPixelMissing() {
        JSONObject spec = new JSONObject();
        spec.put("kind", "pixel");
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class, () -> hook.configure(spec));
        assertTrue(ex.getMessage().contains("pixel"));
    }

    @Test
    void configureThrowsWhenPixelEmpty() {
        JSONObject spec = new JSONObject();
        spec.put("kind", "pixel");
        spec.put("pixel", "   ");
        assertThrows(IllegalArgumentException.class, () -> hook.configure(spec));
    }

    @Test
    void configureThrowsWhenSpecIsNull() {
        assertThrows(IllegalArgumentException.class, () -> hook.configure(null));
    }

    @Test
    void configureThrowsWhenEventNameUnknown() {
        JSONObject spec = new JSONObject();
        spec.put("pixel", "Date();");
        spec.put("events", new JSONArray().put("beforeRun").put("notARealEvent"));
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class, () -> hook.configure(spec));
        assertTrue(ex.getMessage().contains("notARealEvent"));
    }

    @Test
    void configureAcceptsAllKnownEvents() {
        JSONObject spec = new JSONObject();
        spec.put("pixel", "Date();");
        spec.put("events", new JSONArray()
                .put(PixelReactorHook.EVT_ON_ROOM_CREATION)
                .put(PixelReactorHook.EVT_BEFORE_RUN)
                .put(PixelReactorHook.EVT_AFTER_AGENT_INIT)
                .put(PixelReactorHook.EVT_BEFORE_TOOL)
                .put(PixelReactorHook.EVT_AFTER_TOOL)
                .put(PixelReactorHook.EVT_AFTER_RUN)
                .put(PixelReactorHook.EVT_BEFORE_AGENT_DEINIT));
        hook.configure(spec); // no throw
    }

    // ---------- event firing ----------

    @Test
    void firesPixelOnAllEventsWhenFilterEmpty() {
        JSONObject spec = new JSONObject();
        spec.put("pixel", "Date();");
        hook.configure(spec);

        hook.onRoomCreation(ctx);
        hook.beforeRun(ctx);
        hook.afterAgentInit(ctx);
        hook.beforeTool(ctx, "Bash", "call-1", new HashMap<>(), 0);
        hook.afterTool(ctx, "Bash", "call-1", new HashMap<>(), "ok", 12L, true, 0);
        hook.afterRun(ctx, mock(AgentHarnessResult.class));
        hook.beforeAgentDeInit(ctx, mock(AgentHarnessResult.class));

        // 7 lifecycle events, all should have fired the pixel.
        verify(insight, org.mockito.Mockito.times(7)).runPixel(eq("Date();"));
    }

    @Test
    void respectsEventFilterAndSkipsOthers() {
        JSONObject spec = new JSONObject();
        spec.put("pixel", "Date();");
        spec.put("events", new JSONArray().put(PixelReactorHook.EVT_AFTER_RUN));
        hook.configure(spec);

        hook.beforeRun(ctx);
        hook.beforeTool(ctx, "Bash", "call-1", new HashMap<>(), 0);
        hook.afterRun(ctx, mock(AgentHarnessResult.class));

        // Only the afterRun event passes the filter.
        verify(insight, org.mockito.Mockito.times(1)).runPixel(eq("Date();"));
    }

    @Test
    void multipleEventsInFilterFireSelectively() {
        JSONObject spec = new JSONObject();
        spec.put("pixel", "Date();");
        spec.put("events", new JSONArray()
                .put(PixelReactorHook.EVT_BEFORE_TOOL)
                .put(PixelReactorHook.EVT_AFTER_TOOL));
        hook.configure(spec);

        hook.beforeRun(ctx);                                                   // skipped
        hook.beforeTool(ctx, "Bash", "c", new HashMap<>(), 0);                 // fires
        hook.afterTool(ctx, "Bash", "c", new HashMap<>(), "", 1L, true, 0);    // fires
        hook.afterRun(ctx, mock(AgentHarnessResult.class));                    // skipped

        verify(insight, org.mockito.Mockito.times(2)).runPixel(eq("Date();"));
    }

    // ---------- error handling ----------

    @Test
    void swallowsRunPixelExceptionsAndContinues() {
        JSONObject spec = new JSONObject();
        spec.put("pixel", "BadReactor();");
        hook.configure(spec);

        doThrow(new RuntimeException("boom")).when(insight).runPixel("BadReactor();");

        // Should NOT propagate — exception is logged and swallowed.
        hook.beforeRun(ctx);
        hook.afterRun(ctx, mock(AgentHarnessResult.class));

        verify(insight, org.mockito.Mockito.times(2)).runPixel(eq("BadReactor();"));
    }

    @Test
    void skipsFireWhenInsightIsNull() {
        JSONObject spec = new JSONObject();
        spec.put("pixel", "Date();");
        hook.configure(spec);

        AgentRunContext nullInsightCtx = mock(AgentRunContext.class);
        when(nullInsightCtx.getInsight()).thenReturn(null);

        hook.beforeRun(nullInsightCtx);

        verify(insight, never()).runPixel(eq("Date();"));
    }

    @Test
    void skipsFireWhenConfigureWasNeverCalled() {
        // No configure(); pixel is null.
        hook.beforeRun(ctx);
        verify(insight, never()).runPixel(eq("Date();"));
    }

    @Test
    void firesActualPixelStringFromConfig() {
        JSONObject spec = new JSONObject();
        spec.put("pixel", "MyReactor(arg='hello');");
        hook.configure(spec);

        hook.beforeRun(ctx);

        verify(insight).runPixel(eq("MyReactor(arg='hello');"));
    }

    // ---------- params untouched (no interpolation today) ----------

    @Test
    void doesNotInterpolateToolParamsIntoPixel() {
        JSONObject spec = new JSONObject();
        spec.put("pixel", "Echo(name='${toolName}');");
        hook.configure(spec);

        Map<String, Object> params = new HashMap<>();
        params.put("foo", "bar");

        hook.beforeTool(ctx, "Bash", "c", params, 0);

        // Pixel is fired as-is — interpolation is explicitly out of scope.
        verify(insight).runPixel(eq("Echo(name='${toolName}');"));
    }

    @Test
    void afterToolReceivesAllArgsAndStillFires() {
        JSONObject spec = new JSONObject();
        spec.put("pixel", "LogIt();");
        hook.configure(spec);

        assertEquals(0, 0); // placeholder
        hook.afterTool(ctx, "Bash", "c", new HashMap<>(), "result-text", 42L, true, 3);

        verify(insight).runPixel(eq("LogIt();"));
    }
}
