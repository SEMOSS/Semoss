/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *******************************************************************************/
package prerna.engine.impl.model.inferencetracking.reactors.workspaces;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.om.Insight;
import prerna.reactor.agent.hooks.AgentHookRegistry;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Unit tests for {@link GetAgentHooksReactor}. Verifies the response shape
 * is {@code {hooks, knownKinds}}, handles missing CONFIG_JSON gracefully,
 * and enforces the view-permission gate.
 */
class GetAgentHooksReactorTest {

    private static final String WORKSPACE_ID = "ws-abc";

    private GetAgentHooksReactor reactor;
    private Insight insight;
    private User user;

    @BeforeEach
    void setUp() {
        reactor = new GetAgentHooksReactor();
        reactor.setNounStore(new NounStore("test"));
        insight = mock(Insight.class);
        user = mock(User.class);
        reactor.setInsight(insight);
        reactor.keyValue = new HashMap<>();
        reactor.keyValue.put(ReactorKeysEnum.WORKSPACE_ID.getKey(), WORKSPACE_ID);

        when(insight.getUser()).thenReturn(user);
    }

    // ---------- happy path ----------

    @Test
    @SuppressWarnings("unchecked")
    void returnsHooksAndKnownKindsMap() {
        JSONObject cfg = new JSONObject();
        cfg.put("schema_version", 1);
        JSONArray hooksArr = new JSONArray();
        hooksArr.put(new JSONObject().put("kind", "pixel").put("pixel", "Date();")
                .put("events", new JSONArray().put("beforeRun")));
        hooksArr.put(new JSONObject().put("kind", "log_tools"));
        cfg.put("hooks", hooksArr);

        try (MockedStatic<ModelInferenceLogsUtils> mils = Mockito.mockStatic(ModelInferenceLogsUtils.class);
             MockedStatic<SecurityProjectUtils> sec = Mockito.mockStatic(SecurityProjectUtils.class)) {

            mils.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(WORKSPACE_ID)).thenReturn(new HashMap<>());
            mils.when(() -> ModelInferenceLogsUtils.getWorkspaceConfigJson(WORKSPACE_ID)).thenReturn(cfg);
            sec.when(() -> SecurityProjectUtils.userCanViewProject(eq(user), eq(WORKSPACE_ID))).thenReturn(true);

            NounMetadata result = reactor.execute();

            assertEquals(PixelDataType.MAP, result.getNounType());
            Map<String, Object> map = (Map<String, Object>) result.getValue();
            assertNotNull(map);

            List<Map<String, Object>> hooks = (List<Map<String, Object>>) map.get("hooks");
            assertEquals(2, hooks.size());
            assertEquals("pixel", hooks.get(0).get("kind"));
            assertEquals("Date();", hooks.get(0).get("pixel"));
            assertEquals("log_tools", hooks.get(1).get("kind"));

            List<String> kinds = (List<String>) map.get("knownKinds");
            // Built-ins must be present.
            assertTrue(kinds.contains(AgentHookRegistry.PIXEL));
            assertTrue(kinds.contains(AgentHookRegistry.GIT_COMMIT));
            assertTrue(kinds.contains(AgentHookRegistry.LOG_TOOLS));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void returnsEmptyHooksWhenConfigJsonIsNull() {
        try (MockedStatic<ModelInferenceLogsUtils> mils = Mockito.mockStatic(ModelInferenceLogsUtils.class);
             MockedStatic<SecurityProjectUtils> sec = Mockito.mockStatic(SecurityProjectUtils.class)) {

            mils.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(WORKSPACE_ID)).thenReturn(new HashMap<>());
            mils.when(() -> ModelInferenceLogsUtils.getWorkspaceConfigJson(WORKSPACE_ID)).thenReturn(null);
            sec.when(() -> SecurityProjectUtils.userCanViewProject(eq(user), eq(WORKSPACE_ID))).thenReturn(true);

            NounMetadata result = reactor.execute();

            Map<String, Object> map = (Map<String, Object>) result.getValue();
            List<Map<String, Object>> hooks = (List<Map<String, Object>>) map.get("hooks");
            assertNotNull(hooks);
            assertTrue(hooks.isEmpty());

            // knownKinds always populated
            List<String> kinds = (List<String>) map.get("knownKinds");
            assertFalse(kinds.isEmpty());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void returnsEmptyHooksWhenHooksKeyMissingFromConfig() {
        JSONObject cfg = new JSONObject(); // no hooks key

        try (MockedStatic<ModelInferenceLogsUtils> mils = Mockito.mockStatic(ModelInferenceLogsUtils.class);
             MockedStatic<SecurityProjectUtils> sec = Mockito.mockStatic(SecurityProjectUtils.class)) {

            mils.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(WORKSPACE_ID)).thenReturn(new HashMap<>());
            mils.when(() -> ModelInferenceLogsUtils.getWorkspaceConfigJson(WORKSPACE_ID)).thenReturn(cfg);
            sec.when(() -> SecurityProjectUtils.userCanViewProject(eq(user), eq(WORKSPACE_ID))).thenReturn(true);

            NounMetadata result = reactor.execute();
            Map<String, Object> map = (Map<String, Object>) result.getValue();
            assertTrue(((List<?>) map.get("hooks")).isEmpty());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void doesNotThrowWhenConfigJsonFetchFails() {
        try (MockedStatic<ModelInferenceLogsUtils> mils = Mockito.mockStatic(ModelInferenceLogsUtils.class);
             MockedStatic<SecurityProjectUtils> sec = Mockito.mockStatic(SecurityProjectUtils.class)) {

            mils.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(WORKSPACE_ID)).thenReturn(new HashMap<>());
            mils.when(() -> ModelInferenceLogsUtils.getWorkspaceConfigJson(anyString()))
                    .thenThrow(new RuntimeException("DB down"));
            sec.when(() -> SecurityProjectUtils.userCanViewProject(eq(user), eq(WORKSPACE_ID))).thenReturn(true);

            NounMetadata result = reactor.execute();
            Map<String, Object> map = (Map<String, Object>) result.getValue();
            assertTrue(((List<?>) map.get("hooks")).isEmpty());
            // knownKinds always populated even on cfg fetch failure
            assertFalse(((List<?>) map.get("knownKinds")).isEmpty());
        }
    }

    // ---------- gates ----------

    @Test
    void rejectsWhenWorkspaceNotFound() {
        try (MockedStatic<ModelInferenceLogsUtils> mils = Mockito.mockStatic(ModelInferenceLogsUtils.class)) {
            mils.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(WORKSPACE_ID)).thenReturn(null);
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> reactor.execute());
            assertTrue(ex.getMessage().contains("Workspace not found"));
        }
    }

    @Test
    void rejectsWhenUserCannotView() {
        try (MockedStatic<ModelInferenceLogsUtils> mils = Mockito.mockStatic(ModelInferenceLogsUtils.class);
             MockedStatic<SecurityProjectUtils> sec = Mockito.mockStatic(SecurityProjectUtils.class)) {

            mils.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(WORKSPACE_ID)).thenReturn(new HashMap<>());
            sec.when(() -> SecurityProjectUtils.userCanViewProject(eq(user), eq(WORKSPACE_ID))).thenReturn(false);

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> reactor.execute());
            assertTrue(ex.getMessage().contains("does not have access"));
        }
    }

    @Test
    void rejectsWhenWorkspaceIdMissing() {
        reactor.keyValue.remove(ReactorKeysEnum.WORKSPACE_ID.getKey());
        assertThrows(Exception.class, () -> reactor.execute());
    }

    // ---------- nested-object preservation ----------

    @Test
    @SuppressWarnings("unchecked")
    void preservesNestedFieldsThroughJsonToMapConversion() {
        JSONObject cfg = new JSONObject();
        JSONArray hooksArr = new JSONArray();
        JSONObject entry = new JSONObject()
                .put("kind", "pixel")
                .put("pixel", "MyReactor();")
                .put("events", new JSONArray().put("beforeRun").put("afterRun"))
                .put("nested", new JSONObject().put("k", "v"));
        hooksArr.put(entry);
        cfg.put("hooks", hooksArr);

        try (MockedStatic<ModelInferenceLogsUtils> mils = Mockito.mockStatic(ModelInferenceLogsUtils.class);
             MockedStatic<SecurityProjectUtils> sec = Mockito.mockStatic(SecurityProjectUtils.class)) {

            mils.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(WORKSPACE_ID)).thenReturn(new HashMap<>());
            mils.when(() -> ModelInferenceLogsUtils.getWorkspaceConfigJson(WORKSPACE_ID)).thenReturn(cfg);
            sec.when(() -> SecurityProjectUtils.userCanViewProject(eq(user), eq(WORKSPACE_ID))).thenReturn(true);

            NounMetadata result = reactor.execute();
            Map<String, Object> map = (Map<String, Object>) result.getValue();
            List<Map<String, Object>> hooks = (List<Map<String, Object>>) map.get("hooks");
            Map<String, Object> first = hooks.get(0);

            // events array should be a List<Object> after conversion
            assertTrue(first.get("events") instanceof List);
            List<Object> events = (List<Object>) first.get("events");
            assertEquals(2, events.size());
            assertEquals("beforeRun", events.get(0));

            // nested object should be a Map after conversion
            assertTrue(first.get("nested") instanceof Map);
            assertEquals("v", ((Map<String, Object>) first.get("nested")).get("k"));
        }
    }
}
