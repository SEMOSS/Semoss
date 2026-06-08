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
/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *******************************************************************************/
package prerna.engine.impl.model.inferencetracking.reactors.workspaces;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.LinkedHashMap;
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
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Unit tests for {@link SetAgentHooksReactor} covering validation and the
 * happy-path write. The workspace DAO + security utility calls are
 * mocked via {@link MockedStatic}.
 */
class SetAgentHooksReactorTest {

    private static final String WORKSPACE_ID = "ws-123";
    private static final String HOOKS_KEY = "hooks";

    private SetAgentHooksReactor reactor;
    private NounStore nounStore;
    private Insight insight;
    private User user;

    @BeforeEach
    void setUp() {
        reactor = new SetAgentHooksReactor();
        nounStore = new NounStore("test");
        insight = mock(Insight.class);
        user = mock(User.class);

        reactor.setNounStore(nounStore);
        reactor.setInsight(insight);
        reactor.keyValue = new HashMap<>();
        reactor.keyValue.put(ReactorKeysEnum.WORKSPACE_ID.getKey(), WORKSPACE_ID);

        when(insight.getUser()).thenReturn(user);
    }

    private void addHooks(List<Map<String, Object>> hooks) {
        GenRowStruct grs = new GenRowStruct();
        for (Map<String, Object> h : hooks) {
            grs.add(new NounMetadata(h, PixelDataType.MAP));
        }
        nounStore.addNoun(HOOKS_KEY, grs);
    }

    private static Map<String, Object> hook(String kind, String pixel, String... events) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("kind", kind);
        if (pixel != null) m.put("pixel", pixel);
        if (events.length > 0) m.put("events", List.of(events));
        return m;
    }

    // ---------- happy path ----------

    @Test
    void persistsValidHookListAndReturnsConfigJson() {
        addHooks(List.of(
                hook("pixel", "Date();", "beforeRun"),
                hook("log_tools", null) // log_tools needs no extra fields
        ));

        Map<String, Object> currentRow = new HashMap<>();
        currentRow.put("WORKSPACE_ID", WORKSPACE_ID);

        try (MockedStatic<ModelInferenceLogsUtils> mils = Mockito.mockStatic(ModelInferenceLogsUtils.class);
             MockedStatic<SecurityProjectUtils> sec = Mockito.mockStatic(SecurityProjectUtils.class)) {

            mils.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(WORKSPACE_ID)).thenReturn(currentRow);
            mils.when(() -> ModelInferenceLogsUtils.getWorkspaceConfigJson(WORKSPACE_ID)).thenReturn(new JSONObject().put("schema_version", 1));
            mils.when(() -> ModelInferenceLogsUtils.updateWorkspaceConfigJson(eq(WORKSPACE_ID), any(JSONObject.class))).thenAnswer(inv -> null);
            sec.when(() -> SecurityProjectUtils.userCanEditProject(eq(user), eq(WORKSPACE_ID))).thenReturn(true);

            NounMetadata result = reactor.execute();

            assertEquals(PixelDataType.CONST_STRING, result.getNounType());
            String json = (String) result.getValue();
            assertNotNull(json);

            JSONObject cfg = new JSONObject(json);
            JSONArray hooksArr = cfg.getJSONArray("hooks");
            assertEquals(2, hooksArr.length());
            assertEquals("pixel", hooksArr.getJSONObject(0).getString("kind"));
            assertEquals("Date();", hooksArr.getJSONObject(0).getString("pixel"));
            assertEquals("log_tools", hooksArr.getJSONObject(1).getString("kind"));

            // verify CONFIG_JSON was actually written back
            mils.verify(() -> ModelInferenceLogsUtils.updateWorkspaceConfigJson(eq(WORKSPACE_ID), any(JSONObject.class)));
        }
    }

    @Test
    void seedsSchemaVersionWhenNoExistingConfig() {
        // First-time write to a workspace that has no CONFIG_JSON yet.
        addHooks(List.of(hook("log_tools", null)));

        try (MockedStatic<ModelInferenceLogsUtils> mils = Mockito.mockStatic(ModelInferenceLogsUtils.class);
             MockedStatic<SecurityProjectUtils> sec = Mockito.mockStatic(SecurityProjectUtils.class)) {

            mils.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(WORKSPACE_ID)).thenReturn(new HashMap<>());
            mils.when(() -> ModelInferenceLogsUtils.getWorkspaceConfigJson(WORKSPACE_ID)).thenReturn(null);
            sec.when(() -> SecurityProjectUtils.userCanEditProject(eq(user), eq(WORKSPACE_ID))).thenReturn(true);

            NounMetadata result = reactor.execute();

            JSONObject cfg = new JSONObject((String) result.getValue());
            assertEquals(1, cfg.getInt("schema_version"));
            assertEquals(1, cfg.getJSONArray("hooks").length());
            assertEquals("log_tools", cfg.getJSONArray("hooks").getJSONObject(0).getString("kind"));
        }
    }

    // ---------- validation: kind ----------

    @Test
    void rejectsHookWithMissingKind() {
        Map<String, Object> bad = new LinkedHashMap<>();
        bad.put("pixel", "Date();");
        addHooks(List.of(bad));

        try (MockedStatic<ModelInferenceLogsUtils> mils = Mockito.mockStatic(ModelInferenceLogsUtils.class);
             MockedStatic<SecurityProjectUtils> sec = Mockito.mockStatic(SecurityProjectUtils.class)) {

            mils.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(WORKSPACE_ID)).thenReturn(new HashMap<>());
            sec.when(() -> SecurityProjectUtils.userCanEditProject(eq(user), eq(WORKSPACE_ID))).thenReturn(true);

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> reactor.execute());
            assertTrue(ex.getMessage().contains("kind"));
        }
    }

    @Test
    void rejectsHookWithUnknownKind() {
        addHooks(List.of(hook("not_a_real_kind", null)));

        try (MockedStatic<ModelInferenceLogsUtils> mils = Mockito.mockStatic(ModelInferenceLogsUtils.class);
             MockedStatic<SecurityProjectUtils> sec = Mockito.mockStatic(SecurityProjectUtils.class)) {

            mils.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(WORKSPACE_ID)).thenReturn(new HashMap<>());
            sec.when(() -> SecurityProjectUtils.userCanEditProject(eq(user), eq(WORKSPACE_ID))).thenReturn(true);

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> reactor.execute());
            assertTrue(ex.getMessage().contains("not_a_real_kind"));
            assertTrue(ex.getMessage().contains("Known kinds"));
        }
    }

    // ---------- validation: pixel kind ----------

    @Test
    void rejectsPixelKindWithMissingPixelField() {
        Map<String, Object> bad = new LinkedHashMap<>();
        bad.put("kind", "pixel");
        // no pixel field
        addHooks(List.of(bad));

        try (MockedStatic<ModelInferenceLogsUtils> mils = Mockito.mockStatic(ModelInferenceLogsUtils.class);
             MockedStatic<SecurityProjectUtils> sec = Mockito.mockStatic(SecurityProjectUtils.class)) {

            mils.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(WORKSPACE_ID)).thenReturn(new HashMap<>());
            sec.when(() -> SecurityProjectUtils.userCanEditProject(eq(user), eq(WORKSPACE_ID))).thenReturn(true);

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> reactor.execute());
            assertTrue(ex.getMessage().contains("pixel"));
            assertTrue(ex.getMessage().contains("non-empty"));
        }
    }

    @Test
    void rejectsPixelKindWithEmptyPixelField() {
        addHooks(List.of(hook("pixel", "   ")));

        try (MockedStatic<ModelInferenceLogsUtils> mils = Mockito.mockStatic(ModelInferenceLogsUtils.class);
             MockedStatic<SecurityProjectUtils> sec = Mockito.mockStatic(SecurityProjectUtils.class)) {

            mils.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(WORKSPACE_ID)).thenReturn(new HashMap<>());
            sec.when(() -> SecurityProjectUtils.userCanEditProject(eq(user), eq(WORKSPACE_ID))).thenReturn(true);

            assertThrows(IllegalArgumentException.class, () -> reactor.execute());
        }
    }

    // ---------- pre-validation gates ----------

    @Test
    void rejectsWhenWorkspaceNotFound() {
        addHooks(List.of(hook("log_tools", null)));

        try (MockedStatic<ModelInferenceLogsUtils> mils = Mockito.mockStatic(ModelInferenceLogsUtils.class)) {
            mils.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(WORKSPACE_ID)).thenReturn(null);

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> reactor.execute());
            assertTrue(ex.getMessage().contains("Workspace not found"));
        }
    }

    @Test
    void rejectsWhenUserCannotEditWorkspace() {
        addHooks(List.of(hook("log_tools", null)));

        try (MockedStatic<ModelInferenceLogsUtils> mils = Mockito.mockStatic(ModelInferenceLogsUtils.class);
             MockedStatic<SecurityProjectUtils> sec = Mockito.mockStatic(SecurityProjectUtils.class)) {

            mils.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(WORKSPACE_ID)).thenReturn(new HashMap<>());
            sec.when(() -> SecurityProjectUtils.userCanEditProject(eq(user), eq(WORKSPACE_ID))).thenReturn(false);

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> reactor.execute());
            assertTrue(ex.getMessage().contains("does not have access"));
        }
    }

    @Test
    void rejectsWhenWorkspaceIdMissing() {
        reactor.keyValue.remove(ReactorKeysEnum.WORKSPACE_ID.getKey());
        addHooks(List.of(hook("log_tools", null)));

        // organizeKeys throws when a required (1) key is missing.
        assertThrows(Exception.class, () -> reactor.execute());
    }

    // ---------- round-tripping of arbitrary kind-specific fields ----------

    @Test
    void preservesArbitraryFieldsOnHookEntry() {
        Map<String, Object> hookWithExtras = new LinkedHashMap<>();
        hookWithExtras.put("kind", "pixel");
        hookWithExtras.put("pixel", "Date();");
        hookWithExtras.put("events", List.of("beforeRun", "afterRun"));
        hookWithExtras.put("customField", "preserved-value");
        addHooks(List.of(hookWithExtras));

        try (MockedStatic<ModelInferenceLogsUtils> mils = Mockito.mockStatic(ModelInferenceLogsUtils.class);
             MockedStatic<SecurityProjectUtils> sec = Mockito.mockStatic(SecurityProjectUtils.class)) {

            mils.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(WORKSPACE_ID)).thenReturn(new HashMap<>());
            mils.when(() -> ModelInferenceLogsUtils.getWorkspaceConfigJson(anyString())).thenReturn(null);
            sec.when(() -> SecurityProjectUtils.userCanEditProject(eq(user), eq(WORKSPACE_ID))).thenReturn(true);

            NounMetadata result = reactor.execute();
            JSONObject cfg = new JSONObject((String) result.getValue());
            JSONObject entry = cfg.getJSONArray("hooks").getJSONObject(0);

            // Arbitrary fields must round-trip so the loader's configure(spec)
            // sees them.
            assertEquals("preserved-value", entry.getString("customField"));
            JSONArray events = entry.getJSONArray("events");
            assertEquals(2, events.length());
            assertEquals("beforeRun", events.getString(0));
            assertEquals("afterRun", events.getString(1));
        }
    }
}
