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
 *******************************************************************************/
package prerna.reactor.agent.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.AgentRunContext;
import prerna.reactor.agent.exceptions.AgentInputRequiredException;
import prerna.reactor.agent.run.AgentRunActionStore;
import prerna.reactor.agent.runtime.SemossAgentHarness;
import prerna.reactor.agent.subagent.SubAgentToolSynthesizer;
import prerna.reactor.collaboration.BrainForgetReactor;
import prerna.reactor.collaboration.BrainRememberReactor;
import prerna.reactor.collaboration.WorkComposeEmailReactor;
import prerna.reactor.collaboration.WorkSendEmailReactor;

class NativeToolResourceUriUnitTests {
    @Test
    void collaborationReactorsDeclareOnlyResourceUris() {
        Map<AbstractReactor, String> reactors = Map.of(
            new WorkComposeEmailReactor(), "email-compose",
            new WorkSendEmailReactor(), "email-send",
            new BrainRememberReactor(), "memory",
            new BrainForgetReactor(), "memory");
        reactors.forEach((reactor, name) -> {
            JSONObject meta = reactor.asMcpTool().getJSONObject("_meta");
            JSONObject ui = meta.getJSONObject(MCPUtility.SMSS_MCP_UI);
            assertEquals("component://" + name, ui.getString(MCPUtility.UI_RESOURCE_URI));
            assertFalse(ui.has("component"));
        });
        assertEquals("ask", new WorkSendEmailReactor().asMcpTool().getJSONObject("_meta")
            .getString(MCPUtility.SMSS_MCP_EXECUTION));
    }

    @Test
    void delegationViewsAreExplicitWhileWorkflowKindsRemain() {
        JSONObject request = new JSONObject(SubAgentToolSynthesizer.buildDelegateTool()).getJSONObject("_meta");
        JSONObject submit = new JSONObject(SubAgentToolSynthesizer.buildSubmitDelegationTool("Dana"))
            .getJSONObject("_meta");
        assertEquals("semoss_delegate_to_person", request.getString("SMSS_TOOL_KIND"));
        assertEquals("semoss_submit_delegation", submit.getString("SMSS_TOOL_KIND"));
        assertEquals("Dana", submit.getString("SMSS_DELEGATION_REQUESTER"));
        for (JSONObject meta : List.of(request, submit)) {
            assertEquals("ask", meta.getString(MCPUtility.SMSS_MCP_EXECUTION));
            assertEquals("inline", meta.getJSONObject(MCPUtility.SMSS_MCP_UI)
                .getString(MCPUtility.UI_DISPLAY_LOCATION));
        }
        assertEquals("component://delegation-request", request.getJSONObject(MCPUtility.SMSS_MCP_UI)
            .getString(MCPUtility.UI_RESOURCE_URI));
        assertEquals("component://delegation-submit", submit.getJSONObject(MCPUtility.SMSS_MCP_UI)
            .getString(MCPUtility.UI_RESOURCE_URI));
    }

    @Test
    void metadataBuildersIgnoreLegacyComponentsAndPreserveOtherHints() {
        JSONObject ui = new JSONObject();
        MCPUtility.copyUiHints(Map.of("component", "email-send", "autoOpen", "true"), ui);
        assertFalse(ui.has("component"));
        assertTrue(ui.getBoolean("autoOpen"));
    }

    @Test
    void automaticAndPendingCallsKeepResourceUriConfiguration() throws Exception {
        for (String execution : List.of("auto", "ask")) {
            String uri = "component://email-compose?param1=a%26b&empty=";
            Map<String, Object> meta = Map.of(MCPUtility.SMSS_FUNCTION_NAME, "ComposeEmail",
                MCPUtility.SMSS_MCP_EXECUTION, execution,
                MCPUtility.SMSS_MCP_UI, Map.of("resourceURI", uri, "component", "ignored", "autoOpen", true));
            ResponseMessage response = ResponseMessage.toolResponse(
                new HashMap<>(Map.of("name", "renamed", "arguments", new HashMap<>())));
            restore(response, meta);
            JSONObject restored = new JSONObject(response.getToolResponses().get(0)).getJSONObject("_meta");
            JSONObject ui = restored.getJSONObject(MCPUtility.SMSS_MCP_UI);
            assertEquals(uri, ui.getString("resourceURI"));
            assertFalse(ui.has("component"));
            assertTrue(ui.getBoolean("autoOpen"));
            assertEquals(execution, restored.getString(MCPUtility.SMSS_MCP_EXECUTION));
            ResponseMessage persisted = (ResponseMessage) MessageUtils.fromJson(MessageUtils.toJson(response), null);
            assertEquals(restored.toMap(), persisted.getToolResponses().get(0).get("_meta"));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void pendingActionsPreserveNativeMetadataWithoutCreatingPortalUrls() throws Exception {
        AgentRunContext context = mock(AgentRunContext.class);
        when(context.getRunId()).thenReturn("run");
        when(context.getUserId()).thenReturn("user");
        Room room = mock(Room.class);
        when(room.getId()).thenReturn("room");
        Method persist = SemossAgentHarness.class.getDeclaredMethod("persistPendingActions",
            AgentRunContext.class, Room.class, AgentInputRequiredException.class);
        persist.setAccessible(true);
        for (String uri : List.of("component://delegation-submit",
            "component://mail-compose?provider=google&intent=send",
            "component://unknown", "component://mail/compose", "component:/mail-compose")) {
            Map<String, Object> meta = Map.of(MCPUtility.SMSS_ENGINE_ID, "project",
                MCPUtility.SMSS_MCP_EXECUTION, "ask", MCPUtility.SMSS_MCP_UI, Map.of("resourceURI", uri));
            Map<String, Object> args = Map.of("response", "Reviewed answer");
            AgentInputRequiredException pause = new AgentInputRequiredException("parent", List.of(
                Map.of("id", "call", "name", "renamed", "arguments", args, "_meta", meta)));
            try (MockedStatic<AgentRunActionStore> store = mockStatic(AgentRunActionStore.class)) {
                List<Map<String, Object>> actions =
                    (List<Map<String, Object>>) persist.invoke(null, context, room, pause);
                Map<String, Object> action = actions.get(0);
                assertEquals(meta, action.get("toolMeta"));
                assertEquals(args, action.get("toolArgs"));
                assertEquals("ask", action.get("executionMode"));
                assertNull(action.get("uiUrl"));
                store.verify(() -> AgentRunActionStore.insertPendingActions("run", "room", "user", actions));
            }
        }
    }

    @Test
    void aLegacyComponentDoesNotOptAnAutomaticCallIntoNativeMetadataRestoration() throws Exception {
        ResponseMessage response = ResponseMessage.toolResponse(
            new HashMap<>(Map.of("name", "renamed", "arguments", new HashMap<>())));
        restore(response, Map.of(MCPUtility.SMSS_MCP_EXECUTION, "auto",
            MCPUtility.SMSS_MCP_UI, Map.of("component", "email-compose")));
        assertNull(response.getToolResponses().get(0).get("_meta"));
    }

    private static void restore(ResponseMessage response, Map<String, Object> meta) throws Exception {
        Method method = SemossAgentHarness.class.getDeclaredMethod("restoreAskMetadataForParameterTools",
            ResponseMessage.class, List.class, List.class);
        method.setAccessible(true);
        method.invoke(null, response, List.of(Map.of("name", "renamed", "_meta", meta)), List.of());
    }
}
