package prerna.engine.impl.model;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import prerna.auth.User;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.message.*;
import prerna.engine.impl.model.responses.AskStringModelEngineResponse;
import prerna.om.Insight;

class RoomRuntimeContextTest {
    private static final String SYSTEM = "Stable agent instructions. Use the latest SEMOSS runtime status.";

    private Room room() {
        var room = spy(new Room());
        room.setId("runtime-context-test");
        room.setOptionsMap(Map.of("instructions", SYSTEM));
        room.setMessages(new ArrayList<>());
        doReturn(List.of()).when(room).getAllToolsJsonForRoom(anyInt(), anyBoolean());
        return room;
    }

    private Insight insight() {
        var insight = mock(Insight.class);
        var user = mock(User.class, RETURNS_DEEP_STUBS);
        when(user.getPrimaryLoginToken().getId()).thenReturn("fixture-user");
        when(insight.getUser()).thenReturn(user);
        return insight;
    }

    private ResponseMessage calls(Room room, String... ids) {
        List<Map<String, Object>> calls = new ArrayList<>();
        for (String id : ids) calls.add(Map.of("id", id, "name", "ReadFile", "arguments", Map.of("path", id + ".txt")));
        var call = ResponseMessage.toolResponses(calls);
        call.setRoom(room);
        if (!room.getMessages().isEmpty()) call.setParentMessageId(room.getMessages().getLast().getMessageId());
        room.getMessages().add(call);
        return call;
    }

    private InputMessage results(Room room, ResponseMessage call, String... ids) {
        var builder = InputMessage.builder(room).withSystemPrompt(SYSTEM);
        for (String id : ids) builder.withToolResult(id, "ReadFile", "output-" + id, Map.of("path", id + ".txt"), "success", false);
        var input = builder.build(); input.setParentMessageId(call.getMessageId()); input.setVisible(false);
        room.getMessages().add(input);
        return input;
    }

    @Test void successiveRequestsKeepPriorPartsAndAppendStatusAfterAllToolResults() throws Exception {
        var room = room(); var insight = insight(); var engine = mock(IModelEngine.class);
        List<JSONArray> requests = new ArrayList<>();
        when(engine.askRoom(any(), eq(room), anyMap())).thenAnswer(call -> {
            requests.add(new JSONArray((String) ((Map<?, ?>) call.getArgument(2)).get("message_json")));
            return new AskStringModelEngineResponse("Next step", 1, 1);
        });
        var initial = InputMessage.builder(room).withSystemPrompt(SYSTEM)
                .withText("Create slides.\n\n[SEMOSS runtime status]\n40 remaining\n[/SEMOSS runtime status]", "Create slides.").build();
        room.getMessages().add(initial);
        var firstCall = calls(room, "a", "b"); var first = results(room, firstCall, "a", "b");
        try (var store = mockStatic(RoomMessageStore.class); var media = mockStatic(RoomUtils.class)) {
            store.when(() -> RoomMessageStore.currentMessageHistory(room)).thenCallRealMethod();
            room.continueAfterToolExecutionResultsWithRuntimeContext(new HashMap<>(), firstCall.getMessageId(), engine, insight,
                    SYSTEM, "[SEMOSS runtime status]\n39 remaining\n[/SEMOSS runtime status]");
            String priorParts = new JSONArray(requests.getFirst().getJSONObject(requests.getFirst().length()-1).getJSONArray("parts").toString()).toString();
            var secondCall = calls(room, "c"); var second = results(room, secondCall, "c");
            room.continueAfterToolExecutionResultsWithRuntimeContext(new HashMap<>(), secondCall.getMessageId(), engine, insight,
                    SYSTEM, "[SEMOSS runtime status]\n38 remaining; repair rounds remaining: 3\n[/SEMOSS runtime status]");
            assertEquals(SYSTEM, first.getSystemPrompt()); assertEquals(SYSTEM, second.getSystemPrompt());
            assertEquals(List.of(MessagePartType.SYSTEM, MessagePartType.TOOL_RESULT, MessagePartType.TOOL_RESULT, MessagePartType.TEXT),
                    first.getParts().stream().map(MessagePart::getType).toList());
            assertEquals("Create slides.", initial.getInputUIPrompt());
            assertTrue(((TextMessagePart) second.getParts().getLast()).getText().contains("repair rounds remaining: 3"));
            assertEquals(priorParts, requests.get(1).getJSONObject(requests.getFirst().length()-1).getJSONArray("parts").toString());
            for (int i=0; i<requests.getFirst().length(); i++)
                assertEquals(requests.getFirst().getJSONObject(i).getJSONArray("parts").toString(), requests.get(1).getJSONObject(i).getJSONArray("parts").toString());
        }
        // Optional bridge fixture: the Python tests consume the actual Java provider-history serialization.
        String output = System.getProperty("semoss.runtimeContextFixture");
        if (output != null) Files.writeString(Path.of(output), new JSONObject().put("requests", new JSONArray(requests)).toString(2));
    }

    @Test void pendingParallelBatchDoesNotSendStatusOrCallModel() {
        var room = room(); var engine = mock(IModelEngine.class); var call = calls(room, "a", "b");
        var input = results(room, call, "a");
        try (var store = mockStatic(RoomMessageStore.class)) {
            assertNull(room.continueAfterToolExecutionResultsWithRuntimeContext(new HashMap<>(), call.getMessageId(), engine,
                    insight(), SYSTEM, "[SEMOSS runtime status]\n39 remaining\n[/SEMOSS runtime status]"));
            assertFalse(input.hasTextPart());
            verify(engine, never()).askRoom(any(), any(), any());
        }
    }

    @Test void retryOfPendingContinuationDoesNotDuplicateOrRewriteStatus() {
        var room = room(); var engine = mock(IModelEngine.class); var call = calls(room, "a"); var input = results(room, call, "a");
        when(engine.askRoom(any(), any(), anyMap())).thenThrow(new IllegalStateException("offline provider failure"));
        String note = "[SEMOSS runtime status]\n39 remaining\n[/SEMOSS runtime status]";
        try (var store = mockStatic(RoomMessageStore.class)) {
            store.when(() -> RoomMessageStore.currentMessageHistory(room)).thenCallRealMethod();
            for (int i=0; i<2; i++) assertThrows(IllegalStateException.class, () -> room.continueAfterToolExecutionResultsWithRuntimeContext(
                    new HashMap<>(), call.getMessageId(), engine, insight(), SYSTEM, note));
            assertEquals(1, input.getParts().stream().filter(p -> p instanceof TextMessagePart).count());
            assertEquals(SYSTEM, input.getSystemPrompt());
            assertEquals(note, ((TextMessagePart) input.getParts().getLast()).getText());
        }
    }
}
