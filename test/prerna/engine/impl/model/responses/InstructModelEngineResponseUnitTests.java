package prerna.engine.impl.model.responses;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

public class InstructModelEngineResponseUnitTests {
    private InstructModelEngineResponse reactor;

    @Test
    void test() {
        Map<String, String> map = new HashMap(){{put("key", "valuye");}};
        List<Map<String,String>> list = new ArrayList(){{add(map);}};

        InstructModelEngineResponse ans = new InstructModelEngineResponse(list, 1, 2);
        ans.setMessageId("messageId");
        ans.setRoomId("roomId");

        assertNotNull(ans);
        assertEquals("[{key=valuye}]", ans.getResponse().toString());
        assertEquals("messageId", ans.getMessageId());
        assertEquals("roomId", ans.getRoomId());
        assertEquals("{numberOfTokensInResponse=2, numberOfTokensInPrompt=1, response=[{key=valuye}], messageId=messageId, roomId=roomId}", ans.toMap().toString());
        assertEquals(1, (int) ans.getNumberOfTokensInPrompt());
        assertEquals(2, (int) ans.getNumberOfTokensInResponse());
    }

    @Test
    void fromMapException() {
        Map<String, Object> map = new HashMap();
        map.put("response", 1);
        map.put("numberOfTokensInPrompt", 1);
        map.put("numberOfTokensInResponse", 2);

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> {
            reactor.fromMap(map);
        });
        assertEquals("Invalid response type: class java.lang.Integer", e.getMessage());
    }

    @Test
    void fromObj() {
        Map<String, Object> map = new HashMap();
        map.put("response", ((List)(new ArrayList(){{
            add((Map)(new HashMap(){{
                put("key", "valuye");
            }}));
        }})));
        map.put("numberOfTokensInPrompt", 1);
        map.put("numberOfTokensInResponse", 2);

        InstructModelEngineResponse ans = reactor.fromObject(map);

        assertNotNull(ans);
        assertEquals("[{key=valuye}]", ans.getResponse().toString());
        assertEquals(1, (int) ans.getNumberOfTokensInPrompt());
        assertEquals(2, (int) ans.getNumberOfTokensInResponse());
    }

    @Test
    void fromObjException() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> {
            reactor.fromObject(1);
        });
        assertEquals("Expected a Map<String, Object> but got: class java.lang.Integer", e.getMessage());
    }
}
