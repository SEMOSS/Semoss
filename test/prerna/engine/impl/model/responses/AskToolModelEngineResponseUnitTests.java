package prerna.engine.impl.model.responses;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

public class AskToolModelEngineResponseUnitTests {
    private AskToolModelEngineResponse reactor;

    @Test
    void test() {
        Map<String, Object> map = new HashMap();
        map.put("id", "id");
        map.put("name", "name");
        map.put("arguments", ((Map) new HashMap(){{put("key", "value");}}).toString());
        
        AskToolModelEngineResponse ans = new AskToolModelEngineResponse(map, 1, 1);

        assertNotNull(ans);
        assertEquals("{\"name\":\"name\",\"arguments\":\"{key=value}\",\"id\":\"id\"}", ans.getStringResponse());
        assertEquals("id", ans.getToolCallId());
        assertEquals("name", ans.getToolCallName());
        assertEquals(1, (int) ans.getNumberOfTokensInPrompt());
        assertEquals(1, (int) ans.getNumberOfTokensInResponse());
    }

    @Test
    void emptyMap() {
        Map<String, Object> map = new HashMap();
        
        AskToolModelEngineResponse ans = new AskToolModelEngineResponse(map, 0, 0);

        assertNotNull(ans);
        assertEquals("{}", ans.getStringResponse());
        assertNull(ans.getToolCallId());
        assertNull(ans.getToolCallName());
    }

    @Test
    void fromMap() {
        Map<String, Object> map = new HashMap();
        map.put("numberOfTokensInPrompt", 1);
        map.put("numberOfTokensInResponse", 2);

        AskToolModelEngineResponse ans = reactor.fromMap(map);

        assertNotNull(ans);
        assertEquals(1, (int) ans.getNumberOfTokensInPrompt());
        assertEquals(2, (int) ans.getNumberOfTokensInResponse());
    }

    @Test
    void toolCallArgs() {
        Map<String, Object> map = new HashMap();
        map.put("arguments", ((Map) new HashMap()).toString());

        reactor = new AskToolModelEngineResponse(map, 0, 0);
        String str = reactor.getToolCallArgumentsAsString();

        assertNotNull(str);
        assertTrue(str.equals("{}"));
    }

    // TODO: Try and force a JsonProcessingException
    @Test
    void toolCallProcessingError() {
        Map<String, Object> map = new HashMap();
        map.put("arguments", ((Map) new HashMap()).toString());

        reactor = new AskToolModelEngineResponse(map, 0, 0);
        String str = reactor.getToolCallArgumentsAsString();

        assertNotNull(str);
        assertTrue(str.equals("{}"));
    }

    @Test
    void toolCallArgsNull() {
        Map<String, Object> map = new HashMap();

        reactor = new AskToolModelEngineResponse(map, 0, 0);
        String str = reactor.getToolCallArgumentsAsString();

        assertNotNull(str);
        assertTrue(str.equals("{}"));
    }
}
