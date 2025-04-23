package prerna.engine.impl.model.responses;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class AskStringModelEngineResponseUnitTests {
    private AskStringModelEngineResponse reactor;

    @Test
    void constructorTest() {
        AskStringModelEngineResponse ans = new AskStringModelEngineResponse("response", 1, 1);

        assertNotNull(ans);
        assertEquals("response", ans.getStringResponse());
        assertEquals(1, (int) ans.getNumberOfTokensInPrompt());
        assertEquals(1, (int) ans.getNumberOfTokensInResponse());
    }

    @Test
    void fromNull() {
        reactor = new AskStringModelEngineResponse("", 0, 0);
        AskStringModelEngineResponse ans = reactor.fromJson(null);

        assertNull(ans);
    }

    @Test
    void fromJson() {
        JSONObject obj = new JSONObject();
        obj.put("output", new JSONObject());
        obj.put("input_tokens", 1);
        obj.put("output_tokens", 1);

        reactor = new AskStringModelEngineResponse("", 0, 0);
        AskStringModelEngineResponse ans = reactor.fromJson(obj);

        assertNotNull(ans);
        assertEquals("{}", ans.getResponse());
        assertEquals(1, (int) ans.getNumberOfTokensInPrompt());
        assertEquals(1, (int) ans.getNumberOfTokensInResponse());
    }

    @Test
    void fromJsonOutputString() {
        JSONObject obj = new JSONObject();
        obj.put("output", "output");

        reactor = new AskStringModelEngineResponse("", 0, 0);
        AskStringModelEngineResponse ans = reactor.fromJson(obj);

        assertNotNull(ans);
        assertEquals("output", ans.getResponse());
    }

    @Test
    void fromJsonNoOutput() {
        JSONObject obj = new JSONObject();

        reactor = new AskStringModelEngineResponse("", 0, 0);
        AskStringModelEngineResponse ans = reactor.fromJson(obj);

        assertNotNull(ans);
        assertEquals("", ans.getResponse());
    }
}
