package prerna.engine.impl.model.responses;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class AbstractModelEngineResponseUnitTests {
    private AbstractModelEngineResponse<String> abs;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        abs = mock(AbstractModelEngineResponse.class, Mockito.withSettings()
            .useConstructor("", 1, 1)
            .defaultAnswer(Mockito.CALLS_REAL_METHODS));
    }

    @Test
    void test() {
        abs.setResponse("response");
        abs.setNumberOfTokensInPrompt(1);
        abs.setNumberOfTokensInResponse(1);
        abs.setUsageRestriction(new HashMap<>());

        assertEquals("response", abs.getResponse());
        assertEquals(1, (int) abs.getNumberOfTokensInPrompt());
        assertEquals(1, (int) abs.getNumberOfTokensInResponse());
        assertEquals(new HashMap<>(), abs.getUsageRestriction());
        assertEquals(0, (int) abs.getTokens(new Integer(0)));
        assertEquals(0, (int) abs.getTokens(new Long(0)));
        assertEquals(0, (int) abs.getTokens(new Double(0)));
        assertEquals(0, (int) abs.getTokens(new Float(0.0)));
        assertEquals(0, (int) abs.getTokens("0"));
        assertNull(abs.getTokens(null));
        
        Map<String, Object> map = new HashMap<>();
        map.put("response", "response");
        map.put("getNumberOfTokensInPrompt", 1);
        map.put("numberOfTokensInResponse", 1);
        map.put("usageRestriction", new HashMap<>());
        assertEquals(map.get("response"), abs.toMap().get("response"));
        assertEquals(map.get("numberOfTokensInPrompt"), abs.toMap().get("getNumberOfTokensInPrompt"));
        assertEquals(map.get("numberOfTokensInResponse"), abs.toMap().get("numberOfTokensInResponse"));
        assertEquals(map.get("usageRestriction"), abs.toMap().get("usageRestriction"));
    }
}
