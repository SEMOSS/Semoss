package prerna.reactor.playwright;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.engine.api.IModelEngine;
import prerna.om.Insight;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

@Execution(ExecutionMode.SAME_THREAD)
class ImageContextReactorUnitTests {

    private ImageContextReactor reactor;
    private Map<String, String> keyValues;
    private NounStore nounStore;
    private Insight insight;

    @BeforeEach
    void setUp() {
        reactor = new ImageContextReactor();
        keyValues = reactor.keyValue;
        nounStore = new NounStore("image-context");
        reactor.setNounStore(nounStore);
        insight = mock(Insight.class);
        reactor.setInsight(insight);
    }

    @Test
    void execute_returnsModelResponse() {
        keyValues.put("sessionId", "sess-1");
        keyValues.put("tabId", "tab-1");
        keyValues.put(ReactorKeysEnum.ENGINE.getKey(), "engine-1");

        Map<String, Object> params = new HashMap<>();
        params.put("userPrompt", "Summarize this panel");
        params.put("startX", 10);
        params.put("startY", 20);
        params.put("endX", 110);
        params.put("endY", 120);
        addParamValues(params);

        User user = mock(User.class);
        PlaywrightSession session = mock(PlaywrightSession.class);
        when(insight.getUser()).thenReturn(user);
        when(user.getPlaywrightSession("sess-1")).thenReturn(session);
        when(insight.getInsightFolder()).thenReturn("/tmp/insight");

        ScreenshotResponse screenshot = new ScreenshotResponse("b64", 100, 80, 1.0);
        IModelEngine modelEngine = mock(IModelEngine.class);

        try (MockedConstruction<ScreenshotReactor> screenshotMock = Mockito.mockConstruction(ScreenshotReactor.class,
                (mockShot, context) -> when(mockShot.execute()).thenReturn(new NounMetadata(screenshot, PixelDataType.MAP)));
                MockedStatic<Utility> utilityMock = Mockito.mockStatic(Utility.class);
                MockedStatic<PlaywrightUtility> pwMock = Mockito.mockStatic(PlaywrightUtility.class)) {

            utilityMock.when(() -> Utility.getModel("engine-1")).thenReturn(modelEngine);
            pwMock.when(() -> PlaywrightUtility.callModel(anyString(), anyString(), eq(screenshot), eq(modelEngine), anyString(), eq(insight)))
                    .thenReturn("Context summary");

            NounMetadata result = reactor.execute();
            assertEquals(PixelDataType.MAP, result.getNounType());
            @SuppressWarnings("unchecked")
            Map<String, Object> payload = (Map<String, Object>) result.getValue();
            assertEquals("Context summary", payload.get("response"));
        }
    }

    @Test
    void execute_returnsErrorWhenScreenshotFails() {
        keyValues.put("sessionId", "sess-1");
        keyValues.put("tabId", "tab-1");
        keyValues.put(ReactorKeysEnum.ENGINE.getKey(), "engine-1");

        Map<String, Object> params = new HashMap<>();
        params.put("userPrompt", "Describe");
        params.put("startX", 0);
        params.put("startY", 0);
        params.put("endX", 10);
        params.put("endY", 10);
        addParamValues(params);

        User user = mock(User.class);
        PlaywrightSession session = mock(PlaywrightSession.class);
        when(insight.getUser()).thenReturn(user);
        when(user.getPlaywrightSession("sess-1")).thenReturn(session);

        try (MockedConstruction<ScreenshotReactor> screenshotMock = Mockito.mockConstruction(ScreenshotReactor.class,
                (mockShot, context) -> when(mockShot.execute()).thenThrow(new RuntimeException("boom")))) {
            NounMetadata result = reactor.execute();
            @SuppressWarnings("unchecked")
            Map<String, Object> payload = (Map<String, Object>) result.getValue();
            assertTrue(((String) payload.get("response")).startsWith("Error:"));
        }
    }

    private void addParamValues(Map<String, Object> params) {
        GenRowStruct grs = new GenRowStruct();
        grs.add(new NounMetadata(params, PixelDataType.MAP));
        nounStore.addNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey(), grs);
    }
}
