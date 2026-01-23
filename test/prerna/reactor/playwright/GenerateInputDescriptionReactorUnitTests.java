package prerna.reactor.playwright;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.microsoft.playwright.Locator;
import com.microsoft.playwright.Page;

import prerna.auth.User;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.om.Insight;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

@Execution(ExecutionMode.SAME_THREAD)
class GenerateInputDescriptionReactorUnitTests {

    private GenerateInputDescriptionReactor reactor;
    private Map<String, String> keyValues;
    private Insight insight;

    @BeforeEach
    void setUp() {
        reactor = new GenerateInputDescriptionReactor();
        keyValues = reactor.keyValue;
        insight = mock(Insight.class);
        reactor.setInsight(insight);
    }

    @Test
    void execute_withValidContext_returnsDescription() {
        keyValues.put(ReactorKeysEnum.ENGINE.getKey(), "engine-1");
        keyValues.put("sessionId", "sess-1");
        keyValues.put("selector", "#username");
        keyValues.put("tabId", "tab-1");

        User user = mock(User.class);
        PlaywrightSession session = mock(PlaywrightSession.class);
        session.tabPages = new HashMap<>();
        Page page = mock(Page.class);
        session.tabPages.put("tab-1", page);

        when(insight.getUser()).thenReturn(user);
        when(user.getPlaywrightSession("sess-1")).thenReturn(session);
        when(page.title()).thenReturn("Account Settings");

        Locator locator = mock(Locator.class);
        when(page.locator("#username")).thenReturn(locator);

        Map<String, Object> domContext = buildDomContext();
        when(locator.evaluate(any())).thenReturn(domContext);

        IModelEngine modelEngine = mock(IModelEngine.class);
        Room room = mock(Room.class);
        ResponseMessage response = mock(ResponseMessage.class);
        when(response.getContent()).thenReturn("Friendly description");
        when(room.ask(any(InputMessage.class), eq(modelEngine))).thenReturn(response);

        try (MockedStatic<Utility> utilityMock = Mockito.mockStatic(Utility.class);
                MockedStatic<RoomUtils> roomUtilsMock = Mockito.mockStatic(RoomUtils.class)) {
            utilityMock.when(() -> Utility.getModel("engine-1")).thenReturn(modelEngine);
            roomUtilsMock.when(() -> RoomUtils.createRoomIfNotExists(Mockito.anyString(), eq(insight), eq(modelEngine), Mockito.isNull()))
                    .thenReturn(room);

            NounMetadata result = reactor.execute();
            assertEquals(PixelDataType.CONST_STRING, result.getNounType());
            assertEquals("Friendly description", result.getValue());
        }
    }

    @Test
    void execute_withMissingTab_throwsException() {
        keyValues.put(ReactorKeysEnum.ENGINE.getKey(), "engine-1");
        keyValues.put("sessionId", "sess-1");
        keyValues.put("selector", "#username");
        keyValues.put("tabId", "tab-404");

        User user = mock(User.class);
        PlaywrightSession session = mock(PlaywrightSession.class);
        session.tabPages = new HashMap<>();
        when(insight.getUser()).thenReturn(user);
        when(user.getPlaywrightSession("sess-1")).thenReturn(session);

        assertThrows(IllegalArgumentException.class, () -> reactor.execute());
    }

    private Map<String, Object> buildDomContext() {
        Map<String, Object> domContext = new HashMap<>();
        domContext.put("labelText", "Username");

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("placeholder", "Enter username");
        attributes.put("type", "text");
        attributes.put("required", true);

        Map<String, Object> input = new HashMap<>();
        input.put("attributes", attributes);
        input.put("outerHTML", "<input id='username' />");
        domContext.put("input", input);

        domContext.put("nearbyText", List.of("Account", "Profile"));
        domContext.put("headingText", "Profile Info");
        domContext.put("fieldsetLegend", "Credentials");
        domContext.put("ancestry", List.of(Map.of("outerHTML", "<div class='field'></div>")));
        domContext.put("form", Map.of("name", "profileForm"));
        return domContext;
    }
}
