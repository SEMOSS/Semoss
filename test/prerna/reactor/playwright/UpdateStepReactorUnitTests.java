package prerna.reactor.playwright;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

class UpdateStepReactorUnitTests {

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private Insight insight;
	private User user;

	@BeforeEach
	void setUpMocks() {
		insight = mock(Insight.class);
		user = mock(User.class);
		when(insight.getUser()).thenReturn(user);
	}

	@Test
	void updatesStepFields() {
		String sessionId = "session-update";
		String tabId = "tab-update";
		PlaywrightSession session = sessionWithTab(tabId);
		List<List<PlaywrightStep>> history = session.history.steps().get(tabId);
		List<PlaywrightStep> page = new LinkedList<>();
		PlaywrightStep existing = baseStep(1, false, "Old Label", "old text", true, "old description");
		page.add(existing);
		history.add(page);

		PlaywrightStep updateInput = new PlaywrightStep(existing.id(), existing.type(), existing.url(), existing.coords(),
				existing.multiCoords(), existing.prompt(), "new text", existing.pressEnter(), existing.deltaY(),
				existing.waitUntil(), existing.waitAfterMs(), existing.viewport(), existing.timestamp(), "New Label",
				"New description", existing.isPassword(), true, existing.selector(), existing.isTriggerNewTab(),
				Boolean.FALSE, Boolean.TRUE, existing.sendToPlayground(), existing.tag());

		UpdateStepReactor reactor = configuredReactor(sessionId, tabId, session, List.of(updateInput));
		ScreenshotResponse screenshot = new ScreenshotResponse("updated", 100, 200, 1.0);

		try (MockedStatic<ScreenshotReactor> screenshotMock = Mockito.mockStatic(ScreenshotReactor.class)) {
			screenshotMock.when(() -> ScreenshotReactor.screenshot(session, tabId)).thenReturn(screenshot);

			NounMetadata metadata = reactor.execute();
			@SuppressWarnings("unchecked")
			Map<String, Object> result = (Map<String, Object>) metadata.getValue();
			assertSame(screenshot, result.get("screenshot"));

			@SuppressWarnings("unchecked")
			List<PlaywrightStep> updated = (List<PlaywrightStep>) result.get("updatedSteps");
			assertEquals(1, updated.size());
			PlaywrightStep stored = history.get(0).get(0);
			assertEquals("New Label", stored.label());
			assertEquals("new text", stored.text());
			assertTrue(stored.storeValue());
		}
	}

	@Test
	void NoUpdateWhenStoreValueFalse() {
		String sessionId = "session-password";
		String tabId = "tab-password";
		PlaywrightSession session = sessionWithTab(tabId);
		List<PlaywrightStep> page = new LinkedList<>();
		PlaywrightStep existing = baseStep(2, true, "Pwd Label", "secret", false, "hidden");
		page.add(existing);
		session.history.steps().get(tabId).add(page);

		PlaywrightStep updateInput = new PlaywrightStep(existing.id(), existing.type(), existing.url(), existing.coords(),
				existing.multiCoords(), existing.prompt(), "should-not-appear", existing.pressEnter(), existing.deltaY(),
				existing.waitUntil(), existing.waitAfterMs(), existing.viewport(), existing.timestamp(), "Pwd Label",
				"Updated description", existing.isPassword(), true, existing.selector(), existing.isTriggerNewTab(),
				Boolean.TRUE, Boolean.FALSE, existing.sendToPlayground(), existing.tag());

		UpdateStepReactor reactor = configuredReactor(sessionId, tabId, session, List.of(updateInput));
		ScreenshotResponse screenshot = new ScreenshotResponse("pwd", 80, 120, 1.0);

		try (MockedStatic<ScreenshotReactor> screenshotMock = Mockito.mockStatic(ScreenshotReactor.class)) {
			screenshotMock.when(() -> ScreenshotReactor.screenshot(session, tabId)).thenReturn(screenshot);
			reactor.execute();
		}

		PlaywrightStep stored = session.history.steps().get(tabId).get(0).get(0);
		assertEquals("", stored.text());
		assertEquals("Updated description", stored.description());
		assertTrue(!stored.storeValue());
	}

	@Test
	void throwsWhenStepIdMissing() {
		String sessionId = "session-missing";
		String tabId = "tab-missing";
		PlaywrightSession session = sessionWithTab(tabId);
		UpdateStepReactor reactor = configuredReactor(sessionId, tabId, session,
				List.of(baseStep(99, false, "Missing", "", true, "")));

		assertThrows(IllegalArgumentException.class, reactor::execute);
	}

	private static PlaywrightStep baseStep(int id, boolean isPassword, String label, String text, boolean storeValue,
			String description) {
		return new PlaywrightStep(id, PlaywrightStepType.CLICK, "http://example", new Coords(1, 1),
				List.of(new Coords(1, 1)), "prompt", text, Boolean.FALSE, 0, null, 0,
				new Viewport(1280, 720, 1.0), System.currentTimeMillis(), label, description, isPassword, storeValue,
				new Selector("css", "#el"), new TriggerNewTab(false, null), Boolean.TRUE, Boolean.TRUE, Boolean.FALSE,
				"button");
	}

	private UpdateStepReactor configuredReactor(String sessionId, String tabId, PlaywrightSession session,
			List<PlaywrightStep> inputs) {
		UpdateStepReactor reactor = new UpdateStepReactor();
		when(user.getPlaywrightSession(sessionId)).thenReturn(session);
		reactor.setInsight(insight);
		reactor.keyValue.put("sessionId", sessionId);
		reactor.keyValue.put("tabId", tabId);
		reactor.keyValue.put("inputs", "inputs");
		addInputsToStore(reactor, inputs);
		return reactor;
	}

	private static void addInputsToStore(UpdateStepReactor reactor, List<PlaywrightStep> inputs) {
		NounStore store = new NounStore("update-step-test");
		GenRowStruct grs = new GenRowStruct();
		for (PlaywrightStep step : inputs) {
			Map<String, Object> map = MAPPER.convertValue(step, new TypeReference<Map<String, Object>>() {
			});
			grs.add(new NounMetadata(map, PixelDataType.MAP));
		}
		store.addNoun("inputs", grs);
		reactor.setNounStore(store);
	}

	private static PlaywrightSession sessionWithTab(String tabId) {
		PlaywrightSession session = Mockito.mock(PlaywrightSession.class,
				Mockito.withSettings().defaultAnswer(Answers.CALLS_REAL_METHODS));
		Map<String, List<List<PlaywrightStep>>> steps = new LinkedHashMap<>();
		steps.put(tabId, new LinkedList<>());
		session.history = new StepsEnvelope("1", PlaywrightSession.newMeta(""), steps);
		session.tabPages = new HashMap<>();
		session.tabCurrentPageIndex = new HashMap<>();
		session.tabCurrentStepIndex = new HashMap<>();
		setPrivateField(session, "parentChildMap", new HashMap<String, List<String>>());
		return session;
	}

	private static void setPrivateField(Object target, String fieldName, Object value) {
		try {
			Field field = PlaywrightSession.class.getDeclaredField(fieldName);
			field.setAccessible(true);
			field.set(target, value);
		} catch (ReflectiveOperationException e) {
			throw new IllegalStateException(e);
		}
	}
}
