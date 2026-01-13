package prerna.reactor.playwright;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
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
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

class StepReactorUnitTests {

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
	void executeStepAppendsToHistoryWhenNoIndex() {
		String sessionId = "session-default";
		String tabId = "tab-default";
		PlaywrightSession session = sessionWithTab(tabId);
		List<List<PlaywrightStep>> history = session.history.steps().get(tabId);
		history.add(new LinkedList<>());
		PlaywrightStep step = clickStep();
		StepReactor reactor = configuredReactor(sessionId, tabId, session, step, "true", null);
		Map<String, Object> utilityResponse = Map.of("isPageChanged", Boolean.FALSE, "isNewTab", Boolean.FALSE);

		ScreenshotResponse screenshot = new ScreenshotResponse("default", 320, 180, 1.0);

		try (MockedStatic<PlaywrightSessionUtility> utility = Mockito.mockStatic(PlaywrightSessionUtility.class);
				MockedStatic<ScreenshotReactor> screenshotMock = Mockito.mockStatic(ScreenshotReactor.class)) {
			utility.when(() -> PlaywrightSessionUtility.applyStep(session, step, tabId)).thenReturn(utilityResponse);
			screenshotMock.when(() -> ScreenshotReactor.screenshot(session, tabId)).thenReturn(screenshot);

			NounMetadata metadata = reactor.execute();
			assertSame(screenshot, ((Map<?, ?>) metadata.getValue()).get("screenshot"));
		}

		assertEquals(2, history.size());
		assertEquals(0, history.get(0).size());
		assertEquals(1, history.get(1).size());
		assertEquals(1, history.get(1).get(0).id());
	}

	@Test
	void executeContextStepAddsHistoryAndReturnsScreenshot() {
		String sessionId = "session-ctx";
		String tabId = "tab-ctx";
		PlaywrightSession session = sessionWithTab(tabId);
		PlaywrightStep step = contextStep();
		StepReactor reactor = configuredReactor(sessionId, tabId, session, step, "true", null);
		ScreenshotResponse screenshot = new ScreenshotResponse("ctx", 200, 200, 2.0);

		try (MockedStatic<ScreenshotReactor> screenshotMock = Mockito.mockStatic(ScreenshotReactor.class)) {
			screenshotMock.when(() -> ScreenshotReactor.screenshot(session, tabId)).thenReturn(screenshot);
			NounMetadata metadata = reactor.execute();
			assertSame(screenshot, ((Map<?, ?>) metadata.getValue()).get("screenshot"));
			screenshotMock.verify(() -> ScreenshotReactor.screenshot(session, tabId), times(1));
		}

		List<List<PlaywrightStep>> history = session.history.steps().get(tabId);
		assertEquals(1, history.size());
		assertEquals(1, history.get(0).size());
		assertEquals(1, history.get(0).get(0).id());
		assertEquals(1, session.lastStepId);
		assertEquals(1, responseMap(reactor).get("stepId"));
	}

	@Test
	void executeContextStepRequiresPromptAndCoordinates() {
		String sessionId = "session-ctx-bad";
		String tabId = "tab-ctx-bad";
		PlaywrightSession session = sessionWithTab(tabId);
		PlaywrightStep invalid = new PlaywrightStep(0, PlaywrightStepType.CONTEXT, null, null, List.of(), "", null, null,
				null, null, null, null, 0L, null, null, false, false, null, null, Boolean.TRUE, Boolean.TRUE, Boolean.FALSE,
				null);
		StepReactor reactor = configuredReactor(sessionId, tabId, session, invalid, "true", null);

		assertThrows(IllegalArgumentException.class, reactor::execute);
	}

	@Test
	void executeStepWithNewTabDetails() {
		String sessionId = "session-step";
		String tabId = "tab-step";
		PlaywrightSession session = sessionWithTab(tabId);
		PlaywrightStep step = clickStep();
		StepReactor reactor = configuredReactor(sessionId, tabId, session, step, "true", null);
		Map<String, Object> utilityResponse = new HashMap<>();
		utilityResponse.put("isPageChanged", Boolean.TRUE);
		utilityResponse.put("isNewTab", Boolean.TRUE);
		utilityResponse.put("newTabId", "tab-2");
		utilityResponse.put("tabTitle", "Results");

		ScreenshotResponse screenshot = new ScreenshotResponse("new-tab", 400, 250, 1.5);

		try (MockedStatic<PlaywrightSessionUtility> utility = Mockito.mockStatic(PlaywrightSessionUtility.class);
				MockedStatic<ScreenshotReactor> screenshotMock = Mockito.mockStatic(ScreenshotReactor.class)) {
			utility.when(() -> PlaywrightSessionUtility.applyStep(session, step, tabId)).thenReturn(utilityResponse);
			screenshotMock.when(() -> ScreenshotReactor.screenshot(session, "tab-2")).thenReturn(screenshot);

			NounMetadata metadata = reactor.execute();
			assertSame(screenshot, ((Map<?, ?>) metadata.getValue()).get("screenshot"));
			utility.verify(() -> PlaywrightSessionUtility.applyStep(session, step, tabId), times(1));
			screenshotMock.verify(() -> ScreenshotReactor.screenshot(session, "tab-2"), times(1));
		}

		List<List<PlaywrightStep>> history = session.history.steps().get(tabId);
		assertEquals(1, history.size());
		PlaywrightStep stored = history.get(0).get(0);
		assertTrue(stored.isTriggerNewTab().isTrue());
		assertEquals("tab-2", stored.isTriggerNewTab().tabId());
		assertEquals(1, session.getChildTabs(tabId).size());
		assertEquals(Boolean.TRUE, responseMap(reactor).get("isNewTab"));
		assertEquals("tab-2", responseMap(reactor).get("newTabId"));
		assertEquals("Results", responseMap(reactor).get("tabTitle"));
	}

	

	@Test
	void executeStepInsertsAtExplicitIndex() {
		String sessionId = "session-insert";
		String tabId = "tab-insert";
		PlaywrightSession session = sessionWithTab(tabId);
		session.history.steps().get(tabId).add(new LinkedList<>());
		session.history.steps().get(tabId).add(new LinkedList<>());
		PlaywrightStep step = clickStep();
		StepReactor reactor = configuredReactor(sessionId, tabId, session, step, "true", Map.of("stepIndex", "1"));
		Map<String, Object> utilityResponse = Map.of("isPageChanged", Boolean.FALSE, "isNewTab", Boolean.FALSE);

		try (MockedStatic<PlaywrightSessionUtility> utility = Mockito.mockStatic(PlaywrightSessionUtility.class);
				MockedStatic<ScreenshotReactor> screenshotMock = Mockito.mockStatic(ScreenshotReactor.class)) {
			utility.when(() -> PlaywrightSessionUtility.applyStep(session, step, tabId)).thenReturn(utilityResponse);
			screenshotMock.when(() -> ScreenshotReactor.screenshot(session, tabId))
					.thenReturn(new ScreenshotResponse("existing", 100, 100, 1.0));

			reactor.execute();
		}

		List<List<PlaywrightStep>> history = session.history.steps().get(tabId);
		assertEquals(3, history.size());
		assertEquals(1, history.get(1).size());
		assertEquals(1, responseMap(reactor).get("insertedAtStep"));
	}

	@Test
	void executeStepAppendsToHistoryWhenIndexOutOfBounds() {
		String sessionId = "session-append";
		String tabId = "tab-append";
		PlaywrightSession session = sessionWithTab(tabId);
		session.history.steps().get(tabId).add(new LinkedList<>());
		PlaywrightStep step = clickStep();
		StepReactor reactor = configuredReactor(sessionId, tabId, session, step, "true", Map.of("stepIndex", "10"));
		Map<String, Object> utilityResponse = Map.of("isPageChanged", Boolean.FALSE, "isNewTab", Boolean.FALSE);

		try (MockedStatic<PlaywrightSessionUtility> utility = Mockito.mockStatic(PlaywrightSessionUtility.class);
				MockedStatic<ScreenshotReactor> screenshotMock = Mockito.mockStatic(ScreenshotReactor.class)) {
			utility.when(() -> PlaywrightSessionUtility.applyStep(session, step, tabId)).thenReturn(utilityResponse);
			screenshotMock.when(() -> ScreenshotReactor.screenshot(session, tabId))
					.thenReturn(new ScreenshotResponse("append", 90, 90, 1.0));

			reactor.execute();
		}

		List<List<PlaywrightStep>> history = session.history.steps().get(tabId);
		assertEquals(2, history.size());
		assertEquals(Boolean.TRUE, responseMap(reactor).get("appendedDueToOutOfBounds"));
		assertEquals(1, responseMap(reactor).get("insertedAtStep"));
	}

	@Test
	void executeTypeStepAndNoSaveTextWhenShouldStoreFalse() {
		String sessionId = "session-type";
		String tabId = "tab-type";
		PlaywrightSession session = sessionWithTab(tabId);
		PlaywrightStep step = typeStep("sensitive");
		StepReactor reactor = configuredReactor(sessionId, tabId, session, step, "false", null);
		Map<String, Object> utilityResponse = Map.of("isPageChanged", Boolean.FALSE, "isNewTab", Boolean.FALSE);

		try (MockedStatic<PlaywrightSessionUtility> utility = Mockito.mockStatic(PlaywrightSessionUtility.class);
				MockedStatic<ScreenshotReactor> screenshotMock = Mockito.mockStatic(ScreenshotReactor.class)) {
			utility.when(() -> PlaywrightSessionUtility.applyStep(session, step, tabId)).thenReturn(utilityResponse);
			screenshotMock.when(() -> ScreenshotReactor.screenshot(session, tabId))
					.thenReturn(new ScreenshotResponse("type", 80, 80, 1.0));

			reactor.execute();
		}

		PlaywrightStep stored = session.history.steps().get(tabId).get(0).get(0);
		assertEquals("", stored.text());
	}

	private static Map<String, Object> toParamMap(PlaywrightStep step) {
		return MAPPER.convertValue(step, new TypeReference<Map<String, Object>>() {
		});
	}

	private static void primeParamStore(StepReactor reactor, Map<String, Object> params) {
		NounStore store = new NounStore("step-reactor-test");
		GenRowStruct grs = new GenRowStruct();
		grs.add(new NounMetadata(params, PixelDataType.MAP));
		store.addNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey(), grs);
		reactor.setNounStore(store);
	}

	private StepReactor configuredReactor(String sessionId, String tabId, PlaywrightSession session,
			PlaywrightStep step, String shouldStore, Map<String, String> extraKeys) {
		StepReactor reactor = new StepReactor();
		when(user.getPlaywrightSession(sessionId)).thenReturn(session);
		reactor.setInsight(insight);
		reactor.keyValue.put("sessionId", sessionId);
		reactor.keyValue.put("tabId", tabId);
		reactor.keyValue.put("shouldStore", shouldStore);
		if (extraKeys != null) {
			extraKeys.forEach(reactor.keyValue::put);
		}
		primeParamStore(reactor, toParamMap(step));
		return reactor;
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

	private static PlaywrightStep contextStep() {
		return new PlaywrightStep(0, PlaywrightStepType.CONTEXT, "http://example", new Coords(10, 10),
				List.of(new Coords(10, 10)), "Summarize", "", Boolean.FALSE, null, null, 0,
				new Viewport(1280, 720, 1.0), System.currentTimeMillis(), "Context", "desc", false, false,
				new Selector("css", "#root"), new TriggerNewTab(false, null), Boolean.TRUE, Boolean.TRUE, Boolean.FALSE,
				"div");
	}

	private static PlaywrightStep clickStep() {
		return new PlaywrightStep(0, PlaywrightStepType.CLICK, "http://example", new Coords(100, 200),
				List.of(new Coords(100, 200)), "", "", Boolean.FALSE, null, null, 0,
				new Viewport(1280, 720, 1.0), System.currentTimeMillis(), "Click", "desc", false, false,
				new Selector("css", "#button"), new TriggerNewTab(false, null), Boolean.TRUE, Boolean.TRUE,
				Boolean.FALSE, "button");
	}

	private static PlaywrightStep typeStep(String text) {
		return new PlaywrightStep(0, PlaywrightStepType.TYPE, "http://example", new Coords(50, 60),
				List.of(new Coords(50, 60)), "", text, Boolean.TRUE, null, null, 0,
				new Viewport(1280, 720, 1.0), System.currentTimeMillis(), "Type", "desc", true, true,
				new Selector("css", "#input"), new TriggerNewTab(false, null), Boolean.TRUE, Boolean.TRUE,
				Boolean.FALSE, "input");
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> responseMap(StepReactor reactor) {
		try {
			Field field = StepReactor.class.getDeclaredField("response");
			field.setAccessible(true);
			return (Map<String, Object>) field.get(reactor);
		} catch (ReflectiveOperationException e) {
			throw new IllegalStateException(e);
		}
	}
}
