package prerna.reactor.playwright;

import java.util.List;

public record PlaywrightStep(int id, PlaywrightStepType type, String url, // for NAVIGATE
		Coords coords, // for CLICK/TYPE/SCROLL
		List<Coords> multiCoords, // for CONTEXT
		String prompt, // for CONTEXT
		String text, // for TYPE
		Boolean pressEnter, // for TYPE
		Integer deltaY, // for SCROLL
		String waitUntil, // for NAVIGATE
		Integer waitAfterMs, // generic wait after action
		Viewport viewport, // viewport the coords were computed against
		Long timestamp, String label, String description, boolean isPassword, boolean storeValue, Selector selector,
		TriggerNewTab isTriggerNewTab, Boolean shouldRun, Boolean required) {

	PlaywrightStep(PlaywrightStep s, String text) {
		this(s.id, s.type, s.url, s.coords, s.multiCoords, s.prompt, text, s.pressEnter, s.deltaY, s.waitUntil,
				s.waitAfterMs, s.viewport, s.timestamp, s.label, s.description, s.isPassword, s.storeValue, s.selector,
				s.isTriggerNewTab, s.shouldRun, s.required);
	}

	PlaywrightStep(PlaywrightStep s, int id) {
		this(id, s.type, s.url, s.coords, s.multiCoords, s.prompt, s.text, s.pressEnter, s.deltaY, s.waitUntil,
				s.waitAfterMs, s.viewport, s.timestamp, s.label, s.description, s.isPassword, s.storeValue, s.selector,
				s.isTriggerNewTab, s.shouldRun, s.required);
	}

	PlaywrightStep(PlaywrightStep s, String label, String text, boolean storeValue, String description,
			boolean shouldRun, boolean required) {
		this(s.id, s.type, s.url, s.coords, s.multiCoords, s.prompt, text, s.pressEnter, s.deltaY, s.waitUntil,
				s.waitAfterMs, s.viewport, s.timestamp, label, description, s.isPassword, storeValue, s.selector,
				s.isTriggerNewTab, shouldRun, required);
	}
}
