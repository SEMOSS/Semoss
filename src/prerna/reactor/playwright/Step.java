package prerna.reactor.playwright;

import java.util.List;

public record Step(int id, StepType type, String url, // for NAVIGATE
		Coords coords, // for CLICK/TYPE/SCROLL
		List<Coords> multiCoords, // for CONTEXT
		String prompt, // for CONTEXT
		String text, // for TYPE
		Boolean pressEnter, // for TYPE
		Integer deltaY, // for SCROLL
		String waitUntil, // for NAVIGATE
		Integer waitAfterMs, // generic wait after action
		Viewport viewport, // viewport the coords were computed against
		Long timestamp, String label, boolean isPassword, boolean storeValue, Selector selector,
		TriggerNewTab isTriggerNewTab) {

	public Step(Step s, String text) {
		this(s.id, s.type, s.url, s.coords, s.multiCoords, s.prompt, text, s.pressEnter, s.deltaY, s.waitUntil,
				s.waitAfterMs, s.viewport, s.timestamp, s.label, s.isPassword, s.storeValue, s.selector,
				s.isTriggerNewTab);
	}

	public Step(Step s, int id) {
		this(id, s.type, s.url, s.coords, s.multiCoords, s.prompt, s.text, s.pressEnter, s.deltaY, s.waitUntil,
				s.waitAfterMs, s.viewport, s.timestamp, s.label, s.isPassword, s.storeValue, s.selector,
				s.isTriggerNewTab);
	}

	public Step(Step s, String label, String text, boolean storeValue) {
		this(s.id, s.type, s.url, s.coords, s.multiCoords, s.prompt, text, s.pressEnter, s.deltaY, s.waitUntil,
				s.waitAfterMs, s.viewport, s.timestamp, label, s.isPassword, storeValue, s.selector, s.isTriggerNewTab);
	}
}