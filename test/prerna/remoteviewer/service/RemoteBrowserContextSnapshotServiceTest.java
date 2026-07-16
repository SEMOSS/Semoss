/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 * Licensed under the Apache License, Version 2.0 (the "License");
 *******************************************************************************/
package prerna.remoteviewer.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.reactor.playwright.PlaywrightStep;
import prerna.reactor.playwright.PlaywrightStepType;
import prerna.reactor.playwright.Selector;

class RemoteBrowserContextSnapshotServiceTest {

	@Test
	void removesQueryAndFragmentFromUrls() {
		assertEquals("https://example.com/checkout",
				RemoteBrowserContextSnapshotService.sanitizeUrl("https://example.com/checkout?token=secret#payment"));
	}

	@Test
	void redactsPasswordsAndCollapsesAdjacentScrolls() {
		List<PlaywrightStep> steps = List.of(
				step(1, PlaywrightStepType.NAVIGATE, "https://example.com/login?token=secret", null, null, false,
						false),
				step(2, PlaywrightStepType.TYPE, null, "hunter2", new Selector("css", "input#password"), true,
						true),
				step(3, PlaywrightStepType.TYPE, null, "ibrahim", new Selector("css", "input#username"), false,
						true),
				step(4, PlaywrightStepType.SCROLL, null, null, null, false, false, 300),
				step(5, PlaywrightStepType.SCROLL, null, null, null, false, false, 200));

		List<Map<String, Object>> history = RemoteBrowserContextSnapshotService.normalizeHistory(steps);

		assertEquals(4, history.size());
		assertEquals("https://example.com/login", history.get(0).get("url"));
		assertTrue(Boolean.TRUE.equals(history.get(1).get("sensitive")));
		assertFalse(history.get(1).containsKey("value"));
		assertEquals("ibrahim", history.get(2).get("value"));
		assertEquals(500, history.get(3).get("deltaY"));
		assertEquals(5, history.get(3).get("stepId"));
	}

	@Test
	void renderedContextIsBoundedAndMarksUntrustedContent() {
		Map<String, Object> snapshot = new LinkedHashMap<>();
		snapshot.put("url", "https://example.com/");
		snapshot.put("title", "Example");
		snapshot.put("throughStepId", 1);
		snapshot.put("history", List.of());

		List<Map<String, Object>> elements = new ArrayList<>();
		for (int i = 0; i < 100; i++) {
			Map<String, Object> element = new LinkedHashMap<>();
			element.put("role", "button");
			element.put("name", "Very long action label ".repeat(20) + i);
			element.put("selector", "button:nth-of-type(" + (i + 1) + ")");
			element.put("state", Map.of());
			elements.add(element);
		}
		Map<String, Object> page = new LinkedHashMap<>();
		page.put("elements", elements);
		page.put("headings", List.of());
		page.put("landmarks", List.of());
		page.put("states", List.of());
		page.put("ariaOutline", List.of());
		page.put("candidateCount", 100);
		snapshot.put("page", page);

		String rendered = RemoteBrowserContextSnapshotService.renderContext(snapshot);

		assertTrue(rendered.startsWith("UNTRUSTED WEBSITE DATA"));
		assertTrue(rendered.length() <= RemoteBrowserContextSnapshotService.MAX_TEXT_CHARS);
		assertTrue(rendered.endsWith("[Context truncated to fit the configured limit.]"));
	}

	private static PlaywrightStep step(int id, PlaywrightStepType type, String url, String text, Selector selector,
			boolean password, boolean storeValue) {
		return step(id, type, url, text, selector, password, storeValue, null);
	}

	private static PlaywrightStep step(int id, PlaywrightStepType type, String url, String text, Selector selector,
			boolean password, boolean storeValue, Integer deltaY) {
		return new PlaywrightStep(id, type, url, null, null, null, text, null, deltaY, null, null, null,
				System.currentTimeMillis(), type.name(), null, password, storeValue, selector, null, true, false, false,
				null);
	}
}
