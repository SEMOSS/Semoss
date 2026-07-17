/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 * Licensed under the Apache License, Version 2.0 (the "License");
 *******************************************************************************/
package prerna.remoteviewer.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import prerna.reactor.playwright.PlaywrightStep;
import prerna.reactor.playwright.PlaywrightStepType;
import prerna.remoteviewer.model.RemoteBrowserInputEvent;

class RemoteBrowserRecordingServiceTest {

	@Test
	void mergesOnlyNearbyScrollsWithTheSameDirection() {
		PlaywrightStep down = scroll(120);
		long startedAt = 1_000;

		assertTrue(RemoteBrowserRecordingService.shouldMergeScroll(down, 80, startedAt,
				startedAt + RemoteBrowserRecordingService.SCROLL_BURST_GAP_MS));
		assertFalse(RemoteBrowserRecordingService.shouldMergeScroll(down, -80, startedAt, startedAt + 10));
		assertFalse(RemoteBrowserRecordingService.shouldMergeScroll(down, 80, startedAt,
				startedAt + RemoteBrowserRecordingService.SCROLL_BURST_GAP_MS + 1));
	}

	@Test
	void combinesDistanceWithoutIntegerOverflow() {
		assertEquals(350, RemoteBrowserRecordingService.combineScrollDelta(200, 150));
		assertEquals(Integer.MAX_VALUE,
				RemoteBrowserRecordingService.combineScrollDelta(Integer.MAX_VALUE - 5, 10));
		assertEquals(Integer.MIN_VALUE,
				RemoteBrowserRecordingService.combineScrollDelta(Integer.MIN_VALUE + 5, -10));
	}

	@Test
	void recordsOneSavedAndPreviewStepPerContinuousBurst() {
		RemoteBrowserSession session = recordingSession();
		RemoteBrowserRecordingService.record(session, wheel(20));
		RemoteBrowserRecordingService.record(session, wheel(30));
		RemoteBrowserRecordingService.record(session, wheel(40));

		var saved = session.getRecordingHistory().steps().get("tab-1").get(0);
		assertEquals(1, saved.size());
		assertEquals(90, saved.get(0).deltaY());
		assertEquals(1, session.getRemoteBrowserRecordedSteps().size());
		assertEquals(90, session.getRemoteBrowserRecordedSteps().get(0).getDeltaY());
	}

	@Test
	void splitsBurstsAcrossDirectionChangesAndPauses() {
		RemoteBrowserSession session = recordingSession();
		RemoteBrowserRecordingService.record(session, wheel(50));
		RemoteBrowserRecordingService.record(session, wheel(-20));

		PlaywrightStep last = session.getPendingScrollStep();
		session.setPendingScrollStep(last,
				System.currentTimeMillis() - RemoteBrowserRecordingService.SCROLL_BURST_GAP_MS - 1);
		RemoteBrowserRecordingService.record(session, wheel(-30));

		var saved = session.getRecordingHistory().steps().get("tab-1").get(0);
		assertEquals(3, saved.size());
		assertEquals(50, saved.get(0).deltaY());
		assertEquals(-20, saved.get(1).deltaY());
		assertEquals(-30, saved.get(2).deltaY());
	}

	private static PlaywrightStep scroll(int deltaY) {
		return new PlaywrightStep(1, PlaywrightStepType.SCROLL, null, null, null, null, null, null, deltaY, null,
				null, null, 1_000L, null, null, false, false, null, null, true, false, null, null);
	}

	private static RemoteBrowserSession recordingSession() {
		RemoteBrowserSession session = new RemoteBrowserSession("scroll-test", "test-user", null, 1365, 768);
		session.setRecordingEnabled(true);
		session.clearRecordingBuffer();
		return session;
	}

	private static RemoteBrowserInputEvent wheel(double deltaY) {
		RemoteBrowserInputEvent event = new RemoteBrowserInputEvent();
		event.setType("wheel");
		event.setX(100.0);
		event.setY(200.0);
		event.setDeltaX(0.0);
		event.setDeltaY(deltaY);
		return event;
	}
}
