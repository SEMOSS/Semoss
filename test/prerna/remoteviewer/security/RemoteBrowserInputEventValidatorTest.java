/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 * Licensed under the Apache License, Version 2.0 (the "License");
 *******************************************************************************/
package prerna.remoteviewer.security;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import prerna.remoteviewer.model.RemoteBrowserInputEvent;

class RemoteBrowserInputEventValidatorTest {

	@Test
	void selectedTextRequiresRequestIdAndCompleteBounds() {
		RemoteBrowserInputEvent event = selection(10, 20, 80, 90);
		assertThrows(IllegalArgumentException.class, () -> RemoteBrowserInputEventValidator.validate(event, 100, 100));

		event.setRequestId("selection-1");
		assertDoesNotThrow(() -> RemoteBrowserInputEventValidator.validate(event, 100, 100));
	}

	@Test
	void selectedTextCoordinatesAreClampedAndTinySelectionsRejected() {
		RemoteBrowserInputEvent event = selection(-20, 30, 180, 120);
		event.setRequestId("selection-2");
		RemoteBrowserInputEventValidator.validate(event, 100, 100);
		assertEquals(0, event.getX());
		assertEquals(30, event.getY());
		assertEquals(100, event.getEndX());
		assertEquals(100, event.getEndY());

		RemoteBrowserInputEvent tiny = selection(10, 10, 11, 11);
		tiny.setRequestId("selection-3");
		assertThrows(IllegalArgumentException.class, () -> RemoteBrowserInputEventValidator.validate(tiny, 100, 100));
	}

	private static RemoteBrowserInputEvent selection(double startX, double startY, double endX, double endY) {
		RemoteBrowserInputEvent event = new RemoteBrowserInputEvent();
		event.setType("selected-text-context");
		event.setX(startX);
		event.setY(startY);
		event.setEndX(endX);
		event.setEndY(endY);
		return event;
	}
}
