/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
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

	@Test
	void automationStateMetadataRequiresAValidLiveTabId() {
		RemoteBrowserInputEvent event = new RemoteBrowserInputEvent();
		event.setType("fill-element");
		event.setText("value");
		event.setSelector(new prerna.reactor.playwright.Selector("css", "input", null));
		event.setExpectedUrl("https://example.com");
		event.setExpectedTabId("tab-1");
		assertDoesNotThrow(() -> RemoteBrowserInputEventValidator.validate(event, 100, 100));

		event.setExpectedTabId("first-tab");
		assertThrows(IllegalArgumentException.class, () -> RemoteBrowserInputEventValidator.validate(event, 100, 100));
	}

	@Test
	void newTabAllowsAnOptionalRecordedTabBinding() {
		RemoteBrowserInputEvent event = new RemoteBrowserInputEvent();
		event.setType("new-tab");
		assertDoesNotThrow(() -> RemoteBrowserInputEventValidator.validate(event, 100, 100));

		event.setTargetTabId("tab-2");
		assertDoesNotThrow(() -> RemoteBrowserInputEventValidator.validate(event, 100, 100));

		event.setTargetTabId("invalid");
		assertThrows(IllegalArgumentException.class, () -> RemoteBrowserInputEventValidator.validate(event, 100, 100));
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
