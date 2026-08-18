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
package prerna.reactor.playwright;

import java.util.List;

/**
 * Represents a single, atomic action or event within a Playwright automation
 * sequence. This record is an immutable data carrier designed to hold all
 * necessary information to execute a specific step.
 *
 * @param id               A unique identifier for the step.
 * @param type             The type of action (e.g., NAVIGATE, CLICK, TYPE,
 *                         SCROLL, WAIT, CONTEXT), defined by
 *                         {@link PlaywrightStepType}.
 * @param url              The URL to navigate to (for NAVIGATE steps).
 * @param coords           Coordinates (x, y) for actions like CLICK, TYPE,
 *                         SCROLL.
 * @param multiCoords      A list of coordinates for CONTEXT steps, possibly for
 *                         defining an area.
 * @param prompt           A prompt string for CONTEXT steps.
 * @param text             The text to type (for TYPE steps).
 * @param pressEnter       A boolean indicating whether to press Enter after
 *                         typing (for TYPE steps).
 * @param deltaY           The vertical scroll amount (for SCROLL steps).
 * @param waitUntil        A condition to wait for after navigation (for
 *                         NAVIGATE steps).
 * @param waitAfterMs      A generic wait duration in milliseconds after an
 *                         action.
 * @param viewport         The viewport dimensions when the coordinates were
 *                         computed against.
 * @param timestamp        The time when the step was recorded or created (epoch
 *                         milliseconds).
 * @param label            A human-readable label for the step.
 * @param description      A more detailed description of the step.
 * @param isPassword       A boolean indicating if the typed text is a password
 *                         (for TYPE steps).
 * @param storeValue       A boolean indicating if the value typed should be
 *                         stored as a variable.
 * @param selector         A {@link Selector} object to identify the target
 *                         element.
 * @param isTriggerNewTab  A {@link TriggerNewTab} object indicating if the
 *                         action is expected to open a new tab.
 * @param shouldRun        A boolean indicating if this step should be executed
 *                         during replay.
 * @param required         A boolean indicating if this step is mandatory.
 * 
 * @param sendToPlayground A boolean indicating if the context should be
 *                         automatically sent to the playground (for CONTEXT
 *                         steps).
 * @param tag              An optional tag associated with the step, the element
 *                         tag from ProbeElement response.
 * @param downloadExpected Whether this step is expected to produce a native
 *                         browser download during replay.
 */
public record PlaywrightStep(int id, PlaywrightStepType type, String url, Coords coords, List<Coords> multiCoords,
		String prompt, String text, Boolean pressEnter, Integer deltaY, String waitUntil, Integer waitAfterMs,
		Viewport viewport, Long timestamp, String label, String description, boolean isPassword, boolean storeValue,
		Selector selector, TriggerNewTab isTriggerNewTab, Boolean shouldRun, Boolean required, Boolean sendToPlayground,
		String tag, Boolean downloadExpected) {

	/**
	 * Convenience constructor to create a new PlaywrightStep by copying an existing
	 * one and updating its text content.
	 * 
	 * @param s    The existing PlaywrightStep to copy from.
	 * @param text The new text content for the step.
	 */
	PlaywrightStep(PlaywrightStep s, String text) {
		this(s.id, s.type, s.url, s.coords, s.multiCoords, s.prompt, text, s.pressEnter, s.deltaY, s.waitUntil,
				s.waitAfterMs, s.viewport, s.timestamp, s.label, s.description, s.isPassword, s.storeValue, s.selector,
				s.isTriggerNewTab, s.shouldRun, s.required, s.sendToPlayground, s.tag, s.downloadExpected);
	}

	/**
	 * Convenience constructor to create a new PlaywrightStep by copying an existing
	 * one and updating its ID.
	 * 
	 * @param s  The existing PlaywrightStep to copy from.
	 * @param id The new ID for the step.
	 */
	PlaywrightStep(PlaywrightStep s, int id) {
		this(id, s.type, s.url, s.coords, s.multiCoords, s.prompt, s.text, s.pressEnter, s.deltaY, s.waitUntil,
				s.waitAfterMs, s.viewport, s.timestamp, s.label, s.description, s.isPassword, s.storeValue, s.selector,
				s.isTriggerNewTab, s.shouldRun, s.required, s.sendToPlayground, s.tag, s.downloadExpected);
	}

	/**
	 * Convenience constructor to create a new PlaywrightStep by copying an existing
	 * one and updating its label, text, storeValue, description, shouldRun, and
	 * required flags.
	 * 
	 * @param s           The existing PlaywrightStep to copy from.section
	 * @param label       The new label for the step.
	 * @param text        The new text content for the step.
	 * @param storeValue  The new storeValue flag for the step.
	 * @param description The new description for the step.
	 * @param shouldRun   The new shouldRun flag for the step.
	 * @param required    The new required flag for the step.
	 */
	PlaywrightStep(PlaywrightStep s, String label, String text, boolean storeValue, String description,
			boolean shouldRun, boolean required) {
		this(s.id, s.type, s.url, s.coords, s.multiCoords, s.prompt, text, s.pressEnter, s.deltaY, s.waitUntil,
				s.waitAfterMs, s.viewport, s.timestamp, label, description, s.isPassword, storeValue, s.selector,
				s.isTriggerNewTab, shouldRun, required, s.sendToPlayground, s.tag, s.downloadExpected);
	}

	/** Returns a copy marked as producing a native browser download. */
	public PlaywrightStep withDownloadExpected(boolean expected) {
		return new PlaywrightStep(id, type, url, coords, multiCoords, prompt, text, pressEnter, deltaY, waitUntil,
				waitAfterMs, viewport, timestamp, label, description, isPassword, storeValue, selector, isTriggerNewTab,
				shouldRun, required, sendToPlayground, tag, expected);
	}
}
