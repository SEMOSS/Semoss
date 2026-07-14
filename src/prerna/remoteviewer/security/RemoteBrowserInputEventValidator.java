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
package prerna.remoteviewer.security;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import prerna.remoteviewer.model.RemoteBrowserInputEvent;

/**
 * Validates incoming WebSocket input events from the React frontend. Prevents
 * injection of arbitrary browser commands or excessively large payloads.
 */
public class RemoteBrowserInputEventValidator {

	private static final int MAX_TYPE_TEXT_LENGTH = 2000;
	private static final int MAX_KEY_LENGTH = 64;
	private static final int MAX_URL_LENGTH = 2048;
	private static final int MAX_REQUEST_ID_LENGTH = 128;
	private static final int MAX_WAIT_AFTER_MS = 60_000;

	private static final Set<String> ALLOWED_EVENT_TYPES = new HashSet<>(
			Arrays.asList("mouse-click", "mouse-move", "mouse-down", "mouse-up", "wheel", "type-text", "key",
					"navigate", "close-session", "navigate-back", "navigate-forward", "reload", "recording",
					"recording-control"));

	private static final Set<String> ALLOWED_BUTTONS = new HashSet<>(Arrays.asList("left", "right", "middle"));

	private RemoteBrowserInputEventValidator() {

	}

	/**
	 * Validates the incoming event. Throws {@link IllegalArgumentException} on any
	 * violation.
	 *
	 * @param event    the parsed event from the WebSocket message
	 * @param vpWidth  the browser session viewport width (for coordinate clamping)
	 * @param vpHeight the browser session viewport height (for coordinate clamping)
	 */
	public static void validate(RemoteBrowserInputEvent event, int vpWidth, int vpHeight) {
		if (event == null) {
			throw new IllegalArgumentException("Event must not be null");
		}

		String type = event.getType();
		if (type == null || !ALLOWED_EVENT_TYPES.contains(type)) {
			throw new IllegalArgumentException("Unsupported event type: " + type);
		}
		if (event.getRequestId() != null && event.getRequestId().length() > MAX_REQUEST_ID_LENGTH) {
			throw new IllegalArgumentException("requestId exceeds max length " + MAX_REQUEST_ID_LENGTH);
		}
		if (event.getWaitAfterMs() != null
				&& (event.getWaitAfterMs() < 0 || event.getWaitAfterMs() > MAX_WAIT_AFTER_MS)) {
			throw new IllegalArgumentException("waitAfterMs must be between 0 and " + MAX_WAIT_AFTER_MS);
		}

		switch (type) {
		case "mouse-click":
			if (!hasSelector(event)) {
				requireCoordinates(event, vpWidth, vpHeight);
			} else if (event.getX() != null || event.getY() != null) {
				requireCoordinates(event, vpWidth, vpHeight);
			}
			if (event.getButton() != null && !ALLOWED_BUTTONS.contains(event.getButton())) {
				throw new IllegalArgumentException("Invalid button: " + event.getButton());
			}
			break;

		case "mouse-move":
		case "mouse-down":
		case "mouse-up":
			requireCoordinates(event, vpWidth, vpHeight);
			if (event.getButton() != null && !ALLOWED_BUTTONS.contains(event.getButton())) {
				throw new IllegalArgumentException("Invalid button: " + event.getButton());
			}
			break;

		case "wheel":
			requireCoordinates(event, vpWidth, vpHeight);
			break;

		case "type-text":
			if (event.getText() == null || event.getText().isEmpty()) {
				throw new IllegalArgumentException("type-text event requires non-empty 'text'");
			}
			if (event.getText().length() > MAX_TYPE_TEXT_LENGTH) {
				throw new IllegalArgumentException("type-text exceeds max length " + MAX_TYPE_TEXT_LENGTH);
			}
			break;

		case "key":
			if (event.getKey() == null || event.getKey().isEmpty()) {
				throw new IllegalArgumentException("key event requires non-empty 'key'");
			}
			if (event.getKey().length() > MAX_KEY_LENGTH) {
				throw new IllegalArgumentException("key value exceeds max length");
			}
			break;

		case "navigate":
			if (event.getUrl() == null || event.getUrl().isBlank()) {
				throw new IllegalArgumentException("navigate event requires 'url'");
			}
			if (event.getUrl().length() > MAX_URL_LENGTH) {
				throw new IllegalArgumentException("navigate URL exceeds max length");
			}
			RemoteBrowserUrlSafetyValidator.validate(event.getUrl());
			break;

		case "recording":
		case "recording-control":
			if (event.getRecording() == null && event.getRecord() == null) {
				throw new IllegalArgumentException("recording-control event requires 'recording' or 'record'");
			}
			break;

		// close-session, navigate-back, navigate-forward, reload — no payload to
		// validate
		default:
			break;
		}
	}

	private static void requireCoordinates(RemoteBrowserInputEvent event, int vpWidth, int vpHeight) {
		if (event.getX() == null || event.getY() == null) {
			throw new IllegalArgumentException("Event type '" + event.getType() + "' requires x and y");
		}
		// Clamp coordinates to viewport (mutate in place — safe because we own this
		// object)
		event.setX(Math.max(0, Math.min(event.getX(), vpWidth)));
		event.setY(Math.max(0, Math.min(event.getY(), vpHeight)));
	}

	private static boolean hasSelector(RemoteBrowserInputEvent event) {
		return event.getSelector() != null && event.getSelector().value() != null
				&& !event.getSelector().value().isBlank();
	}
}
