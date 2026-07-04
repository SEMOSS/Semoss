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
package prerna.remoteviewer.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.remoteviewer.model.BrowserInputEvent;
import prerna.remoteviewer.model.RecordedStep;

/**
 * Records deterministic interaction steps for a browser session.
 *
 * <p>
 * For click events, attempts to identify the clicked element via JavaScript to
 * enrich the recorded step with a semantic selector.
 */
public class BrowserRecordingService {

	private static final Logger classLogger = LogManager.getLogger(BrowserRecordingService.class);

	/** Sensitive input types where text should not be recorded. */
	private static final String PROBE_SCRIPT = "([x, y]) => {" + "  const el = document.elementFromPoint(x, y);"
			+ "  if (!el) return null;" + "  const tag = el.tagName.toLowerCase();"
			+ "  const type = (el.getAttribute('type') || '').toLowerCase();"
			+ "  if (tag === 'input' && (type === 'password' || type === 'hidden')) return { masked: true };"
			+ "  const role = el.getAttribute('role') || el.tagName.toLowerCase();"
			+ "  const text = (el.innerText || el.getAttribute('aria-label') || el.getAttribute('title') || '').trim().substring(0, 100);"
			+ "  const id = el.id ? '#' + el.id : null;"
			+ "  const ariaLabel = el.getAttribute('aria-label') ? '[aria-label=\"' + el.getAttribute('aria-label') + '\"]' : null;"
			+ "  const selector = id || ariaLabel || null;" + "  return { role, text, selector };" + "}";

	private BrowserRecordingService() {
	}

	/**
	 * Records an input event as a step in the session's history. Click events are
	 * enriched with element information when possible.
	 */
	public static void record(BrowserSession session, BrowserInputEvent event) {
		String type = event.getType();
		RecordedStep step = new RecordedStep().type(type).url(safeUrl(session))
				.viewport(session.getViewportWidth(), session.getViewportHeight())
				.timestamp(System.currentTimeMillis() / 1000L);

		switch (type) {
		case "mouse-click":
			recordClick(session, event, step);
			break;

		case "type-text":
			// Never record password field text; record that typing occurred
			step.text("[typed]"); // placeholder — actual text not stored
			session.getRecordedSteps().add(step);
			break;

		case "key":
			step.text(event.getKey());
			session.getRecordedSteps().add(step);
			break;

		case "navigate":
			step.url(event.getUrl());
			session.getRecordedSteps().add(step);
			break;

		case "navigate-back":
		case "navigate-forward":
		case "reload":
		case "wheel":
			if (event.getX() != null) {
				step.coordinates(event.getX(), event.getY());
			}
			session.getRecordedSteps().add(step);
			break;

		default:
			// Not all event types need to be recorded
			break;
		}
	}

	private static void recordClick(BrowserSession session, BrowserInputEvent event, RecordedStep step) {
		if (event.getX() == null || event.getY() == null) {
			session.getRecordedSteps().add(step);
			return;
		}
		step.coordinates(event.getX(), event.getY());

		try {
			Object result = session.getPage().evaluate(PROBE_SCRIPT, new double[] { event.getX(), event.getY() });

			if (result instanceof java.util.Map) {
				@SuppressWarnings("unchecked")
				java.util.Map<String, Object> info = (java.util.Map<String, Object>) result;

				if (Boolean.TRUE.equals(info.get("masked"))) {
					// Password field — record coordinates only, no selector or text
					classLogger.debug("Click on masked field at ({}, {}) — not recording selector/text", event.getX(),
							event.getY());
				} else {
					String selector = (String) info.get("selector");
					String text = (String) info.get("text");
					String role = (String) info.get("role");
					if (selector != null) {
						step.selector(selector);
					}
					if (text != null && !text.isBlank()) {
						step.text(text);
					}
					if (role != null) {
						step.role(role);
					}
				}
			}
		} catch (Exception e) {
			// Element probe failure is non-critical — coordinates are still recorded
			classLogger.debug("Element probe failed for click at ({}, {}): {}", event.getX(), event.getY(),
					e.getMessage());
		}

		session.getRecordedSteps().add(step);
	}

	private static String safeUrl(BrowserSession session) {
		try {
			return session.getPage().url();
		} catch (Exception e) {
			return "";
		}
	}
}
