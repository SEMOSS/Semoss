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

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.microsoft.playwright.Mouse;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.options.MouseButton;

import prerna.remoteviewer.model.BrowserInputEvent;
import prerna.remoteviewer.security.UrlSafetyValidator;

/**
 * Maps validated frontend input events onto Playwright browser actions.
 *
 * All calls must be made from the session's dedicated Playwright thread.
 */
public class BrowserInputService {

	private static final Logger classLogger = LogManager.getLogger(BrowserInputService.class);

	private BrowserInputService() {}

	public static void dispatch(BrowserSession session, BrowserInputEvent event) {
		Page page = session.getPage();
		if (page == null || page.isClosed()) {
			return;
		}

		String type = event.getType();
		try {
			switch (type) {
			case "mouse-click":
				click(page, event);
				break;
			case "mouse-move":
				page.mouse().move(event.getX(), event.getY());
				break;
			case "mouse-down":
				page.mouse().down(buildMouseDownOptions(event));
				break;
			case "mouse-up":
				page.mouse().up(buildMouseUpOptions(event));
				break;
			case "wheel":
				wheel(page, event);
				break;
			case "type-text":
				typeText(page, event);
				break;
			case "key":
				key(page, event);
				break;
			case "navigate":
				navigate(page, event);
				break;
			case "navigate-back":
				page.goBack();
				break;
			case "navigate-forward":
				page.goForward();
				break;
			case "reload":
				page.reload();
				break;
			default:
				classLogger.warn("Unhandled input event type: {}", type);
			}
		} catch (Exception e) {
			classLogger.warn("Error dispatching event '{}' on session {}: {}", type, session.getSessionId(),
					e.getMessage());
		}
	}

	private static void click(Page page, BrowserInputEvent event) {
		page.mouse().click(event.getX(), event.getY());
	}

	private static void wheel(Page page, BrowserInputEvent event) {
		page.mouse().wheel(
				event.getDeltaX() != null ? event.getDeltaX() : 0,
				event.getDeltaY() != null ? event.getDeltaY() : 0);
	}

	private static void typeText(Page page, BrowserInputEvent event) {
		// Mask password fields: do not log the text
		page.keyboard().type(event.getText());
	}

	private static void key(Page page, BrowserInputEvent event) {
		String keyCombo = buildKeyCombo(event);
		page.keyboard().press(keyCombo);
	}

	private static void navigate(Page page, BrowserInputEvent event) {
		UrlSafetyValidator.validate(event.getUrl());
		page.navigate(event.getUrl());
	}

	// ---- helpers ----

	private static MouseButton resolveButton(String btn) {
		if (btn == null) return MouseButton.LEFT;
		switch (btn) {
		case "right":  return MouseButton.RIGHT;
		case "middle": return MouseButton.MIDDLE;
		default:       return MouseButton.LEFT;
		}
	}

	private static Mouse.DownOptions buildMouseDownOptions(BrowserInputEvent event) {
		Mouse.DownOptions opts = new Mouse.DownOptions();
		opts.setButton(resolveButton(event.getButton()));
		return opts;
	}

	private static Mouse.UpOptions buildMouseUpOptions(BrowserInputEvent event) {
		Mouse.UpOptions opts = new Mouse.UpOptions();
		opts.setButton(resolveButton(event.getButton()));
		return opts;
	}

	private static String buildKeyCombo(BrowserInputEvent event) {
		Map<String, Boolean> mods = event.getModifiers();
		StringBuilder sb = new StringBuilder();
		if (mods != null) {
			if (Boolean.TRUE.equals(mods.get("ctrl")))  sb.append("Control+");
			if (Boolean.TRUE.equals(mods.get("meta")))  sb.append("Meta+");
			if (Boolean.TRUE.equals(mods.get("alt")))   sb.append("Alt+");
			if (Boolean.TRUE.equals(mods.get("shift"))) sb.append("Shift+");
		}
		sb.append(event.getKey());
		return sb.toString();
	}
}
