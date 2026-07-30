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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.microsoft.playwright.Page;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.reactor.AbstractReactor;
import prerna.remoteviewer.service.RemoteBrowserSession;
import prerna.remoteviewer.service.RemoteBrowserSessionManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * Generates text to autofill the currently focused browser form field using
 * the recent conversation from a Playground room as context.
 *
 * <p>Pixel usage:
 * <pre>
 *   AutofillPlaywrightField(engine="&lt;modelId&gt;", roomId="&lt;roomId&gt;");
 *   AutofillPlaywrightField(engine="&lt;modelId&gt;", roomId="&lt;roomId&gt;", fieldHint="First name", limit=20);
 *   AutofillPlaywrightField(engine="&lt;modelId&gt;", roomId="&lt;roomId&gt;", sessionId="&lt;sessionId&gt;", x=320, y=240);
 * </pre>
 * When {@code sessionId} and coordinates are supplied the reactor evaluates the
 * live Playwright page to resolve the element label/placeholder automatically.
 *
 * <p>Returns a MAP with:
 * <ul>
 *   <li>{@code success} (boolean) – whether generation succeeded</li>
 *   <li>{@code text} (string) – the generated value, when successful</li>
 *   <li>{@code error} (string) – error message, when unsuccessful</li>
 * </ul>
 */
public class AutofillPlaywrightFieldReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AutofillPlaywrightFieldReactor.class);

	private static final int DEFAULT_MESSAGE_LIMIT = 20;
	private static final int MAX_MESSAGE_LIMIT = 50;

	/** Optional key describing the field being filled (e.g. "First name"). */
	private static final String KEY_FIELD_HINT = "fieldHint";
	/** Optional browser session id — when provided with x/y, the label is resolved from the live page. */
	private static final String KEY_SESSION_ID = "sessionId";
	private static final String KEY_X = "x";
	private static final String KEY_Y = "y";

	public AutofillPlaywrightFieldReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ENGINE.getKey(),
				ReactorKeysEnum.ROOM_ID.getKey(),
				KEY_FIELD_HINT,
				ReactorKeysEnum.LIMIT.getKey(),
				KEY_SESSION_ID,
				KEY_X,
				KEY_Y
		};
		this.keyRequired = new int[] { 1, 1, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		Map<String, Object> result = new LinkedHashMap<>();
		try {
			String engineId  = clean(this.keyValue.get(ReactorKeysEnum.ENGINE.getKey()));
			String roomId    = clean(this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey()));
			String fieldHint = clean(this.keyValue.get(KEY_FIELD_HINT));
			int limit        = parseLimit(this.keyValue.get(ReactorKeysEnum.LIMIT.getKey()));
			String sessionId = clean(this.keyValue.get(KEY_SESSION_ID));
			Double x         = parseDouble(this.keyValue.get(KEY_X));
			Double y         = parseDouble(this.keyValue.get(KEY_Y));

			// Auto-resolve fieldHint from the live page when session + coords are available.
			if (fieldHint.isEmpty() && !sessionId.isEmpty() && x != null && y != null) {
				String resolved = resolveFieldLabelAtPoint(sessionId, x, y);
				if (!resolved.isEmpty()) {
					fieldHint = resolved;
				}
			}

			if (engineId.isEmpty()) {
				throw new IllegalArgumentException("A model engine id is required");
			}
			if (roomId.isEmpty()) {
				throw new IllegalArgumentException("A roomId is required");
			}

			User user = this.insight.getUser();
			if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
				throw new IllegalArgumentException("Model " + engineId + " does not exist or user does not have access");
			}

			Room sourceRoom = RoomUtils.getOrLoadRoom(roomId, this.insight);
			String roomContext = buildRoomContext(sourceRoom, limit);

			String prompt = buildPrompt(roomContext, fieldHint);

			IModelEngine modelEngine = Utility.getModel(engineId);
			Room inferenceRoom = RoomUtils.createRoomForStatelessAsk(
					UUID.randomUUID().toString(), this.insight, modelEngine, null);
			InputMessage input = InputMessage.builder(inferenceRoom).withText(prompt).build();
			ResponseMessage response = inferenceRoom.ask(input, modelEngine);

			String generated = response.getContent() == null ? "" : response.getContent().trim();
			result.put("success", true);
			result.put("text", generated);

		} catch (Exception e) {
			classLogger.warn("AutofillPlaywrightField: generation failed: {}", e.getMessage());
			result.put("success", false);
			result.put("error", e.getMessage() != null ? e.getMessage() : "Generation failed");
		}
		return new NounMetadata(result, PixelDataType.MAP);
	}

	// ─────────────────────────────────────────────────────────────────────────

	private static String buildRoomContext(Room room, int limit) {
		List<AbstractMessage> page = RoomUtils.getPagedMessages(room.getMessages(), "DESC", 0, limit);
		List<String> lines = new ArrayList<>();
		for (int i = page.size() - 1; i >= 0; i--) {
			AbstractMessage message = page.get(i);
			if (message == null || !message.isVisible()) {
				continue;
			}
			String role;
			String content;
			if (message instanceof InputMessage input) {
				role = "User";
				content = firstNonBlank(input.getInputUIPrompt(), input.getInputPrompt());
			} else if (message instanceof ResponseMessage resp) {
				role = "Assistant";
				content = resp.getContent();
			} else {
				continue;
			}
			if (content != null && !content.isBlank()) {
				lines.add(role + ": " + content.trim());
			}
		}
		return String.join("\n", lines);
	}

	private static String buildPrompt(String roomContext, String fieldHint) {
		String fieldDescription = fieldHint.isBlank()
				? "an active form field in the browser"
				: "\"" + fieldHint + "\"";

		String contextBlock = roomContext.isBlank()
				? "[No conversation context available]"
				: roomContext;

		return "You are helping a user fill a web browser form field based on their recent conversation.\n\n"
				+ "FIELD: " + fieldDescription + "\n\n"
				+ "CONVERSATION (most recent " + (roomContext.isBlank() ? "0" : "up to 20") + " messages):\n"
				+ contextBlock + "\n\n"
				+ "Based solely on the conversation above, provide the exact text that should be typed into "
				+ fieldDescription + ". "
				+ "Respond with ONLY the raw value — no explanation, no quotes, no formatting. "
				+ "If the conversation does not contain enough information to fill this field, respond with an empty string.";
	}

	// ─────────────────────────────────────────────────────────────────────────

	private static String clean(Object value) {
		if (value == null) return "";
		String s = String.valueOf(value).trim();
		// Strip surrounding quotes added by the pixel parser for string literals.
		if (s.length() >= 2
				&& ((s.startsWith("\"") && s.endsWith("\""))
						|| (s.startsWith("'") && s.endsWith("'")))) {
			s = s.substring(1, s.length() - 1).trim();
		}
		return s;
	}

	private static int parseLimit(Object value) {
		if (value == null) return DEFAULT_MESSAGE_LIMIT;
		try {
			int parsed = Integer.parseInt(String.valueOf(value).trim());
			return Math.min(Math.max(1, parsed), MAX_MESSAGE_LIMIT);
		} catch (NumberFormatException e) {
			return DEFAULT_MESSAGE_LIMIT;
		}
	}

	private static Double parseDouble(Object value) {
		if (value == null) return null;
		try {
			return Double.parseDouble(String.valueOf(value).trim());
		} catch (NumberFormatException e) {
			return null;
		}
	}

	/**
	 * Uses a Playwright page evaluation to find the form element at (x, y) and
	 * return its label text, aria-label, placeholder, or name — whichever is
	 * most descriptive.
	 */
	private String resolveFieldLabelAtPoint(String sessionId, double x, double y) {
		try {
			RemoteBrowserSession session = RemoteBrowserSessionManager.getInstance().getSession(sessionId)
					.orElse(null);
			if (session == null) return "";
			Page page = session.getActivePage();
			if (page == null || page.isClosed()) return "";

			String js = """
					([px, py]) => {
					    const el = document.elementFromPoint(px, py);
					    if (!el) return "";
					    const input = el.closest("input, textarea, select") || el.querySelector("input, textarea, select") || el;
					    if (!input) return "";
					    // 1) Explicit <label for="...">
					    if (input.id) {
					        const label = document.querySelector('label[for="' + CSS.escape(input.id) + '"]');
					        if (label && label.textContent.trim()) return label.textContent.trim();
					    }
					    // 2) Wrapping <label>
					    const wrap = input.closest("label");
					    if (wrap && wrap.textContent.trim()) return wrap.textContent.trim();
					    // 3) aria-label
					    const aria = input.getAttribute("aria-label");
					    if (aria && aria.trim()) return aria.trim();
					    // 4) aria-labelledby
					    const lblId = input.getAttribute("aria-labelledby");
					    if (lblId) {
					        const ref = document.getElementById(lblId);
					        if (ref && ref.textContent.trim()) return ref.textContent.trim();
					    }
					    // 5) placeholder or name
					    return input.placeholder || input.name || input.getAttribute("title") || "";
					}
					""";
			Object result = page.evaluate(js, new double[]{x, y});
			return result instanceof String s ? s.trim() : "";
		} catch (Exception e) {
			classLogger.debug("AutofillPlaywrightField: could not resolve field label at ({}, {}): {}", x, y, e.getMessage());
			return "";
		}
	}

	private static String firstNonBlank(String... values) {
		for (String v : values) {
			if (v != null && !v.isBlank()) return v.trim();
		}
		return "";
	}

	@Override
	public String getReactorDescription() {
		return "Generates text to autofill an active browser form field using recent conversation context from a Playground room. "
				+ "Returns a map with 'success' (boolean), 'text' (the generated value), and 'error' (if unsuccessful).";
	}
}
