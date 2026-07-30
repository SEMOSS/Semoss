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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.playwright.Page;

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
 * Finds all visible, fillable form fields on the current browser page, uses
 * the Playground room conversation as context, and generates a value for each
 * field in a single LLM call.
 *
 * <p>Pixel usage:
 * <pre>
 *   FillPlaywrightForm(engine="&lt;modelId&gt;", roomId="&lt;roomId&gt;", sessionId="&lt;sessionId&gt;");
 * </pre>
 *
 * <p>Returns a MAP with:
 * <ul>
 *   <li>{@code success} (boolean)</li>
 *   <li>{@code fields} (List&lt;Map&gt;) – each entry has {@code selector},
 *       {@code label}, {@code value}, {@code strategy}</li>
 *   <li>{@code error} (string) – when unsuccessful</li>
 * </ul>
 */
public class FillPlaywrightFormReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(FillPlaywrightFormReactor.class);
	private static final ObjectMapper JSON = new ObjectMapper();

	private static final int DEFAULT_MESSAGE_LIMIT = 20;
	private static final String KEY_SESSION_ID = "sessionId";

	/** JS run on the live page to enumerate all fillable visible form fields. */
	private static final String JS_FIND_FIELDS = """
			() => {
			    const EXCLUDED_TYPES = new Set(["hidden","submit","button","reset","image","checkbox","radio","file","color","range"]);
			    const results = [];
			    const seen = new Set();

			    function getBestSelector(el) {
			        if (el.id && /^[\\w-]+$/.test(el.id)) return { strategy: "id", value: el.id };
			        if (el.name) return { strategy: "css", value: el.tagName.toLowerCase() + '[name="' + el.name + '"]' };
			        // Build an nth-child CSS path as fallback
			        const path = [];
			        let cur = el;
			        while (cur && cur !== document.body) {
			            const tag = cur.tagName.toLowerCase();
			            const siblings = Array.from(cur.parentElement?.children || []).filter(c => c.tagName === cur.tagName);
			            const idx = siblings.indexOf(cur) + 1;
			            path.unshift(siblings.length === 1 ? tag : tag + ":nth-of-type(" + idx + ")");
			            cur = cur.parentElement;
			        }
			        return { strategy: "css", value: path.join(" > ") };
			    }

			    function getLabel(el) {
			        if (el.id) {
			            const lbl = document.querySelector('label[for="' + CSS.escape(el.id) + '"]');
			            if (lbl && lbl.textContent.trim()) return lbl.textContent.trim();
			        }
			        const wrap = el.closest("label");
			        if (wrap) return wrap.textContent.replace(el.value || "", "").trim();
			        const aria = el.getAttribute("aria-label");
			        if (aria) return aria.trim();
			        const lblId = el.getAttribute("aria-labelledby");
			        if (lblId) {
			            const ref = document.getElementById(lblId);
			            if (ref) return ref.textContent.trim();
			        }
			        return el.placeholder || el.name || el.getAttribute("title") || el.getAttribute("data-label") || "";
			    }

			    function isVisible(el) {
			        const rect = el.getBoundingClientRect();
			        if (rect.width === 0 || rect.height === 0) return false;
			        const style = window.getComputedStyle(el);
			        if (style.display === "none" || style.visibility === "hidden" || parseFloat(style.opacity) === 0) return false;
			        return true;
			    }

			    const inputs = document.querySelectorAll("input, textarea, select");
			    for (const el of inputs) {
			        if (el.tagName === "INPUT" && EXCLUDED_TYPES.has((el.type || "text").toLowerCase())) continue;
			        if (el.disabled || el.readOnly) continue;
			        if (!isVisible(el)) continue;
			        const sel = getBestSelector(el);
			        const key = sel.strategy + ":" + sel.value;
			        if (seen.has(key)) continue;
			        seen.add(key);
			        results.push({
			            selector: sel,
			            label: getLabel(el),
			            type: el.tagName === "SELECT" ? "select" : (el.type || el.tagName.toLowerCase()),
			            currentValue: el.value || ""
			        });
			    }
			    return results;
			}
			""";

	public FillPlaywrightFormReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ENGINE.getKey(),
				ReactorKeysEnum.ROOM_ID.getKey(),
				KEY_SESSION_ID,
				ReactorKeysEnum.LIMIT.getKey()
		};
		this.keyRequired = new int[] { 1, 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		Map<String, Object> result = new LinkedHashMap<>();
		try {
			String engineId  = clean(this.keyValue.get(ReactorKeysEnum.ENGINE.getKey()));
			String roomId    = clean(this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey()));
			String sessionId = clean(this.keyValue.get(KEY_SESSION_ID));
			int limit        = parseLimit(this.keyValue.get(ReactorKeysEnum.LIMIT.getKey()));

			if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
				throw new IllegalArgumentException("Model " + engineId + " does not exist or user does not have access");
			}

			// 1 — Get page fields via JS
			List<Map<String, Object>> pageFields = extractPageFields(sessionId);
			if (pageFields.isEmpty()) {
				result.put("success", true);
				result.put("fields", List.of());
				result.put("message", "No fillable fields found on the current page");
				return new NounMetadata(result, PixelDataType.MAP);
			}

			// 2 — Build conversation context from room
			Room sourceRoom = RoomUtils.getOrLoadRoom(roomId, this.insight);
			String roomContext = buildRoomContext(sourceRoom, limit);

			// 3 — Build prompt and call LLM once for all fields
			String prompt = buildPrompt(roomContext, pageFields);
			IModelEngine modelEngine = Utility.getModel(engineId);
			Room inferenceRoom = RoomUtils.createRoomForStatelessAsk(
					UUID.randomUUID().toString(), this.insight, modelEngine, null);
			InputMessage input = InputMessage.builder(inferenceRoom).withText(prompt).build();
			ResponseMessage response = inferenceRoom.ask(input, modelEngine);

			// 4 — Parse JSON response
			List<Map<String, Object>> fills = parseFilledFields(response.getContent(), pageFields);

			result.put("success", true);
			result.put("fields", fills);

		} catch (Exception e) {
			classLogger.warn("FillPlaywrightForm: generation failed: {}", e.getMessage());
			result.put("success", false);
			result.put("error", e.getMessage() != null ? e.getMessage() : "Generation failed");
		}
		return new NounMetadata(result, PixelDataType.MAP);
	}

	// ─────────────────────────────────────────────────────────────────────────

	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> extractPageFields(String sessionId) {
		RemoteBrowserSession session = RemoteBrowserSessionManager.getInstance()
				.getSession(sessionId).orElse(null);
		if (session == null) {
			throw new IllegalArgumentException("Browser session '" + sessionId + "' not found");
		}
		String userId = this.insight.getUser().getPrimaryLoginToken().getId();
		if (!userId.equals(session.getUserId())) {
			throw new IllegalArgumentException("Browser session does not belong to the current user");
		}
		Page page = session.getActivePage();
		if (page == null || page.isClosed()) {
			throw new IllegalArgumentException("No active browser page in session '" + sessionId + "'");
		}
		Object raw = page.evaluate(JS_FIND_FIELDS);
		if (!(raw instanceof List)) return List.of();
		return (List<Map<String, Object>>) raw;
	}

	private static String buildRoomContext(Room room, int limit) {
		List<AbstractMessage> page = RoomUtils.getPagedMessages(room.getMessages(), "DESC", 0, limit);
		List<String> lines = new ArrayList<>();
		for (int i = page.size() - 1; i >= 0; i--) {
			AbstractMessage msg = page.get(i);
			if (msg == null || !msg.isVisible()) continue;
			String role, content;
			if (msg instanceof InputMessage inp) {
				role = "User";
				content = firstNonBlank(inp.getInputUIPrompt(), inp.getInputPrompt());
			} else if (msg instanceof ResponseMessage resp) {
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

	private static String buildPrompt(String roomContext, List<Map<String, Object>> fields) {
		StringBuilder fieldList = new StringBuilder();
		for (int i = 0; i < fields.size(); i++) {
			Map<String, Object> f = fields.get(i);
			String label = String.valueOf(f.getOrDefault("label", "")).trim();
			String type  = String.valueOf(f.getOrDefault("type", "text")).trim();
			String cur   = String.valueOf(f.getOrDefault("currentValue", "")).trim();
			fieldList.append(i).append(". label=\"").append(label.isEmpty() ? "(unlabelled)" : label)
					.append("\"  type=").append(type);
			if (!cur.isEmpty()) fieldList.append("  currentValue=\"").append(cur).append("\"");
			fieldList.append("\n");
		}

		return "You are filling a web form. Use the conversation below to generate the value for each field.\n\n"
				+ "Return ONLY a valid JSON array where each element has exactly two keys: "
				+ "\"index\" (the field index number from the list below) and \"value\" (the string to fill in).\n"
				+ "Use an empty string for fields where the conversation does not provide enough information.\n"
				+ "Do NOT include any explanation, markdown, or extra text — just the JSON array.\n\n"
				+ "CONVERSATION:\n"
				+ (roomContext.isBlank() ? "[No conversation context available]" : roomContext)
				+ "\n\nFIELDS:\n" + fieldList
				+ "\nJSON array:";
	}

	/**
	 * Parses the LLM JSON response and merges values back with the original
	 * field descriptors (selector, label).
	 */
	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> parseFilledFields(String llmOutput,
			List<Map<String, Object>> originalFields) {
		List<Map<String, Object>> results = new ArrayList<>();
		try {
			// Extract the first JSON array from the response (model may add surrounding text).
			String trimmed = llmOutput == null ? "" : llmOutput.trim();
			int start = trimmed.indexOf('[');
			int end   = trimmed.lastIndexOf(']');
			if (start < 0 || end <= start) return results;
			String json = trimmed.substring(start, end + 1);

			List<Map<String, Object>> parsed = JSON.readValue(json, new TypeReference<>() {});
			for (Map<String, Object> entry : parsed) {
				Object idxObj = entry.get("index");
				Object valObj = entry.get("value");
				if (idxObj == null || valObj == null) continue;
				int idx;
				try {
					idx = Integer.parseInt(String.valueOf(idxObj).trim());
				} catch (NumberFormatException e) {
					continue;
				}
				if (idx < 0 || idx >= originalFields.size()) continue;
				String value = String.valueOf(valObj).trim();
				if (value.isEmpty()) continue; // skip fields the model left blank

				Map<String, Object> original = originalFields.get(idx);
				Map<String, Object> fill = new LinkedHashMap<>();
				fill.put("index", idx);
				fill.put("label", original.getOrDefault("label", ""));
				fill.put("value", value);
				// Flatten selector for easy frontend use
				Object sel = original.get("selector");
				if (sel instanceof Map<?, ?> selMap) {
					@SuppressWarnings("unchecked")
					Map<String, Object> typedSelMap = (Map<String, Object>) selMap;
					fill.put("selectorStrategy", typedSelMap.getOrDefault("strategy", "css"));
					fill.put("selectorValue",    typedSelMap.getOrDefault("value", ""));
				}
				results.add(fill);
			}
		} catch (Exception e) {
			classLogger.warn("FillPlaywrightForm: could not parse LLM response as JSON: {}", e.getMessage());
		}
		return results;
	}

	// ─────────────────────────────────────────────────────────────────────────

	private static String clean(Object value) {
		if (value == null) return "";
		String s = String.valueOf(value).trim();
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
			return Math.min(Math.max(1, parsed), 50);
		} catch (NumberFormatException e) {
			return DEFAULT_MESSAGE_LIMIT;
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
		return "Finds all visible form fields on the active browser page, generates values from the Playground room "
				+ "conversation in a single LLM call, and returns a list of {label, selectorStrategy, selectorValue, value} "
				+ "entries for the frontend to dispatch as fill-element events.";
	}
}
