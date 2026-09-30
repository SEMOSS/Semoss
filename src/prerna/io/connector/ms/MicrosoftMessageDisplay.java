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
package prerna.io.connector.ms;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;

/**
 * Bounded, untrusted source content for a client renderer; never persisted as
 * model context.
 */
public final class MicrosoftMessageDisplay {

	public static final int MAX_DISPLAY_CHARS = 128 * 1024;

	private MicrosoftMessageDisplay() {
	}

	/**
	 * Keep the original body intact, or fall back to text rather than cutting
	 * through HTML.
	 */
	public static Map<String, Object> body(Map<String, Object> message, String fallback) {
		Map<?, ?> body = message.get("body") instanceof Map<?, ?> map ? map : Map.of();
		String content = body.get("content") instanceof String text ? text : fallback;
		String type = "html".equalsIgnoreCase(String.valueOf(body.get("contentType"))) ? "html" : "text";
		boolean truncated = content.length() > MAX_DISPLAY_CHARS;
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("contentType", truncated ? "text" : type);
		result.put("content",
				truncated ? fallback.substring(0, Math.min(fallback.length(), MAX_DISPLAY_CHARS)) : content);
		result.put("isTruncated", truncated);
		List<Map<String, String>> attachments = new ArrayList<>();
		if (message.get("attachments") instanceof List<?> items) {
			for (Object item : items) {
				if (item instanceof Map<?, ?> attachment) {
					String name = attachment.get("name") instanceof String n && !n.isBlank() ? n : "Attachment or card";
					attachments.add(Map.of("name", name));
				}
			}
		}
		if (!attachments.isEmpty()) {
			result.put("attachments", attachments);
		}
		return result;
	}

	/**
	 * Teams text has no email signatures or quoted-history trimming; preserve code
	 * whitespace.
	 */
	public static String text(Map<String, Object> message) {
		if (!(message.get("body") instanceof Map<?, ?> body)) {
			return "";
		}
		String content = body.get("content") instanceof String text ? text : "";
		if (!"html".equalsIgnoreCase(String.valueOf(body.get("contentType")))) {
			return content.strip();
		}
		Element root = Jsoup.parse(content).body();
		root.select("script,style,template").remove();
		StringBuilder out = new StringBuilder();
		append(root, out, false);
		return out.toString().strip();
	}

	private static void append(Node node, StringBuilder out, boolean pre) {
		if (node instanceof TextNode text) {
			out.append(pre ? text.getWholeText() : text.getWholeText().replaceAll("\\s+", " "));
			return;
		}
		if (!(node instanceof Element element)) {
			return;
		}
		String tag = element.normalName();
		if ("br".equals(tag)) {
			out.append('\n');
			return;
		}
		boolean block = element.isBlock();
		if (block && !out.isEmpty() && out.charAt(out.length() - 1) != '\n') {
			out.append('\n');
		}
		for (Node child : element.childNodes()) {
			append(child, out, pre || "pre".equals(tag) || "code".equals(tag));
		}
		if (block && !out.isEmpty() && out.charAt(out.length() - 1) != '\n') {
			out.append('\n');
		}
	}
}
