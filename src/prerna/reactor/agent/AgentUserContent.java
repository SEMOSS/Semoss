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
package prerna.reactor.agent;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.parser.Parser;
import org.jsoup.safety.Cleaner;
import org.jsoup.safety.Safelist;

import prerna.engine.impl.model.message.InputMessage;

/** Explicit, bounded semantic HTML input shared by composer, history and model. */
public final class AgentUserContent {

	public static final String FORMAT_KEY = "userContentFormat";
	public static final String CONTENT_KEY = "userContent";
	public static final String HTML_V1 = "html-v1";
	public static final int MAX_BYTES = 262144;
	public static final int MAX_TEXT = 100000;
	public static final int MAX_CELLS = 5000;
	public static final int MAX_SPAN = 100;
	public static final int MAX_DEPTH = 32;
	public static final int MAX_NODES = 20000;
	private static final Set<String> SCOPES = Set.of("row", "col", "rowgroup", "colgroup");
	private AgentUserContent() {
	}

	public static boolean isRich(Map<String, Object> params) {
		return params != null && params.containsKey(FORMAT_KEY);
	}

	/** Pixel has decoded explicit rich commands once; retain legacy decoding otherwise. */
	public static String decodeCommand(String input, Map<String, Object> params) {
		if (input == null || isRich(params) || input.indexOf('%') < 0) {
			return input;
		}
		try {
			return URLDecoder.decode(input, StandardCharsets.UTF_8);
		} catch (IllegalArgumentException e) {
			return input;
		}
	}

	public record Normalized(String command, Map<String, Object> params) {
	}

	/** Reject inconsistent rich requests before they reach durable submission or a provider. */
	public static Normalized normalize(String command, Map<String, Object> params) {
		if (!isRich(params)) {
			return new Normalized(command, params);
		}
		if (!HTML_V1.equals(params.get(FORMAT_KEY))) {
			throw new IllegalArgumentException("Unsupported userContentFormat");
		}
		if (!(params.get(CONTENT_KEY) instanceof String content) || content.isBlank()) {
			throw new IllegalArgumentException("Rich userContent is required");
		}
		if (!content.equals(params.get(AgentRunner.AGENT_PARAM_USER_TEXT))) {
			throw new IllegalArgumentException("Rich userText must equal userContent");
		}
		if (command == null || !command.endsWith(content)) {
			throw new IllegalArgumentException("Rich command must end with userContent unchanged");
		}
		String clean = sanitize(content);
		Map<String, Object> normalized = new HashMap<>(params);
		normalized.put(CONTENT_KEY, clean);
		normalized.put(AgentRunner.AGENT_PARAM_USER_TEXT, clean);
		return new Normalized(command.substring(0, command.length() - content.length()) + clean, normalized);
	}

	public static String sanitize(String content) {
		if (content.length() > MAX_BYTES || content.getBytes(StandardCharsets.UTF_8).length > MAX_BYTES) {
			throw new IllegalArgumentException("Rich userContent exceeds the byte limit");
		}
		Parser parser = Parser.htmlParser().setTrackErrors(1);
		Document dirty = parser.parseInput(content, "");
		if (!parser.getErrors().isEmpty()) {
			throw new IllegalArgumentException("Rich userContent contains malformed HTML");
		}
		// Validate the complete source tree before cleaning; dropped tags cannot evade limits.
		int[] counts = new int[3];
		validateTree(dirty.body(), 0, false, counts);
		if (dirty.body().wholeText().length() > MAX_TEXT) {
			throw new IllegalArgumentException("Rich userContent exceeds the text limit");
		}
		Safelist allowed = Safelist.none().addTags("p", "br", "strong", "em", "u", "s", "blockquote", "ul",
				"ol", "li", "pre", "code", "table", "caption", "thead", "tbody", "tfoot", "tr", "th", "td")
				.addAttributes("td", "colspan", "rowspan").addAttributes("th", "colspan", "rowspan", "scope");
		Document safe = new Cleaner(allowed).clean(dirty);
		safe.outputSettings().prettyPrint(false).charset(StandardCharsets.UTF_8);
		String canonical = safe.body().html();
		if (safe.body().text().isBlank()) {
			throw new IllegalArgumentException("Rich userContent must contain text");
		}
		if (canonical.getBytes(StandardCharsets.UTF_8).length > MAX_BYTES) {
			throw new IllegalArgumentException("Canonical rich userContent exceeds the byte limit");
		}
		return canonical;
	}

	private static void validateTree(Node node, int depth, boolean insideTable, int[] counts) {
		if (depth > MAX_DEPTH || ++counts[0] > MAX_NODES) {
			throw new IllegalArgumentException("Rich userContent exceeds structural limits");
		}
		boolean table = node instanceof Element element && "table".equals(element.normalName());
		if (table && insideTable) {
			throw new IllegalArgumentException("Nested rich tables are not supported");
		}
		if (node instanceof Element element && Set.of("td", "th").contains(element.normalName())) {
			if (++counts[1] > MAX_CELLS) {
				throw new IllegalArgumentException("Rich userContent exceeds the cell limit");
			}
			int expandedSlots = 1;
			for (String attribute : new String[] { "colspan", "rowspan" }) {
				if (element.hasAttr(attribute)) {
					String value = element.attr(attribute);
					if (!value.matches("[0-9]{1,3}") || Integer.parseInt(value) < 1
							|| Integer.parseInt(value) > MAX_SPAN) {
						throw new IllegalArgumentException("Rich table span must be between 1 and " + MAX_SPAN);
					}
					expandedSlots *= Integer.parseInt(value);
				}
			}
			counts[2] += expandedSlots;
			if (counts[2] > MAX_CELLS) {
				throw new IllegalArgumentException("Rich userContent exceeds the expanded table cell limit");
			}
			if (element.hasAttr("scope") && !SCOPES.contains(element.attr("scope"))) {
				throw new IllegalArgumentException("Unsupported rich table header scope");
			}
		}
		for (Node child : node.childNodes()) {
			validateTree(child, depth + 1, insideTable || table, counts);
		}
	}

	/** Only the explicit contract opts the saved user message into rich rendering. */
	public static void annotate(InputMessage message, Map<String, Object> params) {
		if (params != null && HTML_V1.equals(params.get(FORMAT_KEY))) {
			message.setOrnament(FORMAT_KEY, HTML_V1);
		}
	}
}
