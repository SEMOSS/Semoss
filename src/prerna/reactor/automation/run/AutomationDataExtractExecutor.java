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
package prerna.reactor.automation.run;

import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.helpers.DefaultHandler;

import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;

/**
 * Extracts one typed value from JSON or XML without invoking Python.
 */
final class AutomationDataExtractExecutor {

	private static final Object MISSING = new Object();

	private AutomationDataExtractExecutor() {
	}

	@SuppressWarnings("unchecked")
	static Object execute(Map<String, Object> config, Map<String, Object> scope, String nodeId,
			Function<String, String> assetReader) {
		Object source = AutomationScopeResolver.resolveExactOrLiteral(
				config.get(AutomationConstants.CONFIG_SOURCE), scope, "Data extract node '" + nodeId + "' source");
		Object pathValue = AutomationScopeResolver.resolveExactOrLiteral(
				config.get(AutomationConstants.CONFIG_PATH), scope, "Data extract node '" + nodeId + "' path");
		Object formatValue = AutomationScopeResolver.resolveExactOrLiteral(
				config.getOrDefault(AutomationConstants.CONFIG_FORMAT, "auto"), scope,
				"Data extract node '" + nodeId + "' format");
		Object missingValue = AutomationScopeResolver.resolveExactOrLiteral(
				config.get(AutomationConstants.CONFIG_MISSING_VALUE), scope,
				"Data extract node '" + nodeId + "' missingValue");
		Object nullValue = AutomationScopeResolver.resolveExactOrLiteral(
				config.get(AutomationConstants.CONFIG_NULL_VALUE), scope,
				"Data extract node '" + nodeId + "' nullValue");
		String path = pathValue == null ? "" : pathValue.toString();
		String format = formatValue == null ? "auto" : formatValue.toString().trim().toLowerCase();
		if (!List.of("auto", "json", "xml").contains(format)) {
			throw new IllegalArgumentException("Data format must be auto, json, or xml.");
		}
		Object data = structuredData(source, format, assetReader);
		Object value = nestedValue(data, pathTokens(path));
		return value == MISSING ? missingValue : value == null ? nullValue : value;
	}

	private static Object structuredData(Object source, String format, Function<String, String> assetReader) {
		if (source instanceof AutomationTableValue table) {
			if ("xml".equals(format)) {
				throw new IllegalArgumentException("XML input cannot be a table value.");
			}
			return table.pageAsMaps(0, Math.toIntExact(table.rowCount()));
		}
		if (source instanceof Map<?, ?> || source instanceof List<?>) {
			if ("xml".equals(format)) {
				throw new IllegalArgumentException("XML input must be text or a run-workspace file path.");
			}
			return source;
		}
		if (!(source instanceof String text) || text.isBlank()) {
			throw new IllegalArgumentException("Data must be a JSON/XML value, encoded content, or file path.");
		}
		List<String> candidates = new ArrayList<>();
		candidates.add(text);
		try {
			candidates.add(new String(Base64.getDecoder().decode(text), StandardCharsets.UTF_8));
		} catch (IllegalArgumentException ignored) {
			// Not Base64; the literal and governed asset path are still attempted.
		}
		for (String candidate : candidates) {
			try {
				return parse(candidate, format);
			} catch (RuntimeException ignored) {
				// Try the next supported representation.
			}
		}
		try {
			return parse(assetReader.apply(text), format);
		} catch (RuntimeException e) {
			throw new IllegalArgumentException(
					"Unable to read JSON or XML from the supplied value or run-workspace file.", e);
		}
	}

	private static Object parse(String source, String format) {
		String text = source == null ? "" : source.replaceFirst("^[\\uFEFF\\r\\n\\t ]+", "");
		if ("json".equals(format) || "auto".equals(format) && (text.startsWith("{") || text.startsWith("["))) {
			try {
				return AutomationRuntimeUtils.GSON.fromJson(text, Object.class);
			} catch (RuntimeException e) {
				throw new IllegalArgumentException("Input is not valid JSON.", e);
			}
		}
		if ("xml".equals(format) || "auto".equals(format) && text.startsWith("<")) {
			return parseXml(text);
		}
		if ("auto".equals(format)) {
			if (text.startsWith("\"") || text.matches("true|false|null|-?[0-9].*")) {
				try {
					return AutomationRuntimeUtils.GSON.fromJson(text, Object.class);
				} catch (RuntimeException ignored) {
					// Continue to the XML parser for the final supported representation.
				}
			}
			return parseXml(text);
		}
		throw new IllegalArgumentException("Input is not valid " + format.toUpperCase() + ".");
	}

	private static Object parseXml(String text) {
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
			factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
			factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
			factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
			factory.setXIncludeAware(false);
			factory.setExpandEntityReferences(false);
			var builder = factory.newDocumentBuilder();
			builder.setErrorHandler(new DefaultHandler());
			Element root = builder.parse(new InputSource(new StringReader(text))).getDocumentElement();
			return Map.of(root.getTagName(), xmlValue(root));
		} catch (Exception e) {
			throw new IllegalArgumentException("Input is not valid XML.", e);
		}
	}

	private static Object xmlValue(Element element) {
		Map<String, Object> result = new LinkedHashMap<>();
		for (int index = 0; index < element.getAttributes().getLength(); index++) {
			Node attribute = element.getAttributes().item(index);
			result.put("@" + attribute.getNodeName(), attribute.getNodeValue());
		}
		List<Element> children = new ArrayList<>();
		NodeList childNodes = element.getChildNodes();
		for (int index = 0; index < childNodes.getLength(); index++) {
			if (childNodes.item(index) instanceof Element child) {
				children.add(child);
			}
		}
		String text = element.getTextContent() == null ? "" : element.getTextContent().trim();
		if (children.isEmpty()) {
			if (result.isEmpty()) {
				return text.isEmpty() ? null : text;
			}
			if (!text.isEmpty()) {
				result.put("#text", text);
			}
			return result;
		}
		for (Element child : children) {
			Object value = xmlValue(child);
			Object existing = result.get(child.getTagName());
			if (existing == null && !result.containsKey(child.getTagName())) {
				result.put(child.getTagName(), value);
			} else if (existing instanceof List<?> list) {
				@SuppressWarnings("unchecked")
				List<Object> values = (List<Object>) list;
				values.add(value);
			} else {
				List<Object> values = new ArrayList<>();
				values.add(existing);
				values.add(value);
				result.put(child.getTagName(), values);
			}
		}
		return result;
	}

	private static List<Object> pathTokens(String path) {
		String value = path == null ? "" : path.trim();
		if (value.isEmpty()) {
			throw new IllegalArgumentException("Value path cannot be blank.");
		}
		if (value.startsWith("/")) {
			List<Object> tokens = new ArrayList<>();
			for (String token : value.substring(1).split("/", -1)) {
				String decoded = token.replace("~1", "/").replace("~0", "~");
				tokens.add(decoded.matches("[0-9]+") ? Integer.valueOf(decoded) : decoded);
			}
			return tokens;
		}
		if ("$".equals(value)) {
			return List.of();
		}
		if (value.startsWith("$")) {
			if (!value.startsWith("$.") && !value.startsWith("$[")) {
				throw new IllegalArgumentException("Value path must use '$.' or '$[' after the root marker.");
			}
			value = value.substring(1);
		}
		List<Object> tokens = new ArrayList<>();
		for (int cursor = 0; cursor < value.length();) {
			char current = value.charAt(cursor);
			if (current == '.') {
				cursor++;
				continue;
			}
			if (current == '[') {
				int end = value.indexOf(']', cursor);
				if (end < 0) {
					throw new IllegalArgumentException("Value path is invalid.");
				}
				String token = value.substring(cursor + 1, end).trim();
				if (token.matches("[0-9]+")) {
					tokens.add(Integer.valueOf(token));
				} else if (token.startsWith("\"") && token.endsWith("\"")) {
					tokens.add(AutomationRuntimeUtils.GSON.fromJson(token, String.class));
				} else if (token.startsWith("'") && token.endsWith("'")) {
					tokens.add(token.substring(1, token.length() - 1));
				} else {
					throw new IllegalArgumentException("Value path is invalid.");
				}
				cursor = end + 1;
				continue;
			}
			int end = cursor;
			while (end < value.length() && value.charAt(end) != '.' && value.charAt(end) != '[') {
				end++;
			}
			String token = value.substring(cursor, end);
			if (token.isEmpty()) {
				throw new IllegalArgumentException("Value path is invalid.");
			}
			tokens.add(token.matches("[0-9]+") ? Integer.valueOf(token) : token);
			cursor = end;
		}
		if (tokens.isEmpty()) {
			throw new IllegalArgumentException("Value path is invalid.");
		}
		return tokens;
	}

	private static Object nestedValue(Object data, List<Object> tokens) {
		Object current = data;
		for (Object token : tokens) {
			if (current instanceof Map<?, ?> map && token instanceof String key && map.containsKey(key)) {
				current = map.get(key);
			} else if (current instanceof List<?> list && token instanceof Integer index && index < list.size()) {
				current = list.get(index);
			} else {
				return MISSING;
			}
		}
		return current;
	}
}
