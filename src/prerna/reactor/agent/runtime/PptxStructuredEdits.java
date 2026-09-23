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
package prerna.reactor.agent.runtime;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;

/** A bounded edit plan; user/model strings are data, never executable source. */
final class PptxStructuredEdits {

	static final String TOOL = "ApplyPptxEdits";

	static Map<String, Object> definition() {
		JSONObject common = new JSONObject()
				.put("type", field("string", "replaceText, setTextColor, or setBackground").put("enum", List.of("replaceText", "setTextColor", "setBackground")))
				.put("part", field("string", "Exact selected slide part returned by PreparePptxEdit"))
				.put("objectId", field("string", "Inspected objectId; required for text operations"))
				.put("index", field("integer", "Inspected slide-wide text index; replaceText only").put("minimum", 0))
				.put("oldText", field("string", "Exact inspected text; replaceText only"))
				.put("newText", field("string", "Replacement wording; replaceText only"))
				.put("color", field("string", "Six-digit RGB, without #; color operations only"));
		JSONObject operation = new JSONObject().put("type", "object").put("properties", common)
				.put("required", List.of("type", "part")).put("additionalProperties", false);
		return new JSONObject().put("name", TOOL).put("title", "Apply selected slide edits")
				.put("description", "After PreparePptxEdit, apply text, text-color or background changes without writing code. Call alone. Supply the COMPLETE operation list on every call, including repairs; edits always start from the protected original. Colors require editType=slides. Checks preservation and automatically reviews only the selected slides. Consider foreground readability when changing a background; add explicit setTextColor operations for the affected text objects.")
				.put("inputSchema", new JSONObject().put("type", "object").put("additionalProperties", false)
						.put("properties", new JSONObject().put("operations", new JSONObject().put("type", "array")
								.put("items", operation).put("minItems", 1).put("maxItems", 200))
								.put("instructions", field("string", "Visual criteria limited to the requested edit, including readability"))
								.put("engine", field("string", "Optional exact caller-supplied vision engine ID")))
						.put("required", List.of("operations")))
				.put("_meta", new JSONObject().put("SMSS_TOOL_KIND", "semoss_pptx_workflow").put("SMSS_MCP_EXECUTION", "auto"))
				.toMap();
	}

	private static JSONObject field(String type, String description) {
		return new JSONObject().put("type", type).put("description", description);
	}

	static JSONArray validate(Object raw, JSONObject contract) {
		if (contract == null) throw new IllegalArgumentException("Call PreparePptxEdit first");
		if (!(raw instanceof List<?> list) || list.isEmpty() || list.size() > 200)
			throw new IllegalArgumentException("Supply 1 to 200 operations");
		Set<String> allowed = new HashSet<>();
		contract.getJSONArray("allowedParts").forEach(p -> allowed.add(String.valueOf(p)));
		Set<String> seen = new HashSet<>();
		JSONArray result = new JSONArray();
		for (Object value : list) {
			if (!(value instanceof Map<?, ?> map)) throw new IllegalArgumentException("Each operation must be an object");
			JSONObject op = new JSONObject(map);
			String type = op.optString("type"), part = op.optString("part");
			Set<String> keys = switch (type) {
			case "replaceText" -> Set.of("type", "part", "objectId", "index", "oldText", "newText");
			case "setTextColor" -> Set.of("type", "part", "objectId", "color");
			case "setBackground" -> Set.of("type", "part", "color");
			default -> throw new IllegalArgumentException("Unsupported edit operation: " + type);
			};
			if (!op.keySet().equals(keys)) throw new IllegalArgumentException("Expected operation fields: " + keys);
			if (!part.matches("ppt/slides/[^/]+\\.xml") || !allowed.contains(part))
				throw new IllegalArgumentException("Operation must target a prepared slide part: " + part);
			for (String key : keys) if (!"index".equals(key) && !(op.get(key) instanceof String))
				throw new IllegalArgumentException(key + " must be a string");
			if (!"setBackground".equals(type) && !op.getString("objectId").matches("[0-9]+"))
				throw new IllegalArgumentException("Use the inspected objectId");
			if ("replaceText".equals(type)) {
				Object index = op.get("index");
				if (!(index instanceof Number n) || n.doubleValue() != n.intValue() || n.intValue() < 0)
					throw new IllegalArgumentException("index must be the inspected non-negative integer");
				if (op.getString("oldText").equals(op.getString("newText"))) throw new IllegalArgumentException("Replacement must change text");
			} else {
				if (!"slides".equals(contract.getString("editType"))) throw new IllegalArgumentException("Color changes require preparing editType=slides before any build");
				if (!op.getString("color").matches("[A-Fa-f0-9]{6}")) throw new IllegalArgumentException("Color must be six hexadecimal digits without #");
			}
			String target = part + ":" + type + ":" + ("replaceText".equals(type) ? op.getInt("index") : op.optString("objectId", "background"));
			if (!seen.add(target)) throw new IllegalArgumentException("Duplicate edit target: " + target);
			result.put(op);
		}
		return result;
	}

	static String generator(String snapshot, String output, JSONArray operations) {
		JSONObject data = new JSONObject().put("input", snapshot).put("output", output).put("operations", operations);
		return "(async () => {\n const path = require('path');\n const edit = require(path.join(ROOT, '.claude/skills/pptx/scripts/structured-edit.js'));\n const args = "
				+ data + ";\n return await edit.apply({...args, JSZip: require('jszip'), input: path.join(ROOT, args.input), output: path.join(ROOT, args.output)});\n})()\n";
	}
}
