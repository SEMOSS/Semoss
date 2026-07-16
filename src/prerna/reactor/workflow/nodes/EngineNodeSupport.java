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
package prerna.reactor.workflow.nodes;

import java.util.Map;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import prerna.reactor.workflow.WorkflowExecutionUtils;

/**
 * Shared helpers for the 5 engine-type node executors (database/model/vector/storage/function-
 * engine). Each of those builds a validated Pixel call from structured {@code node.config} on
 * the backend - reusing the target reactor's existing security/validation/query-building logic
 * unmodified by running it through the normal Pixel path - rather than trusting a frontend-
 * precompiled {@code builtPixel} string (see ticket #2743).
 *
 * <p>Every templated/dynamic value is wrapped in {@code <encode>...</encode>}, not just the
 * "obviously free text" fields (command/query/prompt) that the frontend's preview-only
 * equivalent ({@code buildPixelPreview()} in {@code workflow-utils.ts}) encodes. Any config field
 * can carry a {@code ${var}} reference to an upstream node's output - which may itself be
 * LLM-generated or attacker-influenced content - so any field that isn't a fixed structural
 * literal (an engine id selected from a dropdown is still encoded defensively) gets the same
 * injection protection. This is deliberately more conservative than the frontend preview
 * builder - see epic sub-issue #2750 (verifying encode-wrapping coverage) for the general
 * concern this addresses.
 *
 * <p>The exception is fields that must be embedded as a raw, unquoted Pixel map/list literal
 * (e.g. {@code paramValues}, {@code metadata}) because the target reactor's key expects an
 * actual parsed Map/List noun, not a String - {@code <encode>} would change how the parser
 * types the literal. Those fields go through {@link #resolveAndValidateJsonLiteral} instead:
 * {@code ${var}} substitution happens first (on the raw field, not the whole assembled pixel
 * string), then the result is validated as syntactically complete, balanced JSON before it is
 * spliced in unquoted. This is a real defense, not just documentation of the gap - a resolved
 * value that doesn't parse as balanced JSON (e.g. one containing {@code "], SomeReactor(x=["})
 * is rejected outright rather than silently embedded, so it cannot break out of the literal's
 * boundaries and inject arbitrary Pixel syntax.
 */
final class EngineNodeSupport {

	private EngineNodeSupport() {
		// static utility - no instantiation
	}

	/** Wraps a value as an {@code <encode>}-protected Pixel string literal, e.g. {@code "<encode>foo</encode>"}. */
	static String encoded(Object value) {
		return "\"<encode>" + (value != null ? value : "") + "</encode>\"";
	}

	/**
	 * Resolves {@code ${var}} references in a raw config value, then wraps the already-resolved
	 * result in {@code <encode>...</encode>}. Every engine-type executor resolves each field
	 * individually this way and builds its Pixel call from already-resolved pieces - matching
	 * the established convention elsewhere in this package (e.g. {@code EmailNodeExecutor}) -
	 * rather than assembling a still-templated pixel string and resolving it as one final pass.
	 * Resolving per-field first, and never resolving the assembled string a second time, avoids
	 * a subtle double-substitution risk: if a second whole-string resolve pass ran after this
	 * value was already embedded, and the resolved content happened to itself contain a literal
	 * {@code ${...}} sequence matching a scope key (e.g. upstream data that isn't a template but
	 * looks like one), it would get incorrectly re-substituted.
	 */
	static String resolveEncoded(String rawTemplate, Map<String, String> scope, Map<String, String> configMap) {
		return encoded(WorkflowExecutionUtils.resolve(rawTemplate, scope, configMap));
	}

	/**
	 * Resolves {@code ${var}} references in a raw config value that will be spliced into a
	 * Pixel map/list literal position <em>unquoted</em> (e.g. {@code paramValues=[<result>]}),
	 * then validates the resolved text is syntactically complete, balanced JSON (an object or
	 * array). Throws rather than returning unvalidated content, since a value that isn't
	 * well-formed, self-contained JSON could contain a sequence that breaks out of the literal
	 * and injects arbitrary Pixel syntax once embedded into the assembled pixel string.
	 */
	static String resolveAndValidateJsonLiteral(String rawTemplate, Map<String, String> scope,
			Map<String, String> configMap, String fieldName, String nodeTypeLabel, String nodeLabel) {
		String resolved = WorkflowExecutionUtils.resolve(rawTemplate, scope, configMap);
		try {
			JsonElement el = JsonParser.parseString(resolved);
			if (!el.isJsonObject() && !el.isJsonArray()) {
				throw new IllegalArgumentException("must be a JSON object or array, got: " + resolved);
			}
		} catch (JsonSyntaxException | IllegalArgumentException e) {
			throw new IllegalArgumentException(nodeTypeLabel + " node \"" + nodeLabel + "\": '" + fieldName +
					"' did not resolve to valid, complete JSON after substituting ${var} references (" +
					e.getMessage() + ") - refusing to embed unvalidated content into the Pixel call", e);
		}
		return resolved;
	}

	/**
	 * Resolves {@code ${var}} references in a raw config value that will be embedded as a
	 * quoted, quote-escaped Pixel string (e.g. {@code map=["<result>"]}, used by
	 * {@code FunctionEngineNodeExecutor}), then escapes the resolved text for that position.
	 * Same ordering rationale as {@link #resolveAndValidateJsonLiteral}: resolve the field alone
	 * first, escape the actual resolved content, then splice - not escape-then-resolve, which
	 * would let a substituted value's own quotes reach the pixel string unescaped and break out
	 * of the surrounding {@code "..."} boundary.
	 */
	static String resolveAndEscapeForQuotedPixelString(String rawTemplate, Map<String, String> scope,
			Map<String, String> configMap) {
		String resolved = WorkflowExecutionUtils.resolve(rawTemplate, scope, configMap);
		return resolved.replace("\\", "\\\\").replace("\"", "\\\"");
	}

	/**
	 * Reads a required config field as a String, throwing a clear, node-labeled error if it's
	 * missing or blank - backend-side re-validation, since a workflow.json can be edited directly
	 * (API call bypassing the FE, or a future FE bug) and must never be trusted implicitly.
	 */
	static String required(Map<String, Object> config, String key, String nodeTypeLabel, String nodeLabel) {
		Object v = config.get(key);
		if (v == null || v.toString().isBlank()) {
			throw new IllegalArgumentException(nodeTypeLabel + " node \"" + nodeLabel + "\": '" + key + "' is required");
		}
		return v.toString();
	}

	/** Reads an optional config field as a String, or {@code def} if missing/blank. */
	static String optional(Map<String, Object> config, String key, String def) {
		Object v = config.get(key);
		return (v == null || v.toString().isBlank()) ? def : v.toString();
	}

	/** Reads an optional config field as a String, or {@code null} if missing/blank. */
	static String optional(Map<String, Object> config, String key) {
		return optional(config, key, null);
	}

	/** Reads an optional config field as an int, or {@code def} if missing/blank/unparseable. */
	static int optionalInt(Map<String, Object> config, String key, int def) {
		Object v = config.get(key);
		if (v == null) return def;
		try {
			return Integer.parseInt(v.toString().trim());
		} catch (NumberFormatException e) {
			return def;
		}
	}

	/** Ensures a Pixel statement string ends with a semicolon, as the parser requires. */
	static String terminated(String pixel) {
		String trimmed = pixel.trim();
		return trimmed.endsWith(";") ? trimmed : trimmed + ";";
	}
}
