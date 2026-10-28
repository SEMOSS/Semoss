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
package prerna.reactor.model.serving;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import prerna.engine.impl.CaseInsensitiveProperties;
import prerna.engine.impl.model.serving.ServedModelGuard;
import prerna.engine.impl.model.serving.ServedModelStatus;
import prerna.util.Constants;

/**
 * How a SEMOSS model engine is tied to a model KServe serves.
 * <p>
 * An engine is <em>linked</em> to a served model by two properties of its smss,
 * {@value ServedModelGuard#SERVED_MODEL_PROPERTY} and
 * {@value ServedModelGuard#NAMESPACE_PROPERTY}. CreateServedModelEngine writes
 * them, and they let the engine of a stopped model say so
 * ({@link ServedModelGuard}). An engine <em>calls</em> a served model when its
 * MODEL is the served model's name (the name vLLM serves it as) or its ENDPOINT
 * is the model's endpoint. An engine made by hand calls a model without being
 * linked to it, and can be linked afterwards.
 * <p>
 * Everything here works on values and files, without SEMOSS's engine registry
 * ({@link ServedModelEngineStore}), so it can be tested on its own.
 */
final class ServedModelEngineLinks {

	static final String MODEL = "MODEL";
	static final String ENDPOINT = "ENDPOINT";

	/** A line of an smss file that sets one of the two link properties */
	private static final Pattern LINK_LINE = Pattern.compile("\\s*(" + ServedModelGuard.SERVED_MODEL_PROPERTY + "|"
			+ ServedModelGuard.NAMESPACE_PROPERTY + ")(\\s|=|:).*|\\s*(" + ServedModelGuard.SERVED_MODEL_PROPERTY
			+ "|" + ServedModelGuard.NAMESPACE_PROPERTY + ")\\s*", Pattern.CASE_INSENSITIVE);

	/**
	 * A model engine as its smss describes it.
	 *
	 * @param servedModel the served model it is linked to, or null
	 * @param namespace   the KServe namespace of that model, or null
	 */
	record ModelEngine(String engineId, String engineName, String endpoint, String model, String servedModel,
			String namespace, Path smss) {

		boolean isLinked() {
			return this.servedModel != null;
		}

		/** The same engine, linked to a served model, or unlinked when it is null */
		ModelEngine withLink(String linkedNamespace, String linkedModel) {
			return new ModelEngine(this.engineId, this.engineName, this.endpoint, this.model, linkedModel,
					linkedModel == null ? null : linkedNamespace, this.smss);
		}

		/** For the reactors: the engine, what it is linked to, and the served models it calls */
		Map<String, Object> toMap(List<String> calls) {
			Map<String, Object> map = new LinkedHashMap<>();
			map.put("engineId", this.engineId);
			map.put("engineName", this.engineName);
			map.put("endpoint", this.endpoint);
			map.put("model", this.model);
			map.put("servedModel", this.servedModel);
			map.put("namespace", this.namespace);
			map.put("calls", List.copyOf(calls));
			return map;
		}
	}

	private ServedModelEngineLinks() {
	}

	/** The engine an smss file describes; it is named by its display name, else its alias. */
	static ModelEngine read(String engineId, Path smss) throws IOException {
		CaseInsensitiveProperties properties = new CaseInsensitiveProperties();
		// smss files are properties files, which are ISO-8859-1
		try (Reader reader = Files.newBufferedReader(smss, StandardCharsets.ISO_8859_1)) {
			properties.load(reader);
		}
		String name = blankToNull(properties.getProperty(Constants.ENGINE_DISPLAY_NAME));
		if (name == null) {
			name = blankToNull(properties.getProperty(Constants.ENGINE_ALIAS));
		}
		String servedModel = blankToNull(properties.getProperty(ServedModelGuard.SERVED_MODEL_PROPERTY));
		return new ModelEngine(engineId, name == null ? engineId : name,
				blankToNull(properties.getProperty(ENDPOINT)), blankToNull(properties.getProperty(MODEL)),
				servedModel, servedModel == null ? null
						: blankToNull(properties.getProperty(ServedModelGuard.NAMESPACE_PROPERTY)),
				smss);
	}

	/** The served models the engine calls: by its MODEL, or by its ENDPOINT (either of a model's URLs). */
	static List<String> calls(ModelEngine engine, List<ServedModelStatus> models) {
		List<String> calls = new ArrayList<>();
		String endpoint = normalizeUrl(engine.endpoint());
		for (ServedModelStatus model : models) {
			String name = model.getServedModel();
			boolean byName = name != null && name.equals(engine.model());
			boolean byEndpoint = endpoint != null && (endpoint.equals(normalizeUrl(model.getEndpoint()))
					|| endpoint.equals(normalizeUrl(model.getInClusterUrl())));
			if (byName || byEndpoint) {
				calls.add(name);
			}
		}
		return calls;
	}

	/** Why an engine cannot be linked to a model it does not call, and what to change. */
	static String mismatch(ModelEngine engine, ServedModelStatus model) {
		StringBuilder text = new StringBuilder(engine.engineName()).append(" calls ")
				.append(engine.model() == null ? "no model" : "the model " + engine.model())
				.append(engine.endpoint() == null ? "" : " at " + engine.endpoint()).append(", not ")
				.append(model.getServedModel()).append(". Set its ").append(MODEL).append(" to ")
				.append(model.getServedModel());
		if (model.getEndpoint() != null) {
			text.append(" and its ").append(ENDPOINT).append(" to ").append(model.getEndpoint());
		}
		return text.append(" (its smss, in Model Settings), then link it.").toString();
	}

	/** A URL compared without case, surrounding blanks or a trailing slash; null when blank. */
	static String normalizeUrl(String url) {
		String text = blankToNull(url);
		if (text == null) {
			return null;
		}
		while (text.endsWith("/")) {
			text = text.substring(0, text.length() - 1);
		}
		return text.toLowerCase(Locale.ROOT);
	}

	/**
	 * The smss text with the link set to the served model, or removed when it is
	 * null. Every other line stays as it was, comments and secrets included, and so
	 * do the file's line endings.
	 */
	static String withLink(String smss, String namespace, String servedModel) {
		if (servedModel != null && (namespace == null || !isSafeValue(namespace) || !isSafeValue(servedModel))) {
			throw new IllegalArgumentException("A link needs a namespace and a served model name on one line each");
		}
		String newline = smss.contains("\r\n") ? "\r\n" : "\n";
		List<String> lines = new ArrayList<>(List.of(smss.split("\r?\n", -1)));
		boolean hadLink = lines.removeIf(line -> LINK_LINE.matcher(line).matches());
		if (!hadLink && servedModel == null) {
			// nothing to unlink: the file stays exactly as it is
			return smss;
		}
		// a file that ends with a newline splits into a last, empty element
		boolean endsWithNewline = !lines.isEmpty() && lines.get(lines.size() - 1).isEmpty();
		if (endsWithNewline) {
			lines.remove(lines.size() - 1);
		}
		if (servedModel != null) {
			lines.add(ServedModelGuard.NAMESPACE_PROPERTY + "\t" + namespace);
			lines.add(ServedModelGuard.SERVED_MODEL_PROPERTY + "\t" + servedModel);
		}
		return String.join(newline, lines) + (endsWithNewline || servedModel != null ? newline : "");
	}

	/**
	 * Write the link into an smss file, or remove it when the served model is null.
	 *
	 * @return false when the file already said so, and was left alone
	 */
	static boolean writeLink(Path smss, String namespace, String servedModel) throws IOException {
		String before = Files.readString(smss, StandardCharsets.ISO_8859_1);
		String after = withLink(before, namespace, servedModel);
		if (after.equals(before)) {
			return false;
		}
		Files.writeString(smss, after, StandardCharsets.ISO_8859_1);
		return true;
	}

	private static boolean isSafeValue(String value) {
		return !value.isBlank() && value.indexOf('\n') < 0 && value.indexOf('\r') < 0 && value.indexOf('\\') < 0;
	}

	private static String blankToNull(String value) {
		return value == null || value.isBlank() ? null : value.strip();
	}
}
