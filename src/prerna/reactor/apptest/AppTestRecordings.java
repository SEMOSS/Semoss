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
package prerna.reactor.apptest;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.util.AssetUtility;
import prerna.util.Utility;

/** Declarative test definitions live in non-public project assets; reports remain private. */
final class AppTestRecordings {

	private static final Gson GSON = new Gson();
	private static final Set<String> TYPES = Set.of("CLICK", "FILL", "SELECT", "CHECK", "KEY", "SCROLL",
			"ASSERT_TEXT", "ASSERT_VISIBLE", "NAVIGATE");
	static final int MAX_STEPS = 300;
	/** When true, a saved recording may only target elements by data-testid. */
	static final String REQUIRE_TESTID_PROP = "APP_TEST_REQUIRE_TESTID";
	// a tag name is optional; the value is a quoted attribute value
	private static final Pattern TESTID_SELECTOR = Pattern
			.compile("^(?:[a-zA-Z][a-zA-Z0-9-]*)?\\[data-test(?:id)?=\"(?:[^\"\\\\]|\\\\.)+\"\\]$");
	static final int MAX_RECORDINGS = 25;
	private static final int MAX_BYTES = 256_000;

	private AppTestRecordings() { }

	static Path folder(String projectId) throws IOException {
		Path destination = Path.of(AssetUtility.getProjectAssetsFolder(projectId), "app-tests");
		migrate(legacyFolder(projectId), destination);
		migrate(destination, destination);
		return destination;
	}

	/** Reports are intentionally kept out of the editable/versioned asset tree. */
	static Path legacyFolder(String projectId) {
		return Path.of(AssetUtility.getProjectAppRootFolder(projectId), "app-tests");
	}

	/** Move legacy definitions only; never replace a different file or relocate run evidence. */
	static synchronized void migrate(Path previous, Path destination) throws IOException {
		Path source = previous.resolve("recordings");
		if (!Files.isDirectory(source)) return;
		Path target = destination.resolve("tests");
		Files.createDirectories(target);
		try (var files = Files.list(source)) {
			for (Path file : files.filter(p -> p.getFileName().toString().endsWith(".json")).sorted().toList()) {
				Path next = target.resolve(file.getFileName());
				if (Files.exists(next)) {
					if (Files.mismatch(file, next) != -1) {
						throw new IOException("Conflicting app test recording: " + file.getFileName()
								+ ". Resolve the legacy and assets copies before continuing.");
					}
				} else {
					Files.copy(file, next, StandardCopyOption.COPY_ATTRIBUTES);
				}
				Files.delete(file);
			}
		}
		boolean isEmpty;
		try (var remaining = Files.list(source)) {
			isEmpty = remaining.findAny().isEmpty();
		}
		if (isEmpty) Files.delete(source);
	}

	static String id(String value) {
		try {
			String normalized = UUID.fromString(value).toString();
			if (!normalized.equals(value)) throw new IllegalArgumentException();
			return normalized;
		} catch (RuntimeException e) {
			throw new IllegalArgumentException("Invalid recording id");
		}
	}

	/** Only paths relative to the app portal are accepted, including hash routes. */
	static String relativePath(Object raw) {
		String value = raw == null ? "" : String.valueOf(raw);
		if (value.length() > 2048 || value.indexOf('\\') >= 0 || value.chars().anyMatch(c -> c < 32)
				|| value.startsWith("/")) {
			throw new IllegalArgumentException("Navigation must be a relative path inside this app");
		}
		try {
			URI uri = URI.create(value);
			if (uri.isAbsolute() || uri.getRawAuthority() != null) throw new IllegalArgumentException();
			String decodedPath = uri.getPath();
			if (decodedPath == null || decodedPath.startsWith("/") || decodedPath.contains("\\") || decodedPath.contains("%")) throw new IllegalArgumentException();
			for (String part : decodedPath.split("/")) {
				if (part.equals("..") || part.equals(".")) throw new IllegalArgumentException();
			}
		} catch (RuntimeException e) {
			throw new IllegalArgumentException("Navigation must be a relative path inside this app");
		}
		return value;
	}

	static Map<String, Object> validate(Map<String, Object> input) {
		if (input == null || GSON.toJson(input).getBytes(StandardCharsets.UTF_8).length > MAX_BYTES) {
			throw new IllegalArgumentException("Recording is missing or exceeds 256 KB");
		}
        // Old experimental database-bound recordings must never become normal-connection tests.
        if (input.containsKey("engineTestConfigurationId"))
            throw new IllegalArgumentException("This recording requires experimental database routing, which is not supported by app regression testing v2");
        Map<String, Object> out = new LinkedHashMap<>();
		if (input.containsKey("version") && (!(input.get("version") instanceof Number version) || version.doubleValue() != 1)) {
			throw new IllegalArgumentException("Unsupported recording version");
		}
		out.put("id", input.containsKey("id") ? id(String.valueOf(input.get("id"))) : UUID.randomUUID().toString());
		out.put("version", 1);
		String name = text(input.get("name"), "name", 120);
		if (name.equals(AppTestRuns.PLATFORM_SMOKE_SUITE)) throw new IllegalArgumentException("This suite name is reserved for the platform smoke check");
		out.put("name", name);
		out.put("description", text(input.get("description"), "description", 2000).trim());
		out.put("startPath", relativePath(input.get("startPath")));
		Object rawSteps = input.get("steps");
		if (!(rawSteps instanceof List<?> steps) || steps.isEmpty() || steps.size() > MAX_STEPS) {
			throw new IllegalArgumentException("Recording must have between 1 and " + MAX_STEPS + " steps");
		}
		List<Map<String, Object>> normalized = new ArrayList<>();
		for (Object raw : steps) {
			if (!(raw instanceof Map<?, ?> step)) throw new IllegalArgumentException("Invalid recording step");
			String type = text(step.get("type"), "type", 30);
			if (!TYPES.contains(type)) throw new IllegalArgumentException("Unsupported recording step type");
			Map<String, Object> copy = new LinkedHashMap<>();
			copy.put("type", type);
			if (step.containsKey("pagePath")) copy.put("pagePath", relativePath(step.get("pagePath")));
			if (type.equals("NAVIGATE")) {
				copy.put("path", relativePath(step.get("path")));
			} else {
				copy.put("selector", text(step.get("selector"), "selector", 2000));
			}
			List<String> frames = new ArrayList<>();
			if (step.get("framePath") != null) {
				if (!(step.get("framePath") instanceof List<?> list) || list.size() > 5) {
					throw new IllegalArgumentException("Invalid framePath");
				}
				for (Object frame : list) frames.add(text(frame, "frame selector", 2000));
			}
			copy.put("framePath", frames);
			if (type.equals("FILL") || type.equals("SELECT")) {
				Object value = step.get("value");
				if (!(value instanceof String) || ((String) value).length() > 10_000) {
					throw new IllegalArgumentException("Invalid step value");
				}
				copy.put("value", value);
			}
			if (type.equals("ASSERT_TEXT")) {
				copy.put("text", text(step.get("text"), "assertion text", 10_000));
				if (step.get("exact") != null) {
					if (!(step.get("exact") instanceof Boolean)) throw new IllegalArgumentException("Invalid exact flag");
					copy.put("exact", step.get("exact"));
				}
			}
			if (type.equals("KEY")) copy.put("key", text(step.get("key"), "key", 100));
			if (type.equals("CHECK")) {
				if (!(step.get("checked") instanceof Boolean)) throw new IllegalArgumentException("Missing checked state");
				copy.put("checked", step.get("checked"));
			}
			if (type.equals("SCROLL")) {
				for (String axis : List.of("x", "y")) {
					Object coordinate = step.getOrDefault(axis, null);
					if (!(coordinate instanceof Number number) || !Double.isFinite(number.doubleValue())
							|| Math.abs(number.doubleValue()) > 10_000_000) throw new IllegalArgumentException("Invalid scroll position");
					copy.put(axis, number.doubleValue());
				}
			}
			normalized.add(copy);
		}
		out.put("steps", normalized);
		out.put("updatedAt", Instant.now().toString());
		return out;
	}

	private static String text(Object raw, String field, int max) {
		if (!(raw instanceof String value) || value.isBlank() || value.length() > max) {
			throw new IllegalArgumentException("Invalid recording " + field);
		}
		return value;
	}

	/** True for a selector that targets one element by its data-testid or data-test. */
	static boolean stableSelector(String selector) {
		return selector != null && TESTID_SELECTOR.matcher(selector).matches();
	}

	@SuppressWarnings("unchecked")
	private static void requireStableTargets(Map<String, Object> recording) {
		if (!Boolean.parseBoolean(Utility.getDIHelperProperty(REQUIRE_TESTID_PROP))) return;
		int number = 0;
		for (Map<String, Object> step : (List<Map<String, Object>>) recording.get("steps")) {
			number++;
			if (step.get("selector") != null && !stableSelector(String.valueOf(step.get("selector")))) {
				throw new IllegalArgumentException("Step " + number + " does not target a data-testid. "
						+ "Add a unique data-testid to the element and use [data-testid=\"name\"] as the selector.");
			}
		}
	}

	static synchronized Map<String, Object> save(Path base, Map<String, Object> input) throws IOException {
		Map<String, Object> recording = validate(input);
		requireStableTargets(recording);
		Path dir = base.resolve("tests");
		Files.createDirectories(dir);
		Path file = dir.resolve(recording.get("id") + ".json");
		if (!Files.exists(file) && list(base).size() >= MAX_RECORDINGS) {
			throw new IllegalArgumentException("An app can have at most " + MAX_RECORDINGS + " recordings");
		}
		write(file, recording);
		return recording;
	}

	/** Discovery exposes metadata only; test bodies never enter list responses. */
	static synchronized List<Map<String, Object>> summaries(Path base) throws IOException {
		return list(base).stream().map(recording -> Map.of(
				"id", recording.get("id"), "name", recording.get("name"),
				"description", recording.get("description"))).toList();
	}

	/** Read a single UUID file without parsing any other test in the app. */
	static synchronized Map<String, Object> get(Path base, String recordingId) throws IOException {
		Path file = base.resolve("tests").resolve(id(recordingId) + ".json");
		if (!Files.isRegularFile(file)) throw new IllegalArgumentException("Recording does not exist in this app: " + recordingId);
		return read(file);
	}

	static synchronized List<Map<String, Object>> list(Path base) throws IOException {
		Path dir = base.resolve("tests");
		if (!Files.isDirectory(dir)) return List.of();
		List<Map<String, Object>> recordings = new ArrayList<>();
		try (var files = Files.list(dir)) {
			for (Path file : files.filter(p -> p.getFileName().toString().endsWith(".json")).sorted().toList()) {
				recordings.add(read(file));
				if (recordings.size() > MAX_RECORDINGS) throw new IOException("Recording count exceeds limit");
			}
		}
		return recordings;
	}

	private static Map<String, Object> read(Path file) throws IOException {
		if (Files.size(file) > MAX_BYTES) throw new IOException("Recording exceeds size limit");
		try {
			Map<String, Object> recording = GSON.fromJson(Files.readString(file), new TypeToken<Map<String, Object>>() { }.getType());
			// Older version-1 recordings predate descriptions; migrate only after validation.
			if (recording == null) throw new IllegalArgumentException("Missing recording");
			boolean needsDescription = !recording.containsKey("description");
			if (needsDescription) recording.put("description", "Replays the recorded workflow: " + recording.get("name") + ".");
			Map<String, Object> validated = validate(recording);
			if (!file.getFileName().toString().equals(validated.get("id") + ".json")) {
				throw new IllegalArgumentException("Recording id must match its filename");
			}
			if (recording.get("updatedAt") instanceof String updatedAt) validated.put("updatedAt", updatedAt);
			if (needsDescription) write(file, validated);
			return validated;
		} catch (RuntimeException e) {
			throw new IOException("Saved recording " + file.getFileName() + " is invalid; review it before running app tests", e);
		}
	}

	static synchronized boolean delete(Path base, String recordingId) throws IOException {
		return Files.deleteIfExists(base.resolve("tests").resolve(id(recordingId) + ".json"));
	}

	static void write(Path file, Object value) throws IOException {
		Files.createDirectories(file.getParent());
		Path temporary = Files.createTempFile(file.getParent(), "app-test-", ".tmp");
		try {
			Files.writeString(temporary, GSON.toJson(value), StandardCharsets.UTF_8);
			try {
				Files.move(temporary, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
			} catch (java.nio.file.AtomicMoveNotSupportedException e) {
				Files.move(temporary, file, StandardCopyOption.REPLACE_EXISTING);
			}
		} finally {
			Files.deleteIfExists(temporary);
		}
	}
}
