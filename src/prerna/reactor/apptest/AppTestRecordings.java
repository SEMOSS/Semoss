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
	static final Set<String> TYPES = Set.copyOf(AppTestSchema.TYPES);
	static final int MAX_STEPS = 300;
	/** When true, a saved recording may only target elements by data-testid. */
	static final String REQUIRE_TESTID_PROP = "APP_TEST_REQUIRE_TESTID";
	// a tag name is optional; the value is a quoted attribute value
	private static final Pattern TESTID_SELECTOR = Pattern
			.compile("^(?:[a-zA-Z][a-zA-Z0-9-]*)?\\[data-test(?:id)?=\"(?:[^\"\\\\]|\\\\.)+\"\\]$");
	static final int MAX_RECORDINGS = 25;
    static final int MAX_BYTES = 256_000;

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
			throw new IllegalArgumentException("Invalid recording id: use the canonical lowercase UUID returned by SaveAppTestRecording or ListAppTestRecordings, not a test name or app id. Omit id when saving a new test.");
		}
	}

	/** Only paths relative to the app portal are accepted, including hash routes. */
	static String relativePath(Object raw) {
		String value = raw == null ? "" : String.valueOf(raw);
		if (value.length() > 2048 || value.indexOf('\\') >= 0 || value.chars().anyMatch(c -> c < 32)
				|| value.startsWith("/")) {
			throw new IllegalArgumentException("Navigation must be a relative path inside this app, for example index.html#/boards/b1 or #/boards/b1; use an empty string for the portal root. Do not use a leading slash, full URL or .. segments");
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
			throw new IllegalArgumentException("Navigation must be a relative path inside this app, for example index.html#/boards/b1 or #/boards/b1; use an empty string for the portal root. Do not use a leading slash, full URL or .. segments");
		}
		return value;
	}

	private static String relativeField(Object raw, String field) {
		try {
			return relativePath(raw);
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException(field + ": " + e.getMessage(), e);
		}
	}

	static Map<String, Object> validate(Map<String, Object> input) {
        if (input == null) throw new IllegalArgumentException("recording must be a JSON object, not a JSON string; pass name, description and steps as object fields");
        if (GSON.toJson(input).getBytes(StandardCharsets.UTF_8).length > MAX_BYTES) throw new IllegalArgumentException("Recording exceeds 256 KB");
        if (input.containsKey("engineTestConfigurationId")) throw new IllegalArgumentException("This recording requires experimental database routing, which is not supported by app regression testing v2");
        if (input.containsKey("version") && (!(input.get("version") instanceof Number n) || n.doubleValue() != 1)) throw new IllegalArgumentException("Unsupported recording version; expected 1");
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", input.containsKey("id") ? id(String.valueOf(input.get("id"))) : UUID.randomUUID().toString());
        out.put("version", 1);
        String name = text(input.get("name"), "name", 120);
        if (name.equals(AppTestRuns.PLATFORM_SMOKE_SUITE)) throw new IllegalArgumentException("This suite name is reserved for the platform smoke check");
        out.put("name", name);
        out.put("description", text(input.get("description"), "description", 2000).trim());
        out.put("startPath", relativeField(input.get("startPath"), "startPath"));
        if (input.containsKey("routeMatch")) {
            if (!List.of("EXACT", "PORTAL_ROOT").contains(input.get("routeMatch"))) throw new IllegalArgumentException("routeMatch must be EXACT or PORTAL_ROOT; queries always remain significant");
            out.put("routeMatch", input.get("routeMatch"));
        }
        if (input.containsKey("useVariables")) {
            if (!(input.get("useVariables") instanceof Boolean)) throw new IllegalArgumentException("useVariables must be boolean");
            out.put("useVariables", input.get("useVariables"));
        }
        out.put("steps", validateSteps(input.get("steps"), "Step", 1, MAX_STEPS));
        if (input.containsKey("cleanupSteps")) out.put("cleanupSteps", validateSteps(input.get("cleanupSteps"), "Cleanup step", 0, 50));
        if (Boolean.TRUE.equals(out.get("useVariables"))) {
            var variablePattern = Pattern.compile("\\{\\{([^{}]+)\\}\\}");
            for (String section : List.of("steps","cleanupSteps")) if (out.get(section) instanceof List<?> steps) for (Object value : steps) {
                Map<?,?> step = (Map<?,?>)value;
                for (String field : List.of("value","text","selector","targetSelector","path","pagePath")) if (step.get(field) instanceof String expression) {
                    var matcher = variablePattern.matcher(expression);
                    while (matcher.find()) if (!List.of("runId","testId","attemptId","uniqueId").contains(matcher.group(1))) throw new IllegalArgumentException("Unknown variable in " + section + "." + field + ": use {{runId}}, {{testId}}, {{attemptId}} or {{uniqueId}}; disable useVariables for literal braces");
                }
            }
        }
        out.put("updatedAt", Instant.now().toString());
        return out;
    }

    private static List<Map<String, Object>> validateSteps(Object raw, String prefix, int minimum, int maximum) {
        if (!(raw instanceof List<?> steps) || steps.size() < minimum || steps.size() > maximum)
            throw new IllegalArgumentException(prefix + " list must have between " + minimum + " and " + maximum + " steps");
        List<Map<String, Object>> normalized = new ArrayList<>();
        for (Object item : steps) {
            try {
                if (!(item instanceof Map<?, ?> step)) throw new IllegalArgumentException("expected an object with type and action fields");
                if (!(step.get("type") instanceof String type) || !TYPES.contains(type))
                    throw new IllegalArgumentException("Unsupported recording step type '" + step.get("type") + "'. Supported types: " + String.join(", ", AppTestSchema.TYPES) + " (uppercase). NAVIGATE uses path, not url");
                Map<String, Object> copy = new LinkedHashMap<>();
                copy.put("type", type);
                if (step.containsKey("pagePath")) copy.put("pagePath", relativeField(step.get("pagePath"), "pagePath"));
                if (List.of("NAVIGATE", "ASSERT_URL").contains(type)) {
                    if (!(step.get("path") instanceof String)) throw new IllegalArgumentException(type + " requires a string path (not url); use an explicit empty string for the portal root");
                    copy.put("path", relativeField(step.get("path"), "path"));
                } else copy.put("selector", text(step.get("selector"), "selector", 2000));
                List<String> frames = new ArrayList<>();
                if (step.get("framePath") != null) {
                    if (!(step.get("framePath") instanceof List<?> list) || list.size() > 5) throw new IllegalArgumentException("framePath is only for nested iframes: use at most 5 CSS selector strings; scope an element with a descendant selector instead");
                    for (Object frame : list) {
                        if (!(frame instanceof String)) throw new IllegalArgumentException("framePath is only for nested iframes: each entry must be a CSS selector string, not an object; use a descendant selector to scope a lane or row");
                        frames.add(text(frame, "frame selector", 2000));
                    }
                }
                copy.put("framePath", frames);
                if (List.of("FILL", "SELECT", "ASSERT_VALUE").contains(type)) {
                    if (!(step.get("value") instanceof String value) || value.length() > 10_000) throw new IllegalArgumentException("value must be a string of at most 10000 characters");
                    copy.put("value", value);
                }
                if (type.equals("ASSERT_TEXT")) {
                    copy.put("text", text(step.get("text"), "assertion text", 10000));
                    if (step.containsKey("exact")) {
                        if (!(step.get("exact") instanceof Boolean)) throw new IllegalArgumentException("exact must be boolean");
                        copy.put("exact", step.get("exact"));
                    }
                }
                if (List.of("CHECK", "ASSERT_CHECKED").contains(type)) {
                    if (!(step.get("checked") instanceof Boolean)) throw new IllegalArgumentException("checked must be boolean");
                    copy.put("checked", step.get("checked"));
                }
                if (type.equals("ASSERT_COUNT")) copy.put("count", whole(step.get("count"), "count", 0, 10000));
                if (type.equals("KEY")) copy.put("key", text(step.get("key"), "key", 100));
                if (type.equals("DRAG")) copy.put("targetSelector", text(step.get("targetSelector"), "targetSelector", 2000));
                if (type.equals("WAIT_FOR")) {
                    if (!List.of("visible", "hidden", "attached", "detached").contains(step.get("state"))) throw new IllegalArgumentException("state must be visible, hidden, attached or detached");
                    copy.put("state", step.get("state"));
                }
                if (step.containsKey("timeoutMs")) copy.put("timeoutMs", whole(step.get("timeoutMs"), "timeoutMs", 100, 30000));
                if (type.equals("SCROLL")) for (String axis : List.of("x", "y")) {
                    if (!(step.get(axis) instanceof Number n) || !Double.isFinite(n.doubleValue()) || Math.abs(n.doubleValue()) > 10000000) throw new IllegalArgumentException(axis + " must be a finite scroll position from -10000000 to 10000000");
                    copy.put(axis, n.doubleValue());
                }
                normalized.add(copy);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(prefix + " " + (normalized.size() + 1) + ": " + e.getMessage(), e);
            }
        }
        return normalized;
    }

    private static int whole(Object value, String name, int min, int max) {
        if (!(value instanceof Number n) || !Double.isFinite(n.doubleValue()) || n.doubleValue() != n.intValue() || n.intValue() < min || n.intValue() > max)
            throw new IllegalArgumentException(name + " must be a whole number from " + min + " to " + max);
        return n.intValue();
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
		List<Map<String, Object>> targets = new ArrayList<>((List<Map<String, Object>>) recording.get("steps"));
        if (recording.get("cleanupSteps") instanceof List<?> cleanup) targets.addAll((List<Map<String,Object>>) cleanup);
		for (Map<String, Object> step : targets) {
			number++;
			if ((step.get("selector") != null && !stableSelector(String.valueOf(step.get("selector"))))
                    || (step.get("targetSelector") != null && !stableSelector(String.valueOf(step.get("targetSelector"))))) {
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
		if (!Files.exists(file) && countFiles(dir) >= MAX_RECORDINGS) {
			throw new IllegalArgumentException("An app can have at most " + MAX_RECORDINGS + " recordings");
		}
		write(file, recording);
		return recording;
	}

	/** Counts recording files without parsing them, so an invalid neighbour cannot block saving a new one. */
	private static long countFiles(Path dir) throws IOException {
		try (var files = Files.list(dir)) {
			return files.filter(p -> p.getFileName().toString().endsWith(".json")).count();
		}
	}

	/** Discovery exposes metadata only; test bodies never enter list responses. */
	static synchronized List<Map<String, Object>> summaries(Path base) throws IOException {
		return list(base).stream().map(recording -> Map.of(
				"id", recording.get("id"), "name", recording.get("name"),
				"description", recording.get("description"))).toList();
	}

    /** Metadata-only valid entries plus explicit malformed files; a bad neighbour cannot hide valid tests. */
    static synchronized Map<String, Object> discover(Path base) throws IOException {
        List<Map<String, Object>> valid = new ArrayList<>(), invalid = new ArrayList<>();
        Path dir = base.resolve("tests");
        if (Files.isDirectory(dir)) try (var files = Files.list(dir)) {
            for (Path file : files.filter(p -> p.getFileName().toString().endsWith(".json")).sorted().toList()) {
                try {
                    if (Files.isSymbolicLink(file) || !Files.isRegularFile(file)) throw new IOException("Test must be a regular file, not a symlink");
                    Map<String,Object> definition = read(file);
                    valid.add(Map.of("id", definition.get("id"), "name", definition.get("name"), "description", definition.get("description")));
                } catch (IOException | RuntimeException failure) {
                    invalid.add(Map.of("file", file.getFileName().toString(), "code", "INVALID_DEFINITION", "reason", String.valueOf(failure.getMessage())));
                }
            }
        }
        return Map.of("recordings", valid, "invalid", invalid);
    }

	/** Read a single UUID file without parsing any other test in the app. */
	static synchronized Map<String, Object> get(Path base, String recordingId) throws IOException {
		Path file = base.resolve("tests").resolve(id(recordingId) + ".json");
		if (Files.isSymbolicLink(file) || !Files.isRegularFile(file)) throw new IllegalArgumentException("Recording does not exist in this app: " + recordingId);
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
        if (Files.isSymbolicLink(file)) throw new IOException("App test files must not be symbolic links");
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
			throw new IOException("Saved recording " + file.getFileName() + " is invalid (" + e.getMessage() + "); review it before running app tests", e);
		}
	}

    /** Malformed filenames are deletable only by explicit basename, and valid tests keep the UUID path. */
    static synchronized boolean deleteInvalid(Path base, String fileName) throws IOException {
        if (fileName == null || fileName.length() > 255 || !fileName.endsWith(".json") || fileName.contains("/") || fileName.contains("\\") || !Path.of(fileName).getFileName().toString().equals(fileName)) throw new IllegalArgumentException("Choose a filename from this app's invalid test list");
        Path file = base.resolve("tests").resolve(fileName);
        if (!Files.exists(file, java.nio.file.LinkOption.NOFOLLOW_LINKS)) return false;
        if (!Files.isSymbolicLink(file)) {
            try { read(file); } catch (IOException | RuntimeException invalid) { return Files.deleteIfExists(file); }
            throw new IllegalArgumentException("This test is valid; delete it using its saved recordingId");
        }
        return Files.deleteIfExists(file);
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
