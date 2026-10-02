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

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

/** Discovers, summarizes, and ranks Playwright recording JSON files. */
public class PlaywrightRecordingCatalogService {

	private static final Logger classLogger = LogManager.getLogger(PlaywrightRecordingCatalogService.class);

	static final int MAX_FILES_PER_SOURCE = 500;
	static final long MAX_RECORDING_BYTES = 5L * 1024L * 1024L;

	private static final Pattern SEARCH_PREFIX = Pattern.compile("^(?:https?://)?(?:www\\.)?",
			Pattern.CASE_INSENSITIVE);
	private static final Pattern JSON_SUFFIX = Pattern.compile("\\.json$", Pattern.CASE_INSENSITIVE);
	private static final Pattern NON_ALPHANUMERIC = Pattern.compile("[^a-z0-9]+");
	private static final Set<String> IGNORED_TOKENS = Set.of("com", "net", "org", "edu", "gov", "www", "http", "https",
			"html", "json");
	private static final List<String> META_SEARCH_FIELDS = List.of("title", "description", "intent", "id",
			"requestedStartUrl");
	private static final List<String> STEP_SEARCH_FIELDS = List.of("url", "text", "label", "description", "prompt",
			"selector", "role");
	private static final int DEFAULT_CANDIDATE_LIMIT = 20;
	private static final int MAX_CANDIDATE_LIMIT = 50;

	public Map<String, Object> resolve(Path roomFolder, Path projectRecordingsFolder, String projectId,
			String recordingNameHint, String recordingFile) {
		String hint = trim(recordingNameHint);
		String requestedFile = trim(recordingFile);
		if (hint.isEmpty() && requestedFile.isEmpty()) {
			throw new IllegalArgumentException("recording_name_hint or recording_file is required");
		}

		Catalog catalog = catalog(roomFolder, projectRecordingsFolder, projectId, hint, requestedFile);
		List<Candidate> all = catalog.candidates;

		all.removeIf(candidate -> candidate.score <= 0);
		sortCandidates(all);

		List<Map<String, Object>> candidates = new ArrayList<>();
		for (int i = 0; i < Math.min(DEFAULT_CANDIDATE_LIMIT, all.size()); i++) {
			candidates.add(all.get(i).toMap());
		}

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("selected", candidates.isEmpty() ? null : candidates.get(0));
		result.put("candidates", candidates);
		result.put("searchedProjectRecordings", catalog.projectCount);
		result.put("searchedRoomRecordings", catalog.roomCount);
		return result;
	}

	/**
	 * Lists room recordings for model-assisted discovery. Unlike {@link #resolve},
	 * this includes zero-score recordings so the model can compare their summaries
	 * when deterministic text matching is inconclusive.
	 */
	public Map<String, Object> findRoomRecordings(Path roomFolder, String query, int maxCandidates) {
		String hint = trim(query);
		Catalog catalog = catalog(roomFolder, null, "", hint, "");
		List<Candidate> all = catalog.candidates;
		sortCandidates(all);

		int limit = maxCandidates <= 0 ? DEFAULT_CANDIDATE_LIMIT : Math.min(maxCandidates, MAX_CANDIDATE_LIMIT);
		List<Map<String, Object>> recordings = new ArrayList<>();
		for (int i = 0; i < Math.min(limit, all.size()); i++) {
			recordings.add(all.get(i).toMap());
		}

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("query", hint);
		result.put("recordingCount", catalog.roomCount);
		result.put("recordings", recordings);
		result.put("selectionGuidance",
				"Compare filenames, metadata, URLs, hosts, and stepPreview. If the user did not provide an exact filename, identify the closest candidate and ask for confirmation before calling replay_browser_recording with recording_file.");
		return result;
	}

	private Catalog catalog(Path roomFolder, Path projectRecordingsFolder, String projectId, String hint,
			String requestedFile) {
		List<Candidate> candidates = new ArrayList<>();
		int roomCount = addCandidates(candidates, roomFolder == null ? null : roomFolder.resolve("playwright"), "room",
				"", hint, requestedFile);
		int projectCount = addCandidates(candidates, projectRecordingsFolder, "project", trim(projectId), hint,
				requestedFile);
		return new Catalog(candidates, roomCount, projectCount);
	}

	private static void sortCandidates(List<Candidate> candidates) {
		candidates.sort(Comparator.comparingInt(Candidate::score).reversed()
				.thenComparing(candidate -> "room".equals(candidate.source) ? 0 : 1)
				.thenComparing(candidate -> candidate.fileName.toLowerCase(Locale.ROOT)));
	}

	private int addCandidates(List<Candidate> output, Path directory, String source, String projectId, String hint,
			String requestedFile) {
		if (directory == null || !Files.isDirectory(directory)) {
			return 0;
		}

		Path normalizedDirectory = directory.toAbsolutePath().normalize();
		List<Path> files = new ArrayList<>();
		try (Stream<Path> paths = Files.list(normalizedDirectory)) {
			paths.filter(Files::isRegularFile)
					.filter(path -> path.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".json")).sorted()
					.limit(MAX_FILES_PER_SOURCE).forEach(files::add);
		} catch (IOException e) {
			classLogger.warn("Unable to list recording files in directory '{}'; skipping source", normalizedDirectory,
					e);
			return 0;
		}

		int parsedCount = 0;
		for (Path file : files) {
			Path normalizedFile = file.toAbsolutePath().normalize();
			if (!normalizedFile.startsWith(normalizedDirectory)) {
				continue;
			}
			JsonElement recording = readRecording(normalizedFile);
			if (recording == null) {
				continue;
			}
			parsedCount++;
			output.add(toCandidate(source, projectId, normalizedFile.getFileName().toString(), recording, hint,
					requestedFile));
		}
		return parsedCount;
	}

	private JsonElement readRecording(Path file) {
		try {
			long size = Files.size(file);
			if (size <= 0 || size > MAX_RECORDING_BYTES) {
				return null;
			}
			String json = Files.readString(file, StandardCharsets.UTF_8);
			JsonElement recording = JsonParser.parseString(json);
			return recording != null && recording.isJsonObject() ? recording : null;
		} catch (IOException | RuntimeException e) {
			classLogger.warn("Unable to read/parse recording file '{}'; skipping it", file, e);
			return null;
		}
	}

	private Candidate toCandidate(String source, String projectId, String fileName, JsonElement recording, String hint,
			String requestedFile) {
		List<JsonElement> steps = flattenSteps(recording);
		String searchText = buildSearchText(fileName, recording, steps);
		String normalizedSearch = normalizeSearch(searchText);
		String normalizedFile = normalizeSearch(requestedFile);
		String normalizedHint = normalizeSearch(hint);
		int score = 0;
		LinkedHashSet<String> reasons = new LinkedHashSet<>();

		if (!requestedFile.isEmpty()) {
			if (fileName.equalsIgnoreCase(requestedFile)) {
				score += 100;
				reasons.add("exact filename");
			} else if (!normalizedFile.isEmpty() && normalizedSearch.contains(normalizedFile)) {
				score += 70;
				reasons.add("filename/content contains requested file");
			}
		}

		if (!normalizedHint.isEmpty() && normalizedSearch.contains(normalizedHint)) {
			score += 60;
			reasons.add("contains normalized hint");
		}

		List<String> hintTokens = tokens(hint);
		for (String token : hintTokens) {
			if (searchText.contains(token) || normalizedSearch.contains(token)) {
				score += 15;
				reasons.add("matches " + token);
			}
		}

		String firstUrl = firstUrl(steps);
		if (firstUrl.isEmpty()) {
			firstUrl = text(path(recording, "meta"), "requestedStartUrl");
		}
		String host = host(firstUrl);
		for (String token : hintTokens) {
			if (!host.isEmpty() && host.contains(token)) {
				score += 30;
				reasons.add("host " + host);
			}
		}

		if (score > 0 && "room".equals(source)) {
			score += 5;
		}

		Map<String, Object> summary = summarize(fileName, recording, steps);
		String reason = reasons.isEmpty() ? "no match"
				: String.join(", ", new ArrayList<>(reasons).subList(0, Math.min(4, reasons.size())));
		return new Candidate(source, projectId, fileName, "room".equals(source) ? "/playwright/" + fileName : "", score,
				reason, firstUrl, summary);
	}

	private static List<JsonElement> flattenSteps(JsonElement recording) {
		List<JsonElement> output = new ArrayList<>();
		JsonElement steps = path(recording, "steps");
		if (steps.isJsonObject()) {
			steps.getAsJsonObject().entrySet().forEach(entry -> collectStepNodes(entry.getValue(), output));
		}
		return output;
	}

	private static void collectStepNodes(JsonElement node, List<JsonElement> output) {
		if (node == null || node.isJsonNull()) {
			return;
		}
		if (node.isJsonArray()) {
			node.getAsJsonArray().forEach(child -> collectStepNodes(child, output));
		} else if (node.isJsonObject()) {
			output.add(node);
		}
	}

	private static String buildSearchText(String fileName, JsonElement recording, List<JsonElement> steps) {
		StringBuilder text = new StringBuilder(fileName);
		JsonElement meta = path(recording, "meta");
		for (String field : META_SEARCH_FIELDS) {
			appendText(text, path(meta, field));
		}
		JsonElement searchTerms = path(meta, "searchTerms");
		if (searchTerms.isJsonArray()) {
			searchTerms.getAsJsonArray().forEach(term -> appendText(text, term));
		}
		for (JsonElement step : steps) {
			for (String field : STEP_SEARCH_FIELDS) {
				if ("text".equals(field) && asBoolean(step, "isPassword")) {
					continue;
				}
				appendText(text, path(step, field));
			}
		}
		return text.toString().toLowerCase(Locale.ROOT);
	}

	private static void appendText(StringBuilder output, JsonElement node) {
		if (isString(node) && !node.getAsString().isBlank()) {
			output.append(' ').append(node.getAsString());
		}
	}

	private static Map<String, Object> summarize(String fileName, JsonElement recording, List<JsonElement> steps) {
		List<String> urls = new ArrayList<>();
		List<String> typedValues = new ArrayList<>();
		List<String> stepPreview = new ArrayList<>();
		LinkedHashSet<String> hosts = new LinkedHashSet<>();
		for (JsonElement step : steps) {
			String url = text(step, "url");
			if (!url.isEmpty()) {
				urls.add(url);
				String host = host(url);
				if (!host.isEmpty() && hosts.size() < 10) {
					hosts.add(host);
				}
			}
			if ("TYPE".equalsIgnoreCase(text(step, "type")) && !asBoolean(step, "isPassword")) {
				String typed = text(step, "text");
				if (!typed.isEmpty()) {
					typedValues.add(typed);
				}
			}
			if (stepPreview.size() < 12) {
				stepPreview.add(summarizeStep(step));
			}
		}

		JsonElement meta = path(recording, "meta");
		Map<String, Object> summary = new LinkedHashMap<>();
		summary.put("fileName", fileName);
		summary.put("stepCount", steps.size());
		String requestedStartUrl = text(meta, "requestedStartUrl");
		String firstUrl = urls.isEmpty() ? requestedStartUrl : urls.get(0);
		summary.put("firstUrl", firstUrl);
		summary.put("requestedStartUrl", requestedStartUrl);
		summary.put("urls", urls.stream().distinct().limit(10).toList());
		summary.put("hosts", new ArrayList<>(hosts));
		String typedPreview = String.join(" ", typedValues);
		summary.put("typedTextPreview", typedPreview.substring(0, Math.min(240, typedPreview.length())));
		summary.put("title", text(meta, "title"));
		summary.put("description", text(meta, "description"));
		summary.put("intent", text(meta, "intent"));
		summary.put("stepPreview", stepPreview);
		return summary;
	}

	private static String summarizeStep(JsonElement step) {
		String type = text(step, "type");
		String detail = firstNonBlank(text(step, "description"), text(step, "label"), text(step, "prompt"),
				text(step, "url"));
		if (detail.isEmpty() && !asBoolean(step, "isPassword")) {
			detail = text(step, "text");
		}
		String summary = type.isEmpty() ? "STEP" : type.toUpperCase(Locale.ROOT);
		if (!detail.isEmpty()) {
			summary += ": " + detail;
		}
		return summary.substring(0, Math.min(240, summary.length()));
	}

	private static String firstNonBlank(String... values) {
		for (String value : values) {
			if (value != null && !value.isBlank()) {
				return value;
			}
		}
		return "";
	}

	private static String firstUrl(List<JsonElement> steps) {
		for (JsonElement step : steps) {
			String value = text(step, "url");
			if (!value.isEmpty()) {
				return value;
			}
		}
		return "";
	}

	private static String host(String url) {
		if (url == null || url.isBlank()) {
			return "";
		}
		try {
			String host = URI.create(url).getHost();
			return host == null ? "" : host.toLowerCase(Locale.ROOT).replaceFirst("^www\\.", "");
		} catch (IllegalArgumentException e) {
			classLogger.debug("Unable to parse host from URL '{}'", url, e);
			return "";
		}
	}

	private static String normalizeSearch(String value) {
		String normalized = trim(value).toLowerCase(Locale.ROOT);
		normalized = SEARCH_PREFIX.matcher(normalized).replaceFirst("");
		normalized = JSON_SUFFIX.matcher(normalized).replaceFirst("");
		return NON_ALPHANUMERIC.matcher(normalized).replaceAll("");
	}

	private static List<String> tokens(String value) {
		String normalized = trim(value).toLowerCase(Locale.ROOT);
		normalized = SEARCH_PREFIX.matcher(normalized).replaceFirst("");
		normalized = JSON_SUFFIX.matcher(normalized).replaceFirst("");
		Set<String> seen = new HashSet<>();
		List<String> output = new ArrayList<>();
		for (String token : NON_ALPHANUMERIC.split(normalized)) {
			if (token.length() >= 3 && !IGNORED_TOKENS.contains(token) && seen.add(token)) {
				output.add(token);
			}
		}
		return output;
	}

	private static String text(JsonElement parent, String field) {
		JsonElement value = path(parent, field);
		return isString(value) ? value.getAsString().trim() : "";
	}

	private static JsonElement path(JsonElement parent, String field) {
		if (parent == null || !parent.isJsonObject()) {
			return JsonNull.INSTANCE;
		}
		JsonElement value = parent.getAsJsonObject().get(field);
		return value == null ? JsonNull.INSTANCE : value;
	}

	private static boolean isString(JsonElement node) {
		return node != null && node.isJsonPrimitive() && node.getAsJsonPrimitive().isString();
	}

	/**
	 * Reads a boolean field, tolerating recordings that store the flag as the
	 * strings "true"/"false". Anything else is treated as false.
	 */
	private static boolean asBoolean(JsonElement parent, String field) {
		JsonElement value = path(parent, field);
		if (!value.isJsonPrimitive()) {
			return false;
		}
		JsonPrimitive primitive = value.getAsJsonPrimitive();
		if (primitive.isBoolean()) {
			return primitive.getAsBoolean();
		}
		return primitive.isString() && Boolean.parseBoolean(primitive.getAsString().trim());
	}

	private static String trim(String value) {
		return value == null ? "" : value.trim();
	}

	private static final class Catalog {
		private final List<Candidate> candidates;
		private final int roomCount;
		private final int projectCount;

		private Catalog(List<Candidate> candidates, int roomCount, int projectCount) {
			this.candidates = candidates;
			this.roomCount = roomCount;
			this.projectCount = projectCount;
		}
	}

	private static final class Candidate {
		private final String source;
		private final String projectId;
		private final String fileName;
		private final String roomPath;
		private final int score;
		private final String reason;
		private final String startUrl;
		private final Map<String, Object> summary;

		private Candidate(String source, String projectId, String fileName, String roomPath, int score, String reason,
				String startUrl, Map<String, Object> summary) {
			this.source = source;
			this.projectId = projectId;
			this.fileName = fileName;
			this.roomPath = roomPath;
			this.score = score;
			this.reason = reason;
			this.startUrl = startUrl;
			this.summary = summary;
		}

		private int score() {
			return score;
		}

		private Map<String, Object> toMap() {
			Map<String, Object> result = new LinkedHashMap<>();
			result.put("source", source);
			result.put("projectId", projectId);
			result.put("fileName", fileName);
			result.put("roomPath", roomPath);
			result.put("score", score);
			result.put("reason", reason);
			result.put("startUrl", startUrl);
			result.put("summary", summary);
			return result;
		}
	}
}
