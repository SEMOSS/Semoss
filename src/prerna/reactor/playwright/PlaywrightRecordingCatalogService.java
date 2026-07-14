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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Discovers, summarizes, and ranks Playwright recording JSON files. */
public class PlaywrightRecordingCatalogService {

	static final int MAX_FILES_PER_SOURCE = 500;
	static final long MAX_RECORDING_BYTES = 5L * 1024L * 1024L;

	private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
	private static final Pattern SEARCH_PREFIX = Pattern.compile("^(?:https?://)?(?:www\\.)?",
			Pattern.CASE_INSENSITIVE);
	private static final Pattern JSON_SUFFIX = Pattern.compile("\\.json$", Pattern.CASE_INSENSITIVE);
	private static final Pattern NON_ALPHANUMERIC = Pattern.compile("[^a-z0-9]+");
	private static final Set<String> IGNORED_TOKENS = Set.of("com", "net", "org", "edu", "gov", "www",
			"http", "https", "html", "json");
	private static final List<String> META_SEARCH_FIELDS = List.of("title", "description", "intent", "id");
	private static final List<String> STEP_SEARCH_FIELDS = List.of("url", "text", "label", "description", "prompt",
			"selector", "role");

	public Map<String, Object> resolve(Path roomFolder, Path projectRecordingsFolder, String projectId,
			String recordingNameHint, String recordingFile) {
		String hint = trim(recordingNameHint);
		String requestedFile = trim(recordingFile);
		if (hint.isEmpty() && requestedFile.isEmpty()) {
			throw new IllegalArgumentException("recording_name_hint or recording_file is required");
		}

		List<Candidate> all = new ArrayList<>();
		int roomCount = addCandidates(all, roomFolder == null ? null : roomFolder.resolve("playwright"), "room", "",
				hint, requestedFile);
		int projectCount = addCandidates(all, projectRecordingsFolder, "project", trim(projectId), hint,
				requestedFile);

		all.removeIf(candidate -> candidate.score <= 0);
		all.sort(Comparator.comparingInt(Candidate::score).reversed()
				.thenComparing(candidate -> "room".equals(candidate.source) ? 0 : 1)
				.thenComparing(candidate -> candidate.fileName.toLowerCase(Locale.ROOT)));

		List<Map<String, Object>> candidates = new ArrayList<>();
		for (int i = 0; i < Math.min(20, all.size()); i++) {
			candidates.add(all.get(i).toMap());
		}

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("selected", candidates.isEmpty() ? null : candidates.get(0));
		result.put("candidates", candidates);
		result.put("searchedProjectRecordings", projectCount);
		result.put("searchedRoomRecordings", roomCount);
		return result;
	}

	private int addCandidates(List<Candidate> output, Path directory, String source, String projectId, String hint,
			String requestedFile) {
		if (directory == null || !Files.isDirectory(directory)) {
			return 0;
		}

		Path normalizedDirectory = directory.toAbsolutePath().normalize();
		List<Path> files = new ArrayList<>();
		try (Stream<Path> paths = Files.list(normalizedDirectory)) {
			paths.filter(Files::isRegularFile).filter(path -> path.getFileName().toString().toLowerCase(Locale.ROOT)
					.endsWith(".json")).sorted().limit(MAX_FILES_PER_SOURCE).forEach(files::add);
		} catch (IOException e) {
			return 0;
		}

		int parsedCount = 0;
		for (Path file : files) {
			Path normalizedFile = file.toAbsolutePath().normalize();
			if (!normalizedFile.startsWith(normalizedDirectory)) {
				continue;
			}
			JsonNode recording = readRecording(normalizedFile);
			if (recording == null) {
				continue;
			}
			parsedCount++;
			output.add(toCandidate(source, projectId, normalizedFile.getFileName().toString(), recording, hint,
					requestedFile));
		}
		return parsedCount;
	}

	private JsonNode readRecording(Path file) {
		try {
			long size = Files.size(file);
			if (size <= 0 || size > MAX_RECORDING_BYTES) {
				return null;
			}
			String json = Files.readString(file, StandardCharsets.UTF_8);
			JsonNode recording = JSON_MAPPER.readTree(json);
			return recording != null && recording.isObject() ? recording : null;
		} catch (IOException | RuntimeException e) {
			return null;
		}
	}

	private Candidate toCandidate(String source, String projectId, String fileName, JsonNode recording, String hint,
			String requestedFile) {
		List<JsonNode> steps = flattenSteps(recording);
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
		String reason = reasons.isEmpty() ? "no match" : String.join(", ", new ArrayList<>(reasons).subList(0,
				Math.min(4, reasons.size())));
		return new Candidate(source, projectId, fileName,
				"room".equals(source) ? "/playwright/" + fileName : "", score, reason, firstUrl, summary);
	}

	private static List<JsonNode> flattenSteps(JsonNode recording) {
		List<JsonNode> output = new ArrayList<>();
		JsonNode steps = recording.path("steps");
		if (steps.isObject()) {
			steps.elements().forEachRemaining(node -> collectStepNodes(node, output));
		}
		return output;
	}

	private static void collectStepNodes(JsonNode node, List<JsonNode> output) {
		if (node == null || node.isNull()) {
			return;
		}
		if (node.isArray()) {
			node.elements().forEachRemaining(child -> collectStepNodes(child, output));
		} else if (node.isObject()) {
			output.add(node);
		}
	}

	private static String buildSearchText(String fileName, JsonNode recording, List<JsonNode> steps) {
		StringBuilder text = new StringBuilder(fileName);
		JsonNode meta = recording.path("meta");
		for (String field : META_SEARCH_FIELDS) {
			appendText(text, meta.path(field));
		}
		for (JsonNode step : steps) {
			for (String field : STEP_SEARCH_FIELDS) {
				appendText(text, step.path(field));
			}
		}
		return text.toString().toLowerCase(Locale.ROOT);
	}

	private static void appendText(StringBuilder output, JsonNode node) {
		if (node != null && node.isTextual() && !node.textValue().isBlank()) {
			output.append(' ').append(node.textValue());
		}
	}

	private static Map<String, Object> summarize(String fileName, JsonNode recording, List<JsonNode> steps) {
		List<String> urls = new ArrayList<>();
		List<String> typedValues = new ArrayList<>();
		LinkedHashSet<String> hosts = new LinkedHashSet<>();
		for (JsonNode step : steps) {
			String url = text(step, "url");
			if (!url.isEmpty()) {
				urls.add(url);
				String host = host(url);
				if (!host.isEmpty() && hosts.size() < 10) {
					hosts.add(host);
				}
			}
			if ("TYPE".equalsIgnoreCase(text(step, "type"))) {
				String typed = text(step, "text");
				if (!typed.isEmpty()) {
					typedValues.add(typed);
				}
			}
		}

		JsonNode meta = recording.path("meta");
		Map<String, Object> summary = new LinkedHashMap<>();
		summary.put("fileName", fileName);
		summary.put("stepCount", steps.size());
		summary.put("firstUrl", urls.isEmpty() ? "" : urls.get(0));
		summary.put("hosts", new ArrayList<>(hosts));
		String typedPreview = String.join(" ", typedValues);
		summary.put("typedTextPreview", typedPreview.substring(0, Math.min(240, typedPreview.length())));
		summary.put("title", text(meta, "title"));
		summary.put("description", text(meta, "description"));
		summary.put("intent", text(meta, "intent"));
		return summary;
	}

	private static String firstUrl(List<JsonNode> steps) {
		for (JsonNode step : steps) {
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

	private static String text(JsonNode parent, String field) {
		if (parent == null) {
			return "";
		}
		JsonNode value = parent.path(field);
		return value.isTextual() ? value.textValue().trim() : "";
	}

	private static String trim(String value) {
		return value == null ? "" : value.trim();
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
