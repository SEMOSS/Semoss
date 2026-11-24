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
package prerna.reactor.model;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.auth.AccessToken;
import prerna.engine.impl.model.ModelRouterEngine;
import prerna.engine.impl.model.ModelRouterEngine.ReplaySettings;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.inferencetracking.RouterEvidenceStore;
import prerna.engine.impl.model.message.MessagePartType;
import prerna.om.Insight;
import prerna.reactor.agent.config.AgentConfigLoader;
import prerna.reactor.agent.run.AgentRunStatus;
import prerna.util.ValueUtils;

/**
 * Past router tasks and the original inputs needed to rerun them. At a task's first routed call
 * {@link #snapshotBaseline} keeps a copy of the room's files in {@code <room>/.baseline}; {@link #build} then takes the router's
 * bounded recent runs that are self-contained (completed, top level, one run per room in that history, only calls to currently approved tools and a verified snapshot).
 * It writes a training set and a held-out validation set. Past answers are not used as expected results.
 */
public final class RouterLearningExamples {

	private static final Logger classLogger = LogManager.getLogger(RouterLearningExamples.class);

	private static final Pattern ID = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,63}");
	private static final long MAX_FILE_BYTES = 20L << 20;
	static final String BASELINE = ".baseline";
	private static final String BASELINE_ATTEMPTED = ".baseline-attempted";
	private static final String BASELINE_MANIFEST = ".manifest.json";
	static final int RUN_PAGE_SIZE = 200;

	/** A usable recorded run: its goal, room and the files it started with. */
	record Usable(String runId, String goal, Path room, String hash, String workspaceId, Set<String> requiredTools) {
		Path baseline() {
			return room.resolve(BASELINE);
		}
	}

	/** The built set ids, or {@code train == null} with the number of usable tasks still missing. */
	record Built(String train, String val, int needed) {
	}

	private RouterLearningExamples() {
	}

	/** Publishes a complete first capture, including an empty manifest for fileless rooms. Never raises into the routed call. */
	public static void snapshotBaseline(Room room, int maxBytes) {
		try {
			Path folder = Files.createDirectories(Path.of(room.getRoomFolderPath()));
			Path base = folder.resolve(BASELINE);
			if (!Files.isDirectory(folder, LinkOption.NOFOLLOW_LINKS) || !Files.notExists(base, LinkOption.NOFOLLOW_LINKS)) {
				return;
			}
			try {
				// Claim the first attempt even on failure: a later call may see outputs instead of inputs.
				Files.createFile(folder.resolve(BASELINE_ATTEMPTED));
			} catch (FileAlreadyExistsException e) {
				return;
			}
			Path staged = Files.createDirectory(folder.resolve(".baseline-staged-" + UUID.randomUUID()));
			try {
				Map<String, String> inputs = snapshotFiles(folder, staged, maxBytes, false);
				if (!inputs.equals(snapshotFiles(folder, null, maxBytes, false))) {
					throw new IOException("Starting files changed during capture");
				}
				JsonObject manifest = new JsonObject();
				manifest.add("files", new Gson().toJsonTree(inputs));
				manifest.addProperty("workspace_id", AgentConfigLoader.extractWorkspaceId(room));
				Files.writeString(staged.resolve(BASELINE_MANIFEST), manifest.toString());
				Files.move(staged, base, StandardCopyOption.ATOMIC_MOVE);
			} finally {
				FileUtils.deleteDirectory(staged.toFile());
			}
		} catch (IOException | RuntimeException e) {
			classLogger.warn("Could not snapshot the baseline of a room: {}", e.getMessage());
		}
	}

	/** Newest eligible run per prompt and starting inputs, using exact approved call names. */
	static List<Usable> select(List<Map<String, Object>> runs, Function<String, Path> roomFolder, int maxBytes,
			Function<String, Set<String>> workspaceTools) {
		Map<String, Integer> perRoom = new LinkedHashMap<>();
		runs.forEach(r -> perRoom.merge(ValueUtils.toStringOrEmpty(r.get("roomId")), 1, Integer::sum));
		Map<String, Usable> byExample = new LinkedHashMap<>();
		for (Map<String, Object> run : runs) {
			String goal = ValueUtils.trimToNull(run.get("input"));
			String roomId = ValueUtils.toStringOrEmpty(run.get("roomId"));
			Object messages = run.get("messages");
			if (goal == null || roomId.isBlank() || ValueUtils.trimToNull(run.get("errorMessage")) != null || thumbsDown(messages)
					|| !AgentRunStatus.COMPLETED.name().equals(run.get("status")) || ValueUtils.trimToNull(run.get("parentRunId")) != null
					|| perRoom.get(roomId) != 1 || !endsWithAnswer(messages)) {
				continue;
			}
			Set<String> needed = recordedTools(messages);
			if (needed == null) {
				continue;
			}
			try {
				Path room = roomFolder.apply(roomId);
				Snapshot snapshot = readSnapshot(room.resolve(BASELINE), null, maxBytes);
				String recordedWorkspace = ValueUtils.trimToNull(run.get("workspaceId"));
				String workspace = snapshot.workspaceId() == null ? recordedWorkspace : snapshot.workspaceId();
				if (recordedWorkspace != null && !recordedWorkspace.equals(workspace)) {
					continue;
				}
				Set<String> approved = workspace == null ? null : workspaceTools.apply(workspace);
				if (approved == null || !approved.containsAll(needed)) {
					continue;
				}
				String hash = exampleHash(goal, snapshot.files());
				byExample.putIfAbsent(taskId(workspace, hash), new Usable(ValueUtils.toStringOrEmpty(run.get("runId")), goal, room, hash, workspace, needed));
			} catch (IOException e) {
				classLogger.warn("Run {} left out: {}", run.get("runId"), e.getMessage());
			}
		}
		return new ArrayList<>(byExample.values());
	}

	/** Replay uses the recorded goal, so runs with follow-up user inputs are not safe single-input tasks. */
	private static boolean endsWithAnswer(Object messages) {
		if (!(messages instanceof List<?> list) || list.isEmpty()) {
			return true;
		}
		int inputs = 0;
		for (Object item : list) {
			if (item instanceof Map<?, ?> message && "INPUT".equalsIgnoreCase(ValueUtils.toStringOrEmpty(message.get("io")))
					&& !isToolResult(message)) {
				inputs++;
				if (inputs > 1) {
					return false;
				}
			}
		}
		return !(list.get(list.size() - 1) instanceof Map<?, ?> last && "INPUT".equalsIgnoreCase(ValueUtils.toStringOrEmpty(last.get("io"))));
	}

	private static boolean isToolResult(Map<?, ?> message) {
		return message.get("parts") instanceof List<?> parts && parts.stream().anyMatch(part -> part instanceof Map<?, ?> fields
				&& MessagePartType.TOOL_RESULT.name().equalsIgnoreCase(ValueUtils.toStringOrEmpty(fields.get("type"))));
	}

	/** A user rated a message of the run down. */
	private static boolean thumbsDown(Object messages) {
		for (Object message : messages instanceof List<?> list ? list : List.of()) {
			if (message instanceof Map<?, ?> m && m.get("feedback") instanceof Map<?, ?> f && Boolean.FALSE.equals(f.get("rating"))) {
				return true;
			}
		}
		return false;
	}

	private static Set<String> recordedTools(Object messages) {
		Set<String> names = new LinkedHashSet<>();
		for (Object item : messages instanceof List<?> list ? list : List.of()) {
			// Input messages can also store tool declarations as TOOL_CALL parts.
			if (!(item instanceof Map<?, ?> message) || "INPUT".equalsIgnoreCase(ValueUtils.toStringOrEmpty(message.get("io")))) {
				continue;
			}
			boolean hasCallParts = false;
			for (Object part : message.get("parts") instanceof List<?> parts ? parts : List.of()) {
				if (part instanceof Map<?, ?> fields
						&& MessagePartType.TOOL_CALL.name().equalsIgnoreCase(ValueUtils.toStringOrEmpty(fields.get("type")))) {
					hasCallParts = true;
					Object call = fields.get("toolCall");
					if (!recordTool(call != null ? call : fields.get("tool_call"), names)) {
						return null;
					}
				}
			}
			// The legacy response field mirrors call parts when both are present.
			if (!hasCallParts && message.get("tool_responses") instanceof List<?> calls
					&& calls.stream().anyMatch(call -> !recordTool(call, names))) {
				return null;
			}
		}
		return Set.copyOf(names);
	}

	private static boolean recordTool(Object rawCall, Set<String> names) {
		if (!(rawCall instanceof Map<?, ?> call)) {
			return false;
		}
		if (Boolean.parseBoolean(ValueUtils.toStringOrEmpty(call.get("server_tool")))
				|| Boolean.parseBoolean(ValueUtils.toStringOrEmpty(call.get("serverTool")))) {
			return false;
		}
		Object name = call.get("name");
		if (name == null && call.get("function") instanceof Map<?, ?> function) {
			name = function.get("name");
		}
		if (!(name instanceof String tool) || tool.isBlank()) {
			return false;
		}
		names.add(tool);
		return true;
	}

	private static Map<String, String> snapshotFiles(Path folder, Path copyTo, int maxBytes, boolean baseline) throws IOException {
		Map<String, String> hashes = new TreeMap<>();
		long remaining = maxBytes;
		try (var entries = Files.newDirectoryStream(folder)) {
			for (Path file : entries) {
				String name = file.getFileName().toString();
				if (baseline ? BASELINE_MANIFEST.equals(name) : hidden(file.getFileName())) {
					continue;
				}
				if (hidden(file.getFileName()) || !Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) {
					throw new IOException("Replay requires flat, regular input files: " + name);
				}
				long limit = Math.min(remaining, MAX_FILE_BYTES);
				if (Files.size(file) > limit) {
					throw new IOException("Starting files exceed the snapshot byte limit");
				}
				try (var stream = Files.newInputStream(file, LinkOption.NOFOLLOW_LINKS)) {
					byte[] bytes = stream.readNBytes((int) limit + 1);
					if (bytes.length > limit) {
						throw new IOException("Starting files exceed the snapshot byte limit");
					}
					remaining -= bytes.length;
					hashes.put(name, fingerprint(bytes));
					if (copyTo != null) {
						Files.write(copyTo.resolve(name), bytes);
					}
				}
			}
		} catch (DirectoryIteratorException e) {
			throw e.getCause();
		}
		return hashes;
	}

	private record Snapshot(Map<String, String> files, String workspaceId) {
	}

	private static Snapshot readSnapshot(Path base, Path copyTo, int maxBytes) throws IOException {
		Path manifest = base.resolve(BASELINE_MANIFEST);
		if (!Files.isDirectory(base, LinkOption.NOFOLLOW_LINKS) || !Files.isRegularFile(manifest, LinkOption.NOFOLLOW_LINKS)) {
			throw new IOException("No complete starting-input snapshot");
		}
		Map<String, String> inputs = snapshotFiles(base, copyTo, maxBytes, true);
		try (var reader = new InputStreamReader(Files.newInputStream(manifest, LinkOption.NOFOLLOW_LINKS), StandardCharsets.UTF_8)) {
			JsonObject stored = JsonParser.parseReader(reader).getAsJsonObject();
			boolean current = stored.has("files") && stored.get("files").isJsonObject();
			if (new Gson().toJsonTree(inputs).equals(current ? stored.get("files") : stored)) {
				return new Snapshot(inputs, current ? optionalString(stored, "workspace_id") : null);
			}
		} catch (RuntimeException e) {
			throw new IOException("Invalid starting-input snapshot manifest", e);
		}
		throw new IOException("Starting-input snapshot does not match its manifest");
	}

	private static String exampleHash(String goal, Map<String, String> inputs) {
		return ModelRouterEngine.sha256(new Gson().toJson(List.of(goal, inputs)));
	}

	private static String taskId(String workspace, String hash) {
		return ModelRouterEngine.sha256(workspace + "|" + hash);
	}

	static Built build(Insight insight, String engineId, ReplaySettings settings) throws Exception {
		return write(EvaluateRouterCandidateReactor.evidenceDir(engineId).resolve("replay"), recentEligible(insight, engineId, settings), settings);
	}

	@FunctionalInterface
	interface RunPageReader<T> {
		List<T> read(long offset, int limit) throws Exception;
	}

	/** Reads at most maxRuns, shrinking the last query rather than fetching additional transcripts. */
	static <T> List<T> readPages(int pageSize, int maxRuns, RunPageReader<T> pageReader) throws Exception {
		if (pageSize < 1 || maxRuns < 0) {
			throw new IllegalArgumentException("Invalid history page size or run limit");
		}
		List<T> values = new ArrayList<>();
		while (values.size() < maxRuns) {
			int limit = Math.min(pageSize, maxRuns - values.size());
			List<T> page = pageReader.read(values.size(), limit);
			if (page == null || page.size() > limit) {
				throw new IllegalStateException("Run page did not respect the requested limit");
			}
			values.addAll(page);
			if (page.size() < limit) {
				break;
			}
		}
		return values;
	}

	private static List<Usable> recentEligible(Insight insight, String engineId, ReplaySettings settings) throws Exception {
		boolean pooled = GetRouterEvidenceReactor.pooled(insight, engineId);
		AccessToken login = GetRouterEvidenceReactor.loginOf(insight);
		Instant through = Instant.now();
		Instant since = through.minus(settings.history_days, ChronoUnit.DAYS);
		List<String> attemptRoomIds = RunRouterLearningIfDueReactor.attemptRoomIds(engineId);
		List<Map<String, Object>> runs = readPages(RUN_PAGE_SIZE, settings.max_runs, (offset, limit) -> {
			List<String> ids = RouterEvidenceStore.runIdsExcludingLearningReplays(login, engineId, since, through, limit, offset, pooled, attemptRoomIds);
			List<Map<String, Object>> page = new ArrayList<>(ids.size());
			for (String id : ids) {
				try {
					page.add(GetRouterEvidenceReactor.run(insight, engineId, id, pooled));
				} catch (RuntimeException e) {
					throw new IllegalStateException("Run " + id + " of router " + engineId + " could not be read; evidence is incomplete", e);
				}
			}
			return page;
		});
		Map<String, Set<String>> workspaceTools = new HashMap<>();
		return select(runs, id -> Path.of(Room.roomFolderPath(id)), settings.max_snapshot_bytes, workspace -> {
			if (!workspaceTools.containsKey(workspace)) {
				var snapshot = RouterReplayWorkspace.inspect(insight, workspace);
				workspaceTools.put(workspace, snapshot == null ? null : snapshot.tools());
			}
			return workspaceTools.get(workspace);
		});
	}

	/** A fixed 40% hash bucket is held out, independent of history churn and set quotas. */
	static Built write(Path replay, List<Usable> usable, ReplaySettings settings) throws IOException {
		List<Usable> sorted = usable.stream().sorted(Comparator.comparing(Usable::hash)).toList();
		List<Usable> train = new ArrayList<>();
		List<Usable> val = new ArrayList<>();
		for (Usable task : sorted) {
			if (task.hash().compareTo("66666666") < 0) {
				if (val.size() < settings.validation_tasks) {
					val.add(task);
				}
			} else if (train.size() < settings.max_train_tasks) {
				train.add(task);
			}
		}
		int needed = (settings.validation_tasks - val.size()) + Math.max(0, settings.min_train_tasks - train.size());
		if (needed > 0) {
			return new Built(null, null, needed);
		}
		Built built = new Built("auto-" + UUID.randomUUID(), "auto-" + UUID.randomUUID(), 0);
		Path trainDir = replay.resolve(built.train());
		Path valDir = replay.resolve(built.val());
		try {
			writeSet(trainDir, train, settings.max_snapshot_bytes);
			writeSet(valDir, val, settings.max_snapshot_bytes);
		} catch (IOException e) {
			for (Path dir : List.of(trainDir, valDir)) {
				try {
					FileUtils.deleteDirectory(dir.toFile());
				} catch (IOException cleanup) {
					e.addSuppressed(cleanup);
				}
			}
			throw e;
		}
		return built;
	}

	private static void writeSet(Path dir, List<Usable> tasks, int maxBytes) throws IOException {
		JsonArray manifest = new JsonArray();
		for (Usable task : tasks) {
			String id = taskId(task.workspaceId(), task.hash());
			Path inputs = Files.createDirectories(dir.resolve(id));
			Snapshot snapshot = readSnapshot(task.baseline(), inputs, maxBytes);
			if (!task.hash().equals(exampleHash(task.goal(), snapshot.files()))
					|| snapshot.workspaceId() != null && !task.workspaceId().equals(snapshot.workspaceId())) {
				throw new IOException("Starting inputs changed after replay selection");
			}
			JsonObject entry = new JsonObject();
			entry.addProperty("id", id);
			entry.addProperty("prompt", task.goal());
			entry.addProperty("source_run", task.runId());
			entry.addProperty("workspace_id", task.workspaceId());
			entry.add("required_tools", new Gson().toJsonTree(task.requiredTools().stream().sorted().toList()));
			manifest.add(entry);
		}
		JsonObject root = new JsonObject();
		root.add("tasks", manifest);
		Files.createDirectories(dir);
		Files.writeString(dir.resolve("manifest.json"), new Gson().toJson(root));
	}
	/** A replayable task and the files it starts with. */
	record Task(String id, String prompt, List<Path> inputs, String workspaceId, Set<String> requiredTools) {
		Task(String id, String prompt, List<Path> inputs) {
			this(id, prompt, inputs, null, Set.of());
		}
	}

	/** The folder of a set (or of one of its tasks), after checking the id. */
	static Path setDir(Path replayDir, String id) {
		if (id == null || !ID.matcher(id).matches()) {
			throw new IllegalArgumentException("Invalid task set or task id");
		}
		return replayDir.resolve(id);
	}

	static String replayFingerprint(Path dir) throws IOException {
		Map<String, String> hashes = new TreeMap<>();
		for (Path file : visibleFiles(dir)) {
			String name = dir.relativize(file).toString().replace('\\', '/');
			if (name.equals("manifest.json") || name.contains("/")) {
				hashes.put(name, fingerprint(file));
			}
		}
		if (!hashes.containsKey("manifest.json")) {
			throw new IOException("Replay manifest is missing");
		}
		return prerna.engine.impl.model.ModelRouterEngine.sha256(new Gson().toJson(hashes));
	}

	static String fingerprint(Path file) throws IOException {
		return fingerprint(Files.readAllBytes(file));
	}

	static String fingerprint(byte[] bytes) {
		try {
			return java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes));
		} catch (java.security.NoSuchAlgorithmException e) {
			throw new IllegalStateException(e);
		}
	}

	static List<String> sourceRuns(Path dir, String expectedHash) throws IOException {
		if (!replayFingerprint(dir).equals(expectedHash)) {
			throw new IOException("Training inputs changed after the attempt started");
		}
		JsonArray tasks = readManifest(dir);
		List<String> ids = new ArrayList<>();
		for (JsonElement task : tasks) {
			ids.add(task.getAsJsonObject().get("source_run").getAsString());
		}
		return ids;
	}

	/** The tasks of a set folder; throws IllegalArgumentException when the manifest is missing, empty or has a bad task. */
	static List<Task> load(Path setDir) throws IOException {
		JsonArray array = readManifest(setDir);
		List<Task> tasks = new ArrayList<>();
		for (JsonElement element : array) {
			JsonObject t = element.getAsJsonObject();
			String id = t.get("id").getAsString();
			Path folder = setDir(setDir, id);
			String workspace = optionalString(t, "workspace_id");
			Set<String> tools = new LinkedHashSet<>();
			if (t.has("required_tools")) {
				for (JsonElement tool : t.getAsJsonArray("required_tools")) {
					tools.add(tool.getAsString());
				}
			}
			tasks.add(new Task(id, ValueUtils.trimToNull(t.get("prompt").getAsString()), Files.isDirectory(folder) ? visibleFiles(folder) : List.of(),
					workspace, Set.copyOf(tools)));
		}
		return tasks;
	}

	private static String optionalString(JsonObject object, String key) {
		JsonElement value = object.get(key);
		return value == null || value.isJsonNull() ? null : ValueUtils.trimToNull(value.getAsString());
	}

	private static JsonArray readManifest(Path setDir) throws IOException {
		JsonArray array;
		try {
			array = JsonParser.parseString(Files.readString(setDir.resolve("manifest.json"))).getAsJsonObject().getAsJsonArray("tasks");
		} catch (RuntimeException e) {
			throw new IllegalArgumentException("manifest.json of the task set is not valid: " + e.getMessage(), e);
		}
		if (array == null || array.isEmpty()) {
			throw new IllegalArgumentException("The task set has no tasks");
		}
		return array;
	}

	/** Regular files of a folder outside hidden paths (a name starting with a dot) and not too large, by path. */
	static List<Path> visibleFiles(Path folder) throws IOException {
		if (!Files.isDirectory(folder)) {
			return List.of();
		}
		List<Path> files;
		try (Stream<Path> walk = Files.walk(folder)) {
			files = walk.filter(Files::isRegularFile).filter(f -> !hidden(folder.relativize(f))).sorted().toList();
		}
		for (Path file : files) {
			if (Files.size(file) > MAX_FILE_BYTES) {
				throw new IOException("File exceeds the " + MAX_FILE_BYTES + " byte task evidence limit: " + file.getFileName());
			}
		}
		return files;
	}

	private static boolean hidden(Path relative) {
		for (Path name : relative) {
			if (name.toString().startsWith(".")) {
				return true;
			}
		}
		return false;
	}

	/** Copies a task's input files into a room folder. */
	static void stage(Task task, Path folder) throws IOException {
		Files.createDirectories(folder);
		for (Path input : task.inputs()) {
			Files.copy(input, folder.resolve(input.getFileName()), StandardCopyOption.REPLACE_EXISTING);
		}
	}

}
