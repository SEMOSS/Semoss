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

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.revwalk.RevCommit;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.engine.api.IModelRouterEngine;
import prerna.om.Insight;

/**
 * The version history of a router: the committed router.json versions from the engine's git repository, a plain-English account of
 * what each changed against the version before it, and the restore used by operator rollback. Routes and step
 * types (subtasks) are named by route name and subtask definition, never by id.
 */
final class RouterVersions {

	/** A committed router.json: the newest commit of one config {@code version}. */
	record Version(String version, Instant committedAt, String message, String json) {
	}

	private static final Map<String, ReentrantLock> LOCKS = new ConcurrentHashMap<>();

	static ReentrantLock lock(String engineId) {
		return LOCKS.computeIfAbsent(engineId, ignored -> new ReentrantLock());
	}

	private RouterVersions() {
	}

	/** The versions in the engine's git history of {@code path}, newest first, one entry per {@code version}. */
	static List<Version> history(String versionFolder, String path) throws Exception {
		List<Version> versions = new ArrayList<>();
		Set<String> seen = new HashSet<>();
		if (!new File(versionFolder, ".git").exists()) {
			return versions;
		}
		try (Git git = Git.open(new File(versionFolder))) {
			for (RevCommit commit : git.log().addPath(path).call()) {
				ObjectId blob = git.getRepository().resolve(commit.getName() + ":" + path);
				if (blob == null) {
					continue;
				}
				String json = new String(git.getRepository().open(blob).getBytes(), StandardCharsets.UTF_8);
				JsonElement version = JsonParser.parseString(json).getAsJsonObject().get("version");
				if (version != null && seen.add(version.getAsString())) {
					versions.add(new Version(version.getAsString(), Instant.ofEpochSecond(commit.getCommitTime()), commit.getFullMessage().trim(), json));
				}
			}
		}
		return versions;
	}

	/** Writes {@code targetJson} back as a new commit, keeping the live {@code improvement} section with {@code paused} set. */
	static void restore(Insight insight, String engineId, IModelRouterEngine model, String liveJson, String targetJson, String reason, boolean pause) {
		JsonObject restored = JsonParser.parseString(targetJson).getAsJsonObject();
		JsonObject live = JsonParser.parseString(liveJson).getAsJsonObject().getAsJsonObject("improvement");
		JsonObject improvement = live == null ? new JsonObject() : live.deepCopy();
		improvement.addProperty("paused", pause);
		restored.add("improvement", improvement);
		UpdateModelRouterConfigReactor.save(insight, engineId, model, restored.toString(), reason);
	}

	/** The versions as the status read lists them, newest first: {@code version}, {@code committed_at}, {@code message}, {@code changes}. */
	static List<Map<String, Object>> describe(List<Version> history, int limit) {
		List<Map<String, Object>> out = new ArrayList<>();
		for (int i = 0; i < Math.min(history.size(), limit); i++) {
			Version v = history.get(i);
			List<String> changes = changes(i + 1 < history.size() ? history.get(i + 1).json() : null, v.json());
			Map<String, Object> entry = new LinkedHashMap<>();
			entry.put("version", v.version());
			entry.put("committed_at", v.committedAt().toString());
			entry.put("message", v.message());
			entry.put("changes", changes.isEmpty() ? List.of("no routing change") : changes);
			out.add(entry);
		}
		return out;
	}

	/** What {@code json} changes against {@code previousJson} (null for the first version), one short line each. */
	static List<String> changes(String previousJson, String json) {
		if (previousJson == null) {
			return List.of("initial setup");
		}
		JsonObject before = JsonParser.parseString(previousJson).getAsJsonObject();
		JsonObject after = JsonParser.parseString(json).getAsJsonObject();
		List<String> lines = new ArrayList<>();
		Map<String, JsonObject> routesBefore = byKey(before.getAsJsonArray("routes"), "name");
		Map<String, JsonObject> routesAfter = byKey(after.getAsJsonArray("routes"), "name");
		routesAfter.forEach((name, route) -> {
			if (!routesBefore.containsKey(name)) {
				lines.add("Route " + name + " added");
			} else if (!Objects.equals(text(route, "engine_id"), text(routesBefore.get(name), "engine_id"))) {
				lines.add("Route " + name + " now uses a different model");
			}
		});
		routesBefore.keySet().stream().filter(name -> !routesAfter.containsKey(name)).forEach(name -> lines.add("Route " + name + " removed"));
		Map<String, JsonObject> stepsBefore = byKey(before.getAsJsonArray("subtasks"), "id");
		Map<String, JsonObject> stepsAfter = byKey(after.getAsJsonArray("subtasks"), "id");
		stepsAfter.forEach((id, step) -> {
			JsonObject old = stepsBefore.get(id);
			String label = "\"" + label(step) + "\"";
			if (old == null) {
				lines.add("Step type " + label + " added on " + text(step, "route"));
				return;
			}
			if (!Objects.equals(text(step, "route"), text(old, "route"))) {
				lines.add("Step type " + label + " moved from " + text(old, "route") + " to " + text(step, "route"));
			}
			String skillBefore = text(old, "skill_id");
			String skillAfter = text(step, "skill_id");
			if (!Objects.equals(skillBefore, skillAfter)) {
				lines.add((skillBefore == null ? "Skill attached to " : skillAfter == null ? "Skill removed from " : "Skill updated on ") + label);
			}
			if (!Objects.equals(step.get("recognition_rules"), old.get("recognition_rules"))) {
				lines.add("Recognition rules changed for " + label);
			}
		});
		stepsBefore.forEach((id, step) -> {
			if (!stepsAfter.containsKey(id)) {
				lines.add("Step type \"" + label(step) + "\" removed");
			}
		});
		if (!Objects.equals(text(before, "classifier_engine"), text(after, "classifier_engine"))) {
			lines.add("Classifier changed");
		}
		return lines;
	}

	private static String label(JsonObject step) {
		String definition = text(step, "definition");
		return definition == null ? text(step, "id") : definition.replaceAll("\\.$", "");
	}

	private static Map<String, JsonObject> byKey(JsonArray array, String key) {
		Map<String, JsonObject> out = new LinkedHashMap<>();
		if (array != null) {
			array.forEach(e -> out.put(text(e.getAsJsonObject(), key), e.getAsJsonObject()));
		}
		return out;
	}

	private static String text(JsonObject object, String key) {
		JsonElement value = object.get(key);
		return value == null || value.isJsonNull() ? null : value.getAsString();
	}
}
