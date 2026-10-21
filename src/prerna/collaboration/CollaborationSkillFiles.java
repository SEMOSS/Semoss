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
package prerna.collaboration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.engine.impl.model.Room;
import prerna.reactor.agent.skill.Skill;
import prerna.reactor.agent.skill.SkillProjects;
import prerna.usertracking.AuditEventType;
import prerna.usertracking.AuditTargetType;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

/**
 * The owner's skills as drafts in the chat's own room folder: OpenSkill copies a published skill into
 * skills/&lt;slug&gt;/, the model writes there with its file tools, and PublishSkill checks the folder and mirrors
 * it into the skill, keeping a version in the skill's git history.
 */
public final class CollaborationSkillFiles {

	private static final Logger classLogger = LogManager.getLogger(CollaborationSkillFiles.class);

	public static final String DRAFTS = "skills";
	// written next to a draft once it has a skill, so publishing again updates rather than duplicates
	static final String ID_FILE = ".skill-id";
	static final int MAX_FILES = 20;
	static final int MAX_FILE_CHARS = 30_000;
	static final int MAX_TOTAL_CHARS = 120_000;
	private static final Set<String> FOLDERS = Set.of("references", "templates", "scripts");
	private static final Set<String> EXTENSIONS = Set.of("md", "txt", "html", "htm", "json", "csv", "py", "yaml",
			"yml");
	// left behind by running a script; never published
	private static final Set<String> SKIPPED = Set.of("__pycache__");
	private static final Pattern SEGMENT = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,79}");

	private CollaborationSkillFiles() {
	}

	/** Copies one of the user's own skills into the room as skills/&lt;slug&gt;/, replacing any older draft. */
	public static Map<String, Object> open(User user, String roomId, String skillId) {
		if (!CollaborationSkills.isMine(user, skillId)) {
			throw new IllegalArgumentException("Pick one of your own Collaboration skills; " + skillId
					+ " is not one of them");
		}
		SkillProjects.SkillInfo info = SkillProjects.resolve(skillId);
		if (info.skillDir == null) {
			throw new IllegalStateException("The skill has no files yet");
		}
		Path source = info.skillDir.normalize();
		Path draft = draftsRoot(roomId).resolve(info.slug);
		try {
			deleteTree(draft);
			Map<String, String> files = read(source);
			for (Map.Entry<String, String> file : files.entrySet()) {
				Path target = draft.resolve(file.getKey());
				Files.createDirectories(target.getParent());
				Files.writeString(target, file.getValue(), StandardCharsets.UTF_8);
			}
			Files.createDirectories(draft);
			Files.writeString(draft.resolve(ID_FILE), skillId, StandardCharsets.UTF_8);
			Map<String, Object> out = new LinkedHashMap<>(CollaborationSkills.describe(skillId));
			out.put("path", DRAFTS + "/" + info.slug);
			out.put("files", new ArrayList<>(files.keySet()));
			String skillMd = files.getOrDefault(Skill.SKILL_FILE, "");
			out.put("skillMd", skillMd.length() > MAX_FILE_CHARS ? skillMd.substring(0, MAX_FILE_CHARS) : skillMd);
			out.put("note", "Edit the files under " + DRAFTS + "/" + info.slug + " with the file tools, then "
					+ "PublishSkill with that path. Nothing changes for the owner until then.");
			return out;
		} catch (IOException e) {
			throw new IllegalStateException("Could not open the skill: " + e.getMessage(), e);
		}
	}

	/**
	 * Publishes a draft folder from the room: a new skill the first time, else the skill it was opened from. The
	 * skill's files become exactly the draft's.
	 */
	public static Map<String, Object> publish(User user, String roomId, String path) {
		Path drafts = draftsRoot(roomId);
		Path draft = drafts.getParent().resolve(path == null ? "" : path.trim()).normalize();
		// only a folder directly under skills/, never a staged skill or anything else in the room
		if (!drafts.equals(draft.getParent()) || !Files.isDirectory(draft, LinkOption.NOFOLLOW_LINKS)) {
			throw new IllegalArgumentException("No skill folder at " + path + "; write one as " + DRAFTS
					+ "/<name>/SKILL.md in this chat's folder first");
		}
		Map<String, String> files = check(draft);
		String skillMd = files.remove(Skill.SKILL_FILE);
		Skill.Frontmatter fm = Skill.parseFrontmatter(skillMd);
		if (fm.description == null || fm.description.isBlank()) {
			throw new IllegalArgumentException("SKILL.md needs frontmatter with a description of when to load it");
		}
		String body = Skill.stripFrontmatter(skillMd).strip();
		if (body.isEmpty()) {
			throw new IllegalArgumentException("SKILL.md has no instructions below its frontmatter");
		}

		String skillId = draftSkillId(user, draft);
		boolean created = skillId == null;
		if (created) {
			if (fm.name == null || fm.name.isBlank()) {
				throw new IllegalArgumentException("SKILL.md needs frontmatter with the skill's name");
			}
			for (Map<String, Object> skill : CollaborationSkills.mine(user)) {
				if (fm.name.trim().equalsIgnoreCase(String.valueOf(skill.get("name")))) {
					throw new IllegalArgumentException("You already have a skill named " + fm.name.trim() + " ("
							+ skill.get("id") + "); OpenSkill it and edit that one instead");
				}
			}
			skillId = String.valueOf(CollaborationSkills.create(user, fm.name, fm.description, body).get("id"));
		}

		SkillProjects.SkillInfo info = SkillProjects.resolve(skillId);
		if (info.skillDir == null) {
			throw new IllegalStateException("The skill has no folder to publish into");
		}
		Path dir = info.skillDir.normalize();
		Map<String, Object> result;
		try {
			Map<String, String> before = created ? Map.of() : read(dir);
			// the name never changes; everything else is the draft's
			files.put(Skill.SKILL_FILE, Skill.buildFrontmatter(info.name, fm.description.trim()) + body + "\n");
			for (String old : read(dir).keySet()) {
				if (!files.containsKey(old)) {
					Path gone = dir.resolve(old);
					Files.deleteIfExists(gone);
					dropEmptyFolders(dir, gone.getParent());
				}
			}
			for (Map.Entry<String, String> file : files.entrySet()) {
				Path target = dir.resolve(file.getKey()).normalize();
				Files.createDirectories(target.getParent());
				Files.writeString(target, file.getValue(), StandardCharsets.UTF_8);
			}
			Files.writeString(draft.resolve(ID_FILE), skillId, StandardCharsets.UTF_8);
			result = new LinkedHashMap<>(CollaborationSkills.describe(skillId));
			result.put("status", created ? "created" : "updated");
			result.put("path", DRAFTS + "/" + draft.getFileName());
			result.putAll(changes(before, files));
		} catch (IOException e) {
			throw new IllegalStateException("Could not publish the skill: " + e.getMessage(), e);
		}
		commit(skillId, info.displayName, created ? "created" : "updated");
		UserAuditTrailUtils.recordResourceEvent(user, AuditEventType.SKILL_UPDATE, AuditTargetType.SKILL, skillId,
				info.displayName, skillId, null, null, Map.of("published", true));
		result.put("note", "Published. Tell the owner in a sentence or two what the skill does; no need to read it "
				+ "back. To change it later in this chat, edit the files under " + DRAFTS + "/" + draft.getFileName()
				+ " and call PublishSkill again.");
		return result;
	}

	// the skill a draft was opened from or published as, when it is still the user's own
	private static String draftSkillId(User user, Path draft) {
		Path idFile = draft.resolve(ID_FILE);
		if (!Files.isRegularFile(idFile, LinkOption.NOFOLLOW_LINKS)) {
			return null;
		}
		try {
			String skillId = Files.readString(idFile, StandardCharsets.UTF_8).trim();
			if (CollaborationSkills.isMine(user, skillId)) {
				return skillId;
			}
			throw new IllegalArgumentException("This folder belongs to a skill that is no longer one of yours; "
					+ "remove its " + ID_FILE + " file to publish it as a new skill");
		} catch (IOException e) {
			return null;
		}
	}

	/**
	 * Every file of a draft as path to text, after checking it: SKILL.md at the top, the rest under references/,
	 * templates/ or scripts/, plain names, text types, within the size limits. Hidden files are left out.
	 */
	static Map<String, String> check(Path draft) {
		Map<String, String> files = new TreeMap<>();
		List<String> wrong = new ArrayList<>();
		int total = 0;
		try (Stream<Path> walk = Files.walk(draft)) {
			for (Path file : walk.sorted().toList()) {
				String rel = draft.relativize(file).toString().replace('\\', '/');
				if (rel.isEmpty() || hidden(rel) || skipped(rel)) {
					continue;
				}
				if (Files.isSymbolicLink(file)) {
					wrong.add(rel + " (a link)");
					continue;
				}
				if (Files.isDirectory(file)) {
					continue;
				}
				String problem = problem(rel);
				if (problem != null) {
					wrong.add(rel + " (" + problem + ")");
					continue;
				}
				String text = Files.readString(file, StandardCharsets.UTF_8);
				if (text.length() > MAX_FILE_CHARS) {
					wrong.add(rel + " (over " + MAX_FILE_CHARS + " characters)");
					continue;
				}
				total += text.length();
				files.put(rel, text);
			}
		} catch (IOException | RuntimeException e) {
			throw new IllegalArgumentException("Could not read the skill folder: " + e.getMessage());
		}
		if (!wrong.isEmpty()) {
			throw new IllegalArgumentException("Fix or remove these before publishing: " + String.join("; ", wrong)
					+ ". A skill holds SKILL.md plus text files under references/, templates/ or scripts/.");
		}
		if (!files.containsKey(Skill.SKILL_FILE)) {
			throw new IllegalArgumentException("The folder has no SKILL.md");
		}
		if (files.size() > MAX_FILES) {
			throw new IllegalArgumentException("Keep a skill to " + MAX_FILES + " files");
		}
		if (total > MAX_TOTAL_CHARS) {
			throw new IllegalArgumentException("Keep a skill under " + MAX_TOTAL_CHARS + " characters in all");
		}
		return files;
	}

	// why a file cannot be part of a skill, or null when it can
	private static String problem(String rel) {
		if (Skill.SKILL_FILE.equals(rel)) {
			return null;
		}
		String[] parts = rel.split("/");
		if (parts.length < 2 || parts.length > 3 || !FOLDERS.contains(parts[0])) {
			return "not under references/, templates/ or scripts/";
		}
		for (String part : parts) {
			if (!SEGMENT.matcher(part).matches() || part.contains("..")) {
				return "use letters, numbers, - _ . in names";
			}
		}
		String name = parts[parts.length - 1];
		int dot = name.lastIndexOf('.');
		if (dot < 0 || !EXTENSIONS.contains(name.substring(dot + 1).toLowerCase())) {
			return "not a text file type";
		}
		return null;
	}

	private static boolean hidden(String rel) {
		for (String part : rel.split("/")) {
			if (part.startsWith(".")) {
				return true;
			}
		}
		return false;
	}

	private static boolean skipped(String rel) {
		for (String part : rel.split("/")) {
			if (SKIPPED.contains(part)) {
				return true;
			}
		}
		return false;
	}

	private static Path draftsRoot(String roomId) {
		if (roomId == null || roomId.isBlank()) {
			throw new IllegalStateException("This only works inside a chat");
		}
		return Path.of(Room.roomFolderPath(roomId)).toAbsolutePath().normalize().resolve(DRAFTS);
	}

	// the published files of a skill, as path to text, without hidden ones
	private static Map<String, String> read(Path dir) throws IOException {
		Map<String, String> files = new TreeMap<>();
		if (!Files.isDirectory(dir)) {
			return files;
		}
		try (Stream<Path> walk = Files.walk(dir)) {
			for (Path file : walk.filter(Files::isRegularFile).toList()) {
				String rel = dir.relativize(file).toString().replace('\\', '/');
				if (!hidden(rel) && !skipped(rel)) {
					files.put(rel, Files.readString(file, StandardCharsets.UTF_8));
				}
			}
		}
		return files;
	}

	private static void deleteTree(Path path) throws IOException {
		if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
			return;
		}
		try (Stream<Path> walk = Files.walk(path)) {
			for (Path p : walk.sorted(Comparator.reverseOrder()).toList()) {
				Files.delete(p);
			}
		}
	}

	private static void dropEmptyFolders(Path dir, Path folder) {
		Path current = folder;
		while (current != null && !current.equals(dir) && current.startsWith(dir)) {
			try (Stream<Path> entries = Files.list(current)) {
				if (entries.findAny().isPresent()) {
					return;
				}
			} catch (IOException e) {
				return;
			}
			try {
				Files.delete(current);
			} catch (IOException e) {
				classLogger.debug("Could not tidy {}", current, e);
				return;
			}
			current = current.getParent();
		}
	}

	private static Map<String, Object> changes(Map<String, String> before, Map<String, String> after) {
		List<String> added = new ArrayList<>();
		List<String> changed = new ArrayList<>();
		List<String> removed = new ArrayList<>();
		for (Map.Entry<String, String> file : after.entrySet()) {
			String old = before.get(file.getKey());
			if (old == null) {
				added.add(file.getKey());
			} else if (!old.equals(file.getValue())) {
				changed.add(file.getKey());
			}
		}
		for (String path : before.keySet()) {
			if (!after.containsKey(path)) {
				removed.add(path);
			}
		}
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("added", added);
		out.put("changed", changed);
		out.put("removed", removed);
		out.put("files", new ArrayList<>(after.keySet()));
		return out;
	}

	/** Keeps a version of each change in the skill's git history, as the workbench's commit hook does. */
	private static void commit(String skillId, String name, String note) {
		try {
			IEngine project = Utility.getProject(skillId);
			if (project == null) {
				return;
			}
			String gitFolder = EngineUtility.getSpecificEngineVersionFolder(CATALOG_TYPE.PROJECT, skillId,
					project.getEngineName());
			if (gitFolder == null || !Files.isDirectory(Path.of(gitFolder, ".git"))) {
				return;
			}
			GitRepoUtils.addAllChangesAndCommit(gitFolder, false, "Collaboration skill " + name + ": " + note);
		} catch (Exception e) {
			classLogger.warn("Could not record a version of skill {}: {}", skillId, e.getMessage());
		}
	}
}
