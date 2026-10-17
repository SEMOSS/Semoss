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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.engine.impl.model.RoomUtils;
import prerna.om.Insight;
import prerna.reactor.agent.AgentRunner;
import prerna.reactor.agent.run.AgentRunHandle;
import prerna.reactor.agent.run.AgentRunRequest;
import prerna.reactor.agent.run.AgentRunService;
import prerna.reactor.agent.skill.Skill;
import prerna.reactor.agent.skill.SkillProjects;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

/**
 * Builds or changes one of the owner's skills the way the skill workbench does: a separate agent run rooted in the
 * skill's own folder, with the platform file and Python tools, working from the brief the owner approved.
 */
public final class CollaborationSkillBuilder {

	private static final Logger classLogger = LogManager.getLogger(CollaborationSkillBuilder.class);

	private static final int MAX_TURNS = 40;
	private static final long WAIT_MS = 8 * 60 * 1000L;
	static final int MAX_BRIEF_CHARS = 8_000;

	private CollaborationSkillBuilder() {
	}

	/**
	 * Runs the builder on the user's own skill and waits for it. Returns what changed, the builder's own summary,
	 * and whether it is still going after the wait.
	 */
	public static Map<String, Object> build(User user, Insight insight, String skillId, String brief, boolean created) {
		if (!CollaborationSkills.isMine(user, skillId)) {
			throw new IllegalArgumentException("Pick one of your own Collaboration skills; " + skillId
					+ " is not one of them");
		}
		if (brief == null || brief.isBlank()) {
			throw new IllegalArgumentException("Say what the skill should do or what to change");
		}
		if (brief.length() > MAX_BRIEF_CHARS) {
			throw new IllegalArgumentException("Keep the brief under " + MAX_BRIEF_CHARS + " characters");
		}
		SkillProjects.SkillInfo info = SkillProjects.resolve(skillId);
		Path dir = info.skillDir;
		if (dir == null) {
			throw new IllegalStateException("The skill has no folder to work in");
		}
		String engine = engine(user);
		if (engine == null) {
			throw new IllegalArgumentException("No model is set up for Collaboration; ask an admin to set one");
		}

		Map<String, String> before = snapshot(dir);
		String roomId = UUID.randomUUID().toString();
		RoomUtils.createRoomIfNotExists(roomId, insight, null, "Skill builder: " + info.displayName, null, null, null,
				null, null);
		Map<String, Object> params = new HashMap<>();
		// root the run in the skill's own content folder, as the workbench roots its assistant in the project
		params.put(AgentRunner.PARAM_PROJECT, skillId);
		params.put(AgentRunner.PARAM_SUBDIR, Skill.SKILL_ASSET_SUBFOLDER);
		AgentRunRequest request = new AgentRunRequest(roomId, command(info, brief, created), engine, "semoss", null,
				MAX_TURNS, 0, params, null, null, null, insight);

		AgentRunHandle handle = AgentRunService.get().run(request);
		Map<String, Object> run;
		try {
			run = AgentRunService.get().waitForRun(handle.runId(), insight, WAIT_MS);
		} catch (InterruptedException e) {
			// the owner stopped the chat, so stop the builder too
			AgentRunService.get().stop(handle.runId(), insight);
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Stopped building the skill");
		}

		boolean stillRunning = Boolean.TRUE.equals(run.get("waitTimedOut"));
		Map<String, String> after = snapshot(dir);
		Map<String, Object> result = new LinkedHashMap<>(CollaborationSkills.describe(skillId));
		result.put("status", stillRunning ? "still working" : String.valueOf(run.get("status")).toLowerCase());
		result.putAll(changes(before, after));
		result.put("builderSummary", run.get("finalText"));
		result.put("builderRoomId", roomId);
		if (stillRunning) {
			result.put("note", "The builder is still working on its own and will finish without you. Check the "
					+ "files later with ReadMySkill; do not wait for it with sub-agent tools.");
		} else {
			commit(skillId, info.displayName, brief);
		}
		return result;
	}

	/** The model the owner's assistant uses, else Collaboration's own; either must be one they can use. */
	private static String engine(User user) {
		Map<String, Object> assistant = CollaborationUtils.threadAgent(user);
		Object modelId = assistant == null ? null : assistant.get("modelId");
		if (modelId != null && !String.valueOf(modelId).isBlank()
				&& SecurityEngineUtils.userCanViewEngine(user, String.valueOf(modelId).trim())) {
			return String.valueOf(modelId).trim();
		}
		return BrainTopicModel.engine(user);
	}

	static String command(SkillProjects.SkillInfo info, String brief, boolean created) {
		return "You are building a skill for the owner's Collaboration assistant and routines. Your working "
				+ "directory is the skill's own folder, \"" + info.slug + "\"; everything you write stays in it. "
				+ (created ? "The skill was just created with a placeholder SKILL.md; replace its body."
						: "The skill already exists; read its files first and change only what the brief asks.")
				+ "\n\n"
				+ "How a skill is laid out:\n"
				+ "- SKILL.md is the entry point. Keep its frontmatter: the name must stay \"" + info.name + "\"; "
				+ "update the description only when the brief changes what the skill is for. The description says, "
				+ "in one or two sentences, when an agent should load the skill.\n"
				+ "- Keep SKILL.md short: what the skill is for, the rules that always apply, and a table that routes "
				+ "to the other files with relative links, such as [references/format.md](references/format.md).\n"
				+ "- references/ holds longer guidance an agent loads only when needed, one topic per file.\n"
				+ "- templates/ holds layouts the result is filled into, such as templates/report.html. Use simple "
				+ "email-safe HTML with inline style attributes and table, tr, th, td, h2, h3, p, ul, li, strong, "
				+ "span, div; no scripts, stylesheets, images, forms or outside fonts. Mark what repeats with HTML "
				+ "comments, such as <!-- one row per meeting -->, and say in SKILL.md when to use the template.\n"
				+ "- scripts/ holds Python helpers only when a step is mechanical and repeated; say in SKILL.md how to "
				+ "run them.\n"
				+ "- Write instructions as direct rules for the agent that will use the skill. Results it produces "
				+ "go straight to the owner, so ask for direct report-style writing with no narration.\n"
				+ "- Never put passwords, keys, tokens, or anyone's private data in the skill.\n\n"
				+ "Work with the file tools. Read back what you wrote before you finish. Your final message is shown to "
				+ "the owner: two or three sentences on what the skill now does and which files you added, changed "
				+ "or removed.\n\n"
				+ "Brief from the owner:\n" + brief;
	}

	/** Path to size and modified time for every file under the skill folder. */
	static Map<String, String> snapshot(Path dir) {
		Map<String, String> files = new LinkedHashMap<>();
		if (!Files.isDirectory(dir)) {
			return files;
		}
		try (Stream<Path> walk = Files.walk(dir)) {
			walk.filter(Files::isRegularFile).filter(p -> !dir.relativize(p).toString().startsWith("."))
					.sorted().forEach(p -> {
						try {
							files.put(dir.relativize(p).toString().replace('\\', '/'),
									Files.size(p) + ":" + Files.getLastModifiedTime(p).toMillis());
						} catch (IOException e) {
							classLogger.debug("Could not read {}", p, e);
						}
					});
		} catch (IOException e) {
			classLogger.warn("Could not list skill folder {}: {}", dir, e.getMessage());
		}
		return files;
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
	private static void commit(String skillId, String name, String brief) {
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
			String line = brief.strip().lines().findFirst().orElse("");
			GitRepoUtils.addAllChangesAndCommit(gitFolder, false,
					"Collaboration skill " + name + ": " + (line.length() > 120 ? line.substring(0, 120) : line));
		} catch (Exception e) {
			classLogger.warn("Could not record a version of skill {}: {}", skillId, e.getMessage());
		}
	}
}
