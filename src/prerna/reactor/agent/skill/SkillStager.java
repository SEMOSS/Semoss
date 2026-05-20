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
package prerna.reactor.agent.skill;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.engine.api.IStorageEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.util.Utility;

/**
 * Materializes attached skills into a working directory so Claude Code's
 * {@code .claude/skills/} discovery picks them up at agent run time.
 *
 * <p>For each skill ref in the merged {@code agentConfig.getSkills()} list,
 * this stager:
 * <ol>
 *   <li>Loads the {@code SKILL__} row and resolves the effective version
 *       (the {@code pinned_version} on the ref when set, otherwise
 *       {@code SKILL__.CURRENT_VERSION}).</li>
 *   <li>Checks the existing {@code .skill-meta} sidecar at
 *       {@code <workingDir>/.claude/skills/<slug>/.skill-meta}. When
 *       {@code {skill_id, version, content_hash}} matches, the skill is
 *       already current and the download is skipped.</li>
 *   <li>Otherwise: wipes any previously-staged skill folder for this slug,
 *       creates a fresh one, calls
 *       {@link IStorageEngine#syncStorageToLocal(String, String)} from the
 *       version-specific {@code STORAGE_PREFIX} into the target dir, then
 *       writes the {@code .skill-meta} sidecar with the new tuple.</li>
 * </ol>
 *
 * <p>This is a best-effort step - individual skill failures are logged and
 * the run continues. A failure to stage one skill should not prevent the
 * agent from running with the others.
 *
 * <p>On-disk layout matches what
 * {@link prerna.reactor.agent.AppBuilderHarnessConfiguration} writes for
 * project-local skills, so Claude Code's skill discovery treats both
 * sources identically. When both a project-local skill and a registry
 * skill share a slug, the registry skill wins (it's staged last in the
 * AgentRunner flow) and a warning is logged.
 */
public final class SkillStager {

	private static final Logger logger = LogManager.getLogger(SkillStager.class);

	// Claude Code's discovery convention: it scans <workingDir>/.claude/skills/
	// at run start. Layout matches what AppBuilderHarnessConfiguration writes for
	// project-local skills so both sources show up the same way. A future stager
	// for a different harness would override these (or take them as parameters).
	private static final String CLAUDE_DIR     = ".claude";
	private static final String SKILLS_DIR     = "skills";
	private static final String SKILL_META_FILE = ".skill-meta";

	private SkillStager() {}

	/**
	 * Stage every skill ref in {@code skillRefs} under
	 * {@code <workingDir>/.claude/skills/}. Skipped silently when
	 * {@code workingDir} is blank or {@code skillRefs} is empty.
	 *
	 * @param workingDir agent's working directory (the value returned from
	 *                   {@code AgentRunner.resolveWorkingDir})
	 * @param skillRefs  merged skill references: each map should contain
	 *                   {@code skill_id} (required) and {@code pinned_version}
	 *                   (optional; null/empty = use SKILL.CURRENT_VERSION)
	 */
	public static void stage(String workingDir, List<Map<String, String>> skillRefs) {
		if (workingDir == null || workingDir.trim().isEmpty()) {
			return;
		}
		if (skillRefs == null || skillRefs.isEmpty()) {
			return;
		}

		Path skillsRoot = Paths.get(workingDir, CLAUDE_DIR, SKILLS_DIR);
		try {
			Files.createDirectories(skillsRoot);
		} catch (IOException e) {
			logger.warn("SkillStager: failed to create skills root '{}': {}", skillsRoot, e.getMessage());
			return;
		}

		int staged = 0;
		int skipped = 0;
		int failed = 0;
		for (Map<String, String> ref : skillRefs) {
			if (ref == null) continue;
			String skillId = ref.get("skill_id");
			if (skillId == null || skillId.isEmpty()) {
				logger.warn("SkillStager: skipping skill ref with no skill_id: {}", ref);
				failed++;
				continue;
			}
			String pinned = ref.get("pinned_version");
			try {
				StageOutcome outcome = stageOne(skillsRoot, skillId, pinned);
				if (outcome == StageOutcome.STAGED) staged++;
				else if (outcome == StageOutcome.CACHED) skipped++;
				else failed++;
			} catch (Exception e) {
				logger.warn("SkillStager: failed to stage skill '{}': {}", skillId, e.getMessage(), e);
				failed++;
			}
		}
		logger.info("SkillStager: workingDir='{}' total={} staged={} cached={} failed={}",
				workingDir, skillRefs.size(), staged, skipped, failed);
	}

	private enum StageOutcome { STAGED, CACHED, FAILED }

	private static StageOutcome stageOne(Path skillsRoot, String skillId, String pinnedVersion) throws Exception {
		Map<String, Object> skillRow = ModelInferenceLogsUtils.getSkillEntry(skillId);
		if (skillRow == null) {
			logger.warn("SkillStager: skill '{}' not found in SKILL__; skipping", skillId);
			return StageOutcome.FAILED;
		}
		String slug = (String) skillRow.get("slug");
		String storageEngineId = (String) skillRow.get("storage_engine_id");
		if (slug == null || slug.isEmpty() || storageEngineId == null || storageEngineId.isEmpty()) {
			logger.warn("SkillStager: skill '{}' has no slug or storage_engine_id; skipping", skillId);
			return StageOutcome.FAILED;
		}

		int effectiveVersion;
		String versionPrefix;
		if (pinnedVersion != null && !pinnedVersion.isEmpty()) {
			try {
				effectiveVersion = Integer.parseInt(pinnedVersion);
			} catch (NumberFormatException e) {
				logger.warn("SkillStager: skill '{}' has non-numeric pinned_version='{}'; skipping",
						skillId, pinnedVersion);
				return StageOutcome.FAILED;
			}
			Map<String, Object> versionRow = ModelInferenceLogsUtils.getSkillVersion(skillId, effectiveVersion);
			if (versionRow == null) {
				logger.warn("SkillStager: skill '{}' has no version {}; skipping", skillId, effectiveVersion);
				return StageOutcome.FAILED;
			}
			versionPrefix = (String) versionRow.get("storage_prefix");
		} else {
			Object currentVersionObj = skillRow.get("current_version");
			effectiveVersion = (currentVersionObj instanceof Number)
					? ((Number) currentVersionObj).intValue()
					: -1;
			versionPrefix = (String) skillRow.get("storage_prefix");
		}
		if (versionPrefix == null || versionPrefix.isEmpty()) {
			logger.warn("SkillStager: skill '{}' v{} has no storage_prefix; skipping", skillId, effectiveVersion);
			return StageOutcome.FAILED;
		}
		String expectedHash = pinnedVersion != null && !pinnedVersion.isEmpty()
				? lookupVersionHash(skillId, effectiveVersion)
				: (String) skillRow.get("content_hash");

		Path targetDir = skillsRoot.resolve(slug);
		if (cacheHit(targetDir, skillId, effectiveVersion, expectedHash)) {
			return StageOutcome.CACHED;
		}

		IStorageEngine storage = Utility.getStorage(storageEngineId);
		if (storage == null) {
			logger.warn("SkillStager: could not load storage engine '{}' for skill '{}'; skipping",
					storageEngineId, skillId);
			return StageOutcome.FAILED;
		}

		// Wipe the previous staged copy for this slug. This also handles the
		// project-local-vs-registry conflict noted in the design - the registry
		// skill overlays any pre-existing local content with the same slug.
		if (Files.exists(targetDir)) {
			deleteTree(targetDir);
			logger.info("SkillStager: re-staging skill '{}' (slug='{}') - wiped existing dir", skillId, slug);
		}
		Files.createDirectories(targetDir);

		storage.syncStorageToLocal(versionPrefix, targetDir.toString());
		writeMetaSidecar(targetDir, skillId, effectiveVersion, expectedHash);
		logger.info("SkillStager: staged skill '{}' v{} into '{}'", skillId, effectiveVersion, targetDir);
		return StageOutcome.STAGED;
	}

	private static String lookupVersionHash(String skillId, int version) {
		Map<String, Object> row = ModelInferenceLogsUtils.getSkillVersion(skillId, version);
		return row == null ? null : (String) row.get("content_hash");
	}

	/**
	 * Returns true when an existing {@code .skill-meta} sidecar matches the
	 * tuple we'd otherwise write. Any read error / mismatch returns false,
	 * which forces a re-stage.
	 */
	private static boolean cacheHit(Path targetDir, String skillId, int version, String expectedHash) {
		Path meta = targetDir.resolve(SKILL_META_FILE);
		if (!Files.exists(meta)) {
			return false;
		}
		try {
			String text = new String(Files.readAllBytes(meta), StandardCharsets.UTF_8);
			JSONObject obj = new JSONObject(text);
			String metaSkillId = obj.optString("skill_id", null);
			int metaVersion = obj.optInt("version", -1);
			String metaHash = obj.optString("content_hash", null);
			return skillId.equals(metaSkillId)
					&& version == metaVersion
					&& expectedHash != null
					&& expectedHash.equals(metaHash);
		} catch (Exception e) {
			logger.debug("SkillStager: cache miss for skill '{}' - unreadable sidecar: {}", skillId, e.getMessage());
			return false;
		}
	}

	private static void writeMetaSidecar(Path targetDir, String skillId, int version, String contentHash)
			throws IOException {
		JSONObject obj = new JSONObject();
		obj.put("skill_id", skillId);
		obj.put("version", version);
		if (contentHash != null) {
			obj.put("content_hash", contentHash);
		}
		Files.write(targetDir.resolve(SKILL_META_FILE),
				obj.toString().getBytes(StandardCharsets.UTF_8));
	}

	private static void deleteTree(Path root) throws IOException {
		try (Stream<Path> walk = Files.walk(root)) {
			walk.sorted(Comparator.reverseOrder()).forEach(p -> {
				try {
					Files.deleteIfExists(p);
				} catch (IOException e) {
					logger.warn("SkillStager: failed to delete '{}': {}", p, e.getMessage());
				}
			});
		}
	}
}
