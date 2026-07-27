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
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

/**
 * Materializes attached skills into a working directory so Claude Code's
 * {@code .claude/skills/} discovery picks them up at agent run time.
 *
 * <p>For each skill ref in the merged {@code agentConfig.getSkills()} list,
 * this stager:
 * <ol>
 *   <li>Resolves the skill project's identity and content folder via
 *       {@link SkillProjects#resolve} (SKILL.md frontmatter under the
 *       project's {@code version/assets/public/} or legacy
 *       {@code version/assets/skill/} folder).</li>
 *   <li>Checks the existing {@code .skill-meta} sidecar at
 *       {@code <workingDir>/.claude/skills/<slug>/.skill-meta}. If the source
 *       folder's last-modified time matches what we previously staged, the
 *       cache hits and the copy is skipped.</li>
 *   <li>Otherwise: wipes any previously-staged skill folder for this slug,
 *       copies the source folder over, then writes the {@code .skill-meta}
 *       sidecar.</li>
 * </ol>
 *
 * <p>This is a best-effort step - individual skill failures are logged and
 * the run continues.
 *
 * <p>On-disk layout matches what
 * {@link prerna.reactor.agent.AppBuilderHarnessConfiguration} writes for
 * project-local skills, so Claude Code's skill discovery treats both
 * sources identically. When two attached skills resolve to the same slug,
 * the first one staged wins and the duplicate is skipped with a warning.
 */
public final class SkillStager {

	private static final Logger logger = LogManager.getLogger(SkillStager.class);

	private static final String CLAUDE_DIR      = ".claude";
	private static final String SKILLS_DIR      = "skills";
	private static final String SKILL_META_FILE = ".skill-meta";

	private SkillStager() {}

	/**
	 * Stage every skill ref in {@code skillRefs} under
	 * {@code <workingDir>/.claude/skills/}. Skipped silently when
	 * {@code workingDir} is blank or {@code skillRefs} is empty.
	 *
	 * @param workingDir agent's working directory
	 * @param skillRefs  merged skill references: each map should contain
	 *                   {@code skill_id} (required)
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
		int duplicates = 0;
		int failed = 0;
		Set<String> stagedSlugs = new HashSet<>();
		for (Map<String, String> ref : skillRefs) {
			if (ref == null) continue;
			String skillId = ref.get("skill_id");
			if (skillId == null || skillId.isEmpty()) {
				logger.warn("SkillStager: skipping skill ref with no skill_id: {}", ref);
				failed++;
				continue;
			}
			try {
				SkillProjects.SkillInfo info = SkillProjects.resolve(skillId);
				if (info.skillDir == null) {
					logger.warn("SkillStager: skill '{}' has no skill content folder; skipping", skillId);
					failed++;
					continue;
				}
				if (!stagedSlugs.add(info.slug)) {
					// two attached skills resolved to the same slug - first one staged wins,
					// otherwise the identity mismatch would wipe-and-copy on every run
					logger.warn("SkillStager: skill '{}' resolves to slug '{}' which is already staged this run; "
							+ "skipping duplicate", skillId, info.slug);
					duplicates++;
					continue;
				}
				StageOutcome outcome = stageFromSource(skillsRoot, info.slug, info.skillDir, skillId);
				if (outcome == StageOutcome.STAGED) staged++;
				else if (outcome == StageOutcome.CACHED) skipped++;
				else failed++;
			} catch (Exception e) {
				logger.warn("SkillStager: failed to stage skill '{}': {}", skillId, e.getMessage(), e);
				failed++;
			}
		}
		logger.info("SkillStager: workingDir='{}' total={} staged={} cached={} duplicates={} failed={}",
				workingDir, skillRefs.size(), staged, skipped, duplicates, failed);
	}

	private enum StageOutcome { STAGED, CACHED, FAILED }

	/**
	 * Shared staging core: fingerprint the source, short-circuit on an unchanged
	 * cache, otherwise wipe-and-copy and refresh the {@code .skill-meta} sidecar.
	 * The {@code identity} is the cache key written into (and matched against) the
	 * sidecar - the skill's project id - so different skills never alias on a
	 * shared slug.
	 */
	private static StageOutcome stageFromSource(Path skillsRoot, String slug, Path sourceDir, String identity)
			throws IOException {
		long sourceFingerprint = computeFingerprint(sourceDir);
		Path targetDir = skillsRoot.resolve(slug);
		if (cacheHit(targetDir, identity, sourceFingerprint)) {
			return StageOutcome.CACHED;
		}

		if (Files.exists(targetDir)) {
			deleteTree(targetDir);
			logger.info("SkillStager: re-staging skill '{}' (slug='{}') - wiped existing dir", identity, slug);
		}
		Files.createDirectories(targetDir);

		copyTree(sourceDir, targetDir);
		writeMetaSidecar(targetDir, identity, sourceFingerprint);
		logger.info("SkillStager: staged skill '{}' from '{}' into '{}'", identity, sourceDir, targetDir);
		return StageOutcome.STAGED;
	}

	/**
	 * Cheap fingerprint of the source folder: the maximum file last-modified time
	 * (in milliseconds) across all regular files. Any add/edit/delete shifts this
	 * value, which is enough to invalidate the cache without hashing content.
	 */
	private static long computeFingerprint(Path sourceDir) throws IOException {
		final long[] max = { 0L };
		try (Stream<Path> walk = Files.walk(sourceDir)) {
			walk.filter(Files::isRegularFile).forEach(p -> {
				try {
					long mtime = Files.getLastModifiedTime(p).toMillis();
					if (mtime > max[0]) {
						max[0] = mtime;
					}
				} catch (IOException ignored) {
					// best-effort; missed files just mean cache may re-stage
				}
			});
		}
		return max[0];
	}

	private static boolean cacheHit(Path targetDir, String skillId, long expectedFingerprint) {
		Path meta = targetDir.resolve(SKILL_META_FILE);
		if (!Files.exists(meta)) {
			return false;
		}
		try {
			String text = new String(Files.readAllBytes(meta), StandardCharsets.UTF_8);
			JSONObject obj = new JSONObject(text);
			String metaSkillId = obj.optString("skill_id", null);
			long metaFingerprint = obj.optLong("source_fingerprint", -1L);
			return skillId.equals(metaSkillId) && metaFingerprint == expectedFingerprint;
		} catch (Exception e) {
			logger.debug("SkillStager: cache miss for skill '{}' - unreadable sidecar: {}", skillId, e.getMessage());
			return false;
		}
	}

	private static void writeMetaSidecar(Path targetDir, String skillId, long sourceFingerprint) throws IOException {
		JSONObject obj = new JSONObject();
		obj.put("skill_id", skillId);
		obj.put("source_fingerprint", sourceFingerprint);
		Files.write(targetDir.resolve(SKILL_META_FILE),
				obj.toString().getBytes(StandardCharsets.UTF_8));
	}

	private static void copyTree(Path source, Path target) throws IOException {
		try (Stream<Path> walk = Files.walk(source)) {
			walk.forEach(src -> {
				try {
					Path dest = target.resolve(source.relativize(src).toString());
					if (Files.isDirectory(src)) {
						Files.createDirectories(dest);
					} else {
						Files.createDirectories(dest.getParent());
						Files.copy(src, dest, StandardCopyOption.REPLACE_EXISTING);
					}
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});
		}
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
