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

import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Scans a working directory for Anthropic-style skills and returns a deduplicated
 * catalog of {@code {name, path, directory, description}}.
 *
 * <p>This is the single reusable home for the discovery logic that used to live inside
 * the {@code Agent_Tools} {@code ListSkill} reactor. Under each base folder (root,
 * {@code client/}, {@code java/}, {@code py/}) it checks {@code .skills/},
 * {@code .agents/skills/}, {@code .agents/skill/}, {@code .claude/skills/}, and
 * {@code .claude/skill/} for a {@code <name>/SKILL.md}. Skills are deduplicated by folder
 * name with the same first-match-wins precedence as the {@code LoadSkill} tool.
 *
 * <p>Two consumers render the same catalog differently: {@code ListSkillReactor} emits a
 * markdown tool result, while {@link prerna.reactor.agent.runtime.SemossAgentHarness}
 * emits an {@code <available_skills>} system-prompt block. Registry skills are materialized
 * into the working directory by {@link SkillStager} before this scan runs, so both paths
 * agree on what is available.
 *
 * <p>Path handling mirrors {@code AbstractAgentToolReactor}: paths are canonicalized with
 * forward slashes and reported relative to the working directory.
 */
public final class SkillScanner {

	private static final int DESCRIPTION_PREVIEW_BYTES = 4 * 1024;
	private static final int DESCRIPTION_MAX_CHARS     = 200;

	/** Base folders searched for skills, in priority order - first match wins. */
	public static final String[] SKILL_BASE_DIRS = {
			"",
			"client",
			"java",
			"py"
	};

	/**
	 * Skill host folders checked under each base folder. Plural paths come first to match
	 * existing rooms; singular aliases follow for users who naturally create
	 * {@code .agents/skill} or {@code .claude/skill}.
	 */
	public static final String[] SKILL_HOST_DIRS = {
			".skills",
			".agents/skills",
			".agents/skill",
			".claude/skills",
			".claude/skill"
	};

	private SkillScanner() {}

	/**
	 * Discovers every skill under the conventional host directories of {@code workingDir},
	 * deduplicated by name (first-match-wins). Returns an empty list when {@code workingDir}
	 * is blank or no skills are present. Never throws - unreadable entries are skipped.
	 *
	 * @param workingDir the agent's working directory (the same path {@link SkillStager}
	 *                   stages into)
	 */
	public static List<DiscoveredSkill> scan(String workingDir) {
		return scan(workingDir, false);
	}

	/**
	 * Variant of {@link #scan(String)} that optionally also reads each skill's body content
	 * (everything after the YAML frontmatter; the whole file when there is no frontmatter) into
	 * {@link DiscoveredSkill#getContent()}. When {@code includeContent} is {@code false} no
	 * full-file read is performed and the content is left {@code null}.
	 *
	 * @param workingDir     the agent's working directory (the same path {@link SkillStager} stages into)
	 * @param includeContent whether to also read each skill's body content
	 */
	public static List<DiscoveredSkill> scan(String workingDir, boolean includeContent) {
		return scan(workingDir, includeContent, false);
	}

	/**
	 * Variant of {@link #scan(String, boolean)} that, when {@code includeAll} is {@code true}, also
	 * crawls every other file under each skill's directory into {@link DiscoveredSkill#getFiles()}
	 * (each file's path/directory relative to the working directory, with full content).
	 * {@code includeAll} implies content: each {@link DiscoveredSkill#getContent()} is populated
	 * regardless of {@code includeContent}.
	 *
	 * @param workingDir     the agent's working directory
	 * @param includeContent whether to read each skill's SKILL.md body
	 * @param includeAll     whether to also crawl the rest of each skill folder
	 */
	public static List<DiscoveredSkill> scan(String workingDir, boolean includeContent, boolean includeAll) {
		List<DiscoveredSkill> result = new ArrayList<>();
		if (workingDir == null || workingDir.trim().isEmpty()) {
			return result;
		}
		String root = normalizePath(workingDir);

		Map<String, DiscoveredSkill> found = new LinkedHashMap<>();
		for (String baseDir : SKILL_BASE_DIRS) {
			for (String hostDir : SKILL_HOST_DIRS) {
				File hostFile = new File(root, joinHostPath(baseDir, hostDir));
				if (!hostFile.isDirectory()) {
					continue;
				}
				File[] children = hostFile.listFiles(File::isDirectory);
				if (children == null) {
					continue;
				}
				for (File child : children) {
					String name = child.getName();
					if (found.containsKey(name)) {
						continue;
					}
					File skillMd = new File(child, Skill.SKILL_FILE);
					if (!skillMd.isFile()) {
						continue;
					}
					String relPath = toRelative(root, skillMd.getAbsolutePath());
					String relDir  = toRelative(root, child.getAbsolutePath());
					String description = readDescription(skillMd);
					boolean readContent = includeContent || includeAll;
					String content = readContent ? readBody(skillMd) : null;
					List<SkillFile> files = includeAll ? crawlFiles(child, root) : null;
					found.put(name, new DiscoveredSkill(name, relPath, relDir, description, content, files));
				}
			}
		}
		result.addAll(found.values());
		return result;
	}

	/**
	 * The relative host paths {@link #scan} probes, in scan order (every base x host
	 * combination). Useful for "no skills found, checked: ..." diagnostics.
	 */
	public static List<String> candidateHostPaths() {
		List<String> paths = new ArrayList<>(SKILL_BASE_DIRS.length * SKILL_HOST_DIRS.length);
		for (String baseDir : SKILL_BASE_DIRS) {
			for (String hostDir : SKILL_HOST_DIRS) {
				paths.add(joinHostPath(baseDir, hostDir));
			}
		}
		return paths;
	}

	// ---- path helpers (mirror AbstractAgentToolReactor semantics) ----

	/** Normalizes a filesystem path: canonical, forward slashes, no trailing slash. */
	private static String normalizePath(String path) {
		if (path == null) return "";
		try {
			String normalized = new File(path).getCanonicalPath().replace("\\", "/");
			if (normalized.endsWith("/") && normalized.length() > 1) {
				normalized = normalized.substring(0, normalized.length() - 1);
			}
			return normalized;
		} catch (Exception e) {
			return path.replace("\\", "/");
		}
	}

	/** Returns {@code absolutePath} relative to {@code root}, or the normalized absolute path if outside. */
	private static String toRelative(String root, String absolutePath) {
		String normalized = normalizePath(absolutePath);
		if (normalized.startsWith(root + "/")) {
			return normalized.substring(root.length() + 1);
		}
		if (normalized.equals(root)) {
			return ".";
		}
		return normalized;
	}

	private static String joinHostPath(String baseDir, String hostDir) {
		if (baseDir == null || baseDir.isEmpty()) return hostDir;
		return baseDir + "/" + hostDir;
	}

	/** Reads the head of SKILL.md and pulls a one-liner from frontmatter or the first body line. */
	private static String readDescription(File skillMd) {
		try {
			byte[] bytes;
			long size = Files.size(skillMd.toPath());
			if (size <= DESCRIPTION_PREVIEW_BYTES) {
				bytes = Files.readAllBytes(skillMd.toPath());
			} else {
				bytes = new byte[DESCRIPTION_PREVIEW_BYTES];
				try (InputStream in = Files.newInputStream(skillMd.toPath())) {
					int read = in.read(bytes);
					if (read < bytes.length) {
						byte[] trimmed = new byte[Math.max(0, read)];
						System.arraycopy(bytes, 0, trimmed, 0, trimmed.length);
						bytes = trimmed;
					}
				}
			}
			String head = new String(bytes, StandardCharsets.UTF_8);
			String[] lines = head.split("\\r?\\n", -1);

			// 1. YAML frontmatter description:
			if (lines.length > 0 && "---".equals(lines[0].trim())) {
				for (int i = 1; i < lines.length; i++) {
					String line = lines[i];
					if ("---".equals(line.trim())) {
						break;
					}
					String trimmed = line.trim();
					if (trimmed.toLowerCase().startsWith("description:")) {
						return clip(trimmed.substring("description:".length()).trim());
					}
				}
			}

			// 2. First non-blank, non-frontmatter, non-H1 line.
			boolean inFrontmatter = lines.length > 0 && "---".equals(lines[0].trim());
			boolean closedFrontmatter = !inFrontmatter;
			boolean sawH1 = false;
			for (int i = inFrontmatter ? 1 : 0; i < lines.length; i++) {
				String trimmed = lines[i].trim();
				if (!closedFrontmatter) {
					if ("---".equals(trimmed)) closedFrontmatter = true;
					continue;
				}
				if (trimmed.isEmpty()) continue;
				if (trimmed.startsWith("#")) {
					if (!sawH1 && trimmed.startsWith("# ")) {
						sawH1 = true;
						continue;
					}
					continue;
				}
				return clip(trimmed);
			}
			return "";
		} catch (Exception e) {
			return "";
		}
	}

	private static String clip(String s) {
		if (s == null) return "";
		if (s.length() <= DESCRIPTION_MAX_CHARS) return s;
		return s.substring(0, DESCRIPTION_MAX_CHARS - 1) + "...";
	}

	/**
	 * Reads the full body of SKILL.md - everything after the closing {@code ---} of the YAML
	 * frontmatter, or the entire file when there is no frontmatter. Leading blank lines after the
	 * frontmatter are dropped and trailing whitespace trimmed. Returns {@code ""} on read failure
	 * or when the frontmatter is opened but never closed.
	 */
	private static String readBody(File skillMd) {
		try {
			String text = new String(Files.readAllBytes(skillMd.toPath()), StandardCharsets.UTF_8);
			String[] lines = text.split("\\r?\\n", -1);

			int bodyStart = 0;
			if (lines.length > 0 && "---".equals(lines[0].trim())) {
				int close = -1;
				for (int i = 1; i < lines.length; i++) {
					if ("---".equals(lines[i].trim())) {
						close = i;
						break;
					}
				}
				if (close < 0) {
					return "";
				}
				bodyStart = close + 1;
			}

			while (bodyStart < lines.length && lines[bodyStart].trim().isEmpty()) {
				bodyStart++;
			}

			StringBuilder body = new StringBuilder();
			for (int i = bodyStart; i < lines.length; i++) {
				if (i > bodyStart) {
					body.append('\n');
				}
				body.append(lines[i]);
			}
			return body.toString().stripTrailing();
		} catch (Exception e) {
			return "";
		}
	}

	/**
	 * Crawls every file under {@code skillDir} (recursively, excluding the top-level {@code SKILL.md},
	 * which is already exposed via {@link DiscoveredSkill#getContent()}) into a list of
	 * {@link SkillFile}s. Each file's {@link SkillFile#getPath()} and {@link SkillFile#getDirectory()}
	 * are relative to the working directory {@code root} (forward slashes), mirroring the top-level
	 * skill fields; content is the full UTF-8 file. Directories are not emitted on their own, so a
	 * genuinely empty directory is not represented. Best-effort; symlinks are not followed.
	 */
	private static List<SkillFile> crawlFiles(File skillDir, String root) {
		List<SkillFile> files = new ArrayList<>();
		Path skillRoot = skillDir.toPath();
		try (Stream<Path> walk = Files.walk(skillRoot)) {
			walk.sorted().forEach(p -> {
				if (p.equals(skillRoot) || Files.isDirectory(p)) {
					return;
				}
				// skip the main SKILL.md at the skill-folder root - it is already in getContent()
				if (Skill.SKILL_FILE.equals(p.getFileName().toString())
						&& skillRoot.equals(p.getParent())) {
					return;
				}
				File f = p.toFile();
				String filePath = toRelative(root, f.getAbsolutePath());
				String fileDir  = toRelative(root, f.getParentFile().getAbsolutePath());
				files.add(new SkillFile(filePath, fileDir, readFile(f)));
			});
		} catch (Exception e) {
			// best-effort; return whatever was gathered
		}
		return files;
	}

	/** Reads a file's full content as UTF-8. Returns {@code ""} on failure. */
	private static String readFile(File file) {
		try {
			return new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
		} catch (Exception e) {
			return "";
		}
	}

	/**
	 * A skill discovered on disk. {@link #getPath()} is the working-dir-relative path to the
	 * skill's {@code SKILL.md}; {@link #getDirectory()} is the relative path to its containing
	 * folder (e.g. {@code .claude/skills/pdf}).
	 */
	public static final class DiscoveredSkill {
		private final String name;
		private final String path;
		private final String directory;
		private final String description;
		private final String content;
		private final List<SkillFile> files;

		DiscoveredSkill(String name, String path, String directory, String description) {
			this(name, path, directory, description, null, null);
		}

		DiscoveredSkill(String name, String path, String directory, String description, String content) {
			this(name, path, directory, description, content, null);
		}

		DiscoveredSkill(String name, String path, String directory, String description, String content,
				List<SkillFile> files) {
			this.name = name;
			this.path = path;
			this.directory = directory;
			this.description = description;
			this.content = content;
			this.files = files;
		}

		public String getName()        { return name; }
		public String getPath()        { return path; }
		public String getDirectory()   { return directory; }
		public String getDescription() { return description; }
		/** Body content - everything after the frontmatter; {@code null} when not requested. */
		public String getContent()     { return content; }
		/** Other files under the skill directory; {@code null} when not requested (includeAll). */
		public List<SkillFile> getFiles() { return files; }
	}

	/**
	 * A file discovered under a skill directory by {@code includeAll}. {@link #getPath()} and
	 * {@link #getDirectory()} are relative to the working directory (forward slashes), mirroring the
	 * top-level skill fields.
	 */
	public static final class SkillFile {
		private final String path;
		private final String directory;
		private final String content;

		SkillFile(String path, String directory, String content) {
			this.path = path;
			this.directory = directory;
			this.content = content;
		}

		public String getPath()      { return path; }
		public String getDirectory() { return directory; }
		public String getContent()   { return content; }
	}
}
