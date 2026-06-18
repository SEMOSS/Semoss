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
package prerna.reactor.agent.tools;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Lists every skill discovered under the conventional skill-host directories in the working
 * directory, deduplicated by name with the same first-match-wins precedence as {@code LoadSkill}.
 *
 * <p>For each skill it reports the folder name, its discovered path, and a one-line description
 * pulled from YAML frontmatter ({@code description:}) when present, falling back to the first
 * non-blank line after the H1.
 *
 * <p>Output is a short markdown list intended to fit in a single tool-result turn. Use
 * {@code LoadSkill} (with optional {@code offset}/{@code max_bytes}) to read a specific skill's
 * body once you've identified it here.
 */
public class ListSkillReactor extends AbstractAgentToolReactor {

    private static final int DESCRIPTION_PREVIEW_BYTES = 4 * 1024;
    private static final int DESCRIPTION_MAX_CHARS     = 200;

    /** Same precedence as {@link LoadSkillReactor#SKILL_BASE_DIRS} - keep in sync. */
    private static final String[] SKILL_BASE_DIRS = {
            "",
            "client",
            "java",
            "py"
    };

    /** Same precedence as {@link LoadSkillReactor#SKILL_HOST_DIRS} - keep in sync. */
    private static final String[] SKILL_HOST_DIRS = {
            ".skills",
            ".agents/skills",
            ".agents/skill",
            ".claude/skills",
            ".claude/skill"
    };

    public ListSkillReactor() {
        this.keysToGet   = new String[] {};
        this.keyRequired = new int[]    {};
    }

    @Override
    protected NounMetadata doExecute() throws Exception {
        Map<String, SkillEntry> found = new LinkedHashMap<>();
        List<String> scannedDirs = new ArrayList<>();

        for (String baseDir : SKILL_BASE_DIRS) {
            for (String hostDir : SKILL_HOST_DIRS) {
                String hostPath = joinHostPath(baseDir, hostDir);
                scannedDirs.add(hostPath);

                File hostFile;
                try {
                    hostFile = resolveAndValidate(hostPath);
                } catch (IllegalArgumentException e) {
                    continue;
                }
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
                    File skillMd = new File(child, "SKILL.md");
                    if (!skillMd.isFile()) {
                        continue;
                    }
                    String relPath = toRelative(skillMd.getAbsolutePath());
                    String description = readDescription(skillMd);
                    found.put(name, new SkillEntry(name, relPath, description));
                }
            }
        }

        if (found.isEmpty()) {
            return new NounMetadata(
                    "No skills found.\n\nChecked:\n- " + String.join("\n- ", scannedDirs),
                    PixelDataType.CONST_STRING);
        }

        StringBuilder out = new StringBuilder();
        out.append("Found ").append(found.size()).append(" skill")
           .append(found.size() == 1 ? "" : "s").append(":\n\n");
        for (SkillEntry s : found.values()) {
            out.append("- **").append(s.name).append("** - `").append(s.path).append("`");
            if (s.description != null && !s.description.isEmpty()) {
                out.append("\n  ").append(s.description);
            }
            out.append('\n');
        }
        out.append("\nLoad a skill's body with `LoadSkill(skill_name=\"<name>\")`.");
        return new NounMetadata(out.toString(), PixelDataType.CONST_STRING);
    }

    @Override
    public String getReactorDescription() {
        return "Lists every skill discovered under <root|client|java|py>/(.skills|.agents/skills|"
             + ".agents/skill|.claude/skills|.claude/skill)/<name>/SKILL.md, deduplicated by name "
             + "(first-match-wins, matching LoadSkill precedence). Returns a markdown list of name, "
             + "path, and a one-line description. Use this to discover what's available before "
             + "calling LoadSkill.";
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
                try (java.io.InputStream in = Files.newInputStream(skillMd.toPath())) {
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

    private static String joinHostPath(String baseDir, String hostDir) {
        if (baseDir == null || baseDir.isEmpty()) return hostDir;
        return baseDir + "/" + hostDir;
    }

    private static final class SkillEntry {
        final String name;
        final String path;
        final String description;
        SkillEntry(String name, String path, String description) {
            this.name = name;
            this.path = path;
            this.description = description;
        }
    }
}
