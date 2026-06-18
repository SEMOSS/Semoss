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
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Loads (a chunk of) a named skill from {@code <skill_dir>/<skill_name>/SKILL.md} relative to the
 * working directory.
 *
 * <p>Searches conventional skill-host directories in priority order - first match wins. The root
 * workspace is searched first, then each major implementation folder:
 * <ol>
 *   <li>workspace root</li>
 *   <li>{@code client/}</li>
 *   <li>{@code java/}</li>
 *   <li>{@code py/}</li>
 * </ol>
 * Within each base folder it checks {@code .skills/}, {@code .agents/skills/},
 * {@code .agents/skill/}, {@code .claude/skills/}, and {@code .claude/skill/}.
 *
 * <p><b>Progressive disclosure.</b> Returns at most {@code max_bytes} bytes (default
 * {@value #DEFAULT_MAX_BYTES}) starting at {@code offset} (default 0). When more content remains a
 * footer tells the caller the next offset to request - a model can peek at the head of a skill,
 * decide whether to keep going, and continue paging without ever loading the whole file in one
 * turn. To force a full read, pass {@code max_bytes} larger than the file size (the hard ceiling
 * is {@value #HARD_MAX_BYTES} bytes).
 *
 * <p>Skills are markdown packages (a folder per skill, with a {@code SKILL.md} body and optional
 * helper files like {@code examples/}, {@code scripts/}, {@code reference.md}). This reactor only
 * returns the {@code SKILL.md} body - the model fetches any referenced helper files via
 * {@code ReadFile}.
 *
 * <p>{@code skill_name} is constrained to a single folder name (no slashes, no {@code ..});
 * access outside the working directory is blocked by
 * {@link AbstractAgentToolReactor#resolveAndValidate}.
 */
public class LoadSkillReactor extends AbstractAgentToolReactor {

    /** Default chunk size returned when {@code max_bytes} is unspecified - fits most skills whole. */
    static final int DEFAULT_MAX_BYTES = 8 * 1024;

    /** Hard ceiling on bytes returned in a single call regardless of {@code max_bytes}. */
    static final int HARD_MAX_BYTES = 200 * 1024;

    /** Base folders searched for skills, in priority order - first match wins. */
    static final String[] SKILL_BASE_DIRS = {
            "",
            "client",
            "java",
            "py"
    };

    /**
     * Skill host folders checked under each base folder. Plural paths come first to match existing
     * rooms; singular aliases follow for users who naturally create {@code .agents/skill} or
     * {@code .claude/skill}.
     */
    static final String[] SKILL_HOST_DIRS = {
            ".skills",
            ".agents/skills",
            ".agents/skill",
            ".claude/skills",
            ".claude/skill"
    };

    public LoadSkillReactor() {
        this.keysToGet   = new String[] { "skill_name", "offset", "max_bytes" };
        this.keyRequired = new int[]    { 1,            0,        0           };
    }

    @Override
    protected NounMetadata doExecute() throws Exception {
        String raw = this.keyValue.get("skill_name");
        if (raw == null || raw.trim().isEmpty()) {
            return new NounMetadata("Error: skill_name is required", PixelDataType.CONST_STRING);
        }
        String name = raw.trim();
        if (name.contains("/") || name.contains("\\") || name.contains("..")) {
            return new NounMetadata(
                    "Error: invalid skill_name (must be a single folder name with no slashes or '..'): " + name,
                    PixelDataType.CONST_STRING);
        }

        long offset = parseLongAtLeast(this.keyValue.get("offset"), 0L, 0L);
        int maxBytes = parseIntAtLeast(this.keyValue.get("max_bytes"), DEFAULT_MAX_BYTES, 1);
        if (maxBytes > HARD_MAX_BYTES) {
            maxBytes = HARD_MAX_BYTES;
        }

        File skillFile = null;
        List<String> attempted = new ArrayList<>();
        for (String baseDir : SKILL_BASE_DIRS) {
            for (String hostDir : SKILL_HOST_DIRS) {
                String candidatePath = joinSkillPath(baseDir, hostDir, name);
                attempted.add(candidatePath);
                File candidate = resolveAndValidate(candidatePath);
                if (candidate.isFile()) {
                    skillFile = candidate;
                    break;
                }
            }
            if (skillFile != null) {
                break;
            }
        }
        if (skillFile == null) {
            return new NounMetadata(
                    "Error: skill not found: " + name + " (checked: " + String.join(", ", attempted) + ")",
                    PixelDataType.CONST_STRING);
        }

        long size = Files.size(skillFile.toPath());
        if (offset >= size) {
            return new NounMetadata(
                    "[empty: offset " + offset + " is at or past end-of-file (" + size + " bytes total)]",
                    PixelDataType.CONST_STRING);
        }

        long remaining = size - offset;
        int toRead = (int) Math.min(remaining, (long) maxBytes);

        byte[] bytes = new byte[toRead];
        try (RandomAccessFile raf = new RandomAccessFile(skillFile, "r")) {
            raf.seek(offset);
            raf.readFully(bytes);
        }

        // If we're not at EOF, prefer to end the chunk at the last newline so markdown stays
        // line-aligned and we don't split mid-codepoint. Only back off when there's a newline
        // reasonably late in the chunk; otherwise return the raw byte slice.
        long endOffset = offset + toRead;
        if (endOffset < size) {
            int lastNewline = -1;
            for (int i = bytes.length - 1; i >= 0; i--) {
                if (bytes[i] == (byte) '\n') {
                    lastNewline = i;
                    break;
                }
            }
            // Only trim when keeping at least 50% of the chunk - otherwise the last line of a
            // long chunk would force a tiny return.
            if (lastNewline >= 0 && lastNewline >= bytes.length / 2) {
                int newLen = lastNewline + 1;
                byte[] trimmed = new byte[newLen];
                System.arraycopy(bytes, 0, trimmed, 0, newLen);
                bytes = trimmed;
                endOffset = offset + newLen;
            }
        }

        StringBuilder body = new StringBuilder(new String(bytes, StandardCharsets.UTF_8));
        long bytesRemaining = size - endOffset;
        if (bytesRemaining > 0) {
            body.append("\n\n[--- skill continues: bytes ").append(offset).append('-').append(endOffset - 1)
                .append(" of ").append(size).append("; ").append(bytesRemaining)
                .append(" bytes remaining. To read more call LoadSkill(skill_name=\"").append(name)
                .append("\", offset=").append(endOffset).append("). ---]");
        } else if (offset > 0) {
            body.append("\n\n[--- end of skill: bytes ").append(offset).append('-').append(endOffset - 1)
                .append(" of ").append(size).append(" (final chunk). ---]");
        }
        return new NounMetadata(body.toString(), PixelDataType.CONST_STRING);
    }

    @Override
    public String getReactorDescription() {
        return "Loads a chunk of a named skill from <skill_dir>/<skill_name>/SKILL.md in the working "
             + "directory. Searches root, client/, java/, then py/. Under each folder it checks "
             + ".skills/, .agents/skills/, .agents/skill/, .claude/skills/, and .claude/skill/; "
             + "first match wins. Returns up to max_bytes (default " + DEFAULT_MAX_BYTES + ") bytes "
             + "starting at offset (default 0); when more content remains, the response footer "
             + "names the next offset to request. Pass max_bytes larger than the file (ceiling "
             + HARD_MAX_BYTES + ") to force a full read. Helper files inside the skill folder are "
             + "read via ReadFile.";
    }

    private static String joinSkillPath(String baseDir, String hostDir, String skillName) {
        String suffix = hostDir + "/" + skillName + "/SKILL.md";
        if (baseDir == null || baseDir.isEmpty()) {
            return suffix;
        }
        return baseDir + "/" + suffix;
    }

    private static long parseLongAtLeast(String value, long defaultValue, long minInclusive) {
        if (value == null || value.trim().isEmpty()) return defaultValue;
        try {
            long parsed = Long.parseLong(value.trim());
            return parsed >= minInclusive ? parsed : defaultValue;
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    private static int parseIntAtLeast(String value, int defaultValue, int minInclusive) {
        if (value == null || value.trim().isEmpty()) return defaultValue;
        try {
            int parsed = Integer.parseInt(value.trim());
            return parsed >= minInclusive ? parsed : defaultValue;
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }
}
