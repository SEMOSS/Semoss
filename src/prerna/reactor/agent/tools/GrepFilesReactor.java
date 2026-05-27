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
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Searches file contents with a regular expression.
 *
 * <p>
 * Three output modes:
 * <ul>
 * <li>{@code files_with_matches} (default) — one file path per matching file
 * <li>{@code content} — matching lines in {@code file:line:content} format,
 * with optional
 * context lines via {@code after_context} / {@code before_context} /
 * {@code context}
 * <li>{@code count} — match count per file in {@code file:N} format
 * </ul>
 *
 * <p>
 * Use {@code glob} to restrict which file types are searched (e.g.
 * {@code *.java}).
 * Use {@code case_insensitive=true} for case-insensitive matching.
 * Use {@code head_limit} to cap result volume (default 200).
 *
 * <p>
 * <b>Note:</b> argument names are valid Pixel identifiers (no leading dashes)
 * because
 * MCPUtility constructs a Pixel call literal from these keys; dash-prefixed
 * names break
 * the Pixel parser. The names map 1:1 to grep flags: {@code after_context}=-A,
 * {@code before_context}=-B, {@code context}=-C, {@code case_insensitive}=-i.
 */
public class GrepFilesReactor extends AbstractAgentToolReactor {

    private static final int DEFAULT_HEAD_LIMIT = 200;

    public GrepFilesReactor() {
        this.keysToGet = new String[] { "pattern", "path", "glob", "output_mode",
                "after_context", "before_context", "context", "case_insensitive", "head_limit" };
        this.keyRequired = new int[] { 1, 0, 0, 0, 0, 0, 0, 0, 0 };
    }

    @Override
    protected NounMetadata doExecute() throws Exception {
        String patternStr = this.keyValue.get("pattern");
        String basePath = this.keyValue.get("path");
        String glob = this.keyValue.get("glob");
        String outputMode = this.keyValue.get("output_mode");
        String afterStr = this.keyValue.get("after_context");
        String beforeStr = this.keyValue.get("before_context");
        String contextStr = this.keyValue.get("context");
        String caseInsStr = this.keyValue.get("case_insensitive");
        String headLimitStr = this.keyValue.get("head_limit");

        if (outputMode == null || outputMode.trim().isEmpty())
            outputMode = "files_with_matches";
        int after = parseIntOr(afterStr, 0);
        int before = parseIntOr(beforeStr, 0);
        int ctx = parseIntOr(contextStr, 0);
        if (ctx > 0) {
            after = ctx;
            before = ctx;
        }
        int headLimit = parseIntOr(headLimitStr, DEFAULT_HEAD_LIMIT);
        if (headLimit <= 0)
            headLimit = DEFAULT_HEAD_LIMIT;
        boolean caseInsensitive = "true".equalsIgnoreCase(caseInsStr);

        int flags = caseInsensitive ? Pattern.CASE_INSENSITIVE : 0;
        Pattern regex;
        try {
            regex = Pattern.compile(patternStr, flags);
        } catch (PatternSyntaxException e) {
            return new NounMetadata("Error: invalid regex pattern: " + e.getMessage(), PixelDataType.CONST_STRING);
        }

        File baseDir = resolveAndValidate(basePath);
        if (!baseDir.exists()) {
            return new NounMetadata(
                    "Error: path not found: " + (basePath != null ? basePath : "."),
                    PixelDataType.CONST_STRING);
        }

        PathMatcher fileMatcher = (glob != null && !glob.trim().isEmpty())
                ? FileSystems.getDefault().getPathMatcher("glob:" + glob.trim())
                : null;

        boolean isFilesMode = "files_with_matches".equals(outputMode);
        boolean isCountMode = "count".equals(outputMode);

        List<String> results = new ArrayList<>();

        List<Path> paths = new ArrayList<>();
        Files.walk(baseDir.toPath())
                .filter(p -> !Files.isDirectory(p))
                .filter(p -> fileMatcher == null
                        || fileMatcher.matches(p.getFileName())
                        || fileMatcher.matches(baseDir.toPath().relativize(p))
                        || fileMatcher.matches(p))
                .sorted()
                .forEach(paths::add);

        for (Path p : paths) {
            if (results.size() >= headLimit)
                break;
            try {
                String content = new String(Files.readAllBytes(p), StandardCharsets.UTF_8);
                String[] lines = content.split("\n", -1);
                String relPath = toRelative(p.toAbsolutePath().toString());

                if (isFilesMode) {
                    for (String line : lines) {
                        if (regex.matcher(line).find()) {
                            results.add(relPath);
                            break;
                        }
                    }
                } else if (isCountMode) {
                    long count = 0;
                    for (String line : lines) {
                        if (regex.matcher(line).find())
                            count++;
                    }
                    if (count > 0)
                        results.add(relPath + ":" + count);
                } else {
                    // content mode — matching lines with optional context
                    Set<Integer> printed = new HashSet<>();
                    for (int i = 0; i < lines.length && results.size() < headLimit; i++) {
                        if (regex.matcher(lines[i]).find()) {
                            int startLine = Math.max(0, i - before);
                            int endLine = Math.min(lines.length - 1, i + after);
                            // separator between non-contiguous blocks
                            if (!printed.isEmpty() && !printed.contains(startLine - 1)) {
                                results.add("--");
                            }
                            for (int j = startLine; j <= endLine && results.size() < headLimit; j++) {
                                if (!printed.contains(j)) {
                                    String sep = (j == i) ? ":" : "-";
                                    results.add(relPath + ":" + (j + 1) + sep + lines[j]);
                                    printed.add(j);
                                }
                            }
                        }
                    }
                }
            } catch (Exception ignored) {
                // skip unreadable files
            }
        }

        if (results.isEmpty()) {
            return new NounMetadata("No matches found for: " + patternStr, PixelDataType.CONST_STRING);
        }
        return new NounMetadata(String.join("\n", results), PixelDataType.CONST_STRING);
    }

    private int parseIntOr(String s, int defaultVal) {
        if (s == null || s.trim().isEmpty())
            return defaultVal;
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException e) {
            return defaultVal;
        }
    }

    @Override
    protected String getDescriptionForKey(String key) {
        switch (key) {
            case "pattern":          return "Java regular expression to search for.";
            case "path":             return "Relative directory or file path to search. Defaults to working directory root.";
            case "glob":             return "Glob pattern to restrict which files are searched (e.g. *.java, **/*.ts).";
            case "output_mode":      return "Output format: files_with_matches (default), content (file:line:text), or count (file:N).";
            case "after_context":    return "Lines to include after each matching line (like grep -A).";
            case "before_context":   return "Lines to include before each matching line (like grep -B).";
            case "context":          return "Lines to include both before and after each match (like grep -C). Overrides after_context/before_context.";
            case "case_insensitive": return "If true, match case-insensitively (like grep -i). Defaults to false.";
            case "head_limit":       return "Maximum number of result lines to return. Defaults to 200.";
            default:                 return super.getDescriptionForKey(key);
        }
    }

    @Override
    protected MCP_KEY_TYPE getKeyTypeForMCP(String key) {
        switch (key) {
            case "case_insensitive": return MCP_KEY_TYPE.BOOLEAN;
            case "after_context":
            case "before_context":
            case "context":
            case "head_limit":       return MCP_KEY_TYPE.INTEGER;
            default:                 return super.getKeyTypeForMCP(key);
        }
    }

    @Override
    public String getReactorDescription() {
        return "Searches file contents with a regex. Output modes: files_with_matches (default), content, count. "
                + "Supports after_context/before_context/context lines (grep -A/-B/-C), glob file filtering, "
                + "case_insensitive (grep -i), head_limit result cap.";
    }
}
