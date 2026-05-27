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
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Applies a sequence of string replacements to a single file in one atomic
 * operation.
 *
 * <p>
 * Each edit is applied in order against the running content (so later edits see
 * the effects of
 * earlier ones). If any edit's {@code old_string} is missing or ambiguous, the
 * whole operation
 * aborts and the file is left untouched.
 *
 * <p>
 * The {@code edits} argument is a list of maps with keys {@code old_string},
 * {@code new_string}, and an optional boolean {@code replace_all} (defaults to
 * {@code false}).
 * Mirrors Claude Code's MultiEdit and Codex's apply_patch semantics for batched
 * in-file edits.
 */
public class MultiEditReactor extends AbstractAgentToolReactor {

    public MultiEditReactor() {
        this.keysToGet = new String[] { "file_path", "edits" };
        this.keyRequired = new int[] { 1, 1 };
    }

    @Override
    protected NounMetadata doExecute() throws Exception {
        String filePath = this.keyValue.get("file_path");

        if (filePath == null || filePath.trim().isEmpty()) {
            return new NounMetadata("Error: file_path is required", PixelDataType.CONST_STRING);
        }

        @SuppressWarnings("rawtypes")
        List rawEdits = getList("edits");
        if (rawEdits == null || rawEdits.isEmpty()) {
            return new NounMetadata(
                    "Error: edits is required (list of {old_string, new_string, replace_all?} objects)",
                    PixelDataType.CONST_STRING);
        }

        File file = resolveAndValidate(filePath);
        if (!file.exists() || !file.isFile()) {
            return new NounMetadata("Error: file not found: " + filePath, PixelDataType.CONST_STRING);
        }

        String content = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);

        int totalReplacements = 0;
        for (int i = 0; i < rawEdits.size(); i++) {
            Object item = rawEdits.get(i);
            if (!(item instanceof Map)) {
                return new NounMetadata(
                        "Error: edits[" + i + "] is not an object",
                        PixelDataType.CONST_STRING);
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> edit = (Map<String, Object>) item;
            Object oldObj = edit.get("old_string");
            Object newObj = edit.get("new_string");
            Object allObj = edit.get("replace_all");

            String oldStr = oldObj == null ? null : String.valueOf(oldObj);
            String newStr = newObj == null ? "" : String.valueOf(newObj);
            boolean replaceAll = allObj != null && "true".equalsIgnoreCase(String.valueOf(allObj));

            if (oldStr == null || oldStr.isEmpty()) {
                return new NounMetadata(
                        "Error: edits[" + i + "].old_string must not be empty",
                        PixelDataType.CONST_STRING);
            }

            int occurrences = countOccurrences(content, oldStr);
            if (occurrences == 0) {
                return new NounMetadata(
                        "Error: edits[" + i + "].old_string not found in file",
                        PixelDataType.CONST_STRING);
            }
            if (!replaceAll && occurrences > 1) {
                return new NounMetadata(
                        "Error: edits[" + i + "].old_string appears " + occurrences
                                + " times. Provide more surrounding context or set replace_all=true.",
                        PixelDataType.CONST_STRING);
            }

            if (replaceAll) {
                content = content.replace(oldStr, newStr);
                totalReplacements += occurrences;
            } else {
                content = content.replaceFirst(
                        Pattern.quote(oldStr),
                        Matcher.quoteReplacement(newStr));
                totalReplacements += 1;
            }
        }

        saveTextFileWithInsightAssetsBase64(file, content);
        return new NounMetadata(
                "Applied " + rawEdits.size() + " edit(s), " + totalReplacements
                        + " replacement(s) in: " + toRelative(file.getAbsolutePath()),
                PixelDataType.CONST_STRING);
    }

    private int countOccurrences(String text, String target) {
        int count = 0;
        int idx = 0;
        while ((idx = text.indexOf(target, idx)) != -1) {
            count++;
            idx += target.length();
        }
        return count;
    }

    @Override
    protected String getDescriptionForKey(String key) {
        switch (key) {
            case "file_path": return "Relative path of the file to edit (within the working directory).";
            case "edits":     return "Ordered list of {old_string, new_string, replace_all?} objects. "
                                   + "Applied sequentially; if any edit fails the whole operation aborts.";
            default:          return super.getDescriptionForKey(key);
        }
    }

    @Override
    public String getReactorDescription() {
        return "Applies a sequence of exact-string edits to a single file atomically. "
                + "Each edit is {old_string, new_string, replace_all?}. Aborts and leaves the file "
                + "untouched if any edit fails to match or is non-unique without replace_all.";
    }

    @Override
    protected MCP_KEY_TYPE getKeyTypeForMCP(String key) {
        if ("edits".equals(key)) {
            return MCP_KEY_TYPE.ARRAY;
        }
        return super.getKeyTypeForMCP(key);
    }
}
