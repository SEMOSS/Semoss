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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.JSONArray;
import org.json.JSONObject;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Applies a sequence of exact string replacements to a single file in one atomic operation.
 *
 * <p>Each edit is applied to the result of the previous edit, in order. The whole operation is
 * all-or-nothing: every edit is validated and applied to an in-memory copy first, and the file is
 * written only if all edits succeed. If any {@code old_string} is missing or non-unique (and
 * {@code replace_all} is not set), nothing is written.
 *
 * <p>Edits are passed as {@code edits_json} - a JSON array string - because the MCP-to-Pixel call
 * bridge passes scalar named arguments, not arrays of objects (same reason {@code TodoWrite} uses
 * {@code items_json}). Each element: {@code { old_string, new_string, replace_all? }}.
 */
public class MultiEditReactor extends AbstractAgentToolReactor {

    private static final int MAX_EDITS = 200;

    public MultiEditReactor() {
        this.keysToGet  = new String[]{"file_path", "edits_json"};
        this.keyRequired = new int[]{1, 1};
    }

    @Override
    protected NounMetadata doExecute() throws Exception {
        String filePath = this.keyValue.get("file_path");
        String editsJson = this.keyValue.get("edits_json");

        if (editsJson == null || editsJson.trim().isEmpty()) {
            return err("edits_json is required");
        }

        JSONArray edits;
        try {
            edits = new JSONArray(editsJson.trim());
        } catch (Exception e) {
            return err("edits_json must be a valid JSON array - " + e.getMessage());
        }
        if (edits.length() == 0) {
            return err("edits_json must contain at least one edit");
        }
        if (edits.length() > MAX_EDITS) {
            return err("too many edits (" + edits.length() + " > " + MAX_EDITS + ")");
        }

        File file = resolveAndValidate(filePath);
        if (!file.exists() || !file.isFile()) {
            return new NounMetadata("Error: file not found: " + filePath, PixelDataType.CONST_STRING);
        }

        String content = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);

        // apply every edit to an in-memory copy first - atomic, nothing written until all pass
        String working = content;
        int totalReplacements = 0;
        for (int i = 0; i < edits.length(); i++) {
            JSONObject edit;
            try {
                edit = edits.getJSONObject(i);
            } catch (Exception e) {
                return err("edits[" + i + "] is not an object");
            }

            String oldString = optString(edit, "old_string");
            String newString = optString(edit, "new_string");
            boolean replaceAll = edit.optBoolean("replace_all", false);

            if (oldString == null || oldString.isEmpty()) {
                return err("edits[" + i + "].old_string is required and must not be empty");
            }
            if (newString == null) newString = "";
            if (oldString.equals(newString)) {
                return err("edits[" + i + "].old_string and new_string are identical");
            }

            int occurrences = countOccurrences(working, oldString);
            if (occurrences == 0) {
                return err("edits[" + i + "].old_string not found"
                        + (i > 0 ? " (note: edits apply in order; an earlier edit may have changed this text)" : ""));
            }
            if (!replaceAll && occurrences > 1) {
                return err("edits[" + i + "].old_string appears " + occurrences + " times. "
                        + "Provide more surrounding context to make it unique, or set replace_all=true.");
            }

            if (replaceAll) {
                working = working.replace(oldString, newString);
            } else {
                working = working.replaceFirst(Pattern.quote(oldString), Matcher.quoteReplacement(newString));
            }
            totalReplacements += occurrences;
        }

        saveTextFileWithInsightAssetsBase64(file, working);
        return new NounMetadata(
                "Applied " + edits.length() + " edit(s) (" + totalReplacements + " replacement(s)) to: "
                        + toRelative(file.getAbsolutePath()),
                PixelDataType.CONST_STRING);
    }

    private String optString(JSONObject obj, String key) {
        if (!obj.has(key) || obj.isNull(key)) return null;
        Object v = obj.get(key);
        return v == null ? null : v.toString();
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

    private NounMetadata err(String msg) {
        return new NounMetadata("Error: " + msg, PixelDataType.CONST_STRING);
    }

    @Override
    public String getReactorDescription() {
        return "Applies multiple exact string replacements to a single file in one atomic operation. "
             + "Edits apply sequentially (each to the result of the previous). All-or-nothing: if any "
             + "old_string is missing or non-unique (without replace_all), nothing is written. Prefer "
             + "this over multiple EditFile calls when changing several spots in the same file.";
    }
}
