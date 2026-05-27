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
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Performs an exact string replacement in a file.
 *
 * <p>Fails safely when {@code old_string} is not found or is not unique (to prevent accidental
 * multi-site edits). Set {@code replace_all=true} to explicitly replace every occurrence.
 * Always read the file first to verify the exact string before editing.
 */
public class EditFileReactor extends AbstractAgentToolReactor {

    public EditFileReactor() {
        this.keysToGet = new String[]{"file_path", "old_string", "new_string", "replace_all"};
        this.keyRequired = new int[]{1, 1, 1, 0};
    }

    @Override
    protected NounMetadata doExecute() throws Exception {
        String filePath     = this.keyValue.get("file_path");
        String oldString    = this.keyValue.get("old_string");
        String newString    = this.keyValue.get("new_string");
        String replaceAllStr = this.keyValue.get("replace_all");
        boolean replaceAll  = "true".equalsIgnoreCase(replaceAllStr);

        if (oldString == null || oldString.isEmpty()) {
            return new NounMetadata("Error: old_string must not be empty", PixelDataType.CONST_STRING);
        }
        if (newString == null) newString = "";

        File file = resolveAndValidate(filePath);
        if (!file.exists() || !file.isFile()) {
            return new NounMetadata("Error: file not found: " + filePath, PixelDataType.CONST_STRING);
        }

        String content = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);

        int occurrences = countOccurrences(content, oldString);
        if (occurrences == 0) {
            return new NounMetadata(
                    "Error: old_string not found in file: " + filePath,
                    PixelDataType.CONST_STRING);
        }
        if (!replaceAll && occurrences > 1) {
            return new NounMetadata(
                    "Error: old_string appears " + occurrences + " times in " + filePath
                    + ". Provide more surrounding context to make it unique, or use replace_all=true.",
                    PixelDataType.CONST_STRING);
        }

        String updated;
        if (replaceAll) {
            updated = content.replace(oldString, newString);
        } else {
            // Replace exactly the first (and only) occurrence
            updated = content.replaceFirst(
                    Pattern.quote(oldString),
                    Matcher.quoteReplacement(newString));
        }

        saveTextFileWithInsightAssetsBase64(file, updated);
        return new NounMetadata(
                "Replaced " + occurrences + " occurrence(s) in: " + toRelative(file.getAbsolutePath()),
                PixelDataType.CONST_STRING);
    }

    private int countOccurrences(String text, String target) {
        int count = 0;
        int idx   = 0;
        while ((idx = text.indexOf(target, idx)) != -1) {
            count++;
            idx += target.length();
        }
        return count;
    }

    @Override
    protected String getDescriptionForKey(String key) {
        switch (key) {
            case "file_path":   return "Relative path of the file to edit (within the working directory).";
            case "old_string":  return "Exact literal string to find. Fails if absent or non-unique unless replace_all=true.";
            case "new_string":  return "Replacement text. Pass an empty string to delete the matched text.";
            case "replace_all": return "If true, replace every occurrence of old_string. Defaults to false.";
            default:            return super.getDescriptionForKey(key);
        }
    }

    @Override
    protected MCP_KEY_TYPE getKeyTypeForMCP(String key) {
        if ("replace_all".equals(key)) return MCP_KEY_TYPE.BOOLEAN;
        return super.getKeyTypeForMCP(key);
    }

    @Override
    public String getReactorDescription() {
        return "Performs exact string replacement in a file. Fails if old_string is not unique "
             + "(unless replace_all=true). Prefer this over WriteFile for targeted edits.";
    }
}
