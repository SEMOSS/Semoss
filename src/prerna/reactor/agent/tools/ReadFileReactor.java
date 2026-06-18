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
import java.nio.file.Files;
import java.util.List;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Reads a file from the working directory.
 *
 * <p>Returns content formatted as {@code cat -n} (line-number prefix), making it easy to
 * reference specific line numbers when editing. Use {@code offset} and {@code limit} for large
 * files instead of reading everything at once.
 */
public class ReadFileReactor extends AbstractAgentToolReactor {

    private static final int DEFAULT_MAX_LINES = 2000;

    public ReadFileReactor() {
        this.keysToGet = new String[]{"file_path", "offset", "limit"};
        this.keyRequired = new int[]{1, 0, 0};
    }

    @Override
    protected NounMetadata doExecute() throws Exception {
        String filePath = this.keyValue.get("file_path");
        String offsetStr = this.keyValue.get("offset");
        String limitStr  = this.keyValue.get("limit");

        int offset = parseIntOr(offsetStr, 1);
        int limit  = parseIntOr(limitStr, DEFAULT_MAX_LINES);
        if (offset < 1) offset = 1;
        if (limit  < 1) limit  = DEFAULT_MAX_LINES;

        File file = resolveAndValidate(filePath);
        if (!file.exists()) {
            return new NounMetadata("Error: file not found: " + filePath, PixelDataType.CONST_STRING);
        }
        if (!file.isFile()) {
            return new NounMetadata("Error: not a file: " + filePath, PixelDataType.CONST_STRING);
        }

        List<String> lines = Files.readAllLines(file.toPath());
        int start = offset - 1; // convert to 0-based
        if (start >= lines.size()) {
            return new NounMetadata("", PixelDataType.CONST_STRING);
        }
        int end = Math.min(start + limit, lines.size());

        StringBuilder sb = new StringBuilder();
        for (int i = start; i < end; i++) {
            sb.append(String.format("%6d\t%s%n", i + 1, lines.get(i)));
        }
        return new NounMetadata(sb.toString(), PixelDataType.CONST_STRING);
    }

    private int parseIntOr(String s, int defaultVal) {
        if (s == null || s.trim().isEmpty()) return defaultVal;
        try { return Integer.parseInt(s.trim()); } catch (NumberFormatException e) { return defaultVal; }
    }

    @Override
    public String getReactorDescription() {
        return "Reads a file from the working directory. Returns content with line numbers (cat -n format). "
             + "Use offset (1-based line) and limit (max lines) to read large files in chunks.";
    }
}
