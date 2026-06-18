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
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Finds files matching a glob pattern in the working directory.
 *
 * <p>Supports standard glob syntax: {@code **} for any path depth, {@code *} for any file name
 * segment, {@code ?} for any single character, {@code {a,b}} for alternation.
 * Examples: {@code **&#47;*.java}, {@code src&#47;**&#47;*.ts}, {@code *.{json,yaml}}.
 *
 * <p>Returns paths relative to the working directory, sorted by last modified time (most recent
 * first).
 */
public class GlobFilesReactor extends AbstractAgentToolReactor {

    private static final int MAX_RESULTS = 500;

    public GlobFilesReactor() {
        this.keysToGet = new String[]{"pattern", "path"};
        this.keyRequired = new int[]{1, 0};
    }

    @Override
    protected NounMetadata doExecute() throws Exception {
        String pattern  = this.keyValue.get("pattern");
        String basePath = this.keyValue.get("path");

        File baseDir = resolveAndValidate(basePath);
        if (!baseDir.exists() || !baseDir.isDirectory()) {
            return new NounMetadata(
                    "Error: directory not found: " + (basePath != null ? basePath : "."),
                    PixelDataType.CONST_STRING);
        }

        PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern);

        List<Path> matches = new ArrayList<>();
        Files.walk(baseDir.toPath())
             .filter(p -> !Files.isDirectory(p))
             .filter(p -> {
                 Path rel = baseDir.toPath().relativize(p);
                 // Match against relative path (for ** patterns) or just filename
                 return matcher.matches(rel) || matcher.matches(p.getFileName());
             })
             .limit(MAX_RESULTS)
             .forEach(matches::add);

        if (matches.isEmpty()) {
            return new NounMetadata("No files matched pattern: " + pattern, PixelDataType.CONST_STRING);
        }

        // Sort by last modified time descending (most recently modified first)
        matches.sort(Comparator.comparingLong(
                p -> { try { return -Files.getLastModifiedTime(p).toMillis(); } catch (Exception e) { return 0L; } }));

        List<String> results = new ArrayList<>();
        for (Path p : matches) {
            results.add(toRelative(p.toAbsolutePath().toString()));
        }

        return new NounMetadata(String.join("\n", results), PixelDataType.CONST_STRING);
    }

    @Override
    public String getReactorDescription() {
        return "Finds files matching a glob pattern (e.g. **/*.java, src/**/*.ts). "
             + "Returns matching paths relative to the working directory, sorted by modification time.";
    }
}
