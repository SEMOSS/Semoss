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
package prerna.reactor.agent.runtime;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.Utility;

/**
 * Discovers and reads the nearest {@code AGENTS.md} or {@code CLAUDE.md} by walking
 * up from a starting directory toward the SEMOSS base folder.
 *
 * <p>Used by {@link SemossAgentHarness} to inject workspace context into the system prompt.
 */
class AgentsMdLoader {

    private static final Logger logger = LogManager.getLogger(AgentsMdLoader.class);

    static final String[] NAMES = { "AGENTS.md", "CLAUDE.md" };
    static final int MAX_WALK_DEPTH = 10;
    static final int MAX_BYTES = 100 * 1024;

    private AgentsMdLoader() {}

    /**
     * Walk up from {@code startPath} looking for {@code AGENTS.md} then {@code CLAUDE.md}
     * at each directory level. Returns the content of the first file found, or {@code null}.
     *
     * <p>Stops at the SEMOSS base folder, the filesystem root, or after {@value #MAX_WALK_DEPTH}
     * levels — whichever comes first.
     */
    static String discover(String startPath) {
        if (startPath == null || startPath.isBlank()) {
            return null;
        }

        File dir = new File(startPath).getAbsoluteFile();
        if (!dir.isDirectory()) {
            dir = dir.getParentFile();
        }
        if (dir == null || !dir.exists()) {
            return null;
        }

        String baseFolder = resolveBaseFolder();

        for (int depth = 0; depth < MAX_WALK_DEPTH && dir != null; depth++) {
            for (String name : NAMES) {
                File candidate = new File(dir, name);
                if (candidate.isFile()) {
                    String content = tryRead(candidate);
                    if (content != null) {
                        logger.info("AgentsMdLoader: loaded {} ({} chars) at depth={}",
                                candidate.getAbsolutePath(), content.length(), depth);
                        return content;
                    }
                }
            }

            if (baseFolder != null && dir.getAbsolutePath().equals(baseFolder)) {
                break;
            }

            File parent = dir.getParentFile();
            if (parent == null || parent.equals(dir)) {
                break;
            }
            dir = parent;
        }

        logger.debug("AgentsMdLoader: no AGENTS.md or CLAUDE.md found under startPath={}", startPath);
        return null;
    }

    private static String tryRead(File f) {
        try {
            byte[] bytes = Files.readAllBytes(f.toPath());
            if (bytes.length > MAX_BYTES) {
                logger.warn("AgentsMdLoader: {} is {} bytes (limit {}), skipping",
                        f.getAbsolutePath(), bytes.length, MAX_BYTES);
                return null;
            }
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (IOException e) {
            logger.warn("AgentsMdLoader: could not read {}: {}", f.getAbsolutePath(), e.getMessage());
            return null;
        }
    }

    private static String resolveBaseFolder() {
        try {
            String base = Utility.getBaseFolder();
            if (base != null && !base.isBlank()) {
                return new File(base).getAbsolutePath();
            }
        } catch (Exception ignored) {}
        return null;
    }
}
