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

/**
 * Reads {@code AGENTS.md} or {@code CLAUDE.md} from the agent's working directory.
 *
 * <p><b>Never walks up.</b> The earlier walk-up implementation leaked unrelated
 * repo-level instructions (e.g. {@code Semoss/CLAUDE.md}) into every run when
 * {@code SEMOSS_BASE_FOLDER} lived inside the source tree. We now only look at
 * the working dir itself.
 *
 * <p>Used by {@link prerna.reactor.agent.config.AgentConfigLoader} to inject
 * workspace context into the system prompt.
 */
public class AgentsMdLoader {

    private static final Logger logger = LogManager.getLogger(AgentsMdLoader.class);

    public static final String[] NAMES = { "AGENTS.md", "CLAUDE.md" };
    public static final int MAX_BYTES = 100 * 1024;

    private AgentsMdLoader() {}

    /**
     * Look for {@code AGENTS.md} then {@code CLAUDE.md} at {@code workingDir} itself.
     * Returns the content of the first file found, or {@code null}.
     *
     * <p>No directory traversal. Drop the doc at the root of your workingDir.
     */
    public static String discover(String workingDir) {
        if (workingDir == null || workingDir.isBlank()) {
            return null;
        }

        File dir = new File(workingDir).getAbsoluteFile();
        if (!dir.isDirectory() || !dir.exists()) {
            logger.debug("AgentsMdLoader: workingDir is not a directory: {}", workingDir);
            return null;
        }

        for (String name : NAMES) {
            File candidate = new File(dir, name);
            if (candidate.isFile()) {
                String content = tryRead(candidate);
                if (content != null) {
                    logger.info("AgentsMdLoader: loaded {} ({} chars)",
                            candidate.getAbsolutePath(), content.length());
                    return content;
                }
            }
        }

        logger.debug("AgentsMdLoader: no AGENTS.md or CLAUDE.md at workingDir={}",
                dir.getAbsolutePath());
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
}
