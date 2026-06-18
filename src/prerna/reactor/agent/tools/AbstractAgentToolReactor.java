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
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import org.apache.logging.log4j.Logger;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.FileSystemUtil;
import prerna.util.Utility;

/**
 * Base class for the built-in agent toolkit reactors.
 *
 * <p>Tools operate relative to the insight's working directory (insight folder), which the agent
 * harness sets to the room folder. All path arguments must be relative; absolute paths and path
 * traversal (../) are rejected.
 */
public abstract class AbstractAgentToolReactor extends AbstractReactor {

    /** Normalized absolute path of the insight's working directory. Set in {@link #execute()}. */
    protected String insightFolder;

    @Override
    public NounMetadata execute() {
        Logger logger = getLogger(this.getClass().getName());
        try {
            organizeKeys();
            String rawFolder = insight.getInsightFolder();
            insightFolder = normalizePath(rawFolder);
            logger.info("[AgentTool] {} | insightId={} | rawInsightFolder={} | normalizedInsightFolder={}",
                    this.getClass().getSimpleName(),
                    insight.getInsightId(),
                    rawFolder,
                    insightFolder);
            return doExecute();
        } catch (IllegalArgumentException e) {
            return new NounMetadata("Error: " + e.getMessage(), PixelDataType.CONST_STRING);
        } catch (Exception e) {
            logger.error("[AgentTool] {} error", this.getClass().getSimpleName(), e);
            return new NounMetadata("Error: " + e.getMessage(), PixelDataType.CONST_STRING);
        }
    }

    /** Main execution logic. Implement in each reactor. */
    protected abstract NounMetadata doExecute() throws Exception;

    /**
     * Resolves a relative path against {@link #insightFolder} and validates it does not escape
     * the working directory. Null or empty path resolves to insightFolder itself.
     *
     * @throws IllegalArgumentException if the path is absolute or escapes the root
     */
    protected File resolveAndValidate(String relativePath) {
        if (relativePath == null || relativePath.trim().isEmpty()) {
            return new File(insightFolder);
        }
        String clean = relativePath.trim();
        if (new File(clean).isAbsolute()) {
            throw new IllegalArgumentException("Absolute paths are not allowed: " + clean);
        }
        File resolved = new File(insightFolder, clean);
        String normalizedResolved = normalizePath(resolved.getAbsolutePath());
        if (!normalizedResolved.equals(insightFolder)
                && !normalizedResolved.startsWith(insightFolder + "/")) {
            throw new IllegalArgumentException("Path escapes the working directory: " + clean);
        }
        return resolved;
    }

    /**
     * Saves text through the same byte-safe path as SaveInsightAssetsBase64.
     *
     * <p>The agent-facing tools still accept normal strings, but this helper base64-encodes before
     * calling SEMOSS' shared asset save utility so nested Pixel {@code <encode>} blocks in source
     * files cannot be consumed by Pixel preprocessing.
     */
    protected void saveTextFileWithInsightAssetsBase64(File file, String content) {
        if (content == null) {
            content = "";
        }

        User user = this.insight.getUser();
        if (AbstractSecurityUtils.anonymousUsersEnabled() && user != null && user.isAnonymous()) {
            throwAnonymousUserError();
        }

        String relativePath = toRelative(file.getAbsolutePath());
        if (".".equals(relativePath) || relativePath.startsWith("/")) {
            throw new IllegalArgumentException("A file path is required");
        }

        List<String> filePaths = Collections.singletonList(relativePath);
        boolean strictScriptSource = Boolean.parseBoolean(
                Utility.getDIHelperProperty(Constants.STRICT_SCRIPT_SOURCE));
        FileSystemUtil.validateAssetFiles(filePaths, strictScriptSource);

        String encodedContent = Base64.getEncoder()
                .encodeToString(content.getBytes(StandardCharsets.UTF_8));
        FileSystemUtil.saveAssetFilesBase64(
                insightFolder,
                filePaths,
                Collections.singletonList(encodedContent),
                true);

        if (this.insight.getRoomId() != null) {
            ClusterUtil.pushRoomAsync(this.insight.getRoomId());
        }
    }

    /** Normalizes a filesystem path: canonical, forward slashes, no trailing slash. */
    protected String normalizePath(String path) {
        if (path == null) return "";
        try {
            String normalized = new File(path).getCanonicalPath().replace("\\", "/");
            if (normalized.endsWith("/") && normalized.length() > 1) {
                normalized = normalized.substring(0, normalized.length() - 1);
            }
            return normalized;
        } catch (Exception e) {
            return path.replace("\\", "/");
        }
    }

    /** Returns the path relative to {@link #insightFolder}, or the absolute path if outside. */
    protected String toRelative(String absolutePath) {
        String normalized = normalizePath(absolutePath);
        if (normalized.startsWith(insightFolder + "/")) {
            return normalized.substring(insightFolder.length() + 1);
        }
        if (normalized.equals(insightFolder)) {
            return ".";
        }
        return normalized;
    }
}
