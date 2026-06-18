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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.logging.log4j.Logger;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.CmdExecUtil;

/**
 * Executes a shell command in the working directory.
 *
 * <p>Pipes ({@code |}) between whitelisted commands are supported, enabling powerful one-liners
 * like {@code grep "TODO" src/main.java | head -20}. Command chaining with {@code &&} and
 * {@code ||} is also allowed. Redirects ({@code >}, {@code >>}) and absolute paths are blocked.
 *
 * <p>This reactor:
 * <ul>
 *   <li>Allows pipes and {@code &&}/{@code ||} chaining
 *   <li>Has a large allowed-command whitelist (builds, package managers, scripts)
 *   <li>Uses the insight's {@link CmdExecUtil} which maintains working-dir state across calls
 *       and enforces the sandbox boundary after each execution
 * </ul>
 */
public class BashCommandReactor extends AbstractAgentToolReactor {

    private static final int MAX_COMMAND_LENGTH = 4000;

    private static final Set<String> ALLOWED_COMMANDS = new HashSet<>(Arrays.asList(
            // File system
            "ls", "dir", "pwd", "cat", "head", "tail", "wc", "find", "stat", "touch",
            "mkdir", "rm", "cp", "mv", "chmod",
            // Search
            "grep", "rg", "awk", "sed", "cut", "sort", "uniq", "diff", "patch",
            // Archives
            "zip", "unzip", "tar", "gzip", "gunzip",
            // Network
            "curl", "wget",
            // VCS
            "git",
            // Build tools
            "mvn", "make", "gradle",
            // Runtimes / package managers
            "python3", "python", "node", "npm", "npx", "pnpm", "yarn", "java", "javac",
            // Shell utilities
            "echo", "printf", "xargs", "tee", "tr", "date", "env", "which",
            // Text processing
            "jq", "xmllint", "csvtool"
    ));

    public BashCommandReactor() {
        this.keysToGet = new String[]{"command", "description"};
        this.keyRequired = new int[]{1, 0};
    }

    @Override
    protected NounMetadata doExecute() throws Exception {
        String command     = this.keyValue.get("command");
        String description = this.keyValue.get("description");

        if (command == null || command.trim().isEmpty()) {
            return new NounMetadata("Error: command is required", PixelDataType.CONST_STRING);
        }
        command = command.trim();

        if (command.length() > MAX_COMMAND_LENGTH) {
            return new NounMetadata("Error: command exceeds maximum length of " + MAX_COMMAND_LENGTH, PixelDataType.CONST_STRING);
        }

        // Validate before executing
        String validationError = validateCommand(command);
        if (validationError != null) {
            return new NounMetadata("Error: " + validationError, PixelDataType.CONST_STRING);
        }

        Logger logger = getLogger(BashCommandReactor.class.getName());
        if (description != null && !description.trim().isEmpty()) {
            logger.info("[BashCommand] [{}] executing: {}", description, command);
        } else {
            logger.info("[BashCommand] executing: {}", command);
        }

        String insightRoot = normalizePath(insightFolder);
        // Default agent-tool runs are room-rooted, so CmdExecUtil may not be initialized from
        // project context. Fall back to the room sandbox root in that case.
        CmdExecUtil cmdUtil = insight.getCmdUtil();
        if (cmdUtil == null) {
            cmdUtil = new CmdExecUtil(insight.getUser(), insight.getInsightId(), insightRoot);
        }

        // Reset working dir if it has drifted outside the sandbox (e.g. from a prior cd)
        if (!isWithinRoot(normalizePath(cmdUtil.getWorkingDir()), insightRoot)) {
            cmdUtil.setWorkingDir(insightRoot);
        }

        String output = cmdUtil.executeCommand(command);

        // Post-execution sandbox check - a cd could have moved us outside
        String updatedDir = normalizePath(cmdUtil.getWorkingDir());
        if (!isWithinRoot(updatedDir, insightRoot)) {
            cmdUtil.setWorkingDir(insightRoot);
            throw new IllegalArgumentException(
                    "Command attempted to navigate outside the working directory sandbox.");
        }

        if (output == null) output = "";
        return new NounMetadata(output, PixelDataType.CONST_STRING);
    }

    /**
     * Validates the command string. Returns an error message string, or null if valid.
     *
     * <p>Validation rules:
     * <ul>
     *   <li>Redirects ({@code >}, {@code >>}) are blocked
     *   <li>Backtick command substitution is blocked
     *   <li>Absolute paths ({@code /}, {@code ~}) in arguments are blocked
     *   <li>Path traversal ({@code ..}) is blocked
     *   <li>Each command in a pipeline / chain must be in the whitelist
     * </ul>
     */
    private String validateCommand(String command) {
        // Block redirects - check for > and < that aren't part of >= or <=
        // We allow > inside strings but that's hard to detect; block it at the top level.
        // Simple heuristic: block unquoted > and <
        if (containsUnquoted(command, '>') || containsUnquoted(command, '<')) {
            return "Redirects (>, <, >>) are not allowed. Use curl/wget -o to write files.";
        }

        // Block backtick substitution
        if (command.contains("`")) {
            return "Backtick command substitution is not allowed.";
        }

        // Block $() command substitution
        if (command.contains("$(")) {
            return "Command substitution $() is not allowed.";
        }

        // Block absolute paths and ~ in non-quoted tokens
        for (String token : tokenize(command)) {
            String clean = stripQuotes(token);
            if (clean.isEmpty() || clean.startsWith("-")) continue;
            if (clean.startsWith("/")) {
                return "Absolute paths are not allowed: " + clean;
            }
            if (clean.startsWith("~")) {
                return "Home-directory paths (~) are not allowed: " + clean;
            }
            if (clean.contains("..")) {
                return "Path traversal (..) is not allowed: " + clean;
            }
        }

        // Validate each command in any pipeline / chain
        // Split on |, &&, || to get individual sub-commands
        String[] segments = command.split("(?<!\\|)\\|(?!\\|)|&&|\\|\\|");
        for (String segment : segments) {
            String trimmed = segment.trim();
            if (trimmed.isEmpty()) continue;
            String[] tokens = trimmed.split("\\s+");
            if (tokens.length == 0) continue;
            String baseCmd = tokens[0].toLowerCase();
            if (!ALLOWED_COMMANDS.contains(baseCmd)) {
                return "Command not in allowed list: " + baseCmd
                        + ". Allowed: " + ALLOWED_COMMANDS.toString();
            }
        }

        return null; // valid
    }

    private boolean isWithinRoot(String path, String root) {
        if (root == null || root.isEmpty()) return true;
        if (path.equals(root)) return true;
        String prefix = root.endsWith("/") ? root : root + "/";
        return path.startsWith(prefix);
    }

    private boolean containsUnquoted(String command, char target) {
        boolean inSingle = false;
        boolean inDouble = false;
        for (int i = 0; i < command.length(); i++) {
            char c = command.charAt(i);
            if (c == '\'' && !inDouble) { inSingle = !inSingle; continue; }
            if (c == '"'  && !inSingle) { inDouble = !inDouble; continue; }
            if (!inSingle && !inDouble && c == target) return true;
        }
        return false;
    }

    private String[] tokenize(String command) {
        // Simple whitespace split - doesn't handle quoted spaces but good enough for path checks
        return command.split("\\s+");
    }

    private String stripQuotes(String token) {
        if (token.length() >= 2) {
            if ((token.startsWith("\"") && token.endsWith("\""))
                    || (token.startsWith("'") && token.endsWith("'"))) {
                return token.substring(1, token.length() - 1);
            }
        }
        return token;
    }

    @Override
    public String getReactorDescription() {
        return "Executes a shell command in the working directory. Pipes and && chaining are supported. "
             + "Redirects (>, >>) and absolute paths are blocked. 60-second timeout. "
             + "Use for builds, tests, git operations, and tasks ReadFile/GrepFiles cannot handle.";
    }
}
