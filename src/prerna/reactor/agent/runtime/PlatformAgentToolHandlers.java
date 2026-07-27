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
import java.io.RandomAccessFile;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.reactor.agent.AgentRunContext;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.reactor.agent.skill.SkillScanner;
import prerna.reactor.agent.skill.SkillScanner.DiscoveredSkill;
import prerna.util.CmdExecUtil;
import prerna.util.Constants;
import prerna.util.FileSystemUtil;
import prerna.util.Utility;

final class PlatformAgentToolHandlers {

	private static final Logger logger = LogManager.getLogger(PlatformAgentToolHandlers.class);

	private static final String TOOL_KIND = "semoss_platform_default";
	private static final int DEFAULT_READ_MAX_LINES = 2000;
	private static final int MAX_GLOB_RESULTS = 500;
	private static final int DEFAULT_GREP_HEAD_LIMIT = 200;
	private static final int MAX_MULTI_EDITS = 200;
	private static final String TODOS_FILE = "todos.json";
	private static final Set<String> VALID_TODO_STATUSES = Set.of("pending", "in_progress", "completed");
	private static final Set<String> VALID_TODO_PRIORITIES = Set.of("high", "medium", "low");
	private static final int MAX_TODO_CONTENT_LEN = 2000;
	private static final int MAX_TODO_ID_LEN = 64;
	private static final int MAX_TODO_ITEMS = 200;
	private static final int DEFAULT_SKILL_MAX_BYTES = 8 * 1024;
	private static final int HARD_SKILL_MAX_BYTES = 200 * 1024;
	private static final int MAX_COMMAND_LENGTH = 4000;
	private static final String PROP_ENABLE_BASH = "AGENT_DEFAULT_TOOLS_ENABLE_BASH";

	private static final Set<String> ALLOWED_COMMANDS = new HashSet<>(Arrays.asList(
			"pwd", "ls", "dir", "find", "cat", "head", "tail", "wc", "stat",
			"grep", "rg", "sed", "awk", "cut", "sort", "uniq", "tr", "diff",
			"python", "python3",
			"mkdir", "touch", "cp", "mv",
			"curl", "wget",
			"zip", "unzip",
			"jq", "which"));

	private PlatformAgentToolHandlers() {
	}

	interface ToolHandler {
		String getName();

		JSONObject asToolDefinition();

		String execute(Map<String, Object> params, AgentRunContext ctx) throws Exception;
	}

	static Map<String, ToolHandler> handlersByName() {
		Map<String, ToolHandler> tools = new LinkedHashMap<>();
		add(tools, handler("ReadFile",
				"Reads a file from the working directory. Returns content with line numbers "
						+ "and a continuation marker when more lines remain.",
				objectSchema(props(
						prop("file_path", stringProp("Path to read, relative to the working directory.")),
						prop("offset", integerProp("1-based first line to read. Defaults to 1.")),
						prop("limit", integerProp("Maximum lines to return. Defaults to 2000."))),
						List.of("file_path")),
				PlatformAgentToolHandlers::readFile));
		add(tools, handler("WriteFile",
				"Writes text to a file under the working directory, creating parent directories as needed.",
				objectSchema(props(
						prop("filePath", stringProp("Path to write, relative to the working directory.")),
						prop("content", stringProp("Complete file content."))),
						List.of("filePath", "content")),
				PlatformAgentToolHandlers::writeFile));
		add(tools, handler("EditFile",
				"Performs one exact string replacement in a file. Fails if the old string is not unique unless replace_all=true.",
				objectSchema(props(
						prop("file_path", stringProp("Path to edit, relative to the working directory.")),
						prop("old_string", stringProp("Exact text to replace.")),
						prop("new_string", stringProp("Replacement text.")),
						prop("replace_all", booleanProp("Replace every occurrence instead of requiring uniqueness."))),
						List.of("file_path", "old_string", "new_string")),
				PlatformAgentToolHandlers::editFile));
		add(tools, handler("MultiEdit",
				"Applies multiple exact string replacements to one file in a single all-or-nothing operation.",
				objectSchema(props(
						prop("file_path", stringProp("Path to edit, relative to the working directory.")),
						prop("edits_json", stringProp(
								"JSON array of edits: [{\"old_string\":\"...\",\"new_string\":\"...\",\"replace_all\":false}]."))),
						List.of("file_path", "edits_json")),
				PlatformAgentToolHandlers::multiEdit));
		add(tools, handler("MoveFile",
				"Moves or renames a path under the working directory.",
				objectSchema(props(
						prop("filePath", stringProp("Existing path, relative to the working directory.")),
						prop("newValue", stringProp("New path, relative to the working directory."))),
						List.of("filePath", "newValue")),
				PlatformAgentToolHandlers::moveFile));
		add(tools, handler("DeleteFile",
				"Deletes a file or directory under the working directory.",
				objectSchema(props(
						prop("filePath", stringProp("Path to delete, relative to the working directory."))),
						List.of("filePath")),
				PlatformAgentToolHandlers::deleteFile));
		add(tools, handler("GlobFiles",
				"Finds files matching a glob pattern under the working directory.",
				objectSchema(props(
						prop("pattern", stringProp("Glob pattern such as **/*.java or src/**/*.ts.")),
						prop("path", stringProp("Optional directory to search, relative to the working directory."))),
						List.of("pattern")),
				PlatformAgentToolHandlers::globFiles));
		add(tools, handler("GrepFiles",
				"Searches file contents with a regular expression.",
				objectSchema(props(
						prop("pattern", stringProp("Regex pattern to search for.")),
						prop("path", stringProp("Optional path to search, relative to the working directory.")),
						prop("glob", stringProp("Optional file glob filter, such as *.java.")),
						prop("output_mode", stringProp("files_with_matches, content, or count. Defaults to files_with_matches.")),
						prop("after_context", integerProp("Lines after each match.")),
						prop("before_context", integerProp("Lines before each match.")),
						prop("context", integerProp("Lines before and after each match.")),
						prop("case_insensitive", booleanProp("Case-insensitive matching.")),
						prop("head_limit", integerProp("Maximum result lines. Defaults to 200."))),
						List.of("pattern")),
				PlatformAgentToolHandlers::grepFiles));
		add(tools, handler("ListDirectory",
				"Lists directory contents under the working directory.",
				objectSchema(props(prop("path", stringProp("Optional directory path. Defaults to working directory."))),
						Collections.emptyList()),
				PlatformAgentToolHandlers::listDirectory));
		if (isBashEnabled()) {
			add(tools, handler("BashCommand",
					"Executes one allowlisted shell command in the working directory.",
					objectSchema(props(
							prop("command", stringProp("Single command to execute. Shell chains, pipes, redirects, and command substitution are blocked.")),
							prop("description", stringProp("Short reason for running the command."))),
							List.of("command")),
					PlatformAgentToolHandlers::bashCommand));
		}
		add(tools, handler("TodoWrite",
				"Replaces the current todo list with a validated full-state JSON array.",
				objectSchema(props(prop("items_json", stringProp(
						"JSON array of todo items: [{\"id\":\"...\",\"content\":\"...\",\"status\":\"pending|in_progress|completed\",\"priority\":\"high|medium|low\"}]."))),
						List.of("items_json")),
				PlatformAgentToolHandlers::todoWrite));
		add(tools, handler("TodoRead",
				"Reads the current todo list from todos.json in the working directory.",
				objectSchema(new LinkedHashMap<>(), Collections.emptyList()),
				PlatformAgentToolHandlers::todoRead));
		add(tools, handler("ListSkill",
				"Lists skills discovered under conventional skill folders in the working directory.",
				objectSchema(new LinkedHashMap<>(), Collections.emptyList()),
				PlatformAgentToolHandlers::listSkill));
		add(tools, handler("LoadSkill",
				"Loads a chunk of a named skill from SKILL.md under the working directory.",
				objectSchema(props(
						prop("skill_name", stringProp("Skill folder name to load.")),
						prop("offset", integerProp("Byte offset to start at. Defaults to 0.")),
						prop("max_bytes", integerProp("Maximum bytes to return. Defaults to 8192."))),
						List.of("skill_name")),
				PlatformAgentToolHandlers::loadSkill));
		return Collections.unmodifiableMap(tools);
	}

	private static void add(Map<String, ToolHandler> tools, ToolHandler handler) {
		tools.put(handler.getName(), handler);
	}

	private static ToolHandler handler(String name, String description, JSONObject inputSchema, ToolExecutor executor) {
		return new ToolHandler() {
			@Override
			public String getName() {
				return name;
			}

			@Override
			public JSONObject asToolDefinition() {
				JSONObject tool = new JSONObject();
				tool.put("name", name);
				tool.put("title", MCPUtility.formatToTitleCase(name));
				tool.put("description", description);
				tool.put("inputSchema", inputSchema);
				JSONObject meta = new JSONObject();
				meta.put("SMSS_TOOL_KIND", TOOL_KIND);
				meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.AUTO.getValue());
				tool.put("_meta", meta);
				return tool;
			}

			@Override
			public String execute(Map<String, Object> params, AgentRunContext ctx) throws Exception {
				return executor.execute(params != null ? params : Collections.emptyMap(), toolContext(ctx));
			}
		};
	}

	private static ToolContext toolContext(AgentRunContext ctx) {
		if (ctx == null) {
			throw new IllegalArgumentException("Agent run context is required");
		}
		String workingDir = ctx.getAgentConfig() != null ? ctx.getAgentConfig().getWorkingDir() : null;
		if ((workingDir == null || workingDir.trim().isEmpty()) && ctx.getInsight() != null) {
			workingDir = ctx.getInsight().getInsightFolder();
		}
		if (workingDir == null || workingDir.trim().isEmpty()) {
			throw new IllegalArgumentException("Agent working directory is not set");
		}
		return new ToolContext(ctx, normalizePath(workingDir));
	}

	private static String readFile(Map<String, Object> params, ToolContext tc) throws Exception {
		String filePath = stringParam(params, "file_path");
		int offset = parseIntOr(params.get("offset"), 1);
		int limit = parseIntOr(params.get("limit"), DEFAULT_READ_MAX_LINES);
		if (offset < 1) {
			offset = 1;
		}
		if (limit < 1) {
			limit = DEFAULT_READ_MAX_LINES;
		}
		File file = tc.resolve(filePath);
		if (!file.exists()) {
			return "Error: file not found: " + filePath;
		}
		if (!file.isFile()) {
			return "Error: not a file: " + filePath;
		}
		String content = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
		List<String> lines = content.lines().toList();
		int start = offset - 1;
		if (start >= lines.size()) {
			return "";
		}
		int end = Math.min(start + limit, lines.size());
		StringBuilder sb = new StringBuilder();
		for (int i = start; i < end; i++) {
			sb.append(String.format("%6d\t%s%n", i + 1, lines.get(i)));
		}
		if (end < lines.size()) {
			sb.append(String.format("%n[--- file continues: showing lines %d-%d of %d; continue with offset=%d. ---]%n",
					start + 1, end, lines.size(), end + 1));
		}
		return sb.toString();
	}

	private static String writeFile(Map<String, Object> params, ToolContext tc) {
		String filePath = firstStringParam(params, "filePath", "file_path");
		String content = stringParam(params, "content");
		if (filePath == null || filePath.trim().isEmpty()) {
			return "Error: filePath is required";
		}
		File file = tc.resolve(filePath);
		saveTextFile(file, content, tc);
		return "Wrote file: " + tc.toRelative(file.getAbsolutePath());
	}

	private static String editFile(Map<String, Object> params, ToolContext tc) throws Exception {
		String filePath = stringParam(params, "file_path");
		String oldString = stringParam(params, "old_string");
		String newString = stringParam(params, "new_string");
		boolean replaceAll = parseBoolean(params.get("replace_all"));
		if (oldString == null || oldString.isEmpty()) {
			return "Error: old_string must not be empty";
		}
		if (newString == null) {
			newString = "";
		}
		File file = tc.resolve(filePath);
		if (!file.exists() || !file.isFile()) {
			return "Error: file not found: " + filePath;
		}
		String content = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
		int occurrences = countOccurrences(content, oldString);
		if (occurrences == 0) {
			return "Error: old_string not found in file: " + filePath;
		}
		if (!replaceAll && occurrences > 1) {
			return "Error: old_string appears " + occurrences + " times in " + filePath
					+ ". Provide more surrounding context to make it unique, or use replace_all=true.";
		}
		String updated = replaceAll ? content.replace(oldString, newString)
				: content.replaceFirst(Pattern.quote(oldString), Matcher.quoteReplacement(newString));
		saveTextFile(file, updated, tc);
		return "Replaced " + occurrences + " occurrence(s) in: " + tc.toRelative(file.getAbsolutePath());
	}

	private static String multiEdit(Map<String, Object> params, ToolContext tc) throws Exception {
		String filePath = stringParam(params, "file_path");
		String editsJson = stringParam(params, "edits_json");
		if (editsJson == null || editsJson.trim().isEmpty()) {
			return "Error: edits_json is required";
		}
		JSONArray edits;
		try {
			edits = new JSONArray(editsJson.trim());
		} catch (Exception e) {
			return "Error: edits_json must be a valid JSON array - " + e.getMessage();
		}
		if (edits.length() == 0) {
			return "Error: edits_json must contain at least one edit";
		}
		if (edits.length() > MAX_MULTI_EDITS) {
			return "Error: too many edits (" + edits.length() + " > " + MAX_MULTI_EDITS + ")";
		}
		File file = tc.resolve(filePath);
		if (!file.exists() || !file.isFile()) {
			return "Error: file not found: " + filePath;
		}
		String working = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
		int totalReplacements = 0;
		for (int i = 0; i < edits.length(); i++) {
			JSONObject edit;
			try {
				edit = edits.getJSONObject(i);
			} catch (Exception e) {
				return "Error: edits[" + i + "] is not an object";
			}
			String oldString = optString(edit, "old_string");
			String newString = optString(edit, "new_string");
			boolean replaceAll = edit.optBoolean("replace_all", false);
			if (oldString == null || oldString.isEmpty()) {
				return "Error: edits[" + i + "].old_string is required and must not be empty";
			}
			if (newString == null) {
				newString = "";
			}
			if (oldString.equals(newString)) {
				return "Error: edits[" + i + "].old_string and new_string are identical";
			}
			int occurrences = countOccurrences(working, oldString);
			if (occurrences == 0) {
				return "Error: edits[" + i + "].old_string not found"
						+ (i > 0 ? " (note: edits apply in order; an earlier edit may have changed this text)" : "");
			}
			if (!replaceAll && occurrences > 1) {
				return "Error: edits[" + i + "].old_string appears " + occurrences + " times. "
						+ "Provide more surrounding context to make it unique, or set replace_all=true.";
			}
			working = replaceAll ? working.replace(oldString, newString)
					: working.replaceFirst(Pattern.quote(oldString), Matcher.quoteReplacement(newString));
			totalReplacements += occurrences;
		}
		saveTextFile(file, working, tc);
		return "Applied " + edits.length() + " edit(s) (" + totalReplacements + " replacement(s)) to: "
				+ tc.toRelative(file.getAbsolutePath());
	}

	private static String moveFile(Map<String, Object> params, ToolContext tc) throws Exception {
		String filePath = firstStringParam(params, "filePath", "file_path");
		String newValue = firstStringParam(params, "newValue", "new_value");
		if (filePath == null || filePath.trim().isEmpty()) {
			return "Error: filePath is required";
		}
		if (newValue == null || newValue.trim().isEmpty()) {
			return "Error: newValue is required";
		}
		File source = tc.resolve(filePath);
		File target = tc.resolve(newValue);
		if (!source.exists()) {
			return "Error: file not found: " + filePath;
		}
		File parent = target.getParentFile();
		if (parent != null && !parent.exists()) {
			parent.mkdirs();
		}
		Files.move(source.toPath(), target.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
		pushRoom(tc);
		return "Moved " + tc.toRelative(source.getAbsolutePath()) + " to " + tc.toRelative(target.getAbsolutePath());
	}

	private static String deleteFile(Map<String, Object> params, ToolContext tc) throws Exception {
		String filePath = firstStringParam(params, "filePath", "file_path");
		if (filePath == null || filePath.trim().isEmpty()) {
			return "Error: filePath is required";
		}
		File target = tc.resolve(filePath);
		if (!target.exists()) {
			return "Error: path not found: " + filePath;
		}
		deleteTree(target.toPath());
		pushRoom(tc);
		return "Deleted: " + filePath;
	}

	private static String globFiles(Map<String, Object> params, ToolContext tc) throws Exception {
		String pattern = stringParam(params, "pattern");
		String basePath = stringParam(params, "path");
		File baseDir = tc.resolve(basePath);
		if (!baseDir.exists() || !baseDir.isDirectory()) {
			return "Error: directory not found: " + (basePath != null ? basePath : ".");
		}
		PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern);
		List<Path> matches = new ArrayList<>();
		try (Stream<Path> paths = Files.walk(baseDir.toPath())) {
			paths.filter(p -> !Files.isDirectory(p))
					.filter(p -> {
						Path rel = baseDir.toPath().relativize(p);
						return matcher.matches(rel) || matcher.matches(p.getFileName());
					})
					.limit(MAX_GLOB_RESULTS)
					.forEach(matches::add);
		}
		if (matches.isEmpty()) {
			return "No files matched pattern: " + pattern;
		}
		matches.sort(Comparator.comparingLong(p -> {
			try {
				return -Files.getLastModifiedTime(p).toMillis();
			} catch (Exception e) {
				return 0L;
			}
		}));
		List<String> results = new ArrayList<>();
		for (Path p : matches) {
			results.add(tc.toRelative(p.toAbsolutePath().toString()));
		}
		return String.join("\n", results);
	}

	private static String grepFiles(Map<String, Object> params, ToolContext tc) throws Exception {
		String patternStr = stringParam(params, "pattern");
		String basePath = stringParam(params, "path");
		String glob = stringParam(params, "glob");
		String outputMode = stringParam(params, "output_mode");
		if (outputMode == null || outputMode.trim().isEmpty()) {
			outputMode = "files_with_matches";
		}
		int after = parseIntOr(params.get("after_context"), 0);
		int before = parseIntOr(params.get("before_context"), 0);
		int context = parseIntOr(params.get("context"), 0);
		if (context > 0) {
			after = context;
			before = context;
		}
		int headLimit = parseIntOr(params.get("head_limit"), DEFAULT_GREP_HEAD_LIMIT);
		if (headLimit <= 0) {
			headLimit = DEFAULT_GREP_HEAD_LIMIT;
		}
		int flags = parseBoolean(params.get("case_insensitive")) ? Pattern.CASE_INSENSITIVE : 0;
		Pattern regex;
		try {
			regex = Pattern.compile(patternStr, flags);
		} catch (PatternSyntaxException e) {
			return "Error: invalid regex pattern: " + e.getMessage();
		}
		File baseDir = tc.resolve(basePath);
		if (!baseDir.exists()) {
			return "Error: path not found: " + (basePath != null ? basePath : ".");
		}
		PathMatcher fileMatcher = (glob != null && !glob.trim().isEmpty())
				? FileSystems.getDefault().getPathMatcher("glob:" + glob.trim())
				: null;
		boolean isFilesMode = "files_with_matches".equals(outputMode);
		boolean isCountMode = "count".equals(outputMode);
		List<String> results = new ArrayList<>();
		List<Path> paths = new ArrayList<>();
		try (Stream<Path> walk = Files.walk(baseDir.toPath())) {
			walk.filter(p -> !Files.isDirectory(p))
					.filter(p -> fileMatcher == null || fileMatcher.matches(p.getFileName())
							|| fileMatcher.matches(baseDir.toPath().relativize(p)))
					.sorted()
					.forEach(paths::add);
		}
		for (Path p : paths) {
			if (results.size() >= headLimit) {
				break;
			}
			try {
				String content = new String(Files.readAllBytes(p), StandardCharsets.UTF_8);
				String[] lines = content.split("\n", -1);
				String relPath = tc.toRelative(p.toAbsolutePath().toString());
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
						if (regex.matcher(line).find()) {
							count++;
						}
					}
					if (count > 0) {
						results.add(relPath + ":" + count);
					}
				} else {
					addContentMatches(results, regex, lines, relPath, before, after, headLimit);
				}
			} catch (Exception ignored) {
				// skip unreadable files
			}
		}
		return results.isEmpty() ? "No matches found for: " + patternStr : String.join("\n", results);
	}

	private static String listDirectory(Map<String, Object> params, ToolContext tc) {
		String path = stringParam(params, "path");
		File dir = tc.resolve(path);
		if (!dir.exists()) {
			return "Error: directory not found: " + (path != null ? path : ".");
		}
		if (!dir.isDirectory()) {
			return "Error: not a directory: " + path;
		}
		File[] files = dir.listFiles();
		if (files == null || files.length == 0) {
			return "(empty directory)";
		}
		Arrays.sort(files, (a, b) -> {
			if (a.isDirectory() != b.isDirectory()) {
				return a.isDirectory() ? -1 : 1;
			}
			return a.getName().compareToIgnoreCase(b.getName());
		});
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		StringBuilder sb = new StringBuilder();
		for (File f : files) {
			String type = f.isDirectory() ? "DIR " : "FILE";
			String size = f.isDirectory() ? "         " : String.format("%9d", f.length());
			sb.append(String.format("%s  %s  %s  %s%n", type,
					sdf.format(new java.util.Date(f.lastModified())), size, f.getName()));
		}
		return sb.toString().trim();
	}

	private static String bashCommand(Map<String, Object> params, ToolContext tc) {
		if (!isBashEnabled()) {
			return "Error: BashCommand is disabled. Enable CHROOT_ENABLE or "
					+ PROP_ENABLE_BASH + " to use it.";
		}
		String command = stringParam(params, "command");
		String description = stringParam(params, "description");
		if (command == null || command.trim().isEmpty()) {
			return "Error: command is required";
		}
		command = command.trim();
		if (command.length() > MAX_COMMAND_LENGTH) {
			return "Error: command exceeds maximum length of " + MAX_COMMAND_LENGTH;
		}
		String validationError = validateCommand(command);
		if (validationError != null) {
			return "Error: " + validationError;
		}
		if (description != null && !description.trim().isEmpty()) {
			logger.info("[BashCommand] [{}] executing: {}", description, command);
		} else {
			logger.info("[BashCommand] executing: {}", command);
		}
		CmdExecUtil cmdUtil = tc.ctx.getInsight().getCmdUtil();
		if (cmdUtil == null) {
			cmdUtil = new CmdExecUtil(tc.ctx.getInsight().getUser(), tc.ctx.getInsight().getInsightId(), tc.root);
		}
		if (!isWithinRoot(normalizePath(cmdUtil.getWorkingDir()), tc.root)) {
			cmdUtil.setWorkingDir(tc.root);
		}
		String output = cmdUtil.executeCommand(command);
		String updatedDir = normalizePath(cmdUtil.getWorkingDir());
		if (!isWithinRoot(updatedDir, tc.root)) {
			cmdUtil.setWorkingDir(tc.root);
			throw new IllegalArgumentException("Command attempted to navigate outside the working directory sandbox.");
		}
		return output == null ? "" : output;
	}

	private static String todoWrite(Map<String, Object> params, ToolContext tc) {
		String itemsJson = stringParam(params, "items_json");
		if (itemsJson == null || itemsJson.trim().isEmpty()) {
			return "Error: items_json is required";
		}
		JSONArray items;
		try {
			items = new JSONArray(itemsJson.trim());
		} catch (Exception e) {
			return "Error: items_json must be a valid JSON array - " + e.getMessage();
		}
		if (items.length() > MAX_TODO_ITEMS) {
			return "Error: too many items (" + items.length() + " > " + MAX_TODO_ITEMS + ")";
		}
		Set<String> seenIds = new HashSet<>();
		int pending = 0;
		int inProgress = 0;
		int completed = 0;
		JSONArray validated = new JSONArray();
		for (int i = 0; i < items.length(); i++) {
			JSONObject raw;
			try {
				raw = items.getJSONObject(i);
			} catch (Exception e) {
				return "Error: items[" + i + "] is not an object";
			}
			String id = optTrimmed(raw, "id");
			String content = optTrimmed(raw, "content");
			String status = optTrimmed(raw, "status");
			String priority = optTrimmed(raw, "priority");
			String error = validateTodoItem(i, id, content, status, priority, seenIds);
			if (error != null) {
				return error;
			}
			if ("pending".equals(status)) {
				pending++;
			} else if ("in_progress".equals(status)) {
				inProgress++;
			} else if ("completed".equals(status)) {
				completed++;
			}
			JSONObject clean = new JSONObject();
			clean.put("id", id);
			clean.put("content", content);
			clean.put("status", status);
			if (priority != null && !priority.isEmpty()) {
				clean.put("priority", priority);
			}
			validated.put(clean);
		}
		JSONObject doc = new JSONObject();
		doc.put("updated_at", Instant.now().toString());
		doc.put("items", validated);
		saveTextFile(tc.resolve(TODOS_FILE), doc.toString(2), tc);
		return String.format("Wrote %d todo(s): %d pending, %d in_progress, %d completed",
				validated.length(), pending, inProgress, completed);
	}

	private static String todoRead(Map<String, Object> params, ToolContext tc) throws Exception {
		File file = tc.resolve(TODOS_FILE);
		if (!file.exists() || !file.isFile()) {
			JSONObject empty = new JSONObject();
			empty.put("updated_at", JSONObject.NULL);
			empty.put("items", new JSONArray());
			return empty.toString(2);
		}
		String body = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
		try {
			new JSONObject(body);
		} catch (Exception e) {
			return "Warning: todos.json is corrupt - raw content follows.\n\n" + body;
		}
		return body;
	}

	private static String listSkill(Map<String, Object> params, ToolContext tc) {
		List<DiscoveredSkill> skills = SkillScanner.scan(tc.root);
		if (skills.isEmpty()) {
			return "No skills found.\n\nChecked:\n- " + String.join("\n- ", SkillScanner.candidateHostPaths());
		}
		StringBuilder out = new StringBuilder();
		out.append("Found ").append(skills.size()).append(" skill").append(skills.size() == 1 ? "" : "s")
				.append(":\n\n");
		for (DiscoveredSkill skill : skills) {
			out.append("- **").append(skill.getName()).append("** - `").append(skill.getPath()).append("`");
			if (skill.getDescription() != null && !skill.getDescription().isEmpty()) {
				out.append("\n  ").append(skill.getDescription());
			}
			out.append('\n');
		}
		out.append("\nLoad a skill's body with `LoadSkill(skill_name=\"<name>\")`.");
		return out.toString();
	}

	private static String loadSkill(Map<String, Object> params, ToolContext tc) throws Exception {
		String raw = stringParam(params, "skill_name");
		if (raw == null || raw.trim().isEmpty()) {
			return "Error: skill_name is required";
		}
		String name = raw.trim();
		if (name.contains("/") || name.contains("\\") || name.contains("..")) {
			return "Error: invalid skill_name (must be a single folder name with no slashes or '..'): " + name;
		}
		long offset = parseLongAtLeast(params.get("offset"), 0L, 0L);
		int maxBytes = parseIntAtLeast(params.get("max_bytes"), DEFAULT_SKILL_MAX_BYTES, 1);
		if (maxBytes > HARD_SKILL_MAX_BYTES) {
			maxBytes = HARD_SKILL_MAX_BYTES;
		}
		File skillFile = null;
		List<String> attempted = new ArrayList<>();
		for (String baseDir : SkillScanner.SKILL_BASE_DIRS) {
			for (String hostDir : SkillScanner.SKILL_HOST_DIRS) {
				String candidatePath = joinSkillPath(baseDir, hostDir, name);
				attempted.add(candidatePath);
				File candidate = tc.resolve(candidatePath);
				if (candidate.isFile()) {
					skillFile = candidate;
					break;
				}
			}
			if (skillFile != null) {
				break;
			}
		}
		if (skillFile == null) {
			return "Error: skill not found: " + name + " (checked: " + String.join(", ", attempted) + ")";
		}
		return readSkillChunk(skillFile, name, offset, maxBytes);
	}

	private static void saveTextFile(File file, String content, ToolContext tc) {
		if (content == null) {
			content = "";
		}
		User user = tc.ctx.getInsight().getUser();
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user != null && user.isAnonymous()) {
			throw new IllegalArgumentException("Anonymous users are not allowed to save files");
		}
		String relativePath = tc.toRelative(file.getAbsolutePath());
		if (".".equals(relativePath) || relativePath.startsWith("/")) {
			throw new IllegalArgumentException("A file path is required");
		}
		List<String> filePaths = Collections.singletonList(relativePath);
		boolean strictScriptSource = Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.STRICT_SCRIPT_SOURCE));
		FileSystemUtil.validateAssetFiles(filePaths, strictScriptSource);
		String encodedContent = Base64.getEncoder().encodeToString(content.getBytes(StandardCharsets.UTF_8));
		FileSystemUtil.saveAssetFilesBase64(tc.root, filePaths, Collections.singletonList(encodedContent), true);
		pushRoom(tc);
	}

	private static void pushRoom(ToolContext tc) {
		if (tc.ctx.getInsight() != null && tc.ctx.getInsight().getRoomId() != null) {
			ClusterUtil.pushRoomAsync(tc.ctx.getInsight().getRoomId());
		}
	}

	private static void addContentMatches(List<String> results, Pattern regex, String[] lines, String relPath,
			int before, int after, int headLimit) {
		Set<Integer> printed = new HashSet<>();
		for (int i = 0; i < lines.length && results.size() < headLimit; i++) {
			if (!regex.matcher(lines[i]).find()) {
				continue;
			}
			int startLine = Math.max(0, i - before);
			int endLine = Math.min(lines.length - 1, i + after);
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

	private static String readSkillChunk(File skillFile, String name, long offset, int maxBytes) throws Exception {
		long size = Files.size(skillFile.toPath());
		if (offset >= size) {
			return "[empty: offset " + offset + " is at or past end-of-file (" + size + " bytes total)]";
		}
		long remaining = size - offset;
		int toRead = (int) Math.min(remaining, (long) maxBytes);
		byte[] bytes = new byte[toRead];
		try (RandomAccessFile raf = new RandomAccessFile(skillFile, "r")) {
			raf.seek(offset);
			raf.readFully(bytes);
		}
		long endOffset = offset + toRead;
		if (endOffset < size) {
			int lastNewline = -1;
			for (int i = bytes.length - 1; i >= 0; i--) {
				if (bytes[i] == (byte) '\n') {
					lastNewline = i;
					break;
				}
			}
			if (lastNewline >= 0 && lastNewline >= bytes.length / 2) {
				int newLen = lastNewline + 1;
				byte[] trimmed = new byte[newLen];
				System.arraycopy(bytes, 0, trimmed, 0, newLen);
				bytes = trimmed;
				endOffset = offset + newLen;
			}
		}
		StringBuilder body = new StringBuilder(new String(bytes, StandardCharsets.UTF_8));
		long bytesRemaining = size - endOffset;
		if (bytesRemaining > 0) {
			body.append("\n\n[--- skill continues: bytes ").append(offset).append('-').append(endOffset - 1)
					.append(" of ").append(size).append("; ").append(bytesRemaining)
					.append(" bytes remaining. To read more call LoadSkill(skill_name=\"").append(name)
					.append("\", offset=").append(endOffset).append("). ---]");
		} else if (offset > 0) {
			body.append("\n\n[--- end of skill: bytes ").append(offset).append('-').append(endOffset - 1)
					.append(" of ").append(size).append(" (final chunk). ---]");
		}
		return body.toString();
	}

	private static String validateTodoItem(int index, String id, String content, String status, String priority,
			Set<String> seenIds) {
		if (id == null || id.isEmpty()) {
			return "Error: items[" + index + "].id is required";
		}
		if (id.length() > MAX_TODO_ID_LEN) {
			return "Error: items[" + index + "].id too long (max " + MAX_TODO_ID_LEN + ")";
		}
		if (!seenIds.add(id)) {
			return "Error: duplicate id '" + id + "' at items[" + index + "]";
		}
		if (content == null || content.isEmpty()) {
			return "Error: items[" + index + "].content is required";
		}
		if (content.length() > MAX_TODO_CONTENT_LEN) {
			return "Error: items[" + index + "].content too long (max " + MAX_TODO_CONTENT_LEN + ")";
		}
		if (status == null || !VALID_TODO_STATUSES.contains(status)) {
			return "Error: items[" + index + "].status must be one of " + VALID_TODO_STATUSES
					+ " (got '" + status + "')";
		}
		if (priority != null && !priority.isEmpty() && !VALID_TODO_PRIORITIES.contains(priority)) {
			return "Error: items[" + index + "].priority must be one of " + VALID_TODO_PRIORITIES
					+ " or omitted (got '" + priority + "')";
		}
		return null;
	}

	private static String validateCommand(String command) {
		if (containsUnquoted(command, '>') || containsUnquoted(command, '<')) {
			return "Redirects (>, <, >>) are not allowed. Use curl/wget -o to write files.";
		}
		if (containsUnquoted(command, '|') || containsUnquoted(command, ';')
				|| containsUnquotedSequence(command, "&&") || containsUnquotedSequence(command, "||")) {
			return "Command chaining and pipes are not allowed. Run one command per BashCommand call.";
		}
		if (command.contains("`")) {
			return "Backtick command substitution is not allowed.";
		}
		if (command.contains("$(")) {
			return "Command substitution $() is not allowed.";
		}
		for (String token : tokenize(command)) {
			String clean = stripQuotes(token);
			if (clean.startsWith("/") || clean.startsWith("~")) {
				return "Absolute paths and home-directory paths are not allowed: " + clean;
			}
			if (clean.contains("..")) {
				return "Parent directory traversal (..) is not allowed: " + clean;
			}
		}
		String[] parts = command.trim().split("\\s+");
		if (parts.length > 0) {
			String cmd = stripQuotes(parts[0]);
			if (!cmd.isEmpty() && !ALLOWED_COMMANDS.contains(cmd)) {
				return "Command not allowed: " + cmd;
			}
		}
		return null;
	}

	private static boolean isBashEnabled() {
		if (isTrue(Constants.DISABLE_TERMINAL)) {
			return false;
		}
		String explicit = getTrimmedProperty(PROP_ENABLE_BASH);
		if (explicit != null) {
			return Boolean.parseBoolean(explicit);
		}
		return isTrue(Constants.CHROOT_ENABLE);
	}

	private static boolean isTrue(String property) {
		String value = getTrimmedProperty(property);
		return value != null && Boolean.parseBoolean(value);
	}

	private static String getTrimmedProperty(String property) {
		String value = Utility.getDIHelperProperty(property);
		if (value == null || value.trim().isEmpty()) {
			return null;
		}
		return value.trim();
	}

	private static boolean containsUnquoted(String s, char ch) {
		boolean inSingle = false;
		boolean inDouble = false;
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			if (c == '\'' && !inDouble) {
				inSingle = !inSingle;
			} else if (c == '"' && !inSingle) {
				inDouble = !inDouble;
			} else if (c == ch && !inSingle && !inDouble) {
				return true;
			}
		}
		return false;
	}

	private static boolean containsUnquotedSequence(String s, String sequence) {
		boolean inSingle = false;
		boolean inDouble = false;
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			if (c == '\'' && !inDouble) {
				inSingle = !inSingle;
			} else if (c == '"' && !inSingle) {
				inDouble = !inDouble;
			} else if (!inSingle && !inDouble && s.startsWith(sequence, i)) {
				return true;
			}
		}
		return false;
	}

	private static List<String> tokenize(String command) {
		List<String> tokens = new ArrayList<>();
		StringBuilder current = new StringBuilder();
		boolean inSingle = false;
		boolean inDouble = false;
		for (int i = 0; i < command.length(); i++) {
			char c = command.charAt(i);
			if (c == '\'' && !inDouble) {
				inSingle = !inSingle;
				current.append(c);
			} else if (c == '"' && !inSingle) {
				inDouble = !inDouble;
				current.append(c);
			} else if (Character.isWhitespace(c) && !inSingle && !inDouble) {
				if (current.length() > 0) {
					tokens.add(current.toString());
					current.setLength(0);
				}
			} else {
				current.append(c);
			}
		}
		if (current.length() > 0) {
			tokens.add(current.toString());
		}
		return tokens;
	}

	private static String stripQuotes(String s) {
		if (s == null || s.length() < 2) {
			return s;
		}
		if ((s.startsWith("\"") && s.endsWith("\"")) || (s.startsWith("'") && s.endsWith("'"))) {
			return s.substring(1, s.length() - 1);
		}
		return s;
	}

	private static void deleteTree(Path root) throws Exception {
		if (Files.isDirectory(root)) {
			try (java.util.stream.Stream<Path> walk = Files.walk(root)) {
				List<Path> paths = walk.sorted(Comparator.reverseOrder()).toList();
				for (Path path : paths) {
					Files.deleteIfExists(path);
				}
			}
			return;
		}
		Files.deleteIfExists(root);
	}

	private static boolean isWithinRoot(String path, String root) {
		if (root == null || root.isEmpty()) {
			return true;
		}
		if (path.equals(root)) {
			return true;
		}
		String prefix = root.endsWith("/") ? root : root + "/";
		return path.startsWith(prefix);
	}

	private static int countOccurrences(String text, String target) {
		int count = 0;
		int idx = 0;
		while ((idx = text.indexOf(target, idx)) != -1) {
			count++;
			idx += target.length();
		}
		return count;
	}

	private static String stringParam(Map<String, Object> params, String key) {
		Object value = params.get(key);
		return value == null ? null : value.toString();
	}

	private static String firstStringParam(Map<String, Object> params, String first, String second) {
		String value = stringParam(params, first);
		return value != null ? value : stringParam(params, second);
	}

	private static boolean parseBoolean(Object value) {
		return value != null && "true".equalsIgnoreCase(value.toString().trim());
	}

	private static int parseIntOr(Object value, int defaultValue) {
		Long parsed = parseIntegralLong(value);
		if (parsed == null || parsed < Integer.MIN_VALUE || parsed > Integer.MAX_VALUE) {
			return defaultValue;
		}
		return parsed.intValue();
	}

	private static long parseLongAtLeast(Object value, long defaultValue, long minInclusive) {
		Long parsed = parseIntegralLong(value);
		return parsed != null && parsed >= minInclusive ? parsed : defaultValue;
	}

	private static int parseIntAtLeast(Object value, int defaultValue, int minInclusive) {
		int parsed = parseIntOr(value, defaultValue);
		return parsed >= minInclusive ? parsed : defaultValue;
	}

	private static Long parseIntegralLong(Object value) {
		if (value == null) {
			return null;
		}
		String text = value.toString().trim();
		if (text.isEmpty()) {
			return null;
		}
		try {
			if (value instanceof Number) {
				return new BigDecimal(text).longValueExact();
			}
			return Long.parseLong(text);
		} catch (NumberFormatException | ArithmeticException ignored) {
			return null;
		}
	}

	private static String optString(JSONObject obj, String key) {
		if (!obj.has(key) || obj.isNull(key)) {
			return null;
		}
		Object value = obj.get(key);
		return value == null ? null : value.toString();
	}

	private static String optTrimmed(JSONObject obj, String key) {
		String value = optString(obj, key);
		return value == null ? null : value.trim();
	}

	private static String joinSkillPath(String baseDir, String hostDir, String skillName) {
		String suffix = hostDir + "/" + skillName + "/SKILL.md";
		if (baseDir == null || baseDir.isEmpty()) {
			return suffix;
		}
		return baseDir + "/" + suffix;
	}

	private static String normalizePath(String path) {
		if (path == null) {
			return "";
		}
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

	@SafeVarargs
	private static Map<String, JSONObject> props(Map.Entry<String, JSONObject>... entries) {
		Map<String, JSONObject> props = new LinkedHashMap<>();
		for (Map.Entry<String, JSONObject> entry : entries) {
			props.put(entry.getKey(), entry.getValue());
		}
		return props;
	}

	private static Map.Entry<String, JSONObject> prop(String name, JSONObject schema) {
		return Map.entry(name, schema);
	}

	private static JSONObject objectSchema(Map<String, JSONObject> properties, List<String> required) {
		JSONObject schema = new JSONObject();
		schema.put("type", "object");
		schema.put("properties", properties);
		schema.put("required", required);
		return schema;
	}

	private static JSONObject stringProp(String description) {
		JSONObject prop = new JSONObject();
		prop.put("type", "string");
		prop.put("description", description);
		return prop;
	}

	private static JSONObject integerProp(String description) {
		JSONObject prop = new JSONObject();
		prop.put("type", "integer");
		prop.put("description", description);
		return prop;
	}

	private static JSONObject booleanProp(String description) {
		JSONObject prop = new JSONObject();
		prop.put("type", "boolean");
		prop.put("description", description);
		return prop;
	}

	private interface ToolExecutor {
		String execute(Map<String, Object> params, ToolContext ctx) throws Exception;
	}

	private static final class ToolContext {
		private final AgentRunContext ctx;
		private final String root;

		private ToolContext(AgentRunContext ctx, String root) {
			this.ctx = ctx;
			this.root = root;
		}

		private File resolve(String relativePath) {
			if (relativePath == null || relativePath.trim().isEmpty()) {
				return new File(root);
			}
			String clean = relativePath.trim();
			if (new File(clean).isAbsolute()) {
				throw new IllegalArgumentException("Absolute paths are not allowed: " + clean);
			}
			File resolved = new File(root, clean);
			String normalizedResolved = normalizePath(resolved.getAbsolutePath());
			if (!normalizedResolved.equals(root) && !normalizedResolved.startsWith(root + "/")) {
				throw new IllegalArgumentException("Path escapes the working directory: " + clean);
			}
			return resolved;
		}

		private String toRelative(String absolutePath) {
			String normalized = normalizePath(absolutePath);
			if (normalized.startsWith(root + "/")) {
				return normalized.substring(root.length() + 1);
			}
			if (normalized.equals(root)) {
				return ".";
			}
			return normalized;
		}
	}
}
