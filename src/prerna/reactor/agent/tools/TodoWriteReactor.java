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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Manages a structured task list (mirrors Claude Code's TodoWrite) for the current agent.
 *
 * <p>The list is persisted to {@code .agent/todos.json} inside the working directory so the agent
 * can re-read it across turns and so a human can inspect progress. Each todo has:
 * <ul>
 *   <li>{@code content} — imperative form, e.g. "Run tests"</li>
 *   <li>{@code activeForm} — present continuous form, e.g. "Running tests"</li>
 *   <li>{@code status} — one of {@code pending}, {@code in_progress}, {@code completed}</li>
 * </ul>
 *
 * <p>Conventions enforced:
 * <ul>
 *   <li>At most one task may be {@code in_progress} at a time</li>
 *   <li>Status must be one of the three allowed values</li>
 *   <li>An empty list clears the todo file</li>
 * </ul>
 *
 * <p>The reactor returns a human-readable rendering of the current list with status icons.
 */
public class TodoWriteReactor extends AbstractAgentToolReactor {

    private static final String STORE_DIR  = ".agent";
    private static final String STORE_FILE = "todos.json";

    private static final Set<String> ALLOWED_STATUS = new HashSet<>(Arrays.asList(
            "pending", "in_progress", "completed"));

    public TodoWriteReactor() {
        this.keysToGet   = new String[] { "todos" };
        this.keyRequired = new int[]    { 1       };
    }

    @Override
    protected NounMetadata doExecute() throws Exception {
        @SuppressWarnings("rawtypes")
        List rawTodos = getList("todos");
        if (rawTodos == null) {
            return new NounMetadata(
                    "Error: todos must be a list of {content, activeForm, status} objects (pass an empty list to clear)",
                    PixelDataType.CONST_STRING);
        }

        List<Map<String, String>> normalized = new ArrayList<>();
        int inProgressCount = 0;
        for (int i = 0; i < rawTodos.size(); i++) {
            Object item = rawTodos.get(i);
            if (!(item instanceof Map)) {
                return new NounMetadata(
                        "Error: todos[" + i + "] is not an object",
                        PixelDataType.CONST_STRING);
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> td = (Map<String, Object>) item;

            String content    = strOrNull(td.get("content"));
            String activeForm = strOrNull(td.get("activeForm"));
            String status     = strOrNull(td.get("status"));

            if (content == null || content.isEmpty()) {
                return new NounMetadata(
                        "Error: todos[" + i + "].content is required",
                        PixelDataType.CONST_STRING);
            }
            if (activeForm == null || activeForm.isEmpty()) {
                return new NounMetadata(
                        "Error: todos[" + i + "].activeForm is required",
                        PixelDataType.CONST_STRING);
            }
            if (status == null || !ALLOWED_STATUS.contains(status)) {
                return new NounMetadata(
                        "Error: todos[" + i + "].status must be one of "
                                + ALLOWED_STATUS + " (got: " + status + ")",
                        PixelDataType.CONST_STRING);
            }
            if ("in_progress".equals(status) && ++inProgressCount > 1) {
                return new NounMetadata(
                        "Error: at most one todo may be in_progress at a time",
                        PixelDataType.CONST_STRING);
            }

            Map<String, String> entry = new LinkedHashMap<>();
            entry.put("content", content);
            entry.put("activeForm", activeForm);
            entry.put("status", status);
            normalized.add(entry);
        }

        // Persist to <insightFolder>/.agent/todos.json so the agent can re-read it.
        File storeDir  = resolveAndValidate(STORE_DIR);
        if (!storeDir.exists() && !storeDir.mkdirs()) {
            return new NounMetadata(
                    "Error: failed to create todo store directory: " + STORE_DIR,
                    PixelDataType.CONST_STRING);
        }
        Path storePath = new File(storeDir, STORE_FILE).toPath();
        Files.write(storePath, serialize(normalized).getBytes(StandardCharsets.UTF_8));

        if (normalized.isEmpty()) {
            return new NounMetadata("Todo list cleared.", PixelDataType.CONST_STRING);
        }
        return new NounMetadata(render(normalized), PixelDataType.CONST_STRING);
    }

    private static String strOrNull(Object o) {
        if (o == null) return null;
        String s = String.valueOf(o).trim();
        return s.isEmpty() ? null : s;
    }

    private static String render(List<Map<String, String>> todos) {
        StringBuilder sb = new StringBuilder();
        sb.append("Todo list (").append(todos.size()).append("):\n");
        for (int i = 0; i < todos.size(); i++) {
            Map<String, String> t = todos.get(i);
            String icon;
            switch (t.get("status")) {
                case "completed":  icon = "[x]"; break;
                case "in_progress": icon = "[~]"; break;
                default:           icon = "[ ]"; break;
            }
            String label = "in_progress".equals(t.get("status")) ? t.get("activeForm") : t.get("content");
            sb.append(i + 1).append(". ").append(icon).append(' ').append(label).append('\n');
        }
        return sb.toString().trim();
    }

    /** Minimal JSON array serializer for the {content, activeForm, status} shape. */
    private static String serialize(List<Map<String, String>> todos) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (int i = 0; i < todos.size(); i++) {
            Map<String, String> t = todos.get(i);
            if (i > 0) sb.append(',');
            sb.append("\n  {");
            sb.append("\"content\":\"").append(escape(t.get("content"))).append('"');
            sb.append(",\"activeForm\":\"").append(escape(t.get("activeForm"))).append('"');
            sb.append(",\"status\":\"").append(escape(t.get("status"))).append('"');
            sb.append('}');
        }
        if (!todos.isEmpty()) sb.append('\n');
        sb.append(']');
        sb.append('\n');
        return sb.toString();
    }

    private static String escape(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }

    @Override
    protected String getDescriptionForKey(String key) {
        if ("todos".equals(key)) {
            return "Complete replacement list of todo items. Each item must have: content (imperative, "
                 + "e.g. 'Run tests'), activeForm (present-continuous, e.g. 'Running tests'), and "
                 + "status (pending|in_progress|completed). At most one item may be in_progress. "
                 + "Pass an empty list to clear.";
        }
        return super.getDescriptionForKey(key);
    }

    @Override
    public String getReactorDescription() {
        return "Manages the agent's structured todo list (content, activeForm, status). "
             + "Persists to .agent/todos.json in the working directory. Enforces at most one "
             + "in_progress task. Status must be pending|in_progress|completed. Pass an empty "
             + "list to clear.";
    }

    @Override
    protected MCP_KEY_TYPE getKeyTypeForMCP(String key) {
        if ("todos".equals(key)) {
            return MCP_KEY_TYPE.ARRAY;
        }
        return super.getKeyTypeForMCP(key);
    }
}
