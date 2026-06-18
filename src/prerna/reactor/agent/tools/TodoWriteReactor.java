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
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONObject;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Replaces the room's todo list with a new list. Monolithic full-state write, no incremental ops.
 * Persists as {@code todos.json} in the working directory.
 *
 * <p>Item schema: {@code { id, content, status, priority? }}.
 * Status: {@code pending | in_progress | completed}.
 * Priority (optional): {@code high | medium | low}.
 */
public class TodoWriteReactor extends AbstractAgentToolReactor {

    private static final String TODOS_FILE = "todos.json";
    private static final Set<String> VALID_STATUSES   = Set.of("pending", "in_progress", "completed");
    private static final Set<String> VALID_PRIORITIES = Set.of("high", "medium", "low");
    private static final int MAX_CONTENT_LEN = 2000;
    private static final int MAX_ID_LEN      = 64;
    private static final int MAX_ITEMS       = 200;

    public TodoWriteReactor() {
        this.keysToGet  = new String[]{"items_json"};
        this.keyRequired = new int[]{1};
    }

    @Override
    protected NounMetadata doExecute() throws Exception {
        String itemsJson = this.keyValue.get("items_json");
        if (itemsJson == null || itemsJson.trim().isEmpty()) {
            return err("items_json is required");
        }

        // parse - fail fast with a clear message if not a JSON array
        JSONArray items;
        try {
            items = new JSONArray(itemsJson.trim());
        } catch (Exception e) {
            return err("items_json must be a valid JSON array - " + e.getMessage());
        }

        if (items.length() > MAX_ITEMS) {
            return err("too many items (" + items.length() + " > " + MAX_ITEMS + ")");
        }

        // validate every item BEFORE writing anything - atomic semantics
        Set<String> seenIds = new HashSet<>();
        int pending = 0, inProgress = 0, completed = 0;
        JSONArray validated = new JSONArray();

        for (int i = 0; i < items.length(); i++) {
            JSONObject raw;
            try {
                raw = items.getJSONObject(i);
            } catch (Exception e) {
                return err("items[" + i + "] is not an object");
            }

            String id       = optTrimmed(raw, "id");
            String content  = optTrimmed(raw, "content");
            String status   = optTrimmed(raw, "status");
            String priority = optTrimmed(raw, "priority"); // optional

            if (id == null || id.isEmpty()) {
                return err("items[" + i + "].id is required");
            }
            if (id.length() > MAX_ID_LEN) {
                return err("items[" + i + "].id too long (max " + MAX_ID_LEN + ")");
            }
            if (!seenIds.add(id)) {
                return err("duplicate id '" + id + "' at items[" + i + "]");
            }
            if (content == null || content.isEmpty()) {
                return err("items[" + i + "].content is required");
            }
            if (content.length() > MAX_CONTENT_LEN) {
                return err("items[" + i + "].content too long (max " + MAX_CONTENT_LEN + ")");
            }
            if (status == null || !VALID_STATUSES.contains(status)) {
                return err("items[" + i + "].status must be one of " + VALID_STATUSES
                        + " (got '" + status + "')");
            }
            if (priority != null && !priority.isEmpty() && !VALID_PRIORITIES.contains(priority)) {
                return err("items[" + i + "].priority must be one of " + VALID_PRIORITIES
                        + " or omitted (got '" + priority + "')");
            }

            switch (status) {
                case "pending":     pending++;    break;
                case "in_progress": inProgress++; break;
                case "completed":   completed++;  break;
            }

            // re-emit cleanly so we never write garbage / unknown fields back
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

        File target = resolveAndValidate(TODOS_FILE);
        saveTextFileWithInsightAssetsBase64(target, doc.toString(2));

        String summary = String.format(
                "Wrote %d todo(s): %d pending, %d in_progress, %d completed",
                validated.length(), pending, inProgress, completed);
        return new NounMetadata(summary, PixelDataType.CONST_STRING);
    }

    /** Returns a trimmed string for {@code key}, or {@code null} if missing / JSON null. */
    private String optTrimmed(JSONObject obj, String key) {
        if (!obj.has(key) || obj.isNull(key)) return null;
        Object v = obj.get(key);
        return v == null ? null : v.toString().trim();
    }

    private NounMetadata err(String msg) {
        return new NounMetadata("Error: " + msg, PixelDataType.CONST_STRING);
    }

    @Override
    public String getReactorDescription() {
        return "Replaces the room's todo list with a new list. Atomic monolithic write - pass "
             + "the complete current state every call. Persists as todos.json in the working "
             + "directory. Pair with TodoRead.";
    }
}
