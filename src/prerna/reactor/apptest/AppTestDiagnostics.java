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
package prerna.reactor.apptest;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** Bounded, sanitized preview failures. Never retains request bodies, credentials or SQL values. */
final class AppTestDiagnostics {
    private static final Pattern OPERATION = Pattern.compile("\\s*(?:META\\s*\\|\\s*)?([A-Za-z][A-Za-z0-9_]{0,63})", Pattern.CASE_INSENSITIVE);
    private static final Pattern QUERY = Pattern.compile("(?i)query\\s*=\\s*(?:\\[\\s*(" + RegressionPixelAccess.STRING + ")\\s*]|(" + RegressionPixelAccess.STRING + "))");
    private final List<Map<String, Object>> failures = new ArrayList<>();
    private String phase = "STARTUP";

    synchronized void phase(String phase) { this.phase = phase; }
    synchronized List<Map<String, Object>> snapshot() { return List.copyOf(failures); }
    synchronized String reason() { return failures.isEmpty() ? null : (String)failures.getFirst().get("reason"); }
    synchronized String code() { return failures.isEmpty() ? null : (String)failures.getFirst().get("code"); }

    synchronized void request(String method, String url, String resource, String code) {
        String path = path(url);
        String detail = switch (code) {
            case "TEST_PREVIEW_ASSET_MISSING" -> "Asset is absent from the test snapshot. Check the build and relative asset paths.";
            case "TEST_PREVIEW_EXTERNAL_REQUEST" -> "Request is outside this preview. Bundle dependencies locally or use supported preview APIs.";
            case "TEST_PREVIEW_CSP_BLOCKED" -> "The preview content policy blocked this resource.";
            case "TEST_PREVIEW_STATIC_ASSET_FAILED" -> "The platform static asset could not load. Check that its normal URL serves the expected file without authentication.";
            case "TEST_PREVIEW_STATIC_REDIRECT_BLOCKED" -> "The platform asset URL redirected; redirects are not followed into ordinary APIs or sign-in flows.";
            case "TEST_PREVIEW_REQUEST_TOO_LARGE" -> "SDK request exceeds the preview size limit.";
            case "TEST_PREVIEW_REQUEST_INVALID" -> "Malformed preview request. Use the SDK URL-encoded request format.";
            default -> "This endpoint or request method is unavailable in the test preview.";
        };
        add(code, "TEST_PREVIEW_ASSET_MISSING".equals(code) ? "ASSET" : "REQUEST", Map.of(
                "method", method != null && method.matches("[A-Z]{1,12}") ? method : "REQUEST",
                "path", path, "resourceType", resource != null && resource.matches("[A-Za-z-]{1,32}") ? resource : "unknown"),
                (method != null && method.matches("[A-Z]{1,12}") ? method : "REQUEST") + " " + path + ": " + detail);
    }

    synchronized void pixel(String pixel, String detail, boolean sqlError) {
        var operation = OPERATION.matcher(pixel == null ? "" : pixel);
        String name = operation.find() ? operation.group(1) : "unknown";
        var query = QUERY.matcher(pixel == null ? "" : pixel);
        Map<String,Object> fields = new LinkedHashMap<>();
        fields.put("operation", name);
        if (name.equalsIgnoreCase("SqlQuery") && query.find()) {
            try { fields.put("sqlShape", RegressionSql.shape(com.google.gson.JsonParser.parseString(query.group(1) == null ? query.group(2) : query.group(1)).getAsString())); }
            catch (RuntimeException ignored) { fields.put("sqlShape", "[unrecognized SQL; values omitted]"); }
        }
        String code = sqlError ? "TEST_DATABASE_SQL_ERROR"
                : detail != null && detail.startsWith("UNMAPPED_TEST_DATABASE") ? "UNMAPPED_TEST_DATABASE"
                : fields.containsKey("sqlShape") ? "TEST_DATABASE_SQL_UNSUPPORTED" : "TEST_PREVIEW_PIXEL_UNSUPPORTED";
        if (sqlError && detail != null) {
            var state = Pattern.compile("SQL state ([A-Z0-9]{5})").matcher(detail);
            if (state.find()) fields.put("sqlState", state.group(1));
        }
        // Messages are selected by the implementation; never copy parser/driver exception text.
        String explanation = switch (code) {
            case "TEST_DATABASE_SQL_ERROR" -> "Fixture SQL execution failed. Check table/column definitions, constraints and seed data.";
            case "UNMAPPED_TEST_DATABASE" -> "The engine ID does not match the configured source H2 engine.";
            case "TEST_DATABASE_SQL_UNSUPPORTED" -> "SQL syntax or SqlQuery arguments are outside the supported test subset.";
            default -> "This Pixel operation or argument form is unavailable in the test preview.";
        };
        add(code, fields.containsKey("sqlShape") ? "SQL" : "PIXEL", fields,
                name + (fields.containsKey("sqlShape") ? " [" + fields.get("sqlShape") + "]" : "") + ": " + explanation + (fields.containsKey("sqlState") ? " SQL state " + fields.get("sqlState") + "." : ""));
    }

    private void add(String code, String kind, Map<String,Object> fields, String detail) {
        Map<String,Object> value = new LinkedHashMap<>(fields);
        value.put("phase", phase); value.put("kind", kind); value.put("code", code);
        value.put("reason", phase + ": " + code + ": " + detail + " No request was forwarded to the real database.");
        if (failures.size() < 10 && !failures.contains(value)) failures.add(Map.copyOf(value));
    }

    static String path(String url) {
        try {
            String path = URI.create(url == null ? "" : url).getRawPath();
            if (path == null) return "[non-HTTP resource]";
            path = path.replaceFirst(".*?/app-test-preview/[^/]+/[^/]+/", "/");
            int portal = path.indexOf("/portals/");
            if (portal >= 0) path = "portal/" + path.substring(portal + 9);
            // Opaque identifiers in URLs can be credentials; expose filenames and named route segments only.
            path = path.replaceAll("(?i)[a-z0-9_-]{32,}", "[id]")
                    .replaceAll("(?i)%[0-9a-f]{2}", "_").replaceAll("[^A-Za-z0-9_./\\[\\]-]", "_");
            return path.substring(0, Math.min(path.length(), 240));
        } catch (RuntimeException invalid) { return "[invalid address]"; }
    }

    /** Compact history only accepts bounded diagnostic records, not arbitrary request/evidence fields. */
    static List<Map<String,Object>> compact(Object value) {
        if (!(value instanceof List<?> entries)) return List.of();
        var result = new ArrayList<Map<String,Object>>();
        for (Object entry : entries.stream().limit(10).toList()) {
            if (!(entry instanceof Map<?,?> source)) continue;
            Map<String,Object> safe = new LinkedHashMap<>();
            for (String field : Set.of("phase", "kind", "code", "method", "path", "resourceType", "operation", "sqlShape", "sqlState", "reason"))
                if (source.get(field) instanceof String text) safe.put(field, text.substring(0, Math.min(text.length(), 800)));
            if (!safe.isEmpty()) result.add(safe);
        }
        return result;
    }
}
