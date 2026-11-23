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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;


import prerna.om.Insight;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Entry point for the dedicated Monolith regression endpoint. */
public final class RegressionPixelAccess implements AutoCloseable {
    public static final int MAX_REQUEST_BYTES = 32_768;
    static final String STRING = "\"(?:[^\"\\\\\\p{Cntrl}]++|\\\\(?:[\"\\\\/bfnrt]|u[0-9A-Fa-f]{4}))*\"";
    private static final Pattern PIXEL = Pattern.compile(
            "\\s*(?:META\\s*\\|\\s*)?SqlQuery\\((.*)\\)\\s*;?\\s*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final String VALUE = STRING + "|true|false|[0-9]{1,6}";
    private static final Pattern ARGUMENT = Pattern.compile(
            "\\G\\s*(database|query|limit|commit|transaction|timeout)\\s*=\\s*(?:\\[\\s*(" + VALUE
                    + ")\\s*]|(" + VALUE + "))\\s*(,|$)", Pattern.CASE_INSENSITIVE);
    private static final Map<String, RegressionPixelAccess> ACTIVE = new ConcurrentHashMap<>();
    private static final ThreadLocal<Execution> CURRENT = new ThreadLocal<>();

    private record Request(String sql, int limit) { }
    private record Execution(RegressionPixelAccess session, Request request) { }

    private final String runId;
    private final String secret = UUID.randomUUID() + "-" + UUID.randomUUID();
    private final String originalEngineId;
    private String projectId;
    private String lastFailure;
    final AppTestDiagnostics diagnostics = new AppTestDiagnostics();
    private String activePixel;
    private final RegressionH2Database database;
    private final Instant expiresAt;
    private final Insight insight = new Insight();
    private boolean closed;
    private int reads;
    private int writes;

    RegressionPixelAccess(String runId, String originalEngineId, RegressionH2Database database, Instant expiresAt) {
        this.runId = runId;
        this.originalEngineId = originalEngineId;
        this.database = database;
        this.expiresAt = expiresAt;
        if (ACTIVE.putIfAbsent(runId, this) != null) {
            throw new IllegalArgumentException("A regression session already exists for this run");
        }
    }

    String secret() {
        return secret;
    }

    /** Only a server-generated, unexpired capability selects an execution context. */
    public static RegressionPixelAccess authenticate(String runId, String secret) {
        RegressionPixelAccess session = runId == null ? null : ACTIVE.get(runId);
        if (session == null || secret == null || secret.length() != session.secret.length()
                || !MessageDigest.isEqual(secret.getBytes(StandardCharsets.UTF_8), session.secret.getBytes(StandardCharsets.UTF_8))
                || !Instant.now().isBefore(session.expiresAt)) {
            throw new SecurityException("The regression session is missing, expired, or revoked");
        }
        return session;
    }

    RegressionPixelAccess forApp(String projectId) {
        this.projectId = projectId;
        return this;
    }

    synchronized String lastFailure() { return diagnostics.reason() == null ? lastFailure : diagnostics.reason(); }

    private static Request parse(String pixel, String originalEngineId) {
        if (pixel == null || pixel.length() > 8_192) {
            throw new SecurityException("A single regression SqlQuery is required");
        }
        var match = PIXEL.matcher(pixel);
        if (!match.matches()) throw new SecurityException("UNSUPPORTED_TEST_DATABASE_OPERATION: Only a single literal SqlQuery is supported. Python, custom reactors and other operations are not run.");
        Map<String, com.google.gson.JsonElement> arguments = new java.util.HashMap<>();
        var argument = ARGUMENT.matcher(match.group(1));
        int end = 0;
        boolean trailingComma = false;
        while (argument.find()) {
            String name = argument.group(1).toLowerCase(java.util.Locale.ROOT);
            if (arguments.putIfAbsent(name, com.google.gson.JsonParser.parseString(argument.group(2) == null ? argument.group(3) : argument.group(2))) != null)
                throw new SecurityException("Duplicate regression SqlQuery argument: " + name);
            end = argument.end();
            trailingComma = ",".equals(argument.group(4));
        }
        if (end != match.group(1).length() || trailingComma
                || !arguments.containsKey("database") || !arguments.containsKey("query")
                || !arguments.get("database").getAsJsonPrimitive().isString()
                || !arguments.get("query").getAsJsonPrimitive().isString())
            throw new SecurityException("Use one SqlQuery with literal database and query arguments; optional limit, commit, transaction and timeout are supported.");
        if (!originalEngineId.equals(arguments.get("database").getAsString()))
            throw new SecurityException("UNMAPPED_TEST_DATABASE: This database is outside the configured test environment. The real database was not accessed.");
        for (String flag : List.of("commit", "transaction"))
            if (arguments.containsKey(flag) && !arguments.get(flag).getAsJsonPrimitive().isBoolean())
                throw new SecurityException(flag + " must be a boolean");
        for (String number : List.of("limit", "timeout"))
            if (arguments.containsKey(number) && !arguments.get(number).getAsJsonPrimitive().isNumber())
                throw new SecurityException(number + " must be a whole number");
        String sql = arguments.get("query").getAsString().strip();
        if (sql.endsWith(";")) sql = sql.substring(0, sql.length() - 1).stripTrailing();
        RegressionH2Database.isRead(sql);
        int limit = arguments.containsKey("limit") ? arguments.get("limit").getAsInt() : 100;
        return new Request(sql, limit);
    }

    /**
     * Run the approved SQL through the real Pixel interpreter and SqlQuery reactor.
     * The client expression is parsed here and never submitted to the interpreter.
     */
    public synchronized Map<String, Object> execute(String pixel, String insightId) {
        try { checkInsight(insightId); }
        catch (SecurityException failure) { diagnostics.pixel(pixel, failure.getMessage(), false); throw failure; }
        // Bootstrap is acknowledged locally; ordinary app reactors/Python are never loaded.
        String app = Pattern.quote("\"" + projectId + "\"");
        if (projectId != null && pixel != null && pixel.matches("(?is)\\s*(?:META\\s*\\|\\s*)?(?:init(?:\\s*\\(\\s*\\))?|LoadApp\\s*\\(\\s*(?:" + app + "|project\\s*=\\s*(?:" + app + "|\\[\\s*" + app + "\\s*]))\\s*\\))\\s*;?\\s*")) {
            return result(true, "init;");
        }
        if (pixel != null && pixel.matches("(?is)\\s*(?:META\\s*\\|\\s*)?DropInsight\\(\\s*\\)\\s*;?\\s*")) {
            return result(true, "DropInsight();");
        }
        final Request request;
        try { request = parse(pixel, originalEngineId); }
        catch (IllegalArgumentException | SecurityException failure) {
            lastFailure = failure.getMessage();
            diagnostics.pixel(pixel, failure.getMessage(), false);
            throw failure;
        }
        activePixel = pixel;
        CURRENT.set(new Execution(this, request));
        try {
            List<NounMetadata> results = insight
                    .runPixel("META | SqlQuery(database=[\"regression\"], query=[\"regression\"]);").getResults();
            if (results.size() != 1 || results.getFirst().getOpType().contains(PixelOperationType.ERROR)) {
                throw new IllegalStateException("The regression SqlQuery failed");
            }
            NounMetadata result = results.getFirst();
            return Map.of("insightID", insight.getInsightId(), "pixelReturn", List.of(Map.of(
                    "isMeta", true, "operationType", result.getOpType(), "output", result.getValue(),
                    "pixelExpression", "SqlQuery();", "pixelId", "0", "timeToRun", 0)));
        } finally {
            CURRENT.remove();
            activePixel = null;
        }
    }

    private Map<String,Object> result(Object output, String expression) {
        return Map.of("insightID", insight.getInsightId(), "pixelReturn", List.of(Map.of(
                "isMeta", true, "operationType", List.of("OPERATION"), "output", output,
                "pixelExpression", expression, "pixelId", "0", "timeToRun", 0)));
    }

    private void checkInsight(String insightId) {
        if (closed || !Instant.now().isBefore(expiresAt)) {
            throw new SecurityException("The regression session expired or was revoked");
        }
        if (insightId != null && !insightId.isBlank() && !"new".equals(insightId)
                && !insight.getInsightId().equals(insightId)) {
            throw new SecurityException("The requested Insight is outside this regression run");
        }
    }

    synchronized Map<String, Object> assetResult(String insightId, String content) {
        checkInsight(insightId);
        return Map.of("insightID", insight.getInsightId(), "pixelReturn", List.of(Map.of(
                "isMeta", true, "operationType", List.of("OPERATION"), "output", content,
                "pixelExpression", "GetAppAssets();", "pixelId", "0", "timeToRun", 0)));
    }

    synchronized void rejected(String reason) { lastFailure = reason; }

    /** SqlQuery calls this before normal engine resolution. The scope exists only during the dedicated request. */
    public static NounMetadata query(Insight insight) {
        Execution execution = CURRENT.get();
        if (execution == null) {
            return null;
        }
        if (insight != execution.session.insight) {
            throw new SecurityException("The reactor is outside this regression run");
        }
        Request request = execution.request;
        try {
            Object output = execution.session.database.execute(request.sql, request.limit);
            if (output instanceof Boolean) {
                execution.session.writes++;
                return new NounMetadata(output, PixelDataType.BOOLEAN, PixelOperationType.ALTER_DATABASE);
            }
            execution.session.reads++;
            return new NounMetadata(output, PixelDataType.MAP);
        } catch (SQLException e) {
            String state = e.getSQLState() == null ? "unknown" : e.getSQLState();
            execution.session.lastFailure = "TEST_DATABASE_SQL_ERROR: Check the test schema, seed data and query compatibility (SQL state " + state + "). The real database was not accessed.";
            execution.session.diagnostics.pixel(execution.session.activePixel, execution.session.lastFailure, true);
            throw new IllegalArgumentException(execution.session.lastFailure);
        }
    }

    synchronized boolean exercisedDatabase() {
        return reads > 0 && writes > 0;
    }

    @Override
    public synchronized void close() {
        closed = true;
        ACTIVE.remove(runId, this);
    }
}
