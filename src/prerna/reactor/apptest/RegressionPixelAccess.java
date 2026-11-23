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

import com.google.gson.Gson;

import prerna.om.Insight;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Entry point for the dedicated Monolith regression endpoint. */
public final class RegressionPixelAccess implements AutoCloseable {
    public static final String HEADER = "X-Semoss-Regression-Session";
    public static final String ENDPOINT = "app-regression-pixel/";
    public static final int MAX_REQUEST_BYTES = 32_768;
    private static final String STRING = "\"(?:[^\"\\\\\\p{Cntrl}]++|\\\\(?:[\"\\\\/bfnrt]|u[0-9A-Fa-f]{4}))*\"";
    private static final Pattern PIXEL = Pattern.compile(
            "\\s*(?:META\\s*\\|\\s*)?SqlQuery\\(\\s*(database|query)\\s*=\\s*\\[\\s*(" + STRING
            + ")\\s*]\\s*,\\s*(database|query)\\s*=\\s*\\[\\s*(" + STRING
            + ")\\s*](?:\\s*,\\s*limit\\s*=\\s*\\[\\s*([1-9][0-9]{0,2})\\s*])?\\s*\\)\\s*;?\\s*");
    private static final Map<String, RegressionPixelAccess> ACTIVE = new ConcurrentHashMap<>();
    private static final ThreadLocal<Execution> CURRENT = new ThreadLocal<>();
    private static final Gson JSON = new Gson();

    private record Request(String sql, int limit) { }
    private record Execution(RegressionPixelAccess session, Request request) { }

    private final String runId;
    private final String secret = UUID.randomUUID() + "-" + UUID.randomUUID();
    private final String originalEngineId;
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

    private static Request parse(String pixel, String originalEngineId) {
        if (pixel == null || pixel.length() > 8_192) {
            throw new SecurityException("A single regression SqlQuery is required");
        }
        var match = PIXEL.matcher(pixel);
        if (!match.matches() || match.group(1).equals(match.group(3))) {
            throw new SecurityException("Only a single SqlQuery with literal arguments is supported in regression mode");
        }
        boolean firstIsDatabase = "database".equals(match.group(1));
        String requestedEngine = JSON.fromJson(match.group(firstIsDatabase ? 2 : 4), String.class);
        if (!originalEngineId.equals(requestedEngine)) {
            throw new SecurityException("The requested database is outside this regression run");
        }
        String sql = JSON.fromJson(match.group(firstIsDatabase ? 4 : 2), String.class).strip();
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1).stripTrailing();
        }
        RegressionH2Database.isRead(sql);
        int limit = match.group(5) == null ? 100 : Integer.parseInt(match.group(5));
        return new Request(sql, limit);
    }

    /**
     * Run the approved SQL through the real Pixel interpreter and SqlQuery reactor.
     * The client expression is parsed here and never submitted to the interpreter.
     */
    public synchronized Map<String, Object> execute(String pixel, String insightId) {
        if (closed || !Instant.now().isBefore(expiresAt)) {
            throw new SecurityException("The regression session expired or was revoked");
        }
        if (insightId != null && !insightId.isBlank() && !"new".equals(insightId)
                && !insight.getInsightId().equals(insightId)) {
            throw new SecurityException("The requested Insight is outside this regression run");
        }
        Request request = parse(pixel, originalEngineId);
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
        }
    }

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
            throw new IllegalArgumentException("The SQL could not execute against the isolated H2 fixture");
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
