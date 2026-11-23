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

import java.io.IOException;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.FileMode;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.treewalk.filter.PathFilter;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Route;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.util.AssetUtility;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;

/** Immutable portal bytes and the approved fixtures for one recorded H2 scenario. */
final class RegressionPreview {
    private static final String PORTALS = "assets/portals/";
    private static final String ROOT_PROPERTY = "semoss.appRegressionRoot";
    private static final String MONOLITH_PROPERTY = "semoss.appRegressionMonolithUrl";
    private static final String PIXEL_PATH = "/api/engine/runPixel";
    private static final int MAX_PORTAL_BYTES = 16 * 1024 * 1024;
    private static final String CSP = "default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; "
            + "img-src 'self' data:; font-src 'self'; connect-src 'self'; frame-src 'self'; "
            + "base-uri 'none'; object-src 'none'; form-action 'none'; worker-src 'none'";
    private static final Gson JSON = new Gson();
    private static final Map<String, String> MIME = Map.ofEntries(
            Map.entry("html", "text/html"), Map.entry("js", "application/javascript"),
            Map.entry("mjs", "application/javascript"), Map.entry("css", "text/css"),
            Map.entry("json", "application/json"), Map.entry("svg", "image/svg+xml"),
            Map.entry("png", "image/png"), Map.entry("jpg", "image/jpeg"),
            Map.entry("webp", "image/webp"), Map.entry("woff2", "font/woff2"));
    private static final HttpClient HTTP = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER)
            .proxy(ProxySelector.of(null)).connectTimeout(Duration.ofSeconds(3)).build();

    private record Approval(String publishedSha, String recordingId, String originalEngineId,
            String schemaSha256, String seedSha256, Map<String, String> migrations) { }

    private final URI origin;
    private final URI root;
    private final URI monolith;
    private final String candidateSha;
    private final Approval approval;
    private final Map<String, Object> recording;
    private final Map<String, byte[]> assets;
    private final Path scratch;
    private final String baseline;
    private final String migration;
    private final String seed;

    private RegressionPreview(URI root, URI monolith, String candidateSha, Approval approval,
            Map<String, Object> recording, Map<String, byte[]> assets, Path scratch, String baseline,
            String migration, String seed) {
        this.root = root;
        this.origin = root.resolve("/");
        this.monolith = monolith;
        this.candidateSha = candidateSha;
        this.approval = approval;
        this.recording = recording;
        this.assets = assets;
        this.scratch = scratch;
        this.baseline = baseline;
        this.migration = migration;
        this.seed = seed;
    }

    static RegressionPreview load(User actor, String projectId, String candidateSha, String recordingId, String runId)
            throws IOException {
        String config = System.getProperty(ROOT_PROPERTY);
        if (config == null || config.isBlank()) {
            throw new IOException("H2 regression is disabled on this worker");
        }
        Path configRoot = Path.of(config).toRealPath();
        if (!projectId.matches("[A-Za-z0-9_-]{1,128}") || !sha(candidateSha)) {
            throw new IOException("Provide a project and a full lowercase candidate Git SHA");
        }
        Path projectConfig = configRoot.resolve(projectId);
        if (!Files.isDirectory(projectConfig, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("An operator-approved H2 fixture is required for this app");
        }
        Approval approval = JSON.fromJson(read(projectConfig.resolve("approval.json")), Approval.class);
        if (approval == null || !sha(approval.publishedSha) || !recordingId.equals(approval.recordingId)
                || approval.originalEngineId == null || !approval.originalEngineId.matches("[A-Za-z0-9_-]{1,128}")) {
            throw new IOException("The approved app revision, recording, or H2 dependency is missing");
        }
        if (!SecurityEngineUtils.userCanViewEngine(actor, approval.originalEngineId)) {
            throw new IOException("Viewing the approved source database is required");
        }
        Object[] engineType = SecurityEngineUtils.getEngineTypeAndSubtype(approval.originalEngineId);
        if (engineType[0] != IEngine.CATALOG_TYPE.DATABASE || !RdbmsTypeEnum.H2_DB.getLabel().equals(engineType[1])) {
            throw new IOException("UNSUPPORTED_DEPENDENCY: The approved source engine must be H2");
        }
        String baseline = approvedSql(projectConfig.resolve("baseline.sql"), approval.schemaSha256);
        String seed = approvedSql(projectConfig.resolve("seed.sql"), approval.seedSha256);
        String migrationHash = approval.migrations == null ? null : approval.migrations.get(candidateSha);
        String migration = migrationHash == null ? "" : approvedSql(projectConfig.resolve(candidateSha + ".sql"), migrationHash);
        Path scratch = configRoot.resolve("runs");
        if (!Files.isDirectory(scratch, LinkOption.NOFOLLOW_LINKS) || scratch.toString().contains(";")) {
            throw new IOException("The worker's dedicated H2 run directory is unavailable");
        }
        URI monolith = loopbackMonolith();
        var project = Utility.getProject(projectId);
        var version = Path.of(AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId));
        try (Git git = Git.open(version.toFile()); RevWalk revisions = new RevWalk(git.getRepository())) {
            RevCommit published = revisions.parseCommit(ObjectId.fromString(approval.publishedSha));
            RevCommit candidate = revisions.parseCommit(ObjectId.fromString(candidateSha));
            if (!revisions.isMergedInto(published, candidate)) {
                throw new IOException("The candidate does not descend from the approved app revision");
            }
            String path = "assets/app-tests/tests/" + AppTestRecordings.id(recordingId) + ".json";
            String definition;
            try (TreeWalk file = TreeWalk.forPath(git.getRepository(), path, published.getTree())) {
                if (file == null || !FileMode.REGULAR_FILE.equals(file.getFileMode(0))) {
                    throw new IOException("The approved revision does not contain the saved recording");
                }
                definition = new String(file.getObjectReader().open(file.getObjectId(0))
                        .getBytes(AppTestRecordings.MAX_BYTES), StandardCharsets.UTF_8);
            }
            Map<String, Object> recording = AppTestRecordings.validate(JSON.fromJson(definition,
                    new TypeToken<Map<String, Object>>() { }.getType()));
            if (!recordingId.equals(recording.get("id")) || !hasAssertion(recording)) {
                throw new IOException("The approved recording needs a saved ID and an outcome assertion");
            }
            URI root = AppTestRunner.appRoot("http://" + runId + ".semoss-regression.invalid/", projectId);
            return new RegressionPreview(root, monolith, candidateSha, approval, recording,
                    portal(git, candidate), scratch, baseline, migration, seed);
        }
    }

    static String approvedRecordingId(String projectId) {
        String root = System.getProperty(ROOT_PROPERTY);
        if (root == null || root.isBlank() || !projectId.matches("[A-Za-z0-9_-]{1,128}")) {
            return null;
        }
        try {
            Path projectConfig = Path.of(root).toRealPath().resolve(projectId);
            if (!Files.isDirectory(projectConfig, LinkOption.NOFOLLOW_LINKS)) {
                return null;
            }
            Approval approval = JSON.fromJson(read(projectConfig.resolve("approval.json")), Approval.class);
            return approval == null ? null : AppTestRecordings.id(approval.recordingId);
        } catch (IOException | RuntimeException e) {
            return null;
        }
    }

    private static boolean hasAssertion(Map<String, Object> recording) {
        return recording.get("steps") instanceof List<?> steps
                && steps.stream().anyMatch(step -> step instanceof Map<?, ?> map
                        && String.valueOf(map.get("type")).startsWith("ASSERT_"));
    }

    private static Map<String, byte[]> portal(Git git, RevCommit candidate) throws IOException {
        Map<String, byte[]> assets = new HashMap<>();
        int bytes = 0;
        try (TreeWalk files = new TreeWalk(git.getRepository())) {
            files.addTree(candidate.getTree());
            files.setRecursive(true);
            files.setFilter(PathFilter.create("assets/portals"));
            while (files.next()) {
                String name = files.getPathString().substring(PORTALS.length());
                if (assets.size() >= 256 || !name.matches("[A-Za-z0-9_./-]+")
                        || !FileMode.REGULAR_FILE.equals(files.getFileMode(0)) || mime(name) == null) {
                    throw new IOException("The candidate portal contains an unsupported asset");
                }
                byte[] content = files.getObjectReader().open(files.getObjectId(0)).getBytes(MAX_PORTAL_BYTES - bytes);
                bytes += content.length;
                assets.put(name, content);
            }
        }
        if (!assets.containsKey("index.html")) {
            throw new IOException("Commit a built, self-contained assets/portals/index.html in the candidate");
        }
        return Map.copyOf(assets);
    }

    private static URI loopbackMonolith() throws IOException {
        String url = System.getProperty(MONOLITH_PROPERTY, "");
        if (!url.matches("http://127\\.0\\.0\\.1:[0-9]{1,5}(?:/[A-Za-z0-9_-]+)*/?")) {
            throw new IOException("A loopback Monolith URL is required on the pinned H2 regression worker");
        }
        return URI.create(url.endsWith("/") ? url : url + "/");
    }

    private static boolean sha(String value) {
        return value != null && value.matches("[a-f0-9]{40}");
    }

    private static String read(Path file) throws IOException {
        if (!Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("An approved regression input is missing");
        }
        try (var stream = Files.newInputStream(file, LinkOption.NOFOLLOW_LINKS)) {
            byte[] data = stream.readNBytes(256_001);
            if (data.length > 256_000) {
                throw new IOException("An approved regression input exceeds its size limit");
            }
            return new String(data, StandardCharsets.UTF_8);
        }
    }

    private static String approvedSql(Path file, String expected) throws IOException {
        String sql = read(file);
        if (!hash(sql).equals(expected)) {
            throw new IOException("An H2 fixture differs from the approved SHA-256");
        }
        return sql;
    }

    private static String hash(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    URI root() {
        return root;
    }

    Map<String, Object> recording() {
        return recording;
    }

    Map<String, Object> provenance() {
        return Map.of("mode", "ISOLATED_H2", "candidateCommitSha", candidateSha,
                "publishedCommitSha", approval.publishedSha, "suiteHash", AppTestHistory.definitionHash(recording),
                "schemaHash", approval.schemaSha256, "migrationHash", hash(migration), "seedHash", approval.seedSha256);
    }

    RegressionH2Database openDatabase() throws IOException, SQLException {
        return RegressionH2Database.open(scratch, baseline, migration, seed);
    }

    RegressionPixelAccess openSession(String runId, RegressionH2Database database) {
        return new RegressionPixelAccess(runId, approval.originalEngineId, database, Instant.now().plusSeconds(180));
    }

    void ready(String runId, RegressionPixelAccess session) throws IOException, InterruptedException {
        var probe = request(runId + "/ready", session).GET().build();
        var response = HTTP.send(probe, HttpResponse.BodyHandlers.discarding());
        if (response.statusCode() != 204 || !response.headers().firstValue("X-Semoss-Regression-Ready").orElse("").equals(runId)) {
            throw new IOException("Monolith did not accept the scoped H2 regression session");
        }
    }

    private HttpRequest.Builder request(String path, RegressionPixelAccess session) {
        return HttpRequest.newBuilder(monolith.resolve(RegressionPixelAccess.ENDPOINT + path))
                .header(RegressionPixelAccess.HEADER, session.secret()).timeout(Duration.ofSeconds(10));
    }

    void configureBrowser(BrowserContext context, String runId, RegressionPixelAccess session, AtomicBoolean denied) {
        context.setOffline(true);
        context.routeWebSocket("**/*", socket -> {
            denied.set(true);
            socket.close();
        });
        context.route("**/*", route -> {
            URI url = URI.create(route.request().url());
            String method = route.request().method();
            if (origin.resolve(PIXEL_PATH).equals(url) && "POST".equals(method) && !route.request().isNavigationRequest()) {
                postPixel(route, runId, session, denied);
            } else if ("GET".equals(method) && AppTestRunner.insideApp(root, url.toString())) {
                String name = url.getPath().substring(root.getPath().length());
                name = name.isEmpty() ? "index.html" : name;
                byte[] content = assets.get(name);
                if (content == null) {
                    denied.set(true);
                    route.abort();
                    return;
                }
                route.fulfill(new Route.FulfillOptions().setStatus(200).setContentType(mime(name))
                        .setHeaders(Map.of("Content-Security-Policy", CSP, "X-Content-Type-Options", "nosniff"))
                        .setBodyBytes(content));
            } else if ("GET".equals(method) && origin.resolve("/favicon.ico").equals(url)) {
                route.fulfill(new Route.FulfillOptions().setStatus(404));
            } else {
                denied.set(true);
                route.abort();
            }
        });
    }

    private void postPixel(Route route, String runId, RegressionPixelAccess session, AtomicBoolean denied) {
        try {
            String body = route.request().postData();
            String contentType = route.request().headers().get("content-type");
            if (body == null || body.getBytes(StandardCharsets.UTF_8).length > RegressionPixelAccess.MAX_REQUEST_BYTES
                    || !"application/x-www-form-urlencoded".equals(contentType)) {
                throw new IOException("An SDK form request is required");
            }
            HttpRequest request = request(runId + PIXEL_PATH, session).header("Content-Type", contentType)
                    .POST(HttpRequest.BodyPublishers.ofString(body)).build();
            HttpResponse<java.io.InputStream> response = HTTP.send(request, HttpResponse.BodyHandlers.ofInputStream());
            try (var data = response.body()) {
                if (response.statusCode() != 200) {
                    throw new IOException("The isolated SDK request was denied or failed");
                }
                byte[] json = data.readNBytes(1_000_001);
                if (json.length > 1_000_000) {
                    throw new IOException("The isolated SDK result exceeded its size limit");
                }
                route.fulfill(new Route.FulfillOptions().setStatus(200)
                        .setContentType("application/json").setBodyBytes(json));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            denied.set(true);
            route.abort();
        } catch (IOException e) {
            denied.set(true);
            route.abort();
        }
    }

    private static String mime(String name) {
        return MIME.get(name.substring(name.lastIndexOf('.') + 1));
    }
}
