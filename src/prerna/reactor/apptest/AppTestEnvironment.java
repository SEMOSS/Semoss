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
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.util.AssetUtility;
import prerna.util.sql.RdbmsTypeEnum;

/** App-owned fixture definitions; runtime connections and capabilities never enter project assets. */
public final class AppTestEnvironment implements AutoCloseable {
    public static final String ENDPOINT = "app-test-preview/";
    private static final Gson JSON = new Gson();
    private static final String ASSET_VALUE = "(\\[\\s*" + RegressionPixelAccess.STRING + "\\s*]|" + RegressionPixelAccess.STRING + ")";
    private static final Pattern PUBLIC_ASSET = Pattern.compile(
            "\\s*(?:META\\s*\\|\\s*)?GetAppAssets\\(\\s*(project|filePath)\\s*=\\s*" + ASSET_VALUE
            + "\\s*,\\s*(project|filePath)\\s*=\\s*" + ASSET_VALUE + "\\s*\\)\\s*;?\\s*", Pattern.CASE_INSENSITIVE);
    private static final int MAX_CONFIG_BYTES = 256_000;
    private static final int MAX_ASSET_BYTES = 32 * 1024 * 1024;
    private static final Map<String, AppTestEnvironment> ACTIVE = new ConcurrentHashMap<>();
    private static final Map<String, String> MIME = Map.ofEntries(
            Map.entry("html", "text/html;charset=UTF-8"), Map.entry("js", "application/javascript"),
            Map.entry("mjs", "application/javascript"), Map.entry("css", "text/css"),
            Map.entry("json", "application/json"), Map.entry("svg", "image/svg+xml"),
            Map.entry("png", "image/png"), Map.entry("jpg", "image/jpeg"), Map.entry("jpeg", "image/jpeg"),
            Map.entry("webp", "image/webp"), Map.entry("gif", "image/gif"), Map.entry("ico", "image/x-icon"),
            Map.entry("woff", "font/woff"), Map.entry("woff2", "font/woff2"), Map.entry("ttf", "font/ttf"),
            Map.entry("map", "application/json"));
    private static final org.apache.logging.log4j.Logger LOGGER = org.apache.logging.log4j.LogManager.getLogger(AppTestEnvironment.class);

    private static boolean shuttingDown;
    private static volatile Map<String,byte[]> sdkAssets = Map.of();
    private static final java.util.concurrent.ScheduledExecutorService REAPER = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "app-test-environment-cleanup");
            thread.setDaemon(true);
            return thread;
        });
    static {
        REAPER.scheduleWithFixedDelay(() -> {
            for (AppTestEnvironment environment : ACTIVE.values()) {
                if (!Instant.now().isBefore(environment.expiresAt)) environment.close();
            }
        }, 30, 30, TimeUnit.SECONDS);
    }

    /** Validated immutable inputs shared by a run; every replay still creates its own database. */
    record Definition(String sourceEngineId, String schemaSql, String seedSql, String fixtureHash) { }
    record Portal(Map<String, byte[]> assets, String hash, Map<String,String> publicJson, Map<String,byte[]> sdk) {
        Portal(Map<String,byte[]> assets, String hash) { this(assets, hash, Map.of(), Map.of()); }
        Portal(Map<String,byte[]> assets, String hash, Map<String,String> publicJson) { this(assets, hash, publicJson, Map.of()); }
    }

    /** Called by Monolith at startup with the deployed frontend's fixed SDK directory, never an app-supplied path. */
    public static void loadPlatformSdk(Path directory) throws IOException {
        Map<String,byte[]> snapshot = new java.util.TreeMap<>();
        int size = 0;
        if (directory != null && Files.isDirectory(directory)) {
            Path sdkRoot = directory.toRealPath();
            try (var paths = Files.walk(sdkRoot)) {
                for (Path path : paths.filter(Files::isRegularFile).toList()) {
                    String name = sdkRoot.relativize(path).toString().replace('\\', '/');
                    if (!(name.endsWith(".js") || name.endsWith(".mjs") || name.endsWith(".css")) || name.startsWith(".") || name.contains("/.")) continue;
                    if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || snapshot.size() >= 256 || Files.size(path) > 16 * 1024 * 1024 - size)
                        throw new IOException("Platform SDK snapshot exceeds limits or contains a symlink");
                    byte[] content = Files.readAllBytes(path); size += content.length; snapshot.put(name, content);
                }
            }
        }
        sdkAssets = Map.copyOf(snapshot);
    }
    public record Asset(byte[] bytes, String contentType) { }

    private final String id = UUID.randomUUID().toString();
    private final String owner;
    private final String projectId;
    private final Instant expiresAt;
    private final RegressionH2Database database;
    private final RegressionPixelAccess access;
    private final Portal portal;
    private final Definition definition;
    private final Path scratch;
    private volatile boolean closed;

    private AppTestEnvironment(String owner, String projectId, Definition definition, Portal portal, int seconds)
            throws IOException, SQLException {
        this.owner = owner;
        this.projectId = projectId;
        this.definition = definition;
        this.portal = portal;
        this.expiresAt = Instant.now().plusSeconds(seconds);
        this.scratch = Files.createTempDirectory("semoss-app-test-");
        try {
            this.database = RegressionH2Database.open(scratch, definition.schemaSql, "", definition.seedSql);
        } catch (IOException | SQLException | RuntimeException failure) {
            Files.deleteIfExists(scratch);
            throw failure;
        }
        this.access = new RegressionPixelAccess(id, definition.sourceEngineId, database, expiresAt).forApp(projectId);
    }

    private static Path configFile(String projectId) {
        return Path.of(AssetUtility.getProjectAssetsFolder(projectId), "app-tests", "database.json");
    }

    static Map<String, Object> configuration(String projectId) throws IOException {
        Path file = configFile(projectId);
        if (!Files.exists(file, LinkOption.NOFOLLOW_LINKS)) return Map.of("enabled", false);
        if (!Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS) || Files.size(file) > MAX_CONFIG_BYTES)
            throw new IOException("Test database configuration is invalid or too large");
        try {
            Map<String, Object> value = JSON.fromJson(Files.readString(file), new TypeToken<Map<String, Object>>() { }.getType());
            validate(value);
            return value;
        } catch (RuntimeException failure) {
            throw new IOException("Invalid assets/app-tests/database.json: " + failure.getMessage(), failure);
        }
    }

    static Definition validate(Map<String, Object> value) {
        if (value != null && JSON.toJson(value).getBytes(StandardCharsets.UTF_8).length > MAX_CONFIG_BYTES)
            throw new IllegalArgumentException("Test database configuration exceeds 256 KB");
        if (value != null && value.containsKey("version") && (!(value.get("version") instanceof Number version) || version.doubleValue() != 1))
            throw new IllegalArgumentException("Unsupported test database configuration version");
        if (value == null || !(value.get("enabled") instanceof Boolean))
            throw new IllegalArgumentException("enabled must be true or false");
        if (!Boolean.TRUE.equals(value.get("enabled"))) return null;
        String source = text(value, "sourceEngineId").trim();
        String schema = text(value, "schemaSql");
        String seed = text(value, "seedSql");
        if (!source.matches("[A-Za-z0-9_-]{1,128}")) throw new IllegalArgumentException("Provide a source H2 database ID");
        if (schema.isBlank()) throw new IllegalArgumentException("Schema SQL is required");
        return new Definition(source, schema, seed, hash(JSON.toJson(List.of(source, schema, seed))));
    }

    private static String text(Map<String, Object> value, String field) {
        if (!(value.get(field) instanceof String text)) throw new IllegalArgumentException(field + " must be text");
        return text;
    }

    static Definition definition(User actor, String projectId) throws IOException {
        Definition definition = validate(configuration(projectId));
        if (definition != null) authorize(actor, definition);
        return definition;
    }

    static void authorize(User actor, Definition definition) {
        if (!SecurityEngineUtils.userCanViewEngine(actor, definition.sourceEngineId))
            throw new IllegalArgumentException("You need view access to the source database configured for this app");
        Object[] type = SecurityEngineUtils.getEngineTypeAndSubtype(definition.sourceEngineId);
        if (type == null || type.length < 2 || type[0] != IEngine.CATALOG_TYPE.DATABASE
                || !RdbmsTypeEnum.H2_DB.getLabel().equals(type[1]))
            throw new IllegalArgumentException("UNSUPPORTED_DEPENDENCY: The source database must be H2 in this version");
    }

    static Map<String, Object> save(User actor, String projectId, Map<String, Object> value) throws IOException, SQLException {
        Definition definition = validate(value);
        Map<String, Object> saved = new LinkedHashMap<>();
        saved.put("version", 1);
        saved.put("enabled", definition != null);
        if (definition != null) {
            authorize(actor, definition);
            // Prove the fixture is usable before replacing a working configuration.
            checkFixture(definition);
            saved.put("sourceEngineId", definition.sourceEngineId);
            saved.put("schemaSql", definition.schemaSql);
            saved.put("seedSql", definition.seedSql);
        } else {
            for (String field : List.of("sourceEngineId", "schemaSql", "seedSql"))
                if (value.get(field) instanceof String text) saved.put(field, text);
        }
        saved.put("updatedAt", Instant.now().toString());
        Path file = configFile(projectId);
        AppTestRecordings.write(file, saved);
        ClusterUtil.copyLocalFileToEngineCloudFolder(projectId, IEngine.CATALOG_TYPE.PROJECT, file.toString());
        return saved;
    }

    static void checkFixture(Definition definition) throws IOException, SQLException {
        Path scratch = Files.createTempDirectory("semoss-app-test-validate-");
        try (var ignored = RegressionH2Database.open(scratch, definition.schemaSql, "", definition.seedSql)) {
            // Provision the fixture before accepting the settings or a baseline.
        } finally {
            Files.deleteIfExists(scratch);
        }
    }

    static Portal portal(String projectId) throws IOException {
        return portal(Path.of(AssetUtility.getProjectPortalsFolder(projectId)));
    }

    static Portal portal(Path folder) throws IOException {
        if (!Files.isDirectory(folder, LinkOption.NOFOLLOW_LINKS)) throw new IOException("Build the app into assets/portals first");
        Map<String, byte[]> assets = new java.util.TreeMap<>();
        int bytes = 0;
        try (var paths = Files.walk(folder)) {
            for (Path path : paths.filter(p -> !Files.isDirectory(p, LinkOption.NOFOLLOW_LINKS)).toList()) {
                String name = folder.relativize(path).toString().replace('\\', '/');
                if (name.startsWith(".") || name.contains("/.") || name.endsWith(".map")) continue;
                if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) throw new IOException("Symlink assets are not supported in test previews");
                if (mime(name) == null) continue;
                if (assets.size() >= 1024 || Files.size(path) > MAX_ASSET_BYTES - bytes)
                    throw new IOException("Test preview exceeds 1024 assets or 32 MB");
                byte[] content = Files.readAllBytes(path);
                bytes += content.length;
                assets.put(name, content);
            }
        }
        if (!assets.containsKey("index.html")) throw new IOException("Build the app into assets/portals/index.html first");
        Map<String,String> publicJson = new java.util.TreeMap<>();
        Path publicFolder = folder.resolveSibling("public");
        if (Files.isDirectory(publicFolder, LinkOption.NOFOLLOW_LINKS)) {
            try (var paths = Files.walk(publicFolder)) {
                for (Path path : paths.filter(p -> p.toString().endsWith(".json")).toList()) {
                    if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || Files.size(path) > MAX_CONFIG_BYTES || publicJson.size() >= 32)
                        throw new IOException("Public JSON settings must be regular files, at most 32 files of 256 KB each");
                    String name = "public/" + publicFolder.relativize(path).toString().replace('\\', '/');
                    publicJson.put(name, Files.readString(path));
                }
            }
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            assets.forEach((name, content) -> {
                digest.update(name.getBytes(StandardCharsets.UTF_8)); digest.update((byte) 0); digest.update(content);
            });
            publicJson.forEach((name, content) -> {
                digest.update(name.getBytes(StandardCharsets.UTF_8)); digest.update((byte)0);
                digest.update(content.getBytes(StandardCharsets.UTF_8));
            });
            Map<String,byte[]> sdk = sdkAssets;
            new java.util.TreeMap<>(sdk).forEach((name, content) -> { digest.update(("sdk/" + name).getBytes(StandardCharsets.UTF_8)); digest.update((byte)0); digest.update(content); });
            return new Portal(Map.copyOf(assets), HexFormat.of().formatHex(digest.digest()), Map.copyOf(publicJson), sdk);
        } catch (java.security.NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }

    static synchronized AppTestEnvironment open(String owner, String projectId, Definition definition, Portal portal, boolean manual)
            throws IOException, SQLException {
        if (shuttingDown) throw new IllegalStateException("The test database worker is stopping");
        if (ACTIVE.size() >= 12 || manual && ACTIVE.values().stream().filter(e -> e.owner.equals(owner)).count() >= 3)
            throw new IllegalArgumentException("Too many test previews are open. Close an existing preview and try again");
        AppTestEnvironment environment = new AppTestEnvironment(owner, projectId, definition, portal, manual ? 1800 : 180);
        ACTIVE.put(environment.id, environment);
        return environment;
    }

    /** Capabilities can reach only the fixture and portal belonging to this environment. */
    public static AppTestEnvironment authenticate(String id, String secret) {
        AppTestEnvironment environment = ACTIVE.get(id);
        if (environment == null || environment.closed || !Instant.now().isBefore(environment.expiresAt))
            throw new SecurityException("Test preview expired or was closed. Open a fresh test preview");
        RegressionPixelAccess.authenticate(id, secret);
        return environment;
    }

    static void stop(String owner, String projectId, String id) {
        AppTestEnvironment environment = ACTIVE.get(id);
        if (environment == null) return;
        if (!environment.owner.equals(owner) || !environment.projectId.equals(projectId))
            throw new SecurityException("This test preview belongs to another app or user");
        environment.close();
    }

    Map<String, Object> started() {
        return Map.of("target", projectId, "environmentId", id, "previewPath", basePath() + "public_home/" + projectId + "/portals/",
                "expiresAt", expiresAt.toString(), "mode", "ISOLATED_H2");
    }

    String basePath() { return ENDPOINT + id + "/" + access.secret() + "/"; }
    URI root() { return AppTestRunner.appRoot("http://semoss-app-test.invalid/" + basePath(), projectId); }
    String failure() { return access.lastFailure(); }
    String failureCode() { return access.diagnostics.code(); }
    void phase(String phase) { access.diagnostics.phase(phase); }
    public List<Map<String,Object>> diagnostics() { return access.diagnostics.snapshot(); }
    public void blockedRequest(String method, String url, String resourceType, String code) {
        access.diagnostics.request(method, url, resourceType, code);
    }
    Map<String, Object> provenance() {
        return Map.of("mode", "ISOLATED_H2", "fixtureHash", definition.fixtureHash, "portalHash", portal.hash);
    }

    /** Minimal SDK bootstrap; it carries no normal session, privileges, database IDs or user information. */
    public Map<String, Object> sdkConfig() {
        Map<String,Object> config = new LinkedHashMap<>();
        config.put("security", false); config.put("csrf", false);
        config.put("logins", Map.of("NATIVE", "Regression test preview"));
        config.put("loginDetails", Map.of("NATIVE", Map.of("id", "app-test-preview", "name", "Regression test preview")));
        config.put("primaryLogin", "NATIVE"); config.put("availableProviders", List.of());
        config.put("databaseMetaKeys", List.of()); config.put("projectMetaKeys", List.of());
        config.put("theme", Map.of()); config.put("insightConfig", Map.of());
        config.put("python", false); config.put("r", false); config.put("anonymousUsers", false);
        config.put("notificationEnabled", false); config.put("timeout", 30);
        return config;
    }

    public Map<String, Object> execute(String form) {
        int before = diagnostics().size();
        try { return executeForm(form); }
        catch (RuntimeException failure) {
            if (diagnostics().size() == before) blockedRequest("POST", "/api/engine/runPixel", "fetch", "TEST_PREVIEW_REQUEST_INVALID");
            throw failure;
        }
    }

    private Map<String, Object> executeForm(String form) {
        if (form == null || form.getBytes(StandardCharsets.UTF_8).length > RegressionPixelAccess.MAX_REQUEST_BYTES)
            throw new IllegalArgumentException("Send a bounded URL-encoded SDK expression");
        Map<String, String> fields = new HashMap<>();
        for (String item : form.split("&", -1)) {
            int equals = item.indexOf('=');
            if (equals < 1) throw new IllegalArgumentException("Send a URL-encoded SDK expression");
            String name = URLDecoder.decode(item.substring(0, equals), StandardCharsets.UTF_8);
            if (!Set.of("expression", "insightId", "tz").contains(name)
                    || fields.putIfAbsent(name, URLDecoder.decode(item.substring(equals + 1), StandardCharsets.UTF_8)) != null)
                throw new IllegalArgumentException("Unexpected or duplicate SDK form field");
        }
        String expression = fields.get("expression");
        if (expression != null) {
            var request = PUBLIC_ASSET.matcher(expression);
            if (request.matches()) {
                boolean projectFirst = "project".equalsIgnoreCase(request.group(1));
                String target = assetString(request.group(projectFirst ? 2 : 4));
                String file = assetString(request.group(projectFirst ? 4 : 2));
                String content = portal.publicJson.get(file);
                if (request.group(1).equalsIgnoreCase(request.group(3)) || !projectId.equals(target) || content == null) {
                    String reason = "UNSUPPORTED_TEST_ASSET: Only saved public JSON settings of this app are readable in the test preview. App files are never modified.";
                    access.rejected(reason);
                    access.diagnostics.pixel(expression, reason, false);
                    throw new SecurityException(reason);
                }
                return access.assetResult(fields.get("insightId"), content);
            }
        }
        return access.execute(expression, fields.get("insightId"));
    }

    private static String assetString(String literal) {
        var value = com.google.gson.JsonParser.parseString(literal);
        return value.isJsonArray() ? value.getAsJsonArray().get(0).getAsString() : value.getAsString();
    }

    /** Same useful, sanitized SDK error envelope for manual previews and background playback. */
    public Map<String,Object> pixelError() {
        return Map.of("insightID", "new", "diagnostics", diagnostics(), "pixelReturn", List.of(Map.of(
                "isMeta", true, "pixelId", "0", "pixelExpression", "", "timeToRun", 0,
                "operationType", List.of("ERROR"), "output", failure() == null ? "The preview request failed" : failure())));
    }

    public String contentSecurityPolicy(String modulePath) {
        String base = modulePath + "/" + basePath();
        return "default-src 'none'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; "
                + "img-src 'self' data:; font-src 'self'; connect-src " + base + "api/; "
                + "frame-src 'none'; object-src 'none'; base-uri 'self'; form-action 'none'; worker-src 'none'";
    }

    /** Serve the immutable build, with an SPA fallback only for extensionless navigation routes. */
    public Asset asset(String path, String modulePath) {
        if (path.startsWith("sdk/")) {
            String name = path.substring(4);
            byte[] bytes = portal.sdk.get(name);
            return bytes == null ? null : new Asset(bytes, mime(name));
        }
        String prefix = "public_home/" + projectId + "/portals/";
        if (!path.startsWith(prefix)) throw new IllegalArgumentException("The preview path belongs to a different app");
        String name = path.substring(prefix.length());
        if (name.contains("\\") || name.contains("%") || List.of(name.split("/", -1)).stream().anyMatch(p -> p.equals("..") || p.equals(".")))
            throw new IllegalArgumentException("Invalid preview asset path");
        name = name.isEmpty() ? "index.html" : name;
        byte[] content = portal.assets.get(name);
        if (content == null && !name.substring(name.lastIndexOf('/') + 1).contains(".")) {
            name = "index.html"; content = portal.assets.get(name);
        }
        if (content == null) return null;
        if (name.endsWith(".html")) {
            String base = modulePath + "/" + basePath();
            String root = base + prefix;
            String env = JSON.toJson(Map.of("APP", projectId, "MODULE", base.substring(0, base.length() - 1))).replace("<", "\\u003c");
            String html = new String(content, StandardCharsets.UTF_8)
                    .replaceAll("(?is)<script\\b[^>]*\\bid=[\"']semoss-env[\"'][^>]*>.*?</script>", "")
                    .replaceAll("(?is)<base\\b[^>]*>", "");
            // Rewrite the platform-owned SDK import map to the immutable preview-local copy.
            var document = org.jsoup.Jsoup.parse(html);
            document.outputSettings().prettyPrint(false);
            var importMap = document.getElementById("semoss-sdk-importmap");
            if (importMap != null) importMap.html(JSON.toJson(Map.of("imports", Map.of("@semoss/sdk", base + "sdk/index.mjs"))));
            else if (document.selectFirst("script[type=importmap]") == null)
                document.head().prepend("<script type=\"importmap\">" + JSON.toJson(Map.of("imports", Map.of("@semoss/sdk", base + "sdk/index.mjs"))) + "</script>");
            html = document.html();
            String injection = "<base href=\"" + root + "\"><script id=\"semoss-env\" type=\"application/json\">" + env + "</script>"
                    + "<script>" + bootstrap(base, root) + "</script>";
            var head = Pattern.compile("(?i)<head(?:\\s[^>]*)?>").matcher(html);
            html = head.find() ? html.substring(0, head.end()) + injection + html.substring(head.end()) : injection + html;
            return new Asset(html.getBytes(StandardCharsets.UTF_8), MIME.get("html"));
        }
        return new Asset(content, mime(name));
    }

    private static String bootstrap(String base, String root) {
        // Apps with older SDKs may set MODULE themselves. Rewrite only known SDK URLs; other API traffic is blocked.
        return "(()=>{window.__semossTestBlocked=false;window.__semossTestViolations=[];const blocked=(url,code)=>{window.__semossTestBlocked=true;if(window.__semossTestViolations.length<10)window.__semossTestViolations.push({url,code})};document.addEventListener('securitypolicyviolation',e=>blocked(e.blockedURI,'TEST_PREVIEW_CSP_BLOCKED'));const base=" + JSON.toJson(base) + ",root=" + JSON.toJson(root) + ";"
                + "const rewrite=u=>{const p=new URL(u,location.href);"
                + "if(p.pathname.endsWith('/api/engine/runPixel'))return base+'api/engine/runPixel';"
                + "if(p.pathname.endsWith('/api/config'))return base+'api/config';"
                + "if(p.pathname.includes('/api/')){blocked(p.href,'TEST_PREVIEW_ENDPOINT_UNSUPPORTED');throw Error('UNSUPPORTED_TEST_DATABASE_OPERATION: This API is unavailable in the test preview');}"
                + "return u;};"
                + "const f=window.fetch;window.fetch=(u,o)=>{try{return f.call(window,u instanceof Request?new Request(rewrite(u.url),u):rewrite(String(u)),o)}catch(e){return Promise.reject(e)}};"
                + "const x=XMLHttpRequest.prototype.open;XMLHttpRequest.prototype.open=function(m,u,...a){return x.call(this,m,rewrite(String(u)),...a)};"
                + "document.addEventListener('click',e=>{const a=e.target.closest?.('a[href]');if(!a)return;const h=a.getAttribute('href');"
                + "if(h?.startsWith('/')&&!h.startsWith('//')&&!h.startsWith(root)){e.preventDefault();location.href=root+h.slice(1)}});"
                + "})();";
    }

    private static String mime(String name) { return MIME.get(name.substring(name.lastIndexOf('.') + 1)); }
    private static String hash(String value) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8))); }
        catch (java.security.NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }

    /** Release preview resources on a normal web application undeploy/restart. */
    public static synchronized void shutdown() {
        shuttingDown = true;
        REAPER.shutdownNow();
        for (AppTestEnvironment environment : ACTIVE.values()) environment.close();
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        ACTIVE.remove(id, this);
        access.close();
        try { database.close(); Files.deleteIfExists(scratch); }
        catch (IOException | SQLException failure) { LOGGER.warn("Could not remove expired app test database: environment={}", id, failure); }
    }
}
