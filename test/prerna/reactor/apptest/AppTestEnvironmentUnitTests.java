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

import static org.junit.jupiter.api.Assertions.*;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AppTestEnvironmentUnitTests {
    @TempDir Path temporary;
    private static final String SCHEMA = "CREATE TABLE TASKS (ID INTEGER PRIMARY KEY, TITLE VARCHAR(80));";
    private static final String SEED = "INSERT INTO TASKS (ID,TITLE) VALUES (1,'Seed task');";

    @Test void validatesConfigurationWithoutAcceptingMissingRequiredInputs() {
        assertNull(AppTestEnvironment.validate(Map.of("enabled", false)));
        assertThrows(IllegalArgumentException.class, () -> AppTestEnvironment.validate(Map.of("enabled", true)));
        var definition = AppTestEnvironment.validate(Map.of("enabled", true, "sourceEngineId", "source", "schemaSql", SCHEMA, "seedSql", ""));
        assertEquals(64, definition.fixtureHash().length());
        assertThrows(IllegalArgumentException.class, () -> AppTestEnvironment.validate(Map.of("enabled", true, "sourceEngineId", "../source", "schemaSql", SCHEMA, "seedSql", "")));
    }

    @Test void servesImmutableAssetsAndSpaRoutesButRejectsTraversalAndMissingAssets() throws Exception {
        Files.writeString(temporary.resolve("index.html"), "<html><head></head><body>App</body></html>");
        var portal = AppTestEnvironment.portal(temporary);
        var definition = new AppTestEnvironment.Definition("source", SCHEMA, SEED, "fixture");
        try (var environment = AppTestEnvironment.open("NATIVE:editor", "app", definition, portal, true)) {
            Files.writeString(temporary.resolve("index.html"), "Changed");
            var page = environment.asset("public_home/app/portals/tasks/t1", "/Monolith");
            String html = new String(page.bytes(), StandardCharsets.UTF_8);
            assertTrue(html.contains("App</body>"));
            assertFalse(html.contains("Changed"));
            assertTrue(html.contains("semoss-env"));
            assertTrue(html.contains("/Monolith/app-test-preview/"));
            assertNull(environment.asset("public_home/app/portals/missing.js", "/Monolith"));
            assertThrows(IllegalArgumentException.class, () -> environment.asset("public_home/app/portals/../secret", ""));
            assertThrows(IllegalArgumentException.class, () -> environment.asset("public_home/other/portals/", ""));
            assertTrue(environment.contentSecurityPolicy("https://example.test/Monolith").contains("connect-src https://example.test/Monolith/app-test-preview/"));
        }
    }

    @Test void capabilitiesAndWritesAreScopedAndEveryPreviewStartsFromSeed() throws Exception {
        var definition = new AppTestEnvironment.Definition("source", SCHEMA, SEED, "fixture");
        var portal = new AppTestEnvironment.Portal(Map.of("index.html", "<html></html>".getBytes(StandardCharsets.UTF_8)), "portal");
        String id;
        String secret;
        try (var first = AppTestEnvironment.open("NATIVE:editor", "app", definition, portal, true);
                var second = AppTestEnvironment.open("NATIVE:editor", "app", definition, portal, true)) {
            id = (String) first.started().get("environmentId");
            secret = first.basePath().split("/")[2];
            assertSame(first, AppTestEnvironment.authenticate(id, secret));
            assertThrows(SecurityException.class, () -> AppTestEnvironment.authenticate(id, "wrong"));
            assertThrows(SecurityException.class, () -> AppTestEnvironment.stop("MICROSOFT:editor", "app", (String)first.started().get("environmentId")));
            assertThrows(SecurityException.class, () -> first.execute(form("SqlQuery(database=[\"other\"],query=[\"SELECT TITLE FROM TASKS\"]);")));
            assertEquals(List.of(List.of("Seed task")), rows(second.execute(form("SqlQuery(database=[\"source\"],query=[\"SELECT TITLE FROM TASKS\"]);"))));
            first.execute(form("SqlQuery(query=[\"UPDATE TASKS SET TITLE='Changed' WHERE ID=1\"],database=[\"source\"],commit=[true]);"));
            assertEquals(List.of(List.of("Changed")), rows(first.execute(form("SqlQuery(database=[\"source\"],query=[\"SELECT TITLE FROM TASKS\"],limit=[10]);"))));
            assertEquals(List.of(List.of("Seed task")), rows(second.execute(form("SqlQuery(database=[\"source\"],query=[\"SELECT TITLE FROM TASKS\"]);"))));
            assertThrows(IllegalArgumentException.class, () -> first.execute("expression=a&expression=b"));
            assertThrows(IllegalArgumentException.class, () -> first.execute("expression=%ZZ"));
        }
        assertThrows(SecurityException.class, () -> AppTestEnvironment.authenticate(id, secret));
    }

    @Test void sdkBootstrapDoesNotLoadOrdinaryAppReactors() throws Exception {
        var definition = new AppTestEnvironment.Definition("source", SCHEMA, "", "fixture");
        var portal = new AppTestEnvironment.Portal(Map.of("index.html", new byte[0]), "portal");
        try (var environment = AppTestEnvironment.open("NATIVE:editor", "app", definition, portal, false)) {
            assertNotNull(environment.execute(form("META | init;" )).get("insightID"));
            assertNotNull(environment.execute(form("LoadApp(\"app\");")).get("insightID"));
            assertNotNull(environment.execute(form("META | LoadApp( \"app\" );")).get("insightID"));
            assertNotNull(environment.execute(form("LoadApp(project=[\"app\"]);")).get("insightID"));
            assertNotNull(environment.execute(form("LoadApp(project=\"app\");")).get("insightID"));
            assertNotNull(environment.execute(form("SqlQuery(database=\"source\",query=\"SELECT TITLE FROM TASKS LIMIT 1\",limit=10,commit=true);")).get("insightID"));
            assertThrows(SecurityException.class, () -> environment.execute(form("SqlQuery(database=[\"source\",query=\"SELECT TITLE FROM TASKS\");")));
            assertNotNull(environment.execute(form("DropInsight()" )).get("insightID"));
            assertEquals(Map.of("NATIVE", "Regression test preview"), environment.sdkConfig().get("logins"));
            assertEquals(List.of(), environment.sdkConfig().get("availableProviders"));
            assertThrows(SecurityException.class, () -> environment.execute(form("LoadApp(\"other\");")));
            assertThrows(SecurityException.class, () -> environment.execute(form("Py(\"print(1)\");")));
        }
    }

    @Test void readsOnlyImmutablePublicJsonSettingsOfTheCurrentApp() throws Exception {
        var definition = new AppTestEnvironment.Definition("source", SCHEMA, SEED, "fixture");
        var portal = new AppTestEnvironment.Portal(Map.of("index.html", new byte[0]), "portal",
                Map.of("public/settings.json", "{\"databaseId\":\"source\"}"));
        try (var environment = AppTestEnvironment.open("NATIVE:editor", "app", definition, portal, false)) {
            var response = environment.execute(form("GetAppAssets(project=[\"app\"],filePath=[\"public/settings.json\"]);"));
            var result = (Map<?,?>)((List<?>)response.get("pixelReturn")).getFirst();
            assertEquals("{\"databaseId\":\"source\"}", result.get("output"));
            assertNotNull(environment.execute(form("GetAppAssets(project=\"app\",filePath=\"public/settings.json\");")).get("pixelReturn"));
            assertThrows(SecurityException.class, () -> environment.execute(form("GetAppAssets(project=[\"other\"],filePath=[\"public/settings.json\"]);")));
            assertThrows(SecurityException.class, () -> environment.execute(form("GetAppAssets(project=[\"app\"],filePath=[\"private.json\"]);")));
            assertThrows(SecurityException.class, () -> environment.execute(form("SaveAppAssets(project=[\"app\"],filePath=[\"public/settings.json\"],content=[\"changed\"]);")));
        }
    }

    @Test void usesThePublicPlatformSdkUrlWithoutLoadingFrontendFiles() throws Exception {
        Path portalFolder = Files.createDirectory(temporary.resolve("portals"));
        Files.writeString(portalFolder.resolve("index.html"), "<html><head><script id=\"semoss-sdk-importmap\" type=\"importmap\">{\"imports\":{\"@semoss/sdk\":\"/old/frontend/index.mjs\"}}</script></head><body>App</body></html>");
        var portal = AppTestEnvironment.portal(portalFolder);
        var fixture = new AppTestEnvironment.Definition("source", SCHEMA, SEED, "fixture");
        try (var environment = AppTestEnvironment.open("NATIVE:editor", "app", fixture, portal, false)) {
            String html = new String(environment.asset("public_home/app/portals/", "/Monolith").bytes(), StandardCharsets.UTF_8);
            assertTrue(html.contains(AppTestStaticAssets.sdkEntry()));
            assertFalse(html.contains("/old/frontend/"));
            assertFalse(html.contains(environment.basePath() + "sdk/"));
            String policy = environment.contentSecurityPolicy("https://example.test/Monolith");
            assertTrue(policy.contains("https://example.test" + AppTestStaticAssets.roots().getFirst()));
            assertFalse(policy.contains("script-src 'self'"));
        }
    }

    @Test void retainsFirstDiagnosticAndRedactsCapabilitiesQueriesAndSqlValues() throws Exception {
        var fixture = new AppTestEnvironment.Definition("source", SCHEMA, SEED, "fixture");
        var portal = new AppTestEnvironment.Portal(Map.of("index.html", new byte[0]), "portal");
        try (var environment = AppTestEnvironment.open("NATIVE:editor", "app", fixture, portal, false)) {
            environment.blockedRequest("GET", "https://user:password@example.test/Monolith/" + environment.basePath() + "public_home/app/portals/assets/main.js?token=private-value#secret", "script", "TEST_PREVIEW_ASSET_MISSING");
            for (int i=0; i<20; i++) environment.blockedRequest("POST", "/api/other"+i, "fetch", "TEST_PREVIEW_ENDPOINT_UNSUPPORTED");
            assertEquals(10, environment.diagnostics().size());
            assertTrue(environment.failure().contains("portal/assets/main.js"));
            String json = new com.google.gson.Gson().toJson(environment.diagnostics());
            for (String secret : List.of("password", "private-value", "#secret", environment.basePath())) assertFalse(json.contains(secret));
        }
        try (var environment = AppTestEnvironment.open("NATIVE:editor", "app", fixture, portal, false)) {
            assertThrows(IllegalArgumentException.class, () -> environment.execute(form("SqlQuery(database=[\"source\"],query=[\"SELECT TITLE FROM TASKS WHERE TITLE='private-value' UNION SELECT TITLE FROM TASKS\"]);")));
            String json = new com.google.gson.Gson().toJson(environment.diagnostics());
            assertFalse(json.contains("private-value"));
            assertTrue(json.contains("sqlShape"));
            assertTrue(environment.pixelError().containsKey("diagnostics"));
            assertThrows(IllegalArgumentException.class, () -> environment.execute(form("SqlQuery(database=\"source\",query=\"SELECT FILE_READ('private-path') FROM TASKS\");")));
            String scalar = environment.diagnostics().getLast().toString();
            assertTrue(scalar.contains("FILE_READ"));
            assertTrue(scalar.contains("sqlShape"));
            assertFalse(scalar.contains("private-path"));
        }
    }

    @Test void distinguishesSqlExecutionFailuresFromUnsupportedSyntaxWithoutReturningDriverText() throws Exception {
        var fixture = new AppTestEnvironment.Definition("source", SCHEMA, SEED, "fixture");
        var portal = new AppTestEnvironment.Portal(Map.of("index.html", new byte[0]), "portal");
        try (var environment = AppTestEnvironment.open("NATIVE:editor", "app", fixture, portal, false)) {
            assertThrows(RuntimeException.class, () -> environment.execute(form("SqlQuery(database=[\"source\"],query=[\"SELECT MISSING_COLUMN FROM TASKS\"]);")));
            var first = environment.diagnostics().getFirst();
            assertEquals("TEST_DATABASE_SQL_ERROR", first.get("code"));
            assertTrue(first.get("sqlState").toString().matches("[A-Z0-9]{5}"));
            assertFalse(environment.failure().contains("MISSING_COLUMN"));
            assertTrue(environment.failure().contains("SQL state"));
        }
    }

    private static String form(String expression) {
        return "expression=" + URLEncoder.encode(expression, StandardCharsets.UTF_8);
    }

    private static Object rows(Map<String,Object> response) {
        Map<?,?> result = (Map<?,?>)((List<?>)response.get("pixelReturn")).getFirst();
        return ((Map<?,?>)((Map<?,?>)result.get("output")).get("data")).get("values");
    }
}
