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

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Real Chromium, Pixel interpreter and H2, using the existing runner without a deployed server. */
class AppTestDatabaseReplayUnitTests {
    @TempDir Path temporary;
    private static final String SCHEMA = "CREATE TABLE TASKS (ID INTEGER PRIMARY KEY, TITLE VARCHAR(80));";
    private static final String SEED = "INSERT INTO TASKS (ID,TITLE) VALUES (1,'Seed task');";
    private static final String HTML = """
            <html><head></head><body>
            <div data-testid="title">Loading</div>
            <button data-testid="save">Save</button>
            <button data-testid="other">Other database</button>
            <a data-testid="details" href="tasks/t1">Details</a>
            <a data-testid="home">Home</a>
            <script>
            const pixel = async expression => {
                const response = await fetch('/api/engine/runPixel', {
                    method: 'POST', headers: {'Content-Type':'application/x-www-form-urlencoded'},
                    body: new URLSearchParams({expression})
                });
                return response.json();
            };
            const query = (sql, engine='source') => pixel('SqlQuery(database=['+JSON.stringify(engine)+'],query=['+JSON.stringify(sql)+'],commit=[true]);');
            const read = async () => {
                const result = await query('SELECT TITLE FROM TASKS WHERE ID=1');
                document.querySelector('[data-testid="title"]').textContent = result.pixelReturn[0].output.data.values[0][0];
            };
            document.querySelector('[data-testid="home"]').href = document.baseURI;
            document.querySelector('[data-testid="save"]').onclick = async () => {
                await query("UPDATE TASKS SET TITLE='Changed' WHERE ID=1"); await read();
            };
            document.querySelector('[data-testid="other"]').onclick = () => query('SELECT TITLE FROM TASKS','other');
            (async () => {
                await fetch('/api/config'); await pixel('LoadApp("app");'); await read();
            })();
            </script></body></html>
            """;

    @Test void replaysMultipageDatabaseChangesAndResetsEveryTestAndRepeat() {
        var steps = new ArrayList<Map<String,Object>>();
        steps.add(check("Seed task"));
        steps.add(Map.of("type", "CLICK", "selector", "[data-testid=\"details\"]"));
        steps.add(Map.of("type", "CLICK", "selector", "[data-testid=\"save\"]", "pagePath", "tasks/t1"));
        steps.add(check("Changed"));
        steps.add(Map.of("type", "CLICK", "selector", "[data-testid=\"home\"]"));
        steps.add(check("Changed"));
        var report = run(List.of(recording("Write and navigate", "", steps), recording("Read only", "tasks/t1", List.of(check("Seed task")))), 2);
        var tests = (List<?>)report.get("tests");
        for (Object test : tests) assertEquals("PASSED", ((Map<?,?>)test).get("status"), report.toString());
        assertEquals("ISOLATED_H2", report.get("mode"));
        assertEquals("fixture", report.get("fixtureHash"));
    }

    @Test void rejectsUnmappedDatabaseEvenIfTheAppIgnoresItsRequestFailure() {
        var report = run(List.of(recording("Reject other engine", "", List.of(check("Seed task"),
                Map.of("type", "CLICK", "selector", "[data-testid=\"other\"]"), check("Seed task")))), 1);
        var test = (Map<?,?>)((List<?>)report.get("tests")).getFirst();
        assertEquals("ERROR", test.get("status"), report.toString());
        assertEquals("TEST_DATABASE_UNSUPPORTED", test.get("classification"));
        assertTrue(test.get("reason").toString().contains("UNMAPPED_TEST_DATABASE"));
    }

    private Map<String,Object> run(List<Map<String,Object>> tests, int repeat) {
        var definition = new AppTestEnvironment.Definition("source", SCHEMA, SEED, "fixture");
        var portal = new AppTestEnvironment.Portal(Map.of("index.html", HTML.getBytes(StandardCharsets.UTF_8)), "portal");
        return AppTestRunner.run(UUID.randomUUID().toString(), "app", URI.create("http://semoss-app-test.invalid/"),
                null, null, tests, temporary, System.nanoTime() + 90_000_000_000L, repeat, progress -> {},
                "NATIVE:editor", definition, portal);
    }

    private static Map<String,Object> check(String title) {
        return Map.of("type", "ASSERT_TEXT", "selector", "[data-testid=\"title\"]", "text", title, "exact", true);
    }

    private static Map<String,Object> recording(String name, String start, List<Map<String,Object>> steps) {
        return Map.of("id", UUID.randomUUID().toString(), "name", name, "description", name,
                "version", 1, "startPath", start, "steps", steps);
    }
}
