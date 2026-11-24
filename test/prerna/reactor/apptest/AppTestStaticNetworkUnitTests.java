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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import com.sun.net.httpserver.HttpServer;

/** Real network + Chromium + Pixel/H2. The served SDK module is a small wire-contract fixture. */
class AppTestStaticNetworkUnitTests {
    @TempDir Path temporary;
    private static final String SCHEMA = "CREATE TABLE ITEMS (ID INTEGER PRIMARY KEY,TITLE VARCHAR(40));";
    private static final String SEED = "INSERT INTO ITEMS VALUES (1,'Seed');";
    private static final String CHUNK = """
            export const query = async sql => {
                const response = await fetch('/Monolith/api/engine/runPixel', {
                    method:'POST', headers:{'Content-Type':'application/x-www-form-urlencoded'},
                    body:new URLSearchParams({expression:'SqlQuery(database=["source"],query=['+JSON.stringify(sql)+']);'})
                });
                return response.json();
            };
            """;
    private static final String HTML = """
            <html><head></head><body><div data-testid="title">Loading</div><button data-testid="save">Save</button>
            <script type="module">
            import {query} from '@semoss/sdk';
            const read = async()=>{const r=await query('SELECT TITLE FROM ITEMS LIMIT 1');document.querySelector('[data-testid="title"]').textContent=r.pixelReturn[0].output.data.values[0][0]};
            document.querySelector('[data-testid="save"]').onclick=async()=>{await query("UPDATE ITEMS SET TITLE='Changed' WHERE ID=1");await read()};
            await read();
            </script></body></html>
            """;

    @Test void loadsPublicSdkAndRelativeChunksWithoutCookiesOrOrdinaryBackendRequests() throws Exception {
        AtomicInteger assets = new AtomicInteger(), backend = new AtomicInteger(), credentials = new AtomicInteger();
        HttpServer server = server(assets, backend, credentials);
        try {
            var test = first(run(server, HTML, List.of(check("Seed"),
                    Map.of("type", "CLICK", "selector", "[data-testid=\"save\"]"), check("Changed"))));
            assertEquals("PASSED", test.get("status"), test.toString());
            assertEquals(2, assets.get());
            assertEquals(0, backend.get());
            assertEquals(0, credentials.get());
        } finally { server.stop(0); }
    }

    @Test void blocksRedirectsFromStaticFilesIntoOrdinaryBackendEndpoints() throws Exception {
        AtomicInteger assets = new AtomicInteger(), backend = new AtomicInteger(), credentials = new AtomicInteger();
        HttpServer server = server(assets, backend, credentials);
        String html = "<html><head><script type='module' src='" + AppTestStaticAssets.roots().getFirst()
                + "sdk/dist/redirect.mjs'></script></head><body>App</body></html>";
        try {
            var test = first(run(server, html, List.of(Map.of("type", "ASSERT_VISIBLE", "selector", "body"))));
            assertEquals("ERROR", test.get("status"), test.toString());
            assertEquals("TEST_PREVIEW_STATIC_REDIRECT_BLOCKED", test.get("classification"));
            assertEquals(0, backend.get());
            assertEquals(0, credentials.get());
        } finally { server.stop(0); }
    }

    @Test void blocksOrdinaryApiRequestsAndWritesEvenWhenTheAppIgnoresErrors() throws Exception {
        AtomicInteger assets = new AtomicInteger(), backend = new AtomicInteger(), credentials = new AtomicInteger();
        HttpServer server = server(assets, backend, credentials);
        String html = "<html><head></head><body>App<script>fetch('/Monolith/api/delete.js').catch(()=>{});"
                + "fetch('" + AppTestStaticAssets.sdkEntry() + "',{method:'POST',body:'write'}).catch(()=>{});</script></body></html>";
        try {
            var test = first(run(server, html, List.of(Map.of("type", "ASSERT_VISIBLE", "selector", "body"))));
            assertEquals("ERROR", test.get("status"), test.toString());
            assertEquals("TEST_PREVIEW_ENDPOINT_UNSUPPORTED", test.get("classification"));
            assertEquals(0, backend.get());
            assertEquals(0, assets.get());
        } finally { server.stop(0); }
    }

    @Test void manualPreviewLoadsNormalSdkUrlsUnderTheSamePolicy() throws Exception {
        AtomicInteger assets = new AtomicInteger(), backend = new AtomicInteger(), credentials = new AtomicInteger();
        HttpServer server = server(assets, backend, credentials);
        var fixture = new AppTestEnvironment.Definition("source", SCHEMA, SEED, "fixture");
        var portal = new AppTestEnvironment.Portal(Map.of("index.html", HTML.getBytes(StandardCharsets.UTF_8)), "portal");
        try (var environment = AppTestEnvironment.open("NATIVE:editor", "app", fixture, portal, true);
                var playwright = com.microsoft.playwright.Playwright.create();
                var browser = playwright.chromium().launch(new com.microsoft.playwright.BrowserType.LaunchOptions().setHeadless(true));
                var context = browser.newContext()) {
            String prefix = "/Monolith/" + environment.basePath();
            server.createContext(prefix, exchange -> {
                String relative = exchange.getRequestURI().getPath().substring(prefix.length());
                try {
                    if (relative.equals("api/engine/runPixel")) {
                        byte[] body = exchange.getRequestBody().readAllBytes();
                        var result = environment.execute(new String(body, StandardCharsets.UTF_8));
                        send(exchange, "application/json", new com.google.gson.Gson().toJson(result));
                    } else {
                        var asset = environment.asset(relative, "/Monolith");
                        exchange.getResponseHeaders().set("Content-Security-Policy", environment.contentSecurityPolicy(origin(server)+"/Monolith"));
                        if (asset == null) { exchange.sendResponseHeaders(404,-1); exchange.close(); }
                        else send(exchange,asset.contentType(),new String(asset.bytes(), StandardCharsets.UTF_8));
                    }
                } catch (Exception failure) { exchange.sendResponseHeaders(500,-1); exchange.close(); }
            });
            var page = context.newPage();
            page.navigate(origin(server)+"/Monolith/"+environment.started().get("previewPath"));
            com.microsoft.playwright.assertions.PlaywrightAssertions.assertThat(page.locator("[data-testid=\"title\"]")).hasText("Seed");
            page.locator("[data-testid=\"save\"]").click();
            com.microsoft.playwright.assertions.PlaywrightAssertions.assertThat(page.locator("[data-testid=\"title\"]")).hasText("Changed");
            assertEquals(0,backend.get());
            assertEquals(2,assets.get());
            // Manual preview remains in the user's browser; cookies on public static GETs are normal browser behavior.
            assertTrue(environment.diagnostics().isEmpty(), environment.diagnostics().toString());
            page.evaluate("() => fetch('/Monolith/api/delete.js').catch(()=>null)");
            page.evaluate("() => {try {const x=new XMLHttpRequest(); x.open('POST','/Monolith/api/unsafe'); x.send('write')} catch {}} ");
            assertEquals(0,backend.get());
            assertEquals(Boolean.TRUE,page.evaluate("() => window.__semossTestBlocked"));
        } finally { server.stop(0); }
    }

    private HttpServer server(AtomicInteger assets, AtomicInteger backend, AtomicInteger credentials) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        String sdkRoot = AppTestStaticAssets.roots().getFirst()+"sdk/dist/";
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            if (exchange.getRequestHeaders().getFirst("Cookie") != null || exchange.getRequestHeaders().getFirst("Authorization") != null) credentials.incrementAndGet();
            if (path.startsWith(sdkRoot)) {
                assets.incrementAndGet();
                if (path.endsWith("redirect.mjs")) {
                    exchange.getResponseHeaders().set("Location","/Monolith/api/write");
                    exchange.sendResponseHeaders(302,-1); exchange.close();
                } else {
                    // Ensure a response cookie is not carried into the next background asset download.
                    exchange.getResponseHeaders().add("Set-Cookie","asset-session=anonymous; Path=/");
                    send(exchange,"application/javascript",path.endsWith("index.mjs") ? "export {query} from './chunk.js';" : CHUNK);
                }
            } else { backend.incrementAndGet(); send(exchange,"application/json","{}"); }
        });
        server.start(); return server;
    }

    private Map<String,Object> run(HttpServer server, String html, List<Map<String,Object>> steps) {
        var fixture = new AppTestEnvironment.Definition("source", SCHEMA, SEED, "fixture");
        var portal = new AppTestEnvironment.Portal(Map.of("index.html",html.getBytes(StandardCharsets.UTF_8)),"portal");
        var recording = Map.<String,Object>of("id",UUID.randomUUID().toString(),"name","Network policy","description","Verify selective static access and isolated SQL","version",1,"startPath","","steps",steps);
        return AppTestRunner.run(UUID.randomUUID().toString(),"app",AppTestRunner.appRoot(origin(server)+"/Monolith","app"),null,null,
                List.of(recording),temporary,System.nanoTime()+45_000_000_000L,1,progress->{},"NATIVE:editor",fixture,portal);
    }

    @SuppressWarnings("unchecked") private static Map<String,Object> first(Map<String,Object> report) { return (Map<String,Object>)((List<?>)report.get("tests")).getFirst(); }
    private static String origin(HttpServer server) { return "http://127.0.0.1:"+server.getAddress().getPort(); }
    private static Map<String,Object> check(String title) { return Map.of("type","ASSERT_TEXT","selector","[data-testid=\"title\"]","text",title,"exact",true); }
    private static void send(com.sun.net.httpserver.HttpExchange exchange,String type,String body) throws IOException {
        byte[] bytes=body.getBytes(StandardCharsets.UTF_8);exchange.getResponseHeaders().set("Content-Type",type);
        exchange.sendResponseHeaders(200,bytes.length);exchange.getResponseBody().write(bytes);exchange.close();
    }
}
