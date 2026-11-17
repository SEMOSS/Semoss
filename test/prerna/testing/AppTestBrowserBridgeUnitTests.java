package prerna.testing;

import static org.junit.jupiter.api.Assertions.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import com.microsoft.playwright.*;
import com.sun.net.httpserver.HttpServer;

/** Real browser coverage for navigation and per-iframe routing before any application script runs. */
class AppTestBrowserBridgeUnitTests {
    @Test void keepsTestRoutingAcrossPagesReloadAndExitWithoutChangingNormalClients() throws Exception {
        var expired = new AtomicBoolean();
        var testRequests = new AtomicInteger();
        var normalRequests = new AtomicInteger();
        String token = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/Monolith/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            String header = exchange.getRequestHeaders().getFirst(EngineTestContexts.HEADER);
            String body; String type = "text/html"; int status = 200;
            if (path.endsWith("app-test-worker.js")) {
                body = AppTestBrowserBridge.script(); type = "application/javascript";
                exchange.getResponseHeaders().set("Service-Worker-Allowed", "/Monolith/");
            } else if (path.endsWith("app-test-context")) {
                body = "{}"; type = "application/json";
                if (!token.equals(header) || expired.get()) status = 403;
            } else if (path.endsWith("/api/database")) {
                if (header == null) normalRequests.incrementAndGet(); else testRequests.incrementAndGet();
                body = header == null ? "PRODUCTION" : "TEST"; type = "text/plain";
            } else if (path.contains("/public_home/")) {
                body = "<html><body><h1>App</h1><a id='next' href='details.html'>Next</a><output id='result'></output>"
                    + "<script>fetch('/Monolith/api/database').then(r=>r.text()).then(v=>document.querySelector('#result').textContent=v)</script></body></html>";
            } else body = "<html><body><iframe id='app'></iframe><iframe id='normal'></iframe></body></html>";
            if (token.equals(header) && !expired.get()) exchange.getResponseHeaders().set("X-Semoss-App-Test-Active", token);
            exchange.getResponseHeaders().set("Content-Type", type);
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, bytes.length); exchange.getResponseBody().write(bytes); exchange.close();
        });
        server.start();
        String origin = "http://127.0.0.1:" + server.getAddress().getPort();
        String app = origin + "/Monolith/public_home/app/portals/";
        try (Playwright playwright = Playwright.create(); Browser browser = playwright.chromium().launch(); BrowserContext context = browser.newContext()) {
            Page page = context.newPage(); page.navigate(origin + "/Monolith/");
            page.evaluate("async () => { await navigator.serviceWorker.register('/Monolith/app-test-worker.js',{scope:'/Monolith/'}); await navigator.serviceWorker.ready; }");
            page.waitForFunction("navigator.serviceWorker.controller !== null");
            page.locator("#app").evaluate("(el,url)=>el.src=url", app + "?__semoss_app_test=" + token);
            var target = page.frameLocator("#app");
            target.locator("#result").getByText("TEST", new Locator.GetByTextOptions().setExact(true)).waitFor();
            page.locator("#normal").evaluate("(el,url)=>el.src=url", app);
            page.frameLocator("#normal").locator("#result").getByText("PRODUCTION").waitFor();
            Page ordinaryTab = context.newPage(); ordinaryTab.navigate(app);
            ordinaryTab.locator("#result").getByText("PRODUCTION").waitFor();
            int normalBefore = normalRequests.get();
            target.locator("#next").click();
            target.locator("#result").getByText("TEST").waitFor();
            page.locator("#app").evaluate("el=>el.contentWindow.location.reload()");
            target.locator("#result").getByText("TEST").waitFor();
            assertEquals(normalBefore, normalRequests.get(), "Test navigation must not touch production");
            expired.set(true);
            String failed = (String) page.frames().stream().filter(f -> f.url().contains("details.html")).findFirst().orElseThrow()
                .evaluate("async()=>{const r=await fetch('/Monolith/api/database'); return r.status+':'+await r.text()}");
            assertTrue(failed.startsWith("409:"));
            assertEquals(normalBefore, normalRequests.get());
            page.locator("#app").evaluate("(el,url)=>el.src=url", app + "?__semoss_app_test=" + token + "&__semoss_app_test_exit=" + token);
            target.locator("#result").getByText("PRODUCTION").waitFor();
            assertTrue(testRequests.get() >= 3);
        } finally { server.stop(0); }
    }
}
