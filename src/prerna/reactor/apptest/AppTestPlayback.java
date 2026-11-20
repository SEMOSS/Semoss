package prerna.reactor.apptest;

import static com.microsoft.playwright.assertions.PlaywrightAssertions.assertThat;
import java.net.URI;
import java.util.*;
import com.microsoft.playwright.*;
import com.microsoft.playwright.assertions.LocatorAssertions;
import com.microsoft.playwright.options.WaitForSelectorState;
import com.microsoft.playwright.options.WaitUntilState;

/** Declarative execution and route policy; never expands navigation outside the target app. */
final class AppTestPlayback {
    static final class WrongPage extends AssertionError {
        final String expected;
        WrongPage(String expected, Throwable cause) { super("Expected route was not reached", cause); this.expected = expected; }
    }
    private AppTestPlayback() { }

    static boolean samePage(URI root, String actual, String expected, String policy) {
        if (!AppTestRunner.insideApp(root, actual) || !AppTestRunner.insideApp(root, expected)) return false;
        URI a = URI.create(actual), e = URI.create(expected);
        if (!Objects.equals(a.getRawQuery(), e.getRawQuery())) return false;
        boolean entries = entry(root, a) && entry(root, e);
        if (!("PORTAL_ROOT".equals(policy) && entries) && !Objects.equals(a.getRawPath(), e.getRawPath())) return false;
        String af = a.getRawFragment(), ef = e.getRawFragment();
        if ("PORTAL_ROOT".equals(policy) && entries && rootHash(af) && rootHash(ef)) return true;
        return Objects.equals(af, ef);
    }
    private static boolean entry(URI root, URI value) { return root.getRawPath().equals(value.getRawPath()) || (root.getRawPath() + "index.html").equals(value.getRawPath()); }
    private static boolean rootHash(String value) { return value == null || value.isEmpty() || value.equals("/"); }

    /** Live diagnostics exclude query values and non-route fragments. */
    static String safeRoute(URI root, String url) {
        if (!AppTestRunner.insideApp(root, url)) return "<outside target app or unavailable>";
        URI value = URI.create(url);
        String hash = value.getRawFragment();
        return value.getRawPath().substring(root.getRawPath().length())
            + (value.getRawQuery() == null ? "" : "?<redacted>")
            + (hash == null ? "" : hash.startsWith("/") ? "#" + hash.split("\\?", 2)[0] : "#<fragment>");
    }
    static void waitForRoute(Page page, URI root, String relative, String policy, double timeout) {
        String expected = root.resolve(AppTestRecordings.relativePath(relative)).toString();
        try {
            page.waitForURL(url -> samePage(root, url, expected, policy), new Page.WaitForURLOptions().setTimeout(timeout).setWaitUntil(WaitUntilState.DOMCONTENTLOADED));
        } catch (TimeoutError e) { throw new WrongPage(expected, e); }
    }
    static double timeout(Map<String,Object> step, long deadline) {
        double remaining = (deadline - System.nanoTime()) / 1_000_000.0;
        if (remaining <= 0) throw new IllegalStateException("Run time limit exceeded");
        double configured = step.get("timeoutMs") instanceof Number n ? n.doubleValue() : 5000;
        return Math.max(1, Math.min(configured, remaining));
    }
    static void execute(Page page, URI root, Map<String,Object> step, String policy, long deadline) {
        double timeout = timeout(step, deadline);
        page.setDefaultTimeout(timeout);
        page.setDefaultNavigationTimeout(timeout);
        long actionDeadline = Math.min(deadline, System.nanoTime() + (long)(timeout * 1_000_000));
        if (step.containsKey("pagePath")) waitForRoute(page, root, String.valueOf(step.get("pagePath")), policy, timeout);
        String type = String.valueOf(step.get("type"));
        // Route waits consume the same per-step budget as the action.
        if (type.equals("ASSERT_URL")) { waitForRoute(page, root, String.valueOf(step.get("path")), policy, timeout(step, actionDeadline)); return; }
        if (type.equals("NAVIGATE")) { page.navigate(root.resolve(AppTestRecordings.relativePath(step.get("path"))).toString(), new Page.NavigateOptions().setWaitUntil(WaitUntilState.DOMCONTENTLOADED).setTimeout(timeout(step, actionDeadline))); return; }
        Locator target = locator(page, step);
        timeout = timeout(step, actionDeadline);
        page.setDefaultTimeout(timeout);
        switch (type) {
        case "CLICK" -> target.click(new Locator.ClickOptions().setTimeout(timeout));
        case "FILL" -> {
            if ("password".equalsIgnoreCase(target.getAttribute("type"))) throw new IllegalArgumentException("Password replay is not supported");
            target.fill(String.valueOf(step.get("value")), new Locator.FillOptions().setTimeout(timeout(step, actionDeadline)));
        }
        case "SELECT" -> target.selectOption(String.valueOf(step.get("value")), new Locator.SelectOptionOptions().setTimeout(timeout));
        case "CHECK" -> target.setChecked((Boolean)step.get("checked"), new Locator.SetCheckedOptions().setTimeout(timeout));
        case "KEY" -> target.press(String.valueOf(step.get("key")), new Locator.PressOptions().setTimeout(timeout));
        case "SCROLL" -> target.evaluate("(el,p)=>el.scrollTo(p.x,p.y)", Map.of("x",step.get("x"),"y",step.get("y")));
        case "ASSERT_TEXT" -> {
            if (Boolean.TRUE.equals(step.get("exact"))) assertThat(target).hasText(String.valueOf(step.get("text")), new LocatorAssertions.HasTextOptions().setTimeout(timeout));
            else assertThat(target).containsText(String.valueOf(step.get("text")), new LocatorAssertions.ContainsTextOptions().setTimeout(timeout));
        }
        case "ASSERT_VISIBLE" -> assertThat(target).isVisible(new LocatorAssertions.IsVisibleOptions().setTimeout(timeout));
        case "ASSERT_HIDDEN" -> assertThat(target).isHidden(new LocatorAssertions.IsHiddenOptions().setTimeout(timeout));
        case "ASSERT_ABSENT" -> assertThat(target).hasCount(0, new LocatorAssertions.HasCountOptions().setTimeout(timeout));
        case "ASSERT_COUNT" -> assertThat(target).hasCount(((Number)step.get("count")).intValue(), new LocatorAssertions.HasCountOptions().setTimeout(timeout));
        case "ASSERT_VALUE" -> assertThat(target).hasValue(String.valueOf(step.get("value")), new LocatorAssertions.HasValueOptions().setTimeout(timeout));
        case "ASSERT_CHECKED" -> assertThat(target).isChecked(new LocatorAssertions.IsCheckedOptions().setChecked((Boolean)step.get("checked")).setTimeout(timeout));
        case "WAIT_FOR" -> target.waitFor(new Locator.WaitForOptions().setState(WaitForSelectorState.valueOf(String.valueOf(step.get("state")).toUpperCase(Locale.ROOT))).setTimeout(timeout));
        case "DRAG" -> { Map<String,Object> destination = new LinkedHashMap<>(step); destination.put("selector",step.get("targetSelector")); target.dragTo(locator(page,destination), new Locator.DragToOptions().setTimeout(timeout)); }
        default -> throw new IllegalArgumentException("Unsupported app test step");
        }
    }
    @SuppressWarnings("unchecked")
    static Locator locator(Page page, Map<String,Object> step) {
        FrameLocator frame = null;
        for (String selector : (List<String>)step.getOrDefault("framePath",List.of())) frame = frame == null ? page.frameLocator("css="+selector) : frame.frameLocator("css="+selector);
        return frame == null ? page.locator("css="+step.get("selector")) : frame.locator("css="+step.get("selector"));
    }
    static boolean ambiguous(Throwable e) { return e instanceof PlaywrightException && String.valueOf(e.getMessage()).contains("strict mode violation"); }
    static boolean invalidSelector(Throwable e) { String message = String.valueOf(e.getMessage()).toLowerCase(Locale.ROOT); return e instanceof PlaywrightException && (message.contains("while parsing css selector") || message.contains("not a valid selector") || message.contains("unknown engine")); }
    static List<String> targets(Page page, Map<String,Object> step) {
        try {
            Map<String,Object> scope = new LinkedHashMap<>(step); scope.put("selector","[data-testid]");
            Object raw = locator(page,scope).evaluateAll("els=>els.slice(0,200).filter(el=>!el.closest('[data-private],[data-recording-ignore]')&&!/password|secret|token|credit|ssn/i.test([el.id,el.getAttribute('name'),el.getAttribute('data-testid')].join(' '))).map(el=>el.getAttribute('data-testid')).filter(id=>id&&id.length<=120).slice(0,40)");
            if (raw instanceof List<?> values) return values.stream().filter(String.class::isInstance).map(String.class::cast).distinct().toList();
        } catch (RuntimeException ignored) { /* Diagnostics must not replace the actual failure. */ }
        return List.of();
    }
    @SuppressWarnings("unchecked")
    static Map<String,Object> variables(Map<String,Object> recording, String runId, int attempt) {
        if (!Boolean.TRUE.equals(recording.get("useVariables"))) return recording;
        Map<String,String> variables = Map.of("runId",runId,"testId",String.valueOf(recording.get("id")),"attemptId",runId+"-"+recording.get("id")+"-"+attempt,"uniqueId",UUID.randomUUID().toString());
        Map<String,Object> result = new LinkedHashMap<>(recording);
        for (String field : List.of("steps","cleanupSteps")) if (recording.get(field) instanceof List<?> steps) {
            List<Map<String,Object>> expanded = new ArrayList<>();
            for (Object raw : steps) {
                Map<String,Object> step = new LinkedHashMap<>((Map<String,Object>)raw);
                for (String key : List.of("value","text","selector","targetSelector","path","pagePath")) if (step.get(key) instanceof String text) {
                    for (var variable : variables.entrySet()) text = text.replace("{{"+variable.getKey()+"}}",variable.getValue());
                    step.put(key,text);
                }
                expanded.add(step);
            }
            result.put(field,expanded);
        }
        // Keep the hash tied to the saved template rather than generated values.
        result.put("templateHash",AppTestHistory.definitionHash(recording));
        return result;
    }
}
