package prerna.reactor.playwright;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.microsoft.playwright.JSHandle;
import com.microsoft.playwright.Locator;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.PlaywrightException;
import com.microsoft.playwright.options.AriaRole;
import com.microsoft.playwright.options.LoadState;

/**
 * Utility class for session-related Playwright operations
 */
public class SessionUtility {

    static Map<String, Object> response = new HashMap<String, Object>();

    /**
     * Apply a step to the session and detect page changes
     *
     * @param session The session to apply the step to
     * @param step    The step to apply
     * @param tabId
     * @return true if page changed, false otherwise
     */

    public static Map<String, Object> applyStep(Session session, Step step, String tabId) {
	    Page page = session.tabPages.get(tabId);
	    long startTime = System.currentTimeMillis();
	    boolean pageChanged = false;
        response.put("isNewTab", false);

	    try {
	        String urlBefore = page.url();
	        AtomicBoolean networkTriggered = new AtomicBoolean(false);

	        JSHandle mutationPromise = createMutationObserver(page);

	        page.onRequest(req -> {
	            if ("xhr".equals(req.resourceType()) || "fetch".equals(req.resourceType())) {
	                networkTriggered.set(true);
	            }
	        });

	        executeStepAction(page, step, urlBefore, session);

	        if (step.waitAfterMs() != null && step.waitAfterMs() > 0) {
	            page.waitForTimeout(step.waitAfterMs());
	        }

	        boolean sameUrl = urlBefore.equals(page.url());
	        if (sameUrl && !networkTriggered.get()) {
	            waitForPageOrElement(page, step);
	            pageChanged = detectPageChange(mutationPromise);
	        } else {
	            pageChanged = true;
	        }

	        long elapsed = System.currentTimeMillis() - startTime;
	        System.out.printf("[STEP] %-10s took %d ms (pageChanged=%s)%n",
	                step.type(), elapsed, pageChanged);
            response.put("isPageChanged", pageChanged);
            return response;

	    } catch (Exception e) {
	        System.out.println("Failed to apply step: " + e);
            response.put("isPageChanged", true);
            return response;
	    }
	}

	private static void waitForPageOrElement(Page page, Step step) {
	    try {
	        Selector selector = step.selector();
	        String selectorValue = selector != null ? selector.value() : null;

	        page.waitForFunction(
	            "sel => document.readyState === 'complete' || !!document.querySelector(sel)",
	            selectorValue,
	            new Page.WaitForFunctionOptions().setTimeout(800)
	        );
	    } catch (PlaywrightException e) {
	        System.out.println("Non-blocking wait timeout (safe): " + e.getMessage());
	    }
	}


    private static void executeStepAction(Page page, Step step, String urlBefore, Session session) {
        switch (step.type()) {
            case NAVIGATE -> navigateStep(page, step);
            case CLICK -> clickStep(page, step, urlBefore, session);
            case TYPE -> typeStep(page, step);
            case SCROLL -> scrollStep(page, step);
            case WAIT -> waitStep(page, step);
        }
    }

    private static Locator resolveLocator(Page page, Selector sel) {
        System.out.println("Resolving this locator: " + sel);
        if (sel == null || sel.value() == null) return null;
        String strat = sel.strategy();
        String val = sel.value();
        try {
            Locator loc = switch (strat) {
                case "id"          -> page.locator("#" + cssEscapeIdent(val));
                case "testId"      -> page.getByTestId(val);
                case "label"       -> page.getByLabel(val);
                case "placeholder" -> page.getByPlaceholder(val);
                case "text"        -> page.getByText(val);
                case "css"         -> page.locator(val);
                case "xpath"       -> page.locator("xpath=" + val);
                case "role"        -> {
                    AriaRole role; try { role = AriaRole.valueOf(val.toUpperCase()); } catch (Exception e) { role = null; }
                    yield (role != null) ? page.getByRole(role) : null;
                }
                default -> null;
            };
            if (loc == null) return null;
//            try { if (loc.first().count() == 0) return null; } catch (Exception e) { return null; }
            return loc.first();
        } catch (Exception e) {
            return null;
        }
    }

    private static String cssEscapeIdent(String s) {
        if (s == null || s.isEmpty()) return "";
        StringBuilder out = new StringBuilder(s.length() * 2);
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            boolean first = (i == 0);
            boolean ident =
                    (ch >= 'a' && ch <= 'z') ||
                            (ch >= 'A' && ch <= 'Z') ||
                            (ch >= '0' && ch <= '9') ||
                            ch == '-' || ch == '_';
            if ((first && Character.isDigit(ch)) || !ident) out.append('\\').append(ch);
            else out.append(ch);
        }
        return out.toString();
    }

    private static boolean typeWithFallback(Page page, Step step) {
        boolean typed = false;

        // 1) Selector path
        Locator loc = resolveLocator(page, step.selector());
        if (!typed) typed = focusAndType(loc, step.text());

        // 2) Heal by coords -> locator
        if (!typed && step.coords() != null) {
            Locator healed = null;
            try { healed = healSelector(page, step.coords().x(), step.coords().y()); } catch (Exception ignore) {}
            if (!typed) typed = focusAndType(healed, step.text());
        }

        // 3) Raw coord focus/type but only if the hit is a text control or contentEditable
        if (!typed && step.coords() != null && coordHasHit(page, step.coords().x(), step.coords().y())) {
            try {
                page.mouse().click(step.coords().x(), step.coords().y());
                // Verify focus target is text-capable
                boolean ok = Boolean.TRUE.equals(page.evaluate("() => { " +
                        "const el = document.activeElement; if (!el) return false;" +
                        "return el.tagName === 'INPUT' || el.tagName === 'TEXTAREA' || el.isContentEditable === true;" +
                        "}"));
                if (ok && step.text() != null) {
                    page.keyboard().press("Control+A");
                    page.keyboard().press("Delete");
                    page.keyboard().type(step.text());
                    typed = true;
                }
            } catch (Exception ignore) {}
        }

        if (typed && Boolean.TRUE.equals(step.pressEnter())) {
            try { page.keyboard().press("Enter"); } catch (Exception ignore) {}
        }
        return typed;
    }

    private static boolean coordHasHit(Page page, int x, int y) {
        try {
            Object raw = page.evaluate("({x,y})=>{ const el = document.elementFromPoint(x,y); " +
                            "if(!el) return null; const cs=getComputedStyle(el); " +
                            "return { tag: el.localName, display: cs.display, visibility: cs.visibility, pe: cs.pointerEvents }; }",
                    java.util.Map.of("x", x, "y", y));
            if (raw == null) return false;
            @SuppressWarnings("unchecked")
            Map<String,Object> m = (Map<String,Object>) raw;
            String display = String.valueOf(m.get("display"));
            String visibility = String.valueOf(m.get("visibility"));
            String pe = String.valueOf(m.get("pe"));
            return !"none".equals(display) && !"hidden".equals(visibility) && !"none".equals(pe);
        } catch (Exception ignore) {
            return false;
        }
    }

	private static boolean clickWithFallback(Page page, Step step, String beforeUrl, Session session) {
	    boolean clicked = false;
	    Page newPage = null;
	    
	    // Clear the new tab flag before the click
	    response.put("isNewTab", false);
	    response.remove("newTabId");
	    response.remove("tabTitle");
	
	    // 1) Selector path
	    Locator loc = resolveLocator(page, step.selector());
	    if (isActionable(loc)) {
	        try {
	            // Wait for new page while clicking
	            final Locator finalLoc = loc;
	            newPage = session.ctx.waitForPage(() -> {
	                finalLoc.click(new Locator.ClickOptions().setTimeout(300));
	            });
	            clicked = true;
	        } catch (Exception e) {
	            // Check if click succeeded but no new page
	            try {
	                if (!clicked) {
	                    loc.click(new Locator.ClickOptions().setTimeout(300));
	                    clicked = true;
	                }
	            } catch (Exception ignore) { /* fallback below */ }
	        }
	    }
	
	    // 2) Heal by coords -> locator, then click
	    if (!clicked && step.coords() != null) {
	        Locator healed = null;
	        try { 
	            healed = healSelector(page, step.coords().x(), step.coords().y()); 
	        } catch (Exception ignore) {}
	        
	        if (isActionable(healed)) {
	            try {
	                // Wait for new page while clicking
	                final Locator finalHealed = healed;
	                newPage = session.ctx.waitForPage(() -> {
	                    finalHealed.click(new Locator.ClickOptions().setTimeout(300));
	                });
	                clicked = true;
	            } catch (Exception e) {
	                // Check if click succeeded but no new page
	                try {
	                    if (!clicked) {
	                        healed.click(new Locator.ClickOptions().setTimeout(300));
	                        clicked = true;
	                    }
	                } catch (Exception ignore) { /* fallback below */ }
	            }
	        }
	    }
	
	    // 3) Raw coord click only if there is actually a hit-target
	    if (!clicked && step.coords() != null && coordHasHit(page, step.coords().x(), step.coords().y())) {
	        try {
	            page.mouse().move(step.coords().x(), step.coords().y());
	            final int x = step.coords().x();
	            final int y = step.coords().y();
	            // Wait for new page while clicking
	            newPage = session.ctx.waitForPage(() -> {
	                page.mouse().click(x, y);
	            });
	            clicked = true;
	        } catch (Exception e) {
	            // Check if click succeeded but no new page
	            try {
	                if (!clicked) {
	                    page.mouse().click(step.coords().x(), step.coords().y());
	                    clicked = true;
	                }
	            } catch (Exception ignore) { /* will be treated as not clicked */ }
	        }
	    }
	
	    // Post-click: check if a new tab was opened
        if (clicked && newPage != null) {
            System.out.println("New tab detected: " + newPage.url());
            
            // Wait for the new page to load completely 
            try {
                newPage.waitForLoadState(LoadState.LOAD, new Page.WaitForLoadStateOptions().setTimeout(10000));
            } catch (Exception e) {
                System.out.println("New tab load timeout: " + e.getMessage());
            }
            
            try {
                newPage.waitForLoadState(LoadState.NETWORKIDLE, new Page.WaitForLoadStateOptions().setTimeout(4000));
            } catch (Exception ignored) {
                // Continue if network idle doesn't happen
            }
            
            response.put("isNewTab", true);
            response.put("tabTitle", newPage.title());
            createNewTabRecord(session, newPage);
        }
        
	    // Post-click short waits (navigation or DOM ready) on current page
	    if (clicked) {
	        try { 
	            page.waitForURL(u -> !Objects.equals(u, beforeUrl), new Page.WaitForURLOptions().setTimeout(800));
	        } catch (Exception ignore) {}
	        try { 
	            page.waitForLoadState(LoadState.DOMCONTENTLOADED, new Page.WaitForLoadStateOptions().setTimeout(800));
	        } catch (Exception ignore) {}
	    }
	
	    return clicked;
	}

	private static boolean focusAndType(Locator loc, String text) {
	    if (!isActionable(loc)) return false;
	    try {
	        loc.click(new Locator.ClickOptions().setTimeout(300)); 
	        if (text != null) {
	            String oldVal = null;
	            try { oldVal = loc.inputValue(new Locator.InputValueOptions().setTimeout(100)); } catch (Exception ignore) {}
	            try { loc.fill(text, new Locator.FillOptions().setTimeout(500)); } // reduced from 1000–1200
	            catch (Exception e) { loc.fill(text, new Locator.FillOptions().setTimeout(700)); }

	            try {
	                String newVal = loc.inputValue(new Locator.InputValueOptions().setTimeout(100));
	                if (oldVal != null && Objects.equals(oldVal, newVal)) return false;
	            } catch (Exception ignore) { /* not an input? okay */ }
	        }
	        return true;
	    } catch (Exception e) {
	        return false;
	    }
	}


    private static Locator healSelector(Page page, int x, int y) {
        String script =
                "({x,y})=>{ const el=document.elementFromPoint(x,y); if(!el) return null;"
                        + " const id=el.id; if(id) return {strategy:'id',value:id};"
                        + " const testId=el.getAttribute('data-testid')||el.getAttribute('data-test-id'); if(testId) return {strategy:'testId',value:testId};"
                        + " const role=el.getAttribute('role'); if(role) return {strategy:'role',value:role};"
                        + " function css(e){ if(!e||e===document.body) return 'body'; if(e.id) return '#'+CSS.escape(e.id);"
                        + "   let s=e.localName; if(e.classList.length && e.classList.length<=3) s+='.'+[...e.classList].map(c=>CSS.escape(c)).join('.');"
                        + "   const p=e.parentElement; if(!p) return s; const sib=[...p.children].filter(c=>c.localName===e.localName);"
                        + "   const idx=sib.indexOf(e)+1; return css(p)+' > '+s+`:nth-of-type(${idx})`; }"
                        + " return {strategy:'css', value: css(el)}; }";

        Map<String, String> sel = evaluateSelectorProbeSafely(
                page, script, java.util.Map.of("x", x, "y", y));

        if (sel == null) return null;
        return resolveLocator(page, new Selector(sel.get("strategy"), sel.get("value")));
    }

    private static Map<String, String> evaluateSelectorProbeSafely(Page page, String script, Map<String, Object> args) {
        for (int attempt = 0; attempt < 2; attempt++) {
            try {
                Object raw = page.evaluate(script, args);
                return (Map<String, String>) raw; // may be null
            } catch (PlaywrightException ex) {
                String msg = ex.getMessage();
                boolean ctxDestroyed = msg != null && msg.toLowerCase().contains("execution context was destroyed");
                if (!ctxDestroyed || attempt == 1) return null;
                try { page.waitForLoadState(LoadState.DOMCONTENTLOADED, new Page.WaitForLoadStateOptions().setTimeout(3000)); } catch (Exception ignore) {}
            } catch (Exception e) {
                return null;
            }
        }
        return null;
    }

    private static boolean isActionable(Locator loc) {
        try {
            return loc != null && loc.isVisible() && loc.isEnabled();
        } catch (Exception e) {
            return false;
        }
    }

    private static void navigateStep(Page page, Step step) {
        long start = System.currentTimeMillis();
        var opts = new Page.NavigateOptions()
                .setWaitUntil(com.microsoft.playwright.options.WaitUntilState.LOAD)
                .setTimeout(60_000);
        page.navigate(step.url(), opts);

        // Try to wait for network idle without blocking forever
        try {
            page.waitForLoadState(com.microsoft.playwright.options.LoadState.NETWORKIDLE,
                    new Page.WaitForLoadStateOptions().setTimeout(4_000));
        } catch (PlaywrightException ignored) {
            // Continue if network idle doesn't happen
        } finally {
            response.put("tabTitle", page.title());
        }
        System.out.println("Tab Title: " + response.get("tabTitle"));
        System.out.printf("[ACTION] NAVIGATE took %d ms  %s%n",
                System.currentTimeMillis() - start, step.url());
    }

    private static void clickStep(Page page, Step step, String beforeUrl, Session session) {
        long start = System.currentTimeMillis();
            if (step.selector() != null) {
                Locator loc = resolveLocator(page, step.selector());
                if (loc == null) {
                    // No selector match – don’t drop to coords; surface as SELECTOR_NOT_FOUND
                    throw new PlaywrightException("SELECTOR_NOT_FOUND: " + step.selector().value());
                }
                // otherwise proceed with the clickable path above
            }
            boolean ok = clickWithFallback(page, step, beforeUrl, session);
            if (!ok) {
                throw new PlaywrightException("NO_EFFECT: click had no actionable target (selector not found & no hit at coords).");
            }
            
            System.out.printf("[ACTION] CLICK took %d ms (selector=%s)%n",
                    System.currentTimeMillis() - start,
                    step.selector() != null ? step.selector().value() : "coords");
        }

    private static void typeStep(Page page, Step step) {
    	long start = System.currentTimeMillis();
        if (step.selector() != null) {
            Locator loc = resolveLocator(page, step.selector());
            if (loc == null) {
                // No selector match – don’t drop to coords; surface as SELECTOR_NOT_FOUND
                throw new PlaywrightException("SELECTOR_NOT_FOUND: " + step.selector().value());
            }
            // otherwise proceed with the clickable path above
        }
        boolean ok = typeWithFallback(page, step);
        if (!ok) {
            throw new PlaywrightException("NO_EFFECT: type had no focusable text control (selector not found & no focused input/textarea/contentEditable).");
        }
        
        System.out.printf("[ACTION] TYPE took %d ms (text=%s)%n",
                System.currentTimeMillis() - start,
                step.text() != null ? "\"" + step.text() + "\"" : "null");
    }

    private static void scrollStep(Page page, Step step) {
        long start = System.currentTimeMillis();
        int deltaY = step.deltaY() != null ? step.deltaY() : 300;
        page.mouse().wheel(0, deltaY);
        System.out.printf("[ACTION] SCROLL took %d ms (deltaY=%d)%n",
                System.currentTimeMillis() - start, deltaY);
    }

    private static void waitStep(Page page, Step step) {
        long start = System.currentTimeMillis();
        int ms = step.waitAfterMs() != null ? step.waitAfterMs() : 300;
        page.waitForTimeout(ms);
        System.out.printf("[ACTION] WAIT took %d ms (timeout=%d)%n",
                System.currentTimeMillis() - start, ms);
    }

	private static JSHandle createMutationObserver(Page page) {
	    return page.evaluateHandle(
	        "() => new Promise(resolve => {" +
	        "  const observer = new MutationObserver(muts => {" +
	        "    for (const m of muts) {" +
	        "      if (m.type === 'childList' && (m.addedNodes.length > 0 || m.removedNodes.length > 0)) {" +
	        "        observer.disconnect(); resolve(true); return;" +
	        "      }" +
	        "      if (m.type === 'characterData' && m.target.nodeValue && m.target.nodeValue.trim().length > 0) {" +
	        "        observer.disconnect(); resolve(true); return;" +
	        "      }" +
	        "      if (m.type === 'attributes' && m.attributeName !== 'value') {" +
	        "        observer.disconnect(); resolve(true); return;" +
	        "      }" +
	        "    }" +
	        "  });" +
	        "  observer.observe(document.body, { childList: true, subtree: true, attributes: true, characterData: true });" +
	        "  setTimeout(() => { observer.disconnect(); resolve(false); }, 800);" + 
	        "})"
	    );
	}
    
    private static boolean detectPageChange(JSHandle mutationPromise) {
        try {
            boolean domChanged = (boolean) mutationPromise.evaluate("value => value");
            return domChanged;
        } catch (Exception e) {
            throw new RuntimeException("Failed to evaluate DOM changes: " + e);
        }
    }

    public static void createNewTabRecord(Session session, Page page) {
        // Get the steps map from session.history
        Map<String, List<List<Step>>> stepsMap = session.history.steps();

        // Generate next tab name
        int nextTabIndex = stepsMap.size() + 1;
        String tabId = "tab-" + nextTabIndex;

        // Add new tab record with an empty list of steps
        session.history.steps().put(tabId, new ArrayList<List<Step>>());
        session.tabPages.put(tabId, page);
        
        // Store the new tab ID in the response so it can be returned
        response.put("newTabId", tabId);
    }

}