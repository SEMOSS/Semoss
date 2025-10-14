package prerna.reactor.playwright;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import com.microsoft.playwright.JSHandle;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.PlaywrightException;
import com.microsoft.playwright.options.WaitUntilState;

/**
 * Utility class for session-related Playwright operations
 */
public class SessionUtility {
    
    /**
     * Apply a step to the session and detect page changes
     * @param session The session to apply the step to
     * @param step The step to apply
     * @return true if page changed, false otherwise
     */
    public static boolean applyStep(Session session, Step step) {
        Page page = session.page;
        
        try {
            String urlBefore = page.url();
            
            JSHandle mutationPromise = createMutationObserver(page);
            
            AtomicBoolean networkTriggered = new AtomicBoolean(false);
            page.onRequest(req -> {
                if ("xhr".equals(req.resourceType()) || "fetch".equals(req.resourceType())) {
                    networkTriggered.set(true);
                }
            });
            
            // Execute the step action
            executeStepAction(page, step, urlBefore);
            
            // Wait for any specified delay
            if (step.waitAfterMs() != null && step.waitAfterMs() > 0) {
                page.waitForTimeout(step.waitAfterMs());
            }
            
            // Detect if page changed
            String urlAfter = page.url();
            if (urlBefore.equals(urlAfter) && !networkTriggered.get()) {
                // Small buffer for SPA rendering
                page.waitForTimeout(500);
                return detectPageChange(mutationPromise);
            } else {
                return true;
            }
            
        } catch (Exception e) {
            System.err.println("Failed to apply step: " + e.getMessage());
            return true;
        }
    }
    

    private static void executeStepAction(Page page, Step step, String urlBefore) {
        switch (step.type()) {
            case NAVIGATE -> navigateStep(page, step);
            case CLICK -> clickStep(page, step, urlBefore);
            case TYPE -> typeStep(page, step);
            case SCROLL -> scrollStep(page, step);
            case WAIT -> waitStep(page, step);
        }
    }
    

    private static void navigateStep(Page page, Step step) {
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
        }
    }
    

    private static void clickStep(Page page, Step step, String urlBefore) {
        page.mouse().move(step.coords().x(), step.coords().y());
        page.mouse().click(step.coords().x(), step.coords().y());
        
        // Wait for URL change if navigation happens
        try {
            page.waitForURL(u -> !u.equals(urlBefore),
                    new Page.WaitForURLOptions().setTimeout(6_000));
        } catch (PlaywrightException ignored) {
            // No URL change, continue
        }
        
        // Wait for DOM to be ready
        try {
            page.waitForLoadState(com.microsoft.playwright.options.LoadState.LOAD,
                    new Page.WaitForLoadStateOptions().setTimeout(3_000));
        } catch (PlaywrightException ignored) {
            // Continue even if load doesn't complete
        }
    }
    

    private static void typeStep(Page page, Step step) {
        page.mouse().click(step.coords().x(), step.coords().y());
        // Clear existing text
        page.keyboard().press("Control+A");
        page.keyboard().press("Delete");
        // Type new text
        if (step.text() != null) {
            page.keyboard().type(step.text());
        }
        // Press Enter if required
        if (Boolean.TRUE.equals(step.pressEnter())) {
            page.keyboard().press("Enter");
        }
    }

    private static void scrollStep(Page page, Step step) {
        int deltaY = step.deltaY() != null ? step.deltaY() : 300;
        page.mouse().wheel(0, deltaY);
    }

    private static void waitStep(Page page, Step step) {
        int ms = step.waitAfterMs() != null ? step.waitAfterMs() : 300;
        page.waitForTimeout(ms);
    }
    
    private static JSHandle createMutationObserver(Page page) {
        return page.evaluateHandle(
            "() => new Promise(resolve => {" +
            "  const observer = new MutationObserver((muts) => {" +
            "    for (const m of muts) {" +
            "      if (m.type === 'childList' && (m.addedNodes.length > 0 || m.removedNodes.length > 0)) {" +
            "        observer.disconnect();" +
            "        resolve(true);" +
            "        return;" +
            "      }" +
            "      if (m.type === 'characterData' && m.target.nodeValue.trim().length > 0) {" +
            "        observer.disconnect();" +
            "        resolve(true);" +
            "        return;" +
            "      }" +
            "      if (m.type === 'attributes' && m.attributeName !== 'value') {" +
            "        observer.disconnect();" +
            "        resolve(true);" +
            "        return;" +
            "      }" +
            "    }" +
            "  });" +
            "  observer.observe(document.body, { childList: true, subtree: true, attributes: true, characterData: true });" +
            "  setTimeout(() => { observer.disconnect(); resolve(false); }, 1500);" +
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
}