package prerna.reactor.playwright;

import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Page;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.User;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class Session {
	
	private static final Logger classLogger = LogManager.getLogger(Session.class);
	private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
	private static final long DEFAULT_EXPIRY_MINUTES = 30; //

	public final BrowserContext ctx;
    Map<String, Page> tabPages = new HashMap<>();
    StepsEnvelope history = new StepsEnvelope("1", newMeta(""), new HashMap<>());
    Map<String, Integer> tabCurrentPageIndex = new HashMap<>();
    Map<String, Integer> tabCurrentStepIndex = new HashMap<>();
    boolean isLastPage = false;
    int lastStepId = 0;

    private Map<String, List<String>> parentChildMap = new HashMap<>();

    private boolean closed = false;

    private User user;
    private String sessionId;

    Session(BrowserContext ctx, Page page) {
        this(ctx, page, DEFAULT_EXPIRY_MINUTES);
    }

    Session(BrowserContext ctx, Page page, long expiryMinutes) {
        this.ctx = ctx;
        tabPages.put("tab-1", page);
        history.steps().put("tab-1", new ArrayList<List<Step>>());

        tabCurrentPageIndex.put("tab-1", 0);
        tabCurrentStepIndex.put("tab-1", 0);

        // Schedule automatic expiry
        scheduleExpiry(expiryMinutes);
    }

    private void scheduleExpiry(long expiryMinutes) {
        long expiryTimeMillis = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(expiryMinutes);

        ScheduledFuture<?> expiryTask = scheduler.schedule(() -> {
            try {
                classLogger.info("Session expired after {} minutes, closing session", expiryMinutes);
                close();
            } catch (Exception e) {
                classLogger.error("Error closing expired session", e);
            }
        }, expiryMinutes, TimeUnit.MINUTES);

        classLogger.info("Session scheduled to expire in {} minutes", expiryMinutes);
    }

    void setUserAndSessionId(User user, String sessionId) {
        this.user = user;
        this.sessionId = sessionId;
    }

    public void close() {
        if (closed) {
            return;
        }

        synchronized (this) {
            if (closed) {
                return;
            }

            try {

                // Close all pages
                for (Page page : tabPages.values()) {
                    try {
                        if (page != null && !page.isClosed()) {
                            page.close();
                        }
                    } catch (Exception e) {
                        classLogger.error("Error closing page", e);
                    }
                }

                // Close the browser context
                if (ctx != null && !ctx.browser().isConnected()) {
                    ctx.close();
                }

                closed = true;
                classLogger.info("Session closed successfully");

                // Remove from user's session map
                if (user != null && sessionId != null) {
                    user.removePlaywrightSession(sessionId);
                    classLogger.info("Removed session {} from user's session map", sessionId);
                }
            } catch (Exception e) {
                classLogger.error("Error during session close", e);
                closed = true;
            }
        }
    }

    public static RecordingMeta newMeta(String maybeTitleOrUrl) {
        long now = System.currentTimeMillis();
        return new RecordingMeta(
                java.util.UUID.randomUUID().toString(),
                maybeTitleOrUrl,       // or null; you can set a better title later
                null,                  // description starts empty
                now,
                now
        );
    }

    public int getCurrentPageIndex(String tabId) {
        return tabCurrentPageIndex.getOrDefault(tabId, 0);
    }
    
    public void setCurrentPageIndex(String tabId, int index) {
        tabCurrentPageIndex.put(tabId, index);
    }
    
    public int getCurrentStepIndex(String tabId) {
        return tabCurrentStepIndex.getOrDefault(tabId, 0);
    }
    
    public void setCurrentStepIndex(String tabId, int index) {
        tabCurrentStepIndex.put(tabId, index);
    }
    
    public void incrementPageIndex(String tabId) {
        int current = getCurrentPageIndex(tabId);
        setCurrentPageIndex(tabId, current + 1);
    }
    
    public void incrementStepIndex(String tabId) {
        int current = getCurrentStepIndex(tabId);
        setCurrentStepIndex(tabId, current + 1);
    }


    public void addChildTabRelationship(String parentTabId, String childTabId) {
        parentChildMap.computeIfAbsent(parentTabId, k -> new ArrayList<>());
        if (!parentChildMap.get(parentTabId).contains(childTabId)) {
            parentChildMap.get(parentTabId).add(childTabId);
        }
    }


    public List<String> getChildTabs(String parentTabId) {
        return parentChildMap.getOrDefault(parentTabId, new ArrayList<>());
    }


    public void removeTabRelationships(String tabId) {
        parentChildMap.remove(tabId);
        for (List<String> children : parentChildMap.values()) {
            children.remove(tabId);
        }
    }

}

