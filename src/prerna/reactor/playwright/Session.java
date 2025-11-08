package prerna.reactor.playwright;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Page;

import prerna.auth.User;

public class Session {

	private static final Logger classLogger = LogManager.getLogger(Session.class);
	private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

	public static final long DEFAULT_EXPIRY_MINUTES = 30; //

	public final BrowserContext ctx;
	public Map<String, Page> tabPages = new HashMap<>();
	public StepsEnvelope history = new StepsEnvelope("1", newMeta(""), new HashMap<>());
	public Map<String, Integer> tabCurrentPageIndex = new HashMap<>();
	public Map<String, Integer> tabCurrentStepIndex = new HashMap<>();
	public boolean isLastPage = false;
	public int lastStepId = 0;

	public Map<String, List<String>> parentChildMap = new HashMap<>();

	public boolean closed = false;

	public User user;
	public String sessionId;

	public Session(BrowserContext ctx, Page page) {
		this(ctx, page, DEFAULT_EXPIRY_MINUTES);
	}

	public Session(BrowserContext ctx, Page page, long expiryMinutes) {
		this.ctx = ctx;
		tabPages.put("tab-1", page);
		history.steps().put("tab-1", new ArrayList<List<Step>>());

		tabCurrentPageIndex.put("tab-1", 0);
		tabCurrentStepIndex.put("tab-1", 0);

		// Schedule automatic expiry
		scheduleExpiry(expiryMinutes);
	}

	// Add accessors for context and pages
	public BrowserContext getBrowserContext() {
		return this.ctx;
	}

	public Page getPage() {
		return this.tabPages.get("tab-1");
	}

	public Page getPage(String tabId) {
		return this.tabPages.get(tabId);
	}

	public void putPage(String tabId, Page page) {
		this.tabPages.put(tabId, page);
	}

	public Map<String, Page> getTabPages() {
		return this.tabPages;
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
		return new RecordingMeta(java.util.UUID.randomUUID().toString(), maybeTitleOrUrl, // or null; you can set a
																							// better title later
				null, // description starts empty
				now, now);
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
