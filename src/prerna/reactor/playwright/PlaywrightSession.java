package prerna.reactor.playwright;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.options.LoadState;

import prerna.auth.User;

public class PlaywrightSession {

	private static final Logger classLogger = LogManager.getLogger(PlaywrightSession.class);

	private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
	private static final long DEFAULT_EXPIRY_MINUTES = 30; //

	public final BrowserContext ctx;
	Map<String, Page> tabPages = new HashMap<>();
	StepsEnvelope history = new StepsEnvelope("1", newMeta(""), new HashMap<>());
	Map<String, Integer> tabCurrentPageIndex = new HashMap<>();
	Map<String, Integer> tabCurrentStepIndex = new HashMap<>();
	boolean isLastPage = false;
	int lastStepId = 0;

	private final Map<String, NetworkTracker> tabNetworkTrackers = new ConcurrentHashMap<>();

	private Map<String, List<String>> parentChildMap = new HashMap<>();

	private boolean closed = false;

	private User user;
	private String sessionId;

	PlaywrightSession(BrowserContext ctx, Page page) {
		this(ctx, page, DEFAULT_EXPIRY_MINUTES);
	}

	PlaywrightSession(BrowserContext ctx, Page page, long expiryMinutes) {
		this.ctx = ctx;
		tabPages.put("tab-1", page);
		history.steps().put("tab-1", new ArrayList<List<PlaywrightStep>>());
		attachNetworkListeners("tab-1", page);

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

	void attachNetworkListeners(String tabId, Page page) {
		NetworkTracker tracker = trackerForTab(tabId);
		tracker.updateUrl(currentPageUrl(page));
		page.onRequest(request -> tracker.markRequestStart());
		page.onRequestFinished(request -> tracker.markRequestFinished());
		page.onRequestFailed(request -> tracker.markRequestFinished());
		page.onLoad(p -> {
			tracker.markActivity();
			tracker.updateUrl(currentPageUrl(p));
		});
		page.onDOMContentLoaded(p -> {
			tracker.markActivity();
			tracker.updateUrl(currentPageUrl(p));
		});
		page.onFrameNavigated(frame -> {
			tracker.markActivity();
			if (frame == page.mainFrame()) {
				tracker.updateUrl(currentPageUrl(page));
			}
		});
	}

	private NetworkTracker trackerForTab(String tabId) {
		return tabNetworkTrackers.computeIfAbsent(tabId, id -> new NetworkTracker());
	}

	public boolean isNetworkIdle(String tabId, long quietMillis) {
		return trackerForTab(tabId).isIdle(quietMillis);
	}

	public int getInFlightRequests(String tabId) {
		return trackerForTab(tabId).inFlight();
	}

	public long getLastNetworkActivity(String tabId) {
		return trackerForTab(tabId).lastActivity();
	}

	public String getCurrentUrl(String tabId) {
		String tracked = trackerForTab(tabId).currentUrl();
		if (tracked != null) {
			return tracked;
		}
		Page page = tabPages.get(tabId);
		return page != null ? currentPageUrl(page) : null;
	}

	public void refreshTrackedUrl(String tabId) {
		Page page = tabPages.get(tabId);
		if (page != null) {
			try {
				page.waitForLoadState(LoadState.NETWORKIDLE, new Page.WaitForLoadStateOptions().setTimeout(1_000));
			} catch (Exception ignore) {
				try {
					page.waitForLoadState(LoadState.NETWORKIDLE, new Page.WaitForLoadStateOptions().setTimeout(1_000));
				} catch (Exception ignored) {
					// ignore
				}
			}
			trackerForTab(tabId).updateUrl(currentPageUrl(page));
		}
	}

	private static final class NetworkTracker {
		private final AtomicInteger inFlight = new AtomicInteger(0);
		private final AtomicLong lastActivity = new AtomicLong(System.currentTimeMillis());
		private volatile String currentUrl;

		void markRequestStart() {
			inFlight.incrementAndGet();
			markActivity();
		}

		void markRequestFinished() {
			if (inFlight.decrementAndGet() < 0) {
				inFlight.set(0);
			}
			markActivity();
		}

		void markActivity() {
			lastActivity.set(System.currentTimeMillis());
		}

		boolean isIdle(long quietMillis) {
			long quietFor = System.currentTimeMillis() - lastActivity.get();
			return inFlight.get() == 0 && quietFor >= quietMillis;
		}

		int inFlight() {
			int current = inFlight.get();
			return Math.max(0, current);
		}

		long lastActivity() {
			return lastActivity.get();
		}

		void updateUrl(String url) {
			this.currentUrl = url;
		}

		String currentUrl() {
			return currentUrl;
		}
	}

	private static String currentPageUrl(Page page) {
		if (page == null) {
			return null;
		}
		try {
			Object href = page.evaluate("() => window.location.href");
			if (href instanceof String hrefStr && !hrefStr.isBlank()) {
				return hrefStr;
			}
		} catch (Exception ignore) {
		}
		try {
			return page.url();
		} catch (Exception ignore) {
			return null;
		}
	}

}
