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
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.options.LoadState;

import prerna.auth.User;

/**
 * Manages a Playwright browser session, including multiple tabs, network
 * activity tracking, and session expiry.
 */
public class PlaywrightSession {

	private static final Logger classLogger = LogManager.getLogger(PlaywrightSession.class);

	private static final ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(1);
	private static final long DEFAULT_EXPIRY_MINUTES = 120; //

	private final Map<String, NetworkTracker> tabNetworkTrackers = new ConcurrentHashMap<>();

	/** Serializes commands and event processing on this Playwright connection. */
	private final ReentrantLock operationLock = new ReentrantLock(true);

	private Map<String, List<String>> parentChildMap = new HashMap<>();

	private boolean closed = false;

	private User user;
	private String sessionId;

	StepsEnvelope history = new StepsEnvelope("1", newMeta(""), new HashMap<>());
	Map<String, Page> tabPages = new HashMap<>();
	Map<String, Integer> tabCurrentPageIndex = new HashMap<>();
	Map<String, Integer> tabCurrentStepIndex = new HashMap<>();

	boolean isLastPage = false;
	int lastStepId = 0;

	final BrowserContext CTX;

	/**
	 * Constructs a new PlaywrightSession with a default expiry time.
	 * 
	 * @param ctx  The Playwright BrowserContext for this session.
	 * @param page The initial Page object for this session.
	 */
	PlaywrightSession(BrowserContext ctx, Page page) {
		this(ctx, page, DEFAULT_EXPIRY_MINUTES);
	}

	/**
	 * Constructs a new PlaywrightSession with a specified expiry time.
	 * 
	 * @param ctx           The Playwright BrowserContext for this session.
	 * @param page          The initial Page object for this session.
	 * @param expiryMinutes The number of minutes after which the session should
	 *                      expire.
	 */
	PlaywrightSession(BrowserContext ctx, Page page, long expiryMinutes) {
		this(ctx, page, expiryMinutes, true);
	}

	private PlaywrightSession(BrowserContext ctx, Page page, long expiryMinutes, boolean scheduleExpiry) {
		this.CTX = ctx;
		tabPages.put("tab-1", page);
		history.steps().put("tab-1", new ArrayList<List<PlaywrightStep>>());
		attachNetworkListeners("tab-1", page);

		tabCurrentPageIndex.put("tab-1", 0);
		tabCurrentStepIndex.put("tab-1", 0);

		if (scheduleExpiry) {
			scheduleExpiry(expiryMinutes);
		}
	}

	/**
	 * Creates a session for the remote browser viewer. The viewer transport manages
	 * its own idle TTL via {@code RemoteBrowserSessionManager}; the browser context
	 * is not closed by that TTL.
	 *
	 * @param ctx           The Playwright BrowserContext for this session.
	 * @param page          The initial Page object for this session.
	 * @param expiryMinutes The absolute number of minutes after which the session
	 *                      is closed as a backstop.
	 * @return A new PlaywrightSession.
	 */
	public static PlaywrightSession forRemoteViewer(BrowserContext ctx, Page page, long expiryMinutes) {
		return new PlaywrightSession(ctx, page, expiryMinutes, false);
	}

	/**
	 * Creates a user-owned session for the remote browser viewer and registers it
	 * under the same user/session store used by the older Playwright reactors. This
	 * lets socket viewing stop independently while the browser context/cache stays
	 * alive until the user-owned PlaywrightSession is closed.
	 *
	 * @param user      The SEMOSS user that owns the browser session.
	 * @param sessionId The session id to register under the user.
	 * @param ctx       The Playwright BrowserContext for this session.
	 * @param page      The initial Page object for this session.
	 * @return A new user-owned PlaywrightSession.
	 */
	public static PlaywrightSession forRemoteViewer(User user, String sessionId, BrowserContext ctx, Page page) {
		PlaywrightSession session = new PlaywrightSession(ctx, page, DEFAULT_EXPIRY_MINUTES, false);
		session.setUserAndSessionId(user, sessionId);
		user.setPlaywrightSession(sessionId, session);
		return session;
	}

	/**
	 * Returns the replay history envelope for this session.
	 *
	 * @return The current {@link StepsEnvelope}.
	 */
	public StepsEnvelope getHistory() {
		return history;
	}

	/**
	 * Appends a captured recorder step to the same history shape used by the
	 * Playwright recorder/reactors.
	 *
	 * @param tabId        The tab where the action happened.
	 * @param step         The captured replay step.
	 * @param startNewPage Whether to start a new page group before appending.
	 * @return The appended step with a session-scoped id assigned.
	 */
	public synchronized PlaywrightStep appendRemoteBrowserRecordedStep(String tabId, PlaywrightStep step,
			boolean startNewPage) {
		String resolvedTabId = (tabId == null || tabId.isBlank()) ? "tab-1" : tabId;
		history.steps().computeIfAbsent(resolvedTabId, k -> new ArrayList<List<PlaywrightStep>>());

		int stepId = ++lastStepId;
		PlaywrightStep newStep = new PlaywrightStep(step, stepId);
		List<List<PlaywrightStep>> pages = history.steps().get(resolvedTabId);
		if (startNewPage || pages.isEmpty()) {
			pages.add(new ArrayList<>(List.of(newStep)));
		} else {
			pages.get(pages.size() - 1).add(newStep);
		}
		return newStep;
	}

	/**
	 * Replaces the most recently appended step in the given tab. Used by live
	 * recording to aggregate character-by-character TYPE events into one replayable
	 * TYPE step.
	 *
	 * @param tabId The tab where the action happened.
	 * @param step  The replacement step.
	 */
	public synchronized void replaceLastRemoteBrowserRecordedStep(String tabId, PlaywrightStep step) {
		String resolvedTabId = (tabId == null || tabId.isBlank()) ? "tab-1" : tabId;
		List<List<PlaywrightStep>> pages = history.steps().get(resolvedTabId);
		if (pages == null || pages.isEmpty()) {
			return;
		}
		List<PlaywrightStep> currentPage = pages.get(pages.size() - 1);
		if (currentPage.isEmpty()) {
			return;
		}
		currentPage.set(currentPage.size() - 1, step);
	}

	/**
	 * Retrieves the BrowserContext associated with this session.
	 *
	 * @return The BrowserContext.
	 */
	public BrowserContext getBrowserContext() {
		return this.CTX;
	}

	/**
	 * Retrieves the default (first) Page object for this session.
	 * 
	 * @return The default Page.
	 */
	public Page getPage() {
		return this.tabPages.get("tab-1");
	}

	/**
	 * Retrieves a specific Page object by its tab ID.
	 * 
	 * @param tabId The ID of the tab.
	 * @return The Page object associated with the given tab ID.
	 */
	public Page getPage(String tabId) {
		return this.tabPages.get(tabId);
	}

	/**
	 * Returns the reentrant gate for operations against this session's Playwright
	 * connection. Callers may hold it across an action and its screenshot.
	 */
	public ReentrantLock getOperationLock() {
		return operationLock;
	}

	/**
	 * Adds or updates a Page object for a given tab ID.
	 * 
	 * @param tabId The ID of the tab.
	 * @param page  The Page object to associate with the tab ID.
	 */
	public void putPage(String tabId, Page page) {
		this.tabPages.put(tabId, page);
	}

	/**
	 * Retrieves a map of all tab IDs to their respective Page objects.
	 * 
	 * @return A map of tab IDs to Page objects.
	 */
	public Map<String, Page> getTabPages() {
		return this.tabPages;
	}

	/**
	 * Schedules the session to expire after a specified number of minutes. When
	 * expired, the session will be closed.
	 * 
	 * @param expiryMinutes The number of minutes until session expiry.
	 */
	private void scheduleExpiry(long expiryMinutes) {
		ScheduledFuture<?> expiryTask = SCHEDULER.schedule(() -> {
			try {
				classLogger.info("Session expired after {} minutes, closing session", expiryMinutes);
				close();
			} catch (Exception e) {
				classLogger.error("Error closing expired session", e);
			}
		}, expiryMinutes, TimeUnit.MINUTES);

		classLogger.info("Session scheduled to expire in {} minutes", expiryMinutes);
	}

	/**
	 * Sets the user and session ID for this Playwright session.
	 * 
	 * @param user      The User object associated with this session.
	 * @param sessionId The unique ID for this session.
	 */
	void setUserAndSessionId(User user, String sessionId) {
		this.user = user;
		this.sessionId = sessionId;
	}

	/**
	 * Closes the Playwright session, including all open pages and the browser
	 * context. Also removes the session from the user's active sessions.
	 */
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

	/**
	 * Creates a new RecordingMeta object with a generated UUID and current
	 * timestamp.
	 * 
	 * @param maybeTitleOrUrl An optional title or URL to initialize the meta
	 *                        object.
	 * @return A new RecordingMeta instance.
	 */
	public static RecordingMeta newMeta(String maybeTitleOrUrl) {
		long now = System.currentTimeMillis();
		return new RecordingMeta(java.util.UUID.randomUUID().toString(), maybeTitleOrUrl, null, now, now, null);
	}

	/**
	 * Retrieves the current page index for a given tab.
	 * 
	 * @param tabId The ID of the tab.
	 * @return The current page index, or 0 if not found.
	 */
	public int getCurrentPageIndex(String tabId) {
		return tabCurrentPageIndex.getOrDefault(tabId, 0);
	}

	/**
	 * Sets the current page index for a given tab.
	 * 
	 * @param tabId The ID of the tab.
	 * @param index The new page index.
	 */
	public void setCurrentPageIndex(String tabId, int index) {
		tabCurrentPageIndex.put(tabId, index);
	}

	/**
	 * Retrieves the current step index for a given tab.
	 * 
	 * @param tabId The ID of the tab.
	 * @return The current step index, or 0 if not found.
	 */
	public int getCurrentStepIndex(String tabId) {
		return tabCurrentStepIndex.getOrDefault(tabId, 0);
	}

	/**
	 * Sets the current step index for a given tab.
	 * 
	 * @param tabId The ID of the tab.
	 * @param index The new step index.
	 */
	public void setCurrentStepIndex(String tabId, int index) {
		tabCurrentStepIndex.put(tabId, index);
	}

	/**
	 * Increments the current page index for a given tab.
	 * 
	 * @param tabId The ID of the tab.
	 */
	public void incrementPageIndex(String tabId) {
		int current = getCurrentPageIndex(tabId);
		setCurrentPageIndex(tabId, current + 1);
	}

	/**
	 * Increments the current step index for a given tab.
	 * 
	 * @param tabId The ID of the tab.
	 */
	public void incrementStepIndex(String tabId) {
		int current = getCurrentStepIndex(tabId);
		setCurrentStepIndex(tabId, current + 1);
	}

	/**
	 * Adds a parent-child relationship between tabs.
	 * 
	 * @param parentTabId The ID of the parent tab.
	 * @param childTabId  The ID of the child tab.
	 */
	public void addChildTabRelationship(String parentTabId, String childTabId) {
		parentChildMap.computeIfAbsent(parentTabId, k -> new ArrayList<>());
		if (!parentChildMap.get(parentTabId).contains(childTabId)) {
			parentChildMap.get(parentTabId).add(childTabId);
		}
	}

	/**
	 * Retrieves a list of child tab IDs for a given parent tab ID.
	 * 
	 * @param parentTabId The ID of the parent tab.
	 * @return A list of child tab IDs, or an empty list if no children are found.
	 */
	public List<String> getChildTabs(String parentTabId) {
		return parentChildMap.getOrDefault(parentTabId, new ArrayList<>());
	}

	/**
	 * Removes all parent-child relationships involving a given tab ID.
	 * 
	 * @param tabId The ID of the tab to remove relationships for.
	 */
	public void removeTabRelationships(String tabId) {
		parentChildMap.remove(tabId);
		for (List<String> children : parentChildMap.values()) {
			children.remove(tabId);
		}
	}

	/**
	 * Attaches network activity listeners to a given Playwright Page. These
	 * listeners track in-flight requests and last network activity.
	 * 
	 * @param tabId The ID of the tab.
	 * @param page  The Playwright Page to attach listeners to.
	 */
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

	/**
	 * Retrieves the NetworkTracker for a given tab ID, creating one if it doesn't
	 * exist.
	 * 
	 * @param tabId The ID of the tab.
	 * @return The NetworkTracker for the specified tab.
	 */
	private NetworkTracker trackerForTab(String tabId) {
		return tabNetworkTrackers.computeIfAbsent(tabId, id -> new NetworkTracker());
	}

	/**
	 * Checks if the network for a given tab is idle.
	 * 
	 * @param tabId       The ID of the tab.
	 * @param quietMillis The duration in milliseconds for which the network should
	 *                    be quiet to be considered idle.
	 * @return true if the network is idle, false otherwise.
	 */
	public boolean isNetworkIdle(String tabId, long quietMillis) {
		return trackerForTab(tabId).isIdle(quietMillis);
	}

	/**
	 * Retrieves the number of in-flight network requests for a given tab.
	 * 
	 * @param tabId The ID of the tab.
	 * @return The count of in-flight requests.
	 */
	public int getInFlightRequests(String tabId) {
		return trackerForTab(tabId).inFlight();
	}

	/**
	 * Retrieves the timestamp of the last network activity for a given tab.
	 * 
	 * @param tabId The ID of the tab.
	 * @return The timestamp of the last network activity in milliseconds.
	 */
	public long getLastNetworkActivity(String tabId) {
		return trackerForTab(tabId).lastActivity();
	}

	/**
	 * Retrieves the current URL of a given tab.
	 * 
	 * @param tabId The ID of the tab.
	 * @return The current URL as a String, or null if not available.
	 */
	public String getCurrentUrl(String tabId) {
		String tracked = trackerForTab(tabId).currentUrl();
		if (tracked != null) {
			return tracked;
		}
		Page page = tabPages.get(tabId);
		return page != null ? currentPageUrl(page) : null;
	}

	/**
	 * Refreshes the tracked URL for a given tab by waiting for network idle.
	 * 
	 * @param tabId The ID of the tab.
	 */
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

	/**
	 * Helper class to track network activity for a single tab.
	 */
	private static final class NetworkTracker {
		private final AtomicInteger inFlight = new AtomicInteger(0);
		private final AtomicLong lastActivity = new AtomicLong(System.currentTimeMillis());
		private volatile String currentUrl;

		/**
		 * Marks the start of a network request.
		 */
		void markRequestStart() {
			inFlight.incrementAndGet();
			markActivity();
		}

		/**
		 * Marks the completion of a network request (success or failure).
		 */
		void markRequestFinished() {
			if (inFlight.decrementAndGet() < 0) {
				inFlight.set(0);
			}
			markActivity();
		}

		/**
		 * Updates the timestamp of the last network activity.
		 */
		void markActivity() {
			lastActivity.set(System.currentTimeMillis());
		}

		/**
		 * Checks if the network is idle for a specified duration.
		 * 
		 * @param quietMillis The duration in milliseconds for which the network should
		 *                    be quiet.
		 * @return true if the network is idle, false otherwise.
		 */
		boolean isIdle(long quietMillis) {
			long quietFor = System.currentTimeMillis() - lastActivity.get();
			return inFlight.get() == 0 && quietFor >= quietMillis;
		}

		/**
		 * Retrieves the current number of in-flight requests.
		 * 
		 * @return The number of in-flight requests.
		 */
		int inFlight() {
			int current = inFlight.get();
			return Math.max(0, current);
		}

		/**
		 * Retrieves the timestamp of the last network activity.
		 * 
		 * @return The timestamp in milliseconds.
		 */
		long lastActivity() {
			return lastActivity.get();
		}

		/**
		 * Updates the currently tracked URL.
		 * 
		 * @param url The new URL.
		 */
		void updateUrl(String url) {
			this.currentUrl = url;
		}

		/**
		 * Retrieves the currently tracked URL.
		 * 
		 * @return The current URL.
		 */
		String currentUrl() {
			return currentUrl;
		}
	}

	/**
	 * Retrieves the current URL from a Playwright Page object.
	 * 
	 * @param page The Playwright Page object.
	 * @return The current URL as a String, or null if not available.
	 */
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
