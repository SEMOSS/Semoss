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
package prerna.remoteviewer.service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.options.ScreenshotType;
import com.microsoft.playwright.options.WaitUntilState;

import prerna.auth.User;
import prerna.reactor.playwright.PlaywrightBrowserProvider;
import prerna.reactor.playwright.PlaywrightSession;
import prerna.remoteviewer.model.RemoteBrowserInputEvent;
import prerna.remoteviewer.security.RemoteBrowserUrlSafetyValidator;

/**
 * Singleton that manages all active remote browser sessions.
 *
 * <p>
 * Each logged-in user gets a shared
 * {@link com.microsoft.playwright.BrowserContext} from the same user-owned
 * context used by the older Playwright reactors. The remote viewer adds a
 * short-lived socket/viewer wrapper around a user-owned
 * {@link PlaywrightSession}; socket close/TTL only closes that wrapper, not the
 * browser context/cache.
 */
public class RemoteBrowserSessionManager {

	private static final Logger classLogger = LogManager.getLogger(RemoteBrowserSessionManager.class);

	private static final RemoteBrowserSessionManager INSTANCE = new RemoteBrowserSessionManager();

	/** All active sessions keyed by sessionId. */
	private final Map<String, RemoteBrowserSession> sessions = new ConcurrentHashMap<>();

	/** Canonical remote PlaywrightSession id per logged-in user. */
	private final Map<String, String> userRemoteSessionIds = new ConcurrentHashMap<>();

	/**
	 * Background event loop: drains the event queue and (when a viewer is
	 * connected) streams frames. Runs for the lifetime of the session.
	 */
	private static final Gson LOOP_GSON = new Gson();

	/** TTL in seconds before an idle session is reaped. */
	private final long ttlSeconds;
	private final int defaultViewportWidth;
	private final int defaultViewportHeight;
	private final int maxSessionsPerUser;
	private static final String DEFAULT_START_URL = "https://example.com";
	private static final String JS_PAGE_SCROLL_METRICS = """
			() => {
			  const root = document.scrollingElement || document.documentElement || document.body;
			  const viewportHeight = Math.max(1, window.innerHeight || root?.clientHeight || 1);
			  const scrollHeight = Math.max(viewportHeight, root?.scrollHeight || viewportHeight);
			  const scrollTop = Math.max(0, window.scrollY || root?.scrollTop || 0);
			  return { scrollTop, scrollHeight, viewportHeight };
			}
			""";

	private final ScheduledExecutorService reaper = Executors.newSingleThreadScheduledExecutor(r -> {
		Thread t = new Thread(r, "RemoteBrowserSessionReaper");
		t.setDaemon(true);
		return t;
	});

	private RemoteBrowserSessionManager() {
		this.ttlSeconds = parseLong("REMOTE_BROWSER_SESSION_TTL_SECONDS", 900);
		this.defaultViewportWidth = parseInt("REMOTE_BROWSER_VIEWPORT_WIDTH", 1365);
		this.defaultViewportHeight = parseInt("REMOTE_BROWSER_VIEWPORT_HEIGHT", 768);
		this.maxSessionsPerUser = parseInt("REMOTE_BROWSER_MAX_SESSIONS_PER_USER", 10);

		// Reap expired sessions every 60 seconds
		reaper.scheduleAtFixedRate(this::closeExpiredSessions, 60, 60, TimeUnit.SECONDS);
	}

	public static RemoteBrowserSessionManager getInstance() {
		return INSTANCE;
	}

	/**
	 * Creates or reopens the user's canonical remote browser session and navigates
	 * to {@code url}. The returned {@link RemoteBrowserSession} is the
	 * socket/viewer transport wrapper; the underlying {@link PlaywrightSession} is
	 * stored on the {@link User} and survives viewer close/TTL until that
	 * PlaywrightSession is closed by normal user-session cleanup.
	 *
	 * @param user   the authenticated SEMOSS user
	 * @param url    the target URL (must pass
	 *               {@link RemoteBrowserUrlSafetyValidator})
	 * @param width  requested viewport width (or 0 to use default)
	 * @param height requested viewport height (or 0 to use default)
	 * @return the active viewer/control {@link RemoteBrowserSession}
	 */
	public RemoteBrowserSession createSession(User user, String url, int width, int height) {
		String userId = user.getPrimaryLoginToken().getId();
		String requestedUrl = url == null ? "" : url.trim();
		boolean hasRequestedUrl = !requestedUrl.isBlank();
		if (hasRequestedUrl) {
			RemoteBrowserUrlSafetyValidator.validate(requestedUrl);
		}

		String sessionId = userRemoteSessionIds.get(userId);
		PlaywrightSession playwrightSession = null;
		if (sessionId != null) {
			try {
				playwrightSession = user.getPlaywrightSession(sessionId);
			} catch (IllegalArgumentException e) {
				classLogger.debug("User remote Playwright session {} was not found/reusable: {}", sessionId,
						e.getMessage());
				userRemoteSessionIds.remove(userId, sessionId);
				sessionId = null;
			}
		}
		int vpWidth = width > 0 ? width : defaultViewportWidth;
		int vpHeight = height > 0 ? height : defaultViewportHeight;
		Page page = null;

		if (playwrightSession != null) {
			PlaywrightSession reusableSession = playwrightSession;
			reusableSession.getOperationLock().lock();
			try {
				page = playwrightSession.getPage();
				if (page == null || page.isClosed()) {
					playwrightSession = null;
				}
			} catch (Exception e) {
				classLogger.debug("User remote Playwright session {} is not reusable: {}", sessionId, e.getMessage());
				playwrightSession = null;
			} finally {
				reusableSession.getOperationLock().unlock();
			}
		}

		if (playwrightSession == null) {
			long userCount = sessions.values().stream().filter(s -> !s.isClosed() && userId.equals(s.getUserId()))
					.count();
			if (userCount >= maxSessionsPerUser) {
				throw new IllegalStateException(
						"Maximum concurrent sessions (" + maxSessionsPerUser + ") reached for this user");
			}

			// Reuse the same shared BrowserContext that the older Playwright reactors use
			// for this SEMOSS user. This is what makes cookies/login state visible between
			// the old Playwright app and the socket remote-browser app.
			Browser browser = PlaywrightBrowserProvider.getBrowser();

			Browser.NewContextOptions ctxOpts = new Browser.NewContextOptions().setViewportSize(vpWidth, vpHeight)
					.setDeviceScaleFactor(1.0);
			BrowserContext context = user.getOrCreateSharedPlaywrightContext(browser, ctxOpts);
			page = context.newPage();

			sessionId = UUID.randomUUID().toString();
			playwrightSession = PlaywrightSession.forRemoteViewer(user, sessionId, context, page);
			userRemoteSessionIds.put(userId, sessionId);
			if (!hasRequestedUrl) {
				requestedUrl = DEFAULT_START_URL;
				hasRequestedUrl = true;
			}
		} else {
			playwrightSession.getOperationLock().lock();
			try {
				page.setViewportSize(vpWidth, vpHeight);
			} catch (Exception e) {
				classLogger.debug("Could not update viewport for reused browser session {}: {}", sessionId,
						e.getMessage());
			} finally {
				playwrightSession.getOperationLock().unlock();
			}
			RemoteBrowserSession existingViewer = sessions.get(sessionId);
			if (existingViewer != null && !existingViewer.isClosed()) {
				closeSession(existingViewer);
			}
		}

		RemoteBrowserSession session = new RemoteBrowserSession(sessionId, userId, playwrightSession, vpWidth,
				vpHeight);

		// Store before navigating so the session is findable immediately
		sessions.put(sessionId, session);

		if (hasRequestedUrl) {
			playwrightSession.getOperationLock().lock();
			try {
				// A remote viewer must become available as soon as the redirect chain has
				// committed. Waiting for LOAD can hold the operation lock for the full
				// navigation timeout on SSO/client-redirect pages, preventing the frame
				// sender from showing the intermediate and eventual destination pages.
				page.navigate(requestedUrl, new Page.NavigateOptions().setWaitUntil(WaitUntilState.COMMIT));
			} catch (Exception e) {
				// Navigation failure is non-fatal - the client will see an error frame
				classLogger.warn("Initial navigation to '{}' failed for session {}: {}", requestedUrl, sessionId,
						e.getMessage());
			} finally {
				playwrightSession.getOperationLock().unlock();
			}
		}
		String initialPageUrl;
		playwrightSession.getOperationLock().lock();
		try {
			initialPageUrl = safeUrl(page);
		} finally {
			playwrightSession.getOperationLock().unlock();
		}
		RemoteBrowserRecordingService.recordInitialNavigation(session, initialPageUrl);

		// Register a context-level listener so that pages opened by the browser
		// (e.g. target="_blank" links) are tracked and announced to the viewer.
		registerNewTabListener(session);

		// Start the event-processing loop immediately so that injected events
		// (e.g. from the Chrome extension mock) are processed even before a
		// WebSocket viewer connects. The WebSocket onOpen will reuse this thread.
		Thread loopThread = new Thread(() -> runEventLoop(session), "BrowserLoop-" + sessionId);
		loopThread.setDaemon(true);
		session.setSessionThread(loopThread);
		loopThread.start();

		classLogger.info("Opened remote browser viewer {} for user {} -> {}", sessionId, userId, url);
		return session;
	}

	/**
	 * Registers a {@code context.onPage()} listener that detects browser-opened
	 * tabs (e.g. target="_blank" links) and sends {@code tab-opened} / {@code
	 * tab-activated} WebSocket events to the connected viewer.
	 */
	private void registerNewTabListener(RemoteBrowserSession session) {
		PlaywrightSession ps = session.getPlaywrightSession();
		if (ps == null) {
			return;
		}
		BrowserContext ctx = ps.getBrowserContext();
		if (ctx == null) {
			return;
		}
		ctx.onPage(newPage -> {
			if (session.isClosed()) {
				return;
			}
			try {
				try {
					newPage.waitForLoadState(com.microsoft.playwright.options.LoadState.LOAD,
							new Page.WaitForLoadStateOptions().setTimeout(2_000));
				} catch (Exception ignored) {
				}

				String title = "";
				String newUrl = "";
				try {
					title = newPage.title();
				} catch (Exception ignored) {
				}
				try {
					newUrl = newPage.url();
				} catch (Exception ignored) {
				}

				String newTabId = ps.registerPage(newPage, null);
				session.markNewlyOpenedTab(newTabId);
				newPage.onClose(closedPage -> {
					ps.removeReplayBindings(closedPage);
					if (newTabId.equals(session.getActiveTabId())) {
						session.activateFallbackTab();
					}
					sendTabState(session);
				});

				// Switch active tab to the new one
				session.setActiveTabId(newTabId);

				// Notify the viewer
				RemoteBrowserFrameSender sender = session.getRemoteBrowserFrameSender();
				if (sender != null) {
					sender.send(LOOP_GSON
							.toJson(Map.of("type", "tab-opened", "tabId", newTabId, "title", title, "url", newUrl)));
					sender.send(LOOP_GSON.toJson(Map.of("type", "tab-activated", "tabId", newTabId)));
					sendTabState(session);
				}
				classLogger.info("New tab opened session={} tabId={} url={}", session.getSessionId(), newTabId, newUrl);
			} catch (Exception e) {
				classLogger.warn("Error handling new tab for session {}: {}", session.getSessionId(), e.getMessage());
			}
		});
	}

	/** Sends the complete live-tab state to a newly connected or updated viewer. */
	public void sendTabState(RemoteBrowserSession session) {
		if (session == null) {
			return;
		}
		RemoteBrowserFrameSender sender = session.getRemoteBrowserFrameSender();
		if (sender == null) {
			return;
		}
		List<Map<String, Object>> tabs = new ArrayList<>();
		for (Map.Entry<String, Page> entry : session.getPlaywrightSession().getTabPages().entrySet()) {
			Page tabPage = entry.getValue();
			if (tabPage == null || tabPage.isClosed()) {
				continue;
			}
			Map<String, Object> tab = new LinkedHashMap<>();
			tab.put("tabId", entry.getKey());
			tab.put("title", safeTitle(tabPage));
			tab.put("url", safeUrl(tabPage));
			tabs.add(tab);
		}
		tabs.sort((left, right) -> Integer.compare(tabNumber((String) left.get("tabId")),
				tabNumber((String) right.get("tabId"))));
		Map<String, Object> payload = new LinkedHashMap<>();
		payload.put("type", "tabs-state");
		payload.put("activeTabId", session.getActiveTabId());
		payload.put("tabs", tabs);
		sender.send(LOOP_GSON.toJson(payload));
	}

	private static String safeTitle(Page page) {
		try {
			return page.title();
		} catch (Exception e) {
			return "";
		}
	}

	private static int tabNumber(String tabId) {
		try {
			return Integer.parseInt(tabId.substring("tab-".length()));
		} catch (Exception e) {
			return Integer.MAX_VALUE;
		}
	}

	private static String safeUrl(Page page) {
		try {
			return page.url();
		} catch (Exception e) {
			return "";
		}
	}

	private void runEventLoop(RemoteBrowserSession session) {
		String lastUrl = "";
		String lastTabId = "";
		while (!session.isClosed() && !Thread.currentThread().isInterrupted()) {
			long start = System.currentTimeMillis();
			try {
				// Process all queued events (from WebSocket or inject endpoint)
				RemoteBrowserInputEvent event;
				while ((event = session.eventQueue.poll()) != null) {
					classLogger.info("Remote viewer event dequeued session={} queueRemaining={} type={}",
							session.getSessionId(), session.eventQueue.size(), event.getType());
					if ("selected-text-context".equals(event.getType())) {
						handleSelectedTextContext(session, event);
					} else if (isRecordingControl(event)) {
						try {
							RemoteBrowserRecordingService.record(session, event);
							sendRecordingControlResult(session, event, true, null);
						} catch (RuntimeException e) {
							classLogger.error("Failed to update recording state for session={}", session.getSessionId(),
									e);
							sendRecordingControlResult(session, event, false, e.getMessage());
						}
					} else {
						event.setTabId(session.getActiveTabId());
						session.clearNewlyOpenedTab();
						Map<String, Object> executionResult;
						session.getPlaywrightSession().getOperationLock().lock();
						try {
							if (!isTabControl(event)) {
								RemoteBrowserSelectorService.enrich(session, event);
							}
							executionResult = RemoteBrowserInputService.dispatch(session, event);
							if ("mouse-move".equals(event.getType())) {
								sendCursorState(session, event);
							}
							event.setTriggeredTabId(session.consumeNewlyOpenedTab());
							if (Boolean.TRUE.equals(executionResult.get("success"))) {
								if (event.getTriggeredTabId() != null && event.getReplayTriggerTabId() != null) {
									Page replayPopup = session.getPlaywrightSession()
											.getLivePage(event.getTriggeredTabId());
									session.getPlaywrightSession().bindReplayPage(event.getReplayTriggerTabId(),
											replayPopup);
								}
								RemoteBrowserRecordingService.record(session, event);
							}
						} finally {
							session.getPlaywrightSession().getOperationLock().unlock();
						}
						if (isTabControl(event)) {
							sendTabControlResult(session, event, Boolean.TRUE.equals(executionResult.get("success")),
									stringValue(executionResult.get("error")));
							sendTabState(session);
						} else {
							sendReplayStepResult(session, event, executionResult);
						}
					}
					session.touchActivity();
				}

				Page page = session.getActivePage();
				if (page == null || page.isClosed()) {
					break;
				}

				// Send frame and navigated notification only when a viewer is connected
				RemoteBrowserFrameSender sender = session.getRemoteBrowserFrameSender();
				if (sender != null && session.isWsConnected()) {
					session.getPlaywrightSession().getOperationLock().lock();
					try {
						page = session.getActivePage();
						if (page == null || page.isClosed()) {
							break;
						}
						String currentUrl = page.url();
						String activeTabId = session.getActiveTabId();
						if (!currentUrl.equals(lastUrl) || !activeTabId.equals(lastTabId)) {
							lastUrl = currentUrl;
							lastTabId = activeTabId;
							sender.send(LOOP_GSON
									.toJson(Map.of("type", "navigated", "url", currentUrl, "tabId", activeTabId)));
							sendTabState(session);
						}

						// Send screenshot frame
						try {
							byte[] buf = page.screenshot(new Page.ScreenshotOptions().setFullPage(false)
									.setType(ScreenshotType.JPEG).setQuality(75));
							String b64 = Base64.getEncoder().encodeToString(buf);
							Map<String, Object> metadata = new LinkedHashMap<>();
							metadata.put("width", session.getViewportWidth());
							metadata.put("height", session.getViewportHeight());
							metadata.put("pageScaleFactor", 1);
							Object rawScrollMetrics = page.evaluate(JS_PAGE_SCROLL_METRICS);
							if (rawScrollMetrics instanceof Map<?, ?> scrollMetrics) {
								metadata.put("scrollTop", scrollMetrics.get("scrollTop"));
								metadata.put("scrollHeight", scrollMetrics.get("scrollHeight"));
								metadata.put("viewportHeight", scrollMetrics.get("viewportHeight"));
							}
							sender.send(LOOP_GSON.toJson(
									Map.of("type", "frame", "data", b64, "metadata", metadata)));
						} catch (Exception ignored) {
						}
					} finally {
						session.getPlaywrightSession().getOperationLock().unlock();
					}
				}
			} catch (Exception e) {
				classLogger.debug("Session loop error for {}: {}", session.getSessionId(), e.getMessage());
			}
			long elapsed = System.currentTimeMillis() - start;
			long sleep = 67 - elapsed;
			if (sleep > 0) {
				try {
					Thread.sleep(sleep);
				} catch (InterruptedException e) {
					break;
				}
			}
		}
		classLogger.info("Session loop ended for {}", session.getSessionId());
	}

	private static void sendReplayStepResult(RemoteBrowserSession session, RemoteBrowserInputEvent event,
			Map<String, Object> executionResult) {
		if (event.getRequestId() == null || event.getRequestId().isBlank()) {
			return;
		}
		RemoteBrowserFrameSender sender = session.getRemoteBrowserFrameSender();
		if (sender == null || !session.isWsConnected()) {
			return;
		}
		Map<String, Object> response = new java.util.HashMap<>();
		response.put("type", "replay-step-result");
		response.put("requestId", event.getRequestId());
		response.put("success", Boolean.TRUE.equals(executionResult.get("success")));
		response.put("url", executionResult.get("url"));
		if (executionResult.get("error") != null) {
			response.put("error", executionResult.get("error"));
		}
		sender.send(LOOP_GSON.toJson(response));
	}

	/**
	 * Returns the session, or empty if not found or already closed.
	 */
	public Optional<RemoteBrowserSession> getSession(String sessionId) {
		RemoteBrowserSession s = sessions.get(sessionId);
		if (s == null || s.isClosed()) {
			return Optional.empty();
		}
		return Optional.of(s);
	}

	/**
	 * Closes the socket/viewer wrapper with the given id. The user-owned
	 * PlaywrightSession and browser cache are intentionally left open.
	 */
	public void closeSession(String sessionId) {
		closeSession(sessions.get(sessionId));
	}

	/**
	 * Closes the given socket/viewer wrapper. Idempotent and null-safe.
	 */
	public void closeSession(RemoteBrowserSession session) {
		if (session == null) {
			return;
		}
		sessions.remove(session.getSessionId(), session);
		if (session.markClosed()) {
			closeViewerTransport(session);
		}
	}

	/**
	 * Finishes an explicit viewer interaction and clears its tabs without disposing
	 * the user-owned PlaywrightSession or shared BrowserContext.
	 */
	public void finishSession(String sessionId) {
		finishSession(sessions.get(sessionId));
	}

	public void finishSession(RemoteBrowserSession session) {
		if (session == null) {
			return;
		}
		closeSession(session);
		PlaywrightSession playwrightSession = session.getPlaywrightSession();
		playwrightSession.getOperationLock().lock();
		try {
			playwrightSession.resetPagesForNewViewer();
		} catch (RuntimeException e) {
			classLogger.warn("Failed to reset pages for Playwright session {}: {}", session.getSessionId(),
					e.getMessage());
		} finally {
			playwrightSession.getOperationLock().unlock();
		}
	}

	/**
	 * Reaps sessions whose last activity exceeds the configured TTL.
	 */
	public void closeExpiredSessions() {
		Instant cutoff = Instant.now().minusSeconds(ttlSeconds);
		List<String> toRemove = new ArrayList<>();

		for (Map.Entry<String, RemoteBrowserSession> entry : sessions.entrySet()) {
			RemoteBrowserSession s = entry.getValue();
			if (s.isClosed() || s.getLastActivityAt().isBefore(cutoff)) {
				toRemove.add(entry.getKey());
			}
		}

		for (String id : toRemove) {
			classLogger.info("Reaping expired/closed browser session {}", id);
			closeSession(id);
		}
	}

	/**
	 * Closes all sessions (called on application shutdown).
	 */
	public void shutdownAll() {
		for (Iterator<Map.Entry<String, RemoteBrowserSession>> it = sessions.entrySet().iterator(); it.hasNext();) {
			Map.Entry<String, RemoteBrowserSession> entry = it.next();
			it.remove();
			RemoteBrowserSession session = entry.getValue();
			if (session.markClosed()) {
				closeViewerTransport(session);
			}
		}
		reaper.shutdownNow();
	}

	// ---- helpers ----

	private static boolean isRecordingControl(RemoteBrowserInputEvent event) {
		if (event == null) {
			return false;
		}
		return "recording".equals(event.getType()) || "recording-control".equals(event.getType());
	}

	private static boolean isTabControl(RemoteBrowserInputEvent event) {
		String type = event == null ? null : event.getType();
		return "switch-tab".equals(type) || "switch-replay-tab".equals(type) || "prepare-replay".equals(type)
				|| "close-tab".equals(type);
	}

	private static String stringValue(Object value) {
		return value == null ? null : String.valueOf(value);
	}

	private void sendTabControlResult(RemoteBrowserSession session, RemoteBrowserInputEvent event, boolean success,
			String error) {
		if (event.getRequestId() == null || event.getRequestId().isBlank()) {
			return;
		}
		RemoteBrowserFrameSender sender = session.getRemoteBrowserFrameSender();
		if (sender == null) {
			return;
		}
		Map<String, Object> payload = new LinkedHashMap<>();
		payload.put("type", "tab-control-result");
		payload.put("requestId", event.getRequestId());
		payload.put("success", success);
		payload.put("activeTabId", session.getActiveTabId());
		if (error != null && !error.isBlank()) {
			payload.put("error", error);
		}
		sender.send(LOOP_GSON.toJson(payload));
	}

	private void sendRecordingControlResult(RemoteBrowserSession session, RemoteBrowserInputEvent event,
			boolean success, String error) {
		if (event.getRequestId() == null || event.getRequestId().isBlank()) {
			return;
		}
		RemoteBrowserFrameSender sender = session.getRemoteBrowserFrameSender();
		if (sender == null) {
			return;
		}
		Map<String, Object> payload = new LinkedHashMap<>();
		payload.put("type", "recording-control-result");
		payload.put("requestId", event.getRequestId());
		payload.put("success", success);
		if (error != null && !error.isBlank()) {
			payload.put("error", error);
		}
		sender.send(LOOP_GSON.toJson(payload));
	}

	private void sendCursorState(RemoteBrowserSession session, RemoteBrowserInputEvent event) {
		String cursor = RemoteBrowserInputService.cursorAt(session, event);
		if (!session.updateBrowserCursor(cursor)) {
			return;
		}
		RemoteBrowserFrameSender sender = session.getRemoteBrowserFrameSender();
		if (sender != null && session.isWsConnected()) {
			sender.send(LOOP_GSON.toJson(Map.of("type", "cursor-changed", "cursor", cursor)));
		}
	}

	private static void handleSelectedTextContext(RemoteBrowserSession session, RemoteBrowserInputEvent event) {
		RemoteBrowserFrameSender sender = session.getRemoteBrowserFrameSender();
		if (sender == null || !session.isWsConnected()) {
			return;
		}

		Map<String, Object> response = new java.util.LinkedHashMap<>();
		response.put("type", "selected-text-context-result");
		response.put("requestId", event.getRequestId());
		try {
			response.put("context", RemoteBrowserSelectedTextService.capture(session, event));
			if (Boolean.TRUE.equals(event.getRecord())) {
				event.setTabId(session.getActiveTabId());
				RemoteBrowserRecordingService.record(session, event);
			}
			response.put("success", true);
		} catch (Exception e) {
			classLogger.warn("Selected-text context capture failed for session {}: {}", session.getSessionId(),
					e.getMessage());
			response.put("success", false);
			response.put("error",
					e.getMessage() == null || e.getMessage().isBlank() ? "Could not capture selected website text"
							: e.getMessage());
		}
		sender.send(LOOP_GSON.toJson(response));
	}

	private void closeViewerTransport(RemoteBrowserSession s) {
		// Interrupt the session loop thread if running
		Thread t = s.getSessionThread();
		if (t != null && t.isAlive()) {
			t.interrupt();
		}
		RemoteBrowserRecordingService.discardRecording(s);
		s.setRemoteBrowserFrameSender(null);
		s.setWsConnected(false);
		classLogger.info("Closed remote browser viewer transport {}; Playwright session remains user-owned",
				s.getSessionId());
	}

	private static long parseLong(String envKey, long def) {
		String v = System.getenv(envKey);
		if (v == null) {
			return def;
		}
		try {
			return Long.parseLong(v.trim());
		} catch (NumberFormatException e) {
			return def;
		}
	}

	private static int parseInt(String envKey, int def) {
		String v = System.getenv(envKey);
		if (v == null) {
			return def;
		}
		try {
			return Integer.parseInt(v.trim());
		} catch (NumberFormatException e) {
			return def;
		}
	}
}
