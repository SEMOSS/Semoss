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
import java.util.Iterator;
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

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.BrowserType;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;

import prerna.remoteviewer.security.UrlSafetyValidator;

/**
 * Singleton that manages all active remote browser sessions.
 *
 * <p>Each call to {@link #createSession} launches an isolated Playwright browser context
 * and a dedicated Playwright instance. Sessions are cleaned up on explicit close or TTL
 * expiry.
 */
public class BrowserSessionManager {

	private static final Logger classLogger = LogManager.getLogger(BrowserSessionManager.class);

	private static final BrowserSessionManager INSTANCE = new BrowserSessionManager();

	/** All active sessions keyed by sessionId. */
	private final Map<String, BrowserSession> sessions = new ConcurrentHashMap<>();

	/** TTL in seconds before an idle session is reaped. */
	private final long ttlSeconds;
	private final int defaultViewportWidth;
	private final int defaultViewportHeight;
	private final boolean headless;
	private final int maxSessionsPerUser;

	private final ScheduledExecutorService reaper = Executors.newSingleThreadScheduledExecutor(r -> {
		Thread t = new Thread(r, "BrowserSessionReaper");
		t.setDaemon(true);
		return t;
	});

	private BrowserSessionManager() {
		this.ttlSeconds = parseLong("REMOTE_BROWSER_SESSION_TTL_SECONDS", 900);
		this.defaultViewportWidth = parseInt("REMOTE_BROWSER_VIEWPORT_WIDTH", 1365);
		this.defaultViewportHeight = parseInt("REMOTE_BROWSER_VIEWPORT_HEIGHT", 768);
		this.headless = !"false".equalsIgnoreCase(System.getenv("REMOTE_BROWSER_HEADLESS"));
		this.maxSessionsPerUser = parseInt("REMOTE_BROWSER_MAX_SESSIONS_PER_USER", 3);

		// Reap expired sessions every 60 seconds
		reaper.scheduleAtFixedRate(this::closeExpiredSessions, 60, 60, TimeUnit.SECONDS);
	}

	public static BrowserSessionManager getInstance() {
		return INSTANCE;
	}

	/**
	 * Creates a new isolated browser session for the given user and navigates to {@code url}.
	 *
	 * @param userId  the authenticated SEMOSS user id
	 * @param url     the target URL (must pass {@link UrlSafetyValidator})
	 * @param width   requested viewport width (or 0 to use default)
	 * @param height  requested viewport height (or 0 to use default)
	 * @return the newly created {@link BrowserSession}
	 */
	public BrowserSession createSession(String userId, String url, int width, int height) {
		UrlSafetyValidator.validate(url);

		// Enforce per-user session limit
		long userCount = sessions.values().stream()
				.filter(s -> !s.isClosed() && userId.equals(s.getUserId()))
				.count();
		if (userCount >= maxSessionsPerUser) {
			throw new IllegalStateException(
					"Maximum concurrent sessions (" + maxSessionsPerUser + ") reached for this user");
		}

		int vpWidth = width > 0 ? width : defaultViewportWidth;
		int vpHeight = height > 0 ? height : defaultViewportHeight;

		// Each session owns its own Playwright instance for full isolation
		Playwright playwright = Playwright.create();
		Browser browser = playwright.webkit().launch(buildLaunchOptions());

		Browser.NewContextOptions ctxOpts = new Browser.NewContextOptions()
				.setViewportSize(vpWidth, vpHeight)
				.setDeviceScaleFactor(1.0);
		BrowserContext context = browser.newContext(ctxOpts);
		context.setDefaultTimeout(30_000);
		context.setDefaultNavigationTimeout(30_000);

		Page page = context.newPage();

		String sessionId = UUID.randomUUID().toString();
		BrowserSession session = new BrowserSession(sessionId, userId, context, page, vpWidth, vpHeight);

		// Store before navigating so the session is findable immediately
		sessions.put(sessionId, session);

		try {
			page.navigate(url);
		} catch (Exception e) {
			// Navigation failure is non-fatal — the client will see an error frame
			classLogger.warn("Initial navigation to '{}' failed for session {}: {}", url, sessionId, e.getMessage());
		}

		classLogger.info("Created remote browser session {} for user {} -> {}", sessionId, userId, url);
		return session;
	}

	/**
	 * Returns the session, or empty if not found or already closed.
	 */
	public Optional<BrowserSession> getSession(String sessionId) {
		BrowserSession s = sessions.get(sessionId);
		if (s == null || s.isClosed()) {
			return Optional.empty();
		}
		return Optional.of(s);
	}

	/**
	 * Closes the session and releases all Playwright resources.
	 */
	public void closeSession(String sessionId) {
		BrowserSession s = sessions.remove(sessionId);
		if (s != null && s.markClosed()) {
			safeClose(s);
		}
	}

	/**
	 * Reaps sessions whose last activity exceeds the configured TTL.
	 */
	public void closeExpiredSessions() {
		Instant cutoff = Instant.now().minusSeconds(ttlSeconds);
		List<String> toRemove = new ArrayList<>();

		for (Map.Entry<String, BrowserSession> entry : sessions.entrySet()) {
			BrowserSession s = entry.getValue();
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
		for (Iterator<Map.Entry<String, BrowserSession>> it = sessions.entrySet().iterator(); it.hasNext();) {
			Map.Entry<String, BrowserSession> entry = it.next();
			it.remove();
			safeClose(entry.getValue());
		}
		reaper.shutdownNow();
	}

	// ---- helpers ----

	private BrowserType.LaunchOptions buildLaunchOptions() {
		// WebKit is used on macOS — does not require Chromium sandbox flags
		BrowserType.LaunchOptions opts = new BrowserType.LaunchOptions().setHeadless(headless);
		return opts;
	}

	private void safeClose(BrowserSession s) {
		// Interrupt the session loop thread if running
		Thread t = s.getSessionThread();
		if (t != null && t.isAlive()) {
			t.interrupt();
		}
		try {
			s.getContext().close();
		} catch (Exception e) {
			classLogger.debug("Error closing browser context for session {}: {}", s.getSessionId(), e.getMessage());
		}
		// The Playwright instance is associated with the context's browser
		try {
			s.getContext().browser().close();
		} catch (Exception e) {
			classLogger.debug("Error closing browser for session {}: {}", s.getSessionId(), e.getMessage());
		}
	}

	private static long parseLong(String envKey, long def) {
		String v = System.getenv(envKey);
		if (v == null) return def;
		try { return Long.parseLong(v.trim()); } catch (NumberFormatException e) { return def; }
	}

	private static int parseInt(String envKey, int def) {
		String v = System.getenv(envKey);
		if (v == null) return def;
		try { return Integer.parseInt(v.trim()); } catch (NumberFormatException e) { return def; }
	}
}
