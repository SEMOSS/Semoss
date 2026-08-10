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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Page;

import prerna.reactor.playwright.PlaywrightSession;
import prerna.reactor.playwright.PlaywrightStep;
import prerna.reactor.playwright.StepsEnvelope;
import prerna.remoteviewer.model.RemoteBrowserInputEvent;
import prerna.remoteviewer.model.RemoteBrowserRecordedStep;

/**
 * Represents one isolated remote browser session.
 *
 * <p>
 * This is a thin wrapper around a {@link PlaywrightSession} (which owns the
 * isolated {@link BrowserContext}, {@link Page}, network tracking, and expiry).
 * On top of that it adds the live-streaming transport state that only the
 * remote viewer needs:
 * <ul>
 * <li>A dedicated event-loop thread that processes input events and streams
 * frames</li>
 * <li>The {@link RemoteBrowserFrameSender} used to push frames to the React
 * client, and the {@code wsConnected} flag</li>
 * <li>An input event queue and recorded interaction steps</li>
 * </ul>
 */
public class RemoteBrowserSession {

	private final String sessionId;
	private final String userId;
	private final PlaywrightSession playwrightSession;
	private final int viewportWidth;
	private final int viewportHeight;

	private final Instant createdAt;
	private volatile Instant lastActivityAt;

	private final AtomicBoolean closed = new AtomicBoolean(false);
	private final AtomicBoolean navigationLoading = new AtomicBoolean(false);

	/** Queue of input events waiting to be processed by the session thread. */
	public final BlockingQueue<RemoteBrowserInputEvent> eventQueue = new LinkedBlockingQueue<>(256);

	/** Callback for pushing JSON frames to the connected WebSocket client. */
	private volatile RemoteBrowserFrameSender frameSender;

	/** Whether the WebSocket client is currently connected. */
	private volatile boolean wsConnected = false;

	/** Whether future input events should be captured into the temporary buffer. */
	private volatile boolean recordingEnabled = false;

	/** Temporary, unsaved replay buffer for the current recording window. */
	private StepsEnvelope recordingHistory = new StepsEnvelope("1.0", PlaywrightSession.newMeta(""), new HashMap<>());
	/**
	 * Maps live session tab IDs to compact tab IDs inside the current recording.
	 */
	private final Map<String, String> recordingTabIds = new HashMap<>();

	/** Session-scoped step id for the temporary recording buffer. */
	private int recordingLastStepId = 0;

	/** Recorded interaction steps for this session. */
	private final List<RemoteBrowserRecordedStep> recordedSteps = Collections.synchronizedList(new ArrayList<>());

	/**
	 * Last TYPE step target signature used to aggregate character-by-character
	 * input.
	 */
	private String pendingTypeSignature;

	/** Last TYPE step currently being aggregated. */
	private PlaywrightStep pendingTypeStep;

	/** Whether the next recorded action should start a new replay page group. */
	private final Set<String> tabsStartingNextRecordedPage = new HashSet<>();

	/** Handle to the session's background thread for cleanup. */
	private volatile Thread sessionThread;

	/** The tab currently being streamed and controlled. Defaults to "tab-1". */
	private volatile String activeTabId = "tab-1";

	/** A popup observed while dispatching the current input event. */
	private String newlyOpenedTabId;

	/** Last CSS cursor sent to the remote viewer. */
	private String browserCursor = "default";

	public RemoteBrowserSession(String sessionId, String userId, PlaywrightSession playwrightSession, int viewportWidth,
			int viewportHeight) {
		this.sessionId = sessionId;
		this.userId = userId;
		this.playwrightSession = playwrightSession;
		this.viewportWidth = viewportWidth;
		this.viewportHeight = viewportHeight;
		this.createdAt = Instant.now();
		this.lastActivityAt = Instant.now();
		this.activeTabId = findLastOpenTabId();
	}

	public String getSessionId() {
		return sessionId;
	}

	public String getUserId() {
		return userId;
	}

	/**
	 * The wrapped Playwright session that owns the browser context, page, and
	 * network tracking.
	 */
	public PlaywrightSession getPlaywrightSession() {
		return playwrightSession;
	}

	public BrowserContext getContext() {
		return playwrightSession.getBrowserContext();
	}

	public Page getPage() {
		return playwrightSession.getPage();
	}

	public int getViewportWidth() {
		return viewportWidth;
	}

	public int getViewportHeight() {
		return viewportHeight;
	}

	public Instant getCreatedAt() {
		return createdAt;
	}

	public Instant getLastActivityAt() {
		return lastActivityAt;
	}

	public void touchActivity() {
		this.lastActivityAt = Instant.now();
	}

	public boolean isClosed() {
		return closed.get();
	}

	public boolean markClosed() {
		return closed.compareAndSet(false, true);
	}

	public RemoteBrowserFrameSender getRemoteBrowserFrameSender() {
		return frameSender;
	}

	public void setRemoteBrowserFrameSender(RemoteBrowserFrameSender frameSender) {
		this.frameSender = frameSender;
	}

	public boolean isWsConnected() {
		return wsConnected;
	}

	public void setWsConnected(boolean connected) {
		this.wsConnected = connected;
	}

	/**
	 * Updates the active-page navigation state.
	 *
	 * @return {@code true} only when the state changed
	 */
	public boolean updateNavigationLoading(boolean loading) {
		return navigationLoading.getAndSet(loading) != loading;
	}

	public List<RemoteBrowserRecordedStep> getRemoteBrowserRecordedSteps() {
		return recordedSteps;
	}

	public boolean isRecordingEnabled() {
		return recordingEnabled;
	}

	public void setRecordingEnabled(boolean recordingEnabled) {
		this.recordingEnabled = recordingEnabled;
		if (!recordingEnabled) {
			clearPendingTypeStep();
		}
	}

	public synchronized StepsEnvelope getRecordingHistory() {
		ensureRecordingTab();
		return recordingHistory;
	}

	public synchronized void clearRecordingBuffer() {
		recordingHistory = new StepsEnvelope("1.0", PlaywrightSession.newMeta(""), new HashMap<>());
		recordingLastStepId = 0;
		recordedSteps.clear();
		recordingTabIds.clear();
		recordingTabIds.put(normalizeTabId(activeTabId), "tab-1");
		clearPendingTypeStep();
		tabsStartingNextRecordedPage.clear();
		ensureRecordingTab();
	}

	public synchronized PlaywrightStep appendRemoteBrowserRecordedStep(String tabId, PlaywrightStep step,
			boolean startNewPage) {
		String resolvedTabId = normalizeTabId(tabId);
		ensureRecordingTab(resolvedTabId);

		PlaywrightStep newStep = withStepId(step, ++recordingLastStepId);
		List<List<PlaywrightStep>> pages = recordingHistory.steps().get(resolvedTabId);
		if (startNewPage || pages.isEmpty()) {
			pages.add(new ArrayList<>(List.of(newStep)));
		} else {
			pages.get(pages.size() - 1).add(newStep);
		}
		return newStep;
	}

	public synchronized void replaceLastRemoteBrowserRecordedStep(String tabId, PlaywrightStep step) {
		String resolvedTabId = normalizeTabId(tabId);
		List<List<PlaywrightStep>> pages = recordingHistory.steps().get(resolvedTabId);
		if (pages == null || pages.isEmpty()) {
			return;
		}
		List<PlaywrightStep> currentPage = pages.get(pages.size() - 1);
		if (currentPage.isEmpty()) {
			return;
		}
		currentPage.set(currentPage.size() - 1, step);
	}

	public synchronized PlaywrightStep getPendingTypeStep(String signature) {
		if (signature == null || !signature.equals(pendingTypeSignature)) {
			return null;
		}
		return pendingTypeStep;
	}

	public synchronized PlaywrightStep getPendingTypeStep() {
		return pendingTypeStep;
	}

	public synchronized void setPendingTypeStep(String signature, PlaywrightStep step) {
		this.pendingTypeSignature = signature;
		this.pendingTypeStep = step;
	}

	public synchronized void clearPendingTypeStep() {
		this.pendingTypeSignature = null;
		this.pendingTypeStep = null;
	}

	public synchronized void startNextRemoteBrowserRecordedStepOnNewPage() {
		startNextRemoteBrowserRecordedStepOnNewPage(activeTabId);
	}

	public synchronized boolean consumeNextRemoteBrowserRecordedStepStartsNewPage() {
		return consumeNextRemoteBrowserRecordedStepStartsNewPage(activeTabId);
	}

	public synchronized void startNextRemoteBrowserRecordedStepOnNewPage(String tabId) {
		tabsStartingNextRecordedPage.add(normalizeTabId(tabId));
	}

	public synchronized boolean consumeNextRemoteBrowserRecordedStepStartsNewPage(String tabId) {
		return tabsStartingNextRecordedPage.remove(normalizeTabId(tabId));
	}

	public Thread getSessionThread() {
		return sessionThread;
	}

	public void setSessionThread(Thread t) {
		this.sessionThread = t;
	}

	/** Returns the ID of the currently active/streamed tab. */
	public String getActiveTabId() {
		return activeTabId;
	}

	/**
	 * Updates the active tab. The next frame loop iteration will stream from this
	 * tab.
	 */
	public synchronized boolean setActiveTabId(String tabId) {
		Page page = tabId == null ? null : playwrightSession.getLivePage(tabId);
		if (page == null || page.isClosed()) {
			return false;
		}
		if (!tabId.equals(this.activeTabId)) {
			clearPendingTypeStep();
		}
		this.activeTabId = tabId;
		return true;
	}

	/**
	 * Returns the Playwright {@link com.microsoft.playwright.Page} for the active
	 * tab, falling back to tab-1 if the active tab's page is closed or missing.
	 */
	public Page getActivePage() {
		PlaywrightSession ps = playwrightSession;
		if (ps == null) {
			return null;
		}
		Page p = ps.getLivePage(activeTabId);
		if (p == null || p.isClosed()) {
			p = ps.getPage();
			if (p != null && !p.isClosed()) {
				activeTabId = "tab-1";
			}
		}
		return p;
	}

	public synchronized void clearNewlyOpenedTab() {
		newlyOpenedTabId = null;
	}

	public synchronized void markNewlyOpenedTab(String tabId) {
		newlyOpenedTabId = tabId;
	}

	public synchronized String consumeNewlyOpenedTab() {
		String tabId = newlyOpenedTabId;
		newlyOpenedTabId = null;
		return tabId;
	}

	/** Updates the remote cursor and reports whether the viewer needs a message. */
	public synchronized boolean updateBrowserCursor(String cursor) {
		String resolved = cursor == null || cursor.isBlank() ? "default" : cursor;
		if (resolved.equals(browserCursor)) {
			return false;
		}
		browserCursor = resolved;
		return true;
	}

	/** Resolves a live browser tab to its stable ID in the current recording. */
	public synchronized String getRecordingTabId(String liveTabId) {
		String normalizedLiveTabId = normalizeTabId(liveTabId);
		String existing = recordingTabIds.get(normalizedLiveTabId);
		if (existing != null) {
			return existing;
		}
		int nextIndex = 1;
		while (recordingTabIds.containsValue("tab-" + nextIndex)) {
			nextIndex++;
		}
		String recordingTabId = "tab-" + nextIndex;
		recordingTabIds.put(normalizedLiveTabId, recordingTabId);
		return recordingTabId;
	}

	/** Activates the highest-numbered remaining live tab, if one exists. */
	public synchronized void activateFallbackTab() {
		String fallbackTabId = null;
		for (Map.Entry<String, Page> entry : playwrightSession.getTabPages().entrySet()) {
			Page page = entry.getValue();
			if (page == null || page.isClosed()) {
				continue;
			}
			if (fallbackTabId == null || tabNumber(entry.getKey()) > tabNumber(fallbackTabId)) {
				fallbackTabId = entry.getKey();
			}
		}
		if (fallbackTabId != null) {
			setActiveTabId(fallbackTabId);
		}
	}

	private String findLastOpenTabId() {
		String selected = "tab-1";
		for (Map.Entry<String, Page> entry : playwrightSession.getTabPages().entrySet()) {
			Page page = entry.getValue();
			if (page != null && !page.isClosed() && tabNumber(entry.getKey()) >= tabNumber(selected)) {
				selected = entry.getKey();
			}
		}
		return selected;
	}

	private static int tabNumber(String tabId) {
		try {
			return Integer.parseInt(tabId.substring("tab-".length()));
		} catch (Exception e) {
			return 0;
		}
	}

	private void ensureRecordingTab() {
		ensureRecordingTab("tab-1");
	}

	private void ensureRecordingTab(String tabId) {
		recordingHistory.steps().computeIfAbsent(tabId, k -> new ArrayList<List<PlaywrightStep>>());
	}

	private String normalizeTabId(String tabId) {
		return tabId == null || tabId.isBlank() ? "tab-1" : tabId;
	}

	private PlaywrightStep withStepId(PlaywrightStep step, int id) {
		return new PlaywrightStep(id, step.type(), step.url(), step.coords(), step.multiCoords(), step.prompt(),
				step.text(), step.pressEnter(), step.deltaY(), step.waitUntil(), step.waitAfterMs(), step.viewport(),
				step.timestamp(), step.label(), step.description(), step.isPassword(), step.storeValue(),
				step.selector(), step.isTriggerNewTab(), step.shouldRun(), step.required(), step.sendToPlayground(),
				step.tag());
	}
}
