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
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Page;

import prerna.reactor.playwright.PlaywrightSession;
import prerna.remoteviewer.model.BrowserInputEvent;
import prerna.remoteviewer.model.RecordedStep;

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
 * <li>The {@link FrameSender} used to push frames to the React client, and the
 * {@code wsConnected} flag</li>
 * <li>An input event queue and recorded interaction steps</li>
 * </ul>
 */
public class BrowserSession {

	private final String sessionId;
	private final String userId;
	private final PlaywrightSession playwrightSession;
	private final int viewportWidth;
	private final int viewportHeight;

	private final Instant createdAt;
	private volatile Instant lastActivityAt;

	private final AtomicBoolean closed = new AtomicBoolean(false);

	/** Queue of input events waiting to be processed by the session thread. */
	public final BlockingQueue<BrowserInputEvent> eventQueue = new LinkedBlockingQueue<>(256);

	/** Callback for pushing JSON frames to the connected WebSocket client. */
	private volatile FrameSender frameSender;

	/** Whether the WebSocket client is currently connected. */
	private volatile boolean wsConnected = false;

	/** Recorded interaction steps for this session. */
	private final List<RecordedStep> recordedSteps = Collections.synchronizedList(new ArrayList<>());

	/** Handle to the session's background thread for cleanup. */
	private volatile Thread sessionThread;

	public BrowserSession(String sessionId, String userId, PlaywrightSession playwrightSession, int viewportWidth,
			int viewportHeight) {
		this.sessionId = sessionId;
		this.userId = userId;
		this.playwrightSession = playwrightSession;
		this.viewportWidth = viewportWidth;
		this.viewportHeight = viewportHeight;
		this.createdAt = Instant.now();
		this.lastActivityAt = Instant.now();
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

	public FrameSender getFrameSender() {
		return frameSender;
	}

	public void setFrameSender(FrameSender frameSender) {
		this.frameSender = frameSender;
	}

	public boolean isWsConnected() {
		return wsConnected;
	}

	public void setWsConnected(boolean connected) {
		this.wsConnected = connected;
	}

	public List<RecordedStep> getRecordedSteps() {
		return recordedSteps;
	}

	public Thread getSessionThread() {
		return sessionThread;
	}

	public void setSessionThread(Thread t) {
		this.sessionThread = t;
	}
}
