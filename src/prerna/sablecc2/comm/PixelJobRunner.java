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
package prerna.sablecc2.comm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.sablecc2.PixelRunner;

public class PixelJobRunner implements Runnable {

	public static final String JOB_KEY = "$JOB_ID";

	private static final Logger classLogger = LogManager.getLogger(PixelJobRunner.class);

	private volatile PixelJobStatus status = PixelJobStatus.CREATED;
	private volatile Thread executingThread;
	private String jobId;
	private String sessionId;
	private String routeId;

	private Integer localPort;
	private String localProtocol;
	private String localHostname;

	private Insight insight;
	private volatile PixelRunner runner;
	private List<String> pixel;
	private final AtomicBoolean cancelRequested = new AtomicBoolean(false);

	// Fired during cancellation to abort blocking I/O that Thread.interrupt() can't unblock.
	private volatile Runnable cancelHook;

	private Map<String, String> log4jContextMap;

	public PixelJobRunner(String jobId, Insight insight, String sessionId, String routeId) {
		this.jobId = jobId;
		this.insight = insight;
		this.sessionId = sessionId;
		this.routeId = routeId;

		// capture parent thread context before spawning new thread
		this.localPort = ThreadStore.getLocalPort();
		this.localProtocol = ThreadStore.getLocalProtocol();
		this.localHostname = ThreadStore.getLocalHostname();
		this.log4jContextMap = ThreadContext.getImmutableContext();
	}

	@Override
	public void run() {
		this.executingThread = Thread.currentThread();
		// Enrich the captured MDC context with this insight's *context* projectId so
		// every log line written during pixel execution carries [project=<id>] for
		// log streaming. getProjectId() means something else entirely - "the project
		// this insight is saved in" - and stays null for a transient REPL insight
		// (Terminal, Console) that was only ever scoped via SetContext(...), which
		// sets contextProjectId, not projectId.
		Map<String, String> enrichedContextMap = new HashMap<>(this.log4jContextMap);
		String projectId = insight.getContextProjectId();
		if (projectId != null && !projectId.isBlank()) {
			enrichedContextMap.put("projectId", projectId);
		}
		try (var ctx = CloseableThreadContext.putAll(enrichedContextMap)) {
			// set ThreadStore
			ThreadStore.setInsightId(insight.getInsightId());
			ThreadStore.setSessionId(sessionId);
			ThreadStore.setRouteId(routeId);
			ThreadStore.setJobId(jobId);
			ThreadStore.setUser(insight.getUser());
			ThreadStore.setLocalPort(localPort);
			ThreadStore.setLocalProtocol(localProtocol);
			ThreadStore.setLocalHostname(localHostname);

			this.runner = new PixelRunner();
			if (isCancelRequested()) {
				this.runner.cancelRequest();
				setStatus(PixelJobStatus.CANCELED);
				return;
			}

			try {
				setStatus(PixelJobStatus.IN_PROGRESS);
				this.runner = insight.runPixel(this.runner, this.pixel);
				if (isCancelRequested()) {
					setStatus(PixelJobStatus.CANCELED);
				} else {
					setStatus(PixelJobStatus.PROGRESS_COMPLETE);
				}
			} catch (Exception ex) {
				if (isCancelRequested()) {
					setStatus(PixelJobStatus.CANCELED);
				} else {
					classLogger.error("Pixel runnable failed during execution of: {}", this.pixel, ex);
					setStatus(PixelJobStatus.ERROR);
				}
			}
		}
	}

	public void interrupt() {
		requestCancel();
	}

	public boolean requestCancel() {
		this.cancelRequested.set(true);
		setStatus(PixelJobStatus.CANCELED);
		Thread t = this.executingThread;
		if (t != null) {
			t.interrupt();
		}
		if (this.runner != null) {
			this.runner.cancelRequest();
		}
		Runnable hook = this.cancelHook;
		if (hook != null) {
			try {
				hook.run();
			} catch (Exception e) {
				classLogger.debug("Cancel hook threw during requestCancel", e);
			}
		}
		return true;
	}

	// Set or clear (pass null) the cancellation hook.
	public void setCancelHook(Runnable hook) {
		this.cancelHook = hook;
	}

	public boolean isCancelRequested() {
		return this.cancelRequested.get() || Thread.currentThread().isInterrupted();
	}

	public void joinExecution() throws InterruptedException {
		Thread t = this.executingThread;
		if (t != null) {
			t.join();
		}
	}

	public void addPixel(String pixel) {
		if (this.pixel == null) {
			this.pixel = new ArrayList<>();
		}
		this.pixel.add(pixel);
	}

	public String getJobId() {
		return this.jobId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public Insight getInsight() {
		return this.insight;
	}

	public PixelJobStatus getPixelJobStatus() {
		return this.status;
	}

	public synchronized void setStatus(PixelJobStatus status) {
		if (status == null) {
			return;
		}
		if (this.status == PixelJobStatus.CANCELED && status != PixelJobStatus.CANCELED) {
			return;
		}
		this.status = status;
	}

	public String getStatus() {
		return this.status.getValue();
	}

	public PixelRunner getRunner() {
		return runner;
	}
}
