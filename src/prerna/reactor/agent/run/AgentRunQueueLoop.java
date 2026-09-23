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
package prerna.reactor.agent.run;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.model.Room;
import prerna.om.Insight;

/**
 * Decides which submitted agent runs start on this node, and when.
 *
 * <p>
 * One daemon thread polls for {@code SUBMITTED} rows, and each run it takes is
 * handed to {@link AgentRunExecutor} on its own virtual thread. Virtual threads
 * are what make this cheap: an agent run spends most of its life blocked on
 * model provider calls, so thousands can be in flight at once.
 *
 * <h3>What a run must pass to start</h3> {@link #tryExecute} applies four gates
 * in order, and a run that fails any of them stays {@code SUBMITTED} for a
 * later scan:
 * <ol>
 * <li>it is the oldest submitted run for its room, which is the per-room FIFO
 * order;</li>
 * <li>the room is free on this node, held by {@link AgentRunRegistry};</li>
 * <li>the cluster grants this node the room's turn, held by
 * {@link ClusterRoomTurnLock};</li>
 * <li>the {@code SUBMITTED} to {@code RUNNING} update succeeds, which is the
 * atomic step that settles who executes the run.</li>
 * </ol>
 *
 * <h3>Node affinity</h3> The loop skips runs it has no remembered
 * {@link InsightHandle} for, so a run executes on the node that received it.
 * That snapshot is taken at submission because the request thread is gone by
 * the time the run starts.
 */
final class AgentRunQueueLoop {

	private static final Logger logger = LogManager.getLogger(AgentRunQueueLoop.class);
	private static final int SCAN_LIMIT = 0;
	private static final long IDLE_WAIT_MS = 1000L;

	private final AtomicBoolean started = new AtomicBoolean(false);
	private final Object monitor = new Object();
	private final Map<String, InsightHandle> insightsByRun = new ConcurrentHashMap<>();
	private final Map<String, Room> automationResumeRoomsByRun = new ConcurrentHashMap<>();
	/** Owns the room lock, the run threads, and the turn leases for this node. */
	private final AgentRunRegistry activeRuns = new AgentRunRegistry();

	/**
	 * Snapshots the submitting thread's context so the run can be executed later.
	 * Call this on the request thread, before {@link #signal}.
	 */
	void rememberInsight(String runId, Insight insight) {
		if (runId == null || insight == null) {
			return;
		}
		insightsByRun.put(runId, InsightHandle.capture(runId, insight));
	}

	/**
	 * Captures the approving editor context and the durable owner's room for an
	 * Automation-authorized resume.
	 */
	void rememberAutomationResume(String runId, Insight insight, Room ownerRoom) {
		if (runId == null || insight == null || ownerRoom == null) {
			return;
		}
		automationResumeRoomsByRun.put(runId, ownerRoom);
		rememberInsight(runId, insight);
	}

	/** Starts the loop if it is not running, and wakes it if it is idle. */
	void signal() {
		start();
		synchronized (monitor) {
			monitor.notifyAll();
		}
	}

	/**
	 * Stops the run and frees its room. See
	 * {@link AgentRunRegistry#requestCancel(String)} for why the room is released
	 * without waiting for the run's thread to unwind.
	 *
	 * @return {@code true} when this node was executing the run
	 */
	boolean cancel(String runId) {
		return activeRuns.requestCancel(runId);
	}

	private void start() {
		if (!started.compareAndSet(false, true)) {
			return;
		}
		Thread workerThread = new Thread(this::loop, "agent-run-worker");
		workerThread.setDaemon(true);
		workerThread.start();
	}

	private void loop() {
		while (true) {
			boolean didWork = false;
			try {
				List<AgentRunRecord> records = AgentRunStore.getSubmittedRuns(SCAN_LIMIT, null);
				for (AgentRunRecord record : records) {
					InsightHandle insightHandle = insightsByRun.get(record.runId());
					if (insightHandle == null) {
						continue;
					}
					if (tryExecute(record, insightHandle)) {
						didWork = true;
					}
				}
			} catch (Exception e) {
				logger.warn("AgentRunQueueLoop: queue scan failed: {}", e.getMessage(), e);
			}
			if (!didWork) {
				waitForSignal();
			}
		}
	}

	/**
	 * Starts {@code record} if this node can take it, per the gates in the class
	 * javadoc.
	 *
	 * <p>
	 * From the moment the room is claimed, {@code activeRun.close()} is the only
	 * teardown: every path that gives up below runs it, and the run's own thread
	 * runs it in a {@code finally}. It is idempotent, so overlapping releases are
	 * fine.
	 *
	 * @return {@code true} when a run was started, which tells the loop to keep
	 *         scanning instead of going idle
	 */
	private boolean tryExecute(AgentRunRecord record, InsightHandle insightHandle) {
		String runId = record.runId();
		String roomId = record.roomId();
		if (!AgentRunStore.isOldestSubmittedRunForRoom(runId, roomId)) {
			return false;
		}
		Optional<AgentRunRegistry.ActiveRun> claim = activeRuns.claimRoom(runId, roomId);
		if (claim.isEmpty()) {
			return false;
		}
		AgentRunRegistry.ActiveRun activeRun = claim.get();

		try {
			ClusterRoomTurnLock.RoomTurnLease lease = ClusterRoomTurnLock.tryClaim(runId, roomId);
			if (lease == null) {
				// Another node holds this room's turn. Stay SUBMITTED and retry later.
				activeRun.close();
				return false;
			}
			// Attach immediately so a cancel arriving before the thread starts still
			// finds the lease to close.
			activeRun.attachLease(lease);

			if (!AgentRunStore.markRunningIfSubmitted(runId, runId)) {
				// Already claimed, cancelled, or otherwise no longer SUBMITTED.
				activeRun.close();
				cleanupInsight(runId, insightHandle);
				return false;
			}

			Room automationResumeRoom = automationResumeRoomsByRun.remove(runId);
			Thread thread = Thread.ofVirtual().name("agent-run-" + runId).unstarted(() -> {
				try (var ignored = CloseableThreadContext.putAll(insightHandle.log4jContextMap())) {
					AgentRunExecutor.execute(record, insightHandle, activeRun, automationResumeRoom);
				} finally {
					cleanupInsight(runId, insightHandle);
					activeRun.close();
				}
			});
			activeRun.attachThread(thread);
			thread.start();
			return true;
		} catch (RuntimeException e) {
			activeRun.close();
			throw e;
		}
	}

	private void waitForSignal() {
		synchronized (monitor) {
			try {
				monitor.wait(IDLE_WAIT_MS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}

	/**
	 * Drops the run's remembered context and unregisters its insight copy. Falls
	 * back to {@code insightHandle} so the insight is released even when the map
	 * entry is already gone.
	 */
	private void cleanupInsight(String runId, InsightHandle insightHandle) {
		automationResumeRoomsByRun.remove(runId);
		InsightHandle removed = insightsByRun.remove(runId);
		InsightHandle toCleanup = removed != null ? removed : insightHandle;
		if (toCleanup != null) {
			toCleanup.release();
		}
	}
}
