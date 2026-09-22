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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.model.Room;
import prerna.om.Insight;
import prerna.reactor.agent.stream.AgentRunStreamService;

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
 * <h3>Execution identity</h3> Runs reuse the submitting node's remembered
 * {@link InsightHandle}. A deterministic continuation may recreate the same
 * server-owned background Insight after restart; ordinary user submissions
 * remain node-affine because their live tokens are not persisted.
 */
final class AgentRunQueueLoop {

	private static final Logger logger = LogManager.getLogger(AgentRunQueueLoop.class);
	private static final int SCAN_LIMIT = 0;
	private static final long IDLE_WAIT_MS = 1000L;
	private static final long DELIVERY_RETRY_BASE_MS = 1000L;
	private static final long DELIVERY_RETRY_MAX_MS = 60000L;

	private final AtomicBoolean started = new AtomicBoolean(false);
	private final Object monitor = new Object();
	private final Map<String, InsightHandle> insightsByRun = new ConcurrentHashMap<>();
	private final Map<String, Room> automationResumeRoomsByRun = new ConcurrentHashMap<>();
	private final Queue<String> childCompletions = new ConcurrentLinkedQueue<>();
	private final Set<String> queuedChildCompletions = ConcurrentHashMap.newKeySet();
	// Failed deliveries wait here until their backoff expires.
	private final Map<String, DeliveryRetry> deliveryRetries = new ConcurrentHashMap<>();
	private final AtomicBoolean recoveryScanStarted = new AtomicBoolean(false);
	/** Owns the room lock, the run threads, and the turn leases for this node. */
	private final AgentRunRegistry activeRuns = new AgentRunRegistry();

	/**
	 * Snapshots the submitting thread's context so the run can be executed later.
	 * Call this on the request thread, before {@link #signal}.
	 */
	void rememberInsight(String runId, Insight insight) {
		rememberInsight(runId, insight, false);
	}

	void rememberInsight(String runId, Insight insight, boolean ownsUser) {
		if (runId == null || insight == null) {
			return;
		}
		insightsByRun.put(runId, InsightHandle.capture(runId, insight, ownsUser));
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

	/** Queue one idempotent platform-message append without starting a model run. */
	void enqueueChildCompletion(String childRunId) {
		if (childRunId == null || childRunId.isBlank()) {
			return;
		}
		String normalized = childRunId.trim();
		if (queuedChildCompletions.add(normalized)) {
			childCompletions.offer(normalized);
		}
		signal();
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
		startRecoveryScan();
		while (true) {
			boolean didWork = false;
			try {
				didWork = tryExecuteChildCompletions();
				// Snapshot before the scan so a handle remembered mid-scan is never swept.
				Set<String> rebuiltBeforeScan = rebuiltHandleRunIds();
				List<AgentRunRecord> records = AgentRunStore.getSubmittedRuns(SCAN_LIMIT, null);
				Set<String> submittedRunIds = new HashSet<>();
				for (AgentRunRecord record : records) {
					submittedRunIds.add(record.runId());
				}
				releaseStaleRebuiltHandles(rebuiltBeforeScan, submittedRunIds);
				for (AgentRunRecord record : records) {
					InsightHandle insightHandle = insightsByRun.get(record.runId());
					if (insightHandle == null && record.request().getContinuationChildRunId() != null) {
						try {
							Insight continuationInsight = AgentRunService.createBackgroundExecutionInsight(record.userId(),
									record.request());
							rememberInsight(record.runId(), continuationInsight, true);
							insightHandle = insightsByRun.get(record.runId());
						} catch (SecurityException e) {
							String error = "Background agent authorization failed: " + e.getMessage();
							if (AgentRunStore.markFailedIfSubmitted(record.runId(), record.runId(), error)) {
								AgentRunStreamService.get().markTerminal(record.runId());
							}
							logger.warn("AgentRunQueueLoop: rejected continuation runId={}: {}", record.runId(),
									e.getMessage());
							didWork = true;
							continue;
						}
					}
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

	// Lost in-memory queue items are rebuilt once per JVM from recent durable child rows.
	private void startRecoveryScan() {
		if (!recoveryScanStarted.compareAndSet(false, true)) {
			return;
		}
		Thread.ofVirtual().name("child-completion-recovery").start(() -> {
			try {
				List<String> childRunIds = ChildRunCompletionService.findRecentUndeliveredChildIds();
				for (String childRunId : childRunIds) {
					enqueueChildCompletion(childRunId);
				}
				if (!childRunIds.isEmpty()) {
					logger.info("AgentRunQueueLoop: recovered {} undelivered child completions", childRunIds.size());
				}
			} catch (Exception e) {
				logger.warn("AgentRunQueueLoop: child completion recovery scan failed: {}", e.getMessage(), e);
			}
		});
	}

	private Set<String> rebuiltHandleRunIds() {
		Set<String> runIds = new HashSet<>();
		for (Map.Entry<String, InsightHandle> entry : insightsByRun.entrySet()) {
			if (entry.getValue().ownsUser()) {
				runIds.add(entry.getKey());
			}
		}
		return runIds;
	}

	// A rebuilt context whose run started on another node would otherwise stay in memory forever.
	private void releaseStaleRebuiltHandles(Set<String> rebuiltBeforeScan, Set<String> submittedRunIds) {
		for (String runId : rebuiltBeforeScan) {
			if (!submittedRunIds.contains(runId) && !activeRuns.isRegistered(runId)) {
				InsightHandle removed = insightsByRun.remove(runId);
				if (removed != null) {
					removed.release();
				}
			}
		}
	}

	/**
	 * Give ready child results the parent room turn before a later submitted run so
	 * that run receives the result in its model context.
	 */
	private boolean tryExecuteChildCompletions() {
		boolean didWork = false;
		int pending = childCompletions.size();
		for (int i = 0; i < pending; i++) {
			String childRunId = childCompletions.poll();
			if (childRunId == null) {
				break;
			}
			DeliveryRetry retry = deliveryRetries.get(childRunId);
			if (retry != null && retry.nextAttemptAtMs() > System.currentTimeMillis()) {
				childCompletions.offer(childRunId);
				continue;
			}
			try {
				ChildRunCompletionService.Delivery delivery = ChildRunCompletionService.load(childRunId);
				if (delivery == null) {
					// Terminal child or its same-owner parent no longer exists; nothing to deliver.
					deliveryRetries.remove(childRunId);
					queuedChildCompletions.remove(childRunId);
					continue;
				}
				if (delivery.mode() == SubAgentRunCompletionMode.JOIN) {
					queuedChildCompletions.remove(childRunId);
					continue;
				}
				if (tryExecuteChildCompletion(delivery)) {
					didWork = true;
				} else {
					childCompletions.offer(childRunId);
				}
			} catch (Exception e) {
				retryDelivery(childRunId, e);
			}
		}
		return didWork;
	}

	// Durable work never gives up; capped backoff keeps a stuck item cheap.
	private void retryDelivery(String childRunId, Exception e) {
		DeliveryRetry previous = deliveryRetries.get(childRunId);
		int attempts = previous == null ? 1 : previous.attempts() + 1;
		long delayMs = Math.min(DELIVERY_RETRY_MAX_MS, DELIVERY_RETRY_BASE_MS << Math.min(attempts - 1, 16));
		deliveryRetries.put(childRunId, new DeliveryRetry(attempts, System.currentTimeMillis() + delayMs));
		childCompletions.offer(childRunId);
		// Log on attempts 1, 2, 4, 8, ... so a permanently stuck item does not flood the log.
		if (Integer.bitCount(attempts) == 1) {
			logger.warn("AgentRunQueueLoop: child completion delivery failed runId={} attempt={} retryInMs={}: {}",
					childRunId, attempts, delayMs, e.getMessage(), e);
		}
	}

	private record DeliveryRetry(int attempts, long nextAttemptAtMs) {
	}

	private boolean tryExecuteChildCompletion(ChildRunCompletionService.Delivery delivery) {
		String childRunId = delivery.childRunId();
		String workId = "child-completion:" + childRunId;
		Optional<AgentRunRegistry.ActiveRun> claim = activeRuns.claimRoom(workId, delivery.parentRoomId());
		if (claim.isEmpty()) {
			return false;
		}
		AgentRunRegistry.ActiveRun activeRun = claim.get();
		try {
			ClusterRoomTurnLock.RoomTurnLease lease = ClusterRoomTurnLock.tryClaim(workId,
					delivery.parentRoomId());
			if (lease == null) {
				activeRun.close();
				return false;
			}
			activeRun.attachLease(lease);

			Thread thread = Thread.ofVirtual().name("child-completion-" + childRunId).unstarted(() -> {
				try {
					ChildRunCompletionService.deliver(delivery);
					deliveryRetries.remove(childRunId);
					queuedChildCompletions.remove(childRunId);
				} catch (Exception e) {
					retryDelivery(childRunId, e);
				} finally {
					activeRun.close();
					signal();
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
