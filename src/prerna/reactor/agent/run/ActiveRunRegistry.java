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

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.agent.run.AgentRunQueueCoordinator.ActiveRunLease;

/**
 * In-memory record of the agent runs this node is executing, and the single
 * place that starts, cancels, and tears them down.
 *
 * <p>
 * A room runs one agent at a time. That rule is enforced here:
 * {@link #claimRoom} is the room lock, and holding an {@link ActiveRun} is what
 * it means to own a room. Everything a run holds while in flight -- the room,
 * the executing thread, the cluster turn lease -- lives on that one object and
 * is released by one call to {@link ActiveRun#close()}.
 *
 * <h3>Why the state is centralized</h3> These gates were previously four maps
 * plus a static set spread across the worker and {@code AgentRunner}, each
 * released at its own call sites. Any site that disagreed with the others
 * leaked a room, and a leaked room is unrecoverable without a restart: every
 * later run on it fails to start. One owner with one teardown path removes that
 * whole class of bug.
 *
 * <h3>Ownership, and why release is safe</h3> Cancelling interrupts the run's
 * thread, but an interrupt does not free a thread parked in a non-interruptible
 * call such as an HTTP request to a model provider. So {@link #requestCancel}
 * releases the room immediately rather than waiting for a thread that may never
 * unwind, which means a newer run can claim the room while the previous thread
 * is still alive.
 *
 * <p>
 * That is safe because the {@link ActiveRun} instance is itself the ownership
 * token. Release uses {@code remove(key, value)} against the exact instance
 * that claimed the room, so a late-finishing run can only ever remove its own
 * entry, never the entry of whichever run holds the room now.
 *
 * <p>
 * Instances are safe for concurrent use. {@link ActiveRun#close()} is
 * idempotent and may be called from any thread.
 */
final class ActiveRunRegistry {

	private static final Logger logger = LogManager.getLogger(ActiveRunRegistry.class);

	/**
	 * roomId to the run currently holding it. This map is the room lock: presence
	 * of a key means the room is busy.
	 */
	private final Map<String, ActiveRun> byRoom = new ConcurrentHashMap<>();

	/** runId to the same {@link ActiveRun} instances, for lookup by run. */
	private final Map<String, ActiveRun> byRun = new ConcurrentHashMap<>();

	/**
	 * Claims {@code roomId} for {@code runId}.
	 *
	 * <p>
	 * The returned run is registered but not yet started. Call
	 * {@link ActiveRun#attachLease} and {@link ActiveRun#attachThread} as those
	 * become available, and {@link ActiveRun#close()} when the run finishes or is
	 * abandoned -- including on every path that gives up between claiming and
	 * starting.
	 *
	 * @return the claim, or empty when the room is already busy or this run is
	 *         already registered
	 */
	Optional<ActiveRun> claimRoom(String runId, String roomId) {
		if (runId == null || runId.isBlank() || roomId == null || roomId.isBlank()) {
			throw new IllegalArgumentException(
					"runId and roomId are required to claim a room. runId=" + runId + " roomId=" + roomId);
		}
		ActiveRun candidate = new ActiveRun(runId, roomId);
		ActiveRun roomHolder = byRoom.putIfAbsent(roomId, candidate);
		if (roomHolder != null) {
			logger.debug("ActiveRunRegistry: room '{}' busy with runId='{}', not claiming for runId='{}'", roomId,
					roomHolder.runId, runId);
			return Optional.empty();
		}
		// Losing this race means the same run was submitted twice. Give the room back
		// so the registry never holds a room for a run it did not register.
		ActiveRun runHolder = byRun.putIfAbsent(runId, candidate);
		if (runHolder != null) {
			byRoom.remove(roomId, candidate);
			logger.warn("ActiveRunRegistry: runId='{}' is already registered, releasing room '{}'", runId, roomId);
			return Optional.empty();
		}
		return Optional.of(candidate);
	}

	/**
	 * Marks the run cancelled, interrupts its thread, and releases its room.
	 *
	 * <p>
	 * The room is freed even when the interrupt cannot take effect, so a run wedged
	 * in a provider call never leaves the room permanently unusable. The wedged
	 * thread may still be running afterwards; see the class javadoc for why that
	 * cannot corrupt the next run.
	 *
	 * @return {@code true} when a run was registered under {@code runId}
	 */
	boolean requestCancel(String runId) {
		ActiveRun run = byRun.get(runId);
		if (run == null) {
			logger.debug("ActiveRunRegistry: no active run to cancel for runId='{}'", runId);
			return false;
		}
		run.cancelRequested.set(true);
		Thread thread = run.thread;
		if (thread != null) {
			thread.interrupt();
		}
		logger.info("ActiveRunRegistry: cancel requested for runId='{}' room='{}' (thread {})", run.runId, run.roomId,
				thread == null ? "not yet started" : "interrupted");
		run.close();
		return true;
	}

	/**
	 * One agent run in flight on this node, and every gate it holds.
	 *
	 * <p>
	 * Obtained from {@link #claimRoom} and released by {@link #close()}. The
	 * instance is the ownership token for its room, so it must not be recreated or
	 * copied -- pass this object along rather than the ids it carries.
	 */
	final class ActiveRun implements AutoCloseable {

		private final String runId;
		private final String roomId;
		private final AtomicBoolean cancelRequested = new AtomicBoolean(false);
		private final AtomicBoolean released = new AtomicBoolean(false);

		/** Set by {@link #attachThread}; null until the run's thread exists. */
		private volatile Thread thread;
		/** Set by {@link #attachLease}; null when the cluster queue is disabled. */
		private volatile ActiveRunLease lease;

		private ActiveRun(String runId, String roomId) {
			this.runId = runId;
			this.roomId = roomId;
		}

		/**
		 * Attaches the cluster turn lease this run holds, so {@link #close()} releases
		 * it. Call it as soon as the turn is granted, so a cancel arriving before the
		 * thread starts still closes the lease.
		 */
		void attachLease(ActiveRunLease lease) {
			this.lease = lease;
		}

		/**
		 * Attaches the thread executing this run, so a cancel can interrupt it. Call
		 * this before {@code thread.start()}: a cancel that lands in the gap between
		 * attaching and starting sets {@link #isCancelRequested()}, which the run
		 * checks before doing any work.
		 */
		void attachThread(Thread thread) {
			this.thread = thread;
		}

		/**
		 * Whether {@link ActiveRunRegistry#requestCancel} has been called for this run.
		 *
		 * <p>
		 * Read it from this object, not by looking the run up by id: cancelling
		 * unregisters the run as it frees the room, so a lookup afterwards finds
		 * nothing. This flag stays readable for as long as anyone holds the instance,
		 * which is what lets the run's own thread see a cancel that arrived before it
		 * started.
		 */
		boolean isCancelRequested() {
			return cancelRequested.get();
		}

		/**
		 * Releases the room, the registry entries, and the cluster turn lease.
		 *
		 * <p>
		 * Idempotent and callable from any thread: the run's own thread releases in a
		 * {@code finally}, and a cancel may release the same run concurrently from a
		 * request thread. The first call does the work; later calls return immediately.
		 */
		@Override
		public void close() {
			if (!released.compareAndSet(false, true)) {
				return;
			}
			// Two-arg remove compares against this exact instance, so a run finishing
			// after its room was reclaimed cannot evict the run that holds it now.
			byRoom.remove(roomId, this);
			byRun.remove(runId, this);
			if (lease != null) {
				try {
					lease.close();
				} catch (RuntimeException e) {
					logger.warn("ActiveRunRegistry: failed to close turn lease for runId='{}': {}", runId,
							e.getMessage(), e);
				}
			}
			logger.debug("ActiveRunRegistry: released runId='{}' room='{}'", runId, roomId);
		}

		@Override
		public String toString() {
			return "ActiveRun[runId=" + runId + ", roomId=" + roomId + ", started=" + (thread != null)
					+ ", cancelRequested=" + cancelRequested.get() + "]";
		}
	}
}
