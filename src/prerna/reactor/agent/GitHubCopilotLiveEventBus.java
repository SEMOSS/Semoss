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
package prerna.reactor.agent;

import java.io.Closeable;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * In-memory live event bus for GitHub Copilot websocket replay and fan-out.
 */
public final class GitHubCopilotLiveEventBus {

	public static final int MAX_REPLAY_EVENTS = 500;
	public static final long TERMINAL_RETENTION_MILLIS = TimeUnit.MINUTES.toMillis(5);
	private static final int MAX_SUBSCRIBER_QUEUE_EVENTS = 1000;

	private static final GitHubCopilotLiveEventBus INSTANCE = new GitHubCopilotLiveEventBus();

	private final Map<String, RoomState> rooms = new ConcurrentHashMap<>();

	private GitHubCopilotLiveEventBus() {
	}

	public static GitHubCopilotLiveEventBus getInstance() {
		return INSTANCE;
	}

	public void startRun(String roomId, String runId) {
		if (roomId == null || roomId.trim().isEmpty()) {
			throw new IllegalArgumentException("roomId is required");
		}
		if (runId == null || runId.trim().isEmpty()) {
			throw new IllegalArgumentException("runId is required");
		}

		RoomState state = rooms.computeIfAbsent(roomId, ignored -> new RoomState());
		synchronized (state) {
			evictExpiredStateIfNeeded(roomId, state, System.currentTimeMillis());
			state.activeRunId = runId;
			state.nextSequence = 1L;
			state.active = true;
			state.expiresAtEpochMillis = 0L;
			state.replayBuffer.clear();
		}
	}

	public GitHubCopilotLiveEvent publish(String roomId, String runId, String event, String timestamp,
			boolean ephemeral, Map<String, Object> data, boolean terminal) {
		RoomState state = rooms.computeIfAbsent(roomId, ignored -> new RoomState());
		GitHubCopilotLiveEvent liveEvent;

		synchronized (state) {
			evictExpiredStateIfNeeded(roomId, state, System.currentTimeMillis());

			if (state.activeRunId == null || !state.activeRunId.equals(runId)) {
				state.activeRunId = runId;
				state.nextSequence = 1L;
				state.active = true;
				state.expiresAtEpochMillis = 0L;
				state.replayBuffer.clear();
			}

			liveEvent = new GitHubCopilotLiveEvent(roomId, runId, state.nextSequence++, event, timestamp, ephemeral,
					data);
			state.replayBuffer.addLast(liveEvent);
			while (state.replayBuffer.size() > MAX_REPLAY_EVENTS) {
				state.replayBuffer.removeFirst();
			}
			if (terminal) {
				state.active = false;
				state.expiresAtEpochMillis = System.currentTimeMillis() + TERMINAL_RETENTION_MILLIS;
			}
		}

		for (SubscriptionImpl subscription : state.subscribers) {
			subscription.offer(liveEvent);
		}

		return liveEvent;
	}

	public Subscription subscribe(String roomId, boolean replayCurrentRun) {
		RoomState state = rooms.computeIfAbsent(roomId, ignored -> new RoomState());
		SubscriptionImpl subscription = new SubscriptionImpl(roomId, state);
		List<GitHubCopilotLiveEvent> replay = new ArrayList<>();

		synchronized (state) {
			evictExpiredStateIfNeeded(roomId, state, System.currentTimeMillis());
			state.subscribers.add(subscription);
			if (replayCurrentRun) {
				replay.addAll(state.replayBuffer);
			}
		}

		for (GitHubCopilotLiveEvent event : replay) {
			subscription.offer(event);
		}

		return subscription;
	}

	private void evictExpiredStateIfNeeded(String roomId, RoomState state, long nowMillis) {
		if (state.active) {
			return;
		}
		if (state.expiresAtEpochMillis > 0L && nowMillis > state.expiresAtEpochMillis) {
			state.activeRunId = null;
			state.nextSequence = 1L;
			state.expiresAtEpochMillis = 0L;
			state.replayBuffer.clear();
			if (state.subscribers.isEmpty()) {
				rooms.remove(roomId, state);
			}
		}
	}

	public interface Subscription extends Closeable {
		GitHubCopilotLiveEvent poll(long timeout, TimeUnit unit) throws InterruptedException;
	}

	private static final class RoomState {
		private final Deque<GitHubCopilotLiveEvent> replayBuffer = new ArrayDeque<>();
		private final CopyOnWriteArrayList<SubscriptionImpl> subscribers = new CopyOnWriteArrayList<>();
		private String activeRunId;
		private long nextSequence = 1L;
		private boolean active;
		private long expiresAtEpochMillis;
	}

	private static final class SubscriptionImpl implements Subscription {
		private final String roomId;
		private final RoomState roomState;
		private final LinkedBlockingDeque<GitHubCopilotLiveEvent> queue = new LinkedBlockingDeque<>(
				MAX_SUBSCRIBER_QUEUE_EVENTS);
		private final AtomicBoolean closed = new AtomicBoolean(false);

		private SubscriptionImpl(String roomId, RoomState roomState) {
			this.roomId = roomId;
			this.roomState = roomState;
		}

		private void offer(GitHubCopilotLiveEvent event) {
			if (closed.get()) {
				return;
			}
			while (!queue.offerLast(event)) {
				queue.pollFirst();
			}
		}

		@Override
		public GitHubCopilotLiveEvent poll(long timeout, TimeUnit unit) throws InterruptedException {
			return queue.poll(timeout, unit);
		}

		@Override
		public void close() {
			if (!closed.compareAndSet(false, true)) {
				return;
			}
			roomState.subscribers.remove(this);
			if (!roomState.active && roomState.subscribers.isEmpty() && roomState.replayBuffer.isEmpty()) {
				GitHubCopilotLiveEventBus.getInstance().rooms.remove(roomId, roomState);
			}
		}
	}

	public static String nowTimestamp() {
		return Instant.now().toString();
	}
}
