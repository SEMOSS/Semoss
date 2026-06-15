package prerna.reactor.agent.run;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import prerna.util.Utility;

public final class AgentRunEventBus {

	private static final int DEFAULT_REPLAY_LIMIT = 500;
	private static final AgentRunEventBus INSTANCE = new AgentRunEventBus();

	private final Map<String, List<AgentRunEvent>> eventsByRun = new ConcurrentHashMap<>();
	private final Map<String, CopyOnWriteArrayList<Consumer<AgentRunEvent>>> subscribersByRun = new ConcurrentHashMap<>();
	private final AtomicLong sequence = new AtomicLong(0L);

	private AgentRunEventBus() {
	}

	public static AgentRunEventBus get() {
		return INSTANCE;
	}

	public AgentRunEvent publishStatus(String runId, String roomId, AgentRunStatus status, boolean terminal) {
		Map<String, Object> data = new HashMap<>();
		data.put("runId", runId);
		data.put("roomId", roomId);
		data.put("status", status == null ? null : status.name());
		return publish(runId, "status", data, terminal);
	}

	public AgentRunEvent publishMessage(String runId, Map<String, Object> data) {
		return publish(runId, "message", data, false);
	}

	public AgentRunEvent publishArtifact(String runId, Map<String, Object> data) {
		return publish(runId, "artifact", data, false);
	}

	public AgentRunEvent publish(String runId, String event, Map<String, Object> data, boolean terminal) {
		if (runId == null || runId.trim().isEmpty()) {
			return null;
		}
		AgentRunEvent runEvent = new AgentRunEvent(runId, sequence.incrementAndGet(), event, Instant.now().toString(),
				data == null ? new HashMap<>() : new HashMap<>(data), terminal);
		List<AgentRunEvent> replay = eventsByRun.computeIfAbsent(runId, k -> new ArrayList<>());
		synchronized (replay) {
			replay.add(runEvent);
			int replayLimit = replayLimit();
			while (replay.size() > replayLimit) {
				replay.remove(0);
			}
		}
		CopyOnWriteArrayList<Consumer<AgentRunEvent>> subscribers = subscribersByRun.get(runId);
		if (subscribers != null) {
			for (Consumer<AgentRunEvent> subscriber : subscribers) {
				subscriber.accept(runEvent);
			}
		}
		return runEvent;
	}

	public List<AgentRunEvent> replay(String runId) {
		List<AgentRunEvent> replay = eventsByRun.get(runId);
		if (replay == null) {
			return new ArrayList<>();
		}
		synchronized (replay) {
			return new ArrayList<>(replay);
		}
	}

	public AutoCloseable subscribe(String runId, Consumer<AgentRunEvent> consumer) {
		CopyOnWriteArrayList<Consumer<AgentRunEvent>> subscribers = subscribersByRun.computeIfAbsent(runId,
				k -> new CopyOnWriteArrayList<>());
		subscribers.add(consumer);
		return () -> subscribers.remove(consumer);
	}

	private static int replayLimit() {
		String configured = Utility.getDIHelperProperty("A2A_STREAM_REPLAY_LIMIT");
		if (configured == null || configured.trim().isEmpty()) {
			return DEFAULT_REPLAY_LIMIT;
		}
		try {
			return Math.max(1, Integer.parseInt(configured.trim()));
		} catch (NumberFormatException e) {
			return DEFAULT_REPLAY_LIMIT;
		}
	}

	public static final class AgentRunEvent {
		private final String runId;
		private final long sequence;
		private final String event;
		private final String timestamp;
		private final Map<String, Object> data;
		private final boolean terminal;

		private AgentRunEvent(String runId, long sequence, String event, String timestamp, Map<String, Object> data,
				boolean terminal) {
			this.runId = runId;
			this.sequence = sequence;
			this.event = event;
			this.timestamp = timestamp;
			this.data = data;
			this.terminal = terminal;
		}

		public String getRunId() {
			return runId;
		}

		public long getSequence() {
			return sequence;
		}

		public String getEvent() {
			return event;
		}

		public String getTimestamp() {
			return timestamp;
		}

		public Map<String, Object> getData() {
			return new HashMap<>(data);
		}

		public boolean isTerminal() {
			return terminal;
		}

		public Map<String, Object> toMap() {
			Map<String, Object> map = new HashMap<>();
			map.put("runId", runId);
			map.put("sequence", sequence);
			map.put("event", event);
			map.put("timestamp", timestamp);
			map.put("data", getData());
			map.put("terminal", terminal);
			return map;
		}
	}
}
