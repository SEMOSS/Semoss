package prerna.reactor.agent.runtime;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.WeakHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.ds.node.NodeTranslator;
import prerna.ds.py.PyTranslator;
import prerna.engine.impl.model.Room;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.om.ThreadStore;
import prerna.tcp.client.SocketClient;

/**
 * Resolves the session-owned, room-scoped Insight used only by the platform's
 * managed Python and Node tools. The agent harness and MCP execution continue
 * to use their transient per-run Insight.
 */
public final class AgentCodeExecutionContext {

	private static final Logger logger = LogManager.getLogger(AgentCodeExecutionContext.class);
	private static final String ROOM_PREFIX = "agent-room:";
	private static final Map<Insight, ReentrantLock> INSIGHT_LOCKS = Collections
			.synchronizedMap(new WeakHashMap<>());

	private AgentCodeExecutionContext() {
	}

	/**
	 * Runs one platform-code call against the room's stable Insight. Calls are
	 * serialized because a cancelled run may release the normal room lease before
	 * its execution thread has fully unwound.
	 */
	static <T> T executeForRoom(Room room, Insight executionInsight, String workingDirectory,
			Function<Insight, T> execution) throws InterruptedException {
		Insight roomInsight = getOrCreateRoomInsight(room, executionInsight, workingDirectory);
		ReentrantLock lock = getLock(roomInsight);
		try {
			lock.lockInterruptibly();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw e;
		}
		try {
			synchronized (roomInsight) {
				if (InsightStore.getInstance().get(roomInsight.getInsightId()) != roomInsight) {
					throw new IllegalStateException("Managed code context is no longer active");
				}
				refresh(roomInsight, room, executionInsight, workingDirectory);
				return execution.apply(roomInsight);
			}
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Best-effort cleanup when a room is removed. Existing workers are contacted,
	 * but this method never starts a Python or Node worker solely for cleanup.
	 */
	public static void removeForRoom(User user, String roomId) {
		if (user == null || roomId == null || roomId.trim().isEmpty()) {
			return;
		}
		String sessionId = requireSessionId();
		Insight roomInsight = InsightStore.getInstance().get(toInsightId(sessionId, roomId));
		if (roomInsight == null) {
			return;
		}
		ReentrantLock lock = getLock(roomInsight);
		lock.lock();
		try {
			synchronized (roomInsight) {
				User contextUser = roomInsight.getUser();
				String requestingUserId = User.getSingleLogginName(user);
				String contextUserId = contextUser != null ? User.getSingleLogginName(contextUser) : null;
				if (contextUserId == null || !Objects.equals(requestingUserId, contextUserId)) {
					logger.warn("Refusing to remove managed code context for room '{}' from a different user", roomId);
					return;
				}
				removePythonContext(user, roomInsight);
				removeNodeContext(user, roomInsight);
				InsightStore.getInstance().remove(roomInsight.getInsightId(), roomInsight);
				InsightStore.getInstance().removeFromSessionHash(sessionId, roomInsight.getInsightId());
			}
		} finally {
			lock.unlock();
		}
	}

	private static Insight getOrCreateRoomInsight(Room room, Insight executionInsight, String workingDirectory) {
		String sessionId = requireSessionId();
		String insightId = toInsightId(sessionId, room.getId());
		InsightStore insightStore = InsightStore.getInstance();
		Insight existing = insightStore.get(insightId);
		if (existing != null) {
			return existing;
		}

		Insight created = new Insight();
		created.setInsightId(insightId);
		refresh(created, room, executionInsight, workingDirectory);
		existing = insightStore.putIfAbsent(insightId, created);
		if (existing != null) {
			return existing;
		}
		insightStore.addToSessionHash(sessionId, insightId);
		return created;
	}

	private static String requireSessionId() {
		String sessionId = ThreadStore.getSessionId();
		if (sessionId == null || sessionId.trim().isEmpty()) {
			throw new IllegalStateException("Managed room code execution requires a session");
		}
		return sessionId.trim();
	}

	private static String toInsightId(String sessionId, String roomId) {
		return ROOM_PREFIX + sessionId + ":" + roomId.trim();
	}

	private static ReentrantLock getLock(Insight insight) {
		synchronized (INSIGHT_LOCKS) {
			return INSIGHT_LOCKS.computeIfAbsent(insight, key -> new ReentrantLock());
		}
	}

	private static void refresh(Insight target, Room room, Insight source, String workingDirectory) {
		target.setUser(source.getUser());
		target.setBaseURL(source.getBaseURL());
		target.setProjectId(source.getProjectId());
		target.setProjectName(source.getProjectName());
		target.setContextProjectId(source.getContextProjectId());
		target.setContextProjectName(source.getContextProjectName());
		target.setRoomForInsight(room);
		target.setInsightFolder(workingDirectory);
	}

	private static void removePythonContext(User user, Insight roomInsight) {
		SocketClient socketClient = user.getPythonSocketClient(false);
		if (socketClient == null || !socketClient.isConnected()) {
			return;
		}
		try {
			new PyTranslator(socketClient, roomInsight).removeInsightGlobals();
		} catch (Exception e) {
			logger.warn("Failed to remove Python platform context '{}'", roomInsight.getInsightId(), e);
		}
	}

	private static void removeNodeContext(User user, Insight roomInsight) {
		SocketClient socketClient = user.getNodeSocketClient(false);
		if (socketClient == null || !socketClient.isConnected()) {
			return;
		}
		try {
			new NodeTranslator(socketClient, roomInsight).removeInsightGlobals();
		} catch (Exception e) {
			logger.warn("Failed to remove Node platform context '{}'", roomInsight.getInsightId(), e);
		}
	}

}
