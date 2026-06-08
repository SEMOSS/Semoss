/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *******************************************************************************/
package prerna.reactor.agent.run;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.om.ThreadStore;
import prerna.reactor.agent.AgentHarnessResult;
import prerna.reactor.agent.AgentRunner;

final class AgentRunWorker {

	private static final Logger logger = LogManager.getLogger(AgentRunWorker.class);
	private static final int SCAN_LIMIT = 0;
	private static final long IDLE_WAIT_MS = 1000L;

	private final AgentRuntimeManager runtime;
	private final AgentRunStore store;
	private final AgentRunQueueCoordinator queueCoordinator;
	private final AtomicBoolean started = new AtomicBoolean(false);
	private final Object monitor = new Object();
	private final Map<String, Insight> insightsByRun = new ConcurrentHashMap<>();
	private final Set<String> localActiveRooms = ConcurrentHashMap.newKeySet();

	AgentRunWorker(AgentRuntimeManager runtime, AgentRunStore store, AgentRunQueueCoordinator queueCoordinator) {
		this.runtime = runtime;
		this.store = store;
		this.queueCoordinator = queueCoordinator;
	}

	void rememberInsight(String runId, Insight insight) {
		if (runId == null || insight == null) {
			return;
		}
		insightsByRun.put(runId, cloneInsight(insight));
	}

	void signal() {
		start();
		synchronized (monitor) {
			monitor.notifyAll();
		}
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
				List<AgentRunRecord> records = store.getQueuedRuns(SCAN_LIMIT, null);
				for (AgentRunRecord record : records) {
					Insight insight = insightsByRun.get(record.getRunId());
					if (insight == null) {
						continue;
					}
					if (tryExecute(record, insight)) {
						didWork = true;
					}
				}
			} catch (Exception e) {
				logger.warn("AgentRunWorker: queue scan failed: {}", e.getMessage(), e);
			}
			if (!didWork) {
				waitForSignal();
			}
		}
	}

	private boolean tryExecute(AgentRunRecord record, Insight insight) {
		String runId = record.getRunId();
		String roomId = record.getRoomId();
		if (!store.isOldestQueuedRunForRoom(runId, roomId)) {
			return false;
		}
		if (!localActiveRooms.add(roomId)) {
			return false;
		}

		AgentRunQueueCoordinator.ActiveRunLease lease = queueCoordinator.tryClaimTurn(runId, roomId);
		if (lease == null) {
			localActiveRooms.remove(roomId);
			return false;
		}
		try {
			String jobId = ThreadStore.getJobId();
			if (!store.markRunningIfQueued(runId, jobId)) {
				lease.close();
				localActiveRooms.remove(roomId);
				return false;
			}
			Thread.ofVirtual().name("agent-run-" + runId).start(() -> {
				try {
					execute(record, insight);
				} finally {
					lease.close();
					localActiveRooms.remove(roomId);
				}
			});
			return true;
		} catch (RuntimeException e) {
			lease.close();
			localActiveRooms.remove(roomId);
			throw e;
		}
	}

	private void execute(AgentRunRecord record, Insight insight) {
		String runId = record.getRunId();
		String jobId = ThreadStore.getJobId();
		try {
			RunAgentRequest request = record.getRequest();
			AgentHarnessResult result = AgentRunner.run(
					request.getRoomId(),
					request.getInput(),
					request.getEngineIdFallback(),
					request.getHarnessType(),
					request.getMaxTurns(),
					request.getMaxReflections(),
					request.getParamMap(),
					request.getAgentParamMap(),
					runId,
					insight);
			if (result != null) {
				store.markInputMessage(runId, result.getInputMessageId());
			}

			jobId = runtime.firstNonBlank(ThreadStore.getJobId(), jobId);
			if (result != null) {
				store.markFinalOutputMessage(runId, result.getFinalOutputMessageId());
			}
			store.markCompleted(runId, jobId, result != null ? result.getFinalText() : null);
			insightsByRun.remove(runId);
		} catch (Exception e) {
			jobId = runtime.firstNonBlank(ThreadStore.getJobId(), jobId);
			if (runtime.isCancelled(e)) {
				store.markCancelled(runId, jobId, runtime.boundedError(e));
			} else {
				store.markFailed(runId, jobId, runtime.boundedError(e));
			}
			insightsByRun.remove(runId);
			logger.warn("AgentRunWorker: runId={} failed: {}", runId, e.getMessage(), e);
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

	private static Insight cloneInsight(Insight source) {
		Insight clone = new Insight();
		clone.setUser(source.getUser());
		clone.setBaseURL(source.getBaseURL());
		clone.setProjectId(source.getProjectId());
		clone.setContextProjectId(source.getContextProjectId());
		InsightStore.getInstance().put(clone);
		return clone;
	}
}
