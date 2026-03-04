package prerna.reactor;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.om.ThreadStore;

/**
 * Tracks reactor execution across threads to enable targeted interruption of
 * specific long-running or stuck reactors.
 */
public class ReactorExecutionTracker {

	private static final Logger classLogger = LogManager.getLogger(ReactorExecutionTracker.class);

	// Singleton instance
	private static ReactorExecutionTracker instance = new ReactorExecutionTracker();

	// Map of thread ID to set of running reactor instances
	private final Map<Long, Set<RunningReactorInfo>> threadReactorMap = new ConcurrentHashMap<>();

	// Map of reactor instance to its execution info
	private final Map<IReactor, RunningReactorInfo> reactorExecutionMap = new ConcurrentHashMap<>();

	// Map of job ID to reactor instances running in that job
	private final Map<String, Set<RunningReactorInfo>> jobReactorMap = new ConcurrentHashMap<>();

	public static ReactorExecutionTracker getInstance() {
		return instance;
	}

	/**
	 * Information about a running reactor instance
	 */
	public static class RunningReactorInfo {
		private final IReactor reactor;
		private final String reactorName;
		private final long threadId;
		private final String threadName;
		private final String jobId;
		private final String sessionId;
		private final long startTime;

		public RunningReactorInfo(IReactor reactor) {
			this.reactor = reactor;
			this.reactorName = reactor.getClass().getSimpleName();
			this.threadId = Thread.currentThread().getId();
			this.threadName = Thread.currentThread().getName();

			// Get job and session context from ThreadStore
			String jobId = ThreadStore.getJobId();
			this.jobId = jobId != null ? jobId : "unknown";

			String sessionId = ThreadStore.getSessionId();
			this.sessionId = sessionId != null ? sessionId : "unknown";

			this.startTime = System.currentTimeMillis();
		}

		public IReactor getReactor() {
			return reactor;
		}

		public String getReactorName() {
			return reactorName;
		}

		public long getThreadId() {
			return threadId;
		}

		public String getThreadName() {
			return threadName;
		}

		public String getJobId() {
			return jobId;
		}

		public String getSessionId() {
			return sessionId;
		}

		public long getStartTime() {
			return startTime;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null || getClass() != obj.getClass()) {
				return false;
			}
			RunningReactorInfo that = (RunningReactorInfo) obj;
			return threadId == that.threadId && reactor.equals(that.reactor);
		}

		@Override
		public int hashCode() {
			return reactor.hashCode() + (int) threadId;
		}
	}

	/**
	 * Register the start of a reactor execution
	 */
	public void registerReactorStart(IReactor reactor) {
		if (reactor == null) {
			return;
		}

		RunningReactorInfo info = new RunningReactorInfo(reactor);

		// Add to thread-reactor mapping
		threadReactorMap.computeIfAbsent(info.getThreadId(), k -> new CopyOnWriteArraySet<>()).add(info);

		// Add to reactor-execution mapping
		reactorExecutionMap.put(reactor, info);

		// Add to job-reactor mapping
		jobReactorMap.computeIfAbsent(info.getJobId(), k -> new CopyOnWriteArraySet<>()).add(info);

		classLogger.info("Registered reactor execution: {} on thread {} (job: {}, session: {})", info.getReactorName(),
				info.getThreadId(), info.getJobId(), info.getSessionId());
	}

	/**
	 * Unregister the end of a reactor execution
	 */
	public void unregisterReactorEnd(IReactor reactor) {
		if (reactor == null) {
			return;
		}

		RunningReactorInfo info = reactorExecutionMap.remove(reactor);
		if (info != null) {
			// Remove from thread-reactor mapping
			Set<RunningReactorInfo> threadReactors = threadReactorMap.get(info.getThreadId());
			if (threadReactors != null) {
				threadReactors.remove(info);
				if (threadReactors.isEmpty()) {
					threadReactorMap.remove(info.getThreadId());
				}
			}

			// Remove from job-reactor mapping
			Set<RunningReactorInfo> jobReactors = jobReactorMap.get(info.getJobId());
			if (jobReactors != null) {
				jobReactors.remove(info);
				if (jobReactors.isEmpty()) {
					jobReactorMap.remove(info.getJobId());
				}
			}

			classLogger.info("Unregistered reactor execution: {} on thread {} (job: {}, session: {})",
					info.getReactorName(), info.getThreadId(), info.getJobId(), info.getSessionId());
		}
	}

	/**
	 * Get all currently running reactors
	 */
	public Set<RunningReactorInfo> getAllRunningReactors() {
		return new CopyOnWriteArraySet<>(reactorExecutionMap.values());
	}

	/**
	 * Interrupt a specific reactor by interrupting its thread
	 */
	public boolean interruptReactor(IReactor reactor) {
		if (reactor == null) {
			return false;
		}

		RunningReactorInfo info = reactorExecutionMap.get(reactor);
		if (info == null) {
			classLogger.warn("Cannot interrupt reactor - not found in tracking: {}",
					reactor.getClass().getSimpleName());
			return false;
		}

		// Find the thread running this reactor
		Thread[] threads = new Thread[Thread.activeCount()];
		Thread.enumerate(threads);

		for (Thread thread : threads) {
			if (thread != null && thread.getId() == info.getThreadId()) {
				classLogger.info("Interrupting reactor {} running on thread {} (ID: {})", info.getReactorName(),
						thread.getName(), thread.getId());

				// Interrupt the thread
				thread.interrupt();

				unregisterReactorEnd(reactor);

				return true;
			}
		}

		classLogger.warn("Could not find thread {} for reactor {}", info.getThreadId(), info.getReactorName());
		return false;
	}

	/**
	 * Interrupt reactors by thread ID
	 */
	public boolean interruptReactorByThread(long threadId) {
		Set<RunningReactorInfo> threadReactors = threadReactorMap.get(threadId);
		if (threadReactors == null || threadReactors.isEmpty()) {
			return false;
		}

		boolean interruptedAny = false;
		for (RunningReactorInfo info : threadReactors) {
			if (interruptReactor(info.getReactor())) {
				interruptedAny = true;
			}
		}

		return interruptedAny;
	}

	/**
	 * Interrupt reactors by name
	 */
	public boolean interruptReactorByName(String reactorName) {
		boolean interruptedAny = false;
		for (RunningReactorInfo info : reactorExecutionMap.values()) {
			if (info.getReactorName().equals(reactorName)) {
				if (interruptReactor(info.getReactor())) {
					interruptedAny = true;
				}
			}
		}
		return interruptedAny;
	}

	/**
	 * Get running reactors by thread ID
	 */
	public Set<RunningReactorInfo> getRunningReactorsByThread(long threadId) {
		return threadReactorMap.getOrDefault(threadId, new CopyOnWriteArraySet<>());
	}

	/**
	 * Get running reactors by job ID
	 */
	public Set<RunningReactorInfo> getRunningReactorsByJob(String jobId) {
		return jobReactorMap.getOrDefault(jobId, new CopyOnWriteArraySet<>());
	}

	/**
	 * Clean up stale tracking data
	 */
	public void cleanupStaleEntries() {
		classLogger.info("Cleaning up stale reactor tracking entries. Active threads: {}, Active reactors: {}",
				threadReactorMap.size(), reactorExecutionMap.size());
	}

	/**
	 * Clear all tracking data (for maintenance purposes)
	 */
	public void clearAllTracking() {
		int threadCount = threadReactorMap.size();
		int reactorCount = reactorExecutionMap.size();
		int jobCount = jobReactorMap.size();

		threadReactorMap.clear();
		reactorExecutionMap.clear();
		jobReactorMap.clear();

		classLogger.info(
				"Cleared all reactor tracking data. Removed {} thread entries, {} reactor entries, {} job entries",
				threadCount, reactorCount, jobCount);
	}
}