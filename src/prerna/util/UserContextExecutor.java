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
package prerna.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.javatuples.Pair;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.om.ThreadStore;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.insight.InsightUtility;

/**
 * Runs a project's work on other threads as the user who is signed in on the
 * calling thread.
 * <p>
 * A project's custom reactor cannot create threads, cannot write the signed in
 * user, insight or session onto a thread ({@code ThreadStore}), and cannot reach
 * {@code InsightStore}, so it has no way to give a worker thread an identity. This
 * class does that on the platform's side. The reactor passes its own
 * {@link Insight}, never a user. The executor reads who is signed in from the
 * calling thread, refuses an insight that belongs to anyone else, and gives every
 * task, on a thread the platform created:
 * <ul>
 * <li>the same signed in user, session, route, job, project context and request
 * details the calling thread has, so an engine call made by the task is checked
 * against that user's permission exactly as if the reactor had made it;</li>
 * <li>an insight of its own, a child of the reactor's, since an insight carries
 * state that is not safe to share between threads;</li>
 * <li>the calling thread's folder rule, so file work stays inside the user's
 * folder when {@code CHROOT_ENABLE} is on.</li>
 * </ul>
 * Everything is removed again when the task ends, whether it returns or throws.
 * <p>
 * The number of threads is the smaller of what the reactor asks for and the
 * {@value #MAX_THREADS_SETTING} setting in RDF_Map ({@value #DEFAULT_MAX_THREADS}
 * when it is not set).
 *
 * <pre>
 * try (UserContextExecutor executor = UserContextExecutor.create(this.insight, 4)) {
 * 	List&lt;Future&lt;String&gt;&gt; futures = new ArrayList&lt;&gt;();
 * 	for (String item : items) {
 * 		futures.add(executor.submit(taskInsight -&gt; draft(taskInsight, item)));
 * 	}
 * 	for (Future&lt;String&gt; future : futures) {
 * 		results.add(future.get());
 * 	}
 * }
 * </pre>
 */
public final class UserContextExecutor implements AutoCloseable {

	private static final Logger classLogger = LogManager.getLogger(UserContextExecutor.class);

	/** The RDF_Map setting that caps how many threads one executor may have. */
	public static final String MAX_THREADS_SETTING = "PROJECT_USER_THREADS_MAX";

	/** How long an idle worker lives, so an executor nobody closed does not keep its threads. */
	static final long IDLE_SECONDS = 60L;

	/** The cap when {@link #MAX_THREADS_SETTING} is not set or is not a number above zero. */
	public static final int DEFAULT_MAX_THREADS = 8;

	private static final AtomicInteger EXECUTOR_COUNT = new AtomicInteger();

	private final int number;
	private final ThreadPoolExecutor pool;
	private final Caller caller;

	private UserContextExecutor(int threads, Caller caller) {
		this.caller = caller;
		this.number = EXECUTOR_COUNT.incrementAndGet();
		int number = this.number;
		AtomicInteger threadCount = new AtomicInteger();
		ThreadFactory factory = task -> {
			Thread thread = new Thread(task, "user-context-" + number + "-" + threadCount.incrementAndGet());
			thread.setDaemon(true);
			return thread;
		};
		this.pool = new ThreadPoolExecutor(threads, threads, IDLE_SECONDS, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
				factory);
		this.pool.allowCoreThreadTimeOut(true);
	}

	/**
	 * Creates an executor whose tasks run as the user signed in on the calling
	 * thread. Call it from the thread that runs the reactor.
	 *
	 * @param insight the calling reactor's insight; it must belong to the user
	 *                signed in on this thread
	 * @param threads how many tasks may run at once; limited by
	 *                {@link #MAX_THREADS_SETTING}
	 * @return the executor; close it when the work is done
	 * @throws SemossPixelException if no user is signed in on this thread, the
	 *                              insight is missing or belongs to another user,
	 *                              or threads is below one
	 */
	public static UserContextExecutor create(Insight insight, int threads) {
		if (threads < 1) {
			throw new SemossPixelException("An executor needs at least one thread");
		}
		User user = ThreadStore.getUser();
		if (user == null) {
			classLogger.warn("Refused an executor for insight {}: no user is signed in on the thread",
					insight == null ? null : insight.getInsightId());
			throw new SemossPixelException("An executor needs a signed in user");
		}
		if (insight == null || !sameUser(user, insight.getUser())) {
			classLogger.warn("Refused an executor: insight {} does not belong to the user signed in on the thread",
					insight == null ? null : insight.getInsightId());
			throw new SemossPixelException("The insight does not belong to the signed in user");
		}
		int cap = maxThreads();
		int granted = Math.min(threads, cap);
		if (granted < threads) {
			classLogger.info("Asked for {} threads on insight {}, which {} limits to {}", threads, insight.getInsightId(),
					MAX_THREADS_SETTING, cap);
		}
		UserContextExecutor executor = new UserContextExecutor(granted, Caller.capture(user, insight));
		classLogger.debug("Created executor {} with {} threads for insight {}", executor.number, granted,
				insight.getInsightId());
		return executor;
	}

	/**
	 * Runs the task as the user who created this executor, with an insight of its
	 * own.
	 *
	 * @param task the work, given the insight it must use
	 * @param <T>  what the work returns
	 * @return the result; an exception the task threw comes out of
	 *         {@link Future#get()}
	 */
	public <T> Future<T> submit(Function<Insight, T> task) {
		if (task == null) {
			throw new SemossPixelException("An executor needs a task");
		}
		Callable<T> call = () -> this.caller.run(this.number, task);
		return this.pool.submit(call);
	}

	/**
	 * Stops the executor. Tasks that have not started are dropped and running ones
	 * are interrupted, so read the results of the tasks first.
	 */
	@Override
	public void close() {
		List<Runnable> dropped = this.pool.shutdownNow();
		if (dropped.isEmpty()) {
			classLogger.debug("Closed executor {}", this.number);
		} else {
			classLogger.warn("Closed executor {} with {} tasks that had not started", this.number, dropped.size());
		}
	}

	/**
	 * @return how many threads this executor has, after the cap
	 */
	public int getThreads() {
		return this.pool.getMaximumPoolSize();
	}

	/**
	 * @return whether an idle thread ends by itself, so a forgotten close does not leak threads
	 */
	boolean idleThreadsEnd() {
		return this.pool.allowsCoreThreadTimeOut();
	}

	/**
	 * Whether two users are the same person: the same object, or the same login ids
	 * and types, since a user is the pair of an id and a login type.
	 */
	static boolean sameUser(User signedIn, User other) {
		if (signedIn == null || other == null) {
			return false;
		}
		if (signedIn == other) {
			return true;
		}
		try {
			// a user with no login has no id to compare, so it is only ever itself
			List<Pair<String, String>> signedInLogins = User.getUserIdAndType(signedIn);
			return signedInLogins.equals(User.getUserIdAndType(other));
		} catch (IllegalArgumentException e) {
			return false;
		}
	}

	static int maxThreads() {
		String value = Utility.getDIHelperProperty(MAX_THREADS_SETTING);
		if (value == null) {
			return DEFAULT_MAX_THREADS;
		}
		try {
			int cap = Integer.parseInt(value.trim());
			if (cap > 0) {
				return cap;
			}
		} catch (NumberFormatException e) {
			// reported below, the same as a number that is not above zero
		}
		classLogger.warn("{} is '{}', which is not a number above zero, so the default of {} is used", MAX_THREADS_SETTING,
				value, DEFAULT_MAX_THREADS);
		return DEFAULT_MAX_THREADS;
	}

	/**
	 * What the calling thread said about the request, read once on that thread and
	 * put on a worker thread for each task.
	 */
	private static final class Caller {
		private final Insight parent;
		private final User user;
		private final String sessionId;
		private final String routeId;
		private final String jobId;
		private final Boolean schedulerMode;
		private final String localHostname;
		private final String localProtocol;
		private final Integer localPort;
		private final String projectId;
		private final String projectName;
		private final String sandboxRoot;
		private final Map<String, String> logContext;

		private Caller(Insight parent, User user) {
			this.parent = parent;
			this.user = user;
			this.sessionId = ThreadStore.getSessionId();
			this.routeId = ThreadStore.getRouteId();
			this.jobId = ThreadStore.getJobId();
			this.schedulerMode = ThreadStore.isSchedulerMode();
			this.localHostname = ThreadStore.getLocalHostname();
			this.localProtocol = ThreadStore.getLocalProtocol();
			this.localPort = ThreadStore.getLocalPort();
			this.projectId = parent.getContextProjectId();
			this.projectName = ThreadStore.getContextProjectNameOverride();
			this.sandboxRoot = SandboxedJavaExecution.sandboxRootOfCurrentThread();
			this.logContext = ThreadContext.getImmutableContext();
		}

		static Caller capture(User user, Insight parent) {
			return new Caller(parent, user);
		}

		<T> T run(int executorNumber, Function<Insight, T> task) {
			Insight child = null;
			try {
				install();
				child = new Insight();
				InsightUtility.transferDefaultVars(this.parent, child);
				child.setContextProjectId(this.projectId);
				// the engine access check and the model call log look the insight up by its id
				InsightStore.getInstance().put(child);
				ThreadStore.setInsightId(child.getInsightId());
				grantFolder(child);
				classLogger.debug("Executor {} started a task on insight {}", executorNumber, child.getInsightId());
				return task.apply(child);
			} catch (RuntimeException | Error e) {
				// the caller sees the failure through the future; this says which task it was
				classLogger.debug("A task of executor {} on insight {} failed", executorNumber,
						child == null ? null : child.getInsightId(), e);
				throw e;
			} finally {
				clean(child);
			}
		}

		private void install() {
			ThreadStore.setUser(this.user);
			if (this.sessionId != null) {
				ThreadStore.setSessionId(this.sessionId);
			}
			if (this.routeId != null) {
				ThreadStore.setRouteId(this.routeId);
			}
			if (this.jobId != null) {
				ThreadStore.setJobId(this.jobId);
			}
			if (this.schedulerMode != null) {
				ThreadStore.setSchedulerMode(this.schedulerMode);
			}
			if (this.localHostname != null) {
				ThreadStore.setLocalHostname(this.localHostname);
			}
			if (this.localProtocol != null) {
				ThreadStore.setLocalProtocol(this.localProtocol);
			}
			if (this.localPort != null) {
				ThreadStore.setLocalPort(this.localPort);
			}
			if (this.projectId != null) {
				ThreadStore.setContextProjectIdOverride(this.projectId);
			}
			if (this.projectName != null) {
				ThreadStore.setContextProjectNameOverride(this.projectName);
			}
			SandboxedJavaExecution.setSandboxRootForCurrentThread(this.sandboxRoot);
			ThreadContext.putAll(this.logContext);
		}

		// An insight made while the user was already set grants its folder to the user; this one is made first
		private void grantFolder(Insight child) {
			if (Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.CHROOT_ENABLE) + "")) {
				this.user.getSandbox().grantFolder(child.getInsightFolder());
			}
		}

		private void revokeFolder(Insight child) {
			if (Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.CHROOT_ENABLE) + "")) {
				this.user.getSandbox().revoke(child.getInsightFolder());
			}
		}

		private void clean(Insight child) {
			ThreadStore.remove();
			SandboxedJavaExecution.clearSandboxRootForCurrentThread();
			ThreadContext.clearAll();
			if (child == null) {
				return;
			}
			InsightStore.getInstance().remove(child.getInsightId());
			revokeFolder(child);
			deleteFolder(child);
		}
	}

	// The child's folder holds only the temporary files of its own task
	private static void deleteFolder(Insight child) {
		try {
			Path folder = Path.of(child.getInsightFolder());
			if (!Files.exists(folder)) {
				return;
			}
			try (var walk = Files.walk(folder)) {
				walk.sorted(Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
			}
		} catch (IOException | RuntimeException e) {
			classLogger.warn("Unable to remove the temporary folder of insight {}", child.getInsightId(), e);
		}
	}
}
