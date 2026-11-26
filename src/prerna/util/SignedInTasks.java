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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import prerna.auth.User;
import prerna.om.ThreadStore;

/**
 * Runs a custom reactor's work on other threads as the signed in user. A
 * project's custom code cannot create threads, and a thread from anywhere else
 * has no signed in user, so the engine refuses the calls it makes. An executor
 * from {@link #newExecutor()} or {@link #newVirtualThreadExecutor()} is an
 * ordinary {@link ExecutorService}: code written for one runs unchanged, and
 * closing it waits for every task.
 *
 * <pre>{@code
 * try (ExecutorService llmExecutor = SignedInTasks.newVirtualThreadExecutor()) {
 * 	List<Future<ResponseMessage>> docCalls = new ArrayList<>();
 * 	for (JsonObject doc : documents) {
 * 		Room docRoom = RoomUtils.createRoomForStatelessAsk(docRoomKey, this.insight, modelEngine, "");
 * 		InputMessage msg = InputMessage.builder(docRoom).build();
 * 		docCalls.add(llmExecutor.submit(() -> docRoom.ask(msg, modelEngine, null, false)));
 * 	}
 * 	for (Future<ResponseMessage> call : docCalls) {
 * 		ResponseMessage response = call.get(timeout, TimeUnit.SECONDS);
 * 	}
 * }
 * }</pre>
 *
 * <p>
 * Each task runs as the user signed in on the thread that submits it, with
 * what that thread says about the request it serves: the insight, session,
 * route and job, the context project and the rest of the {@link ThreadStore}
 * values, the log context, and the user's folder that a project's code opens
 * files in. So an engine call a task makes is checked as the reactor's own
 * calls are, for the same user and app, and a file it opens is inside the same
 * folder. The thread keeps none of it once the task ends.
 *
 * <p>
 * {@link #newExecutor()} runs tasks on platform threads the server shares, up
 * to {@value #PLATFORM_THREADS} at once from every executor, and one executor
 * runs up to {@value #PLATFORM_PARALLEL} of its tasks at once.
 * {@link #newVirtualThreadExecutor()} runs each running task on a virtual
 * thread, up to {@value #VIRTUAL_THREADS} at once across the server and
 * {@value #VIRTUAL_PARALLEL} for one executor, for many tasks that spend their
 * time waiting, such as model calls. Either executor holds up to
 * {@value #MOST_WAITING} more tasks until one of its own ends, and a task past
 * those limits is refused with a {@link RejectedExecutionException}.
 * {@code shutdownNow}, and closing the executor from an interrupted thread,
 * interrupt the running tasks and cancel the waiting ones.
 */
public final class SignedInTasks {

	private static final Logger classLogger = LogManager.getLogger(SignedInTasks.class);

	/**
	 * How many tasks run at once on the platform threads the server shares, from
	 * every {@link #newExecutor()} executor.
	 */
	public static final int PLATFORM_THREADS = 64;

	/**
	 * How many executors' tasks wait for one of the {@link #PLATFORM_THREADS}
	 * before the server refuses a task that would wait as well.
	 */
	public static final int MOST_QUEUED = 1024;

	/**
	 * How many tasks of one {@link #newExecutor()} executor run at once.
	 */
	public static final int PLATFORM_PARALLEL = 16;

	/**
	 * How many tasks run at once on virtual threads across the server, from every
	 * {@link #newVirtualThreadExecutor()} executor.
	 */
	public static final int VIRTUAL_THREADS = 4096;

	/**
	 * How many tasks of one {@link #newVirtualThreadExecutor()} executor run at
	 * once.
	 */
	public static final int VIRTUAL_PARALLEL = 256;

	/**
	 * How many tasks one executor holds while as many of its tasks run as it runs
	 * at once.
	 */
	public static final int MOST_WAITING = 1024;

	private static final String NO_USER = "A task runs as the signed in user, and no user is signed in on this thread";

	private static final String SERVER_FULL = "The server is running as many tasks as it can; submit this one once "
			+ "some end";

	private static final AtomicInteger THREADS_STARTED = new AtomicInteger();

	private static final ThreadPoolExecutor THREAD_POOL = newThreadPool();

	private static final ThreadFactory VIRTUAL_THREAD_FACTORY = Thread.ofVirtual().name("signed-in-virtual-task-", 1)
			.inheritInheritableThreadLocals(false).factory();

	private static final AtomicInteger VIRTUAL_RUNNING = new AtomicInteger();

	private SignedInTasks() {
	}

	/**
	 * Starts an executor whose tasks each run as the user signed in on the thread
	 * that submits it. Close it, as a try-with-resources statement does, to wait
	 * for its tasks.
	 *
	 * @return the executor
	 * @throws IllegalStateException when no user is signed in on this thread
	 */
	public static ExecutorService newExecutor() {
		if (ThreadStore.getUser() == null) {
			throw new IllegalStateException(NO_USER);
		}
		return new SignedInExecutor(ThreadKind.PLATFORM);
	}

	/**
	 * Starts an executor whose tasks each run on a virtual thread as the user
	 * signed in on the thread that submits it. Close it, as a try-with-resources
	 * statement does, to wait for its tasks.
	 *
	 * @return the executor
	 * @throws IllegalStateException when no user is signed in on this thread
	 */
	public static ExecutorService newVirtualThreadExecutor() {
		if (ThreadStore.getUser() == null) {
			throw new IllegalStateException(NO_USER);
		}
		return new SignedInExecutor(ThreadKind.VIRTUAL);
	}

	/**
	 * The threads every executor's tasks run on. A thread inherits nothing from
	 * the request that happened to start it, and one with no work ends after a
	 * minute.
	 *
	 * @return the pool
	 */
	private static ThreadPoolExecutor newThreadPool() {
		ThreadPoolExecutor pool = new ThreadPoolExecutor(PLATFORM_THREADS, PLATFORM_THREADS, 60L, TimeUnit.SECONDS,
				new LinkedBlockingQueue<>(MOST_QUEUED), SignedInTasks::newThread, new ThreadPoolExecutor.AbortPolicy());
		pool.allowCoreThreadTimeOut(true);
		return pool;
	}

	/**
	 * A thread for the pool, which takes no inheritable thread values and no
	 * context class loader from the thread that creates it.
	 *
	 * @param work what the pool runs on it
	 * @return the thread
	 */
	private static Thread newThread(Runnable work) {
		Thread thread = new Thread(null, work, "signed-in-task-" + THREADS_STARTED.incrementAndGet(), 0, false);
		thread.setDaemon(true);
		thread.setContextClassLoader(SignedInTasks.class.getClassLoader());
		return thread;
	}

	/**
	 * What the thread that submits a task says about the request it serves, put on
	 * the thread that runs the task while it runs.
	 *
	 * @param user         the signed in user
	 * @param threadValues the thread's {@link ThreadStore} values
	 * @param logContext   the thread's log context
	 * @param userFolder   the user's folder the thread opens files in, or null
	 */
	private record RequestContext(User user, Map<String, Object> threadValues, Map<String, String> logContext,
			String userFolder) {

		/**
		 * Reads the context of the current thread.
		 *
		 * @return the context
		 * @throws RejectedExecutionException when no user is signed in on this thread
		 */
		static RequestContext capture() {
			User user = ThreadStore.getUser();
			if (user == null) {
				throw new RejectedExecutionException(NO_USER);
			}
			return new RequestContext(user, new HashMap<>(ThreadStore.getTheadMapObject()),
					new HashMap<>(ThreadContext.getImmutableContext()),
					SandboxedJavaExecution.sandboxRootForCurrentThread());
		}

		/**
		 * Runs a task on the current thread in this context, and clears the thread
		 * once the task ends.
		 *
		 * @param command the task
		 */
		void run(Runnable command) {
			try {
				ThreadStore.setUser(this.user);
				ThreadStore.setThreadMapObject(this.threadValues);
				ThreadContext.putAll(this.logContext);
				SandboxedJavaExecution.setSandboxRootForCurrentThread(this.userFolder);
				command.run();
			} catch (RuntimeException | Error e) {
				classLogger.error("A signed in task failed: {}", e.getMessage(), e);
			} finally {
				SandboxedJavaExecution.clearSandboxRootForCurrentThread();
				ThreadContext.clearAll();
				ThreadStore.remove();
			}
		}
	}

	/**
	 * A task an executor accepted, with the context of the thread that submitted
	 * it.
	 *
	 * @param command the task
	 * @param context the context it runs in
	 */
	private record SubmittedTask(Runnable command, RequestContext context) {

		/**
		 * Runs the task in its context.
		 */
		void run() {
			this.context.run(this.command);
		}
	}

	/**
	 * Where an executor's running slots run, and how many it runs at once.
	 */
	private enum ThreadKind {

		/**
		 * The platform threads the server shares.
		 */
		PLATFORM(PLATFORM_PARALLEL) {
			@Override
			boolean start(Runnable slot) {
				try {
					THREAD_POOL.execute(slot);
					return true;
				} catch (RejectedExecutionException e) {
					return false;
				}
			}

			@Override
			void end() {
			}
		},

		/**
		 * A new virtual thread for each slot, up to
		 * {@link SignedInTasks#VIRTUAL_THREADS} across the server.
		 */
		VIRTUAL(VIRTUAL_PARALLEL) {
			@Override
			boolean start(Runnable slot) {
				if (VIRTUAL_RUNNING.incrementAndGet() > VIRTUAL_THREADS) {
					VIRTUAL_RUNNING.decrementAndGet();
					return false;
				}
				Thread thread = VIRTUAL_THREAD_FACTORY.newThread(slot);
				thread.setContextClassLoader(SignedInTasks.class.getClassLoader());
				try {
					thread.start();
					return true;
				} catch (RejectedExecutionException e) {
					VIRTUAL_RUNNING.decrementAndGet();
					return false;
				}
			}

			@Override
			void end() {
				VIRTUAL_RUNNING.decrementAndGet();
			}
		};

		private final int parallel;

		/**
		 * A kind of thread with how many of one executor's tasks run on it at once.
		 *
		 * @param parallel how many tasks of one executor run at once
		 */
		ThreadKind(int parallel) {
			this.parallel = parallel;
		}

		/**
		 * Starts a slot on a thread of this kind.
		 *
		 * @param slot the work of the slot
		 * @return false when the server runs as many threads of this kind as it takes
		 */
		abstract boolean start(Runnable slot);

		/**
		 * Gives back what a started slot held, once the slot has run its last task.
		 */
		abstract void end();
	}

	/**
	 * An executor that runs up to its kind's number of its tasks at once. Each
	 * running slot takes the executor's next waiting task when its task ends, so
	 * the executor's tasks run in the order they were submitted.
	 */
	private static final class SignedInExecutor extends AbstractExecutorService {

		private final ThreadKind kind;
		private final ReentrantLock lock = new ReentrantLock();
		private final Condition allEnded = this.lock.newCondition();
		private final Deque<SubmittedTask> waiting = new ArrayDeque<>();
		private final Set<Thread> runningOn = new HashSet<>();
		private int running;
		private boolean shutdown;
		private volatile boolean stopped;

		/**
		 * Starts an executor whose slots run on threads of one kind.
		 *
		 * @param kind where the slots run
		 */
		SignedInExecutor(ThreadKind kind) {
			this.kind = kind;
		}

		/**
		 * Accepts a task, to run as the user signed in on this thread.
		 *
		 * @param command the task
		 * @throws RejectedExecutionException when no user is signed in on this thread,
		 *                                    the executor is shut down, or it or the
		 *                                    server already holds as many tasks as it
		 *                                    takes
		 */
		@Override
		public void execute(Runnable command) {
			Objects.requireNonNull(command, "command");
			SubmittedTask task = new SubmittedTask(command, RequestContext.capture());
			this.lock.lock();
			try {
				if (this.shutdown) {
					throw new RejectedExecutionException("The executor is shut down");
				}
				if (this.running < this.kind.parallel && startRunning(task)) {
					return;
				}
				if (this.running == 0) {
					throw new RejectedExecutionException(SERVER_FULL);
				}
				if (this.waiting.size() >= MOST_WAITING) {
					throw new RejectedExecutionException("The executor already holds " + MOST_WAITING
							+ " tasks waiting to run; submit this one once some end");
				}
				this.waiting.add(task);
			} finally {
				this.lock.unlock();
			}
		}

		/**
		 * Starts a running slot with a task. Called with the lock held.
		 *
		 * @param task the task
		 * @return false when the server runs as many threads of the executor's kind as
		 *         it takes
		 */
		private boolean startRunning(SubmittedTask task) {
			this.running++;
			if (this.kind.start(() -> runFrom(task))) {
				return true;
			}
			this.running--;
			return false;
		}

		/**
		 * Runs a task and then each waiting task, one after another, until none is
		 * waiting. Runs on the slot's thread.
		 *
		 * @param first the task the slot started with
		 */
		private void runFrom(SubmittedTask first) {
			Thread thread = Thread.currentThread();
			this.lock.lock();
			try {
				this.runningOn.add(thread);
			} finally {
				this.lock.unlock();
			}
			SubmittedTask task = first;
			while (task != null) {
				if (this.stopped) {
					thread.interrupt();
				} else {
					Thread.interrupted();
				}
				task.run();
				task = nextOrEnd(thread);
			}
		}

		/**
		 * Takes the next waiting task, or ends the slot when none is waiting.
		 *
		 * @param thread the thread the slot runs on
		 * @return the next task, or null once the slot has ended
		 */
		private SubmittedTask nextOrEnd(Thread thread) {
			this.lock.lock();
			try {
				SubmittedTask next = this.waiting.poll();
				if (next == null) {
					this.runningOn.remove(thread);
					this.kind.end();
					this.running--;
					if (this.shutdown && this.running == 0) {
						this.allEnded.signalAll();
					}
				}
				return next;
			} finally {
				this.lock.unlock();
			}
		}

		/**
		 * Accepts no new task. The accepted ones still run.
		 */
		@Override
		public void shutdown() {
			this.lock.lock();
			try {
				this.shutdown = true;
				if (this.running == 0) {
					this.allEnded.signalAll();
				}
			} finally {
				this.lock.unlock();
			}
		}

		/**
		 * Accepts no new task, cancels the waiting ones and interrupts the running
		 * ones.
		 *
		 * @return the tasks that never started
		 */
		@Override
		public List<Runnable> shutdownNow() {
			this.lock.lock();
			try {
				this.shutdown = true;
				this.stopped = true;
				List<Runnable> neverStarted = new ArrayList<>();
				for (SubmittedTask task : this.waiting) {
					neverStarted.add(task.command());
					if (task.command() instanceof Future<?> future) {
						future.cancel(false);
					}
				}
				this.waiting.clear();
				for (Thread thread : this.runningOn) {
					thread.interrupt();
				}
				if (this.running == 0) {
					this.allEnded.signalAll();
				}
				return neverStarted;
			} finally {
				this.lock.unlock();
			}
		}

		/**
		 * Whether the executor accepts no new task.
		 *
		 * @return true once it is shut down
		 */
		@Override
		public boolean isShutdown() {
			this.lock.lock();
			try {
				return this.shutdown;
			} finally {
				this.lock.unlock();
			}
		}

		/**
		 * Whether the executor is shut down and every task it accepted has ended.
		 *
		 * @return true once nothing of it runs or waits
		 */
		@Override
		public boolean isTerminated() {
			this.lock.lock();
			try {
				return this.shutdown && this.running == 0;
			} finally {
				this.lock.unlock();
			}
		}

		/**
		 * Waits until the executor is shut down and every task it accepted has ended.
		 *
		 * @param timeout how long to wait
		 * @param unit    the unit of the timeout
		 * @return true when it ended, false when the time ran out first
		 * @throws InterruptedException when the waiting thread is interrupted
		 */
		@Override
		public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
			long nanos = unit.toNanos(timeout);
			this.lock.lock();
			try {
				while (!this.shutdown || this.running > 0) {
					if (nanos <= 0L) {
						return false;
					}
					nanos = this.allEnded.awaitNanos(nanos);
				}
				return true;
			} finally {
				this.lock.unlock();
			}
		}
	}

}
