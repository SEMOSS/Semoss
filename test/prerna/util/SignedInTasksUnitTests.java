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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.ThreadContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import prerna.auth.User;
import prerna.om.ThreadStore;

/**
 * The executors custom reactors run work on other threads with, one on the
 * platform threads the server shares and one on virtual threads. Each test
 * signs a user in on the test thread, as a request does, and checks what the
 * tasks it submits see on theirs.
 */
class SignedInTasksUnitTests {

	private static final long WAIT_SECONDS = 30;

	/**
	 * The two executors, with how many of their tasks run at once.
	 */
	enum Kind {
		PLATFORM(SignedInTasks.PLATFORM_PARALLEL), VIRTUAL(SignedInTasks.VIRTUAL_PARALLEL);

		private final int parallel;

		Kind(int parallel) {
			this.parallel = parallel;
		}

		ExecutorService newExecutor() {
			return this == PLATFORM ? SignedInTasks.newExecutor() : SignedInTasks.newVirtualThreadExecutor();
		}
	}

	private final User user = new User();
	private final List<ExecutorService> executors = new ArrayList<>();

	@TempDir
	Path folder;

	@AfterEach
	void clear() throws InterruptedException {
		for (ExecutorService executor : this.executors) {
			executor.shutdownNow();
			executor.awaitTermination(WAIT_SECONDS, TimeUnit.SECONDS);
		}
		ThreadStore.remove();
		ThreadContext.clearAll();
		SandboxedJavaExecution.clearSandboxRootForCurrentThread();
	}

	@ParameterizedTest
	@EnumSource(Kind.class)
	void aTaskRunsAsTheUserSignedInOnTheThreadThatSubmitsIt(Kind kind) throws Exception {
		ThreadStore.setUser(this.user);
		ThreadStore.setInsightId("insight-1");
		ThreadStore.setJobId("job-1");
		ThreadStore.setRouteId("route-1");

		List<Object> seen = executor(kind).submit(() -> Arrays.asList(ThreadStore.getUser(), ThreadStore.getInsightId(),
				ThreadStore.getJobId(), ThreadStore.getRouteId(), Thread.currentThread()))
				.get(WAIT_SECONDS, TimeUnit.SECONDS);

		assertSame(this.user, seen.get(0));
		assertEquals(List.of("insight-1", "job-1", "route-1"), seen.subList(1, 4));
		Thread taskThread = (Thread) seen.get(4);
		assertNotSame(Thread.currentThread(), taskThread);
		assertEquals(kind == Kind.VIRTUAL, taskThread.isVirtual());
	}

	@ParameterizedTest
	@EnumSource(Kind.class)
	void anExecutorStartsOnlyForASignedInUser(Kind kind) {
		assertThrows(IllegalStateException.class, kind::newExecutor);
	}

	@ParameterizedTest
	@EnumSource(Kind.class)
	void aTaskSubmittedWithNoSignedInUserIsRefused(Kind kind) {
		ThreadStore.setUser(this.user);
		ExecutorService executor = executor(kind);
		ThreadStore.remove();

		assertThrows(RejectedExecutionException.class, () -> executor.submit(() -> "never runs"));
	}

	@ParameterizedTest
	@EnumSource(Kind.class)
	void aTaskRunsAsTheUserWhoSubmitsItNotTheOneWhoStartedTheExecutor(Kind kind) throws Exception {
		ThreadStore.setUser(this.user);
		ExecutorService executor = executor(kind);
		User other = new User();
		ThreadStore.remove();
		ThreadStore.setUser(other);

		assertSame(other, executor.submit(ThreadStore::getUser).get(WAIT_SECONDS, TimeUnit.SECONDS));
	}

	@ParameterizedTest
	@EnumSource(Kind.class)
	void aTaskCarriesTheLogContextAndTheUsersFolder(Kind kind) throws Exception {
		ThreadStore.setUser(this.user);
		ThreadContext.put("userId", "jdoe");
		SandboxedJavaExecution.setSandboxRootForCurrentThread(this.folder.toString());

		List<String> seen = executor(kind).submit(
				() -> Arrays.asList(ThreadContext.get("userId"), SandboxedJavaExecution.sandboxRootForCurrentThread()))
				.get(WAIT_SECONDS, TimeUnit.SECONDS);

		assertEquals(List.of("jdoe", this.folder.toAbsolutePath().normalize().toString()), seen);
	}

	@ParameterizedTest
	@EnumSource(Kind.class)
	void aThreadKeepsNothingOnceItsTaskEnds(Kind kind) throws Exception {
		ThreadStore.setUser(this.user);
		ThreadStore.setJobId("job-1");
		ThreadContext.put("userId", "jdoe");
		SandboxedJavaExecution.setSandboxRootForCurrentThread(this.folder.toString());
		ExecutorService executor = executor(kind);
		CountDownLatch release = new CountDownLatch(1);
		Set<Thread> firstThreads = ConcurrentHashMap.newKeySet();
		List<Future<?>> first = new ArrayList<>();
		for (int i = 0; i < kind.parallel; i++) {
			first.add(executor.submit(() -> {
				firstThreads.add(Thread.currentThread());
				return release.await(WAIT_SECONDS, TimeUnit.SECONDS);
			}));
		}

		ThreadStore.remove();
		ThreadStore.setUser(this.user);
		ThreadContext.clearAll();
		SandboxedJavaExecution.clearSandboxRootForCurrentThread();
		List<Future<List<Object>>> second = new ArrayList<>();
		for (int i = 0; i < kind.parallel; i++) {
			second.add(executor.submit(() -> Arrays.asList(ThreadStore.getJobId(), ThreadContext.get("userId"),
					SandboxedJavaExecution.sandboxRootForCurrentThread(), Thread.currentThread())));
		}
		release.countDown();

		for (Future<?> task : first) {
			task.get(WAIT_SECONDS, TimeUnit.SECONDS);
		}
		for (Future<List<Object>> task : second) {
			List<Object> seen = task.get(WAIT_SECONDS, TimeUnit.SECONDS);
			assertEquals(Arrays.asList(null, null, null), seen.subList(0, 3));
			assertTrue(firstThreads.contains(seen.get(3)));
		}
	}

	@ParameterizedTest
	@EnumSource(Kind.class)
	void anExecutorRunsAtMostItsParallelTasksAtOnceAndClosingWaitsForEveryTask(Kind kind) throws Exception {
		ThreadStore.setUser(this.user);
		CountDownLatch started = new CountDownLatch(kind.parallel);
		CountDownLatch release = new CountDownLatch(1);
		AtomicInteger runningNow = new AtomicInteger();
		AtomicInteger mostAtOnce = new AtomicInteger();
		List<Future<?>> tasks = new ArrayList<>();

		try (ExecutorService executor = kind.newExecutor()) {
			for (int i = 0; i < kind.parallel + 5; i++) {
				tasks.add(executor.submit(() -> {
					mostAtOnce.accumulateAndGet(runningNow.incrementAndGet(), Math::max);
					started.countDown();
					release.await(WAIT_SECONDS, TimeUnit.SECONDS);
					runningNow.decrementAndGet();
					return null;
				}));
			}
			try {
				assertTrue(started.await(WAIT_SECONDS, TimeUnit.SECONDS));
			} finally {
				release.countDown();
			}
		}

		assertEquals(kind.parallel, mostAtOnce.get());
		for (Future<?> task : tasks) {
			assertTrue(task.isDone());
		}
	}

	@ParameterizedTest
	@EnumSource(Kind.class)
	void aTaskThatFailsHandsItsFailureToItsFuture(Kind kind) {
		ThreadStore.setUser(this.user);

		Future<Object> failing = executor(kind).submit(() -> {
			throw new IllegalArgumentException("bad input");
		});

		ExecutionException failure = assertThrows(ExecutionException.class,
				() -> failing.get(WAIT_SECONDS, TimeUnit.SECONDS));
		assertInstanceOf(IllegalArgumentException.class, failure.getCause());
	}

	@ParameterizedTest
	@EnumSource(Kind.class)
	void anExecutorHoldsAtMostItsLimitOfWaitingTasks(Kind kind) {
		ThreadStore.setUser(this.user);
		ExecutorService executor = executor(kind);
		CountDownLatch release = new CountDownLatch(1);
		try {
			for (int i = 0; i < kind.parallel + SignedInTasks.MOST_WAITING; i++) {
				executor.submit(() -> release.await(WAIT_SECONDS, TimeUnit.SECONDS));
			}

			assertThrows(RejectedExecutionException.class, () -> executor.submit(() -> "one too many"));
		} finally {
			release.countDown();
		}
	}

	@ParameterizedTest
	@EnumSource(Kind.class)
	void shutdownNowInterruptsTheRunningTasksAndCancelsTheWaitingOnes(Kind kind) throws Exception {
		ThreadStore.setUser(this.user);
		ExecutorService executor = executor(kind);
		CountDownLatch started = new CountDownLatch(kind.parallel);
		CountDownLatch never = new CountDownLatch(1);
		List<Future<?>> running = new ArrayList<>();
		for (int i = 0; i < kind.parallel; i++) {
			running.add(executor.submit(() -> {
				started.countDown();
				return never.await(WAIT_SECONDS, TimeUnit.SECONDS);
			}));
		}
		Future<String> waiting = executor.submit(() -> "never runs");
		assertTrue(started.await(WAIT_SECONDS, TimeUnit.SECONDS));

		List<Runnable> neverStarted = executor.shutdownNow();

		assertEquals(List.of(waiting), neverStarted);
		assertTrue(waiting.isCancelled());
		assertTrue(executor.awaitTermination(WAIT_SECONDS, TimeUnit.SECONDS));
		for (Future<?> task : running) {
			ExecutionException failure = assertThrows(ExecutionException.class, task::get);
			assertInstanceOf(InterruptedException.class, failure.getCause());
		}
		assertThrows(RejectedExecutionException.class, () -> executor.submit(() -> "after shutdown"));
	}

	@Test
	void aFullServerRefusesANewExecutorsTaskAndHoldsOneForAnExecutorAlreadyRunning() throws Exception {
		ThreadStore.setUser(this.user);
		CountDownLatch started = new CountDownLatch(SignedInTasks.PLATFORM_THREADS);
		CountDownLatch release = new CountDownLatch(1);
		ExecutorService alreadyRunning = executor(Kind.PLATFORM);
		try {
			alreadyRunning.submit(() -> {
				started.countDown();
				return release.await(WAIT_SECONDS, TimeUnit.SECONDS);
			});
			fill(Kind.PLATFORM, SignedInTasks.PLATFORM_THREADS - 1, () -> {
				started.countDown();
				return release.await(WAIT_SECONDS, TimeUnit.SECONDS);
			});
			assertTrue(started.await(WAIT_SECONDS, TimeUnit.SECONDS));
			fill(Kind.PLATFORM, SignedInTasks.MOST_QUEUED, () -> release.await(WAIT_SECONDS, TimeUnit.SECONDS));

			Future<String> held = alreadyRunning.submit(() -> "held");
			assertThrows(RejectedExecutionException.class, () -> executor(Kind.PLATFORM).submit(() -> "refused"));

			release.countDown();
			assertEquals("held", held.get(WAIT_SECONDS, TimeUnit.SECONDS));
		} finally {
			release.countDown();
		}
	}

	@Test
	void aServerRunningAllItsVirtualThreadsRefusesANewExecutorsTaskAndHoldsOneForAnExecutorAlreadyRunning()
			throws Exception {
		ThreadStore.setUser(this.user);
		CountDownLatch release = new CountDownLatch(1);
		ExecutorService alreadyRunning = executor(Kind.VIRTUAL);
		try {
			alreadyRunning.submit(() -> release.await(WAIT_SECONDS, TimeUnit.SECONDS));
			fill(Kind.VIRTUAL, SignedInTasks.VIRTUAL_THREADS - 1, () -> release.await(WAIT_SECONDS, TimeUnit.SECONDS));

			Future<String> held = alreadyRunning.submit(() -> "held");
			assertThrows(RejectedExecutionException.class, () -> executor(Kind.VIRTUAL).submit(() -> "refused"));

			release.countDown();
			assertEquals("held", held.get(WAIT_SECONDS, TimeUnit.SECONDS));
		} finally {
			release.countDown();
		}
	}

	/**
	 * Submits tasks across as many new executors as it takes for each to start
	 * running every one, so each task takes one of the server's threads of that
	 * kind, or waits for a platform thread.
	 *
	 * @param kind  the executors' kind
	 * @param count how many tasks
	 * @param task  the task
	 */
	private void fill(Kind kind, int count, Callable<Boolean> task) {
		for (int start = 0; start < count; start += kind.parallel) {
			ExecutorService executor = executor(kind);
			for (int i = start; i < Math.min(count, start + kind.parallel); i++) {
				executor.submit(task);
			}
		}
	}

	private ExecutorService executor(Kind kind) {
		ExecutorService executor = kind.newExecutor();
		this.executors.add(executor);
		return executor;
	}

}
