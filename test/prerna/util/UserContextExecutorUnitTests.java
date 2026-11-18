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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.objectweb.asm.Handle;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.om.ThreadStore;
import prerna.sablecc2.om.execptions.SemossPixelException;

/**
 * What {@link UserContextExecutor} gives the work of a project's custom reactor:
 * the signed in user of the calling thread and an insight of its own, nothing
 * the reactor could forge, and a clean thread afterwards. The last two tests load
 * a class through the project sandbox, as a custom reactor is loaded, and read who
 * is signed in from its task on an executor thread, which is where the engine
 * access check reads the user from.
 */
class UserContextExecutorUnitTests {

	private static final String APP = "app-1";
	private static final int SECONDS = 10;

	private final User user = new User();
	private final Insight insight = new Insight();

	@TempDir
	Path folder;

	@BeforeEach
	void aUserSignedInOnTheCallingThread() {
		this.insight.setUser(this.user);
		ThreadStore.setUser(this.user);
		ThreadStore.setSessionId("session-1");
	}

	@AfterEach
	void clear() {
		ThreadStore.remove();
		SandboxProbe.heldInsight = null;
	}

	@Test
	void aTaskRunsAsTheUserSignedInOnTheCallingThread() throws Exception {
		try (UserContextExecutor executor = UserContextExecutor.create(this.insight, 1)) {
			assertSame(this.user, executor.submit(child -> ThreadStore.getUser()).get(SECONDS, TimeUnit.SECONDS));
		}
	}

	@Test
	void aTaskCarriesTheSessionAndTheProjectContextOfTheCaller() throws Exception {
		ThreadStore.setContextProjectIdOverride(APP);
		ThreadStore.setRouteId("route-1");
		try (UserContextExecutor executor = UserContextExecutor.create(this.insight, 1)) {
			List<Object> seen = executor.submit(child -> List.<Object>of(ThreadStore.getSessionId(),
					ThreadStore.getContextProjectIdOverride(), ThreadStore.getRouteId(), child.getContextProjectId()))
					.get(SECONDS, TimeUnit.SECONDS);

			assertEquals(List.of("session-1", APP, "route-1", APP), seen);
		}
	}

	@Test
	void aTaskGetsAnInsightOfItsOwnThatIsInTheStoreOnlyWhileItRuns() throws Exception {
		int before = InsightStore.getInstance().size();
		try (UserContextExecutor executor = UserContextExecutor.create(this.insight, 1)) {
			AtomicReference<Insight> given = new AtomicReference<>();
			boolean registered = executor.submit(child -> {
				given.set(child);
				return InsightStore.getInstance().get(child.getInsightId()) == child
						&& child.getInsightId().equals(ThreadStore.getInsightId());
			}).get(SECONDS, TimeUnit.SECONDS);

			assertTrue(registered);
			assertNotEquals(this.insight.getInsightId(), given.get().getInsightId());
			assertSame(this.user, given.get().getUser());
			assertNull(InsightStore.getInstance().get(given.get().getInsightId()));
			assertEquals(before, InsightStore.getInstance().size());
		}
	}

	@Test
	void everyTaskGetsADifferentInsight() throws Exception {
		try (UserContextExecutor executor = UserContextExecutor.create(this.insight, 2)) {
			Future<String> first = executor.submit(Insight::getInsightId);
			Future<String> second = executor.submit(Insight::getInsightId);

			assertNotEquals(first.get(SECONDS, TimeUnit.SECONDS), second.get(SECONDS, TimeUnit.SECONDS));
		}
	}

	@Test
	void aTaskThatThrowsStillLeavesNothingBehind() {
		int before = InsightStore.getInstance().size();
		try (UserContextExecutor executor = UserContextExecutor.create(this.insight, 1)) {
			Future<Object> failing = executor.submit(child -> {
				throw new IllegalStateException("bad draft");
			});

			ExecutionException failure = assertThrows(ExecutionException.class,
					() -> failing.get(SECONDS, TimeUnit.SECONDS));

			assertEquals("bad draft", failure.getCause().getMessage());
			assertEquals(before, InsightStore.getInstance().size());
		}
	}

	@Test
	void theCallingThreadIsNeverChanged() throws Exception {
		try (UserContextExecutor executor = UserContextExecutor.create(this.insight, 1)) {
			executor.submit(Insight::getInsightId).get(SECONDS, TimeUnit.SECONDS);
		}

		assertSame(this.user, ThreadStore.getUser());
		assertEquals("session-1", ThreadStore.getSessionId());
		assertNull(ThreadStore.getInsightId());
	}

	@Test
	void theFolderRuleOfTheCallingThreadGoesWithTheTask() throws Exception {
		SandboxedJavaExecution.setSandboxRootForCurrentThread(this.folder.toString());
		try (UserContextExecutor executor = UserContextExecutor.create(this.insight, 1)) {
			String seen = executor.submit(child -> SandboxedJavaExecution.sandboxRootOfCurrentThread()).get(SECONDS,
					TimeUnit.SECONDS);

			assertEquals(this.folder.toAbsolutePath().normalize().toString(), seen);
		} finally {
			SandboxedJavaExecution.clearSandboxRootForCurrentThread();
		}
	}

	@Test
	void aCallingThreadWithNoFolderRuleGivesTheTaskNone() throws Exception {
		try (UserContextExecutor executor = UserContextExecutor.create(this.insight, 1)) {
			assertNull(executor.submit(child -> SandboxedJavaExecution.sandboxRootOfCurrentThread()).get(SECONDS,
					TimeUnit.SECONDS));
		}
	}

	@Test
	void tasksRunAtTheSameTime() throws Exception {
		CountDownLatch both = new CountDownLatch(2);
		try (UserContextExecutor executor = UserContextExecutor.create(this.insight, 2)) {
			List<Future<Boolean>> futures = new ArrayList<>();
			for (int i = 0; i < 2; i++) {
				futures.add(executor.submit(child -> {
					both.countDown();
					return awaited(both);
				}));
			}
			for (Future<Boolean> future : futures) {
				assertTrue(future.get(SECONDS, TimeUnit.SECONDS));
			}
		}
	}

	@Test
	void anExecutorNobodyClosedLetsItsIdleThreadsEnd() {
		try (UserContextExecutor executor = UserContextExecutor.create(this.insight, 1)) {
			assertTrue(executor.idleThreadsEnd());
		}
	}

	@Test
	void aCreatorWithNoSignedInUserIsRefused() {
		ThreadStore.remove();

		SemossPixelException refusal = assertThrows(SemossPixelException.class,
				() -> UserContextExecutor.create(this.insight, 1));

		assertEquals("An executor needs a signed in user", refusal.getMessage());
	}

	@Test
	void anInsightOfAnotherUserIsRefused() {
		Insight theirs = new Insight();
		theirs.setUser(new User());

		SemossPixelException refusal = assertThrows(SemossPixelException.class,
				() -> UserContextExecutor.create(theirs, 1));

		assertEquals("The insight does not belong to the signed in user", refusal.getMessage());
	}

	@Test
	void noInsightIsRefused() {
		assertThrows(SemossPixelException.class, () -> UserContextExecutor.create(null, 1));
	}

	@Test
	void fewerThanOneThreadIsRefused() {
		assertThrows(SemossPixelException.class, () -> UserContextExecutor.create(this.insight, 0));
	}

	@Test
	void aTaskIsRequired() {
		try (UserContextExecutor executor = UserContextExecutor.create(this.insight, 1)) {
			assertThrows(SemossPixelException.class, () -> executor.submit(null));
		}
	}

	@Test
	void theSameUserIsTheSameObjectOrTheSameLogins() {
		assertTrue(UserContextExecutor.sameUser(this.user, this.user));
		assertFalse(UserContextExecutor.sameUser(this.user, new User()));
		assertFalse(UserContextExecutor.sameUser(this.user, null));
		assertFalse(UserContextExecutor.sameUser(null, this.user));
	}

	@Test
	void theThreadCapComesFromTheSettingAndFallsBackToTheDefault() {
		try (MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class, Mockito.CALLS_REAL_METHODS)) {
			utility.when(() -> Utility.getDIHelperProperty(UserContextExecutor.MAX_THREADS_SETTING))
					.thenReturn(null, "3", " 5 ", "0", "-2", "many");

			assertEquals(UserContextExecutor.DEFAULT_MAX_THREADS, UserContextExecutor.maxThreads());
			assertEquals(3, UserContextExecutor.maxThreads());
			assertEquals(5, UserContextExecutor.maxThreads());
			assertEquals(UserContextExecutor.DEFAULT_MAX_THREADS, UserContextExecutor.maxThreads());
			assertEquals(UserContextExecutor.DEFAULT_MAX_THREADS, UserContextExecutor.maxThreads());
			assertEquals(UserContextExecutor.DEFAULT_MAX_THREADS, UserContextExecutor.maxThreads());
		}
	}

	@Test
	void anExecutorNeverHasMoreThreadsThanTheCap() {
		try (MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class, Mockito.CALLS_REAL_METHODS)) {
			utility.when(() -> Utility.getDIHelperProperty(UserContextExecutor.MAX_THREADS_SETTING)).thenReturn("2");

			try (UserContextExecutor large = UserContextExecutor.create(this.insight, 50);
					UserContextExecutor small = UserContextExecutor.create(this.insight, 1)) {
				assertEquals(2, large.getThreads());
				assertEquals(1, small.getThreads());
			}
		}
	}

	@Test
	void theExecutorIsNotOnTheListOfClassesACustomReactorCannotUse() {
		assertFalse(SemossClassLoader.isReflectionApiBlocked(UserContextExecutor.class.getName()));
	}

	@Test
	void aCustomReactorsTaskSeesTheSignedInUserOnAnExecutorThread() throws Throwable {
		SandboxProbe.heldInsight = this.insight;

		assertSame(this.user, runProbe("onExecutor"));
	}

	@Test
	void theSameTaskOnAThreadMadeByHandSeesNoUser() throws Throwable {
		SandboxProbe.heldInsight = this.insight;

		assertNull(runProbe("onBareThread"));
	}

	/**
	 * Has the custom code of a project hand a task that reads the signed in user to the
	 * named platform method, as a reactor hands its work to a helper.
	 *
	 * @param platformMethod {@code onExecutor} or {@code onBareThread}
	 * @return what the task returned
	 * @throws Throwable what the task or the call threw
	 */
	private Object runProbe(String platformMethod) throws Throwable {
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("run", mv -> {
			mv.visitInvokeDynamicInsn("apply", "()Ljava/util/function/Function;",
					new Handle(Opcodes.H_INVOKESTATIC, "java/lang/invoke/LambdaMetafactory", "metafactory",
							"(Ljava/lang/invoke/MethodHandles$Lookup;Ljava/lang/String;Ljava/lang/invoke/MethodType;"
									+ "Ljava/lang/invoke/MethodType;Ljava/lang/invoke/MethodHandle;"
									+ "Ljava/lang/invoke/MethodType;)Ljava/lang/invoke/CallSite;",
							false),
					Type.getType("(Ljava/lang/Object;)Ljava/lang/Object;"),
					new Handle(Opcodes.H_INVOKESTATIC, "prerna/util/SandboxProbe", "readSignedInUser",
							"(Lprerna/om/Insight;)Ljava/lang/Object;", false),
					Type.getType("(Lprerna/om/Insight;)Ljava/lang/Object;"));
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, "prerna/util/SandboxProbe", platformMethod,
					"(Ljava/util/function/Function;)Ljava/lang/Object;", false);
		}).bytes(), APP);
		return SandboxProbe.run(probe, "run");
	}

	private static boolean awaited(CountDownLatch latch) {
		try {
			return latch.await(SECONDS, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return false;
		}
	}
}
