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
package prerna.engine.impl.pipeline;

import java.util.Set;

import prerna.util.SandboxedJavaExecution;

/**
 * Work the platform does for itself and for no user, such as syncing an
 * engine's files across a cluster or pulling a project from Git. The engine
 * proxy checks every call against the signed in user and refuses a call the
 * platform makes with none; inside a platform operation the platform's own
 * calls go through, whoever is signed in on the thread.
 *
 * <pre>
 * try (PlatformOperation operation = PlatformOperation.begin("cluster sync")) {
 * 	engine.close();
 * 	pullFiles();
 * 	engine.open(smssProp);
 * }
 * </pre>
 *
 * <p>
 * Only the platform's own code runs one:
 * <ul>
 * <li>a project's custom code cannot load this class;</li>
 * <li>{@link #begin(String)} opens nothing while a class of a project is on the
 * call stack, so a platform method a custom reactor calls does not run one on
 * its behalf;</li>
 * <li>a call a project's custom code makes is never covered: it needs the
 * user's permission or the app's, as always, and so does a call made under
 * custom code that runs inside an operation.</li>
 * </ul>
 *
 * <p>
 * An operation belongs to the thread that began it and ends when it is closed.
 * A thread the work starts does not carry it; that thread begins its own. Keep
 * an operation around the platform's own steps only, never around a pixel or a
 * reactor a user sends.
 */
public final class PlatformOperation implements AutoCloseable {

	private static final ThreadLocal<PlatformOperation> RUNNING = new ThreadLocal<>();

	private static final StackWalker STACK = StackWalker.getInstance(
			Set.of(StackWalker.Option.RETAIN_CLASS_REFERENCE, StackWalker.Option.SHOW_HIDDEN_FRAMES));

	private final String name;
	private final Thread thread;
	private final PlatformOperation outer;
	private final boolean opened;
	private boolean closed;

	private PlatformOperation(String name, PlatformOperation outer, boolean opened) {
		this.name = name;
		this.thread = Thread.currentThread();
		this.outer = outer;
		this.opened = opened;
	}

	/**
	 * Begins a platform operation on this thread. Close it when the work ends; a
	 * try-with-resources statement does. One begun inside another leaves the outer
	 * one running when it closes.
	 *
	 * @param name what the platform is doing, for the log, such as "cluster sync"
	 * @return the operation to close; it covers nothing when a project's custom
	 *         code is on the call stack
	 * @throws IllegalArgumentException when the name is empty
	 */
	public static PlatformOperation begin(String name) {
		if (name == null || name.isBlank()) {
			throw new IllegalArgumentException("A platform operation needs a name");
		}
		PlatformOperation outer = RUNNING.get();
		if (projectCodeIsOnTheStack()) {
			return new PlatformOperation(name, outer, false);
		}
		PlatformOperation operation = new PlatformOperation(name, outer, true);
		RUNNING.set(operation);
		return operation;
	}

	/**
	 * Ends the operation and puts back the one it began inside, if any. Closing it
	 * again, or from another thread, changes nothing.
	 */
	@Override
	public void close() {
		if (!this.opened || this.closed || Thread.currentThread() != this.thread) {
			return;
		}
		this.closed = true;
		if (RUNNING.get() != this) {
			return;
		}
		PlatformOperation stillRunning = this.outer;
		while (stillRunning != null && stillRunning.closed) {
			stillRunning = stillRunning.outer;
		}
		if (stillRunning == null) {
			RUNNING.remove();
		} else {
			RUNNING.set(stillRunning);
		}
	}

	/**
	 * What the operation was begun for.
	 *
	 * @return the name given to {@link #begin(String)}
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * The operation that covers a call the platform is making on this thread now.
	 *
	 * @return the name of the running operation, or null when none is running or a
	 *         project's custom code is on the call stack
	 */
	static String covering() {
		PlatformOperation operation = RUNNING.get();
		if (operation == null || projectCodeIsOnTheStack()) {
			return null;
		}
		return operation.name;
	}

	/**
	 * Whether a class of a project's custom code is anywhere on the call stack.
	 *
	 * @return true when project code is acting, directly or through the platform
	 */
	static boolean projectCodeIsOnTheStack() {
		return STACK.walk(frames -> frames
				.anyMatch(frame -> SandboxedJavaExecution.isProjectClass(frame.getDeclaringClass())));
	}

}
