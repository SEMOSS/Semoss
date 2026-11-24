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

import prerna.auth.access.EngineGrantee;

/**
 * The engine call running on this thread: which engine is answering it, or is
 * being opened, and the app or engine whose permission the call uses, when the
 * engine gives one.
 *
 * <p>
 * The engine proxy begins a call's state once its check has passed and puts the
 * one before it back when the call ends, so while the check of a call runs, the
 * state still names the engine whose code made it. That is how the check knows
 * which engine is calling another one. Loading an engine marks it as opening
 * around its {@code open} ({@link #opening}), so the calls it makes while it
 * opens are its own calls too.
 * </p>
 *
 * <p>
 * The engine's own code reads the grantee while it answers the call: a model
 * counts the call against the App Limit or the Engine Limit of the grantee, for
 * a person whose only way to the model is that app or engine.
 * </p>
 */
public final class EngineCallPermission {

	private static final ThreadLocal<State> RUNNING = new ThreadLocal<>();

	private EngineCallPermission() {
	}

	/**
	 * The app or engine whose permission the call being answered on this thread
	 * uses, when the engine gives it one.
	 *
	 * @return the app whose custom reactors made the call or the engine whose own
	 *         code made it; null for a call made by the platform, by a person, or
	 *         by an app or engine the engine gives no permission
	 */
	public static EngineGrantee granteeMakingTheCall() {
		State state = RUNNING.get();
		return state == null ? null : state.grantee();
	}

	/**
	 * Marks an engine as opening, so the calls it makes from its {@code open} are
	 * its own calls. Only the platform's loader opens one, around the engine's
	 * {@code open}; close it when the engine has opened.
	 *
	 * <pre>
	 * try (EngineCallPermission.Opening opening = EngineCallPermission.opening(engineId, engineType)) {
	 * 	engine.open(smssProp);
	 * }
	 * </pre>
	 *
	 * @param engineId   the engine being opened
	 * @param engineType its catalog type
	 * @return the mark to close once the engine has opened
	 */
	public static Opening opening(String engineId, String engineType) {
		State previous = RUNNING.get();
		RUNNING.set(new State(new EngineGrantee(engineId, engineType), null, true));
		return new Opening(previous);
	}

	/**
	 * The engine whose code is acting on this thread: the one answering the proxy
	 * call that runs here, or the one being opened.
	 *
	 * @return the engine, or null when no engine call runs and none is opening
	 */
	static EngineGrantee actingEngine() {
		State state = RUNNING.get();
		return state == null ? null : state.engine();
	}

	/**
	 * Whether the engine acting on this thread is being opened, rather than
	 * answering a call.
	 *
	 * @return true while an engine's {@code open} runs
	 */
	static boolean isOpening() {
		State state = RUNNING.get();
		return state != null && state.opening();
	}

	/**
	 * Marks the start of a call the proxy passes on to its engine. A call made
	 * inside another call gets its own state, so an engine called by the engine an
	 * app called sees the calling engine, and sees the app only when the engine
	 * gives the calling engine no permission and gives the app one.
	 *
	 * @param engine  the engine answering the call
	 * @param grantee the app or engine whose permission the call uses, or null
	 * @return the state of the call this one runs inside, for
	 *         {@link #restore(State)}
	 */
	static State begin(EngineGrantee engine, EngineGrantee grantee) {
		State previous = RUNNING.get();
		RUNNING.set(new State(engine, grantee, false));
		return previous;
	}

	/**
	 * Marks the end of a call, putting back the state of the call it ran inside.
	 *
	 * @param previous what {@link #begin} returned
	 */
	static void restore(State previous) {
		if (previous == null) {
			RUNNING.remove();
		} else {
			RUNNING.set(previous);
		}
	}

	/**
	 * One call's state.
	 *
	 * @param engine  the engine answering the call or being opened
	 * @param grantee the app or engine whose permission the call uses, or null
	 * @param opening whether the engine is being opened
	 */
	record State(EngineGrantee engine, EngineGrantee grantee, boolean opening) {
	}

	/**
	 * An engine being opened, until it is closed.
	 */
	public static final class Opening implements AutoCloseable {

		private final State previous;
		private final Thread thread;
		private boolean closed;

		private Opening(State previous) {
			this.previous = previous;
			this.thread = Thread.currentThread();
		}

		/**
		 * Ends the opening and puts back the state before it. Closing it again, or from
		 * another thread, changes nothing.
		 */
		@Override
		public void close() {
			if (this.closed || Thread.currentThread() != this.thread) {
				return;
			}
			this.closed = true;
			restore(this.previous);
		}
	}
}
