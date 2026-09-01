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
package prerna.tcp;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class PayloadStruct implements Serializable {

	private static final long serialVersionUID = 1L;

	public String epoc = null;

	public enum OPERATION {
		R, PYTHON, NODE, CHROME, ECHO, ENGINE, REACTOR, INSIGHT, PROJECT, CMD, STDOUT, STDERR, STRUCTURED_STREAM,
		CANCELLED, LOG
	};

	public OPERATION operation = OPERATION.R; // setting default to R
	public String methodName = "method";
	public Object[] payload = null;
	public Class[] payloadClasses = null;

	// this is because python cant marshal java classes
	public String[] payloadClassNames = null;
	public String engineType = null;

	public String ex = null;
	public boolean processed = false;
	public boolean longRunning = false;
	public String env = null;
	public boolean interim = false;

	// this is a little bit of a complex logic
	// the idea here is say you hve something that is not serializable
	// you can keep it on that end and then work it through that
	public String[] inputAlias = null; // this should be the same size as payload
	public String aliasReturn = null;

	public boolean hasReturn = true; // if it is a void set this to true

	// parent epoc
	// to make sure that this is something that is a follow on
	public String parentEpoc = null; // do we need this other than for trace ?
	// is this request or response
	public boolean response = false;

	// object identifier
	// this is specifically useful for things like engine etc.
	public String objId = null;

	// specify the project id for reactor loads
	public String projectId = null;

	// set the project name
	public String projectName = null;

	// specify the portal id for reactor loads
	public String portalId = null;

	// set the insight id
	public String insightId = null;

	// set the job id
	public String jobId = null;

	/**
	 * When true, the python server must NOT install the per-execution cancel trace
	 * (sys.settrace) for this command. Engine-owned python processes never surface
	 * their jobId / insightId to the user, so their executions can never be
	 * cancelled anyway - skipping the trace avoids its (significant) overhead on
	 * import-heavy init / ask scripts.
	 */
	public boolean disableCancelTrace = false;

	// set the session id
	public String sessionId = null;

	/**
	 * Set any paths at the top of sys.path for this python operation required when
	 * we are dealing with multiple insights set at different apps at the same time
	 */
	public String[] asset_paths = null;

	/**
	 * Set runtime vars for the thread of execution (instead of globals() which can
	 * run into race conditions). This is useful to passing ROOT, APP_ROOT,
	 * USER_ROOT variables to the executing python. Within the executing python
	 * code, the user can run:
	 * <p>
	 * smss_get_runtime_var("ROOT")
	 * </p>
	 * in order to get the value
	 */
	public Map<String, Object> runtime_vars = null;

	/**
	 * This is to support legacy code execution for variables like ROOT, APP_ROOT,
	 * USER_ROOT. We should not use this but instead shift towards
	 * {@link #runtime_vars}
	 */
	@Deprecated
	public Map<String, Object> append_vars = null;

	/*
	 * This is really important If we have a User invoking an engine python process
	 * The engine python process has its own unique insight for variable
	 * encapsulation However, we need to know from what insight is the user invoking
	 * this request So that if the engine is making a call back/reactor request It
	 * knows which User invoked for security permissions
	 */
	public String executionInsightId = null;

	/*
	 * For logging purposes Sharing the MDC context for request parameters
	 */
	public Map<String, String> mdc;

	/*
	 * Coordination primitives for the request/response handoff between the caller
	 * thread (blocked in the socket client's executeCommand) and the socket reader
	 * thread (which delivers the response). We use a ReentrantLock + Condition
	 * instead of synchronized/wait/notify because a virtual thread that blocks on
	 * synchronized/Object.wait() pins its carrier (prior to Java 24) - and these
	 * waits can be long-running (python loads, model inference). ReentrantLock does
	 * not pin, so the virtual thread unmounts cleanly.
	 *
	 * Transient so gson/FST serialization ignores them - they are only meaningful
	 * in-process for the instance held in the client's requestMap, never on the
	 * wire.
	 */
	private final transient ReentrantLock responseLock = new ReentrantLock();
	private final transient Condition responseReady = this.responseLock.newCondition();

	/**
	 * Acquire the response lock. Must be held to call {@link #awaitResponse}.
	 */
	public void lockResponse() {
		this.responseLock.lock();
	}

	/**
	 * Release the response lock previously acquired via {@link #lockResponse()}.
	 */
	public void unlockResponse() {
		this.responseLock.unlock();
	}

	/**
	 * Wait for the response signal for at most the given time. The caller MUST
	 * already hold the response lock (via {@link #lockResponse()}).
	 *
	 * @return {@code false} if the waiting time elapsed before a signal arrived
	 */
	public boolean awaitResponse(long timeout, TimeUnit unit) throws InterruptedException {
		return this.responseReady.await(timeout, unit);
	}

	/**
	 * Wait indefinitely for the response signal. The caller MUST already hold the
	 * response lock (via {@link #lockResponse()}). Unlike {@code Object.wait()},
	 * this does not pin a virtual thread's carrier while it blocks.
	 */
	public void awaitResponse() throws InterruptedException {
		this.responseReady.await();
	}

	/**
	 * Signal any thread waiting in {@link #awaitResponse}. Acquires the response
	 * lock internally, so callers must NOT already hold it (mirrors the old
	 * {@code synchronized(ps){ ps.notifyAll(); }} usage).
	 */
	public void signalResponse() {
		this.responseLock.lock();
		try {
			this.responseReady.signalAll();
		} finally {
			this.responseLock.unlock();
		}
	}
}
