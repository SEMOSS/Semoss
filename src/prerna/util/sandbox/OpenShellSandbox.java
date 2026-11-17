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
package prerna.util.sandbox;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.util.OpenShellTransport;
import prerna.util.OpenShellTransporter;
import prerna.util.SymlinkHelper;
import prerna.util.openshell.OpenShellShellRunner;
import prerna.util.openshell.OpenShellUserSandboxes;

/**
 * OpenShell: the worker is the main process of a sandbox created ahead of it, in another container,
 * and files are copied in and written back. One sandbox per user when the user has a primary login,
 * otherwise one per session.
 *
 * <p>
 * The Java worker and the node worker have no OpenShell backend and run the way they always have
 * (fakechroot and the host), logged once each. Fails closed: if the sandbox cannot be had, the worker
 * is never started on the server instead.
 */
public final class OpenShellSandbox extends AbstractSandbox {

	private static final Logger classLogger = LogManager.getLogger(OpenShellSandbox.class);
	private static final Capabilities CAPABILITIES = new Capabilities(false, true, false, true, true);
	private static final AtomicBoolean WARNED_JAVA = new AtomicBoolean(false);

	private final User user;

	OpenShellSandbox(SymlinkHelper helper, User user) {
		super(java.util.Objects.requireNonNull(helper, "a sandbox needs the user's helper"));
		this.user = user;
	}

	@Override
	public String name() {
		return "openshell";
	}

	@Override
	public Capabilities capabilities() {
		return CAPABILITIES;
	}

	@Override
	public WorkerHandle spawnWorker(WorkerSpec spec) {
		switch (spec.kind()) {
		case PYTHON:
			return spawnPython(spec);
		case JAVA:
			if (WARNED_JAVA.compareAndSet(false, true)) {
				classLogger.info("The Java worker has no OpenShell sandbox; it runs in fakechroot on the server");
			}
			try {
				return new FakechrootSandbox(helper).spawnWorker(spec);
			} catch (java.io.IOException e) {
				throw new IllegalStateException("Could not start the Java worker", e);
			}
		default:
			throw new IllegalArgumentException("The " + spec.kind() + " worker has no OpenShell sandbox");
		}
	}

	private WorkerHandle spawnPython(WorkerSpec spec) {
		try {
			OpenShellTransport transport;
			int port;
			boolean userScoped;
			boolean reattached = false;
			if (OpenShellUserSandboxes.appliesTo(spec.user())) {
				// the user's own sandbox, shared across their sessions and server instances: attach if
				// it is running, create it otherwise
				OpenShellUserSandboxes.Attached attached = OpenShellUserSandboxes.acquire(spec.user());
				transport = attached.transport;
				port = attached.port;
				userScoped = true;
				reattached = attached.reattached;
			} else {
				transport = OpenShellTransport.forSession(helper.getUserChrootFolder());
				port = transport.start();
				userScoped = false;
			}
			// the collaborator that receives every path the helper would otherwise have symlinked;
			// paths granted before the worker existed are replayed by setTransport
			helper.setTransport(new OpenShellTransporter(transport));
			return new WorkerHandle(null, null, null, port, spec.serverDirectory(), null, null, null, null, transport,
					userScoped, reattached);
		} catch (Exception e) {
			// fail closed: never fall back to an in-Tomcat worker when the deployment asked for an
			// external boundary
			throw new IllegalStateException(
					"OpenShell executor sandbox unavailable and no fallback is permitted in SANDBOX_MODE=OPENSHELL",
					e);
		}
	}

	@Override
	public ExecResult exec(String workingDir, String command, int timeoutSeconds) {
		if (user == null) {
			return new ExecResult(false, "No user to run the command for", null);
		}
		// the runner tracks cd, pwd and reset itself and applies its own time limit
		OpenShellShellRunner.Result result = OpenShellShellRunner.execute(user, workingDir, command);
		return new ExecResult(true, result.getOutput(), result.getWorkingDir());
	}
}
