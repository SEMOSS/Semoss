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

import prerna.auth.User;

/**
 * One way of sandboxing a user's code: what a caller can ask of it, whichever backend it is.
 *
 * <p>
 * Callers get one from {@link SandboxFactory} and never check {@link SandboxSettings.Mode}
 * themselves. The backends are {@code HostSandbox} (no jail), {@code FakechrootSandbox},
 * {@code NamespaceSandbox} and {@code OpenShellSandbox}. A backend that does not support something
 * says so in {@link #capabilities()} or does it the way it always has, and logs that it did.
 */
public interface ISandbox {

	/** A short stable name for logs: host, fakechroot, namespace or openshell. */
	String name();

	/** What this backend can do. */
	Capabilities capabilities();

	/**
	 * Makes a folder visible, read-write, to the user's workers. Idempotent. A path requested before
	 * a worker exists is remembered and replayed when it starts.
	 *
	 * @param absPath absolute host path
	 */
	void grantFolder(String absPath);

	/**
	 * Makes a project visible to the user's workers by what the user may do with it: an editor gets
	 * the project, a user who can only view it gets the read-only copy.
	 *
	 * @param user      the user
	 * @param projectId the project
	 */
	void grantProject(User user, String projectId);

	/**
	 * Takes a grant back for new opens. Files a worker already has open stay open; stopping the worker
	 * is the only absolute revoke.
	 *
	 * @param absPath absolute host path
	 * @return true when a grant was withdrawn; false when there was none or this backend cannot
	 *         ({@link Capabilities#revoke()})
	 */
	boolean revoke(String absPath);

	/**
	 * Starts a worker in this sandbox. A kind this backend has no jail for runs the way it always has
	 * and the backend logs that.
	 *
	 * @param spec what to start
	 * @return the started worker
	 * @throws Exception when it cannot be started; a backend that was asked for never falls back to a
	 *                   weaker one
	 */
	WorkerHandle spawnWorker(WorkerSpec spec) throws Exception;

	/**
	 * Runs one command to completion. A sandbox that does not track the working directory runs the
	 * command in the one given; one that does ({@link Capabilities#ownsShellState()}) is given the
	 * whole command line and returns the directory afterwards.
	 *
	 * @param workingDir     the working directory
	 * @param command        the command line
	 * @param timeoutSeconds how long it may run; a backend with its own limit applies that instead
	 * @return the outcome
	 */
	ExecResult exec(String workingDir, String command, int timeoutSeconds);
}
