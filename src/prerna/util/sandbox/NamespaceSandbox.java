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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.om.ClientProcessWrapper;
import prerna.util.Constants;
import prerna.util.SymlinkHelper;

/**
 * The namespace jail ({@code py/sandbox_launcher.py}): the worker runs in its own user, mount and
 * network namespace, and folders are added to it live through a control socket.
 *
 * <p>
 * Two things are not namespace-jailed and run the way they always have, logged once each: the Java
 * worker (fakechroot) and one-shot commands (fakechroot, or refused with
 * {@code NAMESPACE_EXEC_FALLBACK=refuse}). The node worker is jailed here and needs no helper.
 */
public final class NamespaceSandbox extends AbstractSandbox {

	private static final Logger classLogger = LogManager.getLogger(NamespaceSandbox.class);
	private static final Capabilities CAPABILITIES = new Capabilities(true, true, true, false, false);
	private static final long CONTROL_SOCKET_WAIT_MS = 60_000L;
	private static final AtomicBoolean WARNED_JAVA = new AtomicBoolean(false);
	private static final AtomicBoolean WARNED_EXEC = new AtomicBoolean(false);

	/** @param helper the user's helper; null only for the node worker, which has none */
	NamespaceSandbox(SymlinkHelper helper) {
		super(helper);
	}

	@Override
	public String name() {
		return "namespace";
	}

	@Override
	public Capabilities capabilities() {
		return CAPABILITIES;
	}

	@Override
	public WorkerHandle spawnWorker(WorkerSpec spec) throws IOException {
		switch (spec.kind()) {
		case PYTHON:
			return spawnPython(spec);
		case NODE:
			return WorkerHandle.jailed(ClientProcessWrapper.startTCPServerNodeSandbox(spec.serverDirectory(),
					spec.port() + "", spec.timeout(), spec.loggerLevel(), null), spec.port(), spec.serverDirectory());
		case JAVA:
			if (WARNED_JAVA.compareAndSet(false, true)) {
				classLogger.info("The Java worker has no namespace jail; it runs in fakechroot");
			}
			return new FakechrootSandbox(helper).spawnWorker(spec);
		default:
			throw new IllegalArgumentException("Unknown worker kind " + spec.kind());
		}
	}

	private WorkerHandle spawnPython(WorkerSpec spec) throws IOException {
		java.util.Objects.requireNonNull(helper, "a python jail needs the user's helper");
		String insightCache = SandboxSettings.INSIGHT_CACHE_DIR.value();
		Path serverDirectoryPath = Files.createTempDirectory(Paths.get(insightCache), "a");
		String serverDirectory = serverDirectoryPath.toString();
		ClientProcessWrapper.writeLogConfigurationFile(serverDirectory);

		// the user's chroot folder name is the source identity; the launcher shortens it to keep
		// AF_UNIX socket paths valid
		String ioDirName = Paths.get(helper.getUserChrootFolder()).getFileName().toString();
		Object[] ret = ClientProcessWrapper.startTCPServerNativePySandbox(serverDirectory, spec.port() + "",
				spec.timeout(), spec.loggerLevel(), ioDirName, helper.getUserChrootFolder());
		WorkerHandle handle = WorkerHandle.jailed(ret, spec.port(), serverDirectory);

		SandboxInjector injector = new SandboxInjector(handle.controlSocketPath());
		boolean ready = false;
		try {
			ready = injector.awaitReady(CONTROL_SOCKET_WAIT_MS);
		} finally {
			if (!ready) {
				// The caller only receives the handle when this method returns, so a worker that
				// never became ready would be owned by nobody: stop it and remove what it made.
				discard(handle);
			}
		}
		if (!ready) {
			throw new IllegalStateException("Timed out waiting for namespace sandbox control socket");
		}
		// folders granted before the worker existed are replayed now
		helper.setInjector(injector);
		return handle;
	}

	/** Stops a worker that never became ready and removes the folders it was given. */
	static void discard(WorkerHandle handle) {
		Process process = handle.process();
		if (process != null) {
			try {
				process.descendants().forEach(ProcessHandle::destroyForcibly);
				process.destroyForcibly();
			} catch (RuntimeException e) {
				classLogger.warn("Could not stop the namespace launcher that failed to start", e);
			}
		}
		for (String folder : new String[] { handle.serverDirectory(), handle.ioDir(), handle.jailDir(),
				handle.controlDir() }) {
			if (folder != null && !folder.isBlank()) {
				FileUtils.deleteQuietly(new File(folder));
			}
		}
	}

	@Override
	public ExecResult exec(String workingDir, String command, int timeoutSeconds) {
		if ("refuse".equalsIgnoreCase(SandboxSettings.NAMESPACE_EXEC_FALLBACK.value())) {
			return new ExecResult(false, "Commands are not available: there is no namespace jail for one-shot "
					+ "commands and NAMESPACE_EXEC_FALLBACK is refuse", null);
		}
		if (WARNED_EXEC.compareAndSet(false, true)) {
			classLogger.info("One-shot commands have no namespace jail; they run in fakechroot "
					+ "(NAMESPACE_EXEC_FALLBACK=refuse to turn them off)");
		}
		return ShellExecutor.runInChroot(helper.getUserChrootFolder(), workingDir, command, timeoutSeconds);
	}
}
