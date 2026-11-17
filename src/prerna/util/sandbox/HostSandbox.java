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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.om.ClientProcessWrapper;

/**
 * No jail: workers and commands run as children of the server, with its permissions. This is what
 * engine-owned workers and {@code Project} python always get, and what everyone gets when
 * {@code CHROOT_ENABLE} is false. Grants do nothing because nothing is hidden.
 */
public final class HostSandbox extends AbstractSandbox {

	private static final Logger classLogger = LogManager.getLogger(HostSandbox.class);
	private static final Capabilities CAPABILITIES = new Capabilities(true, true, false, false, false);

	HostSandbox() {
		super(null);
	}

	@Override
	public String name() {
		return "host";
	}

	@Override
	public Capabilities capabilities() {
		return CAPABILITIES;
	}

	@Override
	public WorkerHandle spawnWorker(WorkerSpec spec) {
		String port = spec.port() + "";
		switch (spec.kind()) {
		case PYTHON: {
			ClientProcessWrapper.writeLogConfigurationFile(spec.serverDirectory());
			Object[] ret = ClientProcessWrapper.startTCPServerNativePy(spec.serverDirectory(), port, spec.venvPath(),
					spec.timeout(), spec.loggerLevel(), spec.engineOwned());
			return WorkerHandle.local((Process) ret[0], (String) ret[1], spec.port(), spec.serverDirectory());
		}
		case JAVA: {
			ClientProcessWrapper.writeLogConfigurationFile(spec.serverDirectory());
			Process process = ClientProcessWrapper.startTCPServer(spec.serverDirectory(), port);
			return WorkerHandle.local(process, null, spec.port(), spec.serverDirectory());
		}
		case NODE: {
			Object[] ret = ClientProcessWrapper.startTCPServerNode(spec.serverDirectory(), port, spec.timeout(),
					spec.loggerLevel());
			return WorkerHandle.local((Process) ret[0], (String) ret[1], spec.port(), spec.serverDirectory());
		}
		default:
			throw new IllegalArgumentException("Unknown worker kind " + spec.kind());
		}
	}

	@Override
	public ExecResult exec(String workingDir, String command, int timeoutSeconds) {
		classLogger.debug("Running a command on the host: no sandbox");
		return ShellExecutor.runOnHost(workingDir, command, timeoutSeconds);
	}
}
