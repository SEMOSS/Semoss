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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import prerna.om.ClientProcessWrapper;
import prerna.util.SymlinkHelper;
import prerna.util.Utility;

/**
 * The fakechroot jail: the worker or command runs in the user's chroot folder, which holds symlinks
 * to what the user may see. The default when jailing is on. Also what the Java worker uses in every
 * jail mode, because it has no other backend.
 */
public final class FakechrootSandbox extends AbstractSandbox {

	private static final Capabilities CAPABILITIES = new Capabilities(true, true, true, false, false);

	FakechrootSandbox(SymlinkHelper helper) {
		super(java.util.Objects.requireNonNull(helper, "a jail needs the user's helper"));
	}

	@Override
	public String name() {
		return "fakechroot";
	}

	@Override
	public Capabilities capabilities() {
		return CAPABILITIES;
	}

	@Override
	public WorkerHandle spawnWorker(WorkerSpec spec) throws IOException {
		String chrootFolder = helper.getUserChrootFolder();
		// a fake semoss home lives in the chroot, so the worker starts in a temp folder under it
		Path chrootPath = Paths.get(chrootFolder);
		Path chrootBaseFolderPath = Paths.get(chrootPath + Utility.getBaseFolder());
		Path serverDirectoryPath = Files.createTempDirectory(chrootBaseFolderPath, "a");
		// the log4j file is replaced as the worker starts in the chroot, so it needs the chroot-relative path
		String relative = chrootPath.relativize(serverDirectoryPath).toString();
		if (!relative.startsWith("/")) {
			relative = "/" + relative;
		}
		ClientProcessWrapper.writeLogConfigurationFile(chrootBaseFolderPath.toString(), relative);

		String port = spec.port() + "";
		switch (spec.kind()) {
		case PYTHON: {
			Object[] ret = ClientProcessWrapper.startTCPServerNativePyChroot(chrootFolder, relative, port,
					spec.timeout(), spec.loggerLevel(), spec.engineOwned());
			return WorkerHandle.local((Process) ret[0], (String) ret[1], spec.port(), serverDirectoryPath.toString());
		}
		case JAVA: {
			Process process = ClientProcessWrapper.startTCPServerChroot(chrootFolder, relative, port);
			return WorkerHandle.local(process, null, spec.port(), serverDirectoryPath.toString());
		}
		default:
			throw new IllegalArgumentException("The " + spec.kind() + " worker has no fakechroot jail");
		}
	}

	@Override
	public ExecResult exec(String workingDir, String command, int timeoutSeconds) {
		return ShellExecutor.runInChroot(helper.getUserChrootFolder(), workingDir, command, timeoutSeconds);
	}
}
