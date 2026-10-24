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

import prerna.util.openshell.OpenShellSecrets;
import prerna.util.openshell.TransportedRoots;

/**
 * {@link SandboxTransporter} for {@link SymlinkHelper.SandboxMode#OPENSHELL}: copies a host path into
 * the sandbox with {@code openshell sandbox upload}.
 *
 * <p>
 * The sandbox has its own filesystem, so unlike the namespace jail -- which runs on this host and is
 * satisfied by binding the real path -- the bytes must actually be transferred. Uploads preserve the
 * absolute path, so a worker that opens an asset by absolute path or inserts it into {@code sys.path}
 * finds it where it expects.
 */
public class OpenShellTransporter implements SandboxTransporter {

	private static final org.apache.logging.log4j.Logger classLogger = org.apache.logging.log4j.LogManager
			.getLogger(OpenShellTransporter.class);

	private final OpenShellTransport transport;

	public OpenShellTransporter(OpenShellTransport transport) {
		this.transport = transport;
	}

	@Override
	public boolean transport(String absPath) {
		if (this.transport == null) {
			classLogger.warn("No OpenShell transport; cannot send {}", absPath);
			return false;
		}
		if (absPath == null || absPath.trim().isEmpty()) {
			return false;
		}
		boolean uploaded = this.transport.upload(absPath);
		if (uploaded) {
			// A project root arriving in the sandbox is when its declared secrets are attached:
			// only users allowed to load the project get here.
			String normalized = Utility.normalizePath(absPath);
			String projectId = TransportedRoots.projectIdOf(normalized);
			if (projectId != null && normalized.matches(".*/" + Constants.PROJECT_FOLDER + "/[^/]+/app_root/?$")) {
				OpenShellSecrets.ensureForProject(this.transport, projectId);
			}
		}
		return uploaded;
	}

	@Override
	public boolean close() {
		return true;
	}
}
