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

import prerna.util.openshell.OpenShellTransport;

/**
 * A started worker: what the caller needs to connect to it and to stop it.
 *
 * @param process           the local process, or null when the worker lives in an external sandbox
 * @param prefix            the worker's request prefix, or null
 * @param udsPath           the unix socket to connect to, or null to connect to {@code port}
 * @param port              the port to connect to
 * @param serverDirectory   the scratch directory the worker uses
 * @param controlSocketPath the namespace jail's control socket, or null
 * @param ioDir             the namespace jail's io folder, or null
 * @param jailDir           the namespace jail's root folder, or null
 * @param controlDir        the namespace jail's control folder, or null
 * @param openshell         the OpenShell transport, or null outside OpenShell
 * @param userScoped        true when the OpenShell sandbox belongs to the user, not this worker
 * @param reattached        true when an existing sandbox was attached instead of created
 */
public record WorkerHandle(Process process, String prefix, String udsPath, int port, String serverDirectory,
		String controlSocketPath, String ioDir, String jailDir, String controlDir, OpenShellTransport openshell,
		boolean userScoped, boolean reattached) {

	/** A worker that is a plain local process on a port. */
	public static WorkerHandle local(Process process, String prefix, int port, String serverDirectory) {
		return new WorkerHandle(process, prefix, null, port, serverDirectory, null, null, null, null, null, false,
				false);
	}

	/** A worker in a namespace jail, from the launcher's result array. */
	public static WorkerHandle jailed(Object[] launched, int port, String serverDirectory) {
		return new WorkerHandle((Process) launched[0], (String) launched[1], (String) launched[2], port,
				serverDirectory, (String) launched[3], (String) launched[4], (String) launched[5],
				(String) launched[6], null, false, false);
	}
}
