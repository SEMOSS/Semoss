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
import prerna.util.SymlinkHelper;

/**
 * The one place that turns the configured mode into a sandbox. Callers never branch on
 * {@link SandboxSettings.Mode}.
 */
public final class SandboxFactory {

	private SandboxFactory() {
		// static utility
	}

	/**
	 * The sandbox for a user's helper. A null helper is the host: engine-owned workers and
	 * {@code Project} python pass none and are not jailed.
	 *
	 * @param helper the user's helper, or null
	 * @param user   the user, or null; OpenShell needs it for one-shot commands
	 * @return the sandbox
	 */
	public static ISandbox of(SymlinkHelper helper, User user) {
		if (helper == null) {
			return new HostSandbox();
		}
		switch (helper.getSandboxMode()) {
		case OPENSHELL:
			return new OpenShellSandbox(helper, user);
		case NAMESPACE:
			return new NamespaceSandbox(helper);
		default:
			return new FakechrootSandbox(helper);
		}
	}

	/** No sandbox. */
	public static ISandbox host() {
		return new HostSandbox();
	}

	/**
	 * The sandbox for the node worker, which picks its jail on its own: the namespace jail in
	 * NAMESPACE mode, and the host otherwise (OpenShell and fakechroot have no node backend).
	 */
	public static ISandbox forNodeWorker() {
		return SandboxSettings.nodeNamespaceJail() ? new NamespaceSandbox(null) : new HostSandbox();
	}
}
