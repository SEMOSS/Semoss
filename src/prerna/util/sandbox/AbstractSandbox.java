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
 * Grants are the same call on every backend: the user's {@link SymlinkHelper} is the ledger and does
 * the backend-specific part (symlink, namespace mount, OpenShell upload), so this only forwards.
 */
abstract class AbstractSandbox implements ISandbox {

	/** The user's helper, or null for the host. */
	protected final SymlinkHelper helper;

	protected AbstractSandbox(SymlinkHelper helper) {
		this.helper = helper;
	}

	@Override
	public void grantFolder(String absPath) {
		if (helper != null) {
			helper.symlinkFolder(absPath);
		}
	}

	@Override
	public void grantProject(User user, String projectId) {
		if (helper != null) {
			helper.symlinkProject(user, projectId);
		}
	}

	@Override
	public boolean revoke(String absPath) {
		return helper != null && helper.revoke(absPath);
	}
}
