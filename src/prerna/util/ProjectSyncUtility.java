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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.project.api.IProject;

public class ProjectSyncUtility {

	private static final Logger classLogger = LogManager.getLogger(ProjectSyncUtility.class);

	private static ConcurrentMap<String, ReentrantLock> prorjectLocks = new ConcurrentHashMap<>();

	public static ReentrantLock getProjectLock(String projectId) {
		prorjectLocks.putIfAbsent(projectId, new ReentrantLock());
		return prorjectLocks.get(projectId);
	}

	public static ConcurrentMap<String, ReentrantLock> getAllLocks(User user) {
		if (!SecurityAdminUtils.userIsAdmin(user)) {
			throw new IllegalArgumentException("User must be an admin to perform this method");
		}
		return prorjectLocks;
	}

	/**
	 * Checks if a project is open on this node, meaning an instance of it is held
	 * in DIHelper. A project that is only cataloged here, with its smss known but
	 * never opened, is not open.
	 *
	 * @param projectId the project to check
	 * @return true if the project is open on this node
	 */
	public static boolean isProjectOpen(String projectId) {
		return DIHelper.getInstance().getProjectProperty(projectId) instanceof IProject;
	}

	/**
	 * Clears the project modules cached in the python process of a project that is
	 * open on this node, through {@link IProject#clearPythonImports()}. A project
	 * that is not open on this node has nothing cached and is left alone, without
	 * being loaded.
	 *
	 * @param projectId the project whose python imports to clear
	 * @return the result of {@code smss_clear_app_imports()}, or null when the
	 *         project or its python process is not running on this node
	 */
	public static Object clearLoadedProjectPythonImports(String projectId) {
		Object loaded = DIHelper.getInstance().getProjectProperty(projectId);
		if (!(loaded instanceof IProject openProject)) {
			classLogger.info("Project {} is not open on this node, no python imports to clear",
					Utility.cleanLogString(projectId));
			return null;
		}
		Object cleared = openProject.clearPythonImports();
		if (cleared == null) {
			classLogger.info("Project {} python process is not running on this node, no python imports to clear",
					Utility.cleanLogString(projectId));
		} else {
			classLogger.info("Cleared python imports for project {}: {}", Utility.cleanLogString(projectId), cleared);
		}
		return cleared;
	}

}
