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
package prerna.reactor.project;

import java.util.Collection;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.ds.py.PyUtils;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.tcp.client.SocketClient;
import prerna.util.EngineUtility;
import prerna.util.ProjectSyncUtility;
import prerna.util.Utility;

/**
 * Reloads the python files of a project without restarting any python process.
 * Runs {@code smss_clear_app_imports()} with the project asset paths in the
 * project's shared python process and in the calling user's python process,
 * wherever each one is running, so the next import of a project module reads
 * the current .py files. In a cluster the reload is published to the other
 * nodes, which clear the imports in the project python process they have
 * running. The user must be able to edit the project.
 */
public class ReloadProjectReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ReloadProjectReactor.class);

	private static final String MODULES_KEY = "modules";

	public ReloadProjectReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = resolveContextEngineId(this.keyValue.get(this.keysToGet[0]));

		User user = this.insight.getUser();
		if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
			throw new IllegalArgumentException(
					"Project does not exist or user does not have access to edit the project");
		}

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		if (!PyUtils.pyEnabled()) {
			noun.addAdditionalReturn(
					NounMetadata.getWarningNounMessage("Python is not enabled, there are no python imports to reload"));
			return noun;
		}

		// each process is cleared on its own, so a failure in one still clears the
		// other and tells the rest of the cluster
		boolean failed = false;
		Object projectCleared = null;
		try {
			// the project's shared python process on this node
			projectCleared = ProjectSyncUtility.clearLoadedProjectPythonImports(projectId);
		} catch (Exception e) {
			classLogger.error("Failed to clear the python imports of project {} in its python process",
					Utility.cleanLogString(projectId), e);
			failed = true;
			noun.addAdditionalReturn(NounMetadata.getWarningNounMessage(
					"Could not reload the python files in the project python process: " + e.getMessage()));
		}
		Object userCleared = null;
		try {
			// the user's own python process, where the app python runs when the project
			// does not use a dedicated process
			userCleared = clearUserPythonImports(user, projectId);
		} catch (Exception e) {
			classLogger.error("Failed to clear the python imports of project {} in the python process of the user",
					Utility.cleanLogString(projectId), e);
			failed = true;
			noun.addAdditionalReturn(NounMetadata.getWarningNounMessage(
					"Could not reload the python files in your python process: " + e.getMessage()));
		}

		try {
			ClusterUtil.publishProjectReload(projectId);
		} catch (SemossPixelException e) {
			noun.addAdditionalReturn(NounMetadata.getWarningNounMessage(e.getMessage()));
		}

		if (projectCleared != null || userCleared != null) {
			int moduleCount = countClearedModules(projectCleared) + countClearedModules(userCleared);
			noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Reloaded the python files. Cleared "
					+ moduleCount + " cached " + (moduleCount == 1 ? "module" : "modules") + "."));
		} else if (!failed) {
			noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage(
					"No python process has these python files loaded on this server. The current files load on their next run."));
		}
		return noun;
	}

	/**
	 * Clears the project modules cached in the calling user's python process,
	 * passing the project asset paths explicitly so the modules imported from the
	 * project are the ones cleared. A process that is not running is not started.
	 *
	 * @param user      the calling user
	 * @param projectId the project whose modules to clear
	 * @return the result of {@code smss_clear_app_imports()}, or null when the user
	 *         has no python process running
	 */
	private Object clearUserPythonImports(User user, String projectId) {
		if (user == null) {
			return null;
		}
		SocketClient userClient = user.getPythonSocketClient(false);
		if (userClient == null || !userClient.isConnected()) {
			return null;
		}
		String projectName = SecurityProjectUtils.getProjectAliasForId(projectId);
		String assetsDir = EngineUtility
				.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.PROJECT, projectId, projectName).replace('\\', '/');
		Object cleared = this.insight.getPyTranslator().runScriptWithExplicitAssetPaths(this.insight,
				PyUtils.CLEAR_APP_IMPORTS_SCRIPT, assetsDir, new String[] { assetsDir + "/py" });
		classLogger.info("Cleared python imports for project {} in the python process of the user: {}",
				Utility.cleanLogString(projectId), cleared);
		return cleared;
	}

	/**
	 * Counts the module keys a {@code smss_clear_app_imports()} result lists as
	 * removed from {@code sys.modules}.
	 *
	 * @param cleared the result of {@code smss_clear_app_imports()}, may be null
	 * @return the number of modules cleared, or 0 when the result has none
	 */
	private static int countClearedModules(Object cleared) {
		if (cleared instanceof Map) {
			Object modules = ((Map<?, ?>) cleared).get(MODULES_KEY);
			if (modules instanceof Collection) {
				return ((Collection<?>) modules).size();
			}
		}
		return 0;
	}

	@Override
	public String getReactorDescription() {
		return """
				Reload the python files of a project without restarting its python process. Clears the \
				project modules cached in the project python process and in the python process of the user, \
				so the next import reads the current .py files. In a cluster every node also clears the \
				imports in the project python process it has running. The user must be able to edit the project.""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The id of the project to reload. Defaults to the project of the current app context";
		}
		return super.getDescriptionForKey(key);
	}

}
