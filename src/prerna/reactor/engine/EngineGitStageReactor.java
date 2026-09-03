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
package prerna.reactor.engine;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.Git;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.git.ProjectGitCommonUtils;
import prerna.util.git.ProjectGitStageUtils;
import prerna.util.git.ProjectGitStatusUtils;

/**
 * Stages or unstages a set of repo-relative paths in an engine's git
 * repository and returns the refreshed {@code EngineGitStatus} payload.
 */
public class EngineGitStageReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(EngineGitStageReactor.class);
	private static final String PATHS_KEY = "paths";
	private static final String ACTION_KEY = "action";

	public EngineGitStageReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), PATHS_KEY, ACTION_KEY };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		if (engineId == null || (engineId = engineId.trim()).isEmpty()) {
			throw new SemossPixelException("Must pass in the engine id");
		}

		List<String> paths = getNounAsStringList(PATHS_KEY);
		if (paths.isEmpty()) {
			throw new SemossPixelException("Must pass in at least one path");
		}

		String actionStr = this.keyValue.get(ACTION_KEY);
		if (actionStr == null || (actionStr = actionStr.trim()).isEmpty()) {
			throw new SemossPixelException("Must pass in the action");
		}
		boolean stage;
		if ("STAGE".equalsIgnoreCase(actionStr)) {
			stage = true;
		} else if ("UNSTAGE".equalsIgnoreCase(actionStr)) {
			stage = false;
		} else {
			throw new SemossPixelException("Action must be STAGE or UNSTAGE");
		}

		if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
			throw new SemossPixelException("Engine does not exist or user does not have access to the engine");
		}

		IEngine engine = Utility.getEngine(engineId);
		String versionFolder = EngineUtility.getSpecificEngineVersionFolder(engine.getCatalogType(), engineId,
				engine.getEngineName());
		File gitDir = new File(versionFolder, ".git");
		if (!gitDir.exists()) {
			throw new SemossPixelException("Engine does not have a git repository yet");
		}

		Map<String, Object> statusMap;
		try (Git thisGit = Git.open(new File(versionFolder))) {
			List<String> safePaths = ProjectGitCommonUtils.validateRepoRelativePaths(thisGit.getRepository(), paths);
			if (stage) {
				ProjectGitStageUtils.stage(thisGit.getRepository(), safePaths);
			} else {
				ProjectGitStageUtils.unstage(thisGit.getRepository(), safePaths);
			}

			if (ClusterUtil.IS_CLUSTER) {
				ClusterUtil.pushEngineFolder(engine, versionFolder);
			}

			statusMap = EngineGitReactorUtils.buildStatusMap(engineId,
					ProjectGitStatusUtils.computeStatus(thisGit.getRepository()));
		} catch (IllegalArgumentException e) {
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Error occurred staging files for engine {}", engineId, e);
			throw new SemossPixelException("Error occurred updating the stage. Detailed error = " + e.getMessage(),
					e);
		}

		return new NounMetadata(statusMap, PixelDataType.MAP, PixelOperationType.ENGINE_INFO);
	}

	@Override
	public String getReactorDescription() {
		return "This reactor stages or unstages files in an engine's git repository and returns the refreshed status";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The engine id";
		} else if (key.equals(PATHS_KEY)) {
			return "The repo-relative file paths to stage or unstage";
		} else if (key.equals(ACTION_KEY)) {
			return "STAGE or UNSTAGE";
		}
		return super.getDescriptionForKey(key);
	}
}
