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

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.Git;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.git.ProjectGitCommonUtils;
import prerna.util.git.ProjectGitConflictUtils;
import prerna.util.git.ProjectGitConflictUtils.GitConflictResolveResult;
import prerna.util.git.ProjectGitConflictUtils.ResolutionMode;
import prerna.util.git.ProjectGitStatusUtils;

/**
 * Resolves a merge conflict for a single file in a project's git repository.
 * Stages the resolved content but never commits.
 */
public class ProjectGitResolveConflictReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ProjectGitResolveConflictReactor.class);
	private static final String RESOLUTION_KEY = "resolution";

	public ProjectGitResolveConflictReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.FILE_PATH.getKey(),
				RESOLUTION_KEY, ReactorKeysEnum.CONTENT.getKey() };
		this.keyRequired = new int[] { 1, 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
			throw new SemossPixelException("Must pass in the project id");
		}
		String filePath = this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
		if (filePath == null || (filePath = filePath.trim()).isEmpty()) {
			throw new SemossPixelException("Must pass in the file path");
		}
		String resolutionStr = this.keyValue.get(RESOLUTION_KEY);
		if (resolutionStr == null || (resolutionStr = resolutionStr.trim()).isEmpty()) {
			throw new SemossPixelException("Must pass in the resolution");
		}
		ResolutionMode mode;
		try {
			mode = ResolutionMode.valueOf(resolutionStr.toUpperCase());
		} catch (IllegalArgumentException e) {
			throw new SemossPixelException("Resolution must be OURS, THEIRS, BOTH, or MANUAL");
		}
		String content = this.keyValue.get(ReactorKeysEnum.CONTENT.getKey());
		if (mode == ResolutionMode.MANUAL && (content == null || content.isEmpty())) {
			throw new SemossPixelException("Manual conflict resolution requires content");
		}

		if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
			throw new SemossPixelException("Project does not exist or user does not have access to the project");
		}

		IProject project = Utility.getProject(projectId);
		String versionFolder = EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.PROJECT, projectId,
				project.getEngineName());
		File gitDir = new File(versionFolder, ".git");
		if (!gitDir.exists()) {
			throw new SemossPixelException("Project does not have a git repository yet");
		}

		Map<String, Object> resultMap = new LinkedHashMap<>();
		try (Git thisGit = Git.open(new File(versionFolder))) {
			String safePath = ProjectGitCommonUtils.validateRepoRelativePath(thisGit.getRepository(), filePath);

			GitConflictResolveResult resolveResult = ProjectGitConflictUtils.resolveConflict(thisGit, safePath, mode,
					content);

			if (ClusterUtil.IS_CLUSTER) {
				ClusterUtil.pushProjectFolder(project, versionFolder);
			}

			resultMap.put("path", safePath);
			resultMap.put("resolved", resolveResult.resolved);
			resultMap.put("remainingConflicts", resolveResult.remainingConflicts);
			resultMap.put("status", ProjectGitReactorUtils.buildStatusMap(projectId,
					ProjectGitStatusUtils.computeStatus(thisGit.getRepository())));
		} catch (IllegalArgumentException e) {
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Error occurred resolving conflict for project {}", projectId, e);
			throw new SemossPixelException(
					"Error occurred resolving the conflict. Detailed error = " + e.getMessage(), e);
		}

		return new NounMetadata(resultMap, PixelDataType.MAP, PixelOperationType.PROJECT_INFO);
	}

	@Override
	public String getReactorDescription() {
		return "This reactor resolves a merge conflict for a file in a project's git repository";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The project id";
		} else if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "The repo-relative file path to resolve";
		} else if (key.equals(RESOLUTION_KEY)) {
			return "OURS, THEIRS, BOTH, or MANUAL";
		} else if (key.equals(ReactorKeysEnum.CONTENT.getKey())) {
			return "The manual resolution content, required only when resolution is MANUAL";
		}
		return super.getDescriptionForKey(key);
	}
}
