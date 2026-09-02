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

import prerna.auth.utils.SecurityProjectUtils;
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
import prerna.util.git.ProjectGitConflictUtils.GitConflictDiffResult;
import prerna.util.git.ProjectGitDiffUtils;
import prerna.util.git.ProjectGitDiffUtils.DiffSide;
import prerna.util.git.ProjectGitDiffUtils.GitDiffResult;

/**
 * Returns the unified diff for a single file on a given side (STAGED,
 * UNSTAGED, or CONFLICT) of a project's git repository.
 */
public class ProjectGitDiffReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ProjectGitDiffReactor.class);
	private static final String SIDE_KEY = "side";

	public ProjectGitDiffReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.FILE_PATH.getKey(),
				SIDE_KEY };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
			throw new SemossPixelException("Must pass in the project id");
		}
		String filePath = this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
		if (filePath == null || (filePath = filePath.trim()).isEmpty()) {
			throw new SemossPixelException("Must pass in the file path");
		}
		String sideStr = this.keyValue.get(SIDE_KEY);
		if (sideStr == null || (sideStr = sideStr.trim()).isEmpty()) {
			throw new SemossPixelException("Must pass in the side");
		}
		sideStr = sideStr.toUpperCase();
		if (!sideStr.equals("STAGED") && !sideStr.equals("UNSTAGED") && !sideStr.equals("CONFLICT")) {
			throw new SemossPixelException("Side must be STAGED, UNSTAGED, or CONFLICT");
		}

		if (!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
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

			if (sideStr.equals("CONFLICT")) {
				GitConflictDiffResult diffResult = ProjectGitConflictUtils
						.computeConflictDiff(thisGit.getRepository(), safePath);
				resultMap.put("path", diffResult.path);
				resultMap.put("side", sideStr);
				resultMap.put("diff", diffResult.diff);
				resultMap.put("isBinary", diffResult.isBinary);
				resultMap.put("isTruncated", diffResult.isTruncated);
				resultMap.put("base", diffResult.base);
				resultMap.put("ours", diffResult.ours);
				resultMap.put("theirs", diffResult.theirs);
				resultMap.put("result", diffResult.result);
			} else {
				DiffSide side = DiffSide.valueOf(sideStr);
				GitDiffResult diffResult = ProjectGitDiffUtils.computeFileDiff(thisGit.getRepository(), safePath,
						side);
				resultMap.put("path", diffResult.path);
				resultMap.put("side", sideStr);
				resultMap.put("diff", diffResult.diff);
				resultMap.put("isBinary", diffResult.isBinary);
				resultMap.put("isTruncated", diffResult.isTruncated);
			}
		} catch (IllegalArgumentException e) {
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Error occurred getting git diff for project {}", projectId, e);
			throw new SemossPixelException("Error occurred getting the git diff. Detailed error = " + e.getMessage(),
					e);
		}

		return new NounMetadata(resultMap, PixelDataType.MAP, PixelOperationType.PROJECT_INFO);
	}

	@Override
	public String getReactorDescription() {
		return "This reactor returns the diff for a file on a given side (STAGED, UNSTAGED, or CONFLICT) "
				+ "in a project's git repository";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The project id";
		} else if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "The repo-relative file path to diff";
		} else if (key.equals(SIDE_KEY)) {
			return "STAGED, UNSTAGED, or CONFLICT";
		}
		return super.getDescriptionForKey(key);
	}
}
