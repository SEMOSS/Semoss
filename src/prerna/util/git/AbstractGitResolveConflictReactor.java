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
package prerna.util.git;

import java.util.LinkedHashMap;
import java.util.Map;

import org.eclipse.jgit.api.Git;

import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.git.GitConflictUtils.GitConflictResolveResult;
import prerna.util.git.GitConflictUtils.ResolutionMode;

/**
 * Resolves a merge conflict for a single file in the target's git repository.
 * Stages the resolved content but never commits.
 */
public abstract class AbstractGitResolveConflictReactor extends AbstractGitWorktreeReactor {

	private static final String RESOLUTION_KEY = "resolution";

	private String filePath;
	private ResolutionMode mode;
	private String content;

	protected AbstractGitResolveConflictReactor(GitReactorTarget target) {
		super(target,
				new String[] { ReactorKeysEnum.FILE_PATH.getKey(), RESOLUTION_KEY, ReactorKeysEnum.CONTENT.getKey() },
				new int[] { 1, 1, 0 });
	}

	@Override
	protected boolean requiresEditPermission() {
		return true;
	}

	@Override
	protected boolean surfacesIllegalArgumentAsUserError() {
		return true;
	}

	@Override
	protected void validateOperationInput() {
		String filePathValue = this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
		if (filePathValue == null || (filePathValue = filePathValue.trim()).isEmpty()) {
			throw new SemossPixelException("Must pass in the file path");
		}
		String resolutionStr = this.keyValue.get(RESOLUTION_KEY);
		if (resolutionStr == null || (resolutionStr = resolutionStr.trim()).isEmpty()) {
			throw new SemossPixelException("Must pass in the resolution");
		}
		ResolutionMode resolutionMode;
		try {
			resolutionMode = ResolutionMode.valueOf(resolutionStr.toUpperCase());
		} catch (IllegalArgumentException e) {
			throw new SemossPixelException("Resolution must be OURS, THEIRS, BOTH, or MANUAL");
		}
		String contentValue = this.keyValue.get(ReactorKeysEnum.CONTENT.getKey());
		if (resolutionMode == ResolutionMode.MANUAL && (contentValue == null || contentValue.isEmpty())) {
			throw new SemossPixelException("Manual conflict resolution requires content");
		}

		this.filePath = filePathValue;
		this.mode = resolutionMode;
		this.content = contentValue;
	}

	@Override
	protected Map<String, Object> runGitOperation(Git thisGit, GitTargetHandle handle) throws Exception {
		String safePath = GitCommonUtils.validateRepoRelativePath(thisGit.getRepository(), this.filePath);

		GitConflictResolveResult resolveResult = GitConflictUtils.resolveConflict(thisGit, safePath, this.mode,
				this.content);

		handle.pushToCluster();

		Map<String, Object> resultMap = new LinkedHashMap<>();
		resultMap.put("path", safePath);
		resultMap.put("resolved", resolveResult.resolved);
		resultMap.put("remainingConflicts", resolveResult.remainingConflicts);
		resultMap.put("status",
				GitReactorResponseUtils.buildStatusMap(GitStatusUtils.computeStatus(thisGit.getRepository())));

		return resultMap;
	}

	@Override
	protected String getOperationLogPhrase() {
		return "resolving conflict";
	}

	@Override
	protected String getOperationErrorMessage() {
		return "Error occurred resolving the conflict.";
	}

	@Override
	public String getReactorDescription() {
		return "This reactor resolves a merge conflict for a file in " + this.target.getLabelWithArticle()
				+ "'s git repository";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "The repo-relative file path to resolve";
		} else if (key.equals(RESOLUTION_KEY)) {
			return "OURS, THEIRS, BOTH, or MANUAL";
		} else if (key.equals(ReactorKeysEnum.CONTENT.getKey())) {
			return "The manual resolution content, required only when resolution is MANUAL";
		}
		return super.getDescriptionForKey(key);
	}
}
