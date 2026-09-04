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
import prerna.util.git.GitConflictUtils.GitConflictDiffResult;
import prerna.util.git.GitDiffUtils.DiffSide;
import prerna.util.git.GitDiffUtils.GitDiffResult;

/**
 * Returns the unified diff for a single file on a given side (STAGED, UNSTAGED,
 * or CONFLICT) of the target's git repository.
 */
public abstract class AbstractGitDiffReactor extends AbstractGitWorktreeReactor {

	private static final String SIDE_KEY = "side";

	private String filePath;
	private String side;

	protected AbstractGitDiffReactor(GitReactorTarget target) {
		super(target, new String[] { ReactorKeysEnum.FILE_PATH.getKey(), SIDE_KEY }, new int[] { 1, 1 });
	}

	@Override
	protected boolean requiresEditPermission() {
		return false;
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
		String sideValue = this.keyValue.get(SIDE_KEY);
		if (sideValue == null || (sideValue = sideValue.trim()).isEmpty()) {
			throw new SemossPixelException("Must pass in the side");
		}
		sideValue = sideValue.toUpperCase();
		if (!sideValue.equals("STAGED") && !sideValue.equals("UNSTAGED") && !sideValue.equals("CONFLICT")) {
			throw new SemossPixelException("Side must be STAGED, UNSTAGED, or CONFLICT");
		}

		this.filePath = filePathValue;
		this.side = sideValue;
	}

	@Override
	protected Map<String, Object> runGitOperation(Git thisGit, GitTargetHandle handle) throws Exception {
		String safePath = GitCommonUtils.validateRepoRelativePath(thisGit.getRepository(), this.filePath);

		Map<String, Object> resultMap = new LinkedHashMap<>();
		if (this.side.equals("CONFLICT")) {
			GitConflictDiffResult diffResult = GitConflictUtils.computeConflictDiff(thisGit.getRepository(), safePath);
			resultMap.put("path", diffResult.path);
			resultMap.put("side", this.side);
			resultMap.put("diff", diffResult.diff);
			resultMap.put("isBinary", diffResult.isBinary);
			resultMap.put("isTruncated", diffResult.isTruncated);
			resultMap.put("base", diffResult.base);
			resultMap.put("ours", diffResult.ours);
			resultMap.put("theirs", diffResult.theirs);
			resultMap.put("result", diffResult.result);
		} else {
			GitDiffResult diffResult = GitDiffUtils.computeFileDiff(thisGit.getRepository(), safePath,
					DiffSide.valueOf(this.side));
			resultMap.put("path", diffResult.path);
			resultMap.put("side", this.side);
			resultMap.put("diff", diffResult.diff);
			resultMap.put("isBinary", diffResult.isBinary);
			resultMap.put("isTruncated", diffResult.isTruncated);
		}

		return resultMap;
	}

	@Override
	protected String getOperationLogPhrase() {
		return "getting git diff";
	}

	@Override
	protected String getOperationErrorMessage() {
		return "Error occurred getting the git diff.";
	}

	@Override
	public String getReactorDescription() {
		return "This reactor returns the diff for a file on a given side (STAGED, UNSTAGED, or CONFLICT) in "
				+ this.target.getLabelWithArticle() + "'s git repository";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "The repo-relative file path to diff";
		} else if (key.equals(SIDE_KEY)) {
			return "STAGED, UNSTAGED, or CONFLICT";
		}
		return super.getDescriptionForKey(key);
	}
}
