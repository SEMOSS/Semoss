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

import java.util.List;
import java.util.Map;

import org.eclipse.jgit.api.Git;

import prerna.sablecc2.om.execptions.SemossPixelException;

/**
 * Stages or unstages a set of repo-relative paths in the target's git
 * repository and returns the refreshed status payload.
 */
public abstract class AbstractGitStageReactor extends AbstractGitWorktreeReactor {

	private static final String PATHS_KEY = "paths";
	private static final String ACTION_KEY = "action";

	private List<String> paths;
	private boolean stage;

	protected AbstractGitStageReactor(GitReactorTarget target) {
		super(target, new String[] { PATHS_KEY, ACTION_KEY }, new int[] { 1, 1 });
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
		List<String> pathValues = getNounAsStringList(PATHS_KEY);
		if (pathValues.isEmpty()) {
			throw new SemossPixelException("Must pass in at least one path");
		}

		String actionStr = this.keyValue.get(ACTION_KEY);
		if (actionStr == null || (actionStr = actionStr.trim()).isEmpty()) {
			throw new SemossPixelException("Must pass in the action");
		}
		if ("STAGE".equalsIgnoreCase(actionStr)) {
			this.stage = true;
		} else if ("UNSTAGE".equalsIgnoreCase(actionStr)) {
			this.stage = false;
		} else {
			throw new SemossPixelException("Action must be STAGE or UNSTAGE");
		}

		this.paths = pathValues;
	}

	@Override
	protected Map<String, Object> runGitOperation(Git thisGit, GitTargetHandle handle) throws Exception {
		List<String> safePaths = GitCommonUtils.validateRepoRelativePaths(thisGit.getRepository(), this.paths);
		if (this.stage) {
			GitStageUtils.stage(thisGit.getRepository(), safePaths);
		} else {
			GitStageUtils.unstage(thisGit.getRepository(), safePaths);
		}

		handle.pushToCluster();

		return GitReactorResponseUtils.buildStatusMap(GitStatusUtils.computeStatus(thisGit.getRepository()));
	}

	@Override
	protected String getOperationLogPhrase() {
		return "staging files";
	}

	@Override
	protected String getOperationErrorMessage() {
		return "Error occurred updating the stage.";
	}

	@Override
	public String getReactorDescription() {
		return "This reactor stages or unstages files in " + this.target.getLabelWithArticle()
				+ "'s git repository and returns the refreshed status";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(PATHS_KEY)) {
			return "The repo-relative file paths to stage or unstage";
		} else if (key.equals(ACTION_KEY)) {
			return "STAGE or UNSTAGE";
		}
		return super.getDescriptionForKey(key);
	}
}
