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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.CheckoutConflictException;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Repository;

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
import prerna.util.git.GitRepoUtils;
import prerna.util.git.ProjectGitBranchUtils;
import prerna.util.git.ProjectGitStatusUtils;

/**
 * Creates a new local branch at an optional start point (default
 * {@code HEAD}) and checks it out only after successful creation.
 */
public class EngineGitCreateBranchReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(EngineGitCreateBranchReactor.class);
	private static final String BRANCH_KEY = "branch";
	private static final String START_POINT_KEY = "startPoint";

	public EngineGitCreateBranchReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), BRANCH_KEY, START_POINT_KEY };
		this.keyRequired = new int[] { 1, 1, 0 };
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
		String branch = this.keyValue.get(BRANCH_KEY);
		if (branch == null || (branch = branch.trim()).isEmpty()) {
			throw new SemossPixelException("Must pass in the branch");
		}
		String startPoint = this.keyValue.get(START_POINT_KEY);
		if (startPoint == null || (startPoint = startPoint.trim()).isEmpty()) {
			startPoint = Constants.HEAD;
		}

		if (!ProjectGitBranchUtils.isValidBranchName(branch)) {
			throw new SemossPixelException("'" + branch + "' is not a valid branch name");
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

		Map<String, Object> resultMap = new LinkedHashMap<>();
		final String branchName = branch;
		final String resolvedStartPoint = startPoint;
		try (Git thisGit = Git.open(new File(versionFolder))) {
			Repository repo = thisGit.getRepository();

			if (repo.findRef(Constants.R_HEADS + branchName) != null) {
				throw new SemossPixelException("Branch '" + branchName + "' already exists");
			}

			ObjectId startPointId = repo.resolve(resolvedStartPoint);
			if (startPointId == null) {
				throw new SemossPixelException("Could not resolve start point: " + resolvedStartPoint);
			}
			GitRepoUtils.assertNoSymlinks(repo, startPointId, branchName);

			try {
				ProjectGitBranchUtils.createAndCheckoutBranch(thisGit, branchName, resolvedStartPoint);
			} catch (CheckoutConflictException e) {
				List<String> conflicts = e.getConflictingPaths();
				Collections.sort(conflicts);
				throw new SemossPixelException("Cannot create and checkout '" + branchName
						+ "': local changes would be overwritten in: " + String.join(", ", conflicts)
						+ ". Commit, stage, or discard these changes first.");
			}

			if (ClusterUtil.IS_CLUSTER) {
				ClusterUtil.pushEngineFolder(engine, versionFolder);
			}

			ObjectId headCommitId = repo.resolve(Constants.HEAD);
			resultMap.put("branch", branchName);
			resultMap.put("headCommitId", headCommitId == null ? null : headCommitId.getName());
			resultMap.put("status",
					EngineGitReactorUtils.buildStatusMap(engineId, ProjectGitStatusUtils.computeStatus(repo)));
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error occurred creating branch for engine {}", engineId, e);
			throw new SemossPixelException("Error occurred creating the branch. Detailed error = " + e.getMessage(),
					e);
		}

		return new NounMetadata(resultMap, PixelDataType.MAP, PixelOperationType.ENGINE_INFO);
	}

	@Override
	public String getReactorDescription() {
		return "This reactor creates and checks out a new local branch in an engine's git repository";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The engine id";
		} else if (key.equals(BRANCH_KEY)) {
			return "The name of the new branch";
		} else if (key.equals(START_POINT_KEY)) {
			return "Optional start point for the new branch, defaults to HEAD";
		}
		return super.getDescriptionForKey(key);
	}
}
