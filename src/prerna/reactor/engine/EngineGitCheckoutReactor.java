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
import org.eclipse.jgit.api.CreateBranchCommand.SetupUpstreamMode;
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
import prerna.util.git.ProjectGitBranchUtils.GitCheckoutTarget;
import prerna.util.git.ProjectGitCommonUtils;
import prerna.util.git.ProjectGitStatusUtils;

/**
 * Switches an existing local branch, or creates a local tracking branch when
 * a unique remote branch is selected. Never forces the checkout and never
 * auto-stashes; refuses when local changes or conflicts would be overwritten.
 */
public class EngineGitCheckoutReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(EngineGitCheckoutReactor.class);
	private static final String BRANCH_KEY = "branch";

	public EngineGitCheckoutReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), BRANCH_KEY };
		this.keyRequired = new int[] { 1, 1 };
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
		try (Git thisGit = Git.open(new File(versionFolder))) {
			Repository repo = thisGit.getRepository();

			if (ProjectGitCommonUtils.resolveHeadState(repo).headCommitId == null) {
				throw new SemossPixelException("Cannot checkout a branch in a repository with no commits");
			}

			GitCheckoutTarget target = ProjectGitBranchUtils.resolveCheckoutTarget(repo, branchName);
			if (!target.ambiguousRemotes.isEmpty()) {
				throw new SemossPixelException("Branch '" + branchName + "' exists on multiple remotes ("
						+ String.join(", ", target.ambiguousRemotes) + "); qualify the branch name to disambiguate");
			}
			if (!target.existsLocally && target.uniqueRemoteRef == null) {
				throw new SemossPixelException("Branch '" + branchName + "' was not found locally or on any remote");
			}

			String refToResolve = target.existsLocally ? Constants.R_HEADS + branchName : target.uniqueRemoteRef;
			ObjectId targetCommitId = repo.resolve(refToResolve);
			if (targetCommitId != null) {
				GitRepoUtils.assertNoSymlinks(repo, targetCommitId, branchName);
			}

			try {
				if (target.existsLocally) {
					thisGit.checkout().setName(branchName).call();
				} else {
					thisGit.checkout().setCreateBranch(true).setName(branchName)
							.setStartPoint(target.uniqueRemoteRef).setUpstreamMode(SetupUpstreamMode.TRACK).call();
				}
			} catch (CheckoutConflictException e) {
				List<String> conflicts = e.getConflictingPaths();
				Collections.sort(conflicts);
				throw new SemossPixelException("Cannot checkout '" + branchName
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
			classLogger.error("Error occurred checking out branch for engine {}", engineId, e);
			throw new SemossPixelException(
					"Error occurred checking out the branch. Detailed error = " + e.getMessage(), e);
		}

		return new NounMetadata(resultMap, PixelDataType.MAP, PixelOperationType.ENGINE_INFO);
	}

	@Override
	public String getReactorDescription() {
		return "This reactor checks out an existing local or remote-tracking branch in an engine's git repository";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The engine id";
		} else if (key.equals(BRANCH_KEY)) {
			return "The name of the branch to check out";
		}
		return super.getDescriptionForKey(key);
	}
}
