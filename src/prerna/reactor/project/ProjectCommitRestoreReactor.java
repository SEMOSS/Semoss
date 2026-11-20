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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ResetCommand.ResetType;
import org.eclipse.jgit.lib.FileMode;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.TreeWalk;

import prerna.auth.AccessToken;
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
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.EngineUtility;
import prerna.util.Utility;

public class ProjectCommitRestoreReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ProjectCommitRestoreReactor.class);
	private static final String COMMIT_ID_KEY = "commitId";
	private static final String ASSETS_PATH = "assets";

	public ProjectCommitRestoreReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), COMMIT_ID_KEY };
		this.keyRequired = new int[] { 1, 1 };
	}

	/**
	 * Restores a nested assets repository to the gitlink recorded by the selected
	 * outer commit. A new nested commit preserves the current branch history while
	 * reproducing the selected commit's file tree. Returns the nested HEAD the
	 * outer restoration commit must record, or {@code null} when assets is not a
	 * gitlink in the selected outer commit.
	 */
	static ObjectId restoreNestedAssetsRepository(Git outerGit, ObjectId targetOuterCommit, File versionFolder,
			String commitId, String author, String email) throws Exception {
		ObjectId targetAssetsCommit;
		RevCommit targetOuter;
		try (RevWalk outerWalk = new RevWalk(outerGit.getRepository())) {
			targetOuter = outerWalk.parseCommit(targetOuterCommit);
		}
		try (TreeWalk assetsEntry = TreeWalk.forPath(outerGit.getRepository(), ASSETS_PATH,
				targetOuter.getTree())) {
			if (assetsEntry == null || !FileMode.GITLINK.equals(assetsEntry.getFileMode(0))) {
				return null;
			}
			targetAssetsCommit = assetsEntry.getObjectId(0);
		}

		File assetsFolder = new File(versionFolder, ASSETS_PATH);
		if (!new File(assetsFolder, ".git").exists()) {
			throw new IllegalArgumentException(
					"Commit " + commitId + " references a nested assets repository that is not available locally");
		}

		try (Git assetsGit = Git.open(assetsFolder);
				RevWalk walk = new RevWalk(assetsGit.getRepository())) {
			ObjectId originalAssetsHead = assetsGit.getRepository().resolve("HEAD");
			if (originalAssetsHead == null) {
				throw new IllegalArgumentException("Nested assets repository has no HEAD commit");
			}
			RevCommit originalCommit = walk.parseCommit(originalAssetsHead);
			RevCommit targetCommit;
			try {
				targetCommit = walk.parseCommit(targetAssetsCommit);
			} catch (Exception e) {
				throw new IllegalArgumentException(
						"Nested assets commit " + targetAssetsCommit.name() + " is not available locally", e);
			}

			// Avoid an empty restoration commit when both commits already describe the
			// same files, while still cleaning the nested working tree.
			if (originalCommit.getTree().getId().equals(targetCommit.getTree().getId())) {
				assetsGit.reset().setMode(ResetType.HARD).setRef(originalAssetsHead.name()).call();
				return originalAssetsHead;
			}

			assetsGit.reset().setMode(ResetType.HARD).setRef(targetAssetsCommit.name()).call();
			assetsGit.reset().setMode(ResetType.SOFT).setRef(originalAssetsHead.name()).call();
			RevCommit restored = assetsGit.commit().setMessage("Reverted to project commit: " + commitId)
					.setAuthor(author, email).call();
			return restored.getId();
		}
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();

		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			classLogger.error("Unauthorized access: you must be logged in to perform this action");
			throwAnonymousUserError();
		}

		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		String commitId = this.keyValue.get(COMMIT_ID_KEY);

		if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must pass in the project id");
		}
		if (commitId == null || (commitId = commitId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must pass in the commit id");
		}

		if (!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access to the project");
		}

		IProject project = Utility.getProject(projectId);
		String versionFolder = EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.PROJECT, projectId,
				project.getEngineName());

		try (Git thisGit = Git.open(new File(versionFolder))) {
			ObjectId commitObjectId = thisGit.getRepository().resolve(commitId);
			if (commitObjectId == null) {
				throw new IllegalArgumentException("Commit id " + commitId + " not found");
			}

			// Save the current HEAD so we can soft-reset back to it
			ObjectId originalHead = thisGit.getRepository().resolve("HEAD");

			AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
			String author = accessToken.getResolvedUsername();
			String email = accessToken.getEmail();
			if (author == null || author.isEmpty()) {
				author = "SEMOSS";
			}
			if (email == null || email.isEmpty()) {
				email = "semoss@semoss.org";
			}

			ObjectId restoredAssetsHead = restoreNestedAssetsRepository(thisGit, commitObjectId,
					new File(versionFolder), commitId, author, email);

			// Step 1: Hard reset to the target commit
			// This sets HEAD, index, AND working tree to the target commit's state
			thisGit.reset().setMode(ResetType.HARD).setRef(commitObjectId.name()).call();

			// Step 2: Soft reset back to the original HEAD
			// This moves HEAD back but keeps index and working tree at the target state
			// Now the index differs from HEAD = ready to commit
			thisGit.reset().setMode(ResetType.SOFT).setRef(originalHead.name()).call();
			if (restoredAssetsHead != null) {
				// The target outer tree contains the historical gitlink. Stage the new
				// nested restoration commit, whose tree has the same historical content.
				thisGit.add().addFilepattern(ASSETS_PATH).call();
			}

			// Step 3: Commit the staged changes (index has target state, HEAD has original)
			thisGit.commit().setMessage("Reverted to commit: " + commitId).setAuthor(author, email).call();

			classLogger.info("Reverted project {} to commit {}", projectId, commitId);
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error reverting project {} to commit {}", projectId, commitId, e);
			throw new IllegalArgumentException("Unable to revert to commit id " + commitId, e);
		}

		if (ClusterUtil.IS_CLUSTER) {
			ClusterUtil.pushProjectFolder(project, versionFolder);
		}

		NounMetadata buildResult = rebuildRestoredApp(project);
		if (buildResult != null && (buildResult.getOpType().contains(PixelOperationType.ERROR)
				|| buildResult.getOpType().contains(PixelOperationType.WARNING))) {
			return getWarning("Project source was restored, but its app could not be rebuilt and published: "
					+ buildResult.getValue());
		}

		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	/**
	 * Generated portal bundles are intentionally not versioned. If this project has
	 * buildable client source, regenerate and publish those bundles after restore.
	 */
	private NounMetadata rebuildRestoredApp(IProject project) {
		File clientFolder = new File(
				AssetUtility.getProjectAssetsFolder(project.getProjectName(), project.getProjectId()), "client");
		if (!clientFolder.isDirectory()) {
			return null;
		}

		classLogger.info("Rebuilding and publishing restored app for project {}", project.getProjectId());
		BuildAndPublishAppReactor buildReactor = new BuildAndPublishAppReactor();
		buildReactor.setInsight(this.insight);
		return buildReactor.buildAndPublish(project);
	}

	@Override
	public String getReactorDescription() {
		return "This reactor reverts to the requested commit id";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "This is a required field containing the project id of a project";
		} else if (key.equals(COMMIT_ID_KEY)) {
			return "This is a required field containing the commit id of a project";
		}
		return super.getDescriptionForKey(key);
	}

}
