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
package prerna.io.connector.github;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.PushResult;
import org.eclipse.jgit.transport.RemoteRefUpdate;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;

import prerna.auth.utils.SecurityExternalConnectorsUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.git.GitRepoUtils;

/**
 * Commits a project's pending changes and pushes them to its linked GitHub
 * repository, authenticating as the configured GitHub App installation.
 * <p>
 * Resolves the project's {@code GITHUB_PROJECT_LINK} row for the installation
 * id and repository, stages + commits any pending changes in the project's
 * local version folder (skipped when the tree is clean), mints a short-lived
 * installation access token via {@link GitHubAppClient}, and pushes the current
 * branch using that token as the git credential ({@code x-access-token}).
 */
public class GitHubPushProjectReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GitHubPushProjectReactor.class);

	public GitHubPushProjectReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.COMMENT_KEY.getKey() };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		if (projectId == null || projectId.trim().isEmpty()) {
			throw new SemossPixelException("Project is required.");
		}
		projectId = projectId.trim();
		String message = this.keyValue.get(ReactorKeysEnum.COMMENT_KEY.getKey());

		Map<String, Object> link = SecurityExternalConnectorsUtils.getGitHubProjectLink(projectId);
		if (link == null) {
			throw new SemossPixelException("Project " + projectId + " is not linked to a GitHub repository.");
		}
		Object installObj = link.get("installationId");
		String repoFullName = (String) link.get("repoFullName");
		if (installObj == null || repoFullName == null || repoFullName.trim().isEmpty()) {
			throw new SemossPixelException(
					"GitHub link for project " + projectId + " is missing installation or repository details.");
		}
		String installationId = String.valueOf(((Number) installObj).longValue());

		String projectName = SecurityProjectUtils.getProjectAliasForId(projectId);
		String localFolder = AssetUtility.getProjectVersionFolder(projectName, projectId);
		String remoteUrl = "https://github.com/" + repoFullName + ".git";

		try {
			// stage + commit any pending changes (no-op when the tree is clean)
			GitRepoUtils.addAllChangesAndCommit(localFolder, false, message);

			String token = GitHubAppClient.getInstallationToken(installationId);
			CredentialsProvider cp = new UsernamePasswordCredentialsProvider("x-access-token", token);
			Iterable<PushResult> pushResults = GitRepoUtils.pushToRemote(localFolder, "origin", remoteUrl, cp);

			boolean success = true;
			List<String> updates = new ArrayList<>();
			for (PushResult pushResult : pushResults) {
				for (RemoteRefUpdate update : pushResult.getRemoteUpdates()) {
					RemoteRefUpdate.Status status = update.getStatus();
					updates.add(update.getRemoteName() + ": " + status);
					if (status != RemoteRefUpdate.Status.OK && status != RemoteRefUpdate.Status.UP_TO_DATE) {
						success = false;
					}
				}
			}

			Map<String, Object> result = new HashMap<>();
			result.put("project", projectId);
			result.put("repo", repoFullName);
			result.put("success", success);
			result.put("updates", updates);
			return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to push GitHub changes for project {}", projectId, e);
			throw new SemossPixelException(
					"Failed to push GitHub changes for project " + projectId + ": " + e.getMessage(), e);
		}
	}

	@Override
	public String getReactorDescription() {
		return "Commit a project's pending changes and push them to its linked GitHub repository, authenticating as the configured GitHub App installation.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.PROJECT.getKey().equals(key)) {
			return "The id of the project whose changes should be committed and pushed to its linked GitHub repository";
		} else if (ReactorKeysEnum.COMMENT_KEY.getKey().equals(key)) {
			return "Optional commit message for the pending changes (defaults to a timestamped message)";
		}
		return super.getDescriptionForKey(key);
	}

}
