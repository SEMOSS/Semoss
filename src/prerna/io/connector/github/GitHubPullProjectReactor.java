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

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.PullResult;
import org.eclipse.jgit.transport.CredentialsProvider;
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
 * Pulls the latest changes from the GitHub repository linked to a project,
 * authenticating as the configured GitHub App installation.
 * <p>
 * Resolves the project's {@code GITHUB_PROJECT_LINK} row for the installation
 * id and repository, mints a short-lived installation access token via
 * {@link GitHubAppClient}, and fetches + merges the project's branch into its
 * local version folder using that token as the git credential
 * ({@code x-access-token}).
 */
public class GitHubPullProjectReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GitHubPullProjectReactor.class);

	public GitHubPullProjectReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		if (projectId == null || projectId.trim().isEmpty()) {
			throw new SemossPixelException("Project is required.");
		}
		projectId = projectId.trim();

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
			String token = GitHubAppClient.getInstallationToken(installationId);
			CredentialsProvider cp = new UsernamePasswordCredentialsProvider("x-access-token", token);
			PullResult pr = GitRepoUtils.pullFromRemote(localFolder, "origin", remoteUrl, cp);

			Map<String, Object> result = new HashMap<>();
			result.put("project", projectId);
			result.put("repo", repoFullName);
			result.put("success", pr.isSuccessful());
			if (pr.getMergeResult() != null) {
				result.put("mergeStatus", pr.getMergeResult().getMergeStatus().toString());
			} else if (pr.getRebaseResult() != null) {
				result.put("rebaseStatus", pr.getRebaseResult().getStatus().toString());
			}
			return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to pull GitHub changes for project {}", projectId, e);
			throw new SemossPixelException(
					"Failed to pull GitHub changes for project " + projectId + ": " + e.getMessage(), e);
		}
	}

	@Override
	public String getReactorDescription() {
		return "Pull the latest changes from the GitHub repository linked to a project, authenticating as the configured GitHub App installation.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.PROJECT.getKey().equals(key)) {
			return "The id of the project whose linked GitHub repository changes should be pulled";
		}
		return super.getDescriptionForKey(key);
	}

}
