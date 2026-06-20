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

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;

import prerna.auth.utils.SecurityExternalConnectorsUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.util.AssetUtility;
import prerna.util.git.GitRepoUtils;

/**
 * Syncs a project's local git working folder from the GitHub repository it is
 * linked to, authenticating as the configured GitHub App installation.
 * <p>
 * This is the shared orchestration used both by interactive reactors and by the
 * inbound push-webhook handler: it resolves the project's
 * {@code GITHUB_PROJECT_LINK}, mints a short-lived installation access token,
 * and applies the remote state to the project's version folder.
 */
public class GitHubProjectSync {

	private static final Logger classLogger = LogManager.getLogger(GitHubProjectSync.class);

	private GitHubProjectSync() {

	}

	/**
	 * Replaces a project's local git with the latest state of its linked GitHub
	 * repository by hard-resetting the project's checked-out branch to the remote.
	 * <p>
	 * The sync only runs when {@code pushedBranch} matches the branch the project
	 * folder currently has checked out, so a push to an unrelated branch is
	 * ignored. This is destructive on the matched branch: uncommitted or divergent
	 * local changes are discarded (see
	 * {@link GitRepoUtils#resetToRemote(String, String, String, String, CredentialsProvider)}).
	 * <p>
	 * After the local repo is updated, the project is pushed to central cloud
	 * storage via {@link ClusterUtil#pushProject(String)} (a no-op when not running
	 * in cluster/cloud mode) so the cloud copy and other cluster nodes reflect the
	 * pulled changes.
	 *
	 * @param projectId    the Semoss project whose local repo should be synced
	 * @param pushedBranch the branch that was pushed (from the webhook); when
	 *                     {@code null} or empty the project's current branch is
	 *                     synced unconditionally
	 * @return the new {@code HEAD} commit SHA after the reset, or {@code null} if
	 *         the push targeted a different branch than the project has checked out
	 *         (sync skipped)
	 * @throws Exception if the link is missing, a token cannot be minted, or the
	 *                   git operation fails
	 */
	public static String syncProjectFromGitHub(String projectId, String pushedBranch) throws Exception {
		Map<String, Object> link = SecurityExternalConnectorsUtils.getGitHubProjectLink(projectId);
		if (link == null) {
			throw new IllegalArgumentException("Project " + projectId + " is not linked to a GitHub repository");
		}
		Object installObj = link.get("installationId");
		String repoFullName = (String) link.get("repoFullName");
		if (installObj == null || repoFullName == null || repoFullName.trim().isEmpty()) {
			throw new IllegalStateException(
					"GitHub link for project " + projectId + " is missing installation or repository details");
		}
		String installationId = String.valueOf(((Number) installObj).longValue());

		String projectName = SecurityProjectUtils.getProjectAliasForId(projectId);
		String localFolder = AssetUtility.getProjectVersionFolder(projectName, projectId);

		// the branch the project is configured to track (set at link time); fall back
		// to the local checked-out branch for legacy links saved without a branch
		String trackedBranch = (String) link.get("branch");
		if (trackedBranch == null || trackedBranch.trim().isEmpty()) {
			trackedBranch = GitRepoUtils.getCurrentBranch(localFolder);
		}

		// only act when the push targets the branch the project tracks
		if (pushedBranch != null && !pushedBranch.isEmpty() && trackedBranch != null
				&& !pushedBranch.equals(trackedBranch)) {
			classLogger.info("Push to branch {} does not match project {} tracked branch {}; skipping sync",
					pushedBranch, projectId, trackedBranch);
			return null;
		}
		String targetBranch = trackedBranch;

		String token = GitHubAppClient.getInstallationToken(installationId);
		CredentialsProvider cp = new UsernamePasswordCredentialsProvider("x-access-token", token);
		String remoteUrl = "https://github.com/" + repoFullName + ".git";

		String newHead = GitRepoUtils.resetToRemote(localFolder, "origin", remoteUrl, targetBranch, cp);
		classLogger.info("Synced project {} to {} from repo {} branch {}", projectId, newHead, repoFullName,
				targetBranch);

		// propagate the pulled changes to central cloud storage (and, on a ZK cluster,
		// notify other nodes to pull). No-op when not running in cluster/cloud mode.
		ClusterUtil.pushProject(projectId);
		classLogger.info("Pushed project {} to cloud storage after GitHub sync", projectId);

		return newHead;
	}

}
