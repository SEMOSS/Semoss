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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ListBranchCommand.ListMode;
import org.eclipse.jgit.api.errors.CheckoutConflictException;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.BranchConfig;
import org.eclipse.jgit.lib.BranchTrackingStatus;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;

import prerna.util.git.GitCommonUtils.GitHeadState;

/**
 * Lists local/remote branches with tracking status and performs create+checkout
 * for the {@code *GitBranchesReactor} and {@code *GitCreateBranchReactor}
 * project and engine reactors. The {@code *GitCheckoutReactor} pair performs
 * its own checkout call (see {@link #resolveCheckoutTarget}) since checkout of
 * an existing branch is not a create+checkout operation.
 */
public final class GitBranchUtils {

	private GitBranchUtils() {

	}

	public static final class GitBranchInfo {
		public final String name;
		public final String fullName;
		public final boolean remote;
		public final boolean current;
		public final String commitId;
		public final String upstream;
		public final Integer ahead;
		public final Integer behind;

		public GitBranchInfo(String name, String fullName, boolean remote, boolean current, String commitId,
				String upstream, Integer ahead, Integer behind) {
			this.name = name;
			this.fullName = fullName;
			this.remote = remote;
			this.current = current;
			this.commitId = commitId;
			this.upstream = upstream;
			this.ahead = ahead;
			this.behind = behind;
		}
	}

	/** Result of resolving a checkout target name against local/remote refs. */
	public static final class GitCheckoutTarget {
		public final boolean existsLocally;
		public final String uniqueRemoteRef;
		public final List<String> ambiguousRemotes;

		public GitCheckoutTarget(boolean existsLocally, String uniqueRemoteRef, List<String> ambiguousRemotes) {
			this.existsLocally = existsLocally;
			this.uniqueRemoteRef = uniqueRemoteRef;
			this.ambiguousRemotes = ambiguousRemotes;
		}
	}

	public static List<GitBranchInfo> listBranches(Repository repo) throws IOException, GitAPIException {
		GitHeadState headState = GitCommonUtils.resolveHeadState(repo);
		List<Ref> refs = Git.wrap(repo).branchList().setListMode(ListMode.ALL).call();

		List<GitBranchInfo> branches = new ArrayList<>();
		for (Ref ref : refs) {
			String fullName = ref.getName();
			if (!fullName.startsWith(Constants.R_HEADS) && !fullName.startsWith(Constants.R_REMOTES)) {
				// BranchListCommand includes a synthetic "HEAD" entry when HEAD is
				// detached - not a real branch, skip it.
				continue;
			}
			boolean remote = fullName.startsWith(Constants.R_REMOTES);
			String name = Repository.shortenRefName(fullName);
			String commitId = ref.getObjectId() == null ? null : ref.getObjectId().getName();
			boolean current = !remote && headState.branch != null && name.equals(headState.branch);

			String upstream = null;
			Integer ahead = null;
			Integer behind = null;
			if (!remote) {
				BranchTrackingStatus trackingStatus = BranchTrackingStatus.of(repo, name);
				if (trackingStatus != null) {
					upstream = Repository.shortenRefName(trackingStatus.getRemoteTrackingBranch());
					ahead = trackingStatus.getAheadCount();
					behind = trackingStatus.getBehindCount();
				} else {
					String trackingBranch = new BranchConfig(repo.getConfig(), name).getTrackingBranch();
					if (trackingBranch != null) {
						upstream = Repository.shortenRefName(trackingBranch);
					}
				}
			}

			branches.add(new GitBranchInfo(name, fullName, remote, current, commitId, upstream, ahead, behind));
		}

		branches.sort((a, b) -> a.fullName.compareTo(b.fullName));
		return branches;
	}

	/**
	 * Resolves whether {@code branchName} exists as a local branch, or as exactly
	 * one remote-tracking branch across all configured remotes.
	 */
	public static GitCheckoutTarget resolveCheckoutTarget(Repository repo, String branchName) throws IOException {
		if (repo.findRef(Constants.R_HEADS + branchName) != null) {
			return new GitCheckoutTarget(true, null, Collections.emptyList());
		}

		List<Ref> remoteRefs = repo.getRefDatabase().getRefsByPrefix(Constants.R_REMOTES);
		List<String> matches = new ArrayList<>();
		List<String> remoteNames = new ArrayList<>();
		for (Ref ref : remoteRefs) {
			String withoutPrefix = ref.getName().substring(Constants.R_REMOTES.length());
			int slash = withoutPrefix.indexOf('/');
			if (slash < 0) {
				continue;
			}
			String remoteName = withoutPrefix.substring(0, slash);
			String branchPart = withoutPrefix.substring(slash + 1);
			if (branchPart.equals(branchName)) {
				matches.add(ref.getName());
				remoteNames.add(remoteName);
			}
		}

		if (matches.isEmpty()) {
			return new GitCheckoutTarget(false, null, Collections.emptyList());
		}
		if (matches.size() == 1) {
			return new GitCheckoutTarget(false, matches.get(0), Collections.emptyList());
		}
		return new GitCheckoutTarget(false, null, remoteNames);
	}

	/**
	 * Creates {@code branchName} at {@code startPoint} and checks it out. If the
	 * checkout fails because it would overwrite local changes, the just-created
	 * branch ref is rolled back so the operation is atomic from the caller's
	 * perspective.
	 */
	public static void createAndCheckoutBranch(Git git, String branchName, String startPoint) throws GitAPIException {
		git.branchCreate().setName(branchName).setStartPoint(startPoint).call();
		try {
			git.checkout().setName(branchName).call();
		} catch (CheckoutConflictException e) {
			git.branchDelete().setBranchNames(branchName).setForce(true).call();
			throw e;
		}
	}

	/**
	 * Legal git ref name under {@code refs/heads/}, and not the reserved name
	 * {@code HEAD}. Also rejects a leading {@code -}: {@code check-ref-format}
	 * itself allows this, but real {@code git branch} refuses it at the porcelain
	 * level to avoid ambiguity with command-line flags, and this API accepts
	 * free-form user-typed names the same way {@code git branch} does.
	 */
	public static boolean isValidBranchName(String branchName) {
		if (branchName == null || branchName.trim().isEmpty()) {
			return false;
		}
		if (branchName.equalsIgnoreCase("HEAD")) {
			return false;
		}
		if (branchName.startsWith("-")) {
			return false;
		}
		return Repository.isValidRefName(Constants.R_HEADS + branchName);
	}
}
