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
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;

import prerna.util.git.ProjectGitCommonUtils.GitHeadState;

/**
 * Computes the {@code git status} equivalent used by
 * {@code ProjectGitStatusReactor} and reused by every mutating project-git
 * reactor to build their embedded "refreshed status" response field.
 */
public final class ProjectGitStatusUtils {

	private static final Comparator<GitFileStatus> BY_PATH = Comparator.comparing(f -> f.path);

	private ProjectGitStatusUtils() {
	}

	public static final class GitFileStatus {
		public final String path;
		public final String status;

		public GitFileStatus(String path, String status) {
			this.path = path;
			this.status = status;
		}
	}

	public static final class GitCommitInfo {
		public final String commitId;
		public final String message;
		public final String author;
		public final String authorEmail;
		public final String date;

		public GitCommitInfo(String commitId, String message, String author, String authorEmail, String date) {
			this.commitId = commitId;
			this.message = message;
			this.author = author;
			this.authorEmail = authorEmail;
			this.date = date;
		}
	}

	public static final class GitStatusResult {
		public final String branch;
		public final boolean detached;
		public final String headCommitId;
		public final GitCommitInfo lastCommit;
		public final List<GitFileStatus> staged;
		public final List<GitFileStatus> unstaged;
		public final List<GitFileStatus> untracked;
		public final List<GitFileStatus> conflicted;
		public final boolean clean;

		public GitStatusResult(String branch, boolean detached, String headCommitId, GitCommitInfo lastCommit,
				List<GitFileStatus> staged, List<GitFileStatus> unstaged, List<GitFileStatus> untracked,
				List<GitFileStatus> conflicted) {
			this.branch = branch;
			this.detached = detached;
			this.headCommitId = headCommitId;
			this.lastCommit = lastCommit;
			this.staged = staged;
			this.unstaged = unstaged;
			this.untracked = untracked;
			this.conflicted = conflicted;
			this.clean = staged.isEmpty() && unstaged.isEmpty() && untracked.isEmpty() && conflicted.isEmpty();
		}
	}

	/**
	 * Returns the empty/unborn-repository status shape: no branch, no last
	 * commit, every file list empty, clean.
	 */
	public static GitStatusResult emptyStatus() {
		return new GitStatusResult(null, false, null, null, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(),
				new ArrayList<>());
	}

	public static GitStatusResult computeStatus(Repository repo) throws IOException, GitAPIException {
		GitHeadState headState = ProjectGitCommonUtils.resolveHeadState(repo);

		GitCommitInfo lastCommit = null;
		if (headState.headCommitId != null) {
			try (RevWalk revWalk = new RevWalk(repo)) {
				RevCommit commit = revWalk.parseCommit(ObjectId.fromString(headState.headCommitId));
				String isoDate = DateTimeFormatter.ISO_INSTANT.format(commit.getAuthorIdent().getWhenAsInstant());
				lastCommit = new GitCommitInfo(commit.getName(), commit.getFullMessage(),
						commit.getAuthorIdent().getName(), commit.getAuthorIdent().getEmailAddress(), isoDate);
			}
		}

		Status status = Git.wrap(repo).status().call();
		Set<String> conflicting = status.getConflicting();

		List<GitFileStatus> staged = new ArrayList<>();
		addAllExcluding(staged, status.getAdded(), "ADDED", conflicting);
		addAllExcluding(staged, status.getChanged(), "MODIFIED", conflicting);
		addAllExcluding(staged, status.getRemoved(), "DELETED", conflicting);

		List<GitFileStatus> unstaged = new ArrayList<>();
		addAllExcluding(unstaged, status.getModified(), "MODIFIED", conflicting);
		addAllExcluding(unstaged, status.getMissing(), "DELETED", conflicting);

		List<GitFileStatus> untracked = new ArrayList<>();
		for (String path : status.getUntracked()) {
			untracked.add(new GitFileStatus(path, "UNTRACKED"));
		}

		List<GitFileStatus> conflicted = new ArrayList<>();
		for (String path : conflicting) {
			conflicted.add(new GitFileStatus(path, "CONFLICTED"));
		}

		staged.sort(BY_PATH);
		unstaged.sort(BY_PATH);
		untracked.sort(BY_PATH);
		conflicted.sort(BY_PATH);

		return new GitStatusResult(headState.branch, headState.detached, headState.headCommitId, lastCommit, staged,
				unstaged, untracked, conflicted);
	}

	private static void addAllExcluding(List<GitFileStatus> target, Set<String> paths, String status,
			Set<String> excluded) {
		for (String path : paths) {
			if (!excluded.contains(path)) {
				target.add(new GitFileStatus(path, status));
			}
		}
	}
}
