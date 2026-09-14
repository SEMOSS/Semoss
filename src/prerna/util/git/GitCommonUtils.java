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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.eclipse.jgit.diff.RawText;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Repository;

/**
 * Shared, plain-Java-testable primitives used by the git working-tree utilities
 * ({@link GitStatusUtils}, {@link GitStageUtils}, {@link GitDiffUtils},
 * {@link GitConflictUtils}, {@link GitBranchUtils}): HEAD/detached-state
 * resolution, repo-relative path validation, binary content detection, and the
 * diff line-count truncation convention. These methods take only
 * {@link Repository}/plain Java types and throw plain exceptions so they can be
 * unit tested against a temporary JGit repository without any
 * pixel/reactor/security dependencies.
 */
public final class GitCommonUtils {

	/**
	 * Matches {@code ProjectCommitDiffReactor}'s existing truncation convention.
	 */
	public static final int MAX_DIFF_LINES = 10000;

	private GitCommonUtils() {

	}

	public static final class GitHeadState {
		public final String branch;
		public final boolean detached;
		public final String headCommitId;

		public GitHeadState(String branch, boolean detached, String headCommitId) {
			this.branch = branch;
			this.detached = detached;
			this.headCommitId = headCommitId;
		}
	}

	public static final class TruncatedText {
		public final String text;
		public final boolean truncated;

		public TruncatedText(String text, boolean truncated) {
			this.text = text;
			this.truncated = truncated;
		}
	}

	/**
	 * Resolves the current HEAD state of the repository. Safe for an unborn
	 * repository (no commits yet, {@code headCommitId} and {@code branch} are both
	 * {@code null}) and for a detached HEAD ({@code branch} is {@code null},
	 * {@code detached} is {@code true}).
	 */
	public static GitHeadState resolveHeadState(Repository repo) throws IOException {
		ObjectId head = repo.resolve(Constants.HEAD);
		if (head == null) {
			return new GitHeadState(null, false, null);
		}
		String fullBranch = repo.getFullBranch();
		boolean detached = fullBranch == null || !fullBranch.startsWith(Constants.R_HEADS);
		String branch = detached ? null : Repository.shortenRefName(fullBranch);
		return new GitHeadState(branch, detached, head.getName());
	}

	/**
	 * Validates that {@code suppliedPath} refers to a location inside the
	 * repository's working tree and returns it normalized to a forward-slash,
	 * repo-relative path safe to use as a JGit file pattern or in an error message
	 * (never an absolute filesystem path).
	 *
	 * @throws IllegalArgumentException if the path is blank, absolute, escapes the
	 *                                  working tree (via {@code ..} traversal or a
	 *                                  symlinked ancestor), or references the
	 *                                  {@code .git} directory
	 */
	public static String validateRepoRelativePath(Repository repo, String suppliedPath) {
		if (suppliedPath == null || suppliedPath.trim().isEmpty()) {
			throw new IllegalArgumentException("A file path must be provided");
		}

		String normalized = suppliedPath.trim().replace('\\', '/');
		while (normalized.contains("//")) {
			normalized = normalized.replace("//", "/");
		}
		if (normalized.startsWith("/") || normalized.matches("^[A-Za-z]:.*") || normalized.contains("://")) {
			throw new IllegalArgumentException("Path must be relative to the repository root: " + suppliedPath);
		}
		if (normalized.startsWith("./")) {
			normalized = normalized.substring(2);
		}
		if (normalized.equals("..") || normalized.startsWith("../") || normalized.contains("/../")
				|| normalized.endsWith("/..")) {
			throw new IllegalArgumentException("Path may not traverse outside the repository: " + suppliedPath);
		}
		if (normalized.isEmpty()) {
			throw new IllegalArgumentException("A file path must be provided");
		}

		String firstSegment = normalized.contains("/") ? normalized.substring(0, normalized.indexOf('/')) : normalized;
		if (firstSegment.equalsIgnoreCase(".git")) {
			throw new IllegalArgumentException("Path may not reference the .git directory: " + suppliedPath);
		}

		File workTree = repo.getWorkTree();
		try {
			String canonicalRoot = workTree.getCanonicalPath();
			String canonicalTarget = new File(workTree, normalized).getCanonicalPath();
			if (!canonicalTarget.equals(canonicalRoot) && !canonicalTarget.startsWith(canonicalRoot + File.separator)) {
				throw new IllegalArgumentException("Path escapes the repository working tree: " + suppliedPath);
			}
		} catch (IOException e) {
			throw new IllegalArgumentException("Unable to validate path: " + suppliedPath, e);
		}

		return normalized;
	}

	/** Batch form of {@link #validateRepoRelativePath(Repository, String)}. */
	public static List<String> validateRepoRelativePaths(Repository repo, List<String> suppliedPaths) {
		if (suppliedPaths == null || suppliedPaths.isEmpty()) {
			throw new IllegalArgumentException("At least one path must be provided");
		}
		List<String> validated = new ArrayList<>();
		for (String path : suppliedPaths) {
			validated.add(validateRepoRelativePath(repo, path));
		}
		return validated;
	}

	/**
	 * Backed by JGit's own binary-content heuristic,
	 * {@link RawText#isBinary(byte[])}.
	 */
	public static boolean isBinaryContent(byte[] content) {
		if (content == null) {
			return false;
		}
		return RawText.isBinary(content);
	}

	/**
	 * Applies the {@link #MAX_DIFF_LINES} truncation convention to {@code text}.
	 */
	public static TruncatedText truncate(String text) {
		if (text == null || text.isEmpty()) {
			return new TruncatedText(text == null ? "" : text, false);
		}
		long lineCount = text.lines().count();
		if (lineCount <= MAX_DIFF_LINES) {
			return new TruncatedText(text, false);
		}
		String clipped = text.lines().limit(MAX_DIFF_LINES).collect(Collectors.joining("\n"));
		return new TruncatedText(clipped, true);
	}
}
