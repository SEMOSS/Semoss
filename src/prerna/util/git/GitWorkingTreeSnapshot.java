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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.eclipse.jgit.api.Git;

import prerna.util.git.GitStatusUtils.GitFileStatus;
import prerna.util.git.GitStatusUtils.GitStatusResult;

/**
 * Immutable signature of the staged, unstaged, and untracked state across one
 * or more related Git worktrees.
 */
public final class GitWorkingTreeSnapshot {

	private final String fingerprint;
	private final List<String> changedFiles;

	private GitWorkingTreeSnapshot(String fingerprint, List<String> changedFiles) {
		this.fingerprint = fingerprint;
		this.changedFiles = Collections.unmodifiableList(changedFiles);
	}

	/**
	 * Captures repository status using {@link GitStatusUtils}. File content is
	 * streamed into the digest so large files are never loaded wholly into memory.
	 */
	public static GitWorkingTreeSnapshot capture(String rootGitFolder, List<String> gitFolders) throws Exception {
		MessageDigest digest = MessageDigest.getInstance("SHA-256");
		Path root = new File(rootGitFolder).toPath().toAbsolutePath().normalize();
		List<String> changedFiles = new ArrayList<>();

		for (String gitFolder : gitFolders) {
			Path repositoryPath = new File(gitFolder).toPath().toAbsolutePath().normalize();
			String prefix = root.equals(repositoryPath) ? "" : root.relativize(repositoryPath).toString().replace('\\', '/') + "/";
			try (Git git = Git.open(repositoryPath.toFile())) {
				GitStatusResult status = GitStatusUtils.computeStatus(git.getRepository());
				captureStatuses(digest, repositoryPath, prefix, status.staged, changedFiles);
				captureStatuses(digest, repositoryPath, prefix, status.unstaged, changedFiles);
				captureStatuses(digest, repositoryPath, prefix, status.untracked, changedFiles);
				captureStatuses(digest, repositoryPath, prefix, status.conflicted, changedFiles);
			}
		}

		Collections.sort(changedFiles);
		return new GitWorkingTreeSnapshot(java.util.HexFormat.of().formatHex(digest.digest()), changedFiles);
	}

	private static void captureStatuses(MessageDigest digest, Path repositoryPath, String prefix,
			List<GitFileStatus> statuses, List<String> changedFiles) throws Exception {
		for (GitFileStatus status : statuses) {
			String displayPath = prefix + status.path;
			changedFiles.add(status.status + " " + displayPath);
			digest.update(status.status.getBytes(StandardCharsets.UTF_8));
			digest.update(displayPath.getBytes(StandardCharsets.UTF_8));

			Path file = repositoryPath.resolve(status.path).normalize();
			if (Files.isSymbolicLink(file)) {
				digest.update(Files.readSymbolicLink(file).toString().getBytes(StandardCharsets.UTF_8));
			} else if (Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) {
				try (InputStream in = new BufferedInputStream(Files.newInputStream(file))) {
					byte[] buffer = new byte[8192];
					int read;
					while ((read = in.read(buffer)) != -1) {
						digest.update(buffer, 0, read);
					}
				}
			}
		}
	}

	public List<String> getChangedFiles() {
		return changedFiles;
	}

	public boolean hasChanges() {
		return !changedFiles.isEmpty();
	}

	@Override
	public boolean equals(Object other) {
		return other instanceof GitWorkingTreeSnapshot
				&& fingerprint.equals(((GitWorkingTreeSnapshot) other).fingerprint);
	}

	@Override
	public int hashCode() {
		return fingerprint.hashCode();
	}
}
