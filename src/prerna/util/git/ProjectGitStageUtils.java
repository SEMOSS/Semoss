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

import org.eclipse.jgit.api.AddCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ResetCommand;
import org.eclipse.jgit.api.RmCommand;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.dircache.DirCache;
import org.eclipse.jgit.lib.Repository;

/**
 * Stages and unstages repo-relative paths for {@code ProjectGitStageReactor}.
 * Callers are responsible for validating paths first, e.g. via
 * {@link ProjectGitCommonUtils#validateRepoRelativePaths(Repository, List)}.
 */
public final class ProjectGitStageUtils {

	private ProjectGitStageUtils() {
	}

	/**
	 * Stages each of {@code repoRelativePaths}. A path that currently exists on
	 * disk is added (covers new/modified files, and the new-path half of a
	 * rename); a path that is missing on disk but tracked in the index is
	 * removed from the index only (covers deletions, and the old-path half of a
	 * rename). Never touches the working tree.
	 *
	 * @throws IllegalArgumentException if a path is neither present on disk nor
	 *                                  tracked in the index
	 */
	public static void stage(Repository repo, List<String> repoRelativePaths) throws IOException, GitAPIException {
		Git git = Git.wrap(repo);
		File workTree = repo.getWorkTree();
		DirCache dirCache = repo.readDirCache();

		List<String> toAdd = new ArrayList<>();
		List<String> toRemove = new ArrayList<>();

		for (String path : repoRelativePaths) {
			File onDisk = new File(workTree, path);
			if (onDisk.exists()) {
				toAdd.add(path);
			} else if (dirCache.findEntry(path) >= 0) {
				toRemove.add(path);
			} else {
				throw new IllegalArgumentException(
						"Path does not correspond to a tracked file or a working-tree change: " + path);
			}
		}

		if (!toAdd.isEmpty()) {
			AddCommand add = git.add();
			for (String path : toAdd) {
				add.addFilepattern(path);
			}
			add.call();
		}
		if (!toRemove.isEmpty()) {
			RmCommand rm = git.rm().setCached(true);
			for (String path : toRemove) {
				rm.addFilepattern(path);
			}
			rm.call();
		}
	}

	/**
	 * Resets only the index entries for {@code repoRelativePaths} back to their
	 * {@code HEAD} content ({@code git reset HEAD -- <path>} semantics), leaving
	 * the working tree and every other index entry untouched.
	 */
	public static void unstage(Repository repo, List<String> repoRelativePaths) throws GitAPIException {
		Git git = Git.wrap(repo);
		ResetCommand reset = git.reset();
		for (String path : repoRelativePaths) {
			reset.addPath(path);
		}
		reset.call();
	}
}
