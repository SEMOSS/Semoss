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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.diff.RenameDetector;
import org.eclipse.jgit.dircache.DirCacheIterator;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.AbstractTreeIterator;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;
import org.eclipse.jgit.treewalk.EmptyTreeIterator;
import org.eclipse.jgit.treewalk.FileTreeIterator;

import prerna.util.git.ProjectGitCommonUtils.TruncatedText;

/**
 * Computes STAGED (HEAD vs index) and UNSTAGED (index vs working tree)
 * single-file diffs for {@code ProjectGitDiffReactor}, reusing the same
 * {@link DiffFormatter} + {@link RenameDetector} + truncation/binary
 * convention already established by {@code ProjectCommitDiffReactor}.
 */
public final class ProjectGitDiffUtils {

	private ProjectGitDiffUtils() {
	}

	public enum DiffSide {
		STAGED, UNSTAGED
	}

	public static final class GitDiffResult {
		public final String path;
		public final String diff;
		public final boolean isBinary;
		public final boolean isTruncated;

		public GitDiffResult(String path, String diff, boolean isBinary, boolean isTruncated) {
			this.path = path;
			this.diff = diff;
			this.isBinary = isBinary;
			this.isTruncated = isTruncated;
		}
	}

	public static GitDiffResult computeFileDiff(Repository repo, String path, DiffSide side) throws IOException {
		AbstractTreeIterator oldTree;
		AbstractTreeIterator newTree;

		if (side == DiffSide.STAGED) {
			oldTree = headTreeIterator(repo);
			newTree = new DirCacheIterator(repo.readDirCache());
		} else {
			oldTree = new DirCacheIterator(repo.readDirCache());
			newTree = new FileTreeIterator(repo);
		}

		try (ByteArrayOutputStream out = new ByteArrayOutputStream();
				DiffFormatter diffFormatter = new DiffFormatter(out)) {
			diffFormatter.setRepository(repo);
			diffFormatter.setDetectRenames(true);

			List<DiffEntry> diffs = diffFormatter.scan(oldTree, newTree);
			RenameDetector renameDetector = new RenameDetector(repo);
			renameDetector.addAll(diffs);
			diffs = renameDetector.compute();

			for (DiffEntry entry : diffs) {
				String entryNewPath = entry.getNewPath();
				String entryOldPath = entry.getOldPath();
				String displayPath = DiffEntry.DEV_NULL.equals(entryNewPath) ? entryOldPath : entryNewPath;
				if (!displayPath.equals(path)) {
					continue;
				}
				return formatEntry(diffFormatter, out, entry, path);
			}
		}

		// no changes for this file on the requested side - a legitimate no-op, not an error
		return new GitDiffResult(path, "", false, false);
	}

	private static GitDiffResult formatEntry(DiffFormatter diffFormatter, ByteArrayOutputStream out, DiffEntry entry,
			String path) {
		boolean isBinary = false;
		String diffText = "";
		try {
			out.reset();
			diffFormatter.format(entry);
			diffText = out.toString("UTF-8");
			if (diffText.contains("Binary files differ")) {
				isBinary = true;
				diffText = "";
			}
		} catch (Exception e) {
			isBinary = true;
			diffText = "";
		}

		boolean isTruncated = false;
		if (!isBinary) {
			TruncatedText truncated = ProjectGitCommonUtils.truncate(diffText);
			diffText = truncated.text;
			isTruncated = truncated.truncated;
		}
		return new GitDiffResult(path, diffText, isBinary, isTruncated);
	}

	private static AbstractTreeIterator headTreeIterator(Repository repo) throws IOException {
		ObjectId headTreeId = repo.resolve("HEAD^{tree}");
		if (headTreeId == null) {
			return new EmptyTreeIterator();
		}
		try (RevWalk revWalk = new RevWalk(repo); ObjectReader reader = repo.newObjectReader()) {
			RevTree tree = revWalk.parseTree(headTreeId);
			CanonicalTreeParser treeParser = new CanonicalTreeParser();
			treeParser.reset(reader, tree.getId());
			return treeParser;
		}
	}
}
