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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.diff.EditList;
import org.eclipse.jgit.diff.MyersDiff;
import org.eclipse.jgit.diff.RawText;
import org.eclipse.jgit.diff.RawTextComparator;
import org.eclipse.jgit.dircache.DirCache;
import org.eclipse.jgit.dircache.DirCacheEntry;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.lib.Repository;

import prerna.util.git.GitCommonUtils.TruncatedText;

/**
 * Backs the {@code CONFLICT} side of the {@code *GitDiffReactor} project and
 * engine reactors and all of the {@code *GitResolveConflictReactor} pair: reads
 * the base/ours/theirs {@link DirCache} conflict stages for a path and resolves
 * a conflict by writing content to the working tree and staging it. Never
 * commits.
 */
public final class GitConflictUtils {

	private GitConflictUtils() {

	}

	public enum ResolutionMode {
		OURS, THEIRS, BOTH, MANUAL
	}

	public static final class GitConflictStages {
		public final byte[] base;
		public final byte[] ours;
		public final byte[] theirs;

		public GitConflictStages(byte[] base, byte[] ours, byte[] theirs) {
			this.base = base;
			this.ours = ours;
			this.theirs = theirs;
		}
	}

	public static final class GitConflictDiffResult {
		public final String path;
		public final String diff;
		public final String base;
		public final String ours;
		public final String theirs;
		public final String result;
		public final boolean isBinary;
		public final boolean isTruncated;

		public GitConflictDiffResult(String path, String diff, String base, String ours, String theirs, String result,
				boolean isBinary, boolean isTruncated) {
			this.path = path;
			this.diff = diff;
			this.base = base;
			this.ours = ours;
			this.theirs = theirs;
			this.result = result;
			this.isBinary = isBinary;
			this.isTruncated = isTruncated;
		}
	}

	public static final class GitConflictResolveResult {
		public final boolean resolved;
		public final List<String> remainingConflicts;

		public GitConflictResolveResult(boolean resolved, List<String> remainingConflicts) {
			this.resolved = resolved;
			this.remainingConflicts = remainingConflicts;
		}
	}

	public static List<String> listConflictedPaths(Repository repo) throws IOException, GitAPIException {
		Status status = Git.wrap(repo).status().call();
		List<String> paths = new ArrayList<>(status.getConflicting());
		Collections.sort(paths);
		return paths;
	}

	/**
	 * Reads the base (stage 1), ours (stage 2), and theirs (stage 3)
	 * {@link DirCache} entries for {@code path}. Any stage absent from the index
	 * (e.g. a delete/modify conflict) comes back as {@code null} in the
	 * corresponding field.
	 */
	public static GitConflictStages readConflictStages(Repository repo, String path) throws IOException {
		DirCache dirCache = repo.readDirCache();
		int idx = dirCache.findEntry(path);
		if (idx < 0) {
			idx = -(idx + 1);
		}

		byte[] base = null;
		byte[] ours = null;
		byte[] theirs = null;

		try (ObjectReader reader = repo.newObjectReader()) {
			int total = dirCache.getEntryCount();
			for (int i = idx; i < total; i++) {
				DirCacheEntry entry = dirCache.getEntry(i);
				if (!entry.getPathString().equals(path)) {
					break;
				}
				int stage = entry.getStage();
				if (stage == 0) {
					continue;
				}
				ObjectLoader loader = reader.open(entry.getObjectId());
				byte[] bytes = loader.getBytes();
				if (stage == 1) {
					base = bytes;
				} else if (stage == 2) {
					ours = bytes;
				} else if (stage == 3) {
					theirs = bytes;
				}
			}
		}

		return new GitConflictStages(base, ours, theirs);
	}

	public static GitConflictDiffResult computeConflictDiff(Repository repo, String path) throws IOException {
		GitConflictStages stages = readConflictStages(repo, path);

		byte[] resultBytes = null;
		Path target = repo.getWorkTree().toPath().resolve(path);
		if (Files.isRegularFile(target)) {
			resultBytes = Files.readAllBytes(target);
		}

		boolean isBinary = GitCommonUtils.isBinaryContent(stages.base) || GitCommonUtils.isBinaryContent(stages.ours)
				|| GitCommonUtils.isBinaryContent(stages.theirs) || GitCommonUtils.isBinaryContent(resultBytes);

		String base = !isBinary && stages.base != null ? new String(stages.base, StandardCharsets.UTF_8) : null;
		String ours = !isBinary && stages.ours != null ? new String(stages.ours, StandardCharsets.UTF_8) : null;
		String theirs = !isBinary && stages.theirs != null ? new String(stages.theirs, StandardCharsets.UTF_8) : null;
		String result = !isBinary && resultBytes != null ? new String(resultBytes, StandardCharsets.UTF_8) : null;

		String diffText = "";
		boolean isTruncated = false;
		if (!isBinary) {
			diffText = diffOursVsTheirs(stages.ours, stages.theirs);
			TruncatedText truncated = GitCommonUtils.truncate(diffText);
			diffText = truncated.text;
			isTruncated = truncated.truncated;
		}

		return new GitConflictDiffResult(path, diffText, base, ours, theirs, result, isBinary, isTruncated);
	}

	/**
	 * Resolves a conflict at {@code path} per {@code mode}, writing content to the
	 * working tree (or removing the file for a delete outcome) and staging the
	 * result so its DirCache entry collapses back to a single stage-0 entry. Never
	 * commits.
	 *
	 * @throws IllegalArgumentException if the path is not currently conflicted,
	 *                                  MANUAL content is missing/binary, or an
	 *                                  unsupported mode is passed
	 * @throws IOException              if the resolution would write through a
	 *                                  pre-existing symbolic link at the target
	 *                                  path
	 */
	public static GitConflictResolveResult resolveConflict(Git git, String path, ResolutionMode mode,
			String manualContent) throws IOException, GitAPIException {
		Repository repo = git.getRepository();
		GitConflictStages stages = readConflictStages(repo, path);
		if (stages.base == null && stages.ours == null && stages.theirs == null) {
			throw new IllegalArgumentException("Path is not currently conflicted: " + path);
		}

		byte[] resolvedBytes = null;
		boolean deleteFile = false;

		switch (mode) {
		case OURS:
			if (stages.ours == null) {
				deleteFile = true;
			} else {
				resolvedBytes = stages.ours;
			}
			break;
		case THEIRS:
			if (stages.theirs == null) {
				deleteFile = true;
			} else {
				resolvedBytes = stages.theirs;
			}
			break;
		case BOTH:
			if (stages.ours == null && stages.theirs == null) {
				deleteFile = true;
			} else if (stages.ours == null) {
				resolvedBytes = stages.theirs;
			} else if (stages.theirs == null) {
				resolvedBytes = stages.ours;
			} else {
				resolvedBytes = concatenate(stages.ours, stages.theirs);
			}
			break;
		case MANUAL:
			if (manualContent == null) {
				throw new IllegalArgumentException("Manual conflict resolution requires content");
			}
			resolvedBytes = manualContent.getBytes(StandardCharsets.UTF_8);
			if (GitCommonUtils.isBinaryContent(resolvedBytes)) {
				throw new IllegalArgumentException("Manual conflict resolution does not support binary content");
			}
			break;
		default:
			throw new IllegalArgumentException("Unsupported resolution mode: " + mode);
		}

		if (deleteFile) {
			git.rm().addFilepattern(path).call();
		} else {
			Path target = repo.getWorkTree().toPath().resolve(path);
			if (Files.isSymbolicLink(target)) {
				throw new IOException("Refusing to write through a symbolic link at " + path);
			}
			Path parent = target.getParent();
			if (parent != null) {
				Files.createDirectories(parent);
			}
			Files.write(target, resolvedBytes);
			git.add().addFilepattern(path).call();
		}

		List<String> remaining = listConflictedPaths(repo);
		remaining.remove(path);

		return new GitConflictResolveResult(true, remaining);
	}

	private static String diffOursVsTheirs(byte[] oursBytes, byte[] theirsBytes) throws IOException {
		RawText oursRaw = new RawText(oursBytes == null ? new byte[0] : oursBytes);
		RawText theirsRaw = new RawText(theirsBytes == null ? new byte[0] : theirsBytes);
		EditList edits = MyersDiff.INSTANCE.diff(RawTextComparator.DEFAULT, oursRaw, theirsRaw);
		try (ByteArrayOutputStream out = new ByteArrayOutputStream();
				DiffFormatter diffFormatter = new DiffFormatter(out)) {
			diffFormatter.format(edits, oursRaw, theirsRaw);
			return out.toString("UTF-8");
		}
	}

	private static byte[] concatenate(byte[] ours, byte[] theirs) {
		String oursText = new String(ours, StandardCharsets.UTF_8);
		if (!oursText.isEmpty() && !oursText.endsWith("\n")) {
			oursText = oursText + "\n";
		}
		byte[] oursWithNewline = oursText.getBytes(StandardCharsets.UTF_8);
		byte[] combined = new byte[oursWithNewline.length + theirs.length];
		System.arraycopy(oursWithNewline, 0, combined, 0, oursWithNewline.length);
		System.arraycopy(theirs, 0, combined, oursWithNewline.length, theirs.length);
		return combined;
	}
}
