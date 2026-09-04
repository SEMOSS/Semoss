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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.dircache.DirCache;
import org.eclipse.jgit.dircache.DirCacheEntry;
import org.eclipse.jgit.lib.Repository;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import prerna.util.git.GitConflictUtils.GitConflictDiffResult;
import prerna.util.git.GitConflictUtils.GitConflictResolveResult;
import prerna.util.git.GitConflictUtils.GitConflictStages;
import prerna.util.git.GitConflictUtils.ResolutionMode;

class GitConflictUtilsTest {

	@TempDir
	Path tempDir;

	private Git git;
	private Repository repo;
	private String initialBranch;

	@BeforeEach
	void setUp() throws Exception {
		git = Git.init().setDirectory(tempDir.toFile()).call();
		repo = git.getRepository();
		initialBranch = repo.getBranch();
	}

	private void writeAndAdd(String path, String content) throws Exception {
		Files.writeString(tempDir.resolve(path), content);
		git.add().addFilepattern(path).call();
	}

	private void commit(String message) throws Exception {
		git.commit().setMessage(message).setAuthor("Test", "test@test.com").call();
	}

	/**
	 * Sets up a modify/modify conflict on conflict.txt: base -&gt; ours vs theirs.
	 */
	private void setUpModifyModifyConflict() throws Exception {
		writeAndAdd("conflict.txt", "base\n");
		commit("base");

		git.branchCreate().setName("feature").call();

		writeAndAdd("conflict.txt", "ours\n");
		commit("ours change");

		git.checkout().setName("feature").call();
		writeAndAdd("conflict.txt", "theirs\n");
		commit("theirs change");

		git.checkout().setName(initialBranch).call();
		git.merge().include(repo.findRef("feature")).call();
	}

	@Test
	void readConflictStages_returnsBaseOursTheirsForModifyModifyConflict() throws Exception {
		setUpModifyModifyConflict();

		GitConflictStages stages = GitConflictUtils.readConflictStages(repo, "conflict.txt");
		assertEquals("base\n", new String(stages.base, StandardCharsets.UTF_8));
		assertEquals("ours\n", new String(stages.ours, StandardCharsets.UTF_8));
		assertEquals("theirs\n", new String(stages.theirs, StandardCharsets.UTF_8));
	}

	@Test
	void readConflictStages_missingOursStage_whenOursDeletedFile_isNull() throws Exception {
		writeAndAdd("conflict.txt", "base\n");
		commit("base");
		git.branchCreate().setName("feature").call();

		git.rm().addFilepattern("conflict.txt").call();
		commit("ours deletes");

		git.checkout().setName("feature").call();
		writeAndAdd("conflict.txt", "theirs change\n");
		commit("theirs change");

		git.checkout().setName(initialBranch).call();
		git.merge().include(repo.findRef("feature")).call();

		GitConflictStages stages = GitConflictUtils.readConflictStages(repo, "conflict.txt");
		assertNull(stages.ours);
		assertNotNull(stages.theirs);
	}

	@Test
	void computeConflictDiff_populatesAllFourContentFieldsAndDiffBetweenOursAndTheirs() throws Exception {
		setUpModifyModifyConflict();

		GitConflictDiffResult result = GitConflictUtils.computeConflictDiff(repo, "conflict.txt");
		assertEquals("base\n", result.base);
		assertEquals("ours\n", result.ours);
		assertEquals("theirs\n", result.theirs);
		assertNotNull(result.result);
		assertFalse(result.isBinary);
		assertFalse(result.diff.isEmpty());
	}

	@Test
	void computeConflictDiff_binaryConflict_setsIsBinaryTrue() throws Exception {
		Files.write(tempDir.resolve("conflict.bin"), new byte[] { 0, 1, 2 });
		git.add().addFilepattern("conflict.bin").call();
		commit("base");
		git.branchCreate().setName("feature").call();

		Files.write(tempDir.resolve("conflict.bin"), new byte[] { 0, 1, 3 });
		git.add().addFilepattern("conflict.bin").call();
		commit("ours change");

		git.checkout().setName("feature").call();
		Files.write(tempDir.resolve("conflict.bin"), new byte[] { 0, 1, 4 });
		git.add().addFilepattern("conflict.bin").call();
		commit("theirs change");

		git.checkout().setName(initialBranch).call();
		git.merge().include(repo.findRef("feature")).call();

		GitConflictDiffResult result = GitConflictUtils.computeConflictDiff(repo, "conflict.bin");
		assertTrue(result.isBinary);
	}

	@Test
	void resolveConflict_ours_writesOursContentAndStagesIt() throws Exception {
		setUpModifyModifyConflict();

		GitConflictResolveResult result = GitConflictUtils.resolveConflict(git, "conflict.txt", ResolutionMode.OURS,
				null);
		assertTrue(result.resolved);
		assertTrue(result.remainingConflicts.isEmpty());
		assertEquals("ours\n", Files.readString(tempDir.resolve("conflict.txt")));

		Status status = git.status().call();
		assertTrue(status.getConflicting().isEmpty());
	}

	@Test
	void resolveConflict_theirs_writesTheirsContentAndStagesIt() throws Exception {
		setUpModifyModifyConflict();

		GitConflictUtils.resolveConflict(git, "conflict.txt", ResolutionMode.THEIRS, null);
		assertEquals("theirs\n", Files.readString(tempDir.resolve("conflict.txt")));
	}

	@Test
	void resolveConflict_ours_whenOursDeleted_removesFileAndStagesDeletion() throws Exception {
		writeAndAdd("conflict.txt", "base\n");
		commit("base");
		git.branchCreate().setName("feature").call();

		git.rm().addFilepattern("conflict.txt").call();
		commit("ours deletes");

		git.checkout().setName("feature").call();
		writeAndAdd("conflict.txt", "theirs change\n");
		commit("theirs change");

		git.checkout().setName(initialBranch).call();
		git.merge().include(repo.findRef("feature")).call();

		GitConflictResolveResult result = GitConflictUtils.resolveConflict(git, "conflict.txt", ResolutionMode.OURS,
				null);
		assertTrue(result.resolved);
		assertFalse(Files.exists(tempDir.resolve("conflict.txt")));
	}

	@Test
	void resolveConflict_both_concatenatesOursThenTheirsWithSeparator() throws Exception {
		setUpModifyModifyConflict();

		GitConflictUtils.resolveConflict(git, "conflict.txt", ResolutionMode.BOTH, null);
		assertEquals("ours\ntheirs\n", Files.readString(tempDir.resolve("conflict.txt")));
	}

	@Test
	void resolveConflict_manual_writesSuppliedContentAndStagesIt() throws Exception {
		setUpModifyModifyConflict();

		GitConflictUtils.resolveConflict(git, "conflict.txt", ResolutionMode.MANUAL, "manual resolution\n");
		assertEquals("manual resolution\n", Files.readString(tempDir.resolve("conflict.txt")));
	}

	@Test
	void resolveConflict_manual_rejectsBinaryContent() throws Exception {
		setUpModifyModifyConflict();
		String binaryLike = "abc\u0000def";

		assertThrows(IllegalArgumentException.class,
				() -> GitConflictUtils.resolveConflict(git, "conflict.txt", ResolutionMode.MANUAL, binaryLike));
	}

	@Test
	void resolveConflict_manual_requiresContent() throws Exception {
		setUpModifyModifyConflict();

		assertThrows(IllegalArgumentException.class,
				() -> GitConflictUtils.resolveConflict(git, "conflict.txt", ResolutionMode.MANUAL, null));
	}

	@Test
	void resolveConflict_pathNotConflicted_throwsIllegalArgumentException() throws Exception {
		writeAndAdd("clean.txt", "content\n");
		commit("init");

		assertThrows(IllegalArgumentException.class,
				() -> GitConflictUtils.resolveConflict(git, "clean.txt", ResolutionMode.OURS, null));
	}

	@Test
	void resolveConflict_addingResolvedPathCollapsesAllThreeStagesToStageZero() throws Exception {
		setUpModifyModifyConflict();

		GitConflictUtils.resolveConflict(git, "conflict.txt", ResolutionMode.OURS, null);

		DirCache dirCache = repo.readDirCache();
		int idx = dirCache.findEntry("conflict.txt");
		assertTrue(idx >= 0);
		DirCacheEntry entry = dirCache.getEntry(idx);
		assertEquals(0, entry.getStage());
		if (idx + 1 < dirCache.getEntryCount()) {
			assertFalse(dirCache.getEntry(idx + 1).getPathString().equals("conflict.txt"));
		}
	}

	@Test
	void resolveConflict_refusesToWriteThroughPreexistingSymlinkAtTargetPath() throws Exception {
		setUpModifyModifyConflict();

		Files.delete(tempDir.resolve("conflict.txt"));
		try {
			Files.createSymbolicLink(tempDir.resolve("conflict.txt"), tempDir.resolve("nonexistent-target"));
		} catch (UnsupportedOperationException | IOException e) {
			Assumptions.assumeTrue(false, "symlinks not supported on this platform/filesystem");
			return;
		}

		assertThrows(IOException.class,
				() -> GitConflictUtils.resolveConflict(git, "conflict.txt", ResolutionMode.OURS, null));
	}

	@Test
	void resolveConflict_remainingConflictsExcludesJustResolvedPath_includesOthers() throws Exception {
		writeAndAdd("a.txt", "base a\n");
		writeAndAdd("b.txt", "base b\n");
		commit("base");
		git.branchCreate().setName("feature").call();

		writeAndAdd("a.txt", "ours a\n");
		writeAndAdd("b.txt", "ours b\n");
		commit("ours change");

		git.checkout().setName("feature").call();
		writeAndAdd("a.txt", "theirs a\n");
		writeAndAdd("b.txt", "theirs b\n");
		commit("theirs change");

		git.checkout().setName(initialBranch).call();
		git.merge().include(repo.findRef("feature")).call();

		GitConflictResolveResult result = GitConflictUtils.resolveConflict(git, "a.txt", ResolutionMode.OURS, null);
		assertEquals(List.of("b.txt"), result.remainingConflicts);
	}

	@Test
	void listConflictedPaths_sortedByPath() throws Exception {
		writeAndAdd("z.txt", "base z\n");
		writeAndAdd("a.txt", "base a\n");
		commit("base");
		git.branchCreate().setName("feature").call();

		writeAndAdd("z.txt", "ours z\n");
		writeAndAdd("a.txt", "ours a\n");
		commit("ours change");

		git.checkout().setName("feature").call();
		writeAndAdd("z.txt", "theirs z\n");
		writeAndAdd("a.txt", "theirs a\n");
		commit("theirs change");

		git.checkout().setName(initialBranch).call();
		git.merge().include(repo.findRef("feature")).call();

		List<String> conflicts = GitConflictUtils.listConflictedPaths(repo);
		assertEquals(List.of("a.txt", "z.txt"), conflicts);
	}
}
