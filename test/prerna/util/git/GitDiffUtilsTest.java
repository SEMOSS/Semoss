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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.Repository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import prerna.util.git.GitDiffUtils.DiffSide;
import prerna.util.git.GitDiffUtils.GitDiffResult;

class GitDiffUtilsTest {

	@TempDir
	Path tempDir;

	private Git git;
	private Repository repo;

	@BeforeEach
	void setUp() throws Exception {
		git = Git.init().setDirectory(tempDir.toFile()).call();
		repo = git.getRepository();
	}

	private void commit(String message) throws Exception {
		git.commit().setMessage(message).setAuthor("Test", "test@test.com").call();
	}

	@Test
	void computeFileDiff_staged_headVsIndex_showsAddedContent() throws Exception {
		Files.writeString(tempDir.resolve("a.txt"), "hello\n");
		git.add().addFilepattern("a.txt").call();

		GitDiffResult result = GitDiffUtils.computeFileDiff(repo, "a.txt", DiffSide.STAGED);
		assertFalse(result.isBinary);
		assertTrue(result.diff.contains("+hello"));
	}

	@Test
	void computeFileDiff_unstaged_indexVsWorkingTree_showsModification() throws Exception {
		Files.writeString(tempDir.resolve("a.txt"), "v1\n");
		git.add().addFilepattern("a.txt").call();
		commit("init");
		Files.writeString(tempDir.resolve("a.txt"), "v2\n");

		GitDiffResult result = GitDiffUtils.computeFileDiff(repo, "a.txt", DiffSide.UNSTAGED);
		assertTrue(result.diff.contains("-v1"));
		assertTrue(result.diff.contains("+v2"));
	}

	@Test
	void computeFileDiff_noChangesOnRequestedSide_returnsEmptyDiffNotError() throws Exception {
		Files.writeString(tempDir.resolve("a.txt"), "v1\n");
		git.add().addFilepattern("a.txt").call();
		commit("init");

		GitDiffResult result = GitDiffUtils.computeFileDiff(repo, "a.txt", DiffSide.UNSTAGED);
		assertEquals("", result.diff);
		assertFalse(result.isBinary);
		assertFalse(result.isTruncated);
	}

	@Test
	void computeFileDiff_binaryFile_setsIsBinaryTrue_diffEmpty() throws Exception {
		Files.write(tempDir.resolve("bin.dat"), new byte[] { 0, 1, 2, 3, 0, 5 });
		git.add().addFilepattern("bin.dat").call();

		GitDiffResult result = GitDiffUtils.computeFileDiff(repo, "bin.dat", DiffSide.STAGED);
		assertTrue(result.isBinary);
		assertEquals("", result.diff);
	}

	@Test
	void computeFileDiff_largeDiff_truncatesAtMaxDiffLinesAndSetsFlag() throws Exception {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < GitCommonUtils.MAX_DIFF_LINES + 100; i++) {
			sb.append("line").append(i).append("\n");
		}
		Files.writeString(tempDir.resolve("big.txt"), sb.toString());
		git.add().addFilepattern("big.txt").call();

		GitDiffResult result = GitDiffUtils.computeFileDiff(repo, "big.txt", DiffSide.STAGED);
		assertTrue(result.isTruncated);
	}

	@Test
	void computeFileDiff_deletedFile_unstagedSide_showsDeletionDiff() throws Exception {
		Files.writeString(tempDir.resolve("a.txt"), "content\n");
		git.add().addFilepattern("a.txt").call();
		commit("init");
		Files.delete(tempDir.resolve("a.txt"));

		GitDiffResult result = GitDiffUtils.computeFileDiff(repo, "a.txt", DiffSide.UNSTAGED);
		assertTrue(result.diff.contains("-content"));
	}

	@Test
	void computeFileDiff_renamedFile_stagedSide_reflectsRenameContentDiff() throws Exception {
		Files.writeString(tempDir.resolve("old.txt"), "same content\nline2\n");
		git.add().addFilepattern("old.txt").call();
		commit("init");

		Files.delete(tempDir.resolve("old.txt"));
		Files.writeString(tempDir.resolve("new.txt"), "same content\nline2 changed\n");
		git.add().addFilepattern("new.txt").call();
		git.rm().setCached(true).addFilepattern("old.txt").call();

		GitDiffResult result = GitDiffUtils.computeFileDiff(repo, "new.txt", DiffSide.STAGED);
		assertFalse(result.isBinary);
		assertTrue(result.diff.contains("line2 changed"));
	}
}
