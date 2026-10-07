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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import prerna.util.git.GitCommonUtils.GitHeadState;
import prerna.util.git.GitCommonUtils.TruncatedText;

class GitCommonUtilsTest {

	@TempDir
	Path tempDir;

	private Git git;
	private Repository repo;

	@BeforeEach
	void setUp() throws Exception {
		git = Git.init().setDirectory(tempDir.toFile()).call();
		repo = git.getRepository();
	}

	@Test
	void resolveHeadState_unbornRepo_returnsAllNulls() throws Exception {
		GitHeadState state = GitCommonUtils.resolveHeadState(repo);
		assertNull(state.branch);
		assertFalse(state.detached);
		assertNull(state.headCommitId);
	}

	@Test
	void resolveHeadState_attachedBranch_returnsBranchNameAndHeadCommit() throws Exception {
		Files.writeString(tempDir.resolve("a.txt"), "hello");
		git.add().addFilepattern("a.txt").call();
		RevCommit commit = git.commit().setMessage("init").setAuthor("Test", "test@test.com").call();

		GitHeadState state = GitCommonUtils.resolveHeadState(repo);
		assertEquals(repo.getBranch(), state.branch);
		assertFalse(state.detached);
		assertEquals(commit.getName(), state.headCommitId);
	}

	@Test
	void resolveHeadState_detachedHead_returnsNullBranchAndDetachedTrue() throws Exception {
		Files.writeString(tempDir.resolve("a.txt"), "hello");
		git.add().addFilepattern("a.txt").call();
		RevCommit commit = git.commit().setMessage("init").setAuthor("Test", "test@test.com").call();
		git.checkout().setName(commit.getName()).call();

		GitHeadState state = GitCommonUtils.resolveHeadState(repo);
		assertNull(state.branch);
		assertTrue(state.detached);
		assertEquals(commit.getName(), state.headCommitId);
	}

	@Test
	void validateRepoRelativePath_rejectsBlankPath() {
		assertThrows(IllegalArgumentException.class, () -> GitCommonUtils.validateRepoRelativePath(repo, ""));
		assertThrows(IllegalArgumentException.class, () -> GitCommonUtils.validateRepoRelativePath(repo, null));
	}

	@Test
	void validateRepoRelativePath_rejectsAbsolutePath() {
		assertThrows(IllegalArgumentException.class,
				() -> GitCommonUtils.validateRepoRelativePath(repo, "/etc/passwd"));
	}

	@Test
	void validateRepoRelativePath_rejectsDotDotTraversal() {
		assertThrows(IllegalArgumentException.class,
				() -> GitCommonUtils.validateRepoRelativePath(repo, "../outside.txt"));
		assertThrows(IllegalArgumentException.class,
				() -> GitCommonUtils.validateRepoRelativePath(repo, "sub/../../outside.txt"));
	}

	@Test
	void validateRepoRelativePath_rejectsPathEscapingWorkTreeViaCanonicalization() throws Exception {
		File outside = Files.createTempDirectory("outside-worktree").toFile();
		outside.deleteOnExit();
		File link = new File(tempDir.toFile(), "escape");
		try {
			Files.createSymbolicLink(link.toPath(), outside.toPath());
		} catch (UnsupportedOperationException | java.io.IOException e) {
			Assumptions.assumeTrue(false, "symlinks not supported on this platform/filesystem");
			return;
		}

		assertThrows(IllegalArgumentException.class,
				() -> GitCommonUtils.validateRepoRelativePath(repo, "escape/secret.txt"));
	}

	@Test
	void validateRepoRelativePath_rejectsDotGitSegment() {
		assertThrows(IllegalArgumentException.class,
				() -> GitCommonUtils.validateRepoRelativePath(repo, ".git/config"));
	}

	@Test
	void validateRepoRelativePath_acceptsAndNormalizesValidNestedPath() {
		String result = GitCommonUtils.validateRepoRelativePath(repo, "src//nested\\file.txt");
		assertEquals("src/nested/file.txt", result);
	}

	@Test
	void validateRepoRelativePaths_rejectsEmptyList() {
		assertThrows(IllegalArgumentException.class,
				() -> GitCommonUtils.validateRepoRelativePaths(repo, Collections.emptyList()));
	}

	@Test
	void validateRepoRelativePaths_validatesEveryEntry() {
		List<String> result = GitCommonUtils.validateRepoRelativePaths(repo, List.of("a.txt", "sub/b.txt"));
		assertEquals(List.of("a.txt", "sub/b.txt"), result);

		assertThrows(IllegalArgumentException.class,
				() -> GitCommonUtils.validateRepoRelativePaths(repo, List.of("a.txt", "../escape.txt")));
	}

	@Test
	void isBinaryContent_detectsNullByteAsBinary() {
		assertTrue(GitCommonUtils.isBinaryContent(new byte[] { 0, 1, 2, 3 }));
	}

	@Test
	void isBinaryContent_detectsPlainTextAsNotBinary() {
		assertFalse(GitCommonUtils.isBinaryContent("hello world".getBytes(StandardCharsets.UTF_8)));
	}

	@Test
	void isBinaryContent_nullIsNotBinary() {
		assertFalse(GitCommonUtils.isBinaryContent(null));
	}

	@Test
	void truncate_leavesShortTextUntouched() {
		String text = "line1\nline2\nline3";
		TruncatedText result = GitCommonUtils.truncate(text);
		assertEquals(text, result.text);
		assertFalse(result.truncated);
	}

	@Test
	void truncate_truncatesAndFlagsAtMaxDiffLines() {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < GitCommonUtils.MAX_DIFF_LINES + 50; i++) {
			sb.append("line").append(i).append("\n");
		}
		TruncatedText result = GitCommonUtils.truncate(sb.toString());
		assertTrue(result.truncated);
		assertEquals(GitCommonUtils.MAX_DIFF_LINES, result.text.lines().count());
	}
}
