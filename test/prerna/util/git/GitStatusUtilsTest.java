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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.MergeResult;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import prerna.util.git.GitStatusUtils.GitStatusResult;

class GitStatusUtilsTest {

	@TempDir
	Path tempDir;

	private Git git;
	private Repository repo;

	@BeforeEach
	void setUp() throws Exception {
		git = Git.init().setDirectory(tempDir.toFile()).call();
		repo = git.getRepository();
	}

	private void writeAndAdd(String path, String content) throws Exception {
		Files.writeString(tempDir.resolve(path), content);
		git.add().addFilepattern(path).call();
	}

	private RevCommit commit(String message) throws Exception {
		return git.commit().setMessage(message).setAuthor("Test", "test@test.com").call();
	}

	@Test
	void computeStatus_unbornRepo_branchAndLastCommitNull_cleanTrue() throws Exception {
		GitStatusResult status = GitStatusUtils.computeStatus(repo);
		assertNull(status.branch);
		assertNull(status.headCommitId);
		assertNull(status.lastCommit);
		assertTrue(status.clean);
	}

	@Test
	void computeStatus_cleanRepoAfterCommit_isClean() throws Exception {
		writeAndAdd("a.txt", "x\n");
		commit("init");

		GitStatusResult status = GitStatusUtils.computeStatus(repo);
		assertTrue(status.clean);
		assertNotNull(status.lastCommit);
	}

	@Test
	void computeStatus_detectsUntrackedFile() throws Exception {
		Files.writeString(tempDir.resolve("new.txt"), "content\n");

		GitStatusResult status = GitStatusUtils.computeStatus(repo);
		assertEquals(1, status.untracked.size());
		assertEquals("new.txt", status.untracked.get(0).path);
		assertEquals("UNTRACKED", status.untracked.get(0).status);
		assertFalse(status.clean);
	}

	@Test
	void computeStatus_detectsUnstagedModification() throws Exception {
		writeAndAdd("file.txt", "v1\n");
		commit("add file");
		Files.writeString(tempDir.resolve("file.txt"), "v2\n");

		GitStatusResult status = GitStatusUtils.computeStatus(repo);
		assertEquals(1, status.unstaged.size());
		assertEquals("MODIFIED", status.unstaged.get(0).status);
	}

	@Test
	void computeStatus_detectsUnstagedDeletion() throws Exception {
		writeAndAdd("file.txt", "v1\n");
		commit("add file");
		Files.delete(tempDir.resolve("file.txt"));

		GitStatusResult status = GitStatusUtils.computeStatus(repo);
		assertEquals(1, status.unstaged.size());
		assertEquals("DELETED", status.unstaged.get(0).status);
	}

	@Test
	void computeStatus_detectsStagedAddition() throws Exception {
		writeAndAdd("staged.txt", "content\n");

		GitStatusResult status = GitStatusUtils.computeStatus(repo);
		assertEquals(1, status.staged.size());
		assertEquals("ADDED", status.staged.get(0).status);
	}

	@Test
	void computeStatus_detectsStagedModification() throws Exception {
		writeAndAdd("file.txt", "v1\n");
		commit("add file");
		Files.writeString(tempDir.resolve("file.txt"), "v2\n");
		git.add().addFilepattern("file.txt").call();

		GitStatusResult status = GitStatusUtils.computeStatus(repo);
		assertEquals(1, status.staged.size());
		assertEquals("MODIFIED", status.staged.get(0).status);
	}

	@Test
	void computeStatus_detectsStagedDeletion() throws Exception {
		writeAndAdd("file.txt", "v1\n");
		commit("add file");
		git.rm().setCached(true).addFilepattern("file.txt").call();

		GitStatusResult status = GitStatusUtils.computeStatus(repo);
		assertEquals(1, status.staged.size());
		assertEquals("DELETED", status.staged.get(0).status);
	}

	@Test
	void computeStatus_renameSurfacesAsDeleteAndAddPair() throws Exception {
		writeAndAdd("old.txt", "content\n");
		commit("add old");

		Files.delete(tempDir.resolve("old.txt"));
		Files.writeString(tempDir.resolve("new.txt"), "content\n");

		GitStatusResult status = GitStatusUtils.computeStatus(repo);
		assertTrue(status.unstaged.stream().anyMatch(f -> f.path.equals("old.txt") && f.status.equals("DELETED")));
		assertTrue(status.untracked.stream().anyMatch(f -> f.path.equals("new.txt")));
	}

	@Test
	void computeStatus_conflictedPathExcludedFromStagedAndUnstaged_andListedInConflicted() throws Exception {
		String initialBranch = repo.getBranch();
		writeAndAdd("conflict.txt", "base\n");
		commit("base commit");

		git.branchCreate().setName("feature").call();

		writeAndAdd("conflict.txt", "main change\n");
		commit("main change");

		git.checkout().setName("feature").call();
		writeAndAdd("conflict.txt", "feature change\n");
		commit("feature change");

		git.checkout().setName(initialBranch).call();
		MergeResult mergeResult = git.merge().include(repo.findRef("feature")).call();
		assertEquals(MergeResult.MergeStatus.CONFLICTING, mergeResult.getMergeStatus());

		GitStatusResult status = GitStatusUtils.computeStatus(repo);
		assertEquals(1, status.conflicted.size());
		assertEquals("conflict.txt", status.conflicted.get(0).path);
		assertEquals("CONFLICTED", status.conflicted.get(0).status);
		assertTrue(status.staged.isEmpty());
		assertTrue(status.unstaged.stream().noneMatch(f -> f.path.equals("conflict.txt")));
		assertFalse(status.clean);
	}

	@Test
	void computeStatus_detachedHead_branchNullDetachedTrue() throws Exception {
		writeAndAdd("a.txt", "x\n");
		RevCommit c = commit("init");
		git.checkout().setName(c.getName()).call();

		GitStatusResult status = GitStatusUtils.computeStatus(repo);
		assertNull(status.branch);
		assertTrue(status.detached);
		assertEquals(c.getName(), status.headCommitId);
	}

	@Test
	void computeStatus_listsAreSortedByPath() throws Exception {
		Files.writeString(tempDir.resolve("zeta.txt"), "z\n");
		Files.writeString(tempDir.resolve("alpha.txt"), "a\n");

		GitStatusResult status = GitStatusUtils.computeStatus(repo);
		assertEquals(2, status.untracked.size());
		assertEquals("alpha.txt", status.untracked.get(0).path);
		assertEquals("zeta.txt", status.untracked.get(1).path);
	}

	@Test
	void computeStatus_lastCommitDateIsIso8601Formatted() throws Exception {
		writeAndAdd("a.txt", "x\n");
		commit("init");

		GitStatusResult status = GitStatusUtils.computeStatus(repo);
		assertNotNull(status.lastCommit.date);
		assertNotNull(Instant.parse(status.lastCommit.date));
	}
}
