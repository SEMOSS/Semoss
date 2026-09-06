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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.lib.Repository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GitStageUtilsTest {

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
	void stage_untrackedFile_movesItToIndexAsAdded() throws Exception {
		Files.writeString(tempDir.resolve("new.txt"), "content");

		GitStageUtils.stage(repo, List.of("new.txt"));

		Status status = git.status().call();
		assertTrue(status.getAdded().contains("new.txt"));
	}

	@Test
	void stage_modifiedFile_movesItToIndexAsChanged() throws Exception {
		Files.writeString(tempDir.resolve("file.txt"), "v1");
		git.add().addFilepattern("file.txt").call();
		commit("init");
		Files.writeString(tempDir.resolve("file.txt"), "v2");

		GitStageUtils.stage(repo, List.of("file.txt"));

		Status status = git.status().call();
		assertTrue(status.getChanged().contains("file.txt"));
	}

	@Test
	void stage_deletedFile_usesRmCachedAndMovesToIndexAsRemoved() throws Exception {
		Files.writeString(tempDir.resolve("file.txt"), "v1");
		git.add().addFilepattern("file.txt").call();
		commit("init");
		Files.delete(tempDir.resolve("file.txt"));

		GitStageUtils.stage(repo, List.of("file.txt"));

		Status status = git.status().call();
		assertTrue(status.getRemoved().contains("file.txt"));
	}

	@Test
	void stage_renamePair_stagingBothPathsResultsInDeletePlusAddInIndex() throws Exception {
		Files.writeString(tempDir.resolve("old.txt"), "content");
		git.add().addFilepattern("old.txt").call();
		commit("init");

		Files.delete(tempDir.resolve("old.txt"));
		Files.writeString(tempDir.resolve("new.txt"), "content");

		GitStageUtils.stage(repo, List.of("old.txt", "new.txt"));

		Status status = git.status().call();
		assertTrue(status.getRemoved().contains("old.txt"));
		assertTrue(status.getAdded().contains("new.txt"));
	}

	@Test
	void stage_pathNotTrackedOrPresent_throwsIllegalArgumentException() {
		assertThrows(IllegalArgumentException.class, () -> GitStageUtils.stage(repo, List.of("does-not-exist.txt")));
	}

	@Test
	void unstage_addedFile_returnsToUntracked_workingTreeUnchanged() throws Exception {
		Files.writeString(tempDir.resolve("new.txt"), "content");
		git.add().addFilepattern("new.txt").call();

		GitStageUtils.unstage(repo, List.of("new.txt"));

		Status status = git.status().call();
		assertTrue(status.getUntracked().contains("new.txt"));
		assertEquals("content", Files.readString(tempDir.resolve("new.txt")));
	}

	@Test
	void unstage_modifiedStagedFile_resetsIndexToHeadContent_workingTreeUnchanged() throws Exception {
		Files.writeString(tempDir.resolve("file.txt"), "v1");
		git.add().addFilepattern("file.txt").call();
		commit("init");

		Files.writeString(tempDir.resolve("file.txt"), "v2");
		git.add().addFilepattern("file.txt").call();

		GitStageUtils.unstage(repo, List.of("file.txt"));

		Status status = git.status().call();
		assertTrue(status.getModified().contains("file.txt"));
		assertTrue(status.getChanged().isEmpty());
		assertEquals("v2", Files.readString(tempDir.resolve("file.txt")));
	}

	@Test
	void unstage_onlyAffectsRequestedPaths_othersRemainStaged() throws Exception {
		Files.writeString(tempDir.resolve("a.txt"), "a");
		Files.writeString(tempDir.resolve("b.txt"), "b");
		git.add().addFilepattern("a.txt").call();
		git.add().addFilepattern("b.txt").call();

		GitStageUtils.unstage(repo, List.of("a.txt"));

		Status status = git.status().call();
		assertTrue(status.getUntracked().contains("a.txt"));
		assertTrue(status.getAdded().contains("b.txt"));
	}
}
