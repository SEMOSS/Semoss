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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.CommitBuilder;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.FileMode;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectInserter;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.TreeFormatter;
import org.eclipse.jgit.revwalk.RevCommit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Confirms {@link GitRepoUtils#assertNoSymlinks(Repository, ObjectId, String)}
 * behaves as before after being widened from {@code private} to
 * {@code public} for reuse by the project git working-tree reactors.
 */
class GitRepoUtilsAssertNoSymlinksTest {

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
	void assertNoSymlinks_pureTextTree_doesNotThrow() throws Exception {
		Files.writeString(tempDir.resolve("a.txt"), "content");
		git.add().addFilepattern("a.txt").call();
		RevCommit commit = git.commit().setMessage("init").setAuthor("Test", "test@test.com").call();

		assertDoesNotThrow(() -> GitRepoUtils.assertNoSymlinks(repo, commit.getId(), "test-repo"));
	}

	@Test
	void assertNoSymlinks_treeContainingSymlink_throwsIOException() throws Exception {
		// Build the symlink tree entry directly via the object database, since
		// Files.createSymbolicLink + `git add` does not reliably preserve the
		// symlink file mode across every platform/filesystem configuration.
		try (ObjectInserter inserter = repo.newObjectInserter()) {
			ObjectId blobId = inserter.insert(Constants.OBJ_BLOB, "/etc/passwd".getBytes(StandardCharsets.UTF_8));

			TreeFormatter treeFormatter = new TreeFormatter();
			treeFormatter.append("link", FileMode.SYMLINK, blobId);
			ObjectId treeId = inserter.insert(treeFormatter);

			PersonIdent ident = new PersonIdent("Test", "test@test.com");
			CommitBuilder commitBuilder = new CommitBuilder();
			commitBuilder.setTreeId(treeId);
			commitBuilder.setMessage("symlink commit");
			commitBuilder.setAuthor(ident);
			commitBuilder.setCommitter(ident);
			ObjectId commitId = inserter.insert(commitBuilder);
			inserter.flush();

			IOException ex = assertThrows(IOException.class,
					() -> GitRepoUtils.assertNoSymlinks(repo, commitId, "test-repo"));
			assertTrue(ex.getMessage().contains("test-repo"));
			assertTrue(ex.getMessage().contains("link"));
		}
	}
}
