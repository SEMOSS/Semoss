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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.CheckoutConflictException;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.RefUpdate;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.StoredConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import prerna.util.git.ProjectGitBranchUtils.GitBranchInfo;
import prerna.util.git.ProjectGitBranchUtils.GitCheckoutTarget;

class ProjectGitBranchUtilsTest {

	@TempDir
	Path tempDir;

	private Git git;
	private Repository repo;

	@BeforeEach
	void setUp() throws Exception {
		git = Git.init().setDirectory(tempDir.toFile()).call();
		repo = git.getRepository();
		Files.writeString(tempDir.resolve("a.txt"), "content");
		git.add().addFilepattern("a.txt").call();
		git.commit().setMessage("init").setAuthor("Test", "test@test.com").call();
	}

	private void fakeRemoteTrackingRef(String remoteName, String branchName) throws Exception {
		RefUpdate update = repo.updateRef("refs/remotes/" + remoteName + "/" + branchName);
		update.setNewObjectId(repo.resolve("HEAD"));
		update.update();
	}

	@Test
	void listBranches_localOnly_noRemote_upstreamAheadBehindAllNull() throws Exception {
		List<GitBranchInfo> branches = ProjectGitBranchUtils.listBranches(repo);
		assertEquals(1, branches.size());
		GitBranchInfo branch = branches.get(0);
		assertFalse(branch.remote);
		assertTrue(branch.current);
		assertNull(branch.upstream);
		assertNull(branch.ahead);
		assertNull(branch.behind);
	}

	@Test
	void listBranches_currentFlagTrueOnlyForCheckedOutLocalBranch() throws Exception {
		String initialBranch = repo.getBranch();
		git.branchCreate().setName("feature").call();

		List<GitBranchInfo> branches = ProjectGitBranchUtils.listBranches(repo);
		for (GitBranchInfo b : branches) {
			if (b.name.equals(initialBranch)) {
				assertTrue(b.current);
			} else if (b.name.equals("feature")) {
				assertFalse(b.current);
			}
		}
	}

	@Test
	void listBranches_detachedHead_noCurrentBranchFlagged() throws Exception {
		ObjectId head = repo.resolve("HEAD");
		git.checkout().setName(head.getName()).call();

		List<GitBranchInfo> branches = ProjectGitBranchUtils.listBranches(repo);
		assertTrue(branches.stream().noneMatch(b -> b.current));
		// BranchListCommand includes a synthetic "HEAD" entry once detached -
		// confirm it's filtered out and only the real local branch remains.
		assertEquals(1, branches.stream().filter(b -> !b.remote).count());
		assertTrue(branches.stream().noneMatch(b -> b.name.equals("HEAD")));
	}

	@Test
	void listBranches_sortedByFullRefName() throws Exception {
		git.branchCreate().setName("aaa").call();
		git.branchCreate().setName("zzz").call();

		List<GitBranchInfo> branches = ProjectGitBranchUtils.listBranches(repo);
		List<String> fullNames = new ArrayList<>();
		for (GitBranchInfo b : branches) {
			fullNames.add(b.fullName);
		}
		List<String> sorted = new ArrayList<>(fullNames);
		Collections.sort(sorted);
		assertEquals(sorted, fullNames);
	}

	@Test
	void listBranches_withRemote_populatesUpstreamAheadBehind() throws Exception {
		String branch = repo.getBranch();
		fakeRemoteTrackingRef("origin", branch);

		StoredConfig config = repo.getConfig();
		// BranchConfig/BranchTrackingStatus resolve the tracking ref through the
		// remote's fetch refspec, so a real (even if unreachable) remote entry is
		// required in addition to the branch.<name>.remote/merge keys.
		config.setString("remote", "origin", "url", "file:///dummy");
		config.setString("remote", "origin", "fetch", "+refs/heads/*:refs/remotes/origin/*");
		config.setString("branch", branch, "remote", "origin");
		config.setString("branch", branch, "merge", "refs/heads/" + branch);
		config.save();

		Files.writeString(tempDir.resolve("b.txt"), "more content");
		git.add().addFilepattern("b.txt").call();
		git.commit().setMessage("local only commit").setAuthor("Test", "test@test.com").call();

		List<GitBranchInfo> branches = ProjectGitBranchUtils.listBranches(repo);
		GitBranchInfo localBranchInfo = branches.stream().filter(b -> !b.remote && b.name.equals(branch)).findFirst()
				.orElseThrow();
		assertEquals("origin/" + branch, localBranchInfo.upstream);
		assertEquals(Integer.valueOf(1), localBranchInfo.ahead);
		assertEquals(Integer.valueOf(0), localBranchInfo.behind);
	}

	@Test
	void resolveCheckoutTarget_existsLocally() throws Exception {
		git.branchCreate().setName("feature").call();
		GitCheckoutTarget target = ProjectGitBranchUtils.resolveCheckoutTarget(repo, "feature");
		assertTrue(target.existsLocally);
	}

	@Test
	void resolveCheckoutTarget_uniqueRemoteMatch_createsTrackingCandidate() throws Exception {
		fakeRemoteTrackingRef("origin", "feature");

		GitCheckoutTarget target = ProjectGitBranchUtils.resolveCheckoutTarget(repo, "feature");
		assertFalse(target.existsLocally);
		assertEquals("refs/remotes/origin/feature", target.uniqueRemoteRef);
	}

	@Test
	void resolveCheckoutTarget_ambiguousAcrossMultipleRemotes_returnsCandidateList() throws Exception {
		fakeRemoteTrackingRef("origin", "feature");
		fakeRemoteTrackingRef("upstream", "feature");

		GitCheckoutTarget target = ProjectGitBranchUtils.resolveCheckoutTarget(repo, "feature");
		assertFalse(target.existsLocally);
		assertNull(target.uniqueRemoteRef);
		assertEquals(2, target.ambiguousRemotes.size());
	}

	@Test
	void resolveCheckoutTarget_noMatch_returnsEmpty() throws Exception {
		GitCheckoutTarget target = ProjectGitBranchUtils.resolveCheckoutTarget(repo, "does-not-exist");
		assertFalse(target.existsLocally);
		assertNull(target.uniqueRemoteRef);
		assertTrue(target.ambiguousRemotes.isEmpty());
	}

	@Test
	void createAndCheckoutBranch_success_switchesHead() throws Exception {
		ProjectGitBranchUtils.createAndCheckoutBranch(git, "feature", "HEAD");
		assertEquals("feature", repo.getBranch());
	}

	@Test
	void createAndCheckoutBranch_checkoutConflict_rollsBackCreatedBranchRef() throws Exception {
		String initialBranch = repo.getBranch();
		git.branchCreate().setName("other").call();
		git.checkout().setName("other").call();
		Files.writeString(tempDir.resolve("a.txt"), "other branch content");
		git.add().addFilepattern("a.txt").call();
		git.commit().setMessage("other change").setAuthor("Test", "test@test.com").call();
		git.checkout().setName(initialBranch).call();

		// dirty the working tree in a way that conflicts with switching to "other"
		Files.writeString(tempDir.resolve("a.txt"), "dirty uncommitted content");

		assertThrows(CheckoutConflictException.class,
				() -> ProjectGitBranchUtils.createAndCheckoutBranch(git, "new-branch", "other"));

		assertNull(repo.findRef("refs/heads/new-branch"));
	}

	@Test
	void isValidBranchName_rejectsIllegalGitRefNameSyntax() {
		assertFalse(ProjectGitBranchUtils.isValidBranchName("has space"));
		assertFalse(ProjectGitBranchUtils.isValidBranchName(".."));
		assertFalse(ProjectGitBranchUtils.isValidBranchName("branch.lock"));
		assertFalse(ProjectGitBranchUtils.isValidBranchName("-leading-dash"));
		assertFalse(ProjectGitBranchUtils.isValidBranchName("bad~name"));
	}

	@Test
	void isValidBranchName_rejectsDuplicateOfHeadCaseInsensitive() {
		assertFalse(ProjectGitBranchUtils.isValidBranchName("HEAD"));
		assertFalse(ProjectGitBranchUtils.isValidBranchName("head"));
	}

	@Test
	void isValidBranchName_acceptsSlashedFeatureBranchNames() {
		assertTrue(ProjectGitBranchUtils.isValidBranchName("feature/example"));
	}

	@Test
	void isValidBranchName_rejectsBlank() {
		assertFalse(ProjectGitBranchUtils.isValidBranchName(""));
		assertFalse(ProjectGitBranchUtils.isValidBranchName(null));
	}
}
