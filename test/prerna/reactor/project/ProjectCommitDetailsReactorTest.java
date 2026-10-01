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
package prerna.reactor.project;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.RefUpdate;
import org.eclipse.jgit.revwalk.RevCommit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineUtility;
import prerna.util.Utility;

/**
 * Covers the {@code parentCommitIds}/{@code refs} extension to
 * {@link ProjectCommitDetailsReactor}, exercised against a real temporary
 * JGit repository with {@code Utility}/{@code EngineUtility}/
 * {@code SecurityProjectUtils} statically mocked to point at it, matching
 * this repo's established static-mocking convention (see
 * {@code ClusterUtilUnitTests}).
 */
class ProjectCommitDetailsReactorTest {

	private static final String PROJECT_ID = "testProjectId";

	@TempDir
	Path tempDir;

	private Git git;
	private ProjectCommitDetailsReactor reactor;
	private MockedStatic<Utility> utilityMock;
	private MockedStatic<EngineUtility> engineUtilityMock;
	private MockedStatic<SecurityProjectUtils> securityMock;

	@BeforeEach
	void setUp() throws Exception {
		git = Git.init().setDirectory(tempDir.toFile()).call();

		IProject project = mock(IProject.class);
		when(project.getEngineName()).thenReturn("testProjectName");

		utilityMock = mockStatic(Utility.class);
		utilityMock.when(() -> Utility.getProject(PROJECT_ID)).thenReturn(project);
		// EngineUtility's static initializer calls Utility.getBaseFolder(); without
		// this stub it resolves to null under the mock and blows up class init
		// (ExceptionInInitializerError) the moment EngineUtility is mocked below.
		utilityMock.when(Utility::getBaseFolder).thenReturn(tempDir.toString());

		engineUtilityMock = mockStatic(EngineUtility.class);
		engineUtilityMock.when(() -> EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.PROJECT,
				PROJECT_ID, "testProjectName")).thenReturn(tempDir.toString());

		securityMock = mockStatic(SecurityProjectUtils.class);
		securityMock.when(() -> SecurityProjectUtils.userCanEditProject(any(), eq(PROJECT_ID))).thenReturn(true);

		reactor = new ProjectCommitDetailsReactor();
		reactor.setNounStore(new NounStore("test"));
		Insight insight = mock(Insight.class);
		User user = mock(User.class);
		reactor.setInsight(insight);
		reactor.keyValue = new HashMap<>();
		when(insight.getUser()).thenReturn(user);
	}

	@AfterEach
	void tearDown() {
		utilityMock.close();
		engineUtilityMock.close();
		securityMock.close();
	}

	private void writeAndAdd(String path, String content) throws Exception {
		Files.writeString(tempDir.resolve(path), content);
		git.add().addFilepattern(path).call();
	}

	private RevCommit commit(String message) throws Exception {
		return git.commit().setMessage(message).setAuthor("Test", "test@test.com").call();
	}

	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> run(String limit, String offset) {
		reactor.keyValue.put(ReactorKeysEnum.PROJECT.getKey(), PROJECT_ID);
		reactor.keyValue.put(ReactorKeysEnum.LIMIT.getKey(), limit);
		reactor.keyValue.put(ReactorKeysEnum.OFFSET.getKey(), offset);
		NounMetadata result = reactor.execute();
		return (List<Map<String, Object>>) result.getValue();
	}

	@SuppressWarnings("unchecked")
	private List<String> parentIds(Map<String, Object> details) {
		return (List<String>) details.get("parentCommitIds");
	}

	@SuppressWarnings("unchecked")
	private List<Map<String, String>> refs(Map<String, Object> details) {
		return (List<Map<String, String>>) details.get("refs");
	}

	@Test
	void unbornHead_returnsEmptyList() {
		List<Map<String, Object>> commits = run("10", "0");
		assertTrue(commits.isEmpty());
	}

	@Test
	void initialCommit_hasEmptyParentCommitIdsList() throws Exception {
		writeAndAdd("a.txt", "content");
		commit("init");

		List<Map<String, Object>> commits = run("10", "0");
		assertEquals(1, commits.size());
		assertEquals(List.of(), parentIds(commits.get(0)));
	}

	@Test
	void linearHistory_parentCommitIdsChainCorrectly() throws Exception {
		writeAndAdd("a.txt", "v1");
		RevCommit c1 = commit("c1");
		writeAndAdd("a.txt", "v2");
		RevCommit c2 = commit("c2");

		List<Map<String, Object>> commits = run("10", "0");
		// git log returns newest first
		Map<String, Object> details2 = commits.get(0);
		Map<String, Object> details1 = commits.get(1);
		assertEquals(c2.getName(), details2.get("commitId"));
		assertEquals(List.of(c1.getName()), parentIds(details2));
		assertEquals(c1.getName(), details1.get("commitId"));
		assertEquals(List.of(), parentIds(details1));
	}

	@Test
	void mergeCommit_hasMultipleParentCommitIds() throws Exception {
		String initialBranch = git.getRepository().getBranch();
		writeAndAdd("a.txt", "base");
		commit("base");
		git.branchCreate().setName("feature").call();

		writeAndAdd("b.txt", "main change");
		RevCommit mainTip = commit("main change");

		git.checkout().setName("feature").call();
		writeAndAdd("c.txt", "feature change");
		RevCommit featureTip = commit("feature change");

		git.checkout().setName(initialBranch).call();
		git.merge().include(featureTip).setMessage("merge feature").call();

		List<Map<String, Object>> commits = run("10", "0");
		List<String> parents = parentIds(commits.get(0));
		assertEquals(2, parents.size());
		assertTrue(parents.contains(mainTip.getName()));
		assertTrue(parents.contains(featureTip.getName()));
	}

	@Test
	void localBranch_refNameHasNoRefsHeadsPrefix() throws Exception {
		writeAndAdd("a.txt", "content");
		commit("init");
		String branch = git.getRepository().getBranch();

		List<Map<String, Object>> commits = run("10", "0");
		List<Map<String, String>> refs = refs(commits.get(0));
		assertTrue(refs.stream().anyMatch(r -> r.get("name").equals(branch) && r.get("type").equals("LOCAL_BRANCH")));
	}

	@Test
	void remoteBranch_refNameRetainsRemotePrefix() throws Exception {
		writeAndAdd("a.txt", "content");
		RevCommit c = commit("init");
		RefUpdate update = git.getRepository().updateRef("refs/remotes/origin/main");
		update.setNewObjectId(c.getId());
		update.update();

		List<Map<String, Object>> commits = run("10", "0");
		List<Map<String, String>> refs = refs(commits.get(0));
		assertTrue(refs.stream()
				.anyMatch(r -> r.get("name").equals("origin/main") && r.get("type").equals("REMOTE_BRANCH")));
	}

	@Test
	void lightweightTag_appearsInTagsAndRefs() throws Exception {
		writeAndAdd("a.txt", "content");
		commit("init");
		git.tag().setName("v1.0.0").setAnnotated(false).call();

		List<Map<String, Object>> commits = run("10", "0");
		@SuppressWarnings("unchecked")
		List<String> tags = (List<String>) commits.get(0).get("tags");
		assertEquals(List.of("v1.0.0"), tags);

		List<Map<String, String>> refs = refs(commits.get(0));
		assertTrue(refs.stream().anyMatch(r -> r.get("name").equals("v1.0.0") && r.get("type").equals("TAG")));
	}

	@Test
	void annotatedTag_resolvesToUnderlyingCommit() throws Exception {
		writeAndAdd("a.txt", "content");
		commit("init");
		git.tag().setName("v2.0.0").setAnnotated(true).setMessage("release").call();

		List<Map<String, Object>> commits = run("10", "0");
		List<Map<String, String>> refs = refs(commits.get(0));
		assertTrue(refs.stream().anyMatch(r -> r.get("name").equals("v2.0.0") && r.get("type").equals("TAG")));
	}

	@Test
	void multipleRefsOnSameCommit_allPresentAndSortedByName() throws Exception {
		writeAndAdd("a.txt", "content");
		RevCommit c = commit("init");
		git.tag().setName("v1.0.0").call();
		RefUpdate update = git.getRepository().updateRef("refs/remotes/origin/main");
		update.setNewObjectId(c.getId());
		update.update();

		List<Map<String, Object>> commits = run("10", "0");
		List<Map<String, String>> refs = refs(commits.get(0));
		// current branch + remote branch + tag
		assertEquals(3, refs.size());
		List<String> actualOrder = refs.stream().map(r -> r.get("name")).collect(Collectors.toList());
		List<String> sortedOrder = actualOrder.stream().sorted().collect(Collectors.toList());
		assertEquals(sortedOrder, actualOrder);
	}

	@Test
	void detachedHead_commitHasNoLocalBranchRef() throws Exception {
		// Detaching HEAD at c2 does not move the branch pointer off it, so c2
		// legitimately keeps its LOCAL_BRANCH ref. Only a commit made *while*
		// detached (c3) has no branch pointing at it - that's the real case to
		// verify.
		writeAndAdd("a.txt", "v1");
		commit("c1");
		writeAndAdd("a.txt", "v2");
		RevCommit c2 = commit("c2");
		git.checkout().setName(c2.getName()).call();
		writeAndAdd("a.txt", "v3");
		RevCommit c3 = commit("c3 while detached");

		List<Map<String, Object>> commits = run("10", "0");
		Map<String, Object> c3Details = commits.stream().filter(d -> d.get("commitId").equals(c3.getName()))
				.findFirst().orElseThrow();
		assertTrue(refs(c3Details).stream().noneMatch(r -> r.get("type").equals("LOCAL_BRANCH")));

		Map<String, Object> c2Details = commits.stream().filter(d -> d.get("commitId").equals(c2.getName()))
				.findFirst().orElseThrow();
		assertTrue(refs(c2Details).stream().anyMatch(r -> r.get("type").equals("LOCAL_BRANCH")));
	}

	@Test
	void existingFieldsUnchanged_shapeAndValues() throws Exception {
		writeAndAdd("a.txt", "content");
		RevCommit c = commit("hello world");

		List<Map<String, Object>> commits = run("10", "0");
		Map<String, Object> details = commits.get(0);
		assertEquals(c.getName(), details.get("commitId"));
		assertTrue(((String) details.get("commitMessage")).startsWith("hello world"));
		@SuppressWarnings("unchecked")
		Map<String, String> author = (Map<String, String>) details.get("author");
		assertEquals("Test", author.get("userId"));
		assertEquals("test@test.com", author.get("userEmail"));
		assertNotNull(details.get("date"));
	}

	@Test
	void pagination_unaffectedByNewFields() throws Exception {
		for (int i = 0; i < 5; i++) {
			writeAndAdd("a.txt", "v" + i);
			commit("commit " + i);
		}

		List<Map<String, Object>> page = run("2", "1");
		assertEquals(2, page.size());
		for (Map<String, Object> details : page) {
			assertNotNull(details.get("parentCommitIds"));
			assertNotNull(details.get("refs"));
		}
	}

	@Test
	void badInput_stillThrowsSemossPixelException() {
		reactor.keyValue.put(ReactorKeysEnum.PROJECT.getKey(), "");
		reactor.keyValue.put(ReactorKeysEnum.LIMIT.getKey(), "10");
		reactor.keyValue.put(ReactorKeysEnum.OFFSET.getKey(), "0");
		assertThrows(SemossPixelException.class, () -> reactor.execute());
	}
}
