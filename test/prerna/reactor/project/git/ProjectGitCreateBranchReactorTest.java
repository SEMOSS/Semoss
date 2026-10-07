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
package prerna.reactor.project.git;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.jgit.api.Git;
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
 * Exercises {@link ProjectGitCreateBranchReactor} end-to-end against a real
 * temporary JGit repository, covering the reactor-level validation that has no
 * equivalent in {@code GitBranchUtils} (duplicate-branch rejection,
 * unresolvable start point) on top of the branch-name-validity check already
 * covered at the utility layer. The body under test lives in
 * {@code AbstractGitCreateBranchReactor}, so this also covers the engine flavor
 * of the same operation; only the project binding is project-specific.
 */
class ProjectGitCreateBranchReactorTest {

	private static final String PROJECT_ID = "testProjectId";

	@TempDir
	Path tempDir;

	private Git git;
	private ProjectGitCreateBranchReactor reactor;
	private MockedStatic<Utility> utilityMock;
	private MockedStatic<EngineUtility> engineUtilityMock;
	private MockedStatic<SecurityProjectUtils> securityMock;

	@BeforeEach
	void setUp() throws Exception {
		git = Git.init().setDirectory(tempDir.toFile()).call();
		Files.writeString(tempDir.resolve("a.txt"), "content");
		git.add().addFilepattern("a.txt").call();
		git.commit().setMessage("init").setAuthor("Test", "test@test.com").call();

		IProject project = mock(IProject.class);
		when(project.getEngineName()).thenReturn("testProjectName");

		utilityMock = mockStatic(Utility.class);
		utilityMock.when(() -> Utility.getProject(PROJECT_ID)).thenReturn(project);
		utilityMock.when(Utility::getBaseFolder).thenReturn(tempDir.toString());

		engineUtilityMock = mockStatic(EngineUtility.class);
		engineUtilityMock.when(() -> EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.PROJECT,
				PROJECT_ID, "testProjectName")).thenReturn(tempDir.toString());

		securityMock = mockStatic(SecurityProjectUtils.class);
		securityMock.when(() -> SecurityProjectUtils.userCanEditProject(any(), eq(PROJECT_ID))).thenReturn(true);

		reactor = new ProjectGitCreateBranchReactor();
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

	private void setKeys(String branch, String startPoint) {
		reactor.keyValue.put(ReactorKeysEnum.PROJECT.getKey(), PROJECT_ID);
		reactor.keyValue.put("branch", branch);
		if (startPoint != null) {
			reactor.keyValue.put("startPoint", startPoint);
		}
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> run(String branch, String startPoint) {
		setKeys(branch, startPoint);
		NounMetadata result = reactor.execute();
		return (Map<String, Object>) result.getValue();
	}

	@Test
	void createsAndChecksOutNewBranch() throws Exception {
		Map<String, Object> result = run("feature/example", null);

		assertEquals("feature/example", result.get("branch"));
		assertEquals("feature/example", git.getRepository().getBranch());
		assertEquals(git.getRepository().resolve("HEAD").getName(), result.get("headCommitId"));
	}

	@Test
	void rejectsDuplicateBranchName() throws Exception {
		git.branchCreate().setName("existing").call();

		SemossPixelException ex = assertThrows(SemossPixelException.class, () -> run("existing", null));
		assertEquals(true, ex.getMessage().contains("already exists"));
	}

	@Test
	void rejectsInvalidBranchName() {
		SemossPixelException ex = assertThrows(SemossPixelException.class, () -> run("has space", null));
		assertEquals(true, ex.getMessage().contains("not a valid branch name"));
	}

	@Test
	void rejectsUnresolvableStartPoint() {
		SemossPixelException ex = assertThrows(SemossPixelException.class,
				() -> run("feature/example", "does-not-exist"));
		assertEquals(true, ex.getMessage().contains("Could not resolve start point"));
	}
}
