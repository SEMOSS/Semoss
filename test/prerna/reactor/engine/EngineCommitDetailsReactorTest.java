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
package prerna.reactor.engine;

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

import org.eclipse.jgit.api.Git;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.om.Insight;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineUtility;
import prerna.util.Utility;

/**
 * Exercises {@link EngineCommitDetailsReactor} against a real temporary JGit
 * repository with {@code Utility}/{@code EngineUtility}/
 * {@code SecurityEngineUtils} statically mocked to point at it.
 */
class EngineCommitDetailsReactorTest {

	private static final String ENGINE_ID = "testEngineId";

	@TempDir
	Path tempDir;

	private Git git;
	private EngineCommitDetailsReactor reactor;
	private MockedStatic<Utility> utilityMock;
	private MockedStatic<EngineUtility> engineUtilityMock;
	private MockedStatic<SecurityEngineUtils> securityMock;

	@BeforeEach
	void setUp() throws Exception {
		git = Git.init().setDirectory(tempDir.toFile()).call();

		IEngine engine = mock(IEngine.class);
		when(engine.getEngineName()).thenReturn("testEngineName");
		when(engine.getCatalogType()).thenReturn(IEngine.CATALOG_TYPE.DATABASE);

		utilityMock = mockStatic(Utility.class);
		utilityMock.when(() -> Utility.getEngine(ENGINE_ID)).thenReturn(engine);
		utilityMock.when(Utility::getBaseFolder).thenReturn(tempDir.toString());

		engineUtilityMock = mockStatic(EngineUtility.class);
		engineUtilityMock.when(() -> EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.DATABASE,
				ENGINE_ID, "testEngineName")).thenReturn(tempDir.toString());

		securityMock = mockStatic(SecurityEngineUtils.class);
		securityMock.when(() -> SecurityEngineUtils.userCanEditEngine(any(), eq(ENGINE_ID))).thenReturn(true);

		reactor = new EngineCommitDetailsReactor();
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

	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> run(String limit, String offset) {
		reactor.keyValue.put(ReactorKeysEnum.ENGINE.getKey(), ENGINE_ID);
		reactor.keyValue.put(ReactorKeysEnum.LIMIT.getKey(), limit);
		reactor.keyValue.put(ReactorKeysEnum.OFFSET.getKey(), offset);
		NounMetadata result = reactor.execute();
		return (List<Map<String, Object>>) result.getValue();
	}

	@Test
	void unbornHead_returnsEmptyList() {
		List<Map<String, Object>> commits = run("10", "0");
		assertTrue(commits.isEmpty());
	}

	@Test
	void singleCommit_hasExpectedFields() throws Exception {
		writeAndAdd("a.txt", "content");
		git.commit().setMessage("init").setAuthor("Tester", "tester@example.com").call();

		List<Map<String, Object>> commits = run("10", "0");
		assertEquals(1, commits.size());

		Map<String, Object> commit = commits.get(0);
		assertNotNull(commit.get("commitId"));
		assertNotNull(commit.get("author"));
		assertNotNull(commit.get("date"));
		assertNotNull(commit.get("commitMessage"));
		assertEquals("init\n", commit.get("commitMessage"));
	}

	@Test
	void pagination_limitRestrictsResults() throws Exception {
		for (int i = 1; i <= 5; i++) {
			writeAndAdd("f" + i + ".txt", "v" + i);
			git.commit().setMessage("commit " + i).setAuthor("Test", "test@test.com").call();
		}

		List<Map<String, Object>> page = run("2", "0");
		assertEquals(2, page.size());
	}

	@Test
	void pagination_offsetSkipsResults() throws Exception {
		for (int i = 1; i <= 3; i++) {
			writeAndAdd("f" + i + ".txt", "v" + i);
			git.commit().setMessage("commit " + i).setAuthor("Test", "test@test.com").call();
		}

		List<Map<String, Object>> allCommits = run("10", "0");
		List<Map<String, Object>> skipped = run("10", "1");

		assertEquals(3, allCommits.size());
		assertEquals(2, skipped.size());
		assertEquals(allCommits.get(1).get("commitId"), skipped.get(0).get("commitId"));
	}

	@Test
	void invalidLimit_throwsException() {
		SemossPixelException ex = assertThrows(SemossPixelException.class, () -> run("notANumber", "0"));
		assertTrue(ex.getMessage().contains("valid integer"));
	}

	@Test
	void zeroLimit_throwsException() {
		SemossPixelException ex = assertThrows(SemossPixelException.class, () -> run("0", "0"));
		assertTrue(ex.getMessage().contains(">= 1"));
	}

	@Test
	void negativeOffset_throwsException() {
		SemossPixelException ex = assertThrows(SemossPixelException.class, () -> run("10", "-1"));
		assertTrue(ex.getMessage().contains(">= 0"));
	}
}
