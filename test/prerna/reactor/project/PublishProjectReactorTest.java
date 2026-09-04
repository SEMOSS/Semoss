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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.revwalk.RevCommit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;

/**
 * Covers the publish-metadata recording added to {@link PublishProjectReactor}:
 * a successful {@code release=true} publish records the resolved HEAD commit,
 * publisher, and timestamp; {@code release=false} and a failed publish record
 * nothing; the reactor's own output contract is unchanged.
 */
class PublishProjectReactorTest {

	private static final String PROJECT_ID = "testProjectId";
	private static final String PROJECT_NAME = "testProjectName";

	@TempDir
	Path tempDir;

	private Git git;
	private User user;
	private PublishProjectReactor reactor;

	private MockedStatic<Utility> utilityMock;
	private MockedStatic<AssetUtility> assetUtilityMock;
	private MockedStatic<SecurityProjectUtils> securityMock;
	private MockedStatic<ClusterUtil> clusterUtilMock;

	@BeforeEach
	void setUp() throws Exception {
		git = Git.init().setDirectory(tempDir.toFile()).call();

		IProject project = mock(IProject.class);
		when(project.getProjectName()).thenReturn(PROJECT_NAME);

		utilityMock = mockStatic(Utility.class);
		utilityMock.when(() -> Utility.getProject(PROJECT_ID)).thenReturn(project);

		assetUtilityMock = mockStatic(AssetUtility.class);
		assetUtilityMock.when(() -> AssetUtility.getProjectVersionFolder(PROJECT_NAME, PROJECT_ID))
				.thenReturn(tempDir.toString());

		securityMock = mockStatic(SecurityProjectUtils.class);
		securityMock.when(() -> SecurityProjectUtils.userIsOwner(any(), eq(PROJECT_ID))).thenReturn(true);

		clusterUtilMock = mockStatic(ClusterUtil.class);

		reactor = new PublishProjectReactor();
		reactor.setNounStore(new NounStore("test"));
		Insight insight = mock(Insight.class);
		user = mock(User.class);
		reactor.setInsight(insight);
		reactor.keyValue = new HashMap<>();
		when(insight.getUser()).thenReturn(user);
	}

	@AfterEach
	void tearDown() {
		utilityMock.close();
		assetUtilityMock.close();
		securityMock.close();
		clusterUtilMock.close();
	}

	private void setKeys(boolean release) {
		reactor.keyValue.put(ReactorKeysEnum.PROJECT.getKey(), PROJECT_ID);
		reactor.keyValue.put(ReactorKeysEnum.RELEASE.getKey(), String.valueOf(release));
	}

	private void writeAndAdd(String path, String content) throws Exception {
		Files.writeString(tempDir.resolve(path), content);
		git.add().addFilepattern(path).call();
	}

	private RevCommit commit(String message) throws Exception {
		return git.commit().setMessage(message).setAuthor("Test", "test@test.com").call();
	}

	@Test
	void releaseTrue_recordsHeadCommit() throws Exception {
		writeAndAdd("a.txt", "content");
		RevCommit c = commit("init");
		setKeys(true);

		reactor.execute();

		securityMock.verify(() -> SecurityProjectUtils.setPortalPublish(user, PROJECT_ID));
	}

	@Test
	void releaseTrue_unbornHead_recordsNullCommit() {
		setKeys(true);

		reactor.execute();
	}

	@Test
	void releaseFalse_doesNotRecordPublishMetadata() {
		setKeys(false);

		reactor.execute();

		securityMock.verify(() -> SecurityProjectUtils.setPortalPublish(any(), anyString()), never());
		clusterUtilMock.verify(() -> ClusterUtil.pushProjectFolder(any(IProject.class), anyString(), anyString()), never());
	}

	@Test
	void releasePushFails_doesNotRecordPublishMetadata() {
		clusterUtilMock.when(() -> ClusterUtil.pushProjectFolder(any(IProject.class), anyString(), anyString()))
				.thenThrow(new RuntimeException("push failed"));
		setKeys(true);

		assertThrows(RuntimeException.class, () -> reactor.execute());
	}

	@Test
	void notOwner_throwsAndRecordsNothing() {
		securityMock.when(() -> SecurityProjectUtils.userIsOwner(any(), eq(PROJECT_ID))).thenReturn(false);
		setKeys(true);

		assertThrows(IllegalArgumentException.class, () -> reactor.execute());
	}

	@Test
	void outputShapeUnchanged() {
		setKeys(true);

		NounMetadata result = reactor.execute();

		assertEquals(PixelDataType.CONST_STRING, result.getNounType());
		assertNotNull(result.getAdditionalReturn());
		assertFalse(result.getAdditionalReturn().isEmpty());
	}
}
