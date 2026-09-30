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
package prerna.reactor.agent.run;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockStatic;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import prerna.cluster.util.ClusterUtil;
import prerna.util.Constants;
import prerna.util.Utility;

class HumanDelegationFileSecurityUnitTests {

	@TempDir
	Path baseFolder;

	@Test
	void listsOnlyRegularFilesContainedInTheDelegationDirectory() throws Exception {
		Path directory = delegationDirectory("room-1", "child-1");
		Path artifact = Files.writeString(directory.resolve("answer.txt"), "answer");
		Path outside = Files.writeString(baseFolder.resolve("outside.txt"), "outside");
		Files.createSymbolicLink(directory.resolve("outside-link.txt"), outside);
		Files.createSymbolicLink(directory.resolve("dangling-link.txt"), baseFolder.resolve("missing.txt"));

		try (MockedStatic<Utility> utility = baseFolder()) {
			List<Map<String, Object>> files = HumanDelegationService.returnedFiles("room-1", "child-1");

			assertEquals(1, files.size());
			assertEquals("delegations/child-1/answer.txt", files.get(0).get("path"));
			assertEquals(Files.size(artifact), files.get(0).get("size"));
		}
	}

	@Test
	void rejectsTraversalInPersistedChildRunIdBeforeListing() {
		try (MockedStatic<Utility> utility = baseFolder()) {
			assertThrows(IllegalArgumentException.class,
					() -> HumanDelegationService.returnedFiles("room-1", "../outside"));
		}
	}

	@Test
	void rejectsDelegationsDirectorySymlinkThatEscapesTheRoom() throws Exception {
		Path room = Files.createDirectories(baseFolder.resolve(Constants.ROOM_FOLDER).resolve("room-1"));
		Path outside = Files.createDirectories(baseFolder.resolve("outside").resolve("child-1"));
		Files.createSymbolicLink(room.resolve(HumanDelegationService.FILES_FOLDER), outside.getParent());

		try (MockedStatic<Utility> utility = baseFolder()) {
			assertThrows(IllegalArgumentException.class,
					() -> HumanDelegationService.returnedFiles("room-1", "child-1"));
		}
	}

	@Test
	void copiesFilesOnlyToTheValidatedDelegationDirectory() throws Exception {
		Path source = Files.writeString(baseFolder.resolve("source.txt"), "answer");
		try (MockedStatic<Utility> utility = baseFolder();
				MockedStatic<ClusterUtil> cluster = mockStatic(ClusterUtil.class)) {
			List<String> copied = HumanDelegationService.copyFiles(List.of(source), "room-1",
					HumanDelegationService.FILES_FOLDER, "child-1");

			assertEquals(List.of("delegations/child-1/source.txt"), copied);
			assertTrue(Files.isRegularFile(baseFolder.resolve(Constants.ROOM_FOLDER).resolve("room-1")
					.resolve("delegations").resolve("child-1").resolve("source.txt")));
		}
	}

	@Test
	void rejectsTraversalInPersistedChildRunIdBeforeCopying() throws Exception {
		Path source = Files.writeString(baseFolder.resolve("source.txt"), "answer");
		try (MockedStatic<Utility> utility = baseFolder();
				MockedStatic<ClusterUtil> cluster = mockStatic(ClusterUtil.class)) {
			assertThrows(IllegalArgumentException.class, () -> HumanDelegationService.copyFiles(List.of(source),
					"room-1", HumanDelegationService.FILES_FOLDER, "../outside"));
		}
	}

	@Test
	void rejectsEscapingTargetSymlinkBeforeRecursiveDeletion() throws Exception {
		Path source = Files.writeString(baseFolder.resolve("source.txt"), "answer");
		Path room = Files.createDirectories(baseFolder.resolve(Constants.ROOM_FOLDER).resolve("room-1")
				.resolve(HumanDelegationService.FILES_FOLDER));
		Path outside = Files.createDirectories(baseFolder.resolve("outside"));
		Path sentinel = Files.writeString(outside.resolve("keep.txt"), "keep");
		Files.createSymbolicLink(room.resolve("child-1"), outside);

		try (MockedStatic<Utility> utility = baseFolder();
				MockedStatic<ClusterUtil> cluster = mockStatic(ClusterUtil.class)) {
			assertThrows(IllegalArgumentException.class, () -> HumanDelegationService.copyFiles(List.of(source),
					"room-1", HumanDelegationService.FILES_FOLDER, "child-1"));
			assertTrue(Files.exists(sentinel));
			assertFalse(Files.exists(outside.resolve("source.txt")));
		}
	}

	private Path delegationDirectory(String roomId, String childRunId) throws Exception {
		return Files.createDirectories(baseFolder.resolve(Constants.ROOM_FOLDER).resolve(roomId)
				.resolve(HumanDelegationService.FILES_FOLDER).resolve(childRunId));
	}

	private MockedStatic<Utility> baseFolder() {
		MockedStatic<Utility> utility = mockStatic(Utility.class);
		utility.when(Utility::getBaseFolder).thenReturn(baseFolder.toString());
		return utility;
	}
}
