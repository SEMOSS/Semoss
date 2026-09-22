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
package prerna.engine.impl.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import prerna.io.connector.ms.MicrosoftGraphDriveClient;

class AbstractMicrosoftGraphStorageEngineUnitTests {

	@TempDir
	Path directory;

	@Test
	void syncStorageToLocalPreservesNestedFiles() throws Exception {
		MicrosoftGraphDriveClient client = mock(MicrosoftGraphDriveClient.class);
		Map<String, Object> item = Map.of("id", "file-id");
		when(client.listFilesRecursively("remote")).thenReturn(Map.of("nested/file.txt", item));
		when(client.downloadBytes(item, "remote/nested/file.txt")).thenReturn(new byte[] { 1, 2, 3 });

		MicrosoftTeamsStorageEngine engine = engine(client);
		Path localRoot = directory.resolve("local");
		engine.syncStorageToLocal("remote", localRoot.toString());

		assertArrayEquals(new byte[] { 1, 2, 3 }, Files.readAllBytes(localRoot.resolve("nested/file.txt")));
	}

	@Test
	void syncStorageToLocalRejectsTraversalBeforeDownload() throws Exception {
		MicrosoftGraphDriveClient client = mock(MicrosoftGraphDriveClient.class);
		when(client.listFilesRecursively("remote"))
				.thenReturn(Map.of("../outside.txt", Map.of("id", "file-id")));

		MicrosoftTeamsStorageEngine engine = engine(client);
		engine.syncStorageToLocal("remote", directory.resolve("local").toString());

		assertFalse(Files.exists(directory.resolve("outside.txt")));
		verify(client, never()).downloadBytes(anyMap(), anyString());
	}

	@Test
	void syncStorageToLocalRejectsEscapingSymlinkBeforeDownload() throws Exception {
		Path localRoot = Files.createDirectory(directory.resolve("local"));
		Path outside = Files.createDirectory(directory.resolve("outside"));
		Files.createSymbolicLink(localRoot.resolve("link"), outside);

		MicrosoftGraphDriveClient client = mock(MicrosoftGraphDriveClient.class);
		when(client.listFilesRecursively("remote"))
				.thenReturn(Map.of("link/outside.txt", Map.of("id", "file-id")));

		MicrosoftTeamsStorageEngine engine = engine(client);
		engine.syncStorageToLocal("remote", localRoot.toString());

		assertFalse(Files.exists(outside.resolve("outside.txt")));
		verify(client, never()).downloadBytes(anyMap(), anyString());
	}

	@Test
	void copyFolderToLocalRejectsTraversalBeforeDownload() throws Exception {
		MicrosoftGraphDriveClient client = mock(MicrosoftGraphDriveClient.class);
		when(client.getItem("remote")).thenReturn(Map.of("folder", Map.of()));
		when(client.listFilesRecursively("remote"))
				.thenReturn(Map.of("../../outside.txt", Map.of("id", "file-id")));

		MicrosoftTeamsStorageEngine engine = engine(client);
		engine.copyToLocal("remote", directory.resolve("local").toString(), null);

		assertFalse(Files.exists(directory.resolve("outside.txt")));
		verify(client, never()).downloadBytes(anyMap(), anyString());
	}

	@Test
	void copyVersionToLocalRejectsTraversalBeforeDownload() throws Exception {
		MicrosoftGraphDriveClient client = mock(MicrosoftGraphDriveClient.class);
		Map<String, Object> item = new HashMap<>();
		item.put("id", "file-id");
		item.put("name", "../outside.txt");
		when(client.getItem("remote/file.txt")).thenReturn(item);

		MicrosoftTeamsStorageEngine engine = engine(client);
		assertThrows(IllegalArgumentException.class,
				() -> engine.copyToLocal("remote/file.txt", directory.resolve("local").toString(), "v1"));

		assertFalse(Files.exists(directory.resolve("outside.txt")));
		verify(client, never()).downloadVersionBytes(eq("file-id"), eq("v1"));
	}

	private static MicrosoftTeamsStorageEngine engine(MicrosoftGraphDriveClient client) {
		MicrosoftTeamsStorageEngine engine = new MicrosoftTeamsStorageEngine();
		engine.driveClient = client;
		return engine;
	}
}
