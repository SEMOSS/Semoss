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
package prerna.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import prerna.reactor.AbstractReactor;
import prerna.reactor.engine.fs.CopyEngineAssetReactor;
import prerna.reactor.insights.fs.CopyInsightAssetReactor;
import prerna.reactor.project.fs.CopyAppAssetReactor;

class FileSystemUtilUnitTests {

	@TempDir
	Path tempDir;

	@Test
	void copyAssetDoesNotReplaceAnExistingDestinationByDefault() throws Exception {
		Files.writeString(tempDir.resolve("source.txt"), "new content");
		Files.writeString(tempDir.resolve("destination.txt"), "old content");

		assertThrows(IllegalArgumentException.class,
				() -> FileSystemUtil.copyAsset(tempDir.toString(), "source.txt", "destination.txt"));
		assertEquals("old content", Files.readString(tempDir.resolve("destination.txt")));
	}

	@Test
	void copyAssetReplacesAnExistingFileWhenOverrideIsTrue() throws Exception {
		Files.writeString(tempDir.resolve("source.txt"), "new content");
		Files.writeString(tempDir.resolve("destination.txt"), "old content");

		FileSystemUtil.copyAsset(tempDir.toString(), "source.txt", "destination.txt", true);

		assertEquals("new content", Files.readString(tempDir.resolve("destination.txt")));
	}

	@Test
	void copyAssetReplacesAnExistingDirectoryWhenOverrideIsTrue() throws Exception {
		Path source = Files.createDirectories(tempDir.resolve("source"));
		Files.writeString(source.resolve("new.txt"), "new content");
		Path destination = Files.createDirectories(tempDir.resolve("destination"));
		Files.writeString(destination.resolve("stale.txt"), "stale content");

		FileSystemUtil.copyAsset(tempDir.toString(), "source", "destination", true);

		assertTrue(Files.exists(destination.resolve("new.txt")));
		assertFalse(Files.exists(destination.resolve("stale.txt")));
	}

	@Test
	void copyAssetDoesNotDeleteTheSourceWhenOverrideTargetsTheSamePath() throws Exception {
		Path source = tempDir.resolve("source.txt");
		Files.writeString(source, "content");

		assertThrows(IllegalArgumentException.class,
				() -> FileSystemUtil.copyAsset(tempDir.toString(), "source.txt", "source.txt", true));
		assertEquals("content", Files.readString(source));
	}

	@Test
	void copyAssetDoesNotDeleteAnAncestorContainingTheSource() throws Exception {
		Path destination = Files.createDirectories(tempDir.resolve("destination"));
		Path source = destination.resolve("source.txt");
		Files.writeString(source, "content");

		assertThrows(IllegalArgumentException.class,
				() -> FileSystemUtil.copyAsset(tempDir.toString(), "destination/source.txt", "destination", true));
		assertEquals("content", Files.readString(source));
	}

	@Test
	void copyReactorsExposeAnOptionalBooleanOverrideDefaultingToFalse() {
		assertOverrideSchema(new CopyAppAssetReactor());
		assertOverrideSchema(new CopyInsightAssetReactor());
		assertOverrideSchema(new CopyEngineAssetReactor());
	}

	private static void assertOverrideSchema(AbstractReactor reactor) {
		JSONObject inputSchema = reactor.asMcpTool().getJSONObject("inputSchema");
		JSONObject override = inputSchema.getJSONObject("properties").getJSONObject("override");
		JSONArray required = inputSchema.getJSONArray("required");

		assertEquals("boolean", override.getString("type"));
		assertFalse(override.getBoolean("default"));
		assertFalse(required.toList().contains("override"));
	}
}
