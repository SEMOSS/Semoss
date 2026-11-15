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
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import prerna.reactor.AbstractReactor;
import prerna.reactor.engine.fs.CopyEngineAssetReactor;
import prerna.reactor.engine.fs.NewEngineAssetsDirectoryReactor;
import prerna.reactor.engine.fs.NewEngineAssetsFileReactor;
import prerna.reactor.engine.fs.RenameEngineAssetReactor;
import prerna.reactor.engine.fs.SaveEngineAssetsBase64Reactor;
import prerna.reactor.engine.fs.SaveEngineAssetsReactor;
import prerna.reactor.engine.fs.UnzipEngineAssetFileReactor;
import prerna.reactor.insights.fs.CopyInsightAssetReactor;
import prerna.reactor.insights.fs.NewInsightAssetsDirectoryReactor;
import prerna.reactor.insights.fs.NewInsightAssetsFileReactor;
import prerna.reactor.insights.fs.RenameInsightAssetReactor;
import prerna.reactor.insights.fs.SaveInsightAssetsBase64Reactor;
import prerna.reactor.insights.fs.SaveInsightAssetsReactor;
import prerna.reactor.insights.fs.UnzipInsightAssetFileReactor;
import prerna.reactor.project.fs.CopyAppAssetReactor;
import prerna.reactor.project.fs.NewAppAssetsDirectoryReactor;
import prerna.reactor.project.fs.NewAppAssetsFileReactor;
import prerna.reactor.project.fs.RenameAppAssetReactor;
import prerna.reactor.project.fs.SaveAppAssetsBase64Reactor;
import prerna.reactor.project.fs.SaveAppAssetsReactor;
import prerna.reactor.project.fs.UnzipAppAssetFileReactor;
import prerna.reactor.room.fs.BrowseRoomAssetsReactor;
import prerna.reactor.room.fs.CopyRoomAssetReactor;
import prerna.reactor.room.fs.DeleteRoomAssetsReactor;
import prerna.reactor.room.fs.DownloadRoomAssetReactor;
import prerna.reactor.room.fs.GetRoomAssetsBase64Reactor;
import prerna.reactor.room.fs.GetRoomAssetsReactor;
import prerna.reactor.room.fs.NewRoomAssetsDirectoryReactor;
import prerna.reactor.room.fs.NewRoomAssetsFileReactor;
import prerna.reactor.room.fs.RenameRoomAssetReactor;
import prerna.reactor.room.fs.SaveRoomAssetsBase64Reactor;
import prerna.reactor.room.fs.SaveRoomAssetsReactor;
import prerna.reactor.room.fs.SearchRoomAssetsReactor;
import prerna.reactor.room.fs.UnzipRoomAssetFileReactor;
import prerna.reactor.user.fs.CopyUserAssetReactor;
import prerna.reactor.user.fs.NewUserAssetsDirectoryReactor;
import prerna.reactor.user.fs.NewUserAssetsFileReactor;
import prerna.reactor.user.fs.RenameUserAssetReactor;
import prerna.reactor.user.fs.SaveUserAssetsBase64Reactor;
import prerna.reactor.user.fs.SaveUserAssetsReactor;
import prerna.reactor.user.fs.UnzipUserAssetFileReactor;
import prerna.util.FileSystemUtil.SavedAsset;

class FileSystemUtilUnitTests {

	@TempDir
	Path tempDir;

	@Test
	void copyAssetKeepsAnExistingDestinationAndNumbersTheCopyWithoutOverride() throws Exception {
		Files.writeString(tempDir.resolve("source.txt"), "new content");
		Files.writeString(tempDir.resolve("destination.txt"), "old content");

		SavedAsset saved = FileSystemUtil.copyAsset(tempDir.toString(), "source.txt", "destination.txt", false);

		assertEquals("destination (2).txt", saved.savedPath());
		assertTrue(saved.renamed());
		assertEquals("old content", Files.readString(tempDir.resolve("destination.txt")));
		assertEquals("new content", Files.readString(tempDir.resolve("destination (2).txt")));
	}

	@Test
	void copyAssetOntoItsOwnPathMakesANumberedDuplicateWithoutOverride() throws Exception {
		Path folder = Files.createDirectories(tempDir.resolve("docs"));
		Files.writeString(folder.resolve("notes.md"), "content");

		SavedAsset file = FileSystemUtil.copyAsset(tempDir.toString(), "/docs/notes.md", "docs/notes.md", false);
		SavedAsset directory = FileSystemUtil.copyAsset(tempDir.toString(), "docs", "docs", false);

		assertEquals("docs/notes (2).md", file.savedPath());
		assertEquals("docs (2)", directory.savedPath());
		assertEquals("content", Files.readString(tempDir.resolve("docs (2)/notes.md")));
	}

	@Test
	void copyAssetStillRefusesToCopyADirectoryInsideItself() throws Exception {
		Files.createDirectories(tempDir.resolve("docs"));

		assertThrows(IllegalArgumentException.class,
				() -> FileSystemUtil.copyAsset(tempDir.toString(), "docs", "docs/inner", false));
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
		assertOverrideSchema(new CopyAppAssetReactor(), false);
		assertOverrideSchema(new CopyInsightAssetReactor(), false);
		assertOverrideSchema(new CopyEngineAssetReactor(), false);
		assertOverrideSchema(new CopyUserAssetReactor(), false);
		assertOverrideSchema(new CopyRoomAssetReactor(), false);
	}

	@Test
	void saveReactorsExposeAnOptionalBooleanOverrideDefaultingToTrue() {
		for (AbstractReactor reactor : List.of(new SaveAppAssetsReactor(), new SaveAppAssetsBase64Reactor(),
				new SaveEngineAssetsReactor(), new SaveEngineAssetsBase64Reactor(), new SaveInsightAssetsReactor(),
				new SaveInsightAssetsBase64Reactor(), new SaveUserAssetsReactor(), new SaveUserAssetsBase64Reactor(),
				new SaveRoomAssetsReactor(), new SaveRoomAssetsBase64Reactor())) {
			assertOverrideSchema(reactor, true);
		}
	}

	@Test
	void createRenameAndUnzipReactorsExposeAnOptionalBooleanOverrideDefaultingToFalse() {
		for (AbstractReactor reactor : List.of(new NewAppAssetsFileReactor(), new NewAppAssetsDirectoryReactor(),
				new RenameAppAssetReactor(), new UnzipAppAssetFileReactor(), new NewEngineAssetsFileReactor(),
				new NewEngineAssetsDirectoryReactor(), new RenameEngineAssetReactor(),
				new UnzipEngineAssetFileReactor(), new NewInsightAssetsFileReactor(),
				new NewInsightAssetsDirectoryReactor(), new RenameInsightAssetReactor(),
				new UnzipInsightAssetFileReactor(), new NewUserAssetsFileReactor(), new NewUserAssetsDirectoryReactor(),
				new RenameUserAssetReactor(), new UnzipUserAssetFileReactor(), new NewRoomAssetsFileReactor(),
				new NewRoomAssetsDirectoryReactor(), new RenameRoomAssetReactor(), new UnzipRoomAssetFileReactor())) {
			assertOverrideSchema(reactor, false);
		}
	}

	@Test
	void roomReactorsRequireTheRoomId() {
		for (AbstractReactor reactor : List.of(new BrowseRoomAssetsReactor(), new SearchRoomAssetsReactor(),
				new GetRoomAssetsReactor(), new GetRoomAssetsBase64Reactor(), new DownloadRoomAssetReactor(),
				new SaveRoomAssetsReactor(), new SaveRoomAssetsBase64Reactor(), new NewRoomAssetsFileReactor(),
				new NewRoomAssetsDirectoryReactor(), new CopyRoomAssetReactor(), new RenameRoomAssetReactor(),
				new DeleteRoomAssetsReactor(), new UnzipRoomAssetFileReactor())) {
			JSONObject inputSchema = reactor.asMcpTool().getJSONObject("inputSchema");
			assertTrue(inputSchema.getJSONArray("required").toList().contains("roomId"),
					reactor.getClass().getSimpleName());
			assertTrue(inputSchema.getJSONObject("properties").getJSONObject("roomId").getString("description")
					.contains("must own the room"), reactor.getClass().getSimpleName());
		}
	}

	private static void assertOverrideSchema(AbstractReactor reactor, boolean expectedDefault) {
		JSONObject inputSchema = reactor.asMcpTool().getJSONObject("inputSchema");
		JSONObject override = inputSchema.getJSONObject("properties").getJSONObject("override");
		JSONArray required = inputSchema.getJSONArray("required");
		String name = reactor.getClass().getSimpleName();

		assertEquals("boolean", override.getString("type"), name);
		assertEquals(expectedDefault, override.getBoolean("default"), name);
		assertFalse(required.toList().contains("override"), name);
	}
}
