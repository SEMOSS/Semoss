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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import prerna.reactor.room.fs.SaveRoomAssetsReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.FileSystemUtil.SavedAsset;

class FileSystemUtilCopyNumberTest {

	@TempDir
	Path tempDir;

	@Test
	void numbersATakenNameTheWayDesktopDownloadsDo() throws Exception {
		Files.writeString(tempDir.resolve("report.pdf"), "1");
		assertEquals("report (2).pdf", next("report.pdf", false));

		Files.writeString(tempDir.resolve("report (2).pdf"), "2");
		assertEquals("report (3).pdf", next("report.pdf", false));
		assertEquals("report (3).pdf", next("report (2).pdf", false));
	}

	@Test
	void keepsCompoundExtensionsLastAndNumbersDotfilesAndDirectoriesAtTheEnd() throws Exception {
		Files.writeString(tempDir.resolve("backup.tar.gz"), "x");
		Files.writeString(tempDir.resolve(".env"), "x");
		Files.writeString(tempDir.resolve("Makefile"), "x");
		Files.createDirectories(tempDir.resolve("v1.2"));

		assertEquals("backup (2).tar.gz", next("backup.tar.gz", false));
		assertEquals(".env (2)", next(".env", false));
		assertEquals("Makefile (2)", next("Makefile", false));
		assertEquals("v1.2 (2)", next("v1.2", true));
	}

	@Test
	void leavesAFreeNameAsItIs() {
		assertEquals("fresh.txt", next("fresh.txt", false));
	}

	@Test
	void claimingCreatesTheEntrySoTheNextClaimMovesOn() throws Exception {
		File first = FileSystemUtil.reserveAvailableFile(tempDir.resolve("a/notes.md").toFile(), false);
		File second = FileSystemUtil.reserveAvailableFile(tempDir.resolve("a/notes.md").toFile(), false);
		File folder = FileSystemUtil.reserveAvailableFile(tempDir.resolve("a/notes.md").toFile(), true);

		assertEquals("notes.md", first.getName());
		assertEquals("notes (2).md", second.getName());
		assertEquals("notes.md (2)", folder.getName());
		assertTrue(second.isFile());
		assertTrue(folder.isDirectory());
	}

	@Test
	void saveReplacesWithOverrideAndKeepsTheExistingFileWithout() throws Exception {
		Files.createDirectories(tempDir.resolve("docs"));
		Files.writeString(tempDir.resolve("docs/notes.md"), "old");

		List<SavedAsset> kept = FileSystemUtil.saveAssetFiles(tempDir.toString(), List.of("/docs/notes.md"),
				List.of("new"), false);
		assertEquals(List.of(new SavedAsset("docs/notes.md", "docs/notes (2).md")), kept);
		assertEquals("old", Files.readString(tempDir.resolve("docs/notes.md")));
		assertEquals("new", Files.readString(tempDir.resolve("docs/notes (2).md")));

		List<SavedAsset> replaced = FileSystemUtil.saveAssetFiles(tempDir.toString(), List.of("docs/notes.md"),
				List.of("newer"), true);
		assertFalse(replaced.get(0).renamed());
		assertEquals("newer", Files.readString(tempDir.resolve("docs/notes.md")));
	}

	@Test
	void saveTheSameNameTwiceInOneCallKeepsBothWithoutOverride() throws Exception {
		List<SavedAsset> saved = FileSystemUtil.saveAssetFiles(tempDir.toString(), List.of("a.txt", "a.txt"),
				List.of("first", "second"), false);

		assertEquals("a.txt", saved.get(0).savedPath());
		assertEquals("a (2).txt", saved.get(1).savedPath());
		assertEquals("first", Files.readString(tempDir.resolve("a.txt")));
		assertEquals("second", Files.readString(tempDir.resolve("a (2).txt")));
	}

	@Test
	void saveBase64WritesDecodedBytesBesideAnExistingFileWithoutOverride() throws Exception {
		byte[] bytes = new byte[] { 0, 1, 2, (byte) 0xFF };
		Files.write(tempDir.resolve("image.png"), new byte[] { 9 });

		List<SavedAsset> saved = FileSystemUtil.saveAssetFilesBase64(tempDir.toString(), List.of("image.png"),
				List.of(Base64.getEncoder().encodeToString(bytes)), true, false);

		assertEquals("image (2).png", saved.get(0).savedPath());
		assertArrayEquals(bytes, Files.readAllBytes(tempDir.resolve("image (2).png")));
		assertArrayEquals(new byte[] { 9 }, Files.readAllBytes(tempDir.resolve("image.png")));
	}

	@Test
	void saveBase64LeavesNoEmptyFileBehindWhenTheContentIsNotBase64() {
		assertThrows(IllegalArgumentException.class, () -> FileSystemUtil.saveAssetFilesBase64(tempDir.toString(),
				List.of("bad.bin"), List.of("not base64!"), true, false));
		assertFalse(Files.exists(tempDir.resolve("bad.bin")));
	}

	@Test
	void newFileNoLongerWipesAnExistingFileWithoutOverride() throws Exception {
		Files.writeString(tempDir.resolve("notebook.ipynb"), "my work");

		SavedAsset kept = FileSystemUtil.createNewAssetFile(tempDir.toString(), "notebook.ipynb", false);
		assertEquals("notebook (2).ipynb", kept.savedPath());
		assertEquals("my work", Files.readString(tempDir.resolve("notebook.ipynb")));
		assertTrue(Files.readString(tempDir.resolve("notebook (2).ipynb")).contains("\"nbformat\": 4"));

		FileSystemUtil.createNewAssetFile(tempDir.toString(), "notebook.ipynb", true);
		assertTrue(Files.readString(tempDir.resolve("notebook.ipynb")).contains("\"nbformat\": 4"));
	}

	@Test
	void newDirectoryUsesTheExistingOneWithOverrideAndNumbersOneWithout() throws Exception {
		Path data = Files.createDirectories(tempDir.resolve("data"));
		Files.writeString(data.resolve("keep.csv"), "x");

		SavedAsset reused = FileSystemUtil.createNewAssetDirectory(tempDir.toString(), "data/", true);
		assertEquals("data", reused.savedPath());
		assertFalse(Files.exists(data.resolve("placeholder.txt")));
		assertTrue(Files.exists(data.resolve("keep.csv")));

		SavedAsset numbered = FileSystemUtil.createNewAssetDirectory(tempDir.toString(), "data", false);
		assertEquals("data (2)", numbered.savedPath());
		assertTrue(Files.exists(tempDir.resolve("data (2)/placeholder.txt")));
	}

	@Test
	void newDirectoryWithOverrideRefusesAFileInTheWay() throws Exception {
		Files.writeString(tempDir.resolve("data"), "x");

		assertThrows(IllegalArgumentException.class,
				() -> FileSystemUtil.createNewAssetDirectory(tempDir.toString(), "data", true));
		assertEquals("data (2)", FileSystemUtil.createNewAssetDirectory(tempDir.toString(), "data", false).savedPath());
	}

	@Test
	void renameNumbersTheEntryWhenTheNewPathIsTakenWithoutOverride() throws Exception {
		Files.writeString(tempDir.resolve("a.txt"), "a");
		Files.writeString(tempDir.resolve("b.txt"), "b");

		SavedAsset saved = FileSystemUtil.renameAsset(tempDir.toString(), "a.txt", "b.txt", false);

		assertEquals("b (2).txt", saved.savedPath());
		assertFalse(Files.exists(tempDir.resolve("a.txt")));
		assertEquals("b", Files.readString(tempDir.resolve("b.txt")));
		assertEquals("a", Files.readString(tempDir.resolve("b (2).txt")));
	}

	@Test
	void renameReplacesTheNewPathWithOverride() throws Exception {
		Files.writeString(tempDir.resolve("a.txt"), "a");
		Files.writeString(tempDir.resolve("b.txt"), "b");

		SavedAsset saved = FileSystemUtil.renameAsset(tempDir.toString(), "a.txt", "b.txt", true);

		assertFalse(saved.renamed());
		assertFalse(Files.exists(tempDir.resolve("a.txt")));
		assertEquals("a", Files.readString(tempDir.resolve("b.txt")));
	}

	@Test
	void renameNeverDeletesTheEntryItIsMoving() throws Exception {
		Path folder = Files.createDirectories(tempDir.resolve("folder"));
		Files.writeString(folder.resolve("a.txt"), "a");

		assertThrows(IllegalArgumentException.class,
				() -> FileSystemUtil.renameAsset(tempDir.toString(), "folder/a.txt", "folder/a.txt", true));
		assertThrows(IllegalArgumentException.class,
				() -> FileSystemUtil.renameAsset(tempDir.toString(), "folder/a.txt", "folder", true));
		assertThrows(IllegalArgumentException.class,
				() -> FileSystemUtil.renameAsset(tempDir.toString(), "folder", "folder/inner", true));
		assertEquals("a", Files.readString(folder.resolve("a.txt")));
	}

	@Test
	void deleteSkipsAPathNamingTheAssetFolderItself() throws Exception {
		Files.writeString(tempDir.resolve("keep.txt"), "x");
		List<String> gitPaths = new ArrayList<>();
		List<File> deleted = new ArrayList<>();

		FileSystemUtil.deleteAssetFiles(tempDir.toString(), List.of("/", "./"), gitPaths, deleted);

		assertTrue(deleted.isEmpty());
		assertTrue(Files.exists(tempDir.resolve("keep.txt")));
	}

	@Test
	void savedAssetsAsNounMetadataReturnsOneMapPerAssetAndAMessageOnlyForRenamedOnes() {
		NounMetadata noun = FileSystemUtil.savedAssetsAsNounMetadata(
				List.of(new SavedAsset("a.txt", "a.txt"), new SavedAsset("b.txt", "b (2).txt")));

		assertEquals(PixelDataType.CUSTOM_DATA_STRUCTURE, noun.getNounType());
		assertEquals(List.of(Map.of("requestedPath", "a.txt", "savedPath", "a.txt", "renamed", false),
				Map.of("requestedPath", "b.txt", "savedPath", "b (2).txt", "renamed", true)), noun.getValue());

		List<NounMetadata> messages = noun.getAdditionalReturn();
		assertEquals(1, messages.size());
		assertTrue(messages.get(0).getOpType().contains(PixelOperationType.SUCCESS));
		assertEquals("'b.txt' already exists, so it was saved as 'b (2).txt'", messages.get(0).getValue());
	}

	@Test
	void savedAssetsAsNounMetadataCarriesNoMessageWhenNothingWasRenamed() {
		NounMetadata noun = FileSystemUtil.savedAssetsAsNounMetadata(List.of(new SavedAsset("a.txt", "a.txt")));

		assertTrue(noun.getAdditionalReturn().isEmpty());
	}

	@Test
	void savedAssetOutputSchemaNamesEveryKeyOfTheMap() {
		JSONObject items = new SaveRoomAssetsReactor().asMcpTool().getJSONObject("outputSchema")
				.getJSONObject("properties").getJSONObject("result").getJSONObject("items");

		assertEquals(new SavedAsset("a", "b").toMap().keySet(), items.getJSONObject("properties").keySet());
		assertEquals("boolean", items.getJSONObject("properties").getJSONObject("renamed").getString("type"));
	}

	@Test
	void unzipExtractsTakenTopLevelEntriesBesideThemWithoutOverride() throws Exception {
		Path folder = Files.createDirectories(tempDir.resolve("uploads"));
		Files.writeString(folder.resolve("readme.md"), "mine");
		Files.createDirectories(folder.resolve("site"));
		Files.writeString(folder.resolve("site/index.html"), "mine");
		Path zip = folder.resolve("bundle.zip");
		writeZip(zip, Map.of("readme.md", "theirs", "site/index.html", "theirs", "notes/a.txt", "theirs"));

		Map<String, String> extracted = ZipUtils.unzip(zip.toString(), folder.toString(), false);

		assertEquals("readme (2).md", extracted.get("readme.md"));
		assertEquals("site (2)", extracted.get("site"));
		assertEquals("notes", extracted.get("notes"));
		assertEquals("mine", Files.readString(folder.resolve("readme.md")));
		assertEquals("mine", Files.readString(folder.resolve("site/index.html")));
		assertEquals("theirs", Files.readString(folder.resolve("site (2)/index.html")));
		assertEquals("theirs", Files.readString(folder.resolve("notes/a.txt")));

		List<SavedAsset> saved = FileSystemUtil.unzippedAssets("uploads/bundle.zip", extracted);
		assertTrue(saved.contains(new SavedAsset("uploads/site", "uploads/site (2)")));
	}

	@Test
	void unzipReplacesExistingEntriesWithOverride() throws Exception {
		Files.writeString(tempDir.resolve("readme.md"), "mine");
		Path zip = tempDir.resolve("bundle.zip");
		writeZip(zip, Map.of("readme.md", "theirs"));

		Map<String, String> extracted = ZipUtils.unzip(zip.toString(), tempDir.toString(), true);

		assertEquals("readme.md", extracted.get("readme.md"));
		assertEquals("theirs", Files.readString(tempDir.resolve("readme.md")));
	}

	private String next(String name, boolean directory) {
		return FileSystemUtil.nextAvailableFile(tempDir.resolve(name).toFile(), directory).getName();
	}

	private static void writeZip(Path zip, Map<String, String> entries) throws Exception {
		try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(zip.toFile()))) {
			for (Map.Entry<String, String> entry : entries.entrySet()) {
				zos.putNextEntry(new ZipEntry(entry.getKey()));
				zos.write(entry.getValue().getBytes(StandardCharsets.UTF_8));
				zos.closeEntry();
			}
		}
	}
}
