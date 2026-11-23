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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipOutputStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests that extracting a zip keeps every entry inside its folder, never writes
 * through a symbolic link, holds the zip to the extraction limits, and leaves
 * nothing of a refused zip behind.
 */
class ZipUtilsUnitTests {

	@TempDir
	Path tempDir;

	private Path folder;
	private Path outside;
	private Properties savedCoreProp;
	private Properties rdfMap;

	@BeforeEach
	void setUp() throws IOException {
		folder = Files.createDirectories(tempDir.resolve("folder"));
		outside = Files.createDirectories(tempDir.resolve("outside"));
		savedCoreProp = InternalUtility.getCoreProp();
		rdfMap = new Properties();
		InternalUtility.setCoreProp(rdfMap);
	}

	@AfterEach
	void restoreRdfMap() {
		InternalUtility.setCoreProp(savedCoreProp);
	}

	@Test
	void extractsEveryEntryAndListsThem() throws Exception {
		Path zip = writeZip(entries("a.txt", "a", "dir/b.txt", "b", "dir/sub/", null));

		Map<String, List<String>> listed = ZipUtils.unzip(zip.toString(), folder.toString());

		assertEquals("a", Files.readString(folder.resolve("a.txt")));
		assertEquals("b", Files.readString(folder.resolve("dir/b.txt")));
		assertTrue(Files.isDirectory(folder.resolve("dir/sub")));
		assertTrue(listed.get("FILE").containsAll(List.of("a.txt", "dir/b.txt")));
		assertTrue(listed.get("DIR").containsAll(List.of("dir", "dir/sub")));
		assertEquals(List.of(), partFiles());
	}

	@Test
	void extractsAnAbsoluteEntryInsideTheFolder() throws Exception {
		Path zip = writeZip(entries("/abs/x.txt", "x"));

		ZipUtils.unzip(zip.toString(), folder.toString(), true);

		assertEquals("x", Files.readString(folder.resolve("abs/x.txt")));
	}

	@Test
	void refusesAnEntryThatClimbsOutOfTheFolderAndRemovesWhatItExtracted() throws Exception {
		Path zip = writeZip(entries("ok.txt", "ok", "nested/ok.txt", "ok", "../evil.txt", "evil"));

		ZipException listing = assertThrows(ZipException.class,
				() -> ZipUtils.unzip(zip.toString(), folder.toString()));
		assertTrue(listing.getMessage().contains("../evil.txt"), listing.getMessage());
		ZipException named = assertThrows(ZipException.class,
				() -> ZipUtils.unzip(zip.toString(), folder.toString(), false));
		assertTrue(named.getMessage().contains("../evil.txt"), named.getMessage());

		assertFalse(Files.exists(tempDir.resolve("evil.txt")));
		assertEquals(List.of(), contents(folder));
	}

	@Test
	void neverWritesThroughASymbolicLinkInTheFolder() throws Exception {
		Path secret = Files.writeString(outside.resolve("secret.txt"), "keep");
		assumeTrue(link(folder.resolve("linked"), outside));
		assumeTrue(link(folder.resolve("file.txt"), secret));

		Path intoLinkedFolder = writeZip(entries("linked/x.txt", "evil"));
		assertThrows(ZipException.class, () -> ZipUtils.unzip(intoLinkedFolder.toString(), folder.toString(), true));
		assertFalse(Files.exists(outside.resolve("x.txt")));

		Path overLinkedFile = writeZip(entries("file.txt", "new"));
		ZipUtils.unzip(overLinkedFile.toString(), folder.toString(), true);
		assertEquals("keep", Files.readString(secret));
		assertFalse(Files.isSymbolicLink(folder.resolve("file.txt")));
		assertEquals("new", Files.readString(folder.resolve("file.txt")));
	}

	@Test
	void refusesAZipWithMoreEntriesThanTheLimitBeforeWritingAnything() throws Exception {
		rdfMap.setProperty(ZipUtils.ZIP_MAX_ENTRIES, "2");
		Path zip = writeZip(entries("a.txt", "a", "b.txt", "b", "c.txt", "c"));

		ZipException refused = assertThrows(ZipException.class,
				() -> ZipUtils.unzip(zip.toString(), folder.toString(), false));

		assertTrue(refused.getMessage().contains(ZipUtils.ZIP_MAX_ENTRIES), refused.getMessage());
		assertEquals(List.of(), contents(folder));
	}

	@Test
	void refusesAZipThatExpandsPastTheByteLimitAndRemovesWhatItExtracted() throws Exception {
		rdfMap.setProperty(ZipUtils.ZIP_MAX_EXTRACTED_BYTES, "1000");
		Path zip = writeZip(entries("small.txt", "a".repeat(500), "big/large.txt", "b".repeat(5000)));

		ZipException refused = assertThrows(ZipException.class,
				() -> ZipUtils.unzip(zip.toString(), folder.toString()));

		assertTrue(refused.getMessage().contains(ZipUtils.ZIP_MAX_EXTRACTED_BYTES), refused.getMessage());
		assertEquals(List.of(), contents(folder));
	}

	@Test
	void refusesAZipThatExpandsPastTheCompressionRatio() throws Exception {
		rdfMap.setProperty(ZipUtils.ZIP_MAX_COMPRESSION_RATIO, "10");
		Path zip = tempDir.resolve("zeros.zip");
		try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(zip.toFile()))) {
			zos.putNextEntry(new ZipEntry("zeros.bin"));
			byte[] megabyte = new byte[1024 * 1024];
			for (int i = 0; i < 101; i++) {
				zos.write(megabyte);
			}
			zos.closeEntry();
		}

		ZipException refused = assertThrows(ZipException.class,
				() -> ZipUtils.unzip(zip.toString(), folder.toString(), true));

		assertTrue(refused.getMessage().contains(ZipUtils.ZIP_MAX_COMPRESSION_RATIO), refused.getMessage());
		assertEquals(List.of(), contents(folder));
	}

	@Test
	void ignoresALimitThatIsNotAPositiveWholeNumber() throws Exception {
		rdfMap.setProperty(ZipUtils.ZIP_MAX_ENTRIES, "many");
		rdfMap.setProperty(ZipUtils.ZIP_MAX_EXTRACTED_BYTES, "-1");
		Path zip = writeZip(entries("a.txt", "a"));

		ZipUtils.unzip(zip.toString(), folder.toString(), true);

		assertEquals("a", Files.readString(folder.resolve("a.txt")));
	}

	/**
	 * Entries in order, as name and content pairs; a null content makes a directory
	 * entry.
	 */
	private static Map<String, String> entries(String... namesAndContents) {
		Map<String, String> entries = new LinkedHashMap<>();
		for (int i = 0; i < namesAndContents.length; i += 2) {
			entries.put(namesAndContents[i], namesAndContents[i + 1]);
		}
		return entries;
	}

	/**
	 * Writes a zip beside the folder, never inside it, so the folder holds only
	 * what is extracted.
	 */
	private Path writeZip(Map<String, String> entries) throws IOException {
		Path zip = Files.createTempFile(tempDir, "test", ".zip");
		try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(zip.toFile()))) {
			for (Map.Entry<String, String> entry : entries.entrySet()) {
				zos.putNextEntry(new ZipEntry(entry.getKey()));
				if (entry.getValue() != null) {
					zos.write(entry.getValue().getBytes(StandardCharsets.UTF_8));
				}
				zos.closeEntry();
			}
		}
		return zip;
	}

	/**
	 * Creates a symbolic link, or reports that this file system cannot.
	 */
	private static boolean link(Path link, Path target) {
		try {
			Files.createSymbolicLink(link, target);
			return true;
		} catch (IOException | UnsupportedOperationException e) {
			return false;
		}
	}

	/**
	 * Everything under a folder, as paths relative to it.
	 */
	private static List<String> contents(Path root) throws IOException {
		try (Stream<Path> paths = Files.walk(root)) {
			return paths.filter(path -> !path.equals(root)).map(path -> root.relativize(path).toString()).sorted()
					.toList();
		}
	}

	/**
	 * Partly written entries left anywhere under the temporary folder.
	 */
	private List<String> partFiles() throws IOException {
		try (Stream<Path> paths = Files.walk(tempDir)) {
			return paths.map(path -> path.getFileName().toString()).filter(name -> name.endsWith(".unzip")).toList();
		}
	}
}
