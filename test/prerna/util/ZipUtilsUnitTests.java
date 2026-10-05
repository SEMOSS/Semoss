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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Covers the "Zip Slip" path-traversal containment check in
 * {@link ZipUtils#unzip(String, String)}: a crafted zip entry must never be
 * allowed to extract to a path outside of the requested destination
 * directory.
 */
class ZipUtilsUnitTests {

	@TempDir
	Path tempDir;

	@Test
	void unzipExtractsNormalEntriesWithinDestination() throws Exception {
		Path zipPath = tempDir.resolve("normal.zip");
		writeZip(zipPath, Map.of("folder/file.txt", "hello world"));

		Path destination = tempDir.resolve("dest-normal");
		Files.createDirectories(destination);

		ZipUtils.unzip(zipPath.toString(), destination.toString());

		Path extracted = destination.resolve("folder").resolve("file.txt");
		assertTrue(Files.exists(extracted), "expected file to be extracted inside the destination folder");
		assertEquals("hello world", Files.readString(extracted));
	}

	@Test
	void unzipRejectsEntryThatTraversesAboveDestination() throws Exception {
		Path zipPath = tempDir.resolve("traversal.zip");
		writeZip(zipPath, Map.of("../../../../tmp/zip-slip-outside.txt", "pwned"));

		Path destination = tempDir.resolve("dest-traversal");
		Files.createDirectories(destination);

		// Whichever layer catches it (the explicit containment check added to
		// unzip(), or this codebase's pre-existing Utility.normalizePath / zip
		// filesystem entry validation), a traversal entry must never succeed and
		// nothing from this zip may land in the destination.
		assertThrows(Exception.class, () -> ZipUtils.unzip(zipPath.toString(), destination.toString()));
		assertDirectoryIsEmpty(destination);
	}

	@Test
	void unzipRejectsEntryThatEscapesThroughASymlinkedSubdirectory() throws Exception {
		// entry.getName() here ("escape/evil.txt") contains no ".." at all, so
		// lexical normalization cannot see this coming - the escape only exists
		// once "escape" is resolved as a symlink pointing outside the
		// destination. Only a canonical-path containment check (which follows
		// symlinks) can catch this, which is why the fix in ZipUtils.unzip uses
		// File.getCanonicalPath() rather than comparing raw strings.
		Path destination = tempDir.resolve("dest-symlink");
		Files.createDirectories(destination);
		Path outsideDir = tempDir.resolve("outside-symlink-target");
		Files.createDirectories(outsideDir);

		try {
			Files.createSymbolicLink(destination.resolve("escape"), outsideDir);
		} catch (IOException | UnsupportedOperationException e) {
			Assumptions.assumeTrue(false, "Symbolic links are not supported on this platform/filesystem: " + e);
			return;
		}

		Path zipPath = tempDir.resolve("symlink-escape.zip");
		writeZip(zipPath, Map.of("escape/evil.txt", "pwned-via-symlink"));

		assertThrows(IOException.class, () -> ZipUtils.unzip(zipPath.toString(), destination.toString()));
		assertFalse(Files.exists(outsideDir.resolve("evil.txt")),
				"entry must not be written through a symlinked subdirectory to a path outside the destination");
	}

	private static void assertDirectoryIsEmpty(Path dir) throws IOException {
		try (Stream<Path> children = Files.list(dir)) {
			assertFalse(children.findAny().isPresent(), "expected '" + dir + "' to remain empty");
		}
	}

	private static void writeZip(Path zipPath, Map<String, String> entries) throws IOException {
		try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(zipPath))) {
			for (Map.Entry<String, String> e : entries.entrySet()) {
				zos.putNextEntry(new ZipEntry(e.getKey()));
				zos.write(e.getValue().getBytes(StandardCharsets.UTF_8));
				zos.closeEntry();
			}
		}
	}

}
