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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ZipUtilsUnitTests {

	@TempDir
	Path root;

	@Test
	void extractsValidatedNestedEntries() throws Exception {
		Path archive = archive("valid.zip", Map.of("folder/file.txt", "content"));
		Path destination = root.resolve("output");

		Map<String, List<String>> extracted = ZipUtils.unzip(archive.toString(), destination.toString());

		assertEquals("content", Files.readString(destination.resolve("folder/file.txt")));
		assertEquals(List.of("folder/file.txt"), extracted.get("FILE"));
	}

	@ParameterizedTest
	@ValueSource(strings = { "../outside.txt", "..\\outside.txt", "/absolute.txt", "C:\\outside.txt" })
	void rejectsTraversalAbsoluteAndDriveQualifiedEntriesBeforeExtraction(String maliciousEntry) throws Exception {
		Path archive = archive("malicious.zip", Map.of("safe.txt", "safe", maliciousEntry, "outside"));
		Path destination = root.resolve("output");

		assertThrows(IOException.class, () -> ZipUtils.unzip(archive.toString(), destination.toString()));
		assertFalse(Files.exists(destination.resolve("safe.txt")));
		assertFalse(Files.exists(root.resolve("outside.txt")));
	}

	@Test
	void rejectsEntriesThatEscapeThroughAnExistingSymlink() throws Exception {
		Path destination = Files.createDirectory(root.resolve("output"));
		Path outside = Files.createDirectory(root.resolve("outside"));
		Files.createSymbolicLink(destination.resolve("link"), outside);
		Path archive = archive("symlink.zip", Map.of("link/escaped.txt", "outside"));

		assertThrows(IOException.class, () -> ZipUtils.unzip(archive.toString(), destination.toString()));
		assertFalse(Files.exists(outside.resolve("escaped.txt")));
	}

	private Path archive(String name, Map<String, String> entries) throws IOException {
		Path archive = root.resolve(name);
		try (ZipOutputStream output = new ZipOutputStream(Files.newOutputStream(archive))) {
			for (Map.Entry<String, String> entry : entries.entrySet()) {
				output.putNextEntry(new ZipEntry(entry.getKey()));
				output.write(entry.getValue().getBytes(StandardCharsets.UTF_8));
				output.closeEntry();
			}
		}
		return archive;
	}
}
