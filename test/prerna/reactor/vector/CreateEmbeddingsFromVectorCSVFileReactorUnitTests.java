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
package prerna.reactor.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CreateEmbeddingsFromVectorCSVFileReactorUnitTests {
	@TempDir
	Path directory;

	@Test
	void extractsCsvAndNestedZipFilesWithinDestination() throws Exception {
		byte[] nestedZip = zipBytes(Map.of("nested/vectors.csv", "nested".getBytes(StandardCharsets.UTF_8)));
		Map<String, byte[]> entries = new LinkedHashMap<>();
		entries.put("data/", null);
		entries.put("data/vectors.csv", "vectors".getBytes(StandardCharsets.UTF_8));
		entries.put("archives/nested.zip", nestedZip);
		entries.put("ignored.txt", "ignored".getBytes(StandardCharsets.UTF_8));
		Path zipFile = writeZip("vectors.zip", entries);
		Path destination = directory.resolve("extracted");
		List<String> validFiles = new ArrayList<>();
		List<String> invalidFiles = new ArrayList<>();

		new CreateEmbeddingsFromVectorCSVFileReactor().unzipAndFilter(zipFile.toString(), destination.toString(),
				validFiles, invalidFiles);

		Path csv = destination.resolve("data/vectors.csv").toRealPath();
		Path nestedCsv = destination.resolve("archives/nested/nested/vectors.csv").toRealPath();
		assertEquals(List.of(csv.toString(), nestedCsv.toString()), validFiles);
		assertTrue(invalidFiles.isEmpty());
		assertEquals("vectors", Files.readString(csv));
		assertEquals("nested", Files.readString(nestedCsv));
		assertFalse(Files.exists(destination.resolve("ignored.txt")));
	}

	@Test
	void rejectsEntriesThatEscapeDestination() throws Exception {
		for (String entryName : List.of("../../outside.csv", "..\\..\\outside.csv", "/tmp/outside.csv",
				"C:/outside.csv")) {
			Path zipFile = writeZip("malicious-" + Math.abs(entryName.hashCode()) + ".zip",
					Map.of(entryName, "outside".getBytes(StandardCharsets.UTF_8)));
			Path destination = directory.resolve("extract-" + Math.abs(entryName.hashCode()));
			assertThrows(IOException.class, () -> new CreateEmbeddingsFromVectorCSVFileReactor().unzipAndFilter(
					zipFile.toString(), destination.toString(), new ArrayList<>(), new ArrayList<>()), entryName);
		}
		assertFalse(Files.exists(directory.resolve("outside.csv")));
	}

	@Test
	void rejectsDirectoryEntryThatEscapesDestination() throws Exception {
		Map<String, byte[]> entries = new LinkedHashMap<>();
		entries.put("../../outside/", null);
		Path zipFile = writeZip("directory-traversal.zip", entries);
		Path destination = directory.resolve("extracted");

		assertThrows(IOException.class, () -> new CreateEmbeddingsFromVectorCSVFileReactor().unzipAndFilter(
				zipFile.toString(), destination.toString(), new ArrayList<>(), new ArrayList<>()));
		assertFalse(Files.exists(directory.resolve("outside")));
		assertTrue(Files.isDirectory(destination));
	}

	private Path writeZip(String name, Map<String, byte[]> entries) throws IOException {
		Path zipFile = directory.resolve(name);
		Files.write(zipFile, zipBytes(entries));
		return zipFile;
	}

	private static byte[] zipBytes(Map<String, byte[]> entries) throws IOException {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		try (ZipOutputStream zip = new ZipOutputStream(bytes)) {
			for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
				zip.putNextEntry(new ZipEntry(entry.getKey()));
				if (entry.getValue() != null) {
					zip.write(entry.getValue());
				}
				zip.closeEntry();
			}
		}
		return bytes.toByteArray();
	}
}
