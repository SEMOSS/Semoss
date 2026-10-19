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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileSystemUtilCopyResolvedFileTest {

	@TempDir
	Path source;

	@TempDir
	Path target;

	@Test
	void copiesIntoMissingParentFolders() throws Exception {
		Path file = source.resolve("deck.pptx");
		Files.write(file, new byte[] { 1, 2, 3 });
		Path destination = target.resolve("version/assets/office-studio/presentations/deck-v1.pptx");
		long size = FileSystemUtil.copyResolvedFile(file.toString(), destination.toString(), false);
		assertEquals(3, size);
		assertTrue(Files.exists(destination));
		assertTrue(Files.exists(file), "source is left in place");
	}

	@Test
	void refusesExistingDestinationUnlessOverride() throws Exception {
		Path file = source.resolve("deck.pptx");
		Files.write(file, new byte[] { 9, 9 });
		Path destination = target.resolve("deck.pptx");
		Files.write(destination, new byte[] { 1 });
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> FileSystemUtil.copyResolvedFile(file.toString(), destination.toString(), false));
		assertTrue(error.getMessage().contains("override=true"));
		assertFalse(error.getMessage().contains(target.toString()), "error must not leak absolute paths");
		assertEquals(1, Files.size(destination));

		assertEquals(2, FileSystemUtil.copyResolvedFile(file.toString(), destination.toString(), true));
		assertEquals(2, Files.size(destination));
	}

	@Test
	void rejectsDirectoriesAndSelfCopies() throws Exception {
		Path folder = source.resolve("folder");
		Files.createDirectories(folder);
		assertThrows(IllegalArgumentException.class,
				() -> FileSystemUtil.copyResolvedFile(folder.toString(), target.resolve("x").toString(), true));
		Path file = source.resolve("a.txt");
		Files.writeString(file, "a");
		assertThrows(IllegalArgumentException.class,
				() -> FileSystemUtil.copyResolvedFile(file.toString(), file.toString(), true));
		assertThrows(IllegalArgumentException.class, () -> FileSystemUtil
				.copyResolvedFile(source.resolve("missing").toString(), target.resolve("x").toString(), true));
	}
}
