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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class UtilityUnitTests {

	@TempDir
	Path tempDir;

	private Path folder;

	@BeforeEach
	void createTheFolderAndAFileOutsideIt() throws IOException {
		this.folder = Files.createDirectories(this.tempDir.resolve("insight"));
		Files.createDirectories(this.folder.resolve("sub"));
		Files.writeString(this.folder.resolve("audio.mp3"), "audio");
		Files.writeString(this.tempDir.resolve("outside.txt"), "secret");
	}

	private String folderPath() {
		return this.folder.toString();
	}

	@Test
	void resolvesAFileInsideTheFolderToItsRealLocation() throws IOException {
		Path expected = this.folder.resolve("audio.mp3").toRealPath();

		for (String relativePath : new String[] { "audio.mp3", "./audio.mp3", "sub/../audio.mp3" }) {
			assertEquals(expected, Utility.resolveContainedFile(folderPath(), relativePath), relativePath);
		}
	}

	@Test
	void followsALinkThatStaysInsideTheFolder() throws IOException {
		Files.createSymbolicLink(this.folder.resolve("sub/copy.mp3"), this.folder.resolve("audio.mp3"));

		assertEquals(this.folder.resolve("audio.mp3").toRealPath(),
				Utility.resolveContainedFile(folderPath(), "sub/copy.mp3"));
	}

	@Test
	void resolvesAFileInsideAFolderThatIsALink() throws IOException {
		Path link = Files.createSymbolicLink(this.tempDir.resolve("linked"), this.folder);

		assertEquals(this.folder.resolve("audio.mp3").toRealPath(),
				Utility.resolveContainedFile(link.toString(), "audio.mp3"));
	}

	@Test
	void refusesABlankFolderOrFilePath() {
		assertEquals("The folder must not be blank",
				assertThrows(IllegalArgumentException.class, () -> Utility.resolveContainedFile(" ", "audio.mp3"))
						.getMessage());
		for (String relativePath : new String[] { null, "", " " }) {
			assertEquals("The file path must not be blank", assertThrows(IllegalArgumentException.class,
					() -> Utility.resolveContainedFile(folderPath(), relativePath)).getMessage());
		}
	}

	@Test
	void refusesAnAbsolutePathEvenInsideTheFolder() {
		String absolute = this.folder.resolve("audio.mp3").toAbsolutePath().toString();

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> Utility.resolveContainedFile(folderPath(), absolute));
		assertEquals("The file path " + absolute + " must be relative to its folder", e.getMessage());
	}

	@Test
	void refusesAPathThatClimbsOutOfTheFolder() {
		for (String relativePath : new String[] { "../outside.txt", "sub/../../outside.txt", "../missing.txt" }) {
			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> Utility.resolveContainedFile(folderPath(), relativePath));
			assertEquals("The file path " + relativePath + " is outside its folder", e.getMessage());
		}
	}

	@Test
	void refusesALinkThatLeadsOutOfTheFolder() throws IOException {
		Files.createSymbolicLink(this.folder.resolve("link.mp3"), this.tempDir.resolve("outside.txt"));

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> Utility.resolveContainedFile(folderPath(), "link.mp3"));
		assertEquals("The file path link.mp3 links outside its folder", e.getMessage());
	}

	@Test
	void refusesAFolder() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> Utility.resolveContainedFile(folderPath(), "sub"));
		assertEquals("The file path sub is not a file", e.getMessage());
	}

	@Test
	void reportsAMissingFileOrFolder() {
		assertThrows(NoSuchFileException.class, () -> Utility.resolveContainedFile(folderPath(), "missing.mp3"));
		assertThrows(NoSuchFileException.class,
				() -> Utility.resolveContainedFile(this.tempDir.resolve("gone").toString(), "audio.mp3"));
	}
}
