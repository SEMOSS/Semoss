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
package prerna.engine.impl;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import prerna.engine.api.IEngine;
import prerna.io.connector.secrets.ISecrets;
import prerna.io.connector.secrets.SecretsFactory;
import prerna.util.ContentChangedException;

class SmssEditCopyUnitTests {

	@TempDir
	Path folder;

	private MockedStatic<SecretsFactory> secretsFactory;
	private ISecrets secretStore;

	@BeforeEach
	void setup() {
		this.secretsFactory = mockStatic(SecretsFactory.class);
		this.secretStore = mock(ISecrets.class);
	}

	@AfterEach
	void cleanup() {
		this.secretsFactory.close();
	}

	@Test
	void theCopyMasksTheSecretsTheFileHolds() throws IOException {
		IEngine engine = engineAt(write("ENGINE\tid\nPASSWORD\thunter2\n"));

		SmssEditCopy copy = SmssEditCopy.read(engine);

		assertTrue(copy.getSmss().startsWith("ENGINE\tid\n"), copy.getSmss());
		assertFalse(copy.getSmss().contains("hunter2"), copy.getSmss());
		assertTrue(copy.getSmssHash().matches("[0-9a-f]{64}"), copy.getSmssHash());
		assertEquals(Map.of(SmssEditCopy.SMSS, copy.getSmss(), SmssEditCopy.SMSS_HASH, copy.getSmssHash()),
				copy.toMap());
	}

	@Test
	void theHashChangesWhenTheFileChanges() throws IOException {
		Path smss = write("ENGINE\tid\nURL\tone\n");
		IEngine engine = engineAt(smss);
		String before = SmssEditCopy.read(engine).getSmssHash();

		Files.writeString(smss, "ENGINE\tid\nURL\ttwo\n");

		assertNotEquals(before, SmssEditCopy.read(engine).getSmssHash());
	}

	@Test
	void theHashChangesWhenOnlyASecretChanges() throws IOException {
		IEngine engine = engineAt(write("ENGINE\tid\n"));
		secretsAre(Map.of("PASSWORD", "one"));
		String before = SmssEditCopy.read(engine).getSmssHash();

		secretsAre(Map.of("PASSWORD", "two"));

		assertNotEquals(before, SmssEditCopy.read(engine).getSmssHash());
	}

	@Test
	void theOrderTheSecretStoreGivesItsKeysInDoesNotChangeTheHash() throws IOException {
		IEngine engine = engineAt(write("ENGINE\tid\n"));
		Map<String, Object> ab = new LinkedHashMap<>();
		ab.put("A_KEY", "1");
		ab.put("B_KEY", "2");
		Map<String, Object> ba = new LinkedHashMap<>();
		ba.put("B_KEY", "2");
		ba.put("A_KEY", "1");

		secretsAre(ab);
		String first = SmssEditCopy.read(engine).getSmssHash();
		secretsAre(ba);

		assertEquals(first, SmssEditCopy.read(engine).getSmssHash());
	}

	@Test
	void theSecretSectionStartsOnItsOwnLine() throws IOException {
		IEngine engine = engineAt(write("ENGINE\tid\nLAST\tvalue"));
		secretsAre(Map.of("API_KEY", "abc"));

		String smss = SmssEditCopy.read(engine).getSmss();

		assertTrue(smss.contains("LAST\tvalue\n#Comments below this section will be lost\n"), smss);
		assertFalse(smss.contains("abc"), smss);
	}

	@Test
	void aMissingFileOrAFolderIsReported() throws IOException {
		IEngine missing = engineAt(this.folder.resolve("missing.smss"));
		IEngine folderEngine = engineAt(Files.createDirectory(this.folder.resolve("folder")));

		for (IEngine engine : new IEngine[] { missing, folderEngine }) {
			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> SmssEditCopy.read(engine));
			assertEquals("Could not find smss file for engine id. Please reach out to an administrator for assistance",
					e.getMessage());
		}
	}

	@Test
	void aFileThatCannotBeReadIsReported() throws IOException {
		IEngine engine = engineAt(write("ENGINE\tid\n"));
		try (MockedStatic<Files> files = mockStatic(Files.class, CALLS_REAL_METHODS)) {
			files.when(() -> Files.readAllBytes(any(Path.class))).thenThrow(new IOException("disk"));

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> SmssEditCopy.read(engine));
			assertEquals("An error occurred reading the current smss details of engine id. Detailed message = disk",
					e.getMessage());
		}
	}

	@Test
	void aSaveFromTheCurrentCopyGoesThrough() throws IOException {
		IEngine engine = engineAt(write("ENGINE\tid\n"));
		String smssHash = SmssEditCopy.read(engine).getSmssHash();

		assertDoesNotThrow(() -> SmssEditCopy.requireCurrent(engine, smssHash));
	}

	@Test
	void aSaveFromAnOlderCopyIsRefusedAsChanged() throws IOException {
		Path smss = write("ENGINE\tid\nURL\tone\n");
		IEngine engine = engineAt(smss);
		String smssHash = SmssEditCopy.read(engine).getSmssHash();
		Files.writeString(smss, "ENGINE\tid\nURL\ttwo\n");

		ContentChangedException e = assertThrows(ContentChangedException.class,
				() -> SmssEditCopy.requireCurrent(engine, smssHash));

		assertEquals(SmssEditCopy.SMSS_CHANGED, e.getErrorCode());
		assertTrue(e.getMessage().contains("changed since you opened it"), e.getMessage());
	}

	@Test
	void aSaveWithoutTheHashIsRefused() throws IOException {
		IEngine engine = engineAt(write("ENGINE\tid\n"));

		for (String smssHash : new String[] { null, " " }) {
			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> SmssEditCopy.requireCurrent(engine, smssHash));
			assertFalse(e instanceof ContentChangedException);
			assertTrue(e.getMessage().contains(SmssEditCopy.SMSS_HASH), e.getMessage());
		}
	}

	private Path write(String content) throws IOException {
		return Files.writeString(this.folder.resolve("engine.smss"), content);
	}

	private void secretsAre(Map<String, Object> secrets) {
		this.secretsFactory.when(SecretsFactory::getSecretConnector).thenReturn(this.secretStore);
		when(this.secretStore.getEngineSecrets(IEngine.CATALOG_TYPE.DATABASE, "id", "name")).thenReturn(secrets);
	}

	private static IEngine engineAt(Path smssFile) {
		IEngine engine = mock(IEngine.class);
		when(engine.getSmssFilePath()).thenReturn(smssFile.toString());
		when(engine.getEngineId()).thenReturn("id");
		when(engine.getEngineName()).thenReturn("name");
		when(engine.getCatalogType()).thenReturn(IEngine.CATALOG_TYPE.DATABASE);
		return engine;
	}
}
