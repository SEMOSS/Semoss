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
package prerna.reactor.playwright;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.SemossUnitTest;
import prerna.om.Insight;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

class ListPlaywrightScriptsReactorTest extends SemossUnitTest {

	private ListPlaywrightScriptsReactor reactor;
	private Map<String, String> keyValues;
	private NounStore nounStore;
	private Insight insight;

	@BeforeEach
	void setUp() throws IOException {
		if (Files.exists(tempDir)) {
			FileUtils.cleanDirectory(tempDir.toFile());
		}

		reactor = new ListPlaywrightScriptsReactor();
		keyValues = reactor.keyValue;
		nounStore = new NounStore("test");
		insight = mock(Insight.class);

		reactor.setNounStore(nounStore);
		reactor.setInsight(insight);
	}

	@Test
	void executeListsAllJsonFiles() throws IOException {
		String projectId = "test-project";
		keyValues.put(ReactorKeysEnum.PROJECT.getKey(), projectId);

		Path recordingsDir = tempDir.resolve("recordings");
		Files.createDirectories(recordingsDir);
		Files.createFile(recordingsDir.resolve("test1.json"));
		Files.createFile(recordingsDir.resolve("test2.json"));
		Files.createFile(recordingsDir.resolve("recording.json"));

		try (MockedStatic<PlaywrightUtility> mockedUtility = Mockito.mockStatic(PlaywrightUtility.class)) {
			mockedUtility.when(() -> PlaywrightUtility.initRecordingsDir(projectId)).thenReturn(recordingsDir);

			NounMetadata result = reactor.execute();

			assertEquals(PixelDataType.VECTOR, result.getNounType());
			@SuppressWarnings("unchecked")
			List<String> fileNames = (List<String>) result.getValue();
			assertEquals(3, fileNames.size());
			assertTrue(fileNames.contains("test1.json"));
			assertTrue(fileNames.contains("test2.json"));
			assertTrue(fileNames.contains("recording.json"));
		}
	}

	@Test
	void executeFiltersNonJsonFiles() throws IOException {
		String projectId = "test-project";
		keyValues.put(ReactorKeysEnum.PROJECT.getKey(), projectId);

		Path recordingsDir = tempDir.resolve("recordings");
		Files.createDirectories(recordingsDir);
		Files.createFile(recordingsDir.resolve("script1.json"));
		Files.createFile(recordingsDir.resolve("script2.json"));
		Files.createFile(recordingsDir.resolve("readme.txt"));
		Files.createFile(recordingsDir.resolve("data.xml"));
		Files.createFile(recordingsDir.resolve("config.yaml"));

		try (MockedStatic<PlaywrightUtility> mockedUtility = Mockito.mockStatic(PlaywrightUtility.class)) {
			mockedUtility.when(() -> PlaywrightUtility.initRecordingsDir(projectId)).thenReturn(recordingsDir);

			NounMetadata result = reactor.execute();

			assertEquals(PixelDataType.VECTOR, result.getNounType());
			@SuppressWarnings("unchecked")
			List<String> fileNames = (List<String>) result.getValue();
			assertEquals(2, fileNames.size());
			assertTrue(fileNames.contains("script1.json"));
			assertTrue(fileNames.contains("script2.json"));
		}
	}

	@Test
	void executeHandlesCaseInsensitiveJsonExtension() throws IOException {
		String projectId = "test-project";
		keyValues.put(ReactorKeysEnum.PROJECT.getKey(), projectId);

		Path recordingsDir = tempDir.resolve("recordings");
		Files.createDirectories(recordingsDir);
		Files.createFile(recordingsDir.resolve("test1.json"));
		Files.createFile(recordingsDir.resolve("test2.JSON"));
		Files.createFile(recordingsDir.resolve("test3.Json"));

		try (MockedStatic<PlaywrightUtility> mockedUtility = Mockito.mockStatic(PlaywrightUtility.class)) {
			mockedUtility.when(() -> PlaywrightUtility.initRecordingsDir(projectId)).thenReturn(recordingsDir);

			NounMetadata result = reactor.execute();

			assertEquals(PixelDataType.VECTOR, result.getNounType());
			@SuppressWarnings("unchecked")
			List<String> fileNames = (List<String>) result.getValue();
			assertEquals(3, fileNames.size());
			assertTrue(fileNames.contains("test1.json"));
			assertTrue(fileNames.contains("test2.JSON"));
			assertTrue(fileNames.contains("test3.Json"));
		}
	}

	@Test
	void executeReturnsEmptyListWhenNoJsonFiles() throws IOException {
		String projectId = "test-project";
		keyValues.put(ReactorKeysEnum.PROJECT.getKey(), projectId);

		Path recordingsDir = tempDir.resolve("recordings");
		Files.createDirectories(recordingsDir);
		Files.createFile(recordingsDir.resolve("readme.txt"));
		Files.createFile(recordingsDir.resolve("config.xml"));

		try (MockedStatic<PlaywrightUtility> mockedUtility = Mockito.mockStatic(PlaywrightUtility.class)) {
			mockedUtility.when(() -> PlaywrightUtility.initRecordingsDir(projectId)).thenReturn(recordingsDir);

			NounMetadata result = reactor.execute();

			assertEquals(PixelDataType.VECTOR, result.getNounType());
			@SuppressWarnings("unchecked")
			List<String> fileNames = (List<String>) result.getValue();
			assertTrue(fileNames.isEmpty());
		}
	}

	@Test
	void executeReturnsEmptyListWhenDirectoryIsEmpty() throws IOException {
		String projectId = "test-project";
		keyValues.put(ReactorKeysEnum.PROJECT.getKey(), projectId);

		Path recordingsDir = tempDir.resolve("recordings");
		Files.createDirectories(recordingsDir);

		try (MockedStatic<PlaywrightUtility> mockedUtility = Mockito.mockStatic(PlaywrightUtility.class)) {
			mockedUtility.when(() -> PlaywrightUtility.initRecordingsDir(projectId)).thenReturn(recordingsDir);

			NounMetadata result = reactor.execute();

			assertEquals(PixelDataType.VECTOR, result.getNounType());
			@SuppressWarnings("unchecked")
			List<String> fileNames = (List<String>) result.getValue();
			assertTrue(fileNames.isEmpty());
		}
	}

	@Test
	void executeThrowsExceptionWhenDirectoryDoesNotExist() throws IOException {
		String projectId = "test-project";
		keyValues.put(ReactorKeysEnum.PROJECT.getKey(), projectId);

		Path recordingsDir = tempDir.resolve("non-existent");

		try (MockedStatic<PlaywrightUtility> mockedUtility = Mockito.mockStatic(PlaywrightUtility.class)) {
			mockedUtility.when(() -> PlaywrightUtility.initRecordingsDir(projectId)).thenReturn(recordingsDir);

			IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
				reactor.execute();
			});

			assertTrue(exception.getMessage().contains("Recordings folder does not exist"));
			assertTrue(exception.getMessage().contains(recordingsDir.toString()));
		}
	}

	@Test
	void executeThrowsExceptionWhenPathIsNotDirectory() throws IOException {
		String projectId = "test-project";
		keyValues.put(ReactorKeysEnum.PROJECT.getKey(), projectId);

		Path recordingsFile = tempDir.resolve("recordings.txt");
		Files.createFile(recordingsFile);

		try (MockedStatic<PlaywrightUtility> mockedUtility = Mockito.mockStatic(PlaywrightUtility.class)) {
			mockedUtility.when(() -> PlaywrightUtility.initRecordingsDir(projectId)).thenReturn(recordingsFile);

			IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
				reactor.execute();
			});

			assertTrue(exception.getMessage().contains("Recordings folder does not exist"));
		}
	}
}
