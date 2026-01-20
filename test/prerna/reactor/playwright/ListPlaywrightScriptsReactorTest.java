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
