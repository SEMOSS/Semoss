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
package prerna.engine.impl.vector;

import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.SemossUnitTest;
import prerna.ds.py.PyTranslator;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.engine.impl.SmssUtilities;
import prerna.om.ClientProcessWrapper;
import prerna.om.Insight;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.BetweenQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.security.HttpHelperUtility;
import prerna.tcp.client.SocketClient;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineUtility;
import prerna.util.SymlinkHelper;
import prerna.util.Utility;

public class FaissDatabaseEngineUnitTests extends SemossUnitTest {
	private Insight insight;
	private FaissDatabaseEngine engine;
	private IModelEngine modelEmbedder;
		
	@BeforeEach
	void setUp() throws IOException {
		FileUtils.cleanDirectory(tempDir.toFile());

		engine = new FaissDatabaseEngine();
		insight = mock(Insight.class);
		modelEmbedder = mock(IModelEngine.class);
	}
	
	@Test
	void testOpen() throws Exception {
		Properties testProps = new Properties();
		String url = "http://fake.url/";
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
		String contentLength = "10";
		String contentOverlap = "10";
		String keepInputOutput = "false";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineAlias);
		testProps.setProperty(Constants.CONTENT_LENGTH, contentLength);
		testProps.setProperty(Constants.CONTENT_OVERLAP, contentOverlap);
		testProps.setProperty(Constants.KEEP_INPUT_OUTPUT, keepInputOutput);

		String engineNameAndId = SmssUtilities.getUniqueName(testEngineAlias, testEngine);
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(engineNameAndId);
		Path engineAssetFolder = engineFolder.resolve("assets");
		Path engineVersionFolder = engineFolder.resolve("version");

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class)
			) {
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineAssetFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineVersionFolder.toString());

				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine, testEngineAlias))
						.thenReturn(engineAssetFolder.toString());
				
				engine.open(testProps);
				assertTrue(Files.exists(engineAssetFolder));
				Properties engineProps = engine.getSmssProp();
				for (Entry<Object, Object> testProp : testProps.entrySet()) {
					assertTrue(engineProps.containsKey(testProp.getKey()));
					assertTrue(engineProps.containsValue(testProp.getValue()));
				}
			}
		}
	}
	
	@Test
	void testSearcherVarStateDuringOpensAndCloses() throws Exception {
		Field searcherField = FaissDatabaseEngine.class.getDeclaredField("vectorDatabaseSearcher");
		boolean accessible = searcherField.canAccess(engine);
		
		try {
			searcherField.setAccessible(true);
			
			String prevValue = (String) searcherField.get(engine);
			assertNull(prevValue);
			
			openEngine(tempDir, engine, null);
			String afterOpenValue = (String) searcherField.get(engine);
			assertNotNull(afterOpenValue);
			
			openEngine(tempDir, engine, null);
			String afterReopenValue = (String) searcherField.get(engine);
			assertEquals(afterOpenValue, afterReopenValue);
			
			engine.close();
			String afterCloseValue = (String) searcherField.get(engine);
			assertNull(afterCloseValue);
		} finally {
			searcherField.setAccessible(accessible);
		}
	}
	
	@Test
	void testGetServerStartCommands() throws Exception {
		openEngine(tempDir, engine, null);
		
		String[] commands = engine.getServerStartCommands();
		assertEquals(4, commands.length);
		String cmnd1 = "from genai_client import get_tokenizer";
		String cmnd2 = "cfg_tokenizer = get_tokenizer(tokenizer_name = '${MODEL}', max_tokens = ${MAX_TOKENS}, tokenizer_type = '${MODEL_TYPE}')";
		String cmnd3 =  "import vector_database";
		String cmnd4 = "=vector_database.FAISSDatabase(embedder_engine_id = '${EMBEDDER_ENGINE_ID}', " +
				"tokenizer = cfg_tokenizer, keyword_engine_id = '${KEYWORD_ENGINE_ID}', distance_method = '${DISTANCE_METHOD}', " +
				"enable_hybrid_search=True)";
		assertEquals(cmnd1, commands[0]);
		assertEquals(cmnd2, commands[1]);
		assertEquals(cmnd3, commands[2]);
		assertTrue(commands[3].endsWith(cmnd4)); // account for random value assigned to vectorDatabaseSearcher
	}
	
	@Test
	void testAddEmbeddings() throws Exception {
		// both objects needed for method call
		Map<String, Object> parameters = new HashMap<>();
		String indexClass = "default";
		parameters.put("indexClass", indexClass);

		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties

		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		Properties embedderProps = new Properties();
		embedderProps.setProperty(Constants.MODEL, embedderModel);
		embedderProps.setProperty(IModelEngine.MODEL_TYPE, embedderModelType);

		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path indexFilesDirPath = Paths.get(engineFolder.toString(), "assets", "schema", indexClass, "indexed_files");
		Path docDirPath = Paths.get(engineFolder.toString(), "assets", "schema", indexClass, "documents");

		SocketClient scMock = mock(SocketClient.class);
		when(scMock.isConnected()).thenReturn(true);
		try (MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);
				MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);
				MockedConstruction<ClientProcessWrapper> mockWrapper = Mockito
						.mockConstruction(ClientProcessWrapper.class, (mock, context) -> {
							doNothing().when(mock).createProcessAndClient(any(boolean.class),
									nullable(SymlinkHelper.class), any(int.class), nullable(String.class),
									nullable(String.class), nullable(String.class), any(boolean.class),
									any(String.class), any(String.class));
							doNothing().when(mock).shutdown(false);
							when(mock.getSocketClient()).thenReturn(scMock);

						});
				MockedConstruction<PyTranslator> mockPYT = Mockito.mockConstruction(PyTranslator.class,
						(mock, context) -> {
							//doNothing().when(mock).setSocketClient(scMock);
							when(mock.runDirectPy(any(Insight.class), anyString())).thenReturn(null).thenReturn(false);
							doNothing().when(mock).runEmptyPy(any());
							when(mock.runScript(any())).thenReturn("true");
						})) {
			// used in verifyModelProps & addEmbeddings
			u.when(() -> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
			when(modelEmbedder.getSmssProp()).thenReturn(embedderProps);
			
			engine.addEmbeddings(new Vector<String>(), insight, new HashMap<>());
			Properties updateEngineProps = engine.getSmssProp();
			assertTrue(Files.exists(indexFilesDirPath));
			assertTrue(Files.exists(docDirPath));
			assertNotNull(updateEngineProps);
			assertTrue(updateEngineProps.containsKey(Constants.MODEL));
			assertEquals(embedderModel, updateEngineProps.get(Constants.MODEL));
			assertTrue(updateEngineProps.containsKey(IModelEngine.MODEL_TYPE));
			assertEquals(embedderModelType, updateEngineProps.get(IModelEngine.MODEL_TYPE));			
		}
	}

	@Test
	void testRemoveDocument() throws Exception {
		// both objects needed for method call
		Map<String, Object> parameters = new HashMap<>();
		String indexClass = "default";
		parameters.put("indexClass", indexClass);

		// run this part first to create an index class in the schema directory
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path indexDirPath = Paths.get(engineFolder.toString(), "assets", "schema", indexClass);
		Files.createDirectories(indexDirPath);

		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties

		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		Properties embedderProps = new Properties();
		embedderProps.setProperty(Constants.MODEL, embedderModel);
		embedderProps.setProperty(IModelEngine.MODEL_TYPE, embedderModelType);

		// set up files
		String fileName1 = "newFile1.csv";
		String fileName2 = "newFile2.csv";
		String fileName3 = "newFile3.csv";
		String fileName4 = "newFile4.csv";

		// create the "index_files"
		Path indexFilesDirPath = Paths.get(indexDirPath.toString(), "indexed_files");
		Files.createDirectories(indexFilesDirPath);
		Path file1 = indexFilesDirPath.resolve(fileName1);
		Path file2 = indexFilesDirPath.resolve(fileName2);
		Files.createFile(file1);
		Files.createFile(file2);

		// this portion will create the files to be removed from "documents"
		Path docDirPath = Paths.get(indexDirPath.toString(), "documents");
		Files.createDirectories(docDirPath);
		Path file3 = docDirPath.resolve(fileName3);
		Path file4 = docDirPath.resolve(fileName4);
		Files.createFile(file3);
		Files.createFile(file4);
		// get list of all files, remove ext from first 2
		List<String> fileNamesMixed = new Vector<>();
		fileNamesMixed.add(fileName1.substring(0, fileName1.length() - 4));
		fileNamesMixed.add(fileName2.substring(0, fileName1.length() - 4));
		fileNamesMixed.add(fileName3);
		fileNamesMixed.add(fileName4);
		// list of full file names
		List<Path> allPaths = new Vector<>();
		allPaths.add(file1);
		allPaths.add(file2);
		allPaths.add(file3);
		allPaths.add(file4);

		SocketClient scMock = mock(SocketClient.class);
		when(scMock.isConnected()).thenReturn(true);

		try (MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);
				MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);
				MockedConstruction<ClientProcessWrapper> mockWrapper = Mockito
						.mockConstruction(ClientProcessWrapper.class, (mock, context) -> {
							doNothing().when(mock).createProcessAndClient(any(boolean.class),
									nullable(SymlinkHelper.class), any(int.class), nullable(String.class),
									nullable(String.class), nullable(String.class), any(boolean.class),
									any(String.class), any(String.class));
							doNothing().when(mock).shutdown(false);
							when(mock.getSocketClient()).thenReturn(scMock);

						});
				MockedConstruction<PyTranslator> mockPYT = Mockito.mockConstruction(PyTranslator.class,
						(mock, context) -> {
							//doNothing().when(mock).setSocketClient(scMock);
							doNothing().when(mock).runEmptyPy(any());
							when(mock.runScript(any())).thenReturn("true");
						})) {
			// used in verifyModelProps & addEmbeddings
			u.when(() -> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
			when(modelEmbedder.getSmssProp()).thenReturn(embedderProps);

			allPaths.forEach(path -> assertTrue(Files.exists(path)));
			engine.removeDocument(fileNamesMixed, parameters);
			allPaths.forEach(path -> assertFalse(Files.exists(path)));
		}
	}
	
	@Test
	void testNearestNeighborCall() throws Exception {
		Number limit = 1;
		String indexClass= "indexClass";
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties

		// model embedder properties in verifyModelProps
		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		Properties embedderProps = new Properties();
		embedderProps.setProperty(Constants.MODEL, embedderModel);
		embedderProps.setProperty(IModelEngine.MODEL_TYPE, embedderModelType);
		
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path docDirPath = Paths.get(engineFolder.toString(), "assets", "schema", indexClass, "documents");
		Files.createDirectories(docDirPath);
				
		String key1 = "key1";
		String value1 = "value1";
		List<Map<String, Object>> expectedOutput = new Vector<>();
		Map<String, Object> output1 = new HashMap<>();
		{
			{
				output1.put(key1, value1);
			}
			expectedOutput.add(output1);
		}
		
		SocketClient scMock = mock(SocketClient.class);
		when(scMock.isConnected()).thenReturn(true);
		try (MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);
				MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);
				MockedConstruction<ClientProcessWrapper> mockWrapper = Mockito
						.mockConstruction(ClientProcessWrapper.class, (mock, context) -> {
							doNothing().when(mock).createProcessAndClient(any(boolean.class),
									nullable(SymlinkHelper.class), any(int.class), nullable(String.class),
									nullable(String.class), nullable(String.class), any(boolean.class),
									any(String.class), any(String.class));
							doNothing().when(mock).shutdown(false);
							when(mock.getSocketClient()).thenReturn(scMock);

						});
				MockedConstruction<PyTranslator> mockPYT = Mockito.mockConstruction(PyTranslator.class, (mock, context) -> {
					//doNothing().when(mock).setSocketClient(scMock);
					when(mock.runDirectPy(any(Insight.class), anyString())).thenReturn(expectedOutput);
					doNothing().when(mock).runEmptyPy(any());
					when(mock.runScript(any())).thenReturn("true");
					when(mock.runSmssWrapperEval(any(String.class))).thenReturn(expectedOutput);
				})) {
			
			// used in verifyModelProps()
			u.when(()-> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
			when(modelEmbedder.getSmssProp()).thenReturn(embedderProps);	
			
			// nearestNeighborCall
			when(insight.getVarStore()).thenReturn(new VarStore());
			
			List<Map<String, Object>> actualOutput = engine.nearestNeighborCall(insight, "question", limit, new HashMap<>());
			assertEquals(1, actualOutput.size());
			Map<String, Object> actualOutput1 = actualOutput.get(0);
			assertEquals(output1, actualOutput1);
			Properties updateEngineProps = engine.getSmssProp();
			assertNotNull(updateEngineProps);
			assertTrue(updateEngineProps.containsKey(Constants.MODEL));
			assertEquals(embedderModel, updateEngineProps.get(Constants.MODEL));
			assertTrue(updateEngineProps.containsKey(IModelEngine.MODEL_TYPE));
			assertEquals(embedderModelType, updateEngineProps.get(IModelEngine.MODEL_TYPE));
		}
	}
	
	@Test
	void testNearestNeighborCallNoInsight() {
		IllegalArgumentException e = assertThrows(
				IllegalArgumentException.class,
				()->engine.nearestNeighborCall(null, null, null, null));
		assertEquals("Insight must be provided to run Model Engine Encoder", e.getMessage());
	}
	
	@Test
	void testRemoveCorruptedFiles() throws Exception {
		String indexClass = "index_class";
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties

		// model embedder properties in verifyModelProps
		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		Properties embedderProps = new Properties();
		embedderProps.setProperty(Constants.MODEL, embedderModel);
		embedderProps.setProperty(IModelEngine.MODEL_TYPE, embedderModelType);

		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path indexDirPath = Paths.get(engineFolder.toString(), "assets", "schema", indexClass);
		Files.createDirectories(indexDirPath);

		String key1 = "key1";
		String value1 = "value1";
		Map<String, String> scriptOutput = new HashMap<>();
		{
			scriptOutput.put(key1, value1);
		}

		SocketClient scMock = mock(SocketClient.class);
		when(scMock.isConnected()).thenReturn(true);
		try (MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);
				MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);
				MockedConstruction<ClientProcessWrapper> mockWrapper = Mockito
						.mockConstruction(ClientProcessWrapper.class, (mock, context) -> {
							doNothing().when(mock).createProcessAndClient(any(boolean.class),
									nullable(SymlinkHelper.class), any(int.class), nullable(String.class),
									nullable(String.class), nullable(String.class), any(boolean.class),
									any(String.class), any(String.class));
							doNothing().when(mock).shutdown(false);
							when(mock.getSocketClient()).thenReturn(scMock);

						});
				MockedConstruction<PyTranslator> mockPYT = Mockito.mockConstruction(PyTranslator.class,
						(mock, context) -> {
							//doNothing().when(mock).setSocketClient(scMock);
							doNothing().when(mock).runEmptyPy(any());
							when(mock.runDirectPy(anyString())).thenReturn(scriptOutput);
						})) {

			// used in verifyModelProps()
			u.when(() -> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
			when(modelEmbedder.getSmssProp()).thenReturn(embedderProps);

			Map<String, String> actualOutput = engine.removeCorruptedFiles(indexClass);
			assertEquals(scriptOutput.size(), actualOutput.size());
			assertEquals(scriptOutput, actualOutput);

			Properties updateEngineProps = engine.getSmssProp();
			assertNotNull(updateEngineProps);
			assertTrue(updateEngineProps.containsKey(Constants.MODEL));
			assertEquals(embedderModel, updateEngineProps.get(Constants.MODEL));
			assertTrue(updateEngineProps.containsKey(IModelEngine.MODEL_TYPE));
			assertEquals(embedderModelType, updateEngineProps.get(IModelEngine.MODEL_TYPE));
		}
	}
	
	@Test
	void testListDocuments() throws Exception {
		String indexClass= "indexClass";
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties

		// model embedder properties in verifyModelProps
		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		Properties embedderProps = new Properties();
		embedderProps.setProperty(Constants.MODEL, embedderModel);
		embedderProps.setProperty(IModelEngine.MODEL_TYPE, embedderModelType);
		
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path docDirPath = Paths.get(engineFolder.toString(), "assets", "schema", indexClass, "documents");
		Files.createDirectories(docDirPath);
		// create 4 new files: newFile1 ... newFile4.txt
		List<String> fileNames = new Vector<>();
		for (int fileNum = 1; fileNum < 5; fileNum++) {
			String fileName = "newFile" + fileNum + ".txt";
			fileNames.add(fileName);
			Path newFilePath = docDirPath.resolve(fileName);
			Files.createFile(newFilePath);
		}
		
		SocketClient scMock = mock(SocketClient.class);
		when(scMock.isConnected()).thenReturn(true);
		try (MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);
				MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);
				MockedConstruction<ClientProcessWrapper> mockWrapper = Mockito
						.mockConstruction(ClientProcessWrapper.class, (mock, context) -> {
							doNothing().when(mock).createProcessAndClient(any(boolean.class),
									nullable(SymlinkHelper.class), any(int.class), nullable(String.class),
									nullable(String.class), nullable(String.class), any(boolean.class),
									any(String.class), any(String.class));
							doNothing().when(mock).shutdown(false);
							when(mock.getSocketClient()).thenReturn(scMock);

						});
				MockedConstruction<PyTranslator> mockPYT = Mockito.mockConstruction(PyTranslator.class, (mock, context) -> {
					//doNothing().when(mock).setSocketClient(scMock);
					when(mock.runDirectPy(anyString())).thenReturn(fileNames);
					doNothing().when(mock).runEmptyPy(any());
					when(mock.runSmssWrapperEval(any(String.class))).thenReturn(fileNames);

				})) {
			
			// used in verifyModelProps
			u.when(()-> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
			when(modelEmbedder.getSmssProp()).thenReturn(embedderProps);	
						
			Map<String, Object> methodParams = new HashMap<>();
			methodParams.put("indexClass", indexClass);
			fileNames.forEach(fileName -> assertTrue(Files.exists(docDirPath.resolve(fileName))));
			List<Map<String, Object>> fileList = engine.listDocuments(methodParams);
			assertEquals(fileNames.size(), fileList.size());
			for (int fileIdx = 0; fileIdx < fileNames.size(); fileIdx++) {
				String currFileName = fileNames.get(fileIdx);
				Map<String, Object> fileData = fileList.get(fileIdx);
				assertEquals(currFileName, fileData.get("fileName"));
				assertEquals(0.0, fileData.get("fileSize"));
				LocalDateTime fileDateTime = LocalDateTime.parse((String) fileData.get("lastModified"),
						DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
				LocalDate fileDate = fileDateTime.toLocalDate();
				LocalDate todaysDate = LocalDate.now();
				assertEquals(todaysDate, fileDate); // checking date, omitting time
			}
			Properties updateEngineProps = engine.getSmssProp();
			assertNotNull(updateEngineProps);
			assertTrue(updateEngineProps.containsKey(Constants.MODEL));
			assertEquals(embedderModel, updateEngineProps.get(Constants.MODEL));
			assertTrue(updateEngineProps.containsKey(IModelEngine.MODEL_TYPE));
			assertEquals(embedderModelType, updateEngineProps.get(IModelEngine.MODEL_TYPE));
		}
	}
	
	@Test
	void testListAllRecords() throws Exception {
		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties

		// model embedder properties in verifyModelProps
		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		Properties embedderProps = new Properties();
		embedderProps.setProperty(Constants.MODEL, embedderModel);
		embedderProps.setProperty(IModelEngine.MODEL_TYPE, embedderModelType);
		
		SocketClient scMock = mock(SocketClient.class);
		when(scMock.isConnected()).thenReturn(true);
				
		String key1 = "key1";
		String value1 = "value1";
		List<Map<String, Object>> expectedOutput = new Vector<>();
		Map<String, Object> output1 = new HashMap<>();
		{
			{
				output1.put(key1, value1);
			}
			expectedOutput.add(new HashMap<>(output1));
			expectedOutput.add(new HashMap<>(output1));
			expectedOutput.add(new HashMap<>(output1));
		}
		
		try (MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);
				MockedConstruction<ClientProcessWrapper> mockWrapper = Mockito
						.mockConstruction(ClientProcessWrapper.class, (mock, context) -> {
							doNothing().when(mock).createProcessAndClient(any(boolean.class),
									nullable(SymlinkHelper.class), any(int.class), nullable(String.class),
									nullable(String.class), nullable(String.class), any(boolean.class),
									any(String.class), any(String.class));
							doNothing().when(mock).shutdown(false);
							when(mock.getSocketClient()).thenReturn(scMock);

						});
				MockedConstruction<PyTranslator> mockPYT = Mockito.mockConstruction(PyTranslator.class, (mock, context) -> {
					//doNothing().when(mock).setSocketClient(scMock);
					doNothing().when(mock).runEmptyPy(any());
					when(mock.runScript(any())).thenReturn("true");
					when(mock.runSmssWrapperEval(any(String.class))).thenReturn(expectedOutput);
					when(mock.runDirectPy(anyString())).thenReturn(expectedOutput);
				})) {
			
			// used in verifyModelProps & addEmbeddings
			u.when(()-> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
			u.when(()-> Utility.getBaseFolder()).thenReturn("");
			u.when(()-> Utility.normalizeParam(any(String.class))).thenReturn(tempDir.toString());

			// model embedder properties in verifyModelProps
			when(modelEmbedder.getSmssProp()).thenReturn(embedderProps);	
			
			List<Map<String, Object>> allRecords = engine.listAllRecords(new HashMap<>());
			assertEquals(expectedOutput.size(), allRecords.size());
			for (Map<String, Object> record : allRecords) {
				for (Map.Entry<String, Object> entry : record.entrySet()) {
					String recordKey = entry.getKey();
					String recordValue = entry.getValue().toString();
					assertEquals(key1, recordKey);
					assertEquals(value1, recordValue);
				}
			}
			
			Properties updateEngineProps = engine.getSmssProp();
			assertNotNull(updateEngineProps);
			assertTrue(updateEngineProps.containsKey(Constants.MODEL));
			assertEquals(embedderModel, updateEngineProps.get(Constants.MODEL));
			assertTrue(updateEngineProps.containsKey(IModelEngine.MODEL_TYPE));
			assertEquals(embedderModelType, updateEngineProps.get(IModelEngine.MODEL_TYPE));
		}
	}
	
	@Test
	void testGetVectorDatabaseType() {
		assertEquals(VectorDatabaseTypeEnum.FAISS, engine.getVectorDatabaseType());
	}
	
	@Test
	void testProcessOrFilter() {
		OrQueryFilter orFilter = new OrQueryFilter();
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("FAISS_TABLE_NAME__TOKEN", ">", 10));
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("FAISS_TABLE_NAME__TOKEN", "<", 5));
		String filter = engine.processOrQueryFilter(orFilter).toString();
		assertEquals("(FAISS_TABLE_NAME__TOKEN>10 or FAISS_TABLE_NAME__TOKEN<5)", filter);
	}
	
	@Test
	void testProcessAndQueryFilter() {
		AndQueryFilter andFilter = new AndQueryFilter();
		andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("FAISS_TABLE_NAME__IS_LATEST", "==", true));
		andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("FAISS_TABLE_NAME__IS_DELETED", "==", false));
		String filter = engine.processAndQueryFilter(andFilter).toString();
		assertEquals("(FAISS_TABLE_NAME__IS_LATEST==True and FAISS_TABLE_NAME__IS_DELETED==False)", filter);
	}
	
	@Test
	void testProcessBetweenQueryFilter() {
		BetweenQueryFilter betweenFilter = new BetweenQueryFilter();
		betweenFilter.setColumn(new QueryColumnSelector("FAISS_TABLE_NAME__COL1"));
		betweenFilter.setStart(4);
		betweenFilter.setEnd(8);
		String filter = engine.processBetweenQueryFilter(betweenFilter).toString();
		assertEquals("COL1  BETWEEN  4  AND  8", filter);
	}
	
	@Test
	void testProcessSimpleQueryFilter() {
		//COL_TO_COL
		SimpleQueryFilter simpleFilter = SimpleQueryFilter.makeColToColFilter(
				new QueryColumnSelector("FAISS_TABLE_NAME__COL1"), ">",
				new QueryColumnSelector("FAISS_TABLE_NAME__COL2"));
		String filter = engine.processSimpleQueryFilter(simpleFilter).toString();
		assertEquals("FAISS_TABLE_NAME__COL1 > FAISS_TABLE_NAME__COL2", filter);
		
		// COL_TO_VAL
		simpleFilter = SimpleQueryFilter.makeColToValFilter("FAISS_TABLE_NAME__COL1", "==", 10);
		filter = engine.processSimpleQueryFilter(simpleFilter).toString();
		assertEquals("FAISS_TABLE_NAME__COL1==10", filter);
		
		// VAL_TO_COL
		NounMetadata lnm = new NounMetadata(new QueryColumnSelector("FAISS_TABLE_NAME__COL1"), PixelDataType.COLUMN);
		NounMetadata rnm = new NounMetadata(10, PixelDataType.CONST_INT);
		simpleFilter = new SimpleQueryFilter(rnm, ">", lnm);
		filter = engine.processSimpleQueryFilter(simpleFilter).toString();
		assertEquals("FAISS_TABLE_NAME__COL1<10", filter);
	}
	
	@Test
	void testAddSelectorToSelectorFilter() {
		NounMetadata lnm = new NounMetadata(new QueryColumnSelector("FAISS_TABLE_NAME__COL1"), PixelDataType.COLUMN);
		NounMetadata rnm = new NounMetadata(new QueryColumnSelector("FAISS_TABLE_NAME__COL2"), PixelDataType.COLUMN);
		String filter = engine.addSelectorToSelectorFilter(lnm, rnm, ">").toString();
		assertEquals("FAISS_TABLE_NAME__COL1 > FAISS_TABLE_NAME__COL2", filter);
	}
	
	@Test
	void testAddSelectorToValuesFilter() {
		NounMetadata lnm = new NounMetadata(new QueryColumnSelector("FAISS_TABLE_NAME__COL1"), PixelDataType.COLUMN);
		NounMetadata rnm = new NounMetadata(10, PixelDataType.CONST_INT);
		String filter = engine.addSelectorToValuesFilter(lnm, rnm, "==").toString();
		assertEquals("FAISS_TABLE_NAME__COL1==10", filter);
	}
	
	@Test
	void testProcessSelector() {
		boolean addProcessedColumn = false;
		QueryConstantSelector constantSelector = new QueryConstantSelector(10);
		String output = engine.processSelector(constantSelector, addProcessedColumn);
		assertEquals("10", output);
		
		constantSelector = new QueryConstantSelector(true);
		output = engine.processSelector(constantSelector, addProcessedColumn);
		assertEquals("True", output);
		
		QueryColumnSelector columnSelector = new QueryColumnSelector("FAISS_TABLE_NAME__COL1");
		output = engine.processColumnSelector(columnSelector, true);
		assertEquals("COL1", output);
	}
	
	@Test
	void testProcessConstantSelector() {
		QueryConstantSelector selector = new QueryConstantSelector(10);
		String output = engine.processConstantSelector(selector);
		assertEquals("10", output);
		
		selector = new QueryConstantSelector(true);
		output = engine.processConstantSelector(selector);
		assertEquals("True", output);
	}
	
	@Test
	void testProcessColumnSelector() {
		String tableName = "FAISS_TABLE_NAME";
		QueryColumnSelector selector = new QueryColumnSelector(tableName + "__COL1");
		String output = engine.processColumnSelector(selector, true);
		assertEquals("COL1", output);
	}
	
	void openEngine(Path tempDir, FaissDatabaseEngine engine, Map<String, String> extraProps) throws Exception {
		Properties testProps = new Properties();
		String url = "http://fake.url/";
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
		String contentLength = "10";
		String contentOverlap = "10";
		String keepInputOutput = "false";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineAlias);
		testProps.setProperty(Constants.CONTENT_LENGTH, contentLength);
		testProps.setProperty(Constants.CONTENT_OVERLAP, contentOverlap);
		testProps.setProperty(Constants.KEEP_INPUT_OUTPUT, keepInputOutput);
		if (extraProps != null) {
			for (Map.Entry<String, String> extraPropsEntry : extraProps.entrySet()) {
				String key = extraPropsEntry.getKey();
				String prop = extraPropsEntry.getValue();
				testProps.setProperty(key, prop);
			}
		}

		String engineNameAndId = SmssUtilities.getUniqueName(testEngineAlias, testEngine);
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(engineNameAndId);
		Path engineAssetFolder = engineFolder.resolve("assets");
		Path engineVersionFolder = engineFolder.resolve("version");

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class)
			) {
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineAssetFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineVersionFolder.toString());

				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine, testEngineAlias))
						.thenReturn(engineAssetFolder.toString());
				
				engine.open(testProps);
				assertTrue(Files.exists(engineAssetFolder));
				Properties engineProps = engine.getSmssProp();
				for (Entry<Object, Object> testProp : testProps.entrySet()) {
					assertTrue(engineProps.containsKey(testProp.getKey()));
					assertTrue(engineProps.containsValue(testProp.getValue()));
				}
			}
		}
	}
}
