/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.engine.impl.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.gson.Gson;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Vector;
import org.apache.hc.core5.http.ContentType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.om.Insight;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineUtility;
import prerna.util.Utility;

public class OpenSearchRestVectorDatabaseEngineUnitTests {
	private Insight insight;
	private OpenSearchRestVectorDatabaseEngine engine;
	private IModelEngine modelEmbedder;

	@BeforeEach
	void setUp() {
		engine = new OpenSearchRestVectorDatabaseEngine();
		insight = mock(Insight.class);
		modelEmbedder = mock(IModelEngine.class);
	}

	@SuppressWarnings("unchecked")
	@Test
	void testOpen(@TempDir Path tempDir) throws Exception {
		Properties testProps = new Properties();
		String url = "http://fake.url/";
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
		String testAPIKey = "TEST_API_KEY";
		String testAPIKeyId = "TEST_API_KEY_ID";
		String contentLength = "100";
		String contentOverlap = "20";
		String chunkUnit = "tokens"; // must be tokens or characters
		String efConstructionInput = "20.0"; // must be floating-point value
		String testMethodName = "TEST_METHOD_NAME";
		testProps.setProperty(Constants.HOSTNAME, url);
		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineAlias);
		testProps.setProperty(Constants.CONTENT_LENGTH, contentLength);
		testProps.setProperty(Constants.CONTENT_OVERLAP, contentOverlap);
		testProps.setProperty(Constants.KEEP_INPUT_OUTPUT, "false");
		testProps.setProperty(Constants.DEFAULT_CHUNK_UNIT, chunkUnit);
		testProps.setProperty(Constants.API_KEY, testAPIKey);
		testProps.setProperty(Constants.API_KEY_ID, testAPIKeyId);
		testProps.setProperty("EF_CONSTRUCTION", efConstructionInput);
		testProps.setProperty("METHOD_NAME", testMethodName);

		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER)
				.resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path schemaPath = engineFolder.resolve("schema");

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);
					MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);) {

				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineFolder.toString());

				// will assure doesIndexExist returns false and enters createIndex
				hhu.when(() -> HttpHelperUtility.headRequest(any(String.class), any(Map.class), nullable(String.class),
						nullable(String.class), nullable(String.class)))
						.thenThrow(new IllegalArgumentException("testing"));

				// in createIndex, also works for updateIndexMapping
				Map<String, Object> parseResponseMap = new HashMap<>();
				{
					parseResponseMap.put("acknowledged", true);
				}
				hhu.when(() -> HttpHelperUtility.putRequestStringBody(any(String.class), nullable(Map.class),
						any(String.class), any(ContentType.class), nullable(String.class), nullable(String.class),
						nullable(String.class))).thenReturn(new Gson().toJson(parseResponseMap));

				engine.open(testProps);
				Properties engineProperties = engine.getSmssProp();
				assertTrue(Files.exists(schemaPath));
				assertNotNull(engineProperties);
				assertFalse(engineProperties.isEmpty());
				assertFalse(testProps.isEmpty());
				for (Entry<Object, Object> testProp : testProps.entrySet()) {
					assertTrue(engineProperties.containsKey(testProp.getKey()));
					assertTrue(engineProperties.containsValue(testProp.getValue()));
				}
			}
		}
	}

	@Test
	void testGetDefaultDistanceMethod() {
		assertEquals("cosinesimil", engine.getDefaultDistanceMethod());
	}

	@Test
	void testAddEmbeddings(@TempDir Path tempDir) throws Exception {
		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties

		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";

		try (MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);
				MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class)) {
			// used in verifyModelProps & addEmbeddings
			u.when(() -> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
			Properties embedderProps = new Properties();
			embedderProps.setProperty(Constants.MODEL, embedderModel);
			embedderProps.setProperty(IModelEngine.MODEL_TYPE, embedderModelType);
			when(modelEmbedder.getSmssProp()).thenReturn(embedderProps);

			// used in vectorCsvTable.generateAndAssignEmbeddings
			EmbeddingsModelEngineResponse outputMocked = mock(EmbeddingsModelEngineResponse.class);
			when(modelEmbedder.embeddings(new Vector<String>(), insight, null)).thenReturn(outputMocked);
			when(outputMocked.getResponse()).thenReturn(new Vector<List<Double>>());

			Map<String, Object> response = new HashMap<>();
			{
				response.put("took", 5);
				response.put("errors", false);
			}

			hhu.when(() -> HttpHelperUtility.postRequestStringBody(any(String.class), any(Map.class), any(String.class),
					any(ContentType.class), nullable(String.class), nullable(String.class), nullable(String.class)))
					.thenReturn(new Gson().toJson(response));

			engine.addEmbeddings(new VectorDatabaseCSVTable(), insight, null);
			Properties updateEngineProps = engine.getSmssProp();
			assertNotNull(updateEngineProps);
			assertTrue(updateEngineProps.containsKey(Constants.MODEL));
			assertEquals(embedderModel, updateEngineProps.get(Constants.MODEL));
			assertTrue(updateEngineProps.containsKey(IModelEngine.MODEL_TYPE));
			assertEquals(embedderModelType, updateEngineProps.get(IModelEngine.MODEL_TYPE));
			assertTrue(updateEngineProps.containsKey(Constants.MAX_TOKENS));
			assertEquals("None", updateEngineProps.get(Constants.MAX_TOKENS));
			assertTrue(updateEngineProps.containsKey(Constants.MAX_TOKENS));
			assertEquals("", updateEngineProps.get(Constants.KEYWORD_ENGINE_ID));
		}
	}

	@Test
	void testAddEmbedderNoInsight(@TempDir Path tempDir) throws Exception {
		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties

		try (MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);) {
			// used in verifyModelProps & addEmbeddings
			u.when(() -> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);

			// model embedder properties
			Properties embedderProps = new Properties();
			embedderProps.setProperty(Constants.MODEL, "embedder_model");
			embedderProps.setProperty(IModelEngine.MODEL_TYPE, "embedder_model_type");
			when(modelEmbedder.getSmssProp()).thenReturn(embedderProps);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> engine.addEmbeddings(new VectorDatabaseCSVTable(), null, null));
			assertEquals("Insight must be provided to run Model Engine Encoder", e.getMessage());
		}
	}

	@Test
	void testAddEmbeddingsNoHTTPResponse(@TempDir Path tempDir) throws Exception {
		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties

		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";

		try (MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);
				MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class)) {
			// used in verifyModelProps & addEmbeddings
			u.when(() -> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
			Properties embedderProps = new Properties();
			embedderProps.setProperty(Constants.MODEL, embedderModel);
			embedderProps.setProperty(IModelEngine.MODEL_TYPE, embedderModelType);
			when(modelEmbedder.getSmssProp()).thenReturn(embedderProps);

			// used in vectorCsvTable.generateAndAssignEmbeddings
			EmbeddingsModelEngineResponse outputMocked = mock(EmbeddingsModelEngineResponse.class);
			when(modelEmbedder.embeddings(new Vector<String>(), insight, null)).thenReturn(outputMocked);
			when(outputMocked.getResponse()).thenReturn(new Vector<List<Double>>());

			Map<String, Object> response = null;

			hhu.when(() -> HttpHelperUtility.postRequestStringBody(any(String.class), any(Map.class), any(String.class),
					any(ContentType.class), nullable(String.class), nullable(String.class), nullable(String.class)))
					.thenReturn(response);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> engine.addEmbeddings(new VectorDatabaseCSVTable(), insight, null));
			assertEquals("Received no response from open search endpoint", e.getMessage());
		}
	}

	@Test
	void removeDocument(@TempDir Path tempDir) throws Exception {
		openEngine(tempDir, engine, null); // set initial properties
		// both objects needed for method call
		Map<String, Object> parameters = new HashMap<>();
		String indexClass = "TEST_INDEX_CLASS";
		parameters.put("indexClass", indexClass);

		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER)
				.resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path docDirPath = Paths.get(engineFolder.toString(), "schema", indexClass, "documents");
		Files.createDirectories(docDirPath);
		String fileName1 = "newFile1.txt";
		String fileName2 = "newFile2.txt";
		Files.createFile(docDirPath.resolve(fileName1));
		Files.createFile(docDirPath.resolve(fileName2));
		List<String> fileNames = new Vector<>();
		fileNames.add(fileName1);
		fileNames.add(fileName2);

		try (MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);) {

			Map<String, Object> response = new HashMap<>();
			{
				response.put("deleted", fileNames);
				response.put("failures", new Vector<>());
			}
			String responseString = new Gson().toJson(response);
			// we sub in ANY string for request body and url
			hhu.when(() -> HttpHelperUtility.postRequestStringBody(any(String.class), any(Map.class), any(String.class),
					any(ContentType.class), nullable(String.class), nullable(String.class), nullable(String.class)))
					.thenReturn(responseString);

			fileNames.forEach(fileName -> assertTrue(Files.exists(docDirPath.resolve(fileName))));
			engine.removeDocument(fileNames, parameters);
			fileNames.forEach(fileName -> assertFalse(Files.exists(docDirPath.resolve(fileName))));
		}
	}

	@Test
	void nearestNeighborCall(@TempDir Path tempDir) throws Exception {
		Number limit = 1;
		String searchStatement = "searchStatement";
		String testEmbedderId = "123-456-789";
		List<List<Double>> mockedEmbeddingList = new Vector<>();
		List<Double> mockedEmbeddings = new Vector<>();
		{
			mockedEmbeddings.add(new Double(0.2));
			mockedEmbeddings.add(new Double(0.4));
			mockedEmbeddings.add(new Double(0.6));
			mockedEmbeddings.add(new Double(0.8));

			mockedEmbeddingList.add(mockedEmbeddings);
		}
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties

		try (MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);
				MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class)) {
			// used in verifyModelProps & addEmbeddings
			u.when(() -> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);

			// model embedder properties
			Properties embedderProps = new Properties();
			embedderProps.setProperty(Constants.MODEL, "embedder_model");
			embedderProps.setProperty(IModelEngine.MODEL_TYPE, "embedder_model_type");
			when(modelEmbedder.getSmssProp()).thenReturn(embedderProps);
			///// all previous statements were for verifyModelProps()

			// mocking getting embeddings
			EmbeddingsModelEngineResponse responseMocked = mock(EmbeddingsModelEngineResponse.class);
			when(modelEmbedder.embeddings(Arrays.asList(new String[]{searchStatement}), insight, null))
					.thenReturn(responseMocked);
			when(responseMocked.getResponse()).thenReturn(mockedEmbeddingList);

			// creating the json response from http request
			String testSource = "TEST_SOURCE";
			String testModality = "TEST_MODALITY";
			String testDivider = "TEST_DIVIDER";
			String testPart = "TEST_PART";
			long testTokens = 123456789;
			String testContent = "TEST_CONTENT";
			Map<String, Object> response = new HashMap<>();
			{
				Map<String, Object> hitsMap = new HashMap<>();
				{
					List<Map<String, Object>> hits = new Vector<>();
					{
						Map<String, Object> singleHit = new HashMap<>();
						{
							Map<String, Object> source = new HashMap<>();
							source.put(VectorDatabaseCSVTable.SOURCE, testSource);
							source.put(VectorDatabaseCSVTable.MODALITY, testModality);
							source.put(VectorDatabaseCSVTable.DIVIDER, testDivider);
							source.put(VectorDatabaseCSVTable.PART, testPart);
							source.put(VectorDatabaseCSVTable.TOKENS, testTokens);
							source.put(VectorDatabaseCSVTable.CONTENT, testContent);
							singleHit.put("_source", source);
						}
						singleHit.put("_score", 2.2);
						// add it in multiple times
						hits.add(singleHit);
						hits.add(singleHit);
						hits.add(singleHit);
					}
					hitsMap.put("hits", hits);
				}
				response.put("hits", hitsMap);
			}
			String nearestNeigborResponse = new Gson().toJson(response);

			hhu.when(() -> HttpHelperUtility.postRequestStringBody(any(String.class), any(Map.class), any(String.class),
					any(ContentType.class), nullable(String.class), nullable(String.class), nullable(String.class)))
					.thenReturn(nearestNeigborResponse);

			List<Map<String, Object>> returnMetadata = engine.nearestNeighborCall(insight, searchStatement, limit,
					new HashMap<>());
			assertEquals(3, returnMetadata.size());
			for (Map<String, Object> metadata : returnMetadata) {
				assertEquals(testSource, metadata.get(VectorDatabaseCSVTable.SOURCE));
				assertEquals(testModality, metadata.get(VectorDatabaseCSVTable.MODALITY));
				assertEquals(testDivider, metadata.get(VectorDatabaseCSVTable.DIVIDER));
				assertEquals(testPart, metadata.get(VectorDatabaseCSVTable.PART));
				assertEquals(testTokens, metadata.get(VectorDatabaseCSVTable.TOKENS));
				assertEquals(testContent, metadata.get(VectorDatabaseCSVTable.CONTENT));
				assertEquals(2.2, metadata.get("Score"));
			}
		}
	}

	@Test
	void testNearestNeighborCallNoInsight() {
		Number limit = 1;
		String searchStatement = "searchStatement";
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> engine.nearestNeighborCall(null, searchStatement, limit, null));
		assertEquals("Insight must be provided to run Model Engine Encoder", e.getMessage());
	}

	@Test
	void listDocuments(@TempDir Path tempDir) throws Exception {
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties

		Map<String, Object> parameters = new HashMap<>();
		String indexClass = "TEST_INDEX_CLASS";
		parameters.put("indexClass", indexClass);

		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER)
				.resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path docDirPath = Paths.get(engineFolder.toString(), "schema", indexClass, "documents");
		Files.createDirectories(docDirPath);
		// create 4 new files: newFile1 ... newFile4.txt
		List<String> fileNames = new Vector<>();
		for (int fileNum = 1; fileNum < 5; fileNum++) {
			String fileName = "newFile" + fileNum + ".txt";
			fileNames.add(fileName);
			Path newFilePath = docDirPath.resolve(fileName);
			Files.createFile(newFilePath);
		}

		try (MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class)) {

			// create http-request json response
			Map<String, Object> response = new HashMap<>();
			{
				Map<String, Object> agg = new HashMap<>();
				{
					Map<String, Object> sources = new HashMap<>();
					{
						List<Map<String, Object>> buckets = new Vector<>();
						{
							for (String fileName : fileNames) {
								Map<String, Object> bucket = new HashMap<>();
								bucket.put("key", fileName);
								buckets.add(bucket);
							}
						}

						sources.put("buckets", buckets);
					}
					agg.put("unique_sources", sources);
				}
				response.put("aggregations", agg);
			}
			String httpResponse = new Gson().toJson(response);

			hhu.when(() -> HttpHelperUtility.postRequestStringBody(any(String.class), any(Map.class), any(String.class),
					any(ContentType.class), nullable(String.class), nullable(String.class), nullable(String.class)))
					.thenReturn(httpResponse);

			List<Map<String, Object>> fileList = engine.listDocuments(parameters);
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
		}
	}

	@Test
	void listAllRecords(@TempDir Path tempDir) throws Exception {
		openEngine(tempDir, engine, null); // set initial properties
		// creating the json response from http request
		String testSource = "TEST_SOURCE";
		String testModality = "TEST_MODALITY";
		String testDivider = "TEST_DIVIDER";
		String testPart = "TEST_PART";
		long testTokens = 123456789;
		String testContent = "TEST_CONTENT";
		Map<String, Object> response = new HashMap<>();
		{
			Map<String, Object> hitsMap = new HashMap<>();
			{
				List<Map<String, Object>> hits = new Vector<>();
				{
					Map<String, Object> singleField = new HashMap<>();
					{
						Map<String, Object> fields = new HashMap<>();
						fields.put(VectorDatabaseCSVTable.SOURCE, testSource);
						fields.put(VectorDatabaseCSVTable.MODALITY, testModality);
						fields.put(VectorDatabaseCSVTable.DIVIDER, testDivider);
						fields.put(VectorDatabaseCSVTable.PART, testPart);
						fields.put(VectorDatabaseCSVTable.TOKENS, testTokens);
						fields.put(VectorDatabaseCSVTable.CONTENT, testContent);
						singleField.put("fields", fields);
					}
					// add it in multiple times
					hits.add(singleField);
					hits.add(singleField);
					hits.add(singleField);
				}
				hitsMap.put("hits", hits);
			}
			response.put("hits", hitsMap);
		}
		String httpResponse = new Gson().toJson(response);
		try (MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class)) {
			hhu.when(() -> HttpHelperUtility.postRequestStringBody(any(String.class), any(Map.class), any(String.class),
					any(ContentType.class), nullable(String.class), nullable(String.class), nullable(String.class)))
					.thenReturn(httpResponse);

			List<Map<String, Object>> listOfRecords = engine.listAllRecords(new HashMap<>());
			assertEquals(3, listOfRecords.size());
			for (Map<String, Object> record : listOfRecords) {
				assertEquals(testSource, record.get(VectorDatabaseCSVTable.SOURCE));
				assertEquals(testModality, record.get(VectorDatabaseCSVTable.MODALITY));
				assertEquals(testDivider, record.get(VectorDatabaseCSVTable.DIVIDER));
				assertEquals(testPart, record.get(VectorDatabaseCSVTable.PART));
				assertEquals(testTokens, record.get(VectorDatabaseCSVTable.TOKENS));
				assertEquals(testContent, record.get(VectorDatabaseCSVTable.CONTENT));
			}
		}
	}

	@Test
	void testGetVectorDatabaseType() {
		assertEquals(VectorDatabaseTypeEnum.OPEN_SEARCH, engine.getVectorDatabaseType());
	}

	void openEngine(Path tempDir, OpenSearchRestVectorDatabaseEngine engine, Map<String, String> extraProps)
			throws Exception {
		Properties testProps = new Properties();
		String url = "http://fake.url/";
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
		String testAPIKey = "TEST_API_KEY";
		String testAPIKeyId = "TEST_API_KEY_ID";
		String contentLength = "100";
		String contentOverlap = "20";
		String chunkUnit = "tokens"; // must be tokens or characters
		String efConstructionInput = "20.0"; // must be floating-point value
		String testMethodName = "TEST_METHOD_NAME";
		testProps.setProperty(Constants.HOSTNAME, url);
		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineAlias);
		testProps.setProperty(Constants.CONTENT_LENGTH, contentLength);
		testProps.setProperty(Constants.CONTENT_OVERLAP, contentOverlap);
		testProps.setProperty(Constants.KEEP_INPUT_OUTPUT, "false");
		testProps.setProperty(Constants.DEFAULT_CHUNK_UNIT, chunkUnit);
		testProps.setProperty(Constants.API_KEY, testAPIKey);
		testProps.setProperty(Constants.API_KEY_ID, testAPIKeyId);
		testProps.setProperty("EF_CONSTRUCTION", efConstructionInput);
		testProps.setProperty("METHOD_NAME", testMethodName);

		if (extraProps != null) {
			for (Map.Entry<String, String> extraPropsEntry : extraProps.entrySet()) {
				String key = extraPropsEntry.getKey();
				String prop = extraPropsEntry.getValue();
				testProps.setProperty(key, prop);
			}
		}

		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER)
				.resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path schemaPath = engineFolder.resolve("schema");

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);
					MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);) {

				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineFolder.toString());

				// will assure doesIndexExist returns false and enters createIndex
				hhu.when(() -> HttpHelperUtility.headRequest(any(String.class), any(Map.class), nullable(String.class),
						nullable(String.class), nullable(String.class)))
						.thenThrow(new IllegalArgumentException("testing"));

				// in createIndex, also works for updateIndexMapping
				Map<String, Object> parseResponseMap = new HashMap<>();
				{
					parseResponseMap.put("acknowledged", true);
				}
				hhu.when(() -> HttpHelperUtility.putRequestStringBody(any(String.class), nullable(Map.class),
						any(String.class), any(ContentType.class), nullable(String.class), nullable(String.class),
						nullable(String.class))).thenReturn(new Gson().toJson(parseResponseMap));

				engine.open(testProps);
				Properties engineProperties = engine.getSmssProp();
				assertTrue(Files.exists(schemaPath));
				assertNotNull(engineProperties);
				assertFalse(engineProperties.isEmpty());
				assertFalse(testProps.isEmpty());
				for (Entry<Object, Object> testProp : testProps.entrySet()) {
					assertTrue(engineProperties.containsKey(testProp.getKey()));
					assertTrue(engineProperties.containsValue(testProp.getValue()));
				}
			}
		}
	}
}
