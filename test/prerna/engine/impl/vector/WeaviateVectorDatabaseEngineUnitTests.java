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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Vector;
import java.util.function.Function;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import io.weaviate.client6.v1.api.WeaviateClient;
import io.weaviate.client6.v1.api.collections.CollectionHandle;
import io.weaviate.client6.v1.api.collections.WeaviateCollectionsClient;
import io.weaviate.client6.v1.api.collections.WeaviateObject;
import io.weaviate.client6.v1.api.collections.aggregate.AggregateResponseGroup;
import io.weaviate.client6.v1.api.collections.aggregate.AggregateResponseGrouped;
import io.weaviate.client6.v1.api.collections.aggregate.GroupBy;
import io.weaviate.client6.v1.api.collections.aggregate.GroupedBy;
import io.weaviate.client6.v1.api.collections.aggregate.WeaviateAggregateClient;
import io.weaviate.client6.v1.api.collections.data.DeleteManyResponse;
import io.weaviate.client6.v1.api.collections.data.InsertManyResponse;
import io.weaviate.client6.v1.api.collections.data.WeaviateDataClient;
import io.weaviate.client6.v1.api.collections.query.Filter;
import io.weaviate.client6.v1.api.collections.query.QueryMetadata;
import io.weaviate.client6.v1.api.collections.query.QueryResponse;
import io.weaviate.client6.v1.api.collections.query.WeaviateQueryClient;
import prerna.SemossUnitTest;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineUtility;
import prerna.util.Utility;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class WeaviateVectorDatabaseEngineUnitTests extends SemossUnitTest {
	private Insight insight;
	private WeaviateVectorDatabaseEngine engine;
	private IModelEngine modelEmbedder;

	// v6 client sub-objects are exposed as public final fields / concrete classes,
	// so we mock the classes and inject them into the fields via reflection
	private WeaviateClient clientMock;
	private WeaviateCollectionsClient collectionsMock;
	private CollectionHandle handleMock;
	private WeaviateDataClient dataMock;
	private WeaviateQueryClient queryMock;
	private WeaviateAggregateClient aggregateMock;

	final private String source = "source";
	final private String modality = "modality";
	final private String divider = "divider";
	final private String part = "part";
	final private String content = "content";

	@BeforeEach
	void setUp() throws IOException {
		FileUtils.cleanDirectory(tempDir.toFile());

		engine = new WeaviateVectorDatabaseEngine();
		insight = mock(Insight.class);
		modelEmbedder = mock(IModelEngine.class);
	}

	/**
	 * Sets a (possibly final) instance field via reflection - used to wire our mock
	 * sub-clients into the v6 client's public final fields.
	 */
	private static void setField(Object target, String fieldName, Object value) throws Exception {
		Field field = null;
		Class<?> current = target.getClass();
		while (current != null && field == null) {
			try {
				field = current.getDeclaredField(fieldName);
			} catch (NoSuchFieldException e) {
				current = current.getSuperclass();
			}
		}
		if (field == null) {
			throw new NoSuchFieldException(fieldName);
		}
		field.setAccessible(true);
		field.set(target, value);
	}

	/**
	 * Stubs the static v6 connection factory and wires up the collections / data /
	 * query mock chain so open() and the CRUD/search methods have something to call.
	 */
	private void wireWeaviate(MockedStatic<WeaviateClient> wc) throws Exception {
		clientMock = mock(WeaviateClient.class);
		wc.when(() -> WeaviateClient.connectToCustom(any())).thenReturn(clientMock);

		collectionsMock = mock(WeaviateCollectionsClient.class);
		setField(clientMock, "collections", collectionsMock);
		// createClass: collection does not exist yet, so create() is invoked
		when(collectionsMock.exists(anyString())).thenReturn(false);

		handleMock = mock(CollectionHandle.class);
		// createClass now defines properties, so it calls create(name, config)
		when(collectionsMock.create(anyString(), any())).thenReturn(handleMock);
		when(collectionsMock.use(anyString())).thenReturn(handleMock);

		dataMock = mock(WeaviateDataClient.class);
		queryMock = mock(WeaviateQueryClient.class);
		aggregateMock = mock(WeaviateAggregateClient.class);
		setField(handleMock, "data", dataMock);
		setField(handleMock, "query", queryMock);
		setField(handleMock, "aggregate", aggregateMock);
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
		String apiKey = "API_KEY";
		String weaviateClassname = "WEAVIATE_CLASS_NAME";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineAlias);
		testProps.setProperty(Constants.CONTENT_LENGTH, contentLength);
		testProps.setProperty(Constants.CONTENT_OVERLAP, contentOverlap);
		testProps.setProperty(Constants.KEEP_INPUT_OUTPUT, keepInputOutput);
		testProps.setProperty(Constants.API_KEY, apiKey);
		testProps.setProperty(Constants.HOSTNAME, url);
		testProps.setProperty("WEAVIATE_CLASSNAME", weaviateClassname);

		String engineNameAndId = SmssUtilities.getUniqueName(testEngineAlias, testEngine);
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(engineNameAndId);
		Path engineAssetFolder = engineFolder.resolve("assets");
		Path engineVersionFolder = engineFolder.resolve("version");

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class)) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<WeaviateClient> wc = Mockito.mockStatic(WeaviateClient.class)) {

				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineAssetFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineVersionFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineAssetFolder.toString());

				wireWeaviate(wc);

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
	void testOpenNoHostname() throws Exception {
		Properties testProps = new Properties();
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
		String contentLength = "10";
		String contentOverlap = "10";
		String keepInputOutput = "false";
		String apiKey = "API_KEY";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineAlias);
		testProps.setProperty(Constants.CONTENT_LENGTH, contentLength);
		testProps.setProperty(Constants.CONTENT_OVERLAP, contentOverlap);
		testProps.setProperty(Constants.KEEP_INPUT_OUTPUT, keepInputOutput);
		testProps.setProperty(Constants.API_KEY, apiKey);
//		testProps.setProperty(Constants.HOSTNAME, url);

		String engineNameAndId = SmssUtilities.getUniqueName(testEngineAlias, testEngine);
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(engineNameAndId);
		Path engineAssetFolder = engineFolder.resolve("assets");
		Path engineVersionFolder = engineFolder.resolve("version");

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class)) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<WeaviateClient> wc = Mockito.mockStatic(WeaviateClient.class)) {

				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineAssetFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineVersionFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineAssetFolder.toString());

				IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> engine.open(testProps));
				assertEquals("Must define the host", e.getMessage());
			}
		}
	}

	@Test
	void testOpenNoAPIKey() throws Exception {
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
//		testProps.setProperty(Constants.API_KEY, apiKey);
		testProps.setProperty(Constants.HOSTNAME, url);

		String engineNameAndId = SmssUtilities.getUniqueName(testEngineAlias, testEngine);
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(engineNameAndId);
		Path engineAssetFolder = engineFolder.resolve("assets");
		Path engineVersionFolder = engineFolder.resolve("version");

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class)) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<WeaviateClient> wc = Mockito.mockStatic(WeaviateClient.class)) {

				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineAssetFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineVersionFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineAssetFolder.toString());

				IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> engine.open(testProps));
				assertEquals("Must define the api key", e.getMessage());
			}
		}
	}

	@Test
	void testAddEmbeddings() throws Exception {
		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		String testEmbedderId = "123-456-789";

		Properties testProps = new Properties();
		String url = "http://fake.url/";
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
		String contentLength = "10";
		String contentOverlap = "10";
		String keepInputOutput = "false";
		String apiKey = "API_KEY";
		String weaviateClassname = "WEAVIATE_CLASS_NAME";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineAlias);
		testProps.setProperty(Constants.CONTENT_LENGTH, contentLength);
		testProps.setProperty(Constants.CONTENT_OVERLAP, contentOverlap);
		testProps.setProperty(Constants.KEEP_INPUT_OUTPUT, keepInputOutput);
		testProps.setProperty(Constants.API_KEY, apiKey);
		testProps.setProperty(Constants.HOSTNAME, url);
		testProps.setProperty("WEAVIATE_CLASSNAME", weaviateClassname);
		testProps.setProperty(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);

		String engineNameAndId = SmssUtilities.getUniqueName(testEngineAlias, testEngine);
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(engineNameAndId);
		Path engineAssetFolder = engineFolder.resolve("assets");
		Path engineVersionFolder = engineFolder.resolve("version");

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class)) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<WeaviateClient> wc = Mockito.mockStatic(WeaviateClient.class)) {

				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineAssetFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineVersionFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineAssetFolder.toString());

				wireWeaviate(wc);

				engine.open(testProps);

				try (MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);) {
					u.when(() -> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
					// model embedder properties
					Properties embedderProps = new Properties();
					embedderProps.setProperty(Constants.MODEL, embedderModel);
					embedderProps.setProperty(IModelEngine.MODEL_TYPE, embedderModelType);
					when(modelEmbedder.getSmssProp()).thenReturn(embedderProps);

					// used in addEmbeddings()
					VectorDatabaseCSVTable vectorCsvTable = mock();
					vectorCsvTable.rows = new Vector<>(); // empty list for rows
					doNothing().when(vectorCsvTable).generateAndAssignEmbeddings(modelEmbedder, insight);

					// nothing to insert -> empty response
					when(dataMock.insertMany(anyList())).thenReturn(
							new InsertManyResponse(0f, new ArrayList<>(), new ArrayList<>(), new ArrayList<>()));

					engine.addEmbeddings(vectorCsvTable, insight, null);

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

	@Test
	void testRemoveDocument() throws Exception {
		Properties testProps = new Properties();
		String url = "http://fake.url/";
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
		String contentLength = "10";
		String contentOverlap = "10";
		String keepInputOutput = "false";
		String apiKey = "API_KEY";
		String weaviateClassname = "WEAVIATE_CLASS_NAME";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineAlias);
		testProps.setProperty(Constants.CONTENT_LENGTH, contentLength);
		testProps.setProperty(Constants.CONTENT_OVERLAP, contentOverlap);
		testProps.setProperty(Constants.KEEP_INPUT_OUTPUT, keepInputOutput);
		testProps.setProperty(Constants.API_KEY, apiKey);
		testProps.setProperty(Constants.HOSTNAME, url);
		testProps.setProperty("WEAVIATE_CLASSNAME", weaviateClassname);

		String indexClass = "INDEX_CLASS";

		String engineNameAndId = SmssUtilities.getUniqueName(testEngineAlias, testEngine);
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(engineNameAndId);
		Path engineAssetFolder = engineFolder.resolve("assets");
		Path engineVersionFolder = engineFolder.resolve("version");

		Path docDirPath = Paths.get(engineFolder.toString(), "assets", "schema", indexClass, "documents");
		Files.createDirectories(docDirPath);
		String fileName = "newFile1.txt";
		List<String> fileNames = new Vector<>();
		fileNames.add(fileName);
		Path newFilePath = docDirPath.resolve(fileName);
		Files.createFile(newFilePath);

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class)) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<WeaviateClient> wc = Mockito.mockStatic(WeaviateClient.class)) {

				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineAssetFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineVersionFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineAssetFolder.toString());

				wireWeaviate(wc);

				engine.open(testProps);

				// used in removeDocument()
				when(dataMock.deleteMany(any(Filter.class)))
						.thenReturn(new DeleteManyResponse(0f, 0L, 1L, 1L, new ArrayList<>()));

				Map<String, Object> parameters = new HashMap<>();
				parameters.put("indexClass", indexClass);
				assertTrue(Files.exists(newFilePath));
				engine.removeDocument(fileNames, parameters);
				assertFalse(Files.exists(newFilePath));
			}
		}
	}

	@Test
	void testNearestNeighborCallHybrid() throws Exception {
		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		String testEmbedderId = "123-456-789";

		Properties testProps = new Properties();
		String url = "http://fake.url/";
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
		String contentLength = "10";
		String contentOverlap = "10";
		String keepInputOutput = "false";
		String apiKey = "API_KEY";
		String weaviateClassname = "WEAVIATE_CLASS_NAME";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineAlias);
		testProps.setProperty(Constants.CONTENT_LENGTH, contentLength);
		testProps.setProperty(Constants.CONTENT_OVERLAP, contentOverlap);
		testProps.setProperty(Constants.KEEP_INPUT_OUTPUT, keepInputOutput);
		testProps.setProperty(Constants.API_KEY, apiKey);
		testProps.setProperty(Constants.HOSTNAME, url);
		testProps.setProperty("WEAVIATE_CLASSNAME", weaviateClassname);
		testProps.setProperty(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		// hybrid search is on by default (ENABLE_HYBRID_SEARCH defaults to true)

		String engineNameAndId = SmssUtilities.getUniqueName(testEngineAlias, testEngine);
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(engineNameAndId);
		Path engineAssetFolder = engineFolder.resolve("assets");
		Path engineVersionFolder = engineFolder.resolve("version");

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class)) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<WeaviateClient> wc = Mockito.mockStatic(WeaviateClient.class)) {

				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineAssetFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineVersionFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineAssetFolder.toString());

				wireWeaviate(wc);

				engine.open(testProps);

				try (MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);) {
					u.when(() -> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
					Properties embedderProps = new Properties();
					embedderProps.setProperty(Constants.MODEL, embedderModel);
					embedderProps.setProperty(IModelEngine.MODEL_TYPE, embedderModelType);
					when(modelEmbedder.getSmssProp()).thenReturn(embedderProps);
					// used in getEmbeddingsFloat()
					EmbeddingsModelEngineResponse embeddingRespMock = mock();
					when(modelEmbedder.embeddings(any(List.class), any(Insight.class), nullable(Map.class)))
							.thenReturn(embeddingRespMock);
					List<List<Double>> emb = new Vector<>();
					emb.add(new Vector<>());
					when(embeddingRespMock.getResponse()).thenReturn(emb);

					// hybrid returns a single fused score
					QueryResponse response = buildQueryResponse(weaviateClassname, new QueryMetadata(null, null, 0.9f, null));
					when(queryMock.hybrid(anyString(), any(Function.class))).thenReturn(response);

					String searchStatement = "searchStatement";
					int limit = 1;
					List<Map<String, Object>> retOut = engine.nearestNeighborCall(insight, searchStatement, limit,
							new HashMap<>());
					assertEquals(1, retOut.size());
					for (Map<String, Object> outputMap : retOut) {
						assertEquals(source, outputMap.get(VectorDatabaseCSVTable.SOURCE));
						assertEquals(modality, outputMap.get(VectorDatabaseCSVTable.MODALITY));
						assertEquals(divider, outputMap.get(VectorDatabaseCSVTable.DIVIDER));
						assertEquals(part, outputMap.get(VectorDatabaseCSVTable.PART));
						assertEquals(content, outputMap.get(VectorDatabaseCSVTable.CONTENT));
						assertEquals(Float.valueOf(0.9f), outputMap.get("Score"));
						assertFalse(outputMap.containsKey("Distance"));
					}
				}
			}
		}
	}

	@Test
	void testNearestNeighborCallVectorOnly() throws Exception {
		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		String testEmbedderId = "123-456-789";

		Properties testProps = new Properties();
		String url = "http://fake.url/";
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
		String contentLength = "10";
		String contentOverlap = "10";
		String keepInputOutput = "false";
		String apiKey = "API_KEY";
		String weaviateClassname = "WEAVIATE_CLASS_NAME";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineAlias);
		testProps.setProperty(Constants.CONTENT_LENGTH, contentLength);
		testProps.setProperty(Constants.CONTENT_OVERLAP, contentOverlap);
		testProps.setProperty(Constants.KEEP_INPUT_OUTPUT, keepInputOutput);
		testProps.setProperty(Constants.API_KEY, apiKey);
		testProps.setProperty(Constants.HOSTNAME, url);
		testProps.setProperty("WEAVIATE_CLASSNAME", weaviateClassname);
		testProps.setProperty(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		// explicitly disable hybrid to exercise the vector-only path
		testProps.setProperty(AbstractVectorDatabaseEngine.ENABLE_HYBRID_SEARCH, "false");

		String engineNameAndId = SmssUtilities.getUniqueName(testEngineAlias, testEngine);
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(engineNameAndId);
		Path engineAssetFolder = engineFolder.resolve("assets");
		Path engineVersionFolder = engineFolder.resolve("version");

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class)) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<WeaviateClient> wc = Mockito.mockStatic(WeaviateClient.class)) {

				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineAssetFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineVersionFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineAssetFolder.toString());

				wireWeaviate(wc);

				engine.open(testProps);

				try (MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);) {
					u.when(() -> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
					Properties embedderProps = new Properties();
					embedderProps.setProperty(Constants.MODEL, embedderModel);
					embedderProps.setProperty(IModelEngine.MODEL_TYPE, embedderModelType);
					when(modelEmbedder.getSmssProp()).thenReturn(embedderProps);
					// used in getEmbeddingsFloat()
					EmbeddingsModelEngineResponse embeddingRespMock = mock();
					when(modelEmbedder.embeddings(any(List.class), any(Insight.class), nullable(Map.class)))
							.thenReturn(embeddingRespMock);
					List<List<Double>> emb = new Vector<>();
					emb.add(new Vector<>());
					when(embeddingRespMock.getResponse()).thenReturn(emb);

					// vector-only returns certainty (as Score) + distance
					QueryResponse response = buildQueryResponse(weaviateClassname, new QueryMetadata(5f, 10f, null, null));
					when(queryMock.nearVector(any(float[].class), any(Function.class))).thenReturn(response);

					String searchStatement = "searchStatement";
					int limit = 1;
					List<Map<String, Object>> retOut = engine.nearestNeighborCall(insight, searchStatement, limit,
							new HashMap<>());
					assertEquals(1, retOut.size());
					for (Map<String, Object> outputMap : retOut) {
						assertEquals(source, outputMap.get(VectorDatabaseCSVTable.SOURCE));
						assertEquals(modality, outputMap.get(VectorDatabaseCSVTable.MODALITY));
						assertEquals(divider, outputMap.get(VectorDatabaseCSVTable.DIVIDER));
						assertEquals(part, outputMap.get(VectorDatabaseCSVTable.PART));
						assertEquals(content, outputMap.get(VectorDatabaseCSVTable.CONTENT));
						assertEquals(Float.valueOf(10f), outputMap.get("Score"));
						assertEquals(Float.valueOf(5f), outputMap.get("Distance"));
					}
				}
			}
		}
	}

	/**
	 * Builds a real (record) query response holding a single result whose property
	 * values echo their names, mirroring what the engine reads back.
	 */
	private QueryResponse buildQueryResponse(String className, QueryMetadata metadata) {
		Map<String, Object> properties = new HashMap<>();
		properties.put(source, source);
		properties.put(divider, divider);
		properties.put(modality, modality);
		properties.put(part, part);
		properties.put(content, content);

		WeaviateObject<Map<String, Object>> object = new WeaviateObject<>("uuid", className, null, properties, null,
				null, null, metadata, null);
		List<WeaviateObject<Map<String, Object>>> objects = new ArrayList<>();
		objects.add(object);
		return new QueryResponse<>(objects, null);
	}

	@Test
	void testNearestNeighborCallNoInsight() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> engine.nearestNeighbor(null, null, null, null));
		assertEquals("Insight must be provided to run Model Engine Encoder", e.getMessage());
	}

	@Test
	void testListDocuments() throws Exception {
		Properties testProps = new Properties();
		String url = "http://fake.url/";
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
		String contentLength = "10";
		String contentOverlap = "10";
		String keepInputOutput = "false";
		String apiKey = "API_KEY";
		String weaviateClassname = "WEAVIATE_CLASS_NAME";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineAlias);
		testProps.setProperty(Constants.CONTENT_LENGTH, contentLength);
		testProps.setProperty(Constants.CONTENT_OVERLAP, contentOverlap);
		testProps.setProperty(Constants.KEEP_INPUT_OUTPUT, keepInputOutput);
		testProps.setProperty(Constants.API_KEY, apiKey);
		testProps.setProperty(Constants.HOSTNAME, url);
		testProps.setProperty("WEAVIATE_CLASSNAME", weaviateClassname);

		String indexClass = "INDEX_CLASS";

		String engineNameAndId = SmssUtilities.getUniqueName(testEngineAlias, testEngine);
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(engineNameAndId);
		Path engineAssetFolder = engineFolder.resolve("assets");
		Path engineVersionFolder = engineFolder.resolve("version");

		Path docDirPath = Paths.get(engineFolder.toString(), "assets", "schema", indexClass, "documents");
		Files.createDirectories(docDirPath);
		List<String> fileNames = new Vector<>();
		for (int fileNum = 1; fileNum < 5; fileNum++) {
			String fileName = "newFile" + fileNum + ".txt";
			fileNames.add(fileName);
			Path newFilePath = docDirPath.resolve(fileName);
			Files.createFile(newFilePath);
		}

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class)) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<WeaviateClient> wc = Mockito.mockStatic(WeaviateClient.class)) {

				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineAssetFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineVersionFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineAssetFolder.toString());

				wireWeaviate(wc);

				engine.open(testProps);

				// listDocuments groups on the source property in weaviate; return one group
				// per file created on disk
				List<AggregateResponseGroup<?>> groups = new ArrayList<>();
				for (String fileName : fileNames) {
					GroupedBy<String> groupedBy = new GroupedBy<>("source", fileName);
					groups.add(new AggregateResponseGroup<>(groupedBy, new HashMap<>(), 1L));
				}
				when(aggregateMock.overAll(any(GroupBy.class))).thenReturn(new AggregateResponseGrouped(groups));

				Map<String, Object> parameters = new HashMap<>();
				parameters.put("indexClass", indexClass);
				List<Map<String, Object>> docsOutput = engine.listDocuments(parameters);
				assertEquals(fileNames.size(), docsOutput.size());
				for (int fileIdx = 0; fileIdx < fileNames.size(); fileIdx++) {
					Map<String, Object> outputDoc = docsOutput.get(fileIdx);
					assertTrue(fileNames.contains(outputDoc.get("fileName")));
					assertEquals(0.0, outputDoc.get("fileSize"));
					LocalDateTime fileDateTime = LocalDateTime.parse((String) outputDoc.get("lastModified"),
							DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
					LocalDate fileDate = fileDateTime.toLocalDate();
					LocalDate todaysDate = LocalDate.now();
					assertEquals(todaysDate, fileDate); // checking date, omitting time
				}
			}
		}
	}

	@Test
	void testListAllRecords() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> engine.listAllRecords(null));
		assertEquals("This method has not been implemented yet", e.getMessage());
	}

	@Test
	void testGetVectorDatabaseType() {
		assertEquals(VectorDatabaseTypeEnum.WEAVIATE, engine.getVectorDatabaseType());
	}
}
