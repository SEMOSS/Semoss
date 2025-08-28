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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.gson.internal.LinkedTreeMap;
import io.weaviate.client.WeaviateAuthClient;
import io.weaviate.client.WeaviateClient;
import io.weaviate.client.base.Result;
import io.weaviate.client.v1.batch.Batch;
import io.weaviate.client.v1.batch.api.ObjectsBatchDeleter;
import io.weaviate.client.v1.batch.api.ObjectsBatcher;
import io.weaviate.client.v1.batch.model.BatchDeleteResponse;
import io.weaviate.client.v1.batch.model.BatchDeleteResponse.Results;
import io.weaviate.client.v1.graphql.GraphQL;
import io.weaviate.client.v1.graphql.model.GraphQLResponse;
import io.weaviate.client.v1.graphql.query.Get;
import io.weaviate.client.v1.graphql.query.fields.Field;
import io.weaviate.client.v1.schema.Schema;
import io.weaviate.client.v1.schema.api.ClassCreator;
import io.weaviate.client.v1.schema.api.SchemaGetter;
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
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineUtility;
import prerna.util.Utility;

public class WeaviateVectorDatabaseEngineUnitTests {
	private Insight insight;
	private WeaviateVectorDatabaseEngine engine;
	private IModelEngine modelEmbedder;

	private final String source = "source";
	private final String modality = "modality";
	private final String divider = "divider";
	private final String part = "part";
	private final String content = "content";

	@BeforeEach
	void setUp() {
		engine = new WeaviateVectorDatabaseEngine();
		insight = mock(Insight.class);
		modelEmbedder = mock(IModelEngine.class);
	}

	@Test
	void testOpen(@TempDir Path tempDir) throws Exception {
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

		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER)
				.resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path schemaPath = engineFolder.resolve("schema");

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<WeaviateAuthClient> wac = Mockito.mockStatic(WeaviateAuthClient.class);) {
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineFolder.toString());
				// used in connect2Weviate
				WeaviateClient clientMock = mock();
				wac.when(() -> WeaviateAuthClient.apiKey(any(), any())).thenReturn(clientMock);
				// used in createClass
				Schema schemaMock = mock();
				when(clientMock.schema()).thenReturn(schemaMock);
				SchemaGetter getterMock = mock();
				when(schemaMock.getter()).thenReturn(getterMock);
				Result<io.weaviate.client.v1.schema.model.Schema> resultMock = mock();
				when(getterMock.run()).thenReturn(resultMock);
				io.weaviate.client.v1.schema.model.Schema schMock = mock();
				when(resultMock.getResult()).thenReturn(schMock);
				when(schMock.getClasses()).thenReturn(new Vector<>());
				ClassCreator clssMock = mock();
				when(schemaMock.classCreator()).thenReturn(clssMock);
				when(clssMock.withClass(any())).thenReturn(clssMock);
				when(clssMock.run()).thenReturn(null);

				engine.open(testProps);
				assertTrue(Files.exists(schemaPath));
				Properties engineProps = engine.getSmssProp();
				for (Entry<Object, Object> testProp : testProps.entrySet()) {
					assertTrue(engineProps.containsKey(testProp.getKey()));
					assertTrue(engineProps.containsValue(testProp.getValue()));
				}
			}
		}
	}

	@Test
	void testOpenNoHostname(@TempDir Path tempDir) throws Exception {
		Properties testProps = new Properties();
		String url = "http://fake.url/";
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
		// testProps.setProperty(Constants.HOSTNAME, url);

		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER)
				.resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path schemaPath = engineFolder.resolve("schema");

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);) {
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineFolder.toString());

				IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> engine.open(testProps));
				assertEquals("Must define the host", e.getMessage());
			}
		}
	}

	@Test
	void testOpenNoAPIKey(@TempDir Path tempDir) throws Exception {
		Properties testProps = new Properties();
		String url = "http://fake.url/";
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
		// testProps.setProperty(Constants.API_KEY, apiKey);
		testProps.setProperty(Constants.HOSTNAME, url);

		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER)
				.resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path schemaPath = engineFolder.resolve("schema");

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);) {
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineFolder.toString());

				IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> engine.open(testProps));
				assertEquals("Must define the api key", e.getMessage());
			}
		}
	}

	@Test
	void testAddEmbeddings(@TempDir Path tempDir) throws Exception {
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

		String indexClass = "INDEX_CLASS";

		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER)
				.resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path schemaPath = Paths.get(engineFolder.toString(), "schema");
		Path docDirPath = Paths.get(schemaPath.toString(), indexClass, "documents");
		Files.createDirectories(docDirPath);

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<WeaviateAuthClient> wac = Mockito.mockStatic(WeaviateAuthClient.class);) {
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineFolder.toString());
				// used in connect2Weviate
				WeaviateClient clientMock = mock();
				wac.when(() -> WeaviateAuthClient.apiKey(any(), any())).thenReturn(clientMock);
				// used in createClass
				Schema schemaMock = mock();
				when(clientMock.schema()).thenReturn(schemaMock);
				SchemaGetter getterMock = mock();
				when(schemaMock.getter()).thenReturn(getterMock);
				Result<io.weaviate.client.v1.schema.model.Schema> resultMock = mock();
				when(getterMock.run()).thenReturn(resultMock);
				io.weaviate.client.v1.schema.model.Schema schMock = mock();
				when(resultMock.getResult()).thenReturn(schMock);
				when(schMock.getClasses()).thenReturn(new Vector<>());
				ClassCreator clssMock = mock();
				when(schemaMock.classCreator()).thenReturn(clssMock);
				when(clssMock.withClass(any())).thenReturn(clssMock);
				when(clssMock.run()).thenReturn(null);

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

					Batch batchMock = mock();
					when(clientMock.batch()).thenReturn(batchMock);
					ObjectsBatcher obMock = mock();
					when(batchMock.objectsBatcher()).thenReturn(obMock);

					engine.addEmbeddings(vectorCsvTable, insight, null);

					assertTrue(Files.exists(schemaPath));
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
	void testRemoveDocument(@TempDir Path tempDir) throws Exception {
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

		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER)
				.resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path docDirPath = Paths.get(engineFolder.toString(), "schema", indexClass, "documents");
		Files.createDirectories(docDirPath);
		String fileName = "newFile1.txt";
		List<String> fileNames = new Vector<>();
		fileNames.add(fileName);
		Path newFilePath = docDirPath.resolve(fileName);
		Files.createFile(newFilePath);

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<WeaviateAuthClient> wac = Mockito.mockStatic(WeaviateAuthClient.class);) {
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineFolder.toString());
				// used in connect2Weviate
				WeaviateClient clientMock = mock();
				wac.when(() -> WeaviateAuthClient.apiKey(any(), any())).thenReturn(clientMock);
				// used in createClass
				Schema schemaMock = mock();
				when(clientMock.schema()).thenReturn(schemaMock);
				SchemaGetter getterMock = mock();
				when(schemaMock.getter()).thenReturn(getterMock);
				Result<io.weaviate.client.v1.schema.model.Schema> resultMock = mock();
				when(getterMock.run()).thenReturn(resultMock);
				io.weaviate.client.v1.schema.model.Schema schMock = mock();
				when(resultMock.getResult()).thenReturn(schMock);
				when(schMock.getClasses()).thenReturn(new Vector<>());
				ClassCreator clssMock = mock();
				when(schemaMock.classCreator()).thenReturn(clssMock);
				when(clssMock.withClass(any())).thenReturn(clssMock);
				when(clssMock.run()).thenReturn(null);

				engine.open(testProps);

				// used in removeDocument()
				Batch batchMock = mock();
				when(clientMock.batch()).thenReturn(batchMock);
				ObjectsBatchDeleter obdMock = mock();
				when(batchMock.objectsBatchDeleter()).thenReturn(obdMock);
				when(obdMock.withClassName(weaviateClassname)).thenReturn(obdMock);
				when(obdMock.withWhere(any())).thenReturn(obdMock);
				Result<BatchDeleteResponse> batchRespResMock = mock();
				when(obdMock.run()).thenReturn(batchRespResMock);
				BatchDeleteResponse bdRespMock = mock();
				when(batchRespResMock.getResult()).thenReturn(bdRespMock);
				Results bdResults = mock();
				when(bdRespMock.getResults()).thenReturn(bdResults);
				when(bdResults.getSuccessful()).thenReturn(1L);

				Map<String, Object> parameters = new HashMap<>();
				parameters.put("indexClass", indexClass);
				assertTrue(Files.exists(newFilePath));
				engine.removeDocument(fileNames, parameters);
				assertFalse(Files.exists(newFilePath));
			}
		}
	}

	@Test
	void testNearestNeighborCall(@TempDir Path tempDir) throws Exception {
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

		String indexClass = "INDEX_CLASS";

		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER)
				.resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path docDirPath = Paths.get(engineFolder.toString(), "schema", indexClass, "documents");
		Files.createDirectories(docDirPath);

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<WeaviateAuthClient> wac = Mockito.mockStatic(WeaviateAuthClient.class);) {
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineFolder.toString());
				// used in connect2Weviate
				WeaviateClient clientMock = mock();
				wac.when(() -> WeaviateAuthClient.apiKey(any(), any())).thenReturn(clientMock);
				// used in createClass
				Schema schemaMock = mock();
				when(clientMock.schema()).thenReturn(schemaMock);
				SchemaGetter getterMock = mock();
				when(schemaMock.getter()).thenReturn(getterMock);
				Result<io.weaviate.client.v1.schema.model.Schema> resultMock = mock();
				when(getterMock.run()).thenReturn(resultMock);
				io.weaviate.client.v1.schema.model.Schema schMock = mock();
				when(resultMock.getResult()).thenReturn(schMock);
				when(schMock.getClasses()).thenReturn(new Vector<>());
				ClassCreator clssMock = mock();
				when(schemaMock.classCreator()).thenReturn(clssMock);
				when(clssMock.withClass(any())).thenReturn(clssMock);
				when(clssMock.run()).thenReturn(null);

				engine.open(testProps);

				try (MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);) {
					u.when(() -> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
					// model embedder properties
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

					// used in nearestNeighborCall()
					GraphQL gqlMock = mock();
					when(clientMock.graphQL()).thenReturn(gqlMock);
					Get getMock = mock();
					when(gqlMock.get()).thenReturn(getMock);
					when(getMock.withClassName(any())).thenReturn(getMock);
					when(getMock.withFields(any(Field.class), any(Field.class), any(Field.class), any(Field.class),
							any(Field.class), any(Field.class))).thenReturn(getMock);
					when(getMock.withNearVector(any())).thenReturn(getMock);
					when(getMock.withAutocut(any())).thenReturn(getMock);
					when(getMock.withLimit(any())).thenReturn(getMock);
					Result<GraphQLResponse> gqlResultMock = mock();
					when(getMock.run()).thenReturn(gqlResultMock);
					GraphQLResponse gqlResponseMock = mock();
					when(gqlResultMock.getResult()).thenReturn(gqlResponseMock);

					LinkedTreeMap treemap = new LinkedTreeMap<>();
					LinkedTreeMap innerTreemap = new LinkedTreeMap<>();
					{
						List<Object> outputList = new ArrayList<>();
						{
							LinkedTreeMap outputTreemap = new LinkedTreeMap<>();
							outputTreemap.put(source, source);
							outputTreemap.put(divider, divider);
							outputTreemap.put(modality, modality);
							outputTreemap.put(part, part);
							outputTreemap.put(content, content);
							{
								LinkedTreeMap additionalTreemap = new LinkedTreeMap<>();
								additionalTreemap.put("certainty", 10);
								additionalTreemap.put("distance", 5);
								outputTreemap.put("_additional", additionalTreemap);
							}
							outputList.add(outputTreemap);
						}
						innerTreemap.put(weaviateClassname, outputList);
					}
					treemap.put("Get", innerTreemap);
					when(gqlResponseMock.getData()).thenReturn(treemap);

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
						assertEquals(10, outputMap.get("Score"));
						assertEquals(5, outputMap.get("Distance"));
					}
				}
			}
		}
	}

	@Test
	void testNearestNeighborCallNoInsight() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> engine.nearestNeighbor(null, null, null, null));
		assertEquals("Insight must be provided to run Model Engine Encoder", e.getMessage());
	}

	@Test
	void testListDocuments(@TempDir Path tempDir) throws Exception {
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

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<WeaviateAuthClient> wac = Mockito.mockStatic(WeaviateAuthClient.class);) {
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineFolder.toString());
				// used in connect2Weviate
				WeaviateClient clientMock = mock();
				wac.when(() -> WeaviateAuthClient.apiKey(any(), any())).thenReturn(clientMock);
				// used in createClass
				Schema schemaMock = mock();
				when(clientMock.schema()).thenReturn(schemaMock);
				SchemaGetter getterMock = mock();
				when(schemaMock.getter()).thenReturn(getterMock);
				Result<io.weaviate.client.v1.schema.model.Schema> resultMock = mock();
				when(getterMock.run()).thenReturn(resultMock);
				io.weaviate.client.v1.schema.model.Schema schMock = mock();
				when(resultMock.getResult()).thenReturn(schMock);
				when(schMock.getClasses()).thenReturn(new Vector<>());
				ClassCreator clssMock = mock();
				when(schemaMock.classCreator()).thenReturn(clssMock);
				when(clssMock.withClass(any())).thenReturn(clssMock);
				when(clssMock.run()).thenReturn(null);

				engine.open(testProps);

				Map<String, Object> parameters = new HashMap<>();
				parameters.put("indexClass", indexClass);
				List<Map<String, Object>> docsOutput = engine.listDocuments(parameters);
				assertEquals(fileNames.size(), docsOutput.size());
				for (int fileIdx = 0; fileIdx < fileNames.size(); fileIdx++) {
					Map<String, Object> outputDoc = docsOutput.get(fileIdx);
					assertEquals(fileNames.get(fileIdx), outputDoc.get("fileName"));
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
