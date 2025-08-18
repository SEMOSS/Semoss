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
import org.apache.hc.core5.http.HttpHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import prerna.auth.User;
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

public class MilvusVectorDatabaseEngineUnitTests {
	private User user;
	private Insight insight;
	private MilvusVectorDatabaseEngine engine;
	private IModelEngine modelEmbedder;
		
	@BeforeEach
	void setUp() {
		user = mock(User.class);
		engine = new MilvusVectorDatabaseEngine();
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
		String testAPIKey = "TEST_API_KEY";
		String testCollectionName = "collection_name";
		String testDatabaseName = "database_name";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineAlias);
		testProps.setProperty(Constants.CONTENT_LENGTH, contentLength);
		testProps.setProperty(Constants.CONTENT_OVERLAP, contentOverlap);
		testProps.setProperty(Constants.KEEP_INPUT_OUTPUT, keepInputOutput);
		testProps.setProperty(Constants.HOSTNAME, url);
		testProps.setProperty(Constants.API_KEY, testAPIKey);
		testProps.setProperty("COLLECTION_NAME", testCollectionName);
		testProps.setProperty("DATABASE_NAME", testDatabaseName);
		
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path schemaPath = engineFolder.resolve("schema");

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);) {
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineFolder.toString());
				// used in doesDatabaseExist()
				Map<String, Object> databaseResponse = new HashMap<>();//{"code":0,"data":["databaseName1"]}
				{
					databaseResponse.put("code", 0);
					List<String> dbNames = new Vector<>();
					dbNames.add(testDatabaseName);
					databaseResponse.put("data", dbNames);
				}
				JsonObject dbrequest = new JsonObject();
				dbrequest.addProperty("dbName", testDatabaseName);
				Map<String, String> headers = new HashMap<>();
				headers.put(HttpHeaders.CONTENT_TYPE, "application/json");
				headers.put(HttpHeaders.AUTHORIZATION, "Bearer " + testAPIKey);
				hhu.when(() -> HttpHelperUtility.postRequestStringBody("http://fake.url//v2/vectordb/databases/list",
						headers, dbrequest.toString(), ContentType.APPLICATION_JSON, null, null, null))
						.thenReturn(new Gson().toJson(databaseResponse));
				
				// used in doesCollectionExist()
				Map<String, Object> collectionResponse = new HashMap<>();//{"code":0,"data":["collectionName1"]}
				{
					collectionResponse.put("code", 0);
					List<String> collectionNames = new Vector<>();
					collectionNames.add(testCollectionName);
					collectionResponse.put("data", collectionNames);
				}
				hhu.when(() -> HttpHelperUtility.postRequestStringBody("http://fake.url//v2/vectordb/collections/list",
						headers, dbrequest.toString(), ContentType.APPLICATION_JSON, null, null, null))
						.thenReturn(new Gson().toJson(collectionResponse));

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
	void testOpenNoAPIKey(@TempDir Path tempDir) throws Exception {
		Properties testProps = new Properties();
		String url = "http://fake.url/";
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
		String contentLength = "10";
		String contentOverlap = "10";
		String keepInputOutput = "false";
		String testAPIKey = "TEST_API_KEY";
		String testCollectionName = "collection_name";
		String testDatabaseName = "database_name";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineAlias);
		testProps.setProperty(Constants.CONTENT_LENGTH, contentLength);
		testProps.setProperty(Constants.CONTENT_OVERLAP, contentOverlap);
		testProps.setProperty(Constants.KEEP_INPUT_OUTPUT, keepInputOutput);
		testProps.setProperty(Constants.HOSTNAME, url);
//		testProps.setProperty(Constants.API_KEY, testAPIKey);
		testProps.setProperty("COLLECTION_NAME", testCollectionName);
		testProps.setProperty("DATABASE_NAME", testDatabaseName);
		
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path schemaPath = engineFolder.resolve("schema");

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);) {
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineFolder.toString());
				// used in doesDatabaseExist()
				Map<String, Object> databaseResponse = new HashMap<>();//{"code":0,"data":["databaseName1"]}
				{
					databaseResponse.put("code", 0);
					List<String> dbNames = new Vector<>();
					dbNames.add(testDatabaseName);
					databaseResponse.put("data", dbNames);
				}
				JsonObject dbrequest = new JsonObject();
				dbrequest.addProperty("dbName", testDatabaseName);
				Map<String, String> headers = new HashMap<>();
				headers.put(HttpHeaders.CONTENT_TYPE, "application/json");
				headers.put(HttpHeaders.AUTHORIZATION, "Bearer " + testAPIKey);
				hhu.when(() -> HttpHelperUtility.postRequestStringBody("http://fake.url//v2/vectordb/databases/list",
						headers, dbrequest.toString(), ContentType.APPLICATION_JSON, null, null, null))
						.thenReturn(new Gson().toJson(databaseResponse));
				
				// used in doesCollectionExist()
				Map<String, Object> collectionResponse = new HashMap<>();//{"code":0,"data":["collectionName1"]}
				{
					collectionResponse.put("code", 0);
					List<String> collectionNames = new Vector<>();
					collectionNames.add(testCollectionName);
					collectionResponse.put("data", collectionNames);
				}
				hhu.when(() -> HttpHelperUtility.postRequestStringBody("http://fake.url//v2/vectordb/collections/list",
						headers, dbrequest.toString(), ContentType.APPLICATION_JSON, null, null, null))
						.thenReturn(new Gson().toJson(collectionResponse));

				IllegalArgumentException e = assertThrows(
						IllegalArgumentException.class,
						()->engine.open(testProps));
				assertEquals("Must define the api key", e.getMessage());
			}
		}
	}
	
	@Test
	void testOpenNoCollectionName(@TempDir Path tempDir) throws Exception {
		Properties testProps = new Properties();
		String url = "http://fake.url/";
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
		String contentLength = "10";
		String contentOverlap = "10";
		String keepInputOutput = "false";
		String testAPIKey = "TEST_API_KEY";
		String testCollectionName = "collection_name";
		String testDatabaseName = "database_name";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineAlias);
		testProps.setProperty(Constants.CONTENT_LENGTH, contentLength);
		testProps.setProperty(Constants.CONTENT_OVERLAP, contentOverlap);
		testProps.setProperty(Constants.KEEP_INPUT_OUTPUT, keepInputOutput);
		testProps.setProperty(Constants.HOSTNAME, url);
		testProps.setProperty(Constants.API_KEY, testAPIKey);
//		testProps.setProperty("COLLECTION_NAME", testCollectionName);
		testProps.setProperty("DATABASE_NAME", testDatabaseName);
		
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path schemaPath = engineFolder.resolve("schema");

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);) {
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineFolder.toString());
				// used in doesDatabaseExist()
				Map<String, Object> databaseResponse = new HashMap<>();//{"code":0,"data":["databaseName1"]}
				{
					databaseResponse.put("code", 0);
					List<String> dbNames = new Vector<>();
					dbNames.add(testDatabaseName);
					databaseResponse.put("data", dbNames);
				}
				JsonObject dbrequest = new JsonObject();
				dbrequest.addProperty("dbName", testDatabaseName);
				Map<String, String> headers = new HashMap<>();
				headers.put(HttpHeaders.CONTENT_TYPE, "application/json");
				headers.put(HttpHeaders.AUTHORIZATION, "Bearer " + testAPIKey);
				hhu.when(() -> HttpHelperUtility.postRequestStringBody("http://fake.url//v2/vectordb/databases/list",
						headers, dbrequest.toString(), ContentType.APPLICATION_JSON, null, null, null))
						.thenReturn(new Gson().toJson(databaseResponse));
				
				// used in doesCollectionExist()
				Map<String, Object> collectionResponse = new HashMap<>();//{"code":0,"data":["collectionName1"]}
				{
					collectionResponse.put("code", 0);
					List<String> collectionNames = new Vector<>();
					collectionNames.add(testCollectionName);
					collectionResponse.put("data", collectionNames);
				}
				hhu.when(() -> HttpHelperUtility.postRequestStringBody("http://fake.url//v2/vectordb/collections/list",
						headers, dbrequest.toString(), ContentType.APPLICATION_JSON, null, null, null))
						.thenReturn(new Gson().toJson(collectionResponse));

				IllegalArgumentException e = assertThrows(
						IllegalArgumentException.class,
						()->engine.open(testProps));
				assertEquals("Collection name must be provided", e.getMessage());
			}
		}
	}
	
	@Test
	void testOpenFailedDBNameEndpoint(@TempDir Path tempDir) throws Exception {
		Properties testProps = new Properties();
		String url = "http://fake.url/";
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
		String contentLength = "10";
		String contentOverlap = "10";
		String keepInputOutput = "false";
		String testAPIKey = "TEST_API_KEY";
		String testCollectionName = "collection_name";
		String testDatabaseName = "database_name";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineAlias);
		testProps.setProperty(Constants.CONTENT_LENGTH, contentLength);
		testProps.setProperty(Constants.CONTENT_OVERLAP, contentOverlap);
		testProps.setProperty(Constants.KEEP_INPUT_OUTPUT, keepInputOutput);
		testProps.setProperty(Constants.HOSTNAME, url);
		testProps.setProperty(Constants.API_KEY, testAPIKey);
		testProps.setProperty("COLLECTION_NAME", testCollectionName);
		testProps.setProperty("DATABASE_NAME", testDatabaseName);
		
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path schemaPath = engineFolder.resolve("schema");

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);) {
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineFolder.toString());
				// used in doesDatabaseExist()
				Gson gson = new Gson();
				Map<String, Object> databaseResponse = new HashMap<>();//{"code":0,"data":["databaseName1"]}
				{
					databaseResponse.put("code", 1);
					List<String> dbNames = new Vector<>();
					dbNames.add(testDatabaseName);
					databaseResponse.put("data", dbNames);
				}
				JsonObject dbrequest = new JsonObject();
				dbrequest.addProperty("dbName", testDatabaseName);
				Map<String, String> headers = new HashMap<>();
				headers.put(HttpHeaders.CONTENT_TYPE, "application/json");
				headers.put(HttpHeaders.AUTHORIZATION, "Bearer " + testAPIKey);
				hhu.when(() -> HttpHelperUtility.postRequestStringBody("http://fake.url//v2/vectordb/databases/list",
						headers, dbrequest.toString(), ContentType.APPLICATION_JSON, null, null, null))
						.thenReturn(gson.toJson(databaseResponse));
				
				// used in doesCollectionExist()
				Map<String, Object> collectionResponse = new HashMap<>();//{"code":0,"data":["collectionName1"]}
				{
					collectionResponse.put("code", 0);
					List<String> collectionNames = new Vector<>();
					collectionNames.add(testCollectionName);
					collectionResponse.put("data", collectionNames);
				}
				hhu.when(() -> HttpHelperUtility.postRequestStringBody("http://fake.url//v2/vectordb/collections/list",
						headers, dbrequest.toString(), ContentType.APPLICATION_JSON, null, null, null))
						.thenReturn(gson.toJson(collectionResponse));

				RuntimeException e = assertThrows(
						RuntimeException.class,
						()->engine.open(testProps));
				assertEquals("Failed to pull database list endpoint. Detailed error = " + gson.toJson(databaseResponse), e.getMessage());
			}
		}
	}
	
	@Test
	void testOpenFailedCollectionNameEndpoint(@TempDir Path tempDir) throws Exception {
		Properties testProps = new Properties();
		String url = "http://fake.url/";
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
		String contentLength = "10";
		String contentOverlap = "10";
		String keepInputOutput = "false";
		String testAPIKey = "TEST_API_KEY";
		String testCollectionName = "collection_name";
		String testDatabaseName = "database_name";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineAlias);
		testProps.setProperty(Constants.CONTENT_LENGTH, contentLength);
		testProps.setProperty(Constants.CONTENT_OVERLAP, contentOverlap);
		testProps.setProperty(Constants.KEEP_INPUT_OUTPUT, keepInputOutput);
		testProps.setProperty(Constants.HOSTNAME, url);
		testProps.setProperty(Constants.API_KEY, testAPIKey);
		testProps.setProperty("COLLECTION_NAME", testCollectionName);
		testProps.setProperty("DATABASE_NAME", testDatabaseName);
		
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path schemaPath = engineFolder.resolve("schema");

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);) {
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineFolder.toString());
				// used in doesDatabaseExist()
				Gson gson = new Gson();
				Map<String, Object> databaseResponse = new HashMap<>();//{"code":0,"data":["databaseName1"]}
				{
					databaseResponse.put("code", 0);
					List<String> dbNames = new Vector<>();
					dbNames.add(testDatabaseName);
					databaseResponse.put("data", dbNames);
				}
				JsonObject dbrequest = new JsonObject();
				dbrequest.addProperty("dbName", testDatabaseName);
				Map<String, String> headers = new HashMap<>();
				headers.put(HttpHeaders.CONTENT_TYPE, "application/json");
				headers.put(HttpHeaders.AUTHORIZATION, "Bearer " + testAPIKey);
				hhu.when(() -> HttpHelperUtility.postRequestStringBody("http://fake.url//v2/vectordb/databases/list",
						headers, dbrequest.toString(), ContentType.APPLICATION_JSON, null, null, null))
						.thenReturn(gson.toJson(databaseResponse));
				
				// used in doesCollectionExist()
				Map<String, Object> collectionResponse = new HashMap<>();//{"code":0,"data":["collectionName1"]}
				{
					collectionResponse.put("code", 1);
					List<String> collectionNames = new Vector<>();
					collectionNames.add(testCollectionName);
					collectionResponse.put("data", collectionNames);
				}
				hhu.when(() -> HttpHelperUtility.postRequestStringBody("http://fake.url//v2/vectordb/collections/list",
						headers, dbrequest.toString(), ContentType.APPLICATION_JSON, null, null, null))
						.thenReturn(gson.toJson(collectionResponse));

				RuntimeException e = assertThrows(
						RuntimeException.class,
						()->engine.open(testProps));
				assertEquals("Failed to execute collection list endpoint. Detailed error = " + gson.toJson(collectionResponse), e.getMessage());
			}
		}
	}
	
	@Test
	void testGetDefaultDistanceMethod() {
		assertEquals("COSINE", engine.getDefaultDistanceMethod());
	}
	
	@Test
	void testAddEmbeddings(@TempDir Path tempDir) throws Exception {
		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties, connection

		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		verifyModelProps(engine, testEmbedderId, embedderModel, embedderModelType);
		
		try(MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);
				MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);){
			u.when(()->Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
			
			Map<String, Object> response = new HashMap<>();//{"code":0,"data":["collectionName1"]}
			{
				response.put("code", 0);
				Map<String, Object> data = new HashMap<>();
				data.put("insertCount", 10);
				response.put("data", data);
			}
			hhu.when(() -> HttpHelperUtility.postRequestStringBody(any(String.class),
					any(Map.class), any(String.class), any(ContentType.class), nullable(String.class), 
					nullable(String.class), nullable(String.class)))
					.thenReturn(new Gson().toJson(response));
			
			VectorDatabaseCSVTable tableMock = mock();
			engine.addEmbeddings(tableMock, insight, new HashMap<>());
			Properties updateEngineProps = engine.getSmssProp();
			assertNotNull(updateEngineProps);
			assertTrue(updateEngineProps.containsKey(Constants.MODEL));
			assertEquals(embedderModel, updateEngineProps.get(Constants.MODEL));
			assertTrue(updateEngineProps.containsKey(IModelEngine.MODEL_TYPE));
			assertEquals(embedderModelType, updateEngineProps.get(IModelEngine.MODEL_TYPE));
		}
	}
	
	@Test
	void testAddEmbeddingsNoInsight(@TempDir Path tempDir) throws Exception {
		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties, connection

		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		verifyModelProps(engine, testEmbedderId, embedderModel, embedderModelType);
		IllegalArgumentException e = assertThrows(
				IllegalArgumentException.class,
				()->engine.addEmbeddings(new VectorDatabaseCSVTable(), null, null));
		assertEquals("Insight must be provided to run Model Engine Encoder", e.getMessage());
	}
	
	@Test
	void testAddEmbeddingsFailedEndPoint(@TempDir Path tempDir) throws Exception {
		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties, connection

		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		verifyModelProps(engine, testEmbedderId, embedderModel, embedderModelType);

		try (MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);
				MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);) {
			u.when(() -> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
			Gson gson = new Gson();
			Map<String, Object> response = new HashMap<>();// {"code":0,"data":["collectionName1"]}
			{
				response.put("code", 1);
				Map<String, Object> data = new HashMap<>();
				data.put("insertCount", 10);
				response.put("data", data);
			}
			hhu.when(() -> HttpHelperUtility.postRequestStringBody(any(String.class), any(Map.class), any(String.class),
					any(ContentType.class), nullable(String.class), nullable(String.class), nullable(String.class)))
					.thenReturn(gson.toJson(response));

			VectorDatabaseCSVTable tableMock = mock();

			String testCollectionName = "collection_name";
			String testDatabaseName = "database_name";
			RuntimeException e = assertThrows(RuntimeException.class,
					() -> engine.addEmbeddings(tableMock, insight, new HashMap<>()));
			assertEquals("Failed to insert collections " + testCollectionName + " in database " + testDatabaseName
					+ ". Detailed error = " + gson.toJson(response), e.getMessage());
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
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path docDirPath = Paths.get(engineFolder.toString(), "schema", indexClass, "documents");
		Files.createDirectories(docDirPath);	
		String fileName1 = "newFile1.txt";
		String fileName2 = "newFile2.txt";
	    Files.createFile(docDirPath.resolve(fileName1));
	    Files.createFile(docDirPath.resolve(fileName2));
	    List<String> fileNames = new Vector<>();
	    fileNames.add(fileName1);
	    fileNames.add(fileName2);
	    
		try(MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);){
						
			Map<String, Object> response = new HashMap<>();
			{
				response.put("code", 0);
				Map<String, Object> data = new HashMap<>();
				data.put("deleteCount", 2.0);
				response.put("data", data);
			}
			String responseString = new Gson().toJson(response);
			// we sub in ANY string for request body and url
			hhu.when(() -> HttpHelperUtility.postRequestStringBody(any(String.class), any(Map.class),
					any(String.class), any(ContentType.class), nullable(String.class), nullable(String.class),
					nullable(String.class))).thenReturn(responseString);

			fileNames.forEach(fileName -> assertTrue(Files.exists(docDirPath.resolve(fileName))));
			engine.removeDocument(fileNames, parameters);
			fileNames.forEach(fileName -> assertFalse(Files.exists(docDirPath.resolve(fileName))));
		}
	}
	
	@Test
	void testNearestNeighborCall(@TempDir Path tempDir) throws Exception {
		Number limit = 1;
		String searchStatement = "searchStatement";
		List<List<Double>> mockedEmbeddingList = new Vector<>();
		List<Double> mockedEmbeddings = new Vector<>();
		{
			mockedEmbeddings.add(new Double(0.2));
			mockedEmbeddings.add(new Double(0.4));
			mockedEmbeddings.add(new Double(0.6));
			mockedEmbeddings.add(new Double(0.8));
			
			mockedEmbeddingList.add(mockedEmbeddings);
		}

		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties
		
		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		verifyModelProps(engine, testEmbedderId, embedderModel, embedderModelType);
		
		try(MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);
				MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class)){
			u.when(()-> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
			// mocking getting embeddings
			EmbeddingsModelEngineResponse responseMocked = mock(EmbeddingsModelEngineResponse.class);
			when(modelEmbedder.embeddings(Arrays.asList(new String[] {searchStatement}), insight, null)).thenReturn(responseMocked);
			when(responseMocked.getResponse()).thenReturn(mockedEmbeddingList);
			
			// creating the json response from http request
			String testSource = "TEST_SOURCE";
			String testModality = "TEST_MODALITY";
			String testDivider = "TEST_DIVIDER";
			String testPart = "TEST_PART";
			int testTokens = 123;
			String testContent = "TEST_CONTENT";
			Map<String, Object> response = new HashMap<>();
			{
				List<Map<String, Object>> data = new Vector<>();
				Map<String, Object> source = new HashMap<>();
				source.put(VectorDatabaseCSVTable.SOURCE, testSource);
				source.put(VectorDatabaseCSVTable.MODALITY, testModality);
				source.put(VectorDatabaseCSVTable.DIVIDER, testDivider);
				source.put(VectorDatabaseCSVTable.PART, testPart);
				source.put(VectorDatabaseCSVTable.TOKENS, testTokens);
				source.put(VectorDatabaseCSVTable.CONTENT, testContent);
				source.put("distance", 10.0);
				data.add(source);
				data.add(source);
				data.add(source);
				data.add(source);
				data.add(source);
				response.put("data", data);
			}
			String nearestNeigborResponse = new Gson().toJson(response);
			
			hhu.when(() -> HttpHelperUtility.postRequestStringBody(any(String.class), any(Map.class),
					any(String.class), any(ContentType.class), nullable(String.class), nullable(String.class),
					nullable(String.class))).thenReturn(nearestNeigborResponse);
			
			List<Map<String, Object>> returnMetadata = engine.nearestNeighborCall(insight, searchStatement, limit, new HashMap<>());
			assertEquals(5, returnMetadata.size());
			for (Map<String, Object> metadata : returnMetadata) {
				assertEquals(testSource, metadata.get(VectorDatabaseCSVTable.SOURCE));
				assertEquals(testModality, metadata.get(VectorDatabaseCSVTable.MODALITY));
				assertEquals(testDivider, metadata.get(VectorDatabaseCSVTable.DIVIDER));
				assertEquals(testPart, metadata.get(VectorDatabaseCSVTable.PART));
				assertEquals(testTokens, metadata.get(VectorDatabaseCSVTable.TOKENS));
				assertEquals(testContent, metadata.get(VectorDatabaseCSVTable.CONTENT));
				assertEquals(10.0, ((JsonElement)metadata.get("Score")).getAsDouble());
			}
		}
	}
	
	@Test
	void testNearestNeighborCallNoInsight(@TempDir Path tempDir) throws Exception {
		Number limit = 1;
		String searchStatement = "searchStatement";
		List<List<Double>> mockedEmbeddingList = new Vector<>();
		List<Double> mockedEmbeddings = new Vector<>();
		{
			mockedEmbeddings.add(new Double(0.2));
			mockedEmbeddings.add(new Double(0.4));
			mockedEmbeddings.add(new Double(0.6));
			mockedEmbeddings.add(new Double(0.8));
			
			mockedEmbeddingList.add(mockedEmbeddings);
		}

		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties
		
		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		verifyModelProps(engine, testEmbedderId, embedderModel, embedderModelType);
		
		try(MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);
				MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class)){
			u.when(()-> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
			// mocking getting embeddings
			EmbeddingsModelEngineResponse responseMocked = mock(EmbeddingsModelEngineResponse.class);
			when(modelEmbedder.embeddings(Arrays.asList(new String[] {searchStatement}), insight, null)).thenReturn(responseMocked);
			when(responseMocked.getResponse()).thenReturn(mockedEmbeddingList);
			
			// creating the json response from http request
			String testSource = "TEST_SOURCE";
			String testModality = "TEST_MODALITY";
			String testDivider = "TEST_DIVIDER";
			String testPart = "TEST_PART";
			int testTokens = 123;
			String testContent = "TEST_CONTENT";
			Map<String, Object> response = new HashMap<>();
			{
				List<Map<String, Object>> data = new Vector<>();
				Map<String, Object> source = new HashMap<>();
				source.put(VectorDatabaseCSVTable.SOURCE, testSource);
				source.put(VectorDatabaseCSVTable.MODALITY, testModality);
				source.put(VectorDatabaseCSVTable.DIVIDER, testDivider);
				source.put(VectorDatabaseCSVTable.PART, testPart);
				source.put(VectorDatabaseCSVTable.TOKENS, testTokens);
				source.put(VectorDatabaseCSVTable.CONTENT, testContent);
				source.put("distance", 10.0);
				data.add(source);
				data.add(source);
				data.add(source);
				data.add(source);
				data.add(source);
				response.put("data", data);
			}
			String nearestNeigborResponse = new Gson().toJson(response);
			
			hhu.when(() -> HttpHelperUtility.postRequestStringBody(any(String.class), any(Map.class),
					any(String.class), any(ContentType.class), nullable(String.class), nullable(String.class),
					nullable(String.class))).thenReturn(nearestNeigborResponse);
			
			IllegalArgumentException e = assertThrows(
					IllegalArgumentException.class, 
					()-> engine.nearestNeighborCall(null, searchStatement, limit, new HashMap<>()));
			assertEquals("Insight must be provided to run Model Engine Encoder", e.getMessage());
		}
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

		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
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
		
		try (MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class)){
			
			Map<String, Object> response = new HashMap<>();
			{
				List<Map<String, Object>> data = new Vector<>();
				for (String fileName : fileNames) {
					Map<String, Object> source = new HashMap<>();
					source.put(VectorDatabaseCSVTable.SOURCE, fileName);
					data.add(source);
				}
				response.put("data", data);
			}
			String httpResponse = new Gson().toJson(response);
			
			hhu.when(() -> HttpHelperUtility.postRequestStringBody(any(String.class), any(Map.class),
					any(String.class), any(ContentType.class), nullable(String.class), nullable(String.class),
					nullable(String.class))).thenReturn(httpResponse);
			
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
		int testTokens = 123;
		String testContent = "TEST_CONTENT";
		Map<String, Object> response = new HashMap<>();
		{
			List<Map<String, Object>> data = new Vector<>();
			Map<String, Object> source = new HashMap<>();
			source.put(VectorDatabaseCSVTable.SOURCE, testSource);
			source.put(VectorDatabaseCSVTable.MODALITY, testModality);
			source.put(VectorDatabaseCSVTable.DIVIDER, testDivider);
			source.put(VectorDatabaseCSVTable.PART, testPart);
			source.put(VectorDatabaseCSVTable.TOKENS, testTokens);
			source.put(VectorDatabaseCSVTable.CONTENT, testContent);
			source.put("id", "ID");
			data.add(source);
			data.add(source);
			data.add(source);
			data.add(source);
			data.add(source);
			response.put("data", data);
		}
		String httpResponse = new Gson().toJson(response);
		try(MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class)){
			hhu.when(() -> HttpHelperUtility.postRequestStringBody(any(String.class), any(Map.class),
					any(String.class), any(ContentType.class), nullable(String.class), nullable(String.class),
					nullable(String.class))).thenReturn(httpResponse);
			
			List<Map<String, Object>> listOfRecords = engine.listAllRecords(new HashMap<>());
			assertEquals(5, listOfRecords.size());
			for (Map<String, Object> record : listOfRecords) {
				assertEquals(testSource, record.get(VectorDatabaseCSVTable.SOURCE));
				assertEquals(testModality, record.get(VectorDatabaseCSVTable.MODALITY));
				assertEquals(testDivider, record.get(VectorDatabaseCSVTable.DIVIDER));
				assertEquals(testPart, record.get(VectorDatabaseCSVTable.PART));
				assertEquals(testTokens, record.get(VectorDatabaseCSVTable.TOKENS));
				assertEquals(testContent, record.get(VectorDatabaseCSVTable.CONTENT));
				assertEquals("ID", record.get("id"));
			}
		}
	}

	@Test
	void testGetVectorDatabaseType() {
		assertEquals(VectorDatabaseTypeEnum.MILVUS, engine.getVectorDatabaseType());
	}
	
	void openEngine(Path tempDir, MilvusVectorDatabaseEngine engine, Map<String, String> extraProps) throws Exception {
		Properties testProps = new Properties();
		String url = "http://fake.url/";
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
		String contentLength = "10";
		String contentOverlap = "10";
		String keepInputOutput = "false";
		String testAPIKey = "TEST_API_KEY";
		String testCollectionName = "collection_name";
		String testDatabaseName = "database_name";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineAlias);
		testProps.setProperty(Constants.CONTENT_LENGTH, contentLength);
		testProps.setProperty(Constants.CONTENT_OVERLAP, contentOverlap);
		testProps.setProperty(Constants.KEEP_INPUT_OUTPUT, keepInputOutput);
		testProps.setProperty(Constants.HOSTNAME, url);
		testProps.setProperty(Constants.API_KEY, testAPIKey);
		testProps.setProperty("COLLECTION_NAME", testCollectionName);
		testProps.setProperty("DATABASE_NAME", testDatabaseName);
		
		if (extraProps != null) {
			for (Map.Entry<String, String> extraPropsEntry : extraProps.entrySet()) {
				String key = extraPropsEntry.getKey();
				String prop = extraPropsEntry.getValue();
				testProps.setProperty(key, prop);
			}
		}
		
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path schemaPath = engineFolder.resolve("schema");

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);) {
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineFolder.toString());
				// used in doesDatabaseExist()
				Map<String, Object> databaseResponse = new HashMap<>();//{"code":0,"data":["databaseName1"]}
				{
					databaseResponse.put("code", 0);
					List<String> dbNames = new Vector<>();
					dbNames.add(testDatabaseName);
					databaseResponse.put("data", dbNames);
				}
				JsonObject dbrequest = new JsonObject();
				dbrequest.addProperty("dbName", testDatabaseName);
				Map<String, String> headers = new HashMap<>();
				headers.put(HttpHeaders.CONTENT_TYPE, "application/json");
				headers.put(HttpHeaders.AUTHORIZATION, "Bearer " + testAPIKey);
				hhu.when(() -> HttpHelperUtility.postRequestStringBody("http://fake.url//v2/vectordb/databases/list",
						headers, dbrequest.toString(), ContentType.APPLICATION_JSON, null, null, null))
						.thenReturn(new Gson().toJson(databaseResponse));
				
				// used in doesCollectionExist()
				Map<String, Object> collectionResponse = new HashMap<>();//{"code":0,"data":["collectionName1"]}
				{
					collectionResponse.put("code", 0);
					List<String> collectionNames = new Vector<>();
					collectionNames.add(testCollectionName);
					collectionResponse.put("data", collectionNames);
				}
				hhu.when(() -> HttpHelperUtility.postRequestStringBody("http://fake.url//v2/vectordb/collections/list",
						headers, dbrequest.toString(), ContentType.APPLICATION_JSON, null, null, null))
						.thenReturn(new Gson().toJson(collectionResponse));

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
	
	void verifyModelProps(MilvusVectorDatabaseEngine engine, String testEmbedderId, String embedderModel,
			String embedderModelType) {
		try (MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);) {
			u.when(() -> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
			// model embedder properties
			Properties embedderProps = new Properties();
			embedderProps.setProperty(Constants.MODEL, embedderModel);
			embedderProps.setProperty(IModelEngine.MODEL_TYPE, embedderModelType);
			when(modelEmbedder.getSmssProp()).thenReturn(embedderProps);
			engine.verifyModelProps();
		}
	}
}
