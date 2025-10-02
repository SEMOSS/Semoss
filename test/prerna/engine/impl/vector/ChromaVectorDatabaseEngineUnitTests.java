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
import java.util.ArrayList;
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
import com.google.gson.Gson;
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

public class ChromaVectorDatabaseEngineUnitTests {

	private Insight insight;
	private ChromaVectorDatabaseEngine engine;
	private IModelEngine modelEmbedder;
	
	@BeforeEach
	void setUp() {
		engine = new ChromaVectorDatabaseEngine();
		insight = mock(Insight.class);
		modelEmbedder = mock(IModelEngine.class);
	}

	@Test
	void testOpen(@TempDir Path tempDir) throws Exception {
		String classname = "TEST_CHROMA_CLASS";
		String testId = "TEST_ID";
		
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
		testProps.setProperty(Constants.HOSTNAME, url);
		testProps.setProperty(ChromaVectorDatabaseEngine.CHROMA_CLASSNAME, classname);
		

	String engineNameAndId = SmssUtilities.getUniqueName(testEngineAlias, testEngine);
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(engineNameAndId);
		Path engineAssetFolder = engineFolder.resolve("assets");
		Path engineVersionFolder = engineFolder.resolve("version");

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);) {
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineAssetFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineVersionFolder.toString());

				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine, testEngineAlias))
						.thenReturn(engineAssetFolder.toString());
				// used in createCollection()
				List<Map<String, Object>> testRequestResponse = new Vector<>();
				{
					// this will be the return value for this response with a name
					Map<String, Object> testRespMapWithId = new HashMap<>();
					testRespMapWithId.put("name", classname);
					testRespMapWithId.put("id", testId);
					testRequestResponse.add(testRespMapWithId);
				}
				hhu.when(() -> HttpHelperUtility.getRequest(url, null, null, null, null)).thenReturn(new Gson().toJson(testRequestResponse));
				
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
	void testGetDefaultDistanceMethod() {
		assertEquals("cosine", engine.getDefaultDistanceMethod());
	}
	
	@Test
	void testAddEmbeddings(@TempDir Path tempDir) throws Exception {
		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties
		
		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		
		try(MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);
				MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class)){
			// used in verifyModelProps & addEmbeddings
			u.when(()-> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
			
			// model embedder properties
			Properties embedderProps = new Properties();
			embedderProps.setProperty(Constants.MODEL, embedderModel);
			embedderProps.setProperty(IModelEngine.MODEL_TYPE, embedderModelType);
			when(modelEmbedder.getSmssProp()).thenReturn(embedderProps);
			
			EmbeddingsModelEngineResponse outputMocked = mock(EmbeddingsModelEngineResponse.class);
			when(modelEmbedder.embeddings(new Vector<String>(), insight, null)).thenReturn(outputMocked);
			when(outputMocked.getResponse()).thenReturn(new Vector<List<Double>>());
			
			Map<String, Object> vectors = new HashMap<>();
			List<String> ids = new ArrayList<>();
			List<Float[]> embeddings = new ArrayList<>();
			List<Map<String, Object>> metadatas = new ArrayList<>();
			vectors.put("ids", ids);
			vectors.put("embeddings", embeddings);
			vectors.put("metadatas", metadatas);
			String body = new Gson().toJson(vectors);

			hhu.when(() -> HttpHelperUtility.postRequestStringBody("http://fake.url/" + "TEST_ID" + "/add", 
					null, body, ContentType.APPLICATION_JSON, null, null, null)).thenReturn("response");
			
			engine.addEmbeddings(new VectorDatabaseCSVTable(), insight, null);
			Properties updateEngineProps = engine.getSmssProp();
			assertNotNull(updateEngineProps);
			assertTrue(updateEngineProps.containsKey(Constants.MODEL));
			assertEquals(embedderModel, updateEngineProps.get(Constants.MODEL));
			assertTrue(updateEngineProps.containsKey(IModelEngine.MODEL_TYPE));
			assertEquals(embedderModelType, updateEngineProps.get(IModelEngine.MODEL_TYPE));
		}
	}
	
	@Test
	void testAddEmbeddingsNoEmbedderEngineId(@TempDir Path tempDir) throws Exception {
		openEngine(tempDir, engine, null); // set initial properties
		IllegalArgumentException e = assertThrows(
				IllegalArgumentException.class,
				() -> engine.addEmbeddings(new VectorDatabaseCSVTable(), insight, null));
		assertEquals("Must define the embedder engine id for this vector database using " + Constants.EMBEDDER_ENGINE_ID,
				e.getMessage());
	}
	
	@Test
	void testAddEmbeddingsNoModelEmbedder(@TempDir Path tempDir) throws Exception {
		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties
		
		NullPointerException e = assertThrows(
				NullPointerException.class,
				() -> engine.addEmbeddings(new VectorDatabaseCSVTable(), insight, null));
		assertEquals("Could not find the defined embedder engine id for this vector database with value = " + testEmbedderId,
				e.getMessage());

	}
	
	@Test
	void testAddEmbedderNoModelProperties(@TempDir Path tempDir) throws Exception {
		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties
		
		try(MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);){
			// used in verifyModelProps & addEmbeddings
			u.when(()-> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
			
			// model embedder properties
			Properties embedderProps = new Properties();
			when(modelEmbedder.getSmssProp()).thenReturn(embedderProps);
			
			IllegalArgumentException e = assertThrows(
					IllegalArgumentException.class,
					() -> engine.addEmbeddings(new VectorDatabaseCSVTable(), insight, null));
			assertEquals("Embedder engine exists but does not contain key " + Constants.MODEL,
					e.getMessage());		
		}
	}
	
	@Test
	void testAddEmbedderNoInsight(@TempDir Path tempDir) throws Exception {
		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties
		
		try(MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);){
			// used in verifyModelProps & addEmbeddings
			u.when(()-> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
			
			// model embedder properties
			Properties embedderProps = new Properties();
			embedderProps.setProperty(Constants.MODEL, "embedder_model");
			embedderProps.setProperty(IModelEngine.MODEL_TYPE, "embedder_model_type");
			when(modelEmbedder.getSmssProp()).thenReturn(embedderProps);

			IllegalArgumentException e = assertThrows(
					IllegalArgumentException.class,
					() -> engine.addEmbeddings(new VectorDatabaseCSVTable(), null, null));
			assertEquals("Insight must be provided to run Model Engine Encoder",
					e.getMessage());	
		}
	}
	
	@Test
	void testRemoveDocument(@TempDir Path tempDir) throws Exception {
		String classname = "TEST_CHROMA_CLASS";
		String testId = "TEST_ID";
		
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
		testProps.setProperty(Constants.HOSTNAME, url);
		testProps.setProperty(ChromaVectorDatabaseEngine.CHROMA_CLASSNAME, classname);
		
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

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
				 MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);) {
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineAssetFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineVersionFolder.toString());

				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine, testEngineAlias))
						.thenReturn(engineAssetFolder.toString());
				// used in createCollection()
				List<Map<String, Object>> testRequestResponse = new Vector<>();
				{
					// this will be the return value for this response with a name
					Map<String, Object> testRespMapWithId = new HashMap<>();
					testRespMapWithId.put("name", classname);
					testRespMapWithId.put("id", testId);
					testRequestResponse.add(testRespMapWithId);
				}
				hhu.when(() -> HttpHelperUtility.getRequest(url, null, null, null, null)).thenReturn(new Gson().toJson(testRequestResponse));
				
				engine.open(testProps);
				
				hhu.when(() -> HttpHelperUtility.postRequestStringBody(any(String.class), nullable(Map.class),
						any(String.class), any(ContentType.class), nullable(String.class), nullable(String.class),
						nullable(String.class))).thenReturn("response");

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
		
		try(MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);
				MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class)){
			// used in verifyModelProps & addEmbeddings
			u.when(()-> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
			
			// model embedder properties
			Properties embedderProps = new Properties();
			embedderProps.setProperty(Constants.MODEL, "embedder_model");
			embedderProps.setProperty(IModelEngine.MODEL_TYPE, "embedder_model_type");
			when(modelEmbedder.getSmssProp()).thenReturn(embedderProps);
			///// all previous statements were for verifyModelProps()
			
			// mocking getting embeddings
			EmbeddingsModelEngineResponse responseMocked = mock(EmbeddingsModelEngineResponse.class);
			when(modelEmbedder.embeddings(Arrays.asList(new String[] {searchStatement}), insight, null)).thenReturn(responseMocked);
			when(responseMocked.getResponse()).thenReturn(mockedEmbeddingList);
			
			// mocking http request
			Map<String, Object> query = new HashMap<>();
			List<List<Double>> queryEmbeddings = new ArrayList<>();
			// this is done to put a list of embeddings inside another list otherwise the
			// API throws error.
			queryEmbeddings.add(mockedEmbeddings); 
			Gson gson = new Gson();
											
			query.put("query_texts", searchStatement);
			query.put("n_results", limit);
			query.put("query_embeddings", queryEmbeddings);
			String body = gson.toJson(query);
			Map<String, Object> responseMap = new HashMap<>();
			List<Map<String, Object>> metadatas = new Vector<>();
			Map<String, Object> metadata = new HashMap<>();
			{
				metadata.put("test_meta", "meta1");
				metadatas.add(metadata);
				responseMap.put("metadatas", metadatas);
			}
			String nearestNeigborResponse = gson.toJson(responseMap);
			
			hhu.when(()-> HttpHelperUtility.postRequestStringBody(
					"http://fake.url/" + "TEST_ID" + "/query",
					null, body, ContentType.APPLICATION_JSON, null, null, null)).thenReturn(nearestNeigborResponse);
			/*
			 * TODO there is an issue with mismatched return values on ChromaVectorDatabaseEngine
			 * update this once the issue is fixed
			 */
//			List<Map<String, Object>> returnMetadata = engine.nearestNeighborCall(insight, searchStatement, limit, null);
//			assertEquals(metadata, returnMetadata);
		}
	}
	
	@Test
	void testNearestNeighborCallNoInsight() {
		Number limit = 1;
		String searchStatement = "searchStatement";
		IllegalArgumentException e = assertThrows(
				IllegalArgumentException.class,
				()->engine.nearestNeighborCall(null, searchStatement, limit, null));
		assertEquals("Insight must be provided to run Model Engine Encoder", 
				e.getMessage());
	}
	
	@Test
	void testListDocuments(@TempDir Path tempDir) throws Exception {
		String classname = "TEST_CHROMA_CLASS";
		String testId = "TEST_ID";
		
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
		testProps.setProperty(Constants.HOSTNAME, url);
		testProps.setProperty(ChromaVectorDatabaseEngine.CHROMA_CLASSNAME, classname);
		
		String indexClass = "INDEX_CLASS";

		String engineNameAndId = SmssUtilities.getUniqueName(testEngineAlias, testEngine);
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(engineNameAndId);
		Path engineAssetFolder = engineFolder.resolve("assets");
		Path engineVersionFolder = engineFolder.resolve("version");

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

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
				 MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);) {
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineAssetFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineVersionFolder.toString());

				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine, testEngineAlias))
						.thenReturn(engineAssetFolder.toString());
				// used in createCollection()
				List<Map<String, Object>> testRequestResponse = new Vector<>();
				{
					// this will be the return value for this response with a name
					Map<String, Object> testRespMapWithId = new HashMap<>();
					testRespMapWithId.put("name", classname);
					testRespMapWithId.put("id", testId);
					testRequestResponse.add(testRespMapWithId);
				}
				hhu.when(() -> HttpHelperUtility.getRequest(url, null, null, null, null)).thenReturn(new Gson().toJson(testRequestResponse));
				
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
		Map<String, Object> parameters = new HashMap<>();
		IllegalArgumentException e = assertThrows(
				IllegalArgumentException.class,
				() -> engine.listAllRecords(parameters));
		assertEquals("This method has not been implemented yet", e.getMessage());
	}
	
	@Test
	void testGetVectorDatabaseType() {
		assertEquals(VectorDatabaseTypeEnum.CHROMA, engine.getVectorDatabaseType());
	}
	
	/*
	 * Set up the properties to allow for other operations
	 */
	void openEngine(Path tempDir, ChromaVectorDatabaseEngine engine, Map<String, String> extraProps) throws Exception {
		String classname = "TEST_CHROMA_CLASS";
		String testId = "TEST_ID";
		
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
		testProps.setProperty(Constants.HOSTNAME, url);
		testProps.setProperty(ChromaVectorDatabaseEngine.CHROMA_CLASSNAME, classname);
		if (extraProps != null) {
			for (Entry<String, String> entry : extraProps.entrySet()) {
				testProps.setProperty(entry.getKey(), entry.getValue());
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
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
				 MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);) {
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineAssetFolder.toString());
				eu.when(() -> EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
						.thenReturn(engineVersionFolder.toString());

				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine, testEngineAlias))
						.thenReturn(engineAssetFolder.toString());
				// used in createCollection()
				List<Map<String, Object>> testRequestResponse = new Vector<>();
				{
					// this will be the return value for this response with a name
					Map<String, Object> testRespMapWithId = new HashMap<>();
					testRespMapWithId.put("name", classname);
					testRespMapWithId.put("id", testId);
					testRequestResponse.add(testRespMapWithId);
				}
				hhu.when(() -> HttpHelperUtility.getRequest(url, null, null, null, null)).thenReturn(new Gson().toJson(testRequestResponse));
				
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
