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

import org.apache.http.HttpHeaders;
import org.apache.http.entity.ContentType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.google.gson.Gson;

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

public class PineConeVectorDatabaseEngineUnitTests {
	private User user;
	private Insight insight;
	private PineConeVectorDatabaseEngine engine;
	private IModelEngine modelEmbedder;
		
	@BeforeEach
	void setUp() {
		user = mock(User.class);
		engine = new PineConeVectorDatabaseEngine();
		insight = mock(Insight.class);
		modelEmbedder = mock(IModelEngine.class);
	}
	
	@Test
	void testOpen(@TempDir Path tempDir) throws Exception {
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
		
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path schemaPath = engineFolder.resolve("schema");
		
		try(MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);){
			DIHelper diMock = mock(DIHelper.class);			
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);){
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineFolder.toString());
				
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
		
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		
		try(MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);){
			DIHelper diMock = mock(DIHelper.class);			
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);){
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineFolder.toString());
				
				IllegalArgumentException e = assertThrows(
						IllegalArgumentException.class,
						()->engine.open(testProps));
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
		testProps.setProperty(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path schemaPath = engineFolder.resolve("schema");
		
		try(MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);){
			DIHelper diMock = mock(DIHelper.class);			
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);){
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineFolder.toString());	
				engine.open(testProps);
				
				try(MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);
					MockedStatic<HttpHelperUtility>hhu = Mockito.mockStatic(HttpHelperUtility.class);){
				u.when(()-> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
				// model embedder properties
				Properties embedderProps = new Properties();
				embedderProps.setProperty(Constants.MODEL, embedderModel);
				embedderProps.setProperty(IModelEngine.MODEL_TYPE, embedderModelType);
				when(modelEmbedder.getSmssProp()).thenReturn(embedderProps);			
				engine.verifyModelProps();
				// used in addEmbeddings()
				VectorDatabaseCSVTable vectorCsvTableMock = mock();
				vectorCsvTableMock.rows = new Vector<>(); // ensure rows.size() == 0
				doNothing().when(vectorCsvTableMock).generateAndAssignEmbeddings(modelEmbedder, insight);
				Map<String, Object> parameters = new HashMap<>();
				hhu.when(() -> HttpHelperUtility.postRequestStringBody(any(String.class), any(Map.class), any(String.class),
						any(ContentType.class), nullable(String.class), nullable(String.class), nullable(String.class))).thenReturn(null);
				engine.addEmbeddings(vectorCsvTableMock, insight, parameters);		
				
				assertTrue(Files.exists(schemaPath));
				Properties engineProps = engine.getSmssProp();
				for (Entry<Object, Object> testProp : testProps.entrySet()) {
					assertTrue(engineProps.containsKey(testProp.getKey())); 
					assertTrue(engineProps.containsValue(testProp.getValue()));
				}
				assertTrue(engineProps.containsKey(Constants.MODEL));
				assertEquals(embedderModel, engineProps.get(Constants.MODEL));
				assertTrue(engineProps.containsKey(IModelEngine.MODEL_TYPE));
				assertEquals(embedderModelType, engineProps.get(IModelEngine.MODEL_TYPE));
				}
			}
		}
	}
	
	@Test
	void testAddEmbeddingsNoInsight(@TempDir Path tempDir) throws Exception {
		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		String testEmbedderId = "123-456-789";
		
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
		testProps.setProperty(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);		
		
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		
		try(MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);){
			DIHelper diMock = mock(DIHelper.class);			
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);){
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineFolder.toString());
				
				engine.open(testProps);
				verifyModelProps(testEmbedderId, embedderModel, embedderModelType);
				IllegalArgumentException e = assertThrows(
						IllegalArgumentException.class,
						()->engine.addEmbeddings(new VectorDatabaseCSVTable(), null, new HashMap<>()));
				assertEquals("Insight must be provided to run Model Engine Encoder", e.getMessage());
			}
		}
	}
	
	@Test
	void removeDocument(@TempDir Path tempDir) throws Exception {
		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		String testEmbedderId = "123-456-789";
		String indexClass = "default";
		
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
		testProps.setProperty(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);

		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path docDirPath = Paths.get(engineFolder.toString(), "schema", indexClass, "documents");
		Files.createDirectories(docDirPath);
		String fileName = "newFile1.txt";
		List<String> fileNames = new Vector<>();
		fileNames.add(fileName);
		Path newFilePath = docDirPath.resolve(fileName);
		Files.createFile(newFilePath);
		
		try(MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);){
			DIHelper diMock = mock(DIHelper.class);			
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);){
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineFolder.toString());	
				engine.open(testProps);
				
				try (MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);
						MockedStatic<HttpHelperUtility>hhu = Mockito.mockStatic(HttpHelperUtility.class);) {
					u.when(() -> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
					// model embedder properties (for verifyModelProps())
					Properties embedderProps = new Properties();
					embedderProps.setProperty(Constants.MODEL, embedderModel);
					embedderProps.setProperty(IModelEngine.MODEL_TYPE, embedderModelType);
					when(modelEmbedder.getSmssProp()).thenReturn(embedderProps);
					engine.verifyModelProps();
					
					hhu.when(() -> HttpHelperUtility.getRequest(any(String.class), any(Map.class),
							nullable(String.class), nullable(String.class), nullable(String.class))).thenReturn("{}");
					
					assertTrue(Files.exists(newFilePath));
					engine.removeDocument(fileNames, new HashMap<>());
					assertFalse(Files.exists(newFilePath));
				}
			}
		}
	}
	
	@Test
	void testNearestNeighborCall(@TempDir Path tempDir) throws Exception {
		Number limit = 1;
		String searchStatement = "searchStatement";
		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		String testEmbedderId = "123-456-789";
		
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
		testProps.setProperty(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);

		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		
		try(MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);){
			DIHelper diMock = mock(DIHelper.class);			
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);){
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineFolder.toString());	
				engine.open(testProps);
				
				try (MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);
						MockedStatic<HttpHelperUtility>hhu = Mockito.mockStatic(HttpHelperUtility.class);) {
					u.when(() -> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
					// model embedder properties (for verifyModelProps())
					Properties embedderProps = new Properties();
					embedderProps.setProperty(Constants.MODEL, embedderModel);
					embedderProps.setProperty(IModelEngine.MODEL_TYPE, embedderModelType);
					when(modelEmbedder.getSmssProp()).thenReturn(embedderProps);
					engine.verifyModelProps();
					
					EmbeddingsModelEngineResponse embResMock = mock();
					when(modelEmbedder.embeddings(any(), any(), any())).thenReturn(embResMock);
					List<List<Double>> embeddings = new Vector<>();
					embeddings.add(new Vector<>());
					when(embResMock.getResponse()).thenReturn(embeddings);
					
					// creating the json response from http request
					String testSource = "TEST_SOURCE";
					String testModality = "TEST_MODALITY";
					String testDivider = "TEST_DIVIDER";
					String testPart = "TEST_PART";
					int testTokens = 1234;
					String testContent = "TEST_CONTENT";
					String testId = "TEST_ID";
					double testScore = 5.5;
					Map<String, Object> response = new HashMap<>();
					{
						List<Map<String, Object>> matches = new Vector<>();
						{
							Map<String, Object> allMetadata = new HashMap<>();
							{
								Map<String, Object> singleMetadata = new HashMap<>();
								{
									singleMetadata.put(VectorDatabaseCSVTable.CONTENT, testContent);
									singleMetadata.put(VectorDatabaseCSVTable.DIVIDER, testDivider);
									singleMetadata.put(VectorDatabaseCSVTable.MODALITY, testModality);
									singleMetadata.put(VectorDatabaseCSVTable.PART, testPart);
									singleMetadata.put(VectorDatabaseCSVTable.SOURCE, testSource);
									singleMetadata.put(VectorDatabaseCSVTable.TOKENS, testTokens);
								}
								// add it in multiple times
								allMetadata.put("metadata",singleMetadata);
								allMetadata.put("id", testId);
								allMetadata.put("score", testScore);
							}
							matches.add(allMetadata);
							matches.add(allMetadata);
							matches.add(allMetadata);
						}
						response.put("matches", matches);
					}
					String nearestNeigborResponse = new Gson().toJson(response);
					hhu.when(() -> HttpHelperUtility.postRequestStringBody(any(String.class), any(Map.class),
							any(String.class), any(ContentType.class), nullable(String.class), nullable(String.class),
							nullable(String.class))).thenReturn(nearestNeigborResponse);					
					List<Map<String, Object>> returnMetadata = engine.nearestNeighborCall(insight, searchStatement, limit, new HashMap<>());
					assertEquals(3, returnMetadata.size());
					for (Map<String, Object> metadata : returnMetadata) {
						assertEquals(testSource, metadata.get(VectorDatabaseCSVTable.SOURCE));
						assertEquals(testModality, metadata.get(VectorDatabaseCSVTable.MODALITY));
						assertEquals(testDivider, metadata.get(VectorDatabaseCSVTable.DIVIDER));
						assertEquals(testPart, metadata.get(VectorDatabaseCSVTable.PART));
						assertEquals(testTokens, ((Double)metadata.get(VectorDatabaseCSVTable.TOKENS)).intValue());
						assertEquals(testContent, metadata.get(VectorDatabaseCSVTable.CONTENT));
						assertEquals(testScore, metadata.get("Score"));
						assertEquals(testId, metadata.get("Id"));
					}
				}
			}
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
	void testListDocuments(@TempDir Path tempDir) throws Exception {
		String indexClass = "default";
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

		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path docDirPath = Paths.get(engineFolder.toString(), "schema", indexClass, AbstractVectorDatabaseEngine.DOCUMENTS_FOLDER_NAME);
		Files.createDirectories(docDirPath);
		// create 4 new files: newFile1 ... newFile4.txt
		List<String> fileNames = new Vector<>();
		for (int fileNum = 1; fileNum < 5; fileNum++) {
			String fileName = "newFile"+fileNum+".txt";
			fileNames.add(fileName);
			Path newFilePath = docDirPath.resolve(fileName);
			Files.createFile(newFilePath);
		}
		
		try(MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);){
			DIHelper diMock = mock(DIHelper.class);			
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<HttpHelperUtility>hhu = Mockito.mockStatic(HttpHelperUtility.class);){
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineFolder.toString());
				Map<String, Object> response = new HashMap<>();
				{
					List<Map<String, Object>> vectors = new Vector<>();
					{
						for (int vecIdx = 0; vecIdx < fileNames.size(); vecIdx++) {
							Map<String, Object> vector = new HashMap<>();
							vector.put("id", vecIdx + "");
							vectors.add(vector);
						}
					}
					response.put("vectors", vectors);
				}
				Map<String, String> headersMap = new HashMap<>();
				headersMap.put("Api-Key", apiKey);
				headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
				// to generate the idListResponse in listDocuments()
				hhu.when(() -> HttpHelperUtility.getRequest("null/vectors/list?namespace=null", headersMap, null, null,
						null)).thenReturn(new Gson().toJson(response));				
				// used in fetchUniqueResourceValues()
				Map<String, Object> fetchResponse = new HashMap<>();
				{
					Map<String, Object> vectors = new HashMap<>();
					{
						for (int vecIdx = 0; vecIdx < fileNames.size(); vecIdx++) {
							Map<String, Object> record = new HashMap<>();
							{
								Map<String, Object> metadata = new HashMap<>();
								{
									metadata.put(VectorDatabaseCSVTable.SOURCE, fileNames.get(vecIdx));
								}
								record.put("metadata", metadata);
							}
							vectors.put(vecIdx + "", record);
						}
					}
					fetchResponse.put("vectors", vectors);
				}
				hhu.when(() -> HttpHelperUtility.getRequest("null/vectors/fetch?namespace=null&ids=0&ids=1&ids=2&ids=3", headersMap, null, null,
						null)).thenReturn(new Gson().toJson(fetchResponse));
				
				engine.open(testProps);
				
				List<Map<String, Object>> docsOutput = engine.listDocuments(new HashMap<>());
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
	void testListAllRecords(@TempDir Path tempDir) throws Exception {
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
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));

		try(MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);){
			DIHelper diMock = mock(DIHelper.class);			
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<HttpHelperUtility>hhu = Mockito.mockStatic(HttpHelperUtility.class);){
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineFolder.toString());
				Map<String, Object> response = new HashMap<>();
				{
					List<Map<String, Object>> vectors = new Vector<>();
					{
						for (int vecIdx = 0; vecIdx < 1; vecIdx++) {
							Map<String, Object> vector = new HashMap<>();
							vector.put("id", vecIdx + "");
							vectors.add(vector);
						}
					}
					response.put("vectors", vectors);
				}
				Map<String, String> headersMap = new HashMap<>();
				headersMap.put("Api-Key", apiKey);
				headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
				// to generate the idListResponse in listDocuments()
				hhu.when(() -> HttpHelperUtility.getRequest("null/vectors/list?namespace=null", headersMap, null, null,
						null)).thenReturn(new Gson().toJson(response));				
				// used in fetchAllValues()
				String testSource = "TEST_SOURCE";
				String testModality = "TEST_MODALITY";
				String testDivider = "TEST_DIVIDER";
				String testPart = "TEST_PART";
				int testTokens = 123;
				String testContent = "TEST_CONTENT";
				Map<String, Object> fetchResponse = new HashMap<>();
				{
					Map<String, Object> vectors = new HashMap<>();
					{
						for (int vecIdx = 0; vecIdx < 5; vecIdx++) {
							Map<String, Object> record = new HashMap<>();
							{
								Map<String, Object> metadata = new HashMap<>();
								{
									metadata.put(VectorDatabaseCSVTable.SOURCE, testSource);
									metadata.put(VectorDatabaseCSVTable.MODALITY, testModality);
									metadata.put(VectorDatabaseCSVTable.DIVIDER, testDivider);
									metadata.put(VectorDatabaseCSVTable.PART, testPart);
									metadata.put(VectorDatabaseCSVTable.TOKENS, testTokens);
									metadata.put(VectorDatabaseCSVTable.CONTENT, testContent);
								}
								record.put("metadata", metadata);
							}
							vectors.put(vecIdx + "", record);
						}
					}
					fetchResponse.put("vectors", vectors);
				}
				hhu.when(() -> HttpHelperUtility.getRequest("null/vectors/fetch?namespace=null&ids=0", headersMap, null, null,
						null)).thenReturn(new Gson().toJson(fetchResponse));
				
				engine.open(testProps);
				List<Map<String, Object>> listOfRecords = engine.listAllRecords(new HashMap<>());
				assertEquals(5, listOfRecords.size());
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
	}
	
	@Test
	void testGetVectorDatabaseType() {
		assertEquals(VectorDatabaseTypeEnum.PINECONE, engine.getVectorDatabaseType());
	}
	
	void verifyModelProps(String testEmbedderId,String embedderModel,String embedderModelType) {
		try(MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);){
			u.when(()-> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
			// model embedder properties
			Properties embedderProps = new Properties();
			embedderProps.setProperty(Constants.MODEL, embedderModel);
			embedderProps.setProperty(IModelEngine.MODEL_TYPE, embedderModelType);
			when(modelEmbedder.getSmssProp()).thenReturn(embedderProps);			
			engine.verifyModelProps();
		}
	}
}










