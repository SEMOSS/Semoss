package prerna.unit.engine.impl.vector;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.nullable;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.apache.http.entity.ContentType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import prerna.auth.User;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.engine.impl.model.responses.InstructModelEngineResponse;
import prerna.engine.impl.vector.ChromaVectorDatabaseEngine;
import prerna.engine.impl.vector.VectorDatabaseCSVTable;
import prerna.om.Insight;
import prerna.security.HttpHelperUtility;
import prerna.testing.ApiTestsSemossConstants;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineUtility;
import prerna.util.Utility;

public class ChromaVectorDatabaseEngineUnitTests {

	private User user;
//	private NounStore ns;
	private Insight insight;
//	private GenRowStruct grs;
	private String engineFileDirectory;
	
	private ChromaVectorDatabaseEngine engine;
	private IModelEngine modelEmbedder;
	
	private FileSystem fs;
	
	@BeforeEach
	void setUp() {
		engine = new ChromaVectorDatabaseEngine();
		insight = mock(Insight.class);
		user = mock(User.class);
		engineFileDirectory = ApiTestsSemossConstants.BASE_DIRECTORY;
		
		modelEmbedder = mock(IModelEngine.class);
		
		fs = Jimfs.newFileSystem(Configuration.unix());
	}

	@Test
	void testOpen() throws Exception {
		Properties testProps = new Properties();
		String url = "http://fake.url/";
		String classname = "TEST_CHROMA_CLASS";
		String testId = "TEST_ID";
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
		String testAPIKey = "TEST_API_KEY";
		testProps.setProperty(Constants.HOSTNAME, url);
//		testProps.setProperty(Constants.API_KEY, null);
		testProps.setProperty(ChromaVectorDatabaseEngine.CHROMA_CLASSNAME, classname);
		testProps.setProperty(Constants.KEEP_INPUT_OUTPUT, "false");
		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineAlias);
		
		List<Map<String, Object>> testRequestResponse = new Vector<>();
		{
			// this will be the return value for this response with a name
			Map<String, Object> testRespMapWithId = new HashMap<>();
			testRespMapWithId.put("name", classname);
			testRespMapWithId.put("id", testId);
			testRequestResponse.add(testRespMapWithId);
		}
		
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		String gsonRequest = gson.toJson(testRequestResponse);
		
		try(MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);){
			DIHelper diMock = mock(DIHelper.class);			
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFileDirectory);
			try (MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);
				MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);){
				
				Gson gsonMock = mock(Gson.class);
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine, testEngineAlias)).thenReturn(engineFileDirectory);
				hhu.when(() -> HttpHelperUtility.getRequest(url, null, null, null, null)).thenReturn(gsonRequest);
				when(gsonMock.fromJson(gsonRequest, new TypeToken<List<Map<String, Object>>>() {}.getType())).thenReturn(testRequestResponse);
				
				engine.open(testProps);
			}
		}
	}
	
	@Test
	void testAddEmbeddings() throws Exception {
		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(engine, extraProps); // set initial properties
		
		try(MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);
				MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class)){
			// used in verifyModelProps & addEmbeddings
			u.when(()-> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
			
			// model embedder properties
			Properties embedderProps = new Properties();
			embedderProps.setProperty(Constants.MODEL, "embedder_model");
			embedderProps.setProperty(IModelEngine.MODEL_TYPE, "embedder_model_type");
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
		}
	}
	
	@Test
	void testAddEmbeddingsNoEmbedderEngineId() throws Exception {
		openEngine(engine, null); // set initial properties
		IllegalArgumentException e = assertThrows(
				IllegalArgumentException.class,
				() -> engine.addEmbeddings(new VectorDatabaseCSVTable(), insight, null));
		assertEquals("Must define the embedder engine id for this vector database using " + Constants.EMBEDDER_ENGINE_ID,
				e.getMessage());
	}
	
	@Test
	void testAddEmbeddingsNoModelEmbedder() throws Exception {
		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(engine, extraProps); // set initial properties
		
		NullPointerException e = assertThrows(
				NullPointerException.class,
				() -> engine.addEmbeddings(new VectorDatabaseCSVTable(), insight, null));
		assertEquals("Could not find the defined embedder engine id for this vector database with value = " + testEmbedderId,
				e.getMessage());

	}
	
	@Test
	void testAddEmbedderNoModelProperties() throws Exception {
		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(engine, extraProps); // set initial properties
		
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
	void testAddEmbedderNoInsight() throws Exception {
		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(engine, extraProps); // set initial properties
		
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
	void testRemoveDocument() throws Exception {
		openEngine(engine, null); // set initial properties
		// both objects needed for method call
		Map<String, Object> parameters = new HashMap<>();
		String indexClass = "TEST_INDEX_CLASS";
		parameters.put("indexClass", indexClass);
	
		String fileName1 = "newFile1.txt";
		String fileName2 = "newFile2.txt";
	    Path path = fs.getPath("C:\\workspace\\Semoss_Dev\\schema/" + indexClass + "/documents");
	    Files.createDirectories(path);
	    Files.createFile(path.resolve(fileName1));
	    Files.createFile(path.resolve(fileName2));
	    List<String> fileNames = new Vector<>();
	    fileNames.add(fileName1);
	    fileNames.add(fileName2);
		try(MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);
				MockedStatic<FileSystems> fss = Mockito.mockStatic(FileSystems.class);){
			
			fss.when(FileSystems::getDefault).thenReturn(fs);
			
			// we sub in ANY string for request body and url
			hhu.when(() -> HttpHelperUtility.postRequestStringBody(any(String.class), nullable(Map.class), any(String.class),
					any(ContentType.class), nullable(String.class), nullable(String.class), nullable(String.class))).thenReturn("response");

			fileNames.forEach(fileName -> assertTrue(Files.exists(path.resolve(fileName))));
			engine.removeDocument(fileNames, parameters);
			fileNames.forEach(fileName -> assertFalse(Files.exists(path.resolve(fileName))));
		}
	}
	
	@Test
	void testNearestNeighborCall() throws Exception {
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
		openEngine(engine, extraProps); // set initial properties
		
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
//			ChromaVectorDatabaseEngine engineMocked = mock(ChromaVectorDatabaseEngine.class);
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
											
			// List<Map<String, Object>> metadatas = new ArrayList<>(); add metadata filter
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
	void testListDocuments() throws Exception {
		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(engine, extraProps); // set initial properties
		
		
		Map<String, Object> parameters = new HashMap<>();
		String indexClass = "TEST_INDEX_CLASS";
		parameters.put("indexClass", indexClass);
	
		String fileName = "newFile.txt";
	    Path path = fs.getPath("C:\\workspace\\Semoss_Dev\\schema/" + indexClass + "/documents");
	    Files.createDirectories(path);
	    Path filePath = path.resolve(fileName);
	    Files.createFile(filePath);
		
		try (MockedStatic<FileSystems> fss = Mockito.mockStatic(FileSystems.class);){
			fss.when(FileSystems::getDefault).thenReturn(fs);
			List<Map<String, Object>> fileList = engine.listDocuments(parameters);
			assertEquals(1, fileList.size());
			Map<String, Object> fileData = fileList.get(0);
			assertEquals(fileName, fileData.get("fileName"));
			assertEquals(0.0, fileData.get("fileSize"));
			Date fileDate = (Date) fileData.get("lastModified");
			LocalDate lfd = fileDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
			LocalDate todaysDate = LocalDate.now();
			assertEquals(todaysDate, lfd);
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
	void openEngine(ChromaVectorDatabaseEngine engine, Map<String, String> extraProps) throws Exception {
		Properties testProps = new Properties();
		String url = "http://fake.url/";
		String classname = "TEST_CHROMA_CLASS";
		String testId = "TEST_ID";
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
		testProps.setProperty(Constants.HOSTNAME, url);
		testProps.setProperty(ChromaVectorDatabaseEngine.CHROMA_CLASSNAME, classname);
		testProps.setProperty(Constants.KEEP_INPUT_OUTPUT, "false");
		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineAlias);
		if (extraProps != null) {
			for (Map.Entry<String, String> extraPropsEntry : extraProps.entrySet()) {
				String key = extraPropsEntry.getKey();
				String prop = extraPropsEntry.getValue();
				testProps.setProperty(key, prop);
			}
		}
		List<Map<String, Object>> testRequestResponse = new Vector<>();
		{
			// this will be the return value for this response with a name
			Map<String, Object> testRespMapWithId = new HashMap<>();
			testRespMapWithId.put("name", classname);
			testRespMapWithId.put("id", testId);
			testRequestResponse.add(testRespMapWithId);
		}
		
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		String gsonRequest = gson.toJson(testRequestResponse);
		
		try(MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);){
			DIHelper diMock = mock(DIHelper.class);			
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFileDirectory);
			try (MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);
				MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);){
				
				Gson gsonMock = mock(Gson.class);
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine, testEngineAlias)).thenReturn(engineFileDirectory);
				hhu.when(() -> HttpHelperUtility.getRequest(url, null, null, null, null)).thenReturn(gsonRequest);
				when(gsonMock.fromJson(gsonRequest, new TypeToken<List<Map<String, Object>>>() {}.getType())).thenReturn(testRequestResponse);
				
				engine.open(testProps);
			}
		}
	}
}
