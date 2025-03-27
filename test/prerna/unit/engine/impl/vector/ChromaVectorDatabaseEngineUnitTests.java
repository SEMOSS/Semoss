package prerna.unit.engine.impl.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.io.FilenameUtils;
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
	
	@BeforeEach
	void setUp() {
		engine = new ChromaVectorDatabaseEngine();
		insight = mock(Insight.class);
		user = mock(User.class);
		engineFileDirectory = ApiTestsSemossConstants.BASE_DIRECTORY;
		
		modelEmbedder = mock(IModelEngine.class);
	}
	
	private IModelEngine getAbstractEmbedder() {
		return new IModelEngine() {
			@Override
			public void setEngineId(String engineId) {
				// TODO Auto-generated method stub	
			}
			@Override
			public String getEngineId() {
				// TODO Auto-generated method stub
				return null;
			}
			@Override
			public void setEngineName(String engineName) {
				// TODO Auto-generated method stub
				
			}
			@Override
			public String getEngineName() {
				// TODO Auto-generated method stub
				return null;
			}
			@Override
			public void open(String smssFilePath) throws Exception {
				// TODO Auto-generated method stub
				
			}
			@Override
			public void open(Properties smssProp) throws Exception {
				// TODO Auto-generated method stub
				
			}
			@Override
			public void setSmssFilePath(String smssFilePath) {
				// TODO Auto-generated method stub
				
			}
			@Override
			public String getSmssFilePath() {
				// TODO Auto-generated method stub
				return null;
			}
			@Override
			public void setSmssProp(Properties smssProp) {
				// TODO Auto-generated method stub
				
			}
			@Override
			public Properties getSmssProp() {
				// TODO Auto-generated method stub
				return null;
			}
			@Override
			public Properties getOrigSmssProp() {
				// TODO Auto-generated method stub
				return null;
			}
			@Override
			public CATALOG_TYPE getCatalogType() {
				// TODO Auto-generated method stub
				return null;
			}
			@Override
			public String getCatalogSubType(Properties smssProp) {
				// TODO Auto-generated method stub
				return null;
			}
			@Override
			public void delete() throws IOException {
				// TODO Auto-generated method stub
				
			}
			@Override
			public boolean holdsFileLocks() {
				// TODO Auto-generated method stub
				return false;
			}
			@Override
			public void close() throws IOException {
				// TODO Auto-generated method stub
				
			}
			@Override
			public ModelTypeEnum getModelType() {
				// TODO Auto-generated method stub
				return null;
			}
			@Override
			public AskModelEngineResponse ask(String question, String context, Insight insight,
					Map<String, Object> parameters) {
				// TODO Auto-generated method stub
				return null;
			}
			@Override
			public InstructModelEngineResponse instruct(String task, String context,
					List<Map<String, Object>> projectData, Insight insight, Map<String, Object> parameters) {
				// TODO Auto-generated method stub
				return null;
			}
			@Override
			public EmbeddingsModelEngineResponse embeddings(List<String> stringsToEmbed, Insight insight,
					Map<String, Object> parameters) {
				// TODO Auto-generated method stub
				return null;
			}
			@Override
			public EmbeddingsModelEngineResponse imageEmbeddings(List<String> imagesToEmbed, Insight insight,
					Map<String, Object> parameters) {
				// TODO Auto-generated method stub
				return null;
			}
		};
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
//		String schemaFolder = 
		openEngine(engine, null); // set initial properties
		// both objects needed for method call
		Map<String, Object> parameters = new HashMap<>();
		List<String> fileNames = new Vector<>(); 
		String doc1 = "doc_1";
		{
			fileNames.add(doc1);
		}
		File file = mock(File.class);
		when(file.exists()).thenReturn(false);
		try(MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);
				MockedStatic<Paths> paths = Mockito.mockStatic(Paths.class);){
			
			Map<String, Object> fileNamesForDelete = new HashMap<>();
			Map<String, String> sourceProperty = new HashMap<>();

			// replace spaces with _ since thats how
			// readCSV creates Source Property.
			sourceProperty.put("Source", doc1.replaceAll(" ", "_")); 
																			
			fileNamesForDelete.put("where", sourceProperty);

			String body = new Gson().toJson(fileNamesForDelete);
			
			hhu.when(() -> HttpHelperUtility.postRequestStringBody("http://fake.url/" + "TEST_ID" + "/add", 
					null, body, ContentType.APPLICATION_JSON, null, null, null)).thenReturn("response");
			
			Path p = mock(Path.class);
			paths.when(() -> Paths.get(doc1)).thenReturn(p);
			when(p.getFileName()).thenReturn(p);
			when(p.toString()).thenReturn(doc1);
		}
	}
	
	@Test
	void testNearestNeighborCall() {
		
	}
	
	@Test
	void testListDocuments() {
		
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
	 * Set up the properties file to allow for other operations
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
