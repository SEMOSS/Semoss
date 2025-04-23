package prerna.engine.impl.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertLinesMatch;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Vector;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.ds.py.PyTranslator;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.engine.impl.CaseInsensitiveProperties;
import prerna.engine.impl.SmssUtilities;
import prerna.om.ClientProcessWrapper;
import prerna.om.Insight;
import prerna.security.HttpHelperUtility;
import prerna.tcp.client.SocketClient;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineUtility;
import prerna.util.SymlinkHelper;
import prerna.util.UploadUtilities;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class AbstractVectorDatabaseUnitTests {
	private User user;
	private Insight insight;
	private AbstractVectorDatabaseEngine engine;
	private IModelEngine modelEmbedder;
	
	private class VectorDatabaseEngine extends AbstractVectorDatabaseEngine {
		@Override
		public VectorDatabaseTypeEnum getVectorDatabaseType() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void addEmbeddings(VectorDatabaseCSVTable vectorCsvTable, Insight insight, Map<String, Object> parameters)
				throws Exception {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void removeDocument(List<String> fileNames, Map<String, Object> parameters) throws Exception {
			// TODO Auto-generated method stub
			
		}

		@Override
		public List<Map<String, Object>> listDocuments(Map<String, Object> parameters) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public List<Map<String, Object>> listAllRecords(Map<String, Object> parameters) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		protected String getDefaultDistanceMethod() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		protected List<Map<String, Object>> nearestNeighborCall(Insight insight, String searchStatement, Number limit,
				Map<String, Object> parameters) {
			// TODO Auto-generated method stub
			return null;
		}
	}
	
	@BeforeEach
	void setUp() {
		user = mock(User.class);
		// using inner class to access abstract methods
		engine = new VectorDatabaseEngine();
		insight = mock(Insight.class);
		modelEmbedder = mock(IModelEngine.class);
	}
	
	@Test
	void testOpenWithFile(@TempDir Path tempDir) throws Exception {
		Properties testProps = new Properties();
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
		String contentLength = "10";
		String contentOverlap = "10";
		String keepInputOutput = "false";
		String chunk = "tokens";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineAlias);
		testProps.setProperty(Constants.CONTENT_LENGTH, contentLength);
		testProps.setProperty(Constants.CONTENT_OVERLAP, contentOverlap);
		testProps.setProperty(Constants.KEEP_INPUT_OUTPUT, keepInputOutput);
		testProps.setProperty(Constants.DEFAULT_CHUNK_UNIT, chunk);

		String engineFolder = tempDir.toString() + "/" + Constants.VECTOR_FOLDER + "/"
				+ SmssUtilities.getUniqueName(testEngineAlias, testEngine);
		String schemaDir = engineFolder + "/schema";
		Path shemaDirPath = Paths.get(schemaDir);
		
		// create props File
		String mainDir = tempDir.toString();
		Path mainDirPath = Paths.get(mainDir);
		String propsFileName = "test.properties";
		Path propsFilePath = mainDirPath.resolve(propsFileName);
		Files.createFile(propsFilePath);
		List<String> lines = new Vector<>();
		for (Entry<Object, Object> entry : testProps.entrySet()) {
			lines.add(entry.getKey().toString() + "  " + entry.getValue().toString());
		}
	    Files.write(propsFilePath, lines);
	    assertLinesMatch(lines, Files.readAllLines(propsFilePath));

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder);
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);) {
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineFolder);
				
				engine.open(propsFilePath.toString());
				assertTrue(Files.exists(shemaDirPath));
				Properties engineProps = engine.getSmssProp();
				for (Entry<Object, Object> testProp : testProps.entrySet()) {
					assertTrue(engineProps.containsKey(testProp.getKey()));
					assertTrue(engineProps.containsValue(testProp.getValue()));
				}
			}
		}
	}
	
	@Test
	void testOpenWithProperties(@TempDir Path tempDir) throws Exception {
		Properties testProps = new Properties();
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
		String contentLength = "10";
		String contentOverlap = "10";
		String keepInputOutput = "false";
		String chunk = "tokens";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineAlias);
		testProps.setProperty(Constants.CONTENT_LENGTH, contentLength);
		testProps.setProperty(Constants.CONTENT_OVERLAP, contentOverlap);
		testProps.setProperty(Constants.KEEP_INPUT_OUTPUT, keepInputOutput);
		testProps.setProperty(Constants.DEFAULT_CHUNK_UNIT, chunk);

		String engineFolder = tempDir.toString() + "/" + Constants.VECTOR_FOLDER + "/"
				+ SmssUtilities.getUniqueName(testEngineAlias, testEngine);
		String schemaDir = engineFolder + "/schema";
		Path shemaDirPath = Paths.get(schemaDir);

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder);
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);) {
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineFolder);
				
				engine.open(testProps);
				assertTrue(Files.exists(shemaDirPath));
				Properties engineProps = engine.getSmssProp();
				for (Entry<Object, Object> testProp : testProps.entrySet()) {
					assertTrue(engineProps.containsKey(testProp.getKey()));
					assertTrue(engineProps.containsValue(testProp.getValue()));
				}
			}
		}
	}
	
	@Test
	void testOpenInvalidChunkUnit(@TempDir Path tempDir) throws Exception {
		Properties testProps = new Properties();
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
		String contentLength = "10";
		String contentOverlap = "10";
		String keepInputOutput = "false";
		String chunk = "invalid";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineAlias);
		testProps.setProperty(Constants.CONTENT_LENGTH, contentLength);
		testProps.setProperty(Constants.CONTENT_OVERLAP, contentOverlap);
		testProps.setProperty(Constants.KEEP_INPUT_OUTPUT, keepInputOutput);
		testProps.setProperty(Constants.DEFAULT_CHUNK_UNIT, chunk);

		String engineFolder = tempDir.toString() + "/" + Constants.VECTOR_FOLDER + "/"
				+ SmssUtilities.getUniqueName(testEngineAlias, testEngine);
		String schemaDir = engineFolder + "/schema";
		Path shemaDirPath = Paths.get(schemaDir);

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder);
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);) {
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineFolder);
				
				IllegalArgumentException e = assertThrows(
						IllegalArgumentException.class,
						()->engine.open(testProps));
				assertEquals("DEFAULT_CHUNK_UNIT should be either 'tokens' or 'characters'", e.getMessage());
			}
		}
	}
	
	@Test
	void testAddDocument(@TempDir Path tempDir) throws Exception {
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
		String testEmbedderId = "123-456-789";
		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties
		verifyModelProps(engine, testEmbedderId, embedderModel, embedderModelType);
		
		String indexClass = "index_class";
		
		Map<String, Object> parameters = new HashMap<>();
		parameters.put("indexClass", indexClass);
		parameters.put(AbstractVectorDatabaseEngine.INSIGHT, insight);
		
		String engineFolder = tempDir.toString() + "/" + Constants.VECTOR_FOLDER + "/"
				+ SmssUtilities.getUniqueName(testEngineAlias, testEngine);
		String schemaDir = engineFolder + "/schema";
		String indexDir = schemaDir + "/" + indexClass;
		Path indexDirPath = Paths.get(indexDir);
		Files.createDirectories(indexDirPath);
		// create schema/index_class/documents
		Path docDirPath = indexDirPath.resolve(AbstractVectorDatabaseEngine.DOCUMENTS_FOLDER_NAME);
		Files.createDirectories(docDirPath);
		// create schem/index_class/indexed_files
		Path indexFileDirPath = indexDirPath.resolve(AbstractVectorDatabaseEngine.INDEXED_FOLDER_NAME);
		Files.createDirectories(indexFileDirPath);

		// we need to create a few files in the "insight" folder
		String insightDir = tempDir.toString() + "/insight";
		Path insightDirPath = Paths.get(insightDir);
		Files.createDirectories(insightDirPath);
		// create 2 files for our insight folder
		String fileName1 = "newFile1.txt";
		String fileName2 = "newFile2.txt";
		Path file1 = insightDirPath.resolve(fileName1);
		Path file2 = insightDirPath.resolve(fileName2);
		Files.createFile(file1);
		Files.createFile(file2);
		List<String> fileNames = new Vector<>();
		fileNames.add(file1.toString());
		fileNames.add(file2.toString());
		
		SocketClient scMock = mock(SocketClient.class); // used in addDocument()->checkSocketStatus()->startServer()
		when(scMock.isConnected()).thenReturn(true);
		try (MockedConstruction<ClientProcessWrapper> mockWrapper = Mockito// used in
																			// addDocument()->checkSocketStatus()->startServer()
				.mockConstruction(ClientProcessWrapper.class, (mock, context) -> {
					doNothing().when(mock).createProcessAndClient(any(boolean.class), nullable(SymlinkHelper.class),
							any(int.class), nullable(String.class), nullable(String.class), nullable(String.class),
							any(boolean.class), any(String.class), any(String.class));
					doNothing().when(mock).shutdown(false);
					when(mock.getSocketClient()).thenReturn(scMock);

				});
				MockedConstruction<PyTranslator> mockPYT = Mockito.mockConstruction(PyTranslator.class, // used in
																										// addDocument()->checkSocketStatus()->startServer()
						(mock, context) -> {
							doNothing().when(mock).setSocketClient(scMock);
							doNothing().when(mock).runEmptyPy(any());
							when(mock.runScript(any())).thenReturn(true);
						});
				MockedStatic<VectorDatabaseCSVTable> vdcsvt = Mockito.mockStatic(VectorDatabaseCSVTable.class);) {

			// used for addEmbeddings() portion
			VectorDatabaseCSVTable vectorCsvTableMock = mock();
			vdcsvt.when(() -> VectorDatabaseCSVTable.initCSVTable(any())).thenReturn(vectorCsvTableMock);
			try (MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);
					MockedStatic<AbstractSqlQueryUtil> asqu = Mockito.mockStatic(AbstractSqlQueryUtil.class);) {
				u.when(() -> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
				u.when(() -> Utility.normalizePath(file1.toString())).thenReturn(file1.toString());
				u.when(() -> Utility.normalizePath(file2.toString())).thenReturn(file2.toString());

				// used in getConnection()
				Connection mockConn = mock(Connection.class);
				asqu.when(() -> AbstractSqlQueryUtil.makeConnection(any(AbstractSqlQueryUtil.class), any(String.class),
						any(CaseInsensitiveProperties.class))).thenReturn(mockConn);
				PreparedStatement psMock = mock();
				when(mockConn.prepareStatement(any())).thenReturn(psMock);
				when(psMock.executeBatch()).thenReturn(new int[0]);

				assertTrue(Files.exists(file1));
				assertTrue(Files.exists(file2));
				engine.addDocument(fileNames, parameters);
				assertTrue(Files.exists(file1)); // remains in insight folder
				assertTrue(Files.exists(file2)); // remains in insight folder
				assertTrue(Files.exists(docDirPath.resolve(fileName1))); // moved to doc folder
				assertTrue(Files.exists(docDirPath.resolve(fileName2))); // moved to doc folder
			}
		}
	}
	
	@Test
	void testAddDocumentNoInsight(@TempDir Path tempDir) throws Exception {
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
		String testEmbedderId = "123-456-789";
		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties
		verifyModelProps(engine, testEmbedderId, embedderModel, embedderModelType);
		
		String indexClass = "index_class";
		
		Map<String, Object> parameters = new HashMap<>();
		parameters.put("indexClass", indexClass);
		
		String engineFolder = tempDir.toString() + "/" + Constants.VECTOR_FOLDER + "/"
				+ SmssUtilities.getUniqueName(testEngineAlias, testEngine);
		String schemaDir = engineFolder + "/schema";
		String indexDir = schemaDir + "/" + indexClass;
		Path indexDirPath = Paths.get(indexDir);
		Files.createDirectories(indexDirPath);
		// create schema/index_class/documents
		Path docDirPath = indexDirPath.resolve(AbstractVectorDatabaseEngine.DOCUMENTS_FOLDER_NAME);
		Files.createDirectories(docDirPath);
		// create schem/index_class/indexed_files
		Path indexFileDirPath = indexDirPath.resolve(AbstractVectorDatabaseEngine.INDEXED_FOLDER_NAME);
		Files.createDirectories(indexFileDirPath);

		// we need to create a few files in the "insight" folder
		String insightDir = tempDir.toString() + "/insight";
		Path insightDirPath = Paths.get(insightDir);
		Files.createDirectories(insightDirPath);
		// create 2 files for our insight folder
		String fileName1 = "newFile1.txt";
		String fileName2 = "newFile2.txt";
		Path file1 = insightDirPath.resolve(fileName1);
		Path file2 = insightDirPath.resolve(fileName2);
		Files.createFile(file1);
		Files.createFile(file2);
		List<String> fileNames = new Vector<>();
		fileNames.add(file1.toString());
		fileNames.add(file2.toString());
		
		SocketClient scMock = mock(SocketClient.class); // used in addDocument()->checkSocketStatus()->startServer()
		when(scMock.isConnected()).thenReturn(true);
		try (MockedConstruction<ClientProcessWrapper> mockWrapper = Mockito// used in
																			// addDocument()->checkSocketStatus()->startServer()
				.mockConstruction(ClientProcessWrapper.class, (mock, context) -> {
					doNothing().when(mock).createProcessAndClient(any(boolean.class), nullable(SymlinkHelper.class),
							any(int.class), nullable(String.class), nullable(String.class), nullable(String.class),
							any(boolean.class), any(String.class), any(String.class));
					doNothing().when(mock).shutdown(false);
					when(mock.getSocketClient()).thenReturn(scMock);

				});
				MockedConstruction<PyTranslator> mockPYT = Mockito.mockConstruction(PyTranslator.class, // used in
																										// addDocument()->checkSocketStatus()->startServer()
						(mock, context) -> {
							doNothing().when(mock).setSocketClient(scMock);
							doNothing().when(mock).runEmptyPy(any());
							when(mock.runScript(any())).thenReturn(true);
						});
				MockedStatic<VectorDatabaseCSVTable> vdcsvt = Mockito.mockStatic(VectorDatabaseCSVTable.class);) {

			// used for addEmbeddings() portion
			VectorDatabaseCSVTable vectorCsvTableMock = mock();
			vdcsvt.when(() -> VectorDatabaseCSVTable.initCSVTable(any())).thenReturn(vectorCsvTableMock);
			try (MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);
					MockedStatic<AbstractSqlQueryUtil> asqu = Mockito.mockStatic(AbstractSqlQueryUtil.class);) {
				u.when(() -> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
				u.when(() -> Utility.normalizePath(file1.toString())).thenReturn(file1.toString());
				u.when(() -> Utility.normalizePath(file2.toString())).thenReturn(file2.toString());

				// used in getConnection()
				Connection mockConn = mock(Connection.class);
				asqu.when(() -> AbstractSqlQueryUtil.makeConnection(any(AbstractSqlQueryUtil.class), any(String.class),
						any(CaseInsensitiveProperties.class))).thenReturn(mockConn);
				PreparedStatement psMock = mock();
				when(mockConn.prepareStatement(any())).thenReturn(psMock);
				when(psMock.executeBatch()).thenReturn(new int[0]);

				IllegalArgumentException e = assertThrows(
						IllegalArgumentException.class,
						()->engine.addDocument(fileNames, parameters));
				assertEquals("Insight must be provided to run Model Engine Encoder", e.getMessage());
			}
		}
	}
	
	@Test
	void testCleanUpDocument(@TempDir Path tempDir) throws Exception {
		// we need to create a file in the "insight" folder
		String insightDir = tempDir.toString() + "/insight";
		Path insightDirPath = Paths.get(insightDir);
		Files.createDirectories(insightDirPath);
		// create file for our insight folder
		String fileName1 = "newFile1.txt";
		Path file1 = insightDirPath.resolve(fileName1);
		Files.createFile(file1);
		assertTrue(Files.exists(file1));
		engine.cleanUpAddDocument(file1.toFile());
		assertFalse(Files.exists(file1));
	}

	@Test
	void testGetIndexFilePath(@TempDir Path tempDir) throws Exception {
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
	    String indexClass = "index_class";
		
	    openEngine(tempDir, engine, null);
		String engineFolder = tempDir.toString() + "/" + Constants.VECTOR_FOLDER + "/"
 				+ SmssUtilities.getUniqueName(testEngineAlias, testEngine);
 		String schemaDir = engineFolder + "/schema"; 		
 		String indexDir = schemaDir + "/" + indexClass + "/indexed_files";
	    
	    engine.addIndexClass(indexClass);
	    assertEquals(Utility.normalizePath(indexDir), engine.getIndexFilesPath(indexClass)); 
	}
	
	@Test
	void testGetIndexFilePathInvalidFile(@TempDir Path tempDir) throws Exception {
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
	    String indexClass = "index_class";
		
	    openEngine(tempDir, engine, null);
		String engineFolder = tempDir.toString() + "/" + Constants.VECTOR_FOLDER + "/"
 				+ SmssUtilities.getUniqueName(testEngineAlias, testEngine);
 		String schemaDir = engineFolder + "/schema"; 		
 		String indexDir = schemaDir + "/" + indexClass + "/indexed_files";
	    
 		IllegalArgumentException e = assertThrows(
 				IllegalArgumentException.class, 
 				()->engine.getIndexFilesPath(indexClass));
 		assertEquals("Unable to retieve document csv from a directory that does not exist", e.getMessage());
	}
	
	@Test
	void testGetDocumentsFilesPath(@TempDir Path tempDir) throws Exception {
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
	    String indexClass = "index_class";
		
	    openEngine(tempDir, engine, null);
		
	 // run this part first to create an index class in the schema directory
 		String engineFolder = tempDir.toString() + "/" + Constants.VECTOR_FOLDER + "/"
 				+ SmssUtilities.getUniqueName(testEngineAlias, testEngine);
 		String schemaDir = engineFolder + "/schema";
 		String docDir = schemaDir + "/" + indexClass + "/" + AbstractVectorDatabaseEngine.DOCUMENTS_FOLDER_NAME;
 		
	    engine.addIndexClass(indexClass);
	    assertEquals(Utility.normalizePath(docDir), engine.getDocumentsFilesPath(indexClass));
	}
	
	@Test
	void testGetDocumentsFilesPathInvalidDir(@TempDir Path tempDir) throws Exception {
		String nonExistantClass = "doesNotExist";
		openEngine(tempDir, engine, null); // adds default index to engine
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> engine.getDocumentsFilesPath(nonExistantClass));
		assertEquals("Unable to retieve document csv from a directory that does not exist", e.getMessage());
	}
	
	@Test
	void testVerifyModelProps(@TempDir Path tempDir) throws Exception {
		String testEmbedderId = "123-456-789";
		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties
		try (MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);) {
			u.when(() -> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
			// model embedder properties
			Properties embedderProps = new Properties();
			embedderProps.setProperty(Constants.MODEL, embedderModel);
			embedderProps.setProperty(IModelEngine.MODEL_TYPE, embedderModelType);
			when(modelEmbedder.getSmssProp()).thenReturn(embedderProps);
			engine.verifyModelProps();
			// verify new props
			Properties engineProps = engine.getSmssProp();
			assertTrue(engineProps.containsKey(Constants.MAX_TOKENS));
			assertEquals("None", engineProps.get(Constants.MAX_TOKENS));
			assertTrue(engineProps.containsKey(Constants.KEYWORD_ENGINE_ID));
			assertEquals("", engineProps.get(Constants.KEYWORD_ENGINE_ID));
		}
	}
	
	@Test
	void testVerifyModelPropsNoEmbedderEngineId(@TempDir Path tempDir) throws Exception {
		String testEmbedderId = "123-456-789";
		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		Map<String, String> extraProps = new HashMap<>();
//		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties
		try (MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);) {
			u.when(() -> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
			// model embedder properties
			Properties embedderProps = new Properties();
			embedderProps.setProperty(Constants.MODEL, embedderModel);
			embedderProps.setProperty(IModelEngine.MODEL_TYPE, embedderModelType);
			when(modelEmbedder.getSmssProp()).thenReturn(embedderProps);
			IllegalArgumentException e = assertThrows(
					IllegalArgumentException.class,
					()->engine.verifyModelProps());
			assertEquals("Must define the embedder engine id for this vector database using " + Constants.EMBEDDER_ENGINE_ID, e.getMessage());
		}
	}
	
	@Test
	void testVerifyModelPropsNoEmbedderEngine(@TempDir Path tempDir) throws Exception {
		String testEmbedderId = "123-456-789";
		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties
		try (MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);) {
			u.when(() -> Utility.getModel(testEmbedderId)).thenReturn(null);
			// model embedder properties
			Properties embedderProps = new Properties();
			embedderProps.setProperty(Constants.MODEL, embedderModel);
			embedderProps.setProperty(IModelEngine.MODEL_TYPE, embedderModelType);
			when(modelEmbedder.getSmssProp()).thenReturn(embedderProps);
			NullPointerException e = assertThrows(
					NullPointerException.class,
					()->engine.verifyModelProps());
			assertEquals("Could not find the defined embedder engine id for this vector database with value = "
					+ testEmbedderId, e.getMessage());
		}
	}
	
	@Test
	void testVerifyModelPropsNoEmbedderProperties(@TempDir Path tempDir) throws Exception {
		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties
		try (MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);) {
			u.when(() -> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
			when(modelEmbedder.getSmssProp()).thenReturn(new Properties());
			IllegalArgumentException e = assertThrows(
					IllegalArgumentException.class,
					()->engine.verifyModelProps());
			assertEquals("Embedder engine exists but does not contain key "
					+ Constants.MODEL, e.getMessage());
		}
	}
	
	@Test
	void testUserCanAccessEmbeddingModels(@TempDir Path tempDir) throws Exception {
		String testEmbedderId = "123-456-789";
		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties
		verifyModelProps(engine, testEmbedderId, embedderModel, embedderModelType);
		
		try(MockedStatic<SecurityEngineUtils> seu = Mockito.mockStatic(SecurityEngineUtils.class);){
			// used in userCanAccessEmbeddingModels
			seu.when(()->SecurityEngineUtils.userCanViewEngine(user, testEmbedderId)).thenReturn(true);
			
			assertTrue(engine.userCanAccessEmbeddingModels(user));
			// verify added embedder engine props
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
	void testUserCanAccessEmbeddingModelsInvalid(@TempDir Path tempDir) throws Exception {
		String testEmbedderId = "123-456-789";
		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties
		verifyModelProps(engine, testEmbedderId, embedderModel, embedderModelType);
		
		try(MockedStatic<SecurityEngineUtils> seu = Mockito.mockStatic(SecurityEngineUtils.class);){
			// used in userCanAccessEmbeddingModels
			seu.when(()->SecurityEngineUtils.userCanViewEngine(user, testEmbedderId)).thenReturn(false);
			
			IllegalArgumentException e = assertThrows(
					IllegalArgumentException.class, 
					()->engine.userCanAccessEmbeddingModels(user));
			assertEquals(
					"Embeddings model " + testEmbedderId + " does not exist or user does not have access to this model"
					,e.getMessage());
		}
	}
	
	@Test
	void testFillVars(@TempDir Path tempDir) throws Exception {
		String testEmbedderId = "123-456-789";
		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties
		verifyModelProps(engine, testEmbedderId, embedderModel, embedderModelType);
		
		String toBeFilled = "The embedder model is name ${" 
		+ Constants.MODEL 
		+ "} and is of type ${" 
		+ IModelEngine.MODEL_TYPE 
		+ "} with ID: ${" + Constants.EMBEDDER_ENGINE_ID + "}";
		String filledStr = engine.fillVars(toBeFilled);
		
		String expected = "The embedder model is name " 
				+ embedderModel 
				+ " and is of type " 
				+ embedderModelType 
				+ " with ID: " + testEmbedderId;
		assertEquals(expected, filledStr);
	}
	
	@Test
	void testGetServerStartCommands() {
		String[] cmmnds = engine.getServerStartCommands();
		List<String> expectedCmmnds = new Vector<>();
		expectedCmmnds.add("from genai_client import get_tokenizer");
		expectedCmmnds.add("cfg_tokenizer = get_tokenizer(tokenizer_name = '${MODEL}', max_tokens = ${MAX_TOKENS}, tokenizer_type = '${MODEL_TYPE}')");
		expectedCmmnds.add("import vector_database");
		assertLinesMatch(expectedCmmnds, Arrays.asList(cmmnds));
	}
	
	@Test
	void testGetInsight() {
		assertEquals(insight, engine.getInsight(insight));
	}
	
	@Test
	void testKeepInputOutput(@TempDir Path tempDir) throws Exception {
		openEngine(tempDir, engine, null); // set initial properties
		assertFalse(engine.keepInputOutput());
	}
	
	@Test
	void testGetSetEngineId(@TempDir Path tempDir) throws Exception {
		String testEngine = "asdf-1234";
		openEngine(tempDir, engine, null); // set initial engine id
		assertEquals(testEngine, engine.getEngineId());
		String newEngineId = "qwer-5678";
		engine.setEngineId(newEngineId);
		assertEquals(newEngineId, engine.getEngineId());
	}
	
	@Test
	void testGetSetEngineName(@TempDir Path tempDir) throws Exception {
		String testEngineAlias = "TEST_ALIAS";
		openEngine(tempDir, engine, null); // set initial engine id
		assertEquals(testEngineAlias, engine.getEngineName());
		String newEngineName = "NEW_ALIAS";
		engine.setEngineName(newEngineName);
		assertEquals(newEngineName, engine.getEngineName());
	}
	
	@Test
	void testGetSetSmssFilePath(@TempDir Path tempDir) throws Exception {
		openEngine(tempDir, engine, null); // set initial engine id
		assertNull(engine.getSmssFilePath());
		String newSmssFilePath = tempDir.toString();
		engine.setSmssFilePath(newSmssFilePath);
		assertEquals(newSmssFilePath, engine.getSmssFilePath());
	}
	
	@Test
	void testGetCatalogType() {
		assertEquals(IEngine.CATALOG_TYPE.VECTOR, engine.getCatalogType());
	}
	
	@Test
	void testDelete(@TempDir Path tempDir) throws Exception {
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
		String testEmbedderId = "123-456-789";
		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties
		verifyModelProps(engine, testEmbedderId, embedderModel, embedderModelType);
		
		String indexClass = "index_class";
		
		Map<String, Object> parameters = new HashMap<>();
		parameters.put("indexClass", indexClass);
		parameters.put(AbstractVectorDatabaseEngine.INSIGHT, insight);
		
		String engineFolder = tempDir.toString() + "/" + Constants.VECTOR_FOLDER + "/"
				+ SmssUtilities.getUniqueName(testEngineAlias, testEngine);
		String schemaDir = engineFolder + "/schema";
		Path schemPath = Paths.get(schemaDir);
		String indexDir = schemaDir + "/" + indexClass;
		Path indexDirPath = Paths.get(indexDir);
		Files.createDirectories(indexDirPath);
		// create schema/index_class/documents
		Path docDirPath = indexDirPath.resolve(AbstractVectorDatabaseEngine.DOCUMENTS_FOLDER_NAME);
		Files.createDirectories(docDirPath);
		// create schem/index_class/indexed_files
		Path indexFileDirPath = indexDirPath.resolve(AbstractVectorDatabaseEngine.INDEXED_FOLDER_NAME);
		Files.createDirectories(indexFileDirPath);

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder);
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);
					MockedStatic<UploadUtilities> uu = Mockito.mockStatic(UploadUtilities.class);) {
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineFolder);
				assertTrue(Files.exists(schemPath)); // created by open()
				assertTrue(Files.exists(indexDirPath));
				assertTrue(Files.exists(docDirPath));
				assertTrue(Files.exists(indexFileDirPath));
				engine.setSmssFilePath("no/file"); // to avoid null pointer exception
				engine.delete();
				assertFalse(Files.exists(schemPath)); // created by open()
				assertFalse(Files.exists(indexDirPath));
				assertFalse(Files.exists(docDirPath));
				assertFalse(Files.exists(indexFileDirPath));
			}
		}
	}
	
	@Test
	void testHoldsFileLocks() {
		assertFalse(engine.holdsFileLocks());
	}

	void openEngine(Path tempDir, AbstractVectorDatabaseEngine engine, Map<String, String> extraProps) throws Exception {
		Properties testProps = new Properties();
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ALIAS";
		String contentLength = "10";
		String contentOverlap = "10";
		String keepInputOutput = "false";
		String chunk = "tokens";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineAlias);
		testProps.setProperty(Constants.CONTENT_LENGTH, contentLength);
		testProps.setProperty(Constants.CONTENT_OVERLAP, contentOverlap);
		testProps.setProperty(Constants.KEEP_INPUT_OUTPUT, keepInputOutput);
		testProps.setProperty(Constants.DEFAULT_CHUNK_UNIT, chunk);
		if (extraProps != null) {
			for (Entry<String, String> entry : extraProps.entrySet()) {
				testProps.setProperty(entry.getKey(), entry.getValue());
			}
		}

		String engineFolder = tempDir.toString() + "/" + Constants.VECTOR_FOLDER + "/"
				+ SmssUtilities.getUniqueName(testEngineAlias, testEngine);
		String schemaDir = engineFolder + "/schema";
		Path shemaDirPath = Paths.get(schemaDir);

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder);
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);) {
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
						testEngineAlias)).thenReturn(engineFolder);
				
				engine.open(testProps);
				assertTrue(Files.exists(shemaDirPath));
				Properties engineProps = engine.getSmssProp();
				for (Entry<Object, Object> testProp : testProps.entrySet()) {
					assertTrue(engineProps.containsKey(testProp.getKey()));
					assertTrue(engineProps.containsValue(testProp.getValue()));
				}
			}
		}
	}
	
	void verifyModelProps(AbstractVectorDatabaseEngine engine, String testEmbedderId, String embedderModel,
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
