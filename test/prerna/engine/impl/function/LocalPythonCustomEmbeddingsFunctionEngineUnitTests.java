package prerna.engine.impl.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Map.Entry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.engine.api.IEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.om.Insight;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineUtility;

public class LocalPythonCustomEmbeddingsFunctionEngineUnitTests {
	private Insight insight;
	private User user;
	private LocalPythonCustomEmbeddingsFunctionEngine engine;

	@BeforeEach
	void setUp() {
		user = mock(User.class);
		// using inner class to access abstract methods
		engine = new LocalPythonCustomEmbeddingsFunctionEngine();
		insight = mock(Insight.class);
	}

	@Test
	void testOpenWithProperties(@TempDir Path tempDir) throws Exception {
		Properties testProps = new Properties();
		String testEngine = "asdf-1234";
		String testEngineAlias = "engine_name";
		String functionName = "function_name";
		String functionDescription = "function_description";
		String testPythonFileName = "test python file name";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineAlias);
		testProps.setProperty(IFunctionEngine.NAME_KEY, functionName);
		testProps.setProperty(IFunctionEngine.DESCRIPTION_KEY, functionDescription);
		testProps.setProperty("PYTHON_FILE_NAME", testPythonFileName);

		Path engineFolder = tempDir.resolve(Constants.FUNCTION_FOLDER)
				.resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path pyDir = engineFolder.resolve("py");

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);) {
				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(any(IEngine.CATALOG_TYPE.class), anyString(),
						anyString())).thenReturn(engineFolder.toString());

				engine.setBasic(true);
				engine.open(testProps);
				Properties engineProps = engine.getSmssProp();
				for (Entry<Object, Object> testProp : testProps.entrySet()) {
					assertTrue(engineProps.containsKey(testProp.getKey()));
					assertTrue(engineProps.containsValue(testProp.getValue()));
				}
			}
		}
	}

	@Test
	void testOpenWithPropertiesNoPythonFunction(@TempDir Path tempDir) throws Exception {
		Properties testProps = new Properties();
		String testEngine = "asdf-1234";
		String testEngineAlias = "engine_name";
		String functionName = "function_name";
		String functionDescription = "function_description";
		String testPythonFileName = "test python file name";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineAlias);
		testProps.setProperty(IFunctionEngine.NAME_KEY, functionName);
		testProps.setProperty(IFunctionEngine.DESCRIPTION_KEY, functionDescription);
//		testProps.setProperty("PYTHON_FILE_NAME", testPythonFileName);

		Path engineFolder = tempDir.resolve(Constants.FUNCTION_FOLDER)
				.resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path pyDir = engineFolder.resolve("py");

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);) {
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.FUNCTION, testEngine,
						testEngineAlias)).thenReturn(engineFolder.toString());

				engine.setBasic(true);
				IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> engine.open(testProps));
				assertEquals("Please enter the name of the python file used to instantiate the function.",
						e.getMessage());
			}
		}
	}

	@Test
	void testCanProcessDocumentNoFunctionName(@TempDir Path tempDir) throws Exception {
		// canProcessFunctionName string will be null
		assertTrue(engine.canProcessDocument(null));

		// canProcessFunctionName will be empty String
		Properties testProps = new Properties();
		String testEngine = "asdf-1234";
		String testEngineAlias = "engine_name";
		String functionName = "function_name";
		String functionDescription = "function_description";
		String testPythonFileName = "test python file name";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineAlias);
		testProps.setProperty(IFunctionEngine.NAME_KEY, functionName);
		testProps.setProperty(IFunctionEngine.DESCRIPTION_KEY, functionDescription);
		testProps.setProperty("PYTHON_FILE_NAME", testPythonFileName);

		Path engineFolder = tempDir.resolve(Constants.FUNCTION_FOLDER)
				.resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path pyDir = engineFolder.resolve("py");

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);) {
				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(any(IEngine.CATALOG_TYPE.class), anyString(),
						anyString())).thenReturn(engineFolder.toString());

				engine.setBasic(true);
				engine.open(testProps);
				Properties engineProps = engine.getSmssProp();
				for (Entry<Object, Object> testProp : testProps.entrySet()) {
					assertTrue(engineProps.containsKey(testProp.getKey()));
					assertTrue(engineProps.containsValue(testProp.getValue()));
				}
			}
		}
		assertTrue(engine.canProcessDocument(null));
	}
	
	@Test
	void testExecute() {
		IllegalArgumentException e = assertThrows(
				IllegalArgumentException.class, 
				() -> engine.execute(null));
		assertEquals("This function engine is only intended to be executed for custom vector db embeddings",
				e.getMessage());
	}
	
	@Test
	void testGetCatalogSubType() {
		assertEquals("LOCAL_PYTHON_CUSTOM_EMBEDDINGS", engine.getCatalogSubType(null));
	}
}
