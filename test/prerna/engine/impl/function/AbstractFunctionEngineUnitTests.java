package prerna.engine.impl.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertLinesMatch;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.SemossUnitTest;
import prerna.auth.User;
import prerna.engine.api.IEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.om.Insight;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineUtility;
import prerna.util.UploadUtilities;

public class AbstractFunctionEngineUnitTests extends SemossUnitTest {

	private Insight insight;
	private User user;
	private AbstractFunctionEngine engine;

	private class FunctionEngine extends AbstractFunctionEngine {

		@Override
		public Object execute(Map<String, Object> parameterValues) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getCatalogSubType(Properties smssProp) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub

		}

	}

	@BeforeEach
	void setUp() throws IOException {
		FileUtils.cleanDirectory(tempDir.toFile());
		user = mock(User.class);
		// using inner class to access abstract methods
		engine = new FunctionEngine();
		insight = mock(Insight.class);
	}

	@Test
	void testOpenWithFile() throws Exception {
		Properties testProps = new Properties();
		String testEngine = "asdf-1234";
		String testEngineName = "engine_name";
		String funtionName = "function_name";
		String functionDescription = "function_description";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineName);
		testProps.setProperty(IFunctionEngine.NAME_KEY, funtionName);
		testProps.setProperty(IFunctionEngine.DESCRIPTION_KEY, functionDescription);

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

		engine.setBasic(true);
		engine.open(propsFilePath.toString());
		Properties engineProps = engine.getSmssProp();
		for (Entry<Object, Object> testProp : testProps.entrySet()) {
			assertTrue(engineProps.containsKey(testProp.getKey()));
			assertTrue(engineProps.containsValue(testProp.getValue()));
		}
		assertEquals(propsFilePath.toString(), engine.getSmssFilePath());
	}

	@Test
	void testOpenWithProperties() throws Exception {
		Properties testProps = new Properties();
		String testEngine = "asdf-1234";
		String testEngineName = "engine_name";
		String functionName = "function_name";
		String functionDescription = "function_description";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineName);
		testProps.setProperty(IFunctionEngine.NAME_KEY, functionName);
		testProps.setProperty(IFunctionEngine.DESCRIPTION_KEY, functionDescription);
		engine.setBasic(true);
		engine.open(testProps);
		Properties engineProps = engine.getSmssProp();
		for (Entry<Object, Object> testProp : testProps.entrySet()) {
			assertTrue(engineProps.containsKey(testProp.getKey()));
			assertTrue(engineProps.containsValue(testProp.getValue()));
		}
	}

	@Test
	void testOpenNoNameKey() throws Exception {
		Properties testProps = new Properties();
		String testEngine = "asdf-1234";
		String testEngineName = "engine_name";
		String funtionName = "function_name";
		String functionDescription = "function_description";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineName);
		testProps.setProperty(IFunctionEngine.DESCRIPTION_KEY, functionDescription);

		engine.setBasic(true);
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> engine.open(testProps));
		assertEquals("Must have key " + IFunctionEngine.NAME_KEY + " in SMSS", e.getMessage());
	}

	@Test
	void testOpenNoDescriptionKey() throws Exception {
		Properties testProps = new Properties();
		String testEngine = "asdf-1234";
		String testEngineName = "engine_name";
		String funtionName = "function_name";
		String functionDescription = "function_description";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineName);
		testProps.setProperty(IFunctionEngine.NAME_KEY, funtionName);

		engine.setBasic(true);
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> engine.open(testProps));
		assertEquals("Must have key " + IFunctionEngine.DESCRIPTION_KEY + " in SMSS", e.getMessage());
	}

	@Test
	void testDelete() throws Exception {
		Properties testProps = new Properties();
		String testEngine = "asdf-1234";
		String testEngineName = "engine_name";
		String funtionName = "function_name";
		String functionDescription = "function_description";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineName);
		testProps.setProperty(IFunctionEngine.NAME_KEY, funtionName);
		testProps.setProperty(IFunctionEngine.DESCRIPTION_KEY, functionDescription);

		// create the engine folder that will be deleted
		String engineFolder = tempDir.toString() + "/" + Constants.FUNCTION_FOLDER + "/"
				+ SmssUtilities.getUniqueName(testEngineName, testEngine);
		Path engineFolderPath = Paths.get(engineFolder);
		Files.createDirectories(engineFolderPath);

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

		// close
		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder);
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					// mock UploadUtilities so that nothing happens when methods of this class are
					// called
					MockedStatic<UploadUtilities> uu = Mockito.mockStatic(UploadUtilities.class);
					MockedStatic<AssetUtility> au = Mockito.mockStatic(AssetUtility.class);) {
				eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(any(IEngine.CATALOG_TYPE.class), anyString()))
						.thenReturn(engineFolder);
				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(any(IEngine.CATALOG_TYPE.class), anyString()))
						.thenReturn(engineFolder);
				au.when(() -> AssetUtility.isGit(isNull())).thenReturn(true);

				// open the function with the file
				engine.open(propsFilePath.toString());
				// verify the prop file path was stored
				assertEquals(propsFilePath.toString(), engine.getSmssFilePath());
				assertTrue(Files.exists(engineFolderPath));
				assertTrue(Files.exists(propsFilePath));
				engine.delete();
				assertFalse(Files.exists(engineFolderPath));
				assertFalse(Files.exists(propsFilePath));
			}
		}
	}

	@Test
	void testGetFunctionDefinitionJson() throws Exception {
		String funtionName = "function_name";
		String functionDescription = "function_description";
		openEngine(engine, null);
		JSONObject functionJson = engine.getFunctionDefintionJson();

		assertEquals(funtionName, functionJson.get("name"));
		assertEquals(functionDescription, functionJson.get("description"));
		assertTrue(functionJson.getJSONObject("parameters").isEmpty());
		assertTrue(functionJson.getJSONArray("required").isEmpty());
	}

	@Test
	void testGetSetEngineId() throws Exception {
		String testEngine = "asdf-1234";
		openEngine(engine, null); // set initial engine id
		assertEquals(testEngine, engine.getEngineId());
		String newEngineId = "qwer-5678";
		engine.setEngineId(newEngineId);
		assertEquals(newEngineId, engine.getEngineId());
	}

	@Test
	void testGetSetEngineName() throws Exception {
		String testEngineName = "engine_name";
		openEngine(engine, null); // set initial engine id
		assertEquals(testEngineName, engine.getEngineName());
		String newEngineName = "new_engine_name";
		engine.setEngineName(newEngineName);
		assertEquals(newEngineName, engine.getEngineName());
	}

	@Test
	void testGetSetFunctionName() throws Exception {
		String funtionName = "function_name";
		openEngine(engine, null); // set initial engine id
		assertEquals(funtionName, engine.getFunctionName());
		String newFunctionName = "new_function_name";
		engine.setFunctionName(newFunctionName);
		assertEquals(newFunctionName, engine.getFunctionName());
	}

	@Test
	void testGetSetDescriptionName() throws Exception {
		String functionDescription = "function_description";
		openEngine(engine, null); // set initial engine id
		assertEquals(functionDescription, engine.getFunctionDescription());
		String newFunctionDescription = "new_function_name";
		engine.setFunctionDescription(newFunctionDescription);
		assertEquals(newFunctionDescription, engine.getFunctionDescription());
	}

	@Test
	void testGetSetFunctionParameters() {
		String funcParamName = "func_param_name";
		String funcParamType = "func_param_type";
		String funcParamDesc = "func_param_desc";
		List<FunctionParameter> params = new Vector<>();
		FunctionParameter param = new FunctionParameter(funcParamName, funcParamType, funcParamDesc);
		params.add(param);

		engine.setParameters(params);

		List<FunctionParameter> engineParams = engine.getParameters();

		assertEquals(params.size(), engineParams.size());
		for (FunctionParameter engineParam : engineParams) {
			assertEquals(funcParamName, engineParam.getParameterName());
			assertEquals(funcParamType, engineParam.getParameterType());
			assertEquals(funcParamDesc, engineParam.getParameterDescription());
		}
	}

	@Test
	void testGetSetRequiredParameters() {
		List<String> requiredParams = new Vector<>();
		{
			for (int idx = 0; idx < 5; idx++) {
				requiredParams.add("required_param" + idx);
			}
		}
		engine.setRequiredParameters(requiredParams);

		List<String> engineReqParams = engine.getRequiredParameters();
		assertEquals(requiredParams.size(), engineReqParams.size());
		for (int reqIdx = 0; reqIdx < engineReqParams.size(); reqIdx++) {
			assertEquals(requiredParams.get(reqIdx), engineReqParams.get(reqIdx));
		}
	}

	@Test
	void testGetSetSmssFilePath() throws Exception {
		openEngine(engine, null); // set initial engine id
		assertNull(engine.getSmssFilePath());
		String newSmssFilePath = tempDir.toString();
		engine.setSmssFilePath(newSmssFilePath);
		assertEquals(newSmssFilePath, engine.getSmssFilePath());
	}

	@Test
	void testGetCatalogType() {
		assertEquals(IEngine.CATALOG_TYPE.FUNCTION, engine.getCatalogType());
	}

	@Test
	void testHoldsFileLocks() {
		assertFalse(engine.holdsFileLocks());
	}

	void openEngine(AbstractFunctionEngine engine, Map<String, String> extraProps) throws Exception {
		Properties testProps = new Properties();
		String testEngine = "asdf-1234";
		String testEngineName = "engine_name";
		String funtionName = "function_name";
		String functionDescription = "function_description";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineName);
		testProps.setProperty(IFunctionEngine.NAME_KEY, funtionName);
		testProps.setProperty(IFunctionEngine.DESCRIPTION_KEY, functionDescription);

		if (extraProps != null) {
			for (Entry<String, String> entry : extraProps.entrySet()) {
				testProps.setProperty(entry.getKey(), entry.getValue());
			}
		}

		engine.setBasic(true);
		engine.open(testProps);
		Properties engineProps = engine.getSmssProp();
		for (Entry<Object, Object> testProp : testProps.entrySet()) {
			assertTrue(engineProps.containsKey(testProp.getKey()));
			assertTrue(engineProps.containsValue(testProp.getValue()));
		}
	}
}
