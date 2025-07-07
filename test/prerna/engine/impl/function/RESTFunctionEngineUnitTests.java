package prerna.engine.impl.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
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
import prerna.util.Utility;

public class RESTFunctionEngineUnitTests {
	private Insight insight;
	private User user;
	private RESTFunctionEngine engine;

	@BeforeEach
	void setUp() {
		user = mock(User.class);
		// using inner class to access abstract methods
		engine = new RESTFunctionEngine();
		insight = mock(Insight.class);
	}
	
	@Test
	void testOpen() throws Exception {
		Properties testProps = new Properties();
		String testEngine = "asdf-1234";
		String testEngineName = "engine_name";
		String functionName = "function_name";
		String functionDescription = "function_description";
		String httpMethod = "GET";
		String url = "http://test.url";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineName);
		testProps.setProperty(IFunctionEngine.NAME_KEY, functionName);
		testProps.setProperty(IFunctionEngine.DESCRIPTION_KEY, functionDescription);
		testProps.setProperty("HTTP_METHOD", httpMethod);
		testProps.setProperty("URL", url);
		
		try (MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);) {
			engine.open(testProps);
			Properties engineProps = engine.getSmssProp();
			for (Entry<Object, Object> testProp : testProps.entrySet()) {
				assertTrue(engineProps.containsKey(testProp.getKey()));
				assertTrue(engineProps.containsValue(testProp.getValue()));
			}
		}
	}
	
	@Test
	void testOpenNoHTTPMethod() throws Exception {
		Properties testProps = new Properties();
		String testEngine = "asdf-1234";
		String testEngineName = "engine_name";
		String functionName = "function_name";
		String functionDescription = "function_description";
		String httpMethod = "GET";
		String url = "http://test.url";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineName);
		testProps.setProperty(IFunctionEngine.NAME_KEY, functionName);
		testProps.setProperty(IFunctionEngine.DESCRIPTION_KEY, functionDescription);
//		testProps.setProperty("HTTP_METHOD", httpMethod);
		testProps.setProperty("URL", url);
		
		IllegalArgumentException e = assertThrows(
				IllegalArgumentException.class,
				()->engine.open(testProps));
		assertEquals("RESTFunctionEngine only supports GET, HEAD, POST, or PUT requests", e.getMessage());
	}
	
	@Test
	void testOpenNoURL() throws Exception {
		Properties testProps = new Properties();
		String testEngine = "asdf-1234";
		String testEngineName = "engine_name";
		String functionName = "function_name";
		String functionDescription = "function_description";
		String httpMethod = "GET";
		String url = "http://test.url";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineName);
		testProps.setProperty(IFunctionEngine.NAME_KEY, functionName);
		testProps.setProperty(IFunctionEngine.DESCRIPTION_KEY, functionDescription);
		testProps.setProperty("HTTP_METHOD", httpMethod);
//		testProps.setProperty("URL", url);
		
		IllegalArgumentException e = assertThrows(
				IllegalArgumentException.class,
				()->engine.open(testProps));
		assertEquals("Must provide a URL", e.getMessage());
	}
	
	@Test
	void testGetCatalogSubType() {
		assertEquals("REST", engine.getCatalogSubType(null));
	}
}
