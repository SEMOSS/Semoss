package prerna.engine.impl.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Map.Entry;
import java.util.Properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.auth.User;
import prerna.engine.api.IFunctionEngine;
import prerna.om.Insight;
import prerna.util.Constants;

public class StreamRESTFunctionEngineUnitTests {
	private Insight insight;
	private User user;
	private StreamRESTFunctionEngine engine;

	@BeforeEach
	void setUp() {
		user = mock(User.class);
		// using inner class to access abstract methods
		engine = new StreamRESTFunctionEngine();
		insight = mock(Insight.class);
	}
	
	@Test
	void testOpen() throws Exception {
		Properties testProps = new Properties();
		String testEngine = "asdf-1234";
		String testEngineName = "engine_name";
		String functionName = "function_name";
		String functionDescription = "function_description";
		String httpMethod = "HTTP_METHOD";
		String url = "URL";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineName);
		testProps.setProperty(IFunctionEngine.NAME_KEY, functionName);
		testProps.setProperty(IFunctionEngine.DESCRIPTION_KEY, functionDescription);
		testProps.setProperty(httpMethod, "GET");
		testProps.setProperty(url, "som/path/for/url");
		
		engine.setBasic(true);
		engine.open(testProps);
		Properties engineProps = engine.getSmssProp();
		for (Entry<Object, Object> testProp : testProps.entrySet()) {
			assertTrue(engineProps.containsKey(testProp.getKey()));
			assertTrue(engineProps.containsValue(testProp.getValue()));
		}
	}
	
	@Test
	void testOpenBadHTTP() throws Exception {
		Properties testProps = new Properties();
		String testEngine = "asdf-1234";
		String testEngineName = "engine_name";
		String functionName = "function_name";
		String functionDescription = "function_description";
		String httpMethod = "HTTP_METHOD";
		String url = "URL";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineName);
		testProps.setProperty(IFunctionEngine.NAME_KEY, functionName);
		testProps.setProperty(IFunctionEngine.DESCRIPTION_KEY, functionDescription);
		testProps.setProperty(httpMethod, "BAD");
		testProps.setProperty(url, "som/path/for/url");
		
		engine.setBasic(true);
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
		String httpMethod = "HTTP_METHOD";
		String url = "URL";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineName);
		testProps.setProperty(IFunctionEngine.NAME_KEY, functionName);
		testProps.setProperty(IFunctionEngine.DESCRIPTION_KEY, functionDescription);
		testProps.setProperty(httpMethod, "GET");
//		testProps.setProperty(url, "som/path/for/url");
		
		engine.setBasic(true);
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
