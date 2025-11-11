package prerna.engine.impl.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Map.Entry;
import java.util.Properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import com.azure.ai.formrecognizer.documentanalysis.DocumentAnalysisClient;
import com.azure.ai.formrecognizer.documentanalysis.DocumentAnalysisClientBuilder;
import com.azure.core.credential.AzureKeyCredential;

import prerna.auth.User;
import prerna.engine.api.FunctionTypeEnum;
import prerna.engine.api.IFunctionEngine;
import prerna.om.Insight;
import prerna.util.Constants;

public class AzureDocumentIntelligenceCustomEmbeddingsFuntionEngineUnitTests {
	private Insight insight;
	private User user;
	private AzureDocumentIntelligenceCustomEmbeddingsFuntionEngine engine;
	
	@BeforeEach
	void setUp() {
		user = mock(User.class);
		// using inner class to access abstract methods
		engine = new AzureDocumentIntelligenceCustomEmbeddingsFuntionEngine();
		insight = mock(Insight.class);
	}
	
	@Test
	void testOpen() throws Exception {
		Properties testProps = new Properties();
		String testEngine = "asdf-1234";
		String testEngineName = "engine_name";
		String url = "test_url";
		String apiKey  = "api_key";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineName);
		testProps.setProperty("URL", url);
		testProps.setProperty(Constants.API_KEY, apiKey);
		DocumentAnalysisClient clientMock = mock();
		try(MockedConstruction<DocumentAnalysisClientBuilder> mockWrapper = Mockito.mockConstruction(
				DocumentAnalysisClientBuilder.class, (mock, context) -> {
					when(mock.credential(any(AzureKeyCredential.class))).thenReturn(mock);
					when(mock.endpoint(url)).thenReturn(mock);
					when(mock.buildClient()).thenReturn(clientMock);
				})){
			engine.open(testProps);
			Properties engineProps = engine.getSmssProp();
			for (Entry<Object, Object> testProp : testProps.entrySet()) {
				assertTrue(engineProps.containsKey(testProp.getKey()));
				assertTrue(engineProps.containsValue(testProp.getValue()));
			}
			assertTrue(engineProps.containsKey(IFunctionEngine.NAME_KEY));
			assertTrue(engineProps.containsValue("Azure Document Intelligence - For Use With Vector Database Engines"));
			assertTrue(engineProps.containsKey(IFunctionEngine.DESCRIPTION_KEY));
			assertTrue(engineProps.containsValue("Execute Azure Document Intelligence"));
		}
	}
	
	@Test
	void testOpenNoConnectionURL() throws Exception {
		Properties testProps = new Properties();
		String testEngine = "asdf-1234";
		String testEngineName = "engine_name";
		String url = "test_url";
		String apiKey  = "api_key";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineName);
//		testProps.setProperty("URL", url);
		testProps.setProperty(Constants.API_KEY, apiKey);
		DocumentAnalysisClient clientMock = mock();
		try(MockedConstruction<DocumentAnalysisClientBuilder> mockWrapper = Mockito.mockConstruction(
				DocumentAnalysisClientBuilder.class, (mock, context) -> {
					when(mock.credential(any(AzureKeyCredential.class))).thenReturn(mock);
					when(mock.endpoint(url)).thenReturn(mock);
					when(mock.buildClient()).thenReturn(clientMock);
				})){
			IllegalArgumentException e = assertThrows(
					IllegalArgumentException.class,
					()->engine.open(testProps));
			assertEquals("Must pass in the connection url", e.getMessage());
		}
	}
	
	@Test
	void testOpenNoAPIKey() throws Exception {
		Properties testProps = new Properties();
		String testEngine = "asdf-1234";
		String testEngineName = "engine_name";
		String url = "test_url";
		String apiKey  = "api_key";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineName);
		testProps.setProperty("URL", url);
//		testProps.setProperty(Constants.API_KEY, apiKey);
		DocumentAnalysisClient clientMock = mock();
		try(MockedConstruction<DocumentAnalysisClientBuilder> mockWrapper = Mockito.mockConstruction(
				DocumentAnalysisClientBuilder.class, (mock, context) -> {
					when(mock.credential(any(AzureKeyCredential.class))).thenReturn(mock);
					when(mock.endpoint(url)).thenReturn(mock);
					when(mock.buildClient()).thenReturn(clientMock);
				})){
			IllegalArgumentException e = assertThrows(
					IllegalArgumentException.class,
					()->engine.open(testProps));
			assertEquals("Must pass in the api key", e.getMessage());
		}
	}
	
	@Test
	void testExecute() {
		IllegalArgumentException e = assertThrows(
				IllegalArgumentException.class,
				()->engine.execute(null));
		assertEquals("This function engine is only intended to be executed for custom vector db embeddings", e.getMessage());
	}
	
	@Test
	void testCanProcessDocumentNonPDF() {
		File file = new File("test.txt");
		assertFalse(engine.canProcessDocument(file));
	}
	
	@Test
	void testGetCatalogSubType() {
		assertEquals(FunctionTypeEnum.AZURE_DOCUMENT_INTELLIGENCE_CUSTOM_EMBEDDINGS.name(),
				engine.getCatalogSubType(null));
	}
}
