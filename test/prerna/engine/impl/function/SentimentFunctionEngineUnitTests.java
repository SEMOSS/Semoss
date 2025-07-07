package prerna.engine.impl.function;

import static org.junit.jupiter.api.Assertions.assertLinesMatch;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.util.Map.Entry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.engine.api.IFunctionEngine;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.Utility;

public class SentimentFunctionEngineUnitTests {
	private Insight insight;
	private User user;
	private SentimentFunctionEngine engine;

	@BeforeEach
	void setUp() {
		user = mock(User.class);
		// using inner class to access abstract methods
		engine = new SentimentFunctionEngine();
		insight = mock(Insight.class);
	}
	
	@Test
	void testOpenWithFile(@TempDir Path tempDir) throws Exception {
		Properties testProps = new Properties();
		String testEngine = "asdf-1234";
		String testEngineName = "engine_name";
		String funtionName = "function_name";
		String functionDescription = "function_description";
		String initCommands = "command_1;command2";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineName);
		testProps.setProperty(IFunctionEngine.NAME_KEY, funtionName);
		testProps.setProperty(IFunctionEngine.DESCRIPTION_KEY, functionDescription);
		testProps.setProperty(Constants.INIT_MODEL_ENGINE, initCommands);

		// create props File
		String mainDir = tempDir.toString();
		Path mainDirPath = Paths.get(mainDir);
		String propsFileName = "test.properties";
		Path propsFilePath = mainDirPath.resolve(propsFileName);
		Files.createFile(propsFilePath);
		assertTrue(Files.exists(propsFilePath));
		List<String> lines = new Vector<>();
		for (Entry<Object, Object> entry : testProps.entrySet()) {
			lines.add(entry.getKey().toString() + "  " + entry.getValue().toString());
		}
	    Files.write(propsFilePath, lines);
	    assertLinesMatch(lines, Files.readAllLines(propsFilePath));
	    
	    {
	    	when(insight.getInsightId()).thenReturn("");
	    	when(insight.getInsightFolder()).thenReturn("");
	    }
	    fail("fail");
		engine.setInsight(insight);
		engine.open(propsFilePath.toString());
		Properties engineProps = engine.getSmssProp();
		for (Entry<Object, Object> testProp : testProps.entrySet()) {
			assertTrue(engineProps.containsKey(testProp.getKey()));
			assertTrue(engineProps.containsValue(testProp.getValue()));
		}
		assertEquals(propsFilePath.toString(), engine.getSmssFilePath());

	}
}
