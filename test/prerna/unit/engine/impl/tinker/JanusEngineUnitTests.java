package prerna.unit.engine.impl.tinker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.tinker.JanusEngine;
import prerna.util.Constants;
import prerna.util.UploadUtilities;

public class JanusEngineUnitTests {

	///////////// Test Open

	// unable to open graph
	// java.lang.NoClassDefFoundError:
	// org/apache/tinkerpop/gremlin/groovy/jsr223/GremlinGroovyScriptEngine

//	@Test
//	public void testOpenEmptyGraph(@TempDir File tempDir) throws Exception {
//		// create in memory janus config
//		Properties janusProps = new Properties();
//		String janusFilePath = "janus.properties";
//		File janusPropFile = new File(tempDir, janusFilePath);
//		try (FileOutputStream output = new FileOutputStream(janusPropFile)) {
//			janusProps.setProperty("storage.backend", "inmemory");
//			janusProps.setProperty("gremlin.tinkergraph.graph-location", "graph.db");
//			janusProps.store(output, "janus properties");
//		} catch (IOException io) {
//			io.printStackTrace();
//		}
//
//		// creating janus smss prop file
//		Properties smssProp = new Properties();
//		String engineId = "engineId";
//		String engineName = "janusTest";
//		String owlFileStr = "janusTest.owl";
//		String typeMapStr = "";
//		String nameMapStr = "";
//		String tinkerDriver = "JANUS";
//		smssProp.setProperty(Constants.ENGINE, engineId);
//		smssProp.setProperty(Constants.ENGINE_ALIAS, engineName);
//		smssProp.setProperty(Constants.OWL, owlFileStr);
//		smssProp.setProperty("TYPE_MAP", typeMapStr);
//		smssProp.setProperty("NAME_MAP", nameMapStr);
//
//		// hacky to get janus engine to work, we need to fix open in tinkerEngine
//		smssProp.setProperty(Constants.TINKER_FILE, janusFilePath);
//		smssProp.setProperty(Constants.TINKER_DRIVER, tinkerDriver);
//
//		try (MockedStatic<SmssUtilities> smssUtils = Mockito.mockStatic(SmssUtilities.class);
//				MockedStatic<UploadUtilities> uploadUtils = Mockito.mockStatic(UploadUtilities.class)) {
//			// static test setup
//			File owlFile = new File(tempDir, engineName + ".OWL");
//			File janusFile = new File(tempDir, janusFilePath);
//			uploadUtils.when(() -> UploadUtilities.generateOwlFile(IEngine.CATALOG_TYPE.DATABASE, engineId, engineName)
//					.getAbsolutePath()).thenReturn(owlFile);
//			smssUtils.when(() -> SmssUtilities.getOwlFile(smssProp)).thenReturn(owlFile);
//			smssUtils.when(() -> SmssUtilities.getTinkerFile(Mockito.any())).thenReturn(janusFile);
//			smssUtils.when(() -> SmssUtilities.getJanusFile(Mockito.any())).thenReturn(janusFile);
//			// testing open
//			JanusEngine je = new JanusEngine();
//			je.open(smssProp);
//
//			// validations
//			// empty graph
//			Graph graph = je.getGraph();
//			Long count = graph.traversal().V().count().next();
//			assertEquals(0, count);
//
//			assertTrue(je.getTypeMap().isEmpty());
//			assertTrue(je.getNameMap().isEmpty());
//			je.close();
//
//		}
//	}

	@Test
	public void testGetDatabaseType() {
		JanusEngine je = new JanusEngine();
		assertEquals(IDatabaseEngine.DATABASE_TYPE.JANUS_GRAPH, je.getDatabaseType());
		try {
			je.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
