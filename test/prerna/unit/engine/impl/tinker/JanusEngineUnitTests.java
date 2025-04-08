package prerna.unit.engine.impl.tinker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.jupiter.api.Test;
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
	
	// java.lang.NoClassDefFoundError: org/apache/tinkerpop/gremlin/groovy/jsr223/GremlinGroovyScriptEngine
//	at org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration$4.<init>(GraphDatabaseConfiguration.java:1307)
//	at org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.<clinit>(GraphDatabaseConfiguration.java:1304)
//	at org.janusgraph.core.JanusGraphFactory.getLocalConfiguration(JanusGraphFactory.java:382)
//	at org.janusgraph.core.JanusGraphFactory.getLocalConfiguration(JanusGraphFactory.java:312)
//	at org.janusgraph.core.JanusGraphFactory.open(JanusGraphFactory.java:94)
//	at prerna.engine.impl.tinker.JanusEngine.open(JanusEngine.java:22)
//	at prerna.unit.engine.impl.tinker.JanusEngineUnitTests.testOpenEmptyGraph(JanusEngineUnitTests.java:85)
//	at java.lang.reflect.Method.invoke(Unknown Source)
//	at java.util.ArrayList.forEach(Unknown Source)
//	at java.util.ArrayList.forEach(Unknown Source)
//Caused by: java.lang.ClassNotFoundException: org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine
//	at java.net.URLClassLoader.findClass(Unknown Source)
//	at java.lang.ClassLoader.loadClass(Unknown Source)
//	at sun.misc.Launcher$AppClassLoader.loadClass(Unknown Source)
//	at java.lang.ClassLoader.loadClass(Unknown Source)
//	... 10 more
	
//	@Test
//	public void testOpenEmptyGraph() {
//		// creating janus smss prop file
//		Properties smssProp = new Properties();
//		String filePath = "janus.properties";
//		String engineId = "engineId";
//		String engineName = "janusTest";
//		String owlFileStr = "janusTest.owl";
//		String typeMapStr = "";
//		String nameMapStr = "";
//		String tinkerDriver = "JANUS";
//		Properties janusProps = new Properties();
//		String janusFilePath = "janus.properties";
//		
//		try (FileOutputStream output = new FileOutputStream(janusFilePath)) {
//			janusProps.setProperty("storage.backend", "inmemory");
//			janusProps.setProperty("gremlin.tinkergraph.graph-location", "graph.db");
//			janusProps.store(output, "janus properties");
//		} catch (IOException io) {
//			io.printStackTrace();
//			fail();
//		}
//		
//		try (FileOutputStream output = new FileOutputStream(filePath)) {
//			smssProp.setProperty(Constants.ENGINE, engineId);
//			smssProp.setProperty(Constants.ENGINE_ALIAS, engineName);
//			smssProp.setProperty(Constants.OWL, owlFileStr);
//			smssProp.setProperty("TYPE_MAP", typeMapStr);
//			smssProp.setProperty("NAME_MAP", nameMapStr);
//			
//			// hacky to get janus engine to work, we need to fix open in tinkerEngine
//			smssProp.setProperty(Constants.TINKER_FILE, janusFilePath);
//			smssProp.setProperty(Constants.TINKER_DRIVER, tinkerDriver);
//
////	smssProp.store(output, "tinker engine props");
//		} catch (IOException io) {
//			io.printStackTrace();
//			fail();
//		}
//
//		try {
//			try (MockedStatic<SmssUtilities> smssUtils = Mockito.mockStatic(SmssUtilities.class);
////			MockedStatic<FileSystems> fss = Mockito.mockStatic(FileSystems.class);
//					MockedStatic<UploadUtilities> uploadUtils = Mockito.mockStatic(UploadUtilities.class)) {
//
////		fss.when(FileSystems::getDefault).thenReturn(fs);
//
//				// static test setup
//				File owlFile = new File(engineName + ".OWL");
//				File janusFile = new File(janusFilePath);
//				uploadUtils.when(() -> UploadUtilities
//						.generateOwlFile(IEngine.CATALOG_TYPE.DATABASE, engineId, engineName).getAbsolutePath())
//						.thenReturn(owlFile);
//				smssUtils.when(() -> SmssUtilities.getOwlFile(smssProp)).thenReturn(owlFile);
//				smssUtils.when(() -> SmssUtilities.getTinkerFile(Mockito.any())).thenReturn(janusFile);
//				smssUtils.when(() -> SmssUtilities.getJanusFile(Mockito.any())).thenReturn(janusFile);
//				// testing open
//				JanusEngine je = new JanusEngine();
//				je.open(smssProp);
//
//				// validations
//				// empty graph
//				Graph graph = je.getGraph();
//				Long count = graph.traversal().V().count().next();
//				assertEquals(0, count);
//
//				assertTrue(je.getTypeMap().isEmpty());
//				assertTrue(je.getNameMap().isEmpty());
//				je.close();
//
////		GraphSONIo reader = Mockito.mock(GraphSONIo.class);
////		Mockito.verify(reader).readGraph(tinkerFile.getAbsolutePath());
//			}
//
//		} catch (Exception e) {
//			e.printStackTrace();
//			fail();
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
