package prerna.unit.engine.impl.tinker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.tinker.TinkerEngine;
import prerna.engine.impl.tinker.TinkerEngine.TINKER_DRIVER;
import prerna.query.interpreters.GremlinNoEdgeBindInterpreter;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

public class TinkerEngineUnitTests {

	///////////// Test Open
	@Test
	public void testOpenEmptyGraph(@TempDir File tempDir) throws Exception {
		// testing setup
		String engineId = "engineId";
		String engineName = "tinkerTest";
		String smssFilePath = engineName + "__" + engineId + ".smss";
		String engineFolder = tempDir + "\\" + engineName;

		// creating tinker smss prop file
		Properties smssProp = new Properties();
		String owlFileStr = "tinkerTest.owl";
		String typeMapStr = "";
		String nameMapStr = "";
		String tinkerDriver = TINKER_DRIVER.JSON.toString();
		String tinkerFilePath = "tinkerTest.json";
		smssProp.setProperty(Constants.ENGINE, engineId);
		smssProp.setProperty(Constants.ENGINE_ALIAS, engineName);
		smssProp.setProperty(Constants.OWL, owlFileStr);
		smssProp.setProperty("TYPE_MAP", typeMapStr);
		smssProp.setProperty("NAME_MAP", nameMapStr);
		smssProp.setProperty(Constants.TINKER_DRIVER, tinkerDriver);
		try (MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class);) {
			utility.when(() -> Utility.getBaseFolder()).thenReturn(tempDir.getAbsolutePath());

			try (MockedStatic<SmssUtilities> smssUtils = Mockito.mockStatic(SmssUtilities.class);
//					MockedStatic<EngineUtility> engineUtils = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<UploadUtilities> uploadUtils = Mockito.mockStatic(UploadUtilities.class)) {
				// static test setup
				File owlFile = new File(tempDir, engineName + ".OWL");
				File tinkerFile = new File(tempDir, tinkerFilePath);
				uploadUtils.when(() -> UploadUtilities
						.generateOwlFile(IEngine.CATALOG_TYPE.DATABASE, engineId, engineName).getAbsolutePath())
						.thenReturn(owlFile);
				smssUtils.when(() -> SmssUtilities.getOwlFile(smssFilePath, smssProp)).thenReturn(owlFile);
				smssUtils.when(() -> SmssUtilities.getTinkerFile(smssProp)).thenReturn(tinkerFile);
				smssUtils.when(() -> SmssUtilities.getUniqueName(engineName, engineId))
						.thenReturn(engineName + "__" + engineId);
//				engineUtils.when(() -> EngineUtility.getSpecificEngineBaseFolder(engineId)).thenReturn(engineFolder);

				// testing open
				TinkerEngine te = new TinkerEngine();
				te.open(smssProp);

				// validations
				// empty graph
				Graph graph = te.getGraph();
				Long count = graph.traversal().V().count().next();
				assertEquals(0, count);

				assertTrue(te.getTypeMap().isEmpty());
				assertTrue(te.getNameMap().isEmpty());
				te.close();

				owlFile.delete();
				tinkerFile.delete();
			}
		}
	}

	@Test
	public void testOpenUseLabel(@TempDir File tempDir) throws Exception {
		// creating tinker smss prop file
		String smssFilePath = "smssfile__ID.smss";
		Properties smssProp = new Properties();
		String filePath = "tinker.properties";
		String engineId = "engineId";
		String engineName = "tinkerTest";
		String owlFileStr = "tinkerTest.owl";
		String tinkerDriver = TINKER_DRIVER.JSON.toString();
		String tinkerFilePath = "tinkerTest.json";
		smssProp.setProperty(Constants.ENGINE, engineId);
		smssProp.setProperty(Constants.ENGINE_ALIAS, engineName);
		smssProp.setProperty(Constants.TINKER_USE_LABEL, "true");
		smssProp.setProperty(Constants.OWL, owlFileStr);
		smssProp.setProperty(Constants.TINKER_DRIVER, tinkerDriver);

		try (MockedStatic<SmssUtilities> smssUtils = Mockito.mockStatic(SmssUtilities.class);
				MockedStatic<UploadUtilities> uploadUtils = Mockito.mockStatic(UploadUtilities.class)) {
			// static test setup
			File owlFile = new File(tempDir, engineName + ".OWL");
			File tinkerFile = new File(tempDir, tinkerFilePath);
			uploadUtils.when(() -> UploadUtilities.generateOwlFile(IEngine.CATALOG_TYPE.DATABASE, engineId, engineName)
					.getAbsolutePath()).thenReturn(owlFile);
			smssUtils.when(() -> SmssUtilities.getOwlFile(smssFilePath, smssProp)).thenReturn(owlFile);
			smssUtils.when(() -> SmssUtilities.getTinkerFile(smssProp)).thenReturn(tinkerFile);
			smssUtils.when(() -> SmssUtilities.getUniqueName(engineName, engineId))
					.thenReturn(engineName + "__" + engineId);

			// testing open
			TinkerEngine te = new TinkerEngine();
			te.open(smssProp);

			// validations
			// empty graph
			Graph graph = te.getGraph();
			Long count = graph.traversal().V().count().next();
			assertEquals(0, count);

			assertTrue(te.getTypeMap().isEmpty());
			assertTrue(te.getNameMap().isEmpty());

			te.close();
		}
	}

	@Test
	public void testOpenBadTypeMaps(@TempDir File tempDir) throws Exception {
		// creating tinker smss prop file
		String smssFilePath = "smssfile__ID.smss";
		Properties smssProp = new Properties();
		String engineId = "engineId";
		String engineName = "tinkerTest";
		String owlFileStr = "tinkerTest.owl";
		String typeMapStr = "x";
		String nameMapStr = "x";
		String tinkerDriver = TINKER_DRIVER.JSON.toString();

		String tinkerFilePath = "tinkerTest.json";
		smssProp.setProperty(Constants.ENGINE, engineId);
		smssProp.setProperty(Constants.ENGINE_ALIAS, engineName);
		smssProp.setProperty(Constants.OWL, owlFileStr);
		smssProp.setProperty("TYPE_MAP", typeMapStr);
		smssProp.setProperty("NAME_MAP", nameMapStr);
		smssProp.setProperty(Constants.TINKER_DRIVER, tinkerDriver);

		try (MockedStatic<SmssUtilities> smssUtils = Mockito.mockStatic(SmssUtilities.class);
				MockedStatic<UploadUtilities> uploadUtils = Mockito.mockStatic(UploadUtilities.class)) {
			// static test setup
			File owlFile = new File(tempDir, engineName + ".OWL");
			File tinkerFile = new File(tempDir, tinkerFilePath);
			uploadUtils.when(() -> UploadUtilities.generateOwlFile(IEngine.CATALOG_TYPE.DATABASE, engineId, engineName)
					.getAbsolutePath()).thenReturn(owlFile);
			smssUtils.when(() -> SmssUtilities.getOwlFile(smssFilePath, smssProp)).thenReturn(owlFile);
			smssUtils.when(() -> SmssUtilities.getTinkerFile(smssProp)).thenReturn(tinkerFile);
			// testing open
			TinkerEngine te = new TinkerEngine();
			te.open(smssProp);

			// validations
			// empty graph
			Graph graph = te.getGraph();
			Long count = graph.traversal().V().count().next();
			assertEquals(0, count);

			assertTrue(te.getTypeMap().isEmpty());
			assertTrue(te.getNameMap().isEmpty());
			te.close();
		}
	}

	@Test
	public void testUpsertVertex(@TempDir File tempDir) throws Exception {
		// creating tinker smss prop file
		String smssFilePath = "smssfile__ID.smss";
		Properties smssProp = new Properties();
		String engineId = "engineId";
		String engineName = "tinkerTest";
		String owlFileStr = "tinkerTest.owl";
		String typeMapStr = " ";
		String nameMapStr = " ";
		String tinkerDriver = TINKER_DRIVER.JSON.toString();
		String tinkerFilePath = "tinkerTest.json";
		smssProp.setProperty(Constants.ENGINE, engineId);
		smssProp.setProperty(Constants.ENGINE_ALIAS, engineName);
		smssProp.setProperty(Constants.OWL, owlFileStr);
		smssProp.setProperty("TYPE_MAP", typeMapStr);
		smssProp.setProperty("NAME_MAP", nameMapStr);
		smssProp.setProperty(Constants.TINKER_DRIVER, tinkerDriver);

		try (MockedStatic<SmssUtilities> smssUtils = Mockito.mockStatic(SmssUtilities.class);
				MockedStatic<UploadUtilities> uploadUtils = Mockito.mockStatic(UploadUtilities.class)) {
			// static test setup
			File owlFile = new File(tempDir, engineName + ".OWL");
			File tinkerFile = new File(tempDir, tinkerFilePath);
			uploadUtils.when(() -> UploadUtilities.generateOwlFile(IEngine.CATALOG_TYPE.DATABASE, engineId, engineName)
					.getAbsolutePath()).thenReturn(owlFile);
			smssUtils.when(() -> SmssUtilities.getOwlFile(smssFilePath, smssProp)).thenReturn(owlFile);
			smssUtils.when(() -> SmssUtilities.getTinkerFile(smssProp)).thenReturn(tinkerFile);
			TinkerEngine te = new TinkerEngine();
			te.open(smssProp);
			Graph graph = te.getGraph();
			Long count = graph.traversal().V().count().next();
			assertEquals(0, count);

			// testing upsertVertex
			String nodeType = "person";
			String instanceName = "alice";
			Vertex vert = (Vertex) te.doAction(IDatabaseEngine.ACTION_TYPE.VERTEX_UPSERT,
					new Object[] { nodeType, instanceName });
			vert.property("age", 22);
			vert.property("last-name", "wonder");

			// validations
			// empty graph
			count = graph.traversal().V().count().next();
			assertEquals(1, count);

			Map<String, String> typeMap = te.getTypeMap();
			assertEquals(1, typeMap.keySet().size());
			assertTrue(typeMap.keySet().contains("person"));
			Map<String, String> nameMap = te.getNameMap();
			assertEquals(1, nameMap.keySet().size());
			assertTrue(nameMap.keySet().contains("person"));

			te.close();
		}
	}

	@Test
	public void testUpsertEdge(@TempDir File tempDir) throws Exception {
		// creating tinker smss prop file
		Properties smssProp = new Properties();
		String engineId = "engineId";
		String engineName = "tinkerTest";
		String smssFilePath = engineName + "__" + engineId + ".smss";
		String owlFileStr = "tinkerTest.owl";
		String typeMapStr = " ";
		String nameMapStr = " ";
		String tinkerDriver = TINKER_DRIVER.JSON.toString();
		String tinkerFilePath = "tinkerTest.json";
		smssProp.setProperty(Constants.ENGINE, engineId);
		smssProp.setProperty(Constants.ENGINE_ALIAS, engineName);
		smssProp.setProperty(Constants.OWL, owlFileStr);
		smssProp.setProperty("TYPE_MAP", typeMapStr);
		smssProp.setProperty("NAME_MAP", nameMapStr);
		smssProp.setProperty(Constants.TINKER_DRIVER, tinkerDriver);
		try (MockedStatic<SmssUtilities> smssUtils = Mockito.mockStatic(SmssUtilities.class);
				MockedStatic<UploadUtilities> uploadUtils = Mockito.mockStatic(UploadUtilities.class)) {
			// static test setup
			File owlFile = new File(tempDir, engineName + ".OWL");
			File tinkerFile = new File(tempDir, tinkerFilePath);
			uploadUtils.when(() -> UploadUtilities.generateOwlFile(IEngine.CATALOG_TYPE.DATABASE, engineId, engineName)
					.getAbsolutePath()).thenReturn(owlFile);
			smssUtils.when(() -> SmssUtilities.getOwlFile(smssFilePath, smssProp)).thenReturn(owlFile);
			smssUtils.when(() -> SmssUtilities.getTinkerFile(smssProp)).thenReturn(tinkerFile);
			smssUtils.when(() -> SmssUtilities.getUniqueName(engineName, engineId))
					.thenReturn(engineName + "__" + engineId);

			TinkerEngine te = new TinkerEngine();
			te.open(smssProp);
			Graph graph = te.getGraph();
			Long count = graph.traversal().V().count().next();
			assertEquals(0, count);

			// testing upsertVertex
			String nodeType = "person";
			String instanceName = "alice";
			Vertex vert = (Vertex) te.doAction(IDatabaseEngine.ACTION_TYPE.VERTEX_UPSERT,
					new Object[] { nodeType, instanceName });
			vert.property("age", 22);
			vert.property("last-name", "wonder");

			String nodeType2 = "animal";
			String instanceName2 = "dog";
			Vertex vert2 = (Vertex) te.doAction(IDatabaseEngine.ACTION_TYPE.VERTEX_UPSERT,
					new Object[] { nodeType2, instanceName2 });
			vert2.property("age", 3);

			Hashtable<String, Object> propHash = new Hashtable<>();
			propHash.put("edgeProp", "propEdge");

			// testing upsertEdge
			te.doAction(IDatabaseEngine.ACTION_TYPE.EDGE_UPSERT,
					new Object[] { vert, nodeType, vert, nodeType2, propHash });

			// validations
			count = graph.traversal().V().count().next();
			assertEquals(2, count);

			count = graph.traversal().E().count().next();
			assertEquals(1, count);

			Map<String, String> typeMap = te.getTypeMap();
			assertEquals(2, typeMap.keySet().size());
			assertTrue(typeMap.keySet().contains("person"));
			assertTrue(typeMap.keySet().contains("animal"));
			Map<String, String> nameMap = te.getNameMap();
			assertEquals(2, nameMap.keySet().size());
			assertTrue(nameMap.keySet().contains("person"));
			assertTrue(nameMap.keySet().contains("animal"));

			// TODO validate edge prop

			te.close();
		}
	}

	@Test
	public void testGetTypeMap(@TempDir File tempDir) throws Exception {
		// creating tinker smss prop file
		String engineId = "engineId";
		String engineName = "tinkerTest";
		String smssFilePath = engineName + "__" + engineId + ".smss";
		String engineFolder = tempDir + "\\" + engineName;
		Properties smssProp = new Properties();

		String owlFileStr = "tinkerTest.owl";
		String typeMapStr = "{\"person\":\"_T_TYPE\", \"animal\":\"_T_TYPE\"}";
		String nameMapStr = "";
		String tinkerDriver = TINKER_DRIVER.JSON.toString();
		String tinkerFilePath = "tinkerTest.json";
		smssProp.setProperty(Constants.ENGINE, engineId);
		smssProp.setProperty(Constants.ENGINE_ALIAS, engineName);
		smssProp.setProperty(Constants.OWL, owlFileStr);
		smssProp.setProperty("TYPE_MAP", typeMapStr);
		smssProp.setProperty("NAME_MAP", nameMapStr);
		smssProp.setProperty(Constants.TINKER_DRIVER, tinkerDriver);

		try (MockedStatic<SmssUtilities> smssUtils = Mockito.mockStatic(SmssUtilities.class);
				MockedStatic<EngineUtility> engineUtils = Mockito.mockStatic(EngineUtility.class);
				MockedStatic<UploadUtilities> uploadUtils = Mockito.mockStatic(UploadUtilities.class)) {
			// static test setup
			File owlFile = new File(tempDir, engineName + ".OWL");
			File tinkerFile = new File(tempDir, tinkerFilePath);
			uploadUtils.when(() -> UploadUtilities.generateOwlFile(IEngine.CATALOG_TYPE.DATABASE, engineId, engineName)
					.getAbsolutePath()).thenReturn(owlFile);
			smssUtils.when(() -> SmssUtilities.getOwlFile(smssFilePath, smssProp)).thenReturn(owlFile);
			smssUtils.when(() -> SmssUtilities.getTinkerFile(smssProp)).thenReturn(tinkerFile);
			engineUtils.when(() -> EngineUtility.getSpecificEngineBaseFolder(engineId)).thenReturn(engineFolder);

			TinkerEngine te = new TinkerEngine();
			te.open(smssProp);
			Graph graph = te.getGraph();
			Long count = graph.traversal().V().count().next();
			assertEquals(0, count);

			// testing getTypeMap
			Map<String, String> typeMap = te.getTypeMap();
			assertEquals(2, typeMap.keySet().size());
			assertTrue(typeMap.keySet().contains("person"));
			assertTrue(typeMap.keySet().contains("animal"));

			te.close();
		}
	}

	@Test
	public void testGetNameMap(@TempDir File tempDir) throws Exception {
		// creating tinker smss prop file
		String smssFilePath = "smssfile__ID.smss";
		Properties smssProp = new Properties();
		String filePath = "tinker.properties";
		String engineId = "engineId";
		String engineName = "tinkerTest";
		String owlFileStr = "tinkerTest.owl";
		String typeMapStr = "";
		String nameMapStr = "{\"person\":\"_T_NAME\", \"animal\":\"_T_NAME\"}";
		String tinkerDriver = TINKER_DRIVER.JSON.toString();
		String tinkerFilePath = "tinkerTest.json";
		smssProp.setProperty(Constants.ENGINE, engineId);
		smssProp.setProperty(Constants.ENGINE_ALIAS, engineName);
		smssProp.setProperty(Constants.OWL, owlFileStr);
		smssProp.setProperty("TYPE_MAP", typeMapStr);
		smssProp.setProperty("NAME_MAP", nameMapStr);
		smssProp.setProperty(Constants.TINKER_DRIVER, tinkerDriver);

		try (MockedStatic<SmssUtilities> smssUtils = Mockito.mockStatic(SmssUtilities.class);
				MockedStatic<UploadUtilities> uploadUtils = Mockito.mockStatic(UploadUtilities.class)) {
			// static test setup
			File owlFile = new File(tempDir, engineName + ".OWL");
			File tinkerFile = new File(tempDir, tinkerFilePath);
			uploadUtils.when(() -> UploadUtilities.generateOwlFile(IEngine.CATALOG_TYPE.DATABASE, engineId, engineName)
					.getAbsolutePath()).thenReturn(owlFile);
			smssUtils.when(() -> SmssUtilities.getOwlFile(smssFilePath, smssProp)).thenReturn(owlFile);
			smssUtils.when(() -> SmssUtilities.getTinkerFile(smssProp)).thenReturn(tinkerFile);
			TinkerEngine te = new TinkerEngine();
			te.open(smssProp);
			Graph graph = te.getGraph();
			Long count = graph.traversal().V().count().next();
			assertEquals(0, count);

			// testing getNameMap
			Map<String, String> typeMap = te.getNameMap();
			assertEquals(2, typeMap.keySet().size());
			assertTrue(typeMap.keySet().contains("person"));
			assertTrue(typeMap.keySet().contains("animal"));

			te.close();
		}
	}

	@Test
	public void testGetDatabaseType() {
		assertEquals(IDatabaseEngine.DATABASE_TYPE.TINKER, new TinkerEngine().getDatabaseType());
	}

	@Test
	public void testGetQueryInterpreter(@TempDir File tempDir) throws Exception {
		// creating tinker smss prop file
		String smssFilePath = "smssfile__ID.smss";
		Properties smssProp = new Properties();
		String filePath = "tinker.properties";
		String engineId = "engineId";
		String engineName = "tinkerTest";
		String owlFileStr = "tinkerTest.owl";
		String tinkerDriver = TINKER_DRIVER.JSON.toString();

		String tinkerFilePath = "tinkerTest.json";
		smssProp.setProperty(Constants.ENGINE, engineId);
		smssProp.setProperty(Constants.ENGINE_ALIAS, engineName);
		smssProp.setProperty(Constants.OWL, owlFileStr);
		smssProp.setProperty(Constants.TINKER_DRIVER, tinkerDriver);

		try (MockedStatic<SmssUtilities> smssUtils = Mockito.mockStatic(SmssUtilities.class);
				MockedStatic<UploadUtilities> uploadUtils = Mockito.mockStatic(UploadUtilities.class)) {
			// static test setup
			File owlFile = new File(tempDir, engineName + ".OWL");
			File tinkerFile = new File(tempDir, tinkerFilePath);
			Mockito.when(tinkerFile.getAbsolutePath()).thenReturn(tinkerFilePath);

			uploadUtils.when(() -> UploadUtilities.generateOwlFile(IEngine.CATALOG_TYPE.DATABASE, engineId, engineName)
					.getAbsolutePath()).thenReturn(owlFile);
			smssUtils.when(() -> SmssUtilities.getOwlFile(smssFilePath, smssProp)).thenReturn(owlFile);
			smssUtils.when(() -> SmssUtilities.getTinkerFile(Mockito.any())).thenReturn(tinkerFile);
			// testing open
			TinkerEngine te = new TinkerEngine();
			te.open(smssProp);

			// validations
			// empty graph
			Graph graph = te.getGraph();
			Long count = graph.traversal().V().count().next();
			assertEquals(0, count);

			// validate interp
			GremlinNoEdgeBindInterpreter interp = (GremlinNoEdgeBindInterpreter) te.getQueryInterpreter();
			assertNotNull(interp);
			assertEquals(engineName, interp.getEngine().getEngineName());
			assertTrue(interp.getNameMap().isEmpty());
			te.close();
		}
	}

	@Test
	public void testCommitJSON(@TempDir File tempDir) throws Exception {
		// creating tinker smss prop file
		String smssFilePath = "smssfile__ID.smss";
		Properties smssProp = new Properties();
		String engineId = "engineId";
		String engineName = "tinkerTest";
		String owlFileStr = "tinkerTest.owl";
		String typeMapStr = "";
		String nameMapStr = "";
		String tinkerDriver = TINKER_DRIVER.JSON.toString();
		String tinkerFilePath = "tinkerTest.json";
		smssProp.setProperty(Constants.ENGINE, engineId);
		smssProp.setProperty(Constants.ENGINE_ALIAS, engineName);
		smssProp.setProperty(Constants.OWL, owlFileStr);
		smssProp.setProperty("TYPE_MAP", typeMapStr);
		smssProp.setProperty("NAME_MAP", nameMapStr);
		smssProp.setProperty(Constants.TINKER_DRIVER, tinkerDriver);

		try (MockedStatic<SmssUtilities> smssUtils = Mockito.mockStatic(SmssUtilities.class);
				MockedStatic<UploadUtilities> uploadUtils = Mockito.mockStatic(UploadUtilities.class)) {
			// static test setup
			File owlFile = new File(tempDir, engineName + ".OWL");
			File tinkerFile = new File(tempDir, tinkerFilePath);
			smssProp.setProperty(Constants.TINKER_FILE, tinkerFile.getAbsolutePath());
			uploadUtils.when(() -> UploadUtilities.generateOwlFile(IEngine.CATALOG_TYPE.DATABASE, engineId, engineName)
					.getAbsolutePath()).thenReturn(owlFile);
			smssUtils.when(() -> SmssUtilities.getOwlFile(smssFilePath, smssProp)).thenReturn(owlFile);
			smssUtils.when(() -> SmssUtilities.getTinkerFile(Mockito.any())).thenReturn(tinkerFile);
			TinkerEngine te = new TinkerEngine();
			te.open(smssProp);
			Graph graph = te.getGraph();
			Long count = graph.traversal().V().count().next();
			assertEquals(0, count);

			// add data
			String nodeType = "person";
			String instanceName = "alice";
			Vertex vert = (Vertex) te.doAction(IDatabaseEngine.ACTION_TYPE.VERTEX_UPSERT,
					new Object[] { nodeType, instanceName });
			vert.property("age", 22);
			vert.property("last-name", "wonder");
			count = graph.traversal().V().count().next();
			assertEquals(1, count);

			// check type maps
			Map<String, String> typeMap = te.getTypeMap();
			assertEquals(1, typeMap.keySet().size());
			assertTrue(typeMap.keySet().contains("person"));
			Map<String, String> nameMap = te.getNameMap();
			assertEquals(1, nameMap.keySet().size());
			assertTrue(nameMap.keySet().contains("person"));

			te.commit();
			te.close();

			// open graph
			te = new TinkerEngine();
			te.open(smssProp);
			graph = te.getGraph();
			count = graph.traversal().V().count().next();
			assertEquals(1, count);
			te.close();
		}
	}

	@Test
	public void testCommitTG(@TempDir File tempDir) throws Exception {
		// creating tinker smss prop file
		String smssFilePath = "smssfile__ID.smss";
		Properties smssProp = new Properties();
		String engineId = "engineId";
		String engineName = "tinkerTest";
		String owlFileStr = "tinkerTest.owl";
		String typeMapStr = "";
		String nameMapStr = "";
		String tinkerDriver = TINKER_DRIVER.TG.toString();
		String tinkerFilePath = "tinkerTest.tg";
		smssProp.setProperty(Constants.ENGINE, engineId);
		smssProp.setProperty(Constants.ENGINE_ALIAS, engineName);
		smssProp.setProperty(Constants.OWL, owlFileStr);
		smssProp.setProperty("TYPE_MAP", typeMapStr);
		smssProp.setProperty("NAME_MAP", nameMapStr);
		smssProp.setProperty(Constants.TINKER_DRIVER, tinkerDriver);

		try (MockedStatic<SmssUtilities> smssUtils = Mockito.mockStatic(SmssUtilities.class);
				MockedStatic<UploadUtilities> uploadUtils = Mockito.mockStatic(UploadUtilities.class)) {
			// static test setup
			File owlFile = new File(tempDir, engineName + ".OWL");
			File tinkerFile = new File(tempDir, tinkerFilePath);
			smssProp.setProperty(Constants.TINKER_FILE, tinkerFile.getAbsolutePath());
			uploadUtils.when(() -> UploadUtilities.generateOwlFile(IEngine.CATALOG_TYPE.DATABASE, engineId, engineName)
					.getAbsolutePath()).thenReturn(owlFile);
			smssUtils.when(() -> SmssUtilities.getOwlFile(smssFilePath, smssProp)).thenReturn(owlFile);
			smssUtils.when(() -> SmssUtilities.getTinkerFile(Mockito.any())).thenReturn(tinkerFile);
			TinkerEngine te = new TinkerEngine();
			te.open(smssProp);
			Graph graph = te.getGraph();
			Long count = graph.traversal().V().count().next();
			assertEquals(0, count);

			// add data
			String nodeType = "person";
			String instanceName = "alice";
			Vertex vert = (Vertex) te.doAction(IDatabaseEngine.ACTION_TYPE.VERTEX_UPSERT,
					new Object[] { nodeType, instanceName });
			vert.property("age", 22);
			vert.property("last-name", "wonder");
			count = graph.traversal().V().count().next();
			assertEquals(1, count);

			// check type maps
			Map<String, String> typeMap = te.getTypeMap();
			assertEquals(1, typeMap.keySet().size());
			assertTrue(typeMap.keySet().contains("person"));
			Map<String, String> nameMap = te.getNameMap();
			assertEquals(1, nameMap.keySet().size());
			assertTrue(nameMap.keySet().contains("person"));

			te.commit();
			te.close();

			// open graph
			te = new TinkerEngine();
			te.open(smssProp);
			graph = te.getGraph();
			count = graph.traversal().V().count().next();
			assertEquals(1, count);
			te.close();
		}
	}

	@Test
	public void testCommitXML(@TempDir File tempDir) throws Exception {
		// creating tinker smss prop file
		String smssFilePath = "smssfile__ID.smss";
		Properties smssProp = new Properties();
		String filePath = "tinker.properties";
		String engineId = "engineId";
		String engineName = "tinkerTest";
		String owlFileStr = "tinkerTest.owl";
		String typeMapStr = "";
		String nameMapStr = "";
		String tinkerDriver = TINKER_DRIVER.XML.toString();
		String tinkerFilePath = "tinkerTest.xml";
		smssProp.setProperty(Constants.ENGINE, engineId);
		smssProp.setProperty(Constants.ENGINE_ALIAS, engineName);
		smssProp.setProperty(Constants.OWL, owlFileStr);
		smssProp.setProperty("TYPE_MAP", typeMapStr);
		smssProp.setProperty("NAME_MAP", nameMapStr);
		smssProp.setProperty(Constants.TINKER_DRIVER, tinkerDriver);

		try (MockedStatic<SmssUtilities> smssUtils = Mockito.mockStatic(SmssUtilities.class);
				MockedStatic<UploadUtilities> uploadUtils = Mockito.mockStatic(UploadUtilities.class)) {
			// static test setup
			File owlFile = new File(tempDir, engineName + ".OWL");
			File tinkerFile = new File(tempDir, tinkerFilePath);
			smssProp.setProperty(Constants.TINKER_FILE, tinkerFile.getAbsolutePath());
			uploadUtils.when(() -> UploadUtilities.generateOwlFile(IEngine.CATALOG_TYPE.DATABASE, engineId, engineName)
					.getAbsolutePath()).thenReturn(owlFile);
			smssUtils.when(() -> SmssUtilities.getOwlFile(smssFilePath, smssProp)).thenReturn(owlFile);
			smssUtils.when(() -> SmssUtilities.getTinkerFile(Mockito.any())).thenReturn(tinkerFile);
			TinkerEngine te = new TinkerEngine();
			te.open(smssProp);
			Graph graph = te.getGraph();
			Long count = graph.traversal().V().count().next();
			assertEquals(0, count);

			// add data
			String nodeType = "person";
			String instanceName = "alice";
			Vertex vert = (Vertex) te.doAction(IDatabaseEngine.ACTION_TYPE.VERTEX_UPSERT,
					new Object[] { nodeType, instanceName });
			vert.property("age", 22);
			vert.property("last-name", "wonder");
			count = graph.traversal().V().count().next();
			assertEquals(1, count);

			// check type maps
			Map<String, String> typeMap = te.getTypeMap();
			assertEquals(1, typeMap.keySet().size());
			assertTrue(typeMap.keySet().contains("person"));
			Map<String, String> nameMap = te.getNameMap();
			assertEquals(1, nameMap.keySet().size());
			assertTrue(nameMap.keySet().contains("person"));

			te.commit();
			te.close();

			// open graph
			te = new TinkerEngine();
			te.open(smssProp);
			graph = te.getGraph();
			count = graph.traversal().V().count().next();
			assertEquals(1, count);
			te.close();
		}
	}

//	@Test
//	public void testCommitNeo4j(@TempDir File tempDir) throws Exception {
//		// creating tinker smss prop file
//		Properties smssProp = new Properties();
//		String engineId = "engineId";
//		String engineName = "tinkerTest";
//		String owlFileStr = "tinkerTest.owl";
//		String typeMapStr = "";
//		String nameMapStr = "";
//		String tinkerDriver = TINKER_DRIVER.NEO4J.toString();
//		String tinkerFilePath = "tinkerTestNeo4j";
//		smssProp.setProperty(Constants.ENGINE, engineId);
//		smssProp.setProperty(Constants.ENGINE_ALIAS, engineName);
//		smssProp.setProperty(Constants.OWL, owlFileStr);
//		smssProp.setProperty("TYPE_MAP", typeMapStr);
//		smssProp.setProperty("NAME_MAP", nameMapStr);
//		smssProp.setProperty(Constants.TINKER_DRIVER, tinkerDriver);
//
//		try (MockedStatic<SmssUtilities> smssUtils = Mockito.mockStatic(SmssUtilities.class);
//				MockedStatic<UploadUtilities> uploadUtils = Mockito.mockStatic(UploadUtilities.class)) {
//			// static test setup
//			File owlFile = new File(tempDir, engineName + ".OWL");
//			File tinkerFile = new File(tempDir, tinkerFilePath);
//			smssProp.setProperty(Constants.TINKER_FILE, tinkerFile.getAbsolutePath());
//			uploadUtils.when(() -> UploadUtilities.generateOwlFile(IEngine.CATALOG_TYPE.DATABASE, engineId, engineName)
//					.getAbsolutePath()).thenReturn(owlFile);
//			smssUtils.when(() -> SmssUtilities.getOwlFile(smssProp)).thenReturn(owlFile);
//			smssUtils.when(() -> SmssUtilities.getTinkerFile(Mockito.any())).thenReturn(tinkerFile);
//			TinkerEngine te = new TinkerEngine();
//			te.open(smssProp);
//			Graph graph = te.getGraph();
//			Long count = graph.traversal().V().count().next();
//			assertEquals(0, count);
//
//			// add data
//			String nodeType = "person";
//			String instanceName = "alice";
//			Vertex vert = (Vertex) te.doAction(IDatabaseEngine.ACTION_TYPE.VERTEX_UPSERT,
//					new Object[] { nodeType, instanceName });
//			vert.property("age", 22);
//			vert.property("last-name", "wonder");
//			count = graph.traversal().V().count().next();
//			assertEquals(1, count);
//
//			// check type maps
//			Map<String, String> typeMap = te.getTypeMap();
//			assertEquals(1, typeMap.keySet().size());
//			assertTrue(typeMap.keySet().contains("person"));
//			Map<String, String> nameMap = te.getNameMap();
//			assertEquals(1, nameMap.keySet().size());
//			assertTrue(nameMap.keySet().contains("person"));
//
//			te.commit();
//			te.close();
//
//			// open graph
//			te = new TinkerEngine();
//			te.open(smssProp);
//			graph = te.getGraph();
//			count = graph.traversal().V().count().next();
//			assertEquals(1, count);
//			te.close();
//		}
//	}
}
