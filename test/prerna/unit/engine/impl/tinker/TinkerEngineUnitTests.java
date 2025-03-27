package prerna.unit.engine.impl.tinker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.tinker.TinkerEngine;
import prerna.poi.main.helper.ImportOptions.TINKER_DRIVER;
import prerna.util.Constants;
import prerna.util.UploadUtilities;

public class TinkerEngineUnitTests {

//	private FileSystem fs;
//
//	@BeforeEach
//	void setup() {
//		fs = Jimfs.newFileSystem(Configuration.unix());
//	}

	///////////// Test Open
	@Test
	public void testOpenEmptyGraph() {
		// creating tinker smss prop file
		Properties smssProp = new Properties();
		String filePath = "tinker.properties";
		String engineId = "engineId";
		String engineName = "tinkerTest";
		String owlFileStr = "tinkerTest.owl";
		String typeMapStr = "";
		String nameMapStr = "";
		String tinkerDriver = TINKER_DRIVER.JSON.toString();

		String tinkerFilePath = "tinkerTest.json";
		try (FileOutputStream output = new FileOutputStream(filePath)) {
			smssProp.setProperty(Constants.ENGINE, engineId);
			smssProp.setProperty(Constants.ENGINE_ALIAS, engineName);
			smssProp.setProperty(Constants.OWL, owlFileStr);
			smssProp.setProperty("TYPE_MAP", typeMapStr);
			smssProp.setProperty("NAME_MAP", nameMapStr);
			smssProp.setProperty(Constants.TINKER_DRIVER, tinkerDriver);
//			smssProp.store(output, "tinker engine props");
		} catch (IOException io) {
			io.printStackTrace();
			fail();
		}

		try {
			try (MockedStatic<SmssUtilities> smssUtils = Mockito.mockStatic(SmssUtilities.class);
//					MockedStatic<FileSystems> fss = Mockito.mockStatic(FileSystems.class);
					MockedStatic<UploadUtilities> uploadUtils = Mockito.mockStatic(UploadUtilities.class)) {

//				fss.when(FileSystems::getDefault).thenReturn(fs);

				// static test setup
				File owlFile = new File(engineName + ".OWL");
				File tinkerFile = new File(tinkerFilePath);
				uploadUtils.when(() -> UploadUtilities
						.generateOwlFile(IEngine.CATALOG_TYPE.DATABASE, engineId, engineName).getAbsolutePath())
						.thenReturn(owlFile);
				smssUtils.when(() -> SmssUtilities.getOwlFile(smssProp)).thenReturn(owlFile);
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

//				GraphSONIo reader = Mockito.mock(GraphSONIo.class);
//				Mockito.verify(reader).readGraph(tinkerFile.getAbsolutePath());
			}

		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	
	@Test
	public void testUpsertVertex() {
		// creating tinker smss prop file
		Properties smssProp = new Properties();
		String filePath = "tinker.properties";
		String engineId = "engineId";
		String engineName = "tinkerTest";
		String owlFileStr = "tinkerTest.owl";
		String typeMapStr = "";
		String nameMapStr = "";
		String tinkerDriver = TINKER_DRIVER.JSON.toString();
		String tinkerFilePath = "tinkerTest.json";
		try (FileOutputStream output = new FileOutputStream(filePath)) {
			smssProp.setProperty(Constants.ENGINE, engineId);
			smssProp.setProperty(Constants.ENGINE_ALIAS, engineName);
			smssProp.setProperty(Constants.OWL, owlFileStr);
			smssProp.setProperty("TYPE_MAP", typeMapStr);
			smssProp.setProperty("NAME_MAP", nameMapStr);
			smssProp.setProperty(Constants.TINKER_DRIVER, tinkerDriver);
//			smssProp.store(output, "tinker engine props");
		} catch (IOException io) {
			io.printStackTrace();
			fail();
		}

		try {
			try (MockedStatic<SmssUtilities> smssUtils = Mockito.mockStatic(SmssUtilities.class);
//					MockedStatic<FileSystems> fss = Mockito.mockStatic(FileSystems.class);
					MockedStatic<UploadUtilities> uploadUtils = Mockito.mockStatic(UploadUtilities.class)) {

//				fss.when(FileSystems::getDefault).thenReturn(fs);

				// static test setup
				File owlFile = new File(engineName + ".OWL");
				File tinkerFile = new File(tinkerFilePath);
				uploadUtils.when(() -> UploadUtilities
						.generateOwlFile(IEngine.CATALOG_TYPE.DATABASE, engineId, engineName).getAbsolutePath())
						.thenReturn(owlFile);
				smssUtils.when(() -> SmssUtilities.getOwlFile(smssProp)).thenReturn(owlFile);
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

//				GraphSONIo reader = Mockito.mock(GraphSONIo.class);
//				Mockito.verify(reader).readGraph(tinkerFile.getAbsolutePath());
			}

		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	
	@Test
	public void testUpsertEdge() {
		// creating tinker smss prop file
		Properties smssProp = new Properties();
		String filePath = "tinker.properties";
		String engineId = "engineId";
		String engineName = "tinkerTest";
		String owlFileStr = "tinkerTest.owl";
		String typeMapStr = "";
		String nameMapStr = "";
		String tinkerDriver = TINKER_DRIVER.JSON.toString();
		String tinkerFilePath = "tinkerTest.json";
		try (FileOutputStream output = new FileOutputStream(filePath)) {
			smssProp.setProperty(Constants.ENGINE, engineId);
			smssProp.setProperty(Constants.ENGINE_ALIAS, engineName);
			smssProp.setProperty(Constants.OWL, owlFileStr);
			smssProp.setProperty("TYPE_MAP", typeMapStr);
			smssProp.setProperty("NAME_MAP", nameMapStr);
			smssProp.setProperty(Constants.TINKER_DRIVER, tinkerDriver);
//			smssProp.store(output, "tinker engine props");
		} catch (IOException io) {
			io.printStackTrace();
			fail();
		}

		try {
			try (MockedStatic<SmssUtilities> smssUtils = Mockito.mockStatic(SmssUtilities.class);
//					MockedStatic<FileSystems> fss = Mockito.mockStatic(FileSystems.class);
					MockedStatic<UploadUtilities> uploadUtils = Mockito.mockStatic(UploadUtilities.class)) {

//				fss.when(FileSystems::getDefault).thenReturn(fs);

				// static test setup
				File owlFile = new File(engineName + ".OWL");
				File tinkerFile = new File(tinkerFilePath);
				uploadUtils.when(() -> UploadUtilities
						.generateOwlFile(IEngine.CATALOG_TYPE.DATABASE, engineId, engineName).getAbsolutePath())
						.thenReturn(owlFile);
				smssUtils.when(() -> SmssUtilities.getOwlFile(smssProp)).thenReturn(owlFile);
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
				
				String nodeType2 = "animal";
				String instanceName2 = "dog";
				Vertex vert2 = (Vertex) te.doAction(IDatabaseEngine.ACTION_TYPE.VERTEX_UPSERT,
						new Object[] { nodeType2, instanceName2 });
				vert2.property("age", 3);
				
				Hashtable<String, Object> propHash = new Hashtable<>();
				// testing upsertEdge
				te.doAction(IDatabaseEngine.ACTION_TYPE.EDGE_UPSERT, new Object[] { vert, nodeType, vert, nodeType2, propHash });

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

				te.close();

//				GraphSONIo reader = Mockito.mock(GraphSONIo.class);
//				Mockito.verify(reader).readGraph(tinkerFile.getAbsolutePath());
			}

		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}


	@Test
	public void testGetDatabaseType() {
		assertEquals(IDatabaseEngine.DATABASE_TYPE.TINKER, new TinkerEngine().getDatabaseType());
	}
}
