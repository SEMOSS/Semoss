/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.engine.impl.tinker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.SemossUnitTest;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.tinker.TinkerEngine.TINKER_DRIVER;
import prerna.query.interpreters.GremlinNoEdgeBindInterpreter;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class TinkerEngineUnitTests extends SemossUnitTest {

	@BeforeEach
	void setup() throws IOException {
		FileUtils.cleanDirectory(tempDir.toFile());
	}

	///////////// Test Open
	@Test
	public void testOpenEmptyGraph() throws Exception {
		// make base folders for the db
		String fileSeparator = java.nio.file.FileSystems.getDefault().getSeparator();
		String baseFolderPath = "semoss";
		File baseFolder = new File(tempDir.toFile(), baseFolderPath);
		baseFolder.mkdir();
		File baseDBFolder = new File(tempDir.toFile(), baseFolderPath + fileSeparator + "db");
		baseDBFolder.mkdir();

		// testing setup
		String engineId = "engineId";
		String engineName = "tinkerTest";
		String smssFilePath = engineName + "__" + engineId + ".smss";
		File dbSMSS = new File(baseDBFolder, smssFilePath);

		// creating tinker smss prop file
		Properties smssProp = new Properties();
		String typeMapStr = "";
		String nameMapStr = "";
		String tinkerDriver = TINKER_DRIVER.JSON.toString();
		String tinkerFilePath = "tinkerTest.json";
		smssProp.setProperty(Constants.ENGINE, engineId);
		smssProp.setProperty(Constants.ENGINE_ALIAS, engineName);
		smssProp.setProperty("TYPE_MAP", typeMapStr);
		smssProp.setProperty("NAME_MAP", nameMapStr);
		smssProp.setProperty(Constants.TINKER_DRIVER, tinkerDriver);
		smssProp.setProperty(Constants.TINKER_FILE, "@BaseFolder@/db/@ENGINE@/"+tinkerFilePath);

		// save prop file
		try (FileOutputStream out = new FileOutputStream(dbSMSS)) {
			smssProp.store(out, "Properties");
		} catch (IOException e) {
			e.printStackTrace();
		}

		try (MockedStatic<DIHelper> mockedSingleton = Mockito.mockStatic(DIHelper.class)) {
			DIHelper instance = Mockito.mock(DIHelper.class);
			when(DIHelper.getInstance()).thenReturn(instance);
			when(instance.getProperty(Constants.BASE_FOLDER)).thenReturn(baseFolder.getAbsolutePath());

			// testing open
			TinkerEngine te = new TinkerEngine();
			te.open(dbSMSS.getAbsolutePath());

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
	public void testOpenUseLabel() throws Exception {
		// make base folders for the db
		String fileSeparator = java.nio.file.FileSystems.getDefault().getSeparator();
		String baseFolderPath = "semoss";
		File baseFolder = new File(tempDir.toFile(), baseFolderPath);
		baseFolder.mkdir();
		File baseDBFolder = new File(tempDir.toFile(), baseFolderPath + fileSeparator + "db");
		baseDBFolder.mkdir();

		// testing setup
		String engineId = "engineId";
		String engineName = "tinkerTest";
		String smssFilePath = engineName + "__" + engineId + ".smss";
		File dbSMSS = new File(baseDBFolder, smssFilePath);

		// creating tinker smss prop file
		Properties smssProp = new Properties();
		String typeMapStr = "";
		String nameMapStr = "";
		String tinkerDriver = TINKER_DRIVER.JSON.toString();
		String tinkerFilePath = "tinkerTest.json";
		smssProp.setProperty(Constants.ENGINE, engineId);
		smssProp.setProperty(Constants.ENGINE_ALIAS, engineName);
		smssProp.setProperty("TYPE_MAP", typeMapStr);
		smssProp.setProperty("NAME_MAP", nameMapStr);
		smssProp.setProperty(Constants.TINKER_DRIVER, tinkerDriver);
		smssProp.setProperty(Constants.TINKER_FILE, "@BaseFolder@/db/@ENGINE@/"+tinkerFilePath);

		// save prop file
		try (FileOutputStream out = new FileOutputStream(dbSMSS)) {
			smssProp.store(out, "Properties");
		} catch (IOException e) {
			e.printStackTrace();
		}

		try (MockedStatic<DIHelper> mockedSingleton = Mockito.mockStatic(DIHelper.class)) {
			DIHelper instance = Mockito.mock(DIHelper.class);
			when(DIHelper.getInstance()).thenReturn(instance);
			when(instance.getProperty(Constants.BASE_FOLDER)).thenReturn(baseFolder.getAbsolutePath());


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
	public void testOpenBadTypeMaps() throws Exception {
		// make base folders for the db
		String fileSeparator = java.nio.file.FileSystems.getDefault().getSeparator();
		String baseFolderPath = "semoss";
		File baseFolder = new File(tempDir.toFile(), baseFolderPath);
		baseFolder.mkdir();
		File baseDBFolder = new File(tempDir.toFile(), baseFolderPath + fileSeparator + "db");
		baseDBFolder.mkdir();

		// testing setup
		String engineId = "engineId";
		String engineName = "tinkerTest";
		String smssFilePath = engineName + "__" + engineId + ".smss";
		File dbSMSS = new File(baseDBFolder, smssFilePath);

		// creating tinker smss prop file
		Properties smssProp = new Properties();
		String typeMapStr = "";
		String nameMapStr = "";
		String tinkerDriver = TINKER_DRIVER.JSON.toString();
		String tinkerFilePath = "tinkerTest.json";
		smssProp.setProperty(Constants.ENGINE, engineId);
		smssProp.setProperty(Constants.ENGINE_ALIAS, engineName);
		smssProp.setProperty("TYPE_MAP", typeMapStr);
		smssProp.setProperty("NAME_MAP", nameMapStr);
		smssProp.setProperty(Constants.TINKER_DRIVER, tinkerDriver);
		smssProp.setProperty(Constants.TINKER_FILE, "@BaseFolder@/db/@ENGINE@/"+tinkerFilePath);

		// save prop file
		try (FileOutputStream out = new FileOutputStream(dbSMSS)) {
			smssProp.store(out, "Properties");
		} catch (IOException e) {
			e.printStackTrace();
		}

		try (MockedStatic<DIHelper> mockedSingleton = Mockito.mockStatic(DIHelper.class)) {
			DIHelper instance = Mockito.mock(DIHelper.class);
			when(DIHelper.getInstance()).thenReturn(instance);
			when(instance.getProperty(Constants.BASE_FOLDER)).thenReturn(baseFolder.getAbsolutePath());

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
	public void testUpsertVertex() throws Exception {
		// make base folders for the db
		String fileSeparator = java.nio.file.FileSystems.getDefault().getSeparator();
		String baseFolderPath = "semoss";
		File baseFolder = new File(tempDir.toFile(), baseFolderPath);
		baseFolder.mkdir();
		File baseDBFolder = new File(tempDir.toFile(), baseFolderPath + fileSeparator + "db");
		baseDBFolder.mkdir();

		// testing setup
		String engineId = "engineId";
		String engineName = "tinkerTest";
		String smssFilePath = engineName + "__" + engineId + ".smss";
		File dbSMSS = new File(baseDBFolder, smssFilePath);

		// creating tinker smss prop file
		Properties smssProp = new Properties();
		String typeMapStr = "";
		String nameMapStr = "";
		String tinkerDriver = TINKER_DRIVER.JSON.toString();
		String tinkerFilePath = "tinkerTest.json";
		smssProp.setProperty(Constants.ENGINE, engineId);
		smssProp.setProperty(Constants.ENGINE_ALIAS, engineName);
		smssProp.setProperty("TYPE_MAP", typeMapStr);
		smssProp.setProperty("NAME_MAP", nameMapStr);
		smssProp.setProperty(Constants.TINKER_DRIVER, tinkerDriver);
		smssProp.setProperty(Constants.TINKER_FILE, "@BaseFolder@/db/@ENGINE@/"+tinkerFilePath);

		// save prop file
		try (FileOutputStream out = new FileOutputStream(dbSMSS)) {
			smssProp.store(out, "Properties");
		} catch (IOException e) {
			e.printStackTrace();
		}

		try (MockedStatic<DIHelper> mockedSingleton = Mockito.mockStatic(DIHelper.class)) {
			DIHelper instance = Mockito.mock(DIHelper.class);
			when(DIHelper.getInstance()).thenReturn(instance);
			when(instance.getProperty(Constants.BASE_FOLDER)).thenReturn(baseFolder.getAbsolutePath());

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
	public void testUpsertEdge() throws Exception {
		// make base folders for the db
		String fileSeparator = java.nio.file.FileSystems.getDefault().getSeparator();
		String baseFolderPath = "semoss";
		File baseFolder = new File(tempDir.toFile(), baseFolderPath);
		baseFolder.mkdir();
		File baseDBFolder = new File(tempDir.toFile(), baseFolderPath + fileSeparator + "db");
		baseDBFolder.mkdir();

		// testing setup
		String engineId = "engineId";
		String engineName = "tinkerTest";
		String smssFilePath = engineName + "__" + engineId + ".smss";
		File dbSMSS = new File(baseDBFolder, smssFilePath);

		// creating tinker smss prop file
		Properties smssProp = new Properties();
		String typeMapStr = "";
		String nameMapStr = "";
		String tinkerDriver = TINKER_DRIVER.JSON.toString();
		String tinkerFilePath = "tinkerTest.json";
		smssProp.setProperty(Constants.ENGINE, engineId);
		smssProp.setProperty(Constants.ENGINE_ALIAS, engineName);
		smssProp.setProperty("TYPE_MAP", typeMapStr);
		smssProp.setProperty("NAME_MAP", nameMapStr);
		smssProp.setProperty(Constants.TINKER_DRIVER, tinkerDriver);
		smssProp.setProperty(Constants.TINKER_FILE, "@BaseFolder@/db/@ENGINE@/"+tinkerFilePath);

		// save prop file
		try (FileOutputStream out = new FileOutputStream(dbSMSS)) {
			smssProp.store(out, "Properties");
		} catch (IOException e) {
			e.printStackTrace();
		}

		try (MockedStatic<DIHelper> mockedSingleton = Mockito.mockStatic(DIHelper.class)) {
			DIHelper instance = Mockito.mock(DIHelper.class);
			when(DIHelper.getInstance()).thenReturn(instance);
			when(instance.getProperty(Constants.BASE_FOLDER)).thenReturn(baseFolder.getAbsolutePath());

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
	public void testGetTypeMap() throws Exception {
		// make base folders for the db
		String fileSeparator = java.nio.file.FileSystems.getDefault().getSeparator();
		String baseFolderPath = "semoss";
		File baseFolder = new File(tempDir.toFile(), baseFolderPath);
		baseFolder.mkdir();
		File baseDBFolder = new File(tempDir.toFile(), baseFolderPath + fileSeparator + "db");
		baseDBFolder.mkdir();

		// testing setup
		String engineId = "engineId";
		String engineName = "tinkerTest";
		String smssFilePath = engineName + "__" + engineId + ".smss";
		File dbSMSS = new File(baseDBFolder, smssFilePath);

		// creating tinker smss prop file
		Properties smssProp = new Properties();
		String typeMapStr = "{\"person\":\"_T_TYPE\", \"animal\":\"_T_TYPE\"}";
		String nameMapStr = "";
		String tinkerDriver = TINKER_DRIVER.JSON.toString();
		String tinkerFilePath = "tinkerTest.json";
		smssProp.setProperty(Constants.ENGINE, engineId);
		smssProp.setProperty(Constants.ENGINE_ALIAS, engineName);
		smssProp.setProperty("TYPE_MAP", typeMapStr);
		smssProp.setProperty("NAME_MAP", nameMapStr);
		smssProp.setProperty(Constants.TINKER_DRIVER, tinkerDriver);
		smssProp.setProperty(Constants.TINKER_FILE, "@BaseFolder@/db/@ENGINE@/"+tinkerFilePath);

		// save prop file
		try (FileOutputStream out = new FileOutputStream(dbSMSS)) {
			smssProp.store(out, "Properties");
		} catch (IOException e) {
			e.printStackTrace();
		}

		try (MockedStatic<DIHelper> mockedSingleton = Mockito.mockStatic(DIHelper.class)) {
			DIHelper instance = Mockito.mock(DIHelper.class);
			when(DIHelper.getInstance()).thenReturn(instance);
			when(instance.getProperty(Constants.BASE_FOLDER)).thenReturn(baseFolder.getAbsolutePath());

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
	public void testGetNameMap() throws Exception {
		// make base folders for the db
		String fileSeparator = java.nio.file.FileSystems.getDefault().getSeparator();
		String baseFolderPath = "semoss";
		File baseFolder = new File(tempDir.toFile(), baseFolderPath);
		baseFolder.mkdir();
		File baseDBFolder = new File(tempDir.toFile(), baseFolderPath + fileSeparator + "db");
		baseDBFolder.mkdir();

		// testing setup
		String engineId = "engineId";
		String engineName = "tinkerTest";
		String smssFilePath = engineName + "__" + engineId + ".smss";
		File dbSMSS = new File(baseDBFolder, smssFilePath);

		// creating tinker smss prop file
		Properties smssProp = new Properties();
		String typeMapStr = "";
		String nameMapStr = "{\"person\":\"_T_NAME\", \"animal\":\"_T_NAME\"}";
		String tinkerDriver = TINKER_DRIVER.JSON.toString();
		String tinkerFilePath = "tinkerTest.json";
		smssProp.setProperty(Constants.ENGINE, engineId);
		smssProp.setProperty(Constants.ENGINE_ALIAS, engineName);
		smssProp.setProperty("TYPE_MAP", typeMapStr);
		smssProp.setProperty("NAME_MAP", nameMapStr);
		smssProp.setProperty(Constants.TINKER_DRIVER, tinkerDriver);
		smssProp.setProperty(Constants.TINKER_FILE, "@BaseFolder@/db/@ENGINE@/"+tinkerFilePath);

		// save prop file
		try (FileOutputStream out = new FileOutputStream(dbSMSS)) {
			smssProp.store(out, "Properties");
		} catch (IOException e) {
			e.printStackTrace();
		}

		try (MockedStatic<DIHelper> mockedSingleton = Mockito.mockStatic(DIHelper.class)) {
			DIHelper instance = Mockito.mock(DIHelper.class);
			when(DIHelper.getInstance()).thenReturn(instance);
			when(instance.getProperty(Constants.BASE_FOLDER)).thenReturn(baseFolder.getAbsolutePath());

			TinkerEngine te = new TinkerEngine();
			te.open(smssProp);
			Graph graph = te.getGraph();
			Long count = graph.traversal().V().count().next();
			assertEquals(0, count);

			// testing getNameMap
			Map<String, String> nameMap = te.getNameMap();
			assertEquals(2, nameMap.keySet().size());
			assertTrue(nameMap.keySet().contains("person"));
			assertTrue(nameMap.keySet().contains("animal"));

			te.close();
		}
	}

	@Test
	public void testGetDatabaseType() throws IOException {
		TinkerEngine te = new TinkerEngine();
		assertEquals(IDatabaseEngine.DATABASE_TYPE.TINKER, te.getDatabaseType());
		try {
			te.close();
		} catch (Exception e) {

		}
	}

	@Test
	public void testGetQueryInterpreter() throws Exception {
		// make base folders for the db
		String fileSeparator = java.nio.file.FileSystems.getDefault().getSeparator();
		String baseFolderPath = "semoss";
		File baseFolder = new File(tempDir.toFile(), baseFolderPath);
		baseFolder.mkdir();
		File baseDBFolder = new File(tempDir.toFile(), baseFolderPath + fileSeparator + "db");
		baseDBFolder.mkdir();

		// testing setup
		String engineId = "engineId";
		String engineName = "tinkerTest";
		String smssFilePath = engineName + "__" + engineId + ".smss";
		File dbSMSS = new File(baseDBFolder, smssFilePath);

		// creating tinker smss prop file
		Properties smssProp = new Properties();
		String typeMapStr = "";
		String nameMapStr = "";
		String tinkerDriver = TINKER_DRIVER.JSON.toString();
		String tinkerFilePath = "tinkerTest.json";
		smssProp.setProperty(Constants.ENGINE, engineId);
		smssProp.setProperty(Constants.ENGINE_ALIAS, engineName);
		smssProp.setProperty("TYPE_MAP", typeMapStr);
		smssProp.setProperty("NAME_MAP", nameMapStr);
		smssProp.setProperty(Constants.TINKER_DRIVER, tinkerDriver);
		smssProp.setProperty(Constants.TINKER_FILE, "@BaseFolder@/db/@ENGINE@/"+tinkerFilePath);

		// save prop file
		try (FileOutputStream out = new FileOutputStream(dbSMSS)) {
			smssProp.store(out, "Properties");
		} catch (IOException e) {
			e.printStackTrace();
		}

		try (MockedStatic<DIHelper> mockedSingleton = Mockito.mockStatic(DIHelper.class)) {
			DIHelper instance = Mockito.mock(DIHelper.class);
			when(DIHelper.getInstance()).thenReturn(instance);
			when(instance.getProperty(Constants.BASE_FOLDER)).thenReturn(baseFolder.getAbsolutePath());

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
	public void testCommitJSON() throws Exception {
		// make base folders for the db
		String fileSeparator = java.nio.file.FileSystems.getDefault().getSeparator();
		String baseFolderPath = "semoss";
		File baseFolder = new File(tempDir.toFile(), baseFolderPath);
		baseFolder.mkdir();
		File baseDBFolder = new File(tempDir.toFile(), baseFolderPath + fileSeparator + "db");
		baseDBFolder.mkdir();

		
		// testing setup
		String engineId = "engineId";
		String engineName = "tinkerTest";
		String smssFilePath = engineName + "__" + engineId + ".smss";
		File dbSMSS = new File(baseDBFolder, smssFilePath);
		File tinkerDBFolder = new File(baseDBFolder, engineName + "__" + engineId);
		tinkerDBFolder.mkdir();
		
		// make empty tinker file for commit to work
		String tinkerFilePath = "tinkerTest.json";

		// creating tinker smss prop file
		Properties smssProp = new Properties();
		String typeMapStr = "";
		String nameMapStr = "";
		String tinkerDriver = TINKER_DRIVER.JSON.toString();
		smssProp.setProperty(Constants.ENGINE, engineId);
		smssProp.setProperty(Constants.ENGINE_ALIAS, engineName);
		smssProp.setProperty("TYPE_MAP", typeMapStr);
		smssProp.setProperty("NAME_MAP", nameMapStr);
		smssProp.setProperty(Constants.TINKER_DRIVER, tinkerDriver);
		smssProp.setProperty(Constants.TINKER_FILE, "@BaseFolder@/db/@ENGINE@/"+tinkerFilePath);

		// save prop file
		try (FileOutputStream out = new FileOutputStream(dbSMSS)) {
			smssProp.store(out, "Properties");
		} catch (IOException e) {
			e.printStackTrace();
		}

		try (MockedStatic<DIHelper> mockedSingleton = Mockito.mockStatic(DIHelper.class)) {
			DIHelper instance = Mockito.mock(DIHelper.class);
			when(DIHelper.getInstance()).thenReturn(instance);
			when(instance.getProperty(Constants.BASE_FOLDER)).thenReturn(baseFolder.getAbsolutePath());
			
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

	//@Test
	public void testCommitTG() throws Exception {
		// make base folders for the db
		String fileSeparator = java.nio.file.FileSystems.getDefault().getSeparator();
		String baseFolderPath = "semoss";
		File baseFolder = new File(tempDir.toFile(), baseFolderPath);
		baseFolder.mkdir();
		File baseDBFolder = new File(tempDir.toFile(), baseFolderPath + fileSeparator + "db");
		baseDBFolder.mkdir();

		// testing setup
		String engineId = "engineId";
		String engineName = "tinkerTest";
		String smssFilePath = engineName + "__" + engineId + ".smss";
		File dbSMSS = new File(baseDBFolder, smssFilePath);
		
		// make empty tinker file for commit to work
		String tinkerFilePath = "tinkerTest.tg";
		File tinkerDBFolder = new File(baseDBFolder, engineName + "__" + engineId);
		tinkerDBFolder.mkdir();

		// creating tinker smss prop file
		Properties smssProp = new Properties();
		String typeMapStr = "";
		String nameMapStr = "";
		String tinkerDriver = TINKER_DRIVER.TG.toString();
		smssProp.setProperty(Constants.ENGINE, engineId);
		smssProp.setProperty(Constants.ENGINE_ALIAS, engineName);
		smssProp.setProperty("TYPE_MAP", typeMapStr);
		smssProp.setProperty("NAME_MAP", nameMapStr);
		smssProp.setProperty(Constants.TINKER_DRIVER, tinkerDriver);
		smssProp.setProperty(Constants.TINKER_FILE, "@BaseFolder@/db/@ENGINE@/"+tinkerFilePath);

		// save prop file
		try (FileOutputStream out = new FileOutputStream(dbSMSS)) {
			smssProp.store(out, "Properties");
		} catch (IOException e) {
			e.printStackTrace();
		}

		try (MockedStatic<DIHelper> mockedSingleton = Mockito.mockStatic(DIHelper.class)) {
			DIHelper instance = Mockito.mock(DIHelper.class);
			when(DIHelper.getInstance()).thenReturn(instance);
			when(instance.getProperty(Constants.BASE_FOLDER)).thenReturn(baseFolder.getAbsolutePath());

			
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
	public void testCommitXML() throws Exception {
		// make base folders for the db
		String fileSeparator = java.nio.file.FileSystems.getDefault().getSeparator();
		String baseFolderPath = "semoss";
		File baseFolder = new File(tempDir.toFile(), baseFolderPath);
		baseFolder.mkdir();
		File baseDBFolder = new File(tempDir.toFile(), baseFolderPath + fileSeparator + "db");
		baseDBFolder.mkdir();

		// testing setup
		String engineId = "engineId";
		String engineName = "tinkerTest";
		String smssFilePath = engineName + "__" + engineId + ".smss";
		File dbSMSS = new File(baseDBFolder, smssFilePath);

		// make empty tinker file for commit to work
		String tinkerFilePath = "tinkerTest.xml";
		File tinkerDBFolder = new File(baseDBFolder, engineName + "__" + engineId);
		tinkerDBFolder.mkdir();
		
		// creating tinker smss prop file
		Properties smssProp = new Properties();
		String typeMapStr = "";
		String nameMapStr = "";
		String tinkerDriver = TINKER_DRIVER.XML.toString();
		smssProp.setProperty(Constants.ENGINE, engineId);
		smssProp.setProperty(Constants.ENGINE_ALIAS, engineName);
		smssProp.setProperty("TYPE_MAP", typeMapStr);
		smssProp.setProperty("NAME_MAP", nameMapStr);
		smssProp.setProperty(Constants.TINKER_DRIVER, tinkerDriver);
		smssProp.setProperty(Constants.TINKER_FILE, "@BaseFolder@/db/@ENGINE@/"+tinkerFilePath);

		// save prop file
		try (FileOutputStream out = new FileOutputStream(dbSMSS)) {
			smssProp.store(out, "Properties");
		} catch (IOException e) {
			e.printStackTrace();
		}

		try (MockedStatic<DIHelper> mockedSingleton = Mockito.mockStatic(DIHelper.class)) {
			DIHelper instance = Mockito.mock(DIHelper.class);
			when(DIHelper.getInstance()).thenReturn(instance);
			when(instance.getProperty(Constants.BASE_FOLDER)).thenReturn(baseFolder.getAbsolutePath());
			
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
//	public void testCommitNeo4j() throws Exception {
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
//			File owlFile = new File(tempDir.toFile(), engineName + ".OWL");
//			File tinkerFile = new File(tempDir.toFile(), tinkerFilePath);
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
