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

import java.io.IOException;

import org.junit.jupiter.api.Test;

import prerna.engine.api.IDatabaseEngine;

public class JanusEngineUnitTests {

	///////////// Test Open

	// unable to open graph
	// java.lang.NoClassDefFoundError:
	// org/apache/tinkerpop/gremlin/groovy/jsr223/GremlinGroovyScriptEngine

//	@Test
//	public void testOpenEmptyGraph() throws Exception {
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
