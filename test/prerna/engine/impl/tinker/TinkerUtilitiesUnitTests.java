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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import prerna.SemossUnitTest;
import prerna.ds.TinkerFrame;

public class TinkerUtilitiesUnitTests extends SemossUnitTest {
	private static final Logger classLogger = LogManager.getLogger(TinkerUtilitiesUnitTests.class);

	@BeforeEach
	void setup() throws IOException {
		FileUtils.cleanDirectory(tempDir.toFile());
	}

	@Test
	public void testRemoveAllVertices() {
		// test setup
		TinkerEngine tinkerEngine = mock(TinkerEngine.class);
		TinkerGraph graph = TinkerGraph.open();
		graph.addVertex("name", "Alice");
		Mockito.when(tinkerEngine.getGraph()).thenReturn(graph);

		Graph testGraph = tinkerEngine.getGraph();
		// test graph
		GraphTraversal<Vertex, Long> iterate = testGraph.traversal().V().count();
		assertTrue(iterate.hasNext());
		while (iterate.hasNext()) {
			Long count = iterate.next();
			assertEquals(1, count);
			classLogger.info("count before drop: " + count);
		}
		TinkerUtilities.removeAllVertices(tinkerEngine);
		iterate = tinkerEngine.getGraph().traversal().V().count();
		assertTrue(iterate.hasNext());
		while (iterate.hasNext()) {
			Long count = iterate.next();
			assertEquals(0, count);
			classLogger.info("count after drop: " + count);
		}
	}

	@Test
	public void testSerializeGraph() throws IOException {
		// tinker frame setup
		TinkerFrame tf = new TinkerFrame();
		String[] values = new String[] { "Alice" };
		String[] headers = new String[] { "name" };
		Map<Integer, Set<Integer>> cardinality = new HashMap<>();
		tf.addRelationship(headers, values, cardinality);

		// run test code
		TinkerUtilities.serializeGraph(tf, tempDir.toFile().getAbsolutePath());

		// validate graph is serialized
		String[] paths = tempDir.toFile().list();
		assertEquals(1, paths.length);

		// expecting output...xml
		String fileName = paths[0];
		assertTrue(fileName.toString().contains("output"));
		assertTrue(fileName.toString().endsWith(".xml"));

		// validating xml file content
		String fileContent = new String(Files.readAllBytes(new File(tempDir.toFile(), fileName).toPath()));
		assertTrue(fileContent.contains("<data key=\"labelV\">vertex</data>"));
		assertTrue(fileContent.contains("<data key=\"" + TinkerFrame.TINKER_ID + "\">name:Alice</data>"));
		assertTrue(fileContent.contains("<data key=\"" + TinkerFrame.TINKER_NAME + "\">Alice</data>"));
		assertTrue(fileContent.contains("<data key=\"" + TinkerFrame.TINKER_TYPE + "\">name</data>"));
	}
}
