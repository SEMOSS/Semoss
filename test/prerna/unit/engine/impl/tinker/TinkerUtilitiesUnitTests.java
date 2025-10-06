package prerna.unit.engine.impl.tinker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import prerna.ds.TinkerFrame;
import prerna.engine.impl.tinker.TinkerEngine;
import prerna.engine.impl.tinker.TinkerUtilities;

public class TinkerUtilitiesUnitTests {
	private static final Logger classLogger = LogManager.getLogger(TinkerUtilitiesUnitTests.class);

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
	public void testSerializeGraph(@TempDir File tempDir) throws IOException {
		// tinker frame setup
		TinkerFrame tf = new TinkerFrame();
		String[] values = new String[] { "Alice" };
		String[] headers = new String[] { "name" };
		Map<Integer, Set<Integer>> cardinality = new HashMap<>();
		tf.addRelationship(headers, values, cardinality);

		// run test code
		TinkerUtilities.serializeGraph(tf, tempDir.getAbsolutePath());

		// validate graph is serialized
		String[] paths = tempDir.list();
		assertEquals(1, paths.length);

		// expecting output...xml
		String fileName = paths[0];
		assertTrue(fileName.toString().contains("output"));
		assertTrue(fileName.toString().endsWith(".xml"));

		// validating xml file content
		String fileContent = new String(Files.readAllBytes(new File(tempDir, fileName).toPath()));
		assertTrue(fileContent.contains("<data key=\"labelV\">vertex</data>"));
		assertTrue(fileContent.contains("<data key=\"" + TinkerFrame.TINKER_ID + "\">name:Alice</data>"));
		assertTrue(fileContent.contains("<data key=\"" + TinkerFrame.TINKER_NAME + "\">Alice</data>"));
		assertTrue(fileContent.contains("<data key=\"" + TinkerFrame.TINKER_TYPE + "\">name</data>"));
	}
}
