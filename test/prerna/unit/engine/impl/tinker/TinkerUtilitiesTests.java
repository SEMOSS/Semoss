package prerna.unit.engine.impl.tinker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import prerna.ds.TinkerFrame;
import prerna.engine.impl.tinker.TinkerEngine;
import prerna.engine.impl.tinker.TinkerUtilities;

public class TinkerUtilitiesTests {
	private static final Logger classLogger = LogManager.getLogger(TinkerUtilitiesTests.class);

	@Test
	public void testRemoveAllVertices() {
		// test setup
		TinkerEngine tinkerEngine =  mock(TinkerEngine.class);
		TinkerGraph graph = TinkerGraph.open();
	    graph.addVertex("name", "Alice");
		Mockito.when(tinkerEngine.getGraph()).thenReturn(graph);
		TinkerUtilities.removeAllVertices(tinkerEngine);

		// test graph
		
		GraphTraversal<Vertex, Long> iterate = graph.traversal().V().count();
		while (iterate.hasNext()) {
			Long count = iterate.next();
			assertEquals(0, count);
			classLogger.info("count before drop: " + count);
		}
//		
//		iterate = graph.traversal().V().drop().iterate().count();
//		while (iterate.hasNext()) {
//			Long count = iterate.next();
//			assertEquals(0, count);
//			classLogger.info("count after drop: " + count);
//		}
		
//		//		graph.traversal().V().drop().iterate();
//		// run test to drop verticies
//
//		// validate drop
//		iterate = graph.traversal().V().count();
//		while (iterate.hasNext()) {
//			Long count = iterate.next();
//			assertEquals(0, count);
//			classLogger.info("count after drop: " + count);
//		}
	}
	
	@Test
	public void testSerializeGraph() {
		TinkerFrame tinkerEngine =  mock(TinkerFrame.class);

		
	}

	
	
}
