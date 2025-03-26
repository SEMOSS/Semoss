package prerna.unit.engine.impl.tinker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.ds.TinkerFrame;
import prerna.engine.impl.tinker.TinkerEngine;
import prerna.engine.impl.tinker.TinkerUtilities;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

public class TinkerUtilitiesTests {
	private static final Logger classLogger = LogManager.getLogger(TinkerUtilitiesTests.class);

	private FileSystem fs;

	@BeforeEach
	void setup() {
		fs = Jimfs.newFileSystem(Configuration.unix());
	}

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
	public void testSerializeGraph() throws IOException {
		TinkerFrame tfMock =  mock(TinkerFrame.class);

		try (MockedStatic<FileSystems> fss = Mockito.mockStatic(FileSystems.class)) {
			fss.when(FileSystems::getDefault).thenReturn(fs);

			Path dir = fs.getPath("dir");
			Files.createDirectory(dir);

			TinkerUtilities.serializeGraph(tfMock, "dir");

			try (Stream<Path> paths = Files.list(dir)) {

				assertEquals(1, paths.count());
				Path result = paths.findFirst().get();
				assertTrue(result.toString().contains("output"));
				assertTrue(result.toString().endsWith(".xml"));

				assertTrue(Files.exists(result));
				List<String> s = Files.readAllLines(result);
				assertEquals(1, s.size());
			}
		}
		
	}

	
	
}
