package prerna.engine.impl.tinker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class TinkerUtilities {

	private static final Logger LOGGER = LogManager.getLogger(TinkerUtilities.class);
	
	private TinkerUtilities() {
		
	}
	
	public static void removeAllVertices(TinkerEngine engine) {
		GraphTraversal<Vertex, Long> iterate = engine.g.traversal().V().drop().iterate().count();
		while(iterate.hasNext()) {
			Long count = iterate.next();
			LOGGER.info("Dropping " + count + " verticies from engine " + engine.getEngineName() + "__" + engine.getEngineId());
		}
	}
}
