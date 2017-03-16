package prerna.ds;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.sun.jersey.core.util.StringIgnoreCaseKeyComparator;

public final class TinkerAlgorithmUtility {

	protected static final Logger LOGGER = LogManager.getLogger(TinkerAlgorithmUtility.class.getName());
	
	private TinkerAlgorithmUtility() {
		
	}
	
	/**
	 * Return the paths that are cycles in the tinkerframe given a max tinker size
	 * @param tf
	 * @param maxCycleSize
	 * @return
	 */
	public static String runLoopIdentifer(TinkerFrame tf, int cycleSize) {
		
		GraphTraversal<Vertex, Path> traversal = tf.g.traversal().V().as("a").repeat(__.out().simplePath()).times(cycleSize)
				.where(__.out().as("a")).path().dedup().by(__.unfold().order().by(TinkerFrame.TINKER_ID, new StringIgnoreCaseKeyComparator()).dedup().fold());
		
		StringBuilder cycles = new StringBuilder();
		while(traversal.hasNext()) {
			Path p = traversal.next();
			int size = p.size();
			Vertex v = p.get(0);
			cycles.append(v.value(TinkerFrame.TINKER_ID) + "");
			for(int stepIdx = 1; stepIdx < size; stepIdx++) {
				v = p.get(stepIdx);
				cycles.append(", " + v.value(TinkerFrame.TINKER_ID));
			}
			cycles.append("\n");
		}
		
		return cycles.toString();
	}
	
}
