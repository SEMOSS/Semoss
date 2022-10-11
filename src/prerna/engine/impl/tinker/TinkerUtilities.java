package prerna.engine.impl.tinker;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;

import prerna.ds.TinkerFrame;
import prerna.util.Constants;

public class TinkerUtilities {

	private static final Logger classLogger = LogManager.getLogger(TinkerUtilities.class);
	
	private TinkerUtilities() {
		
	}
	
	/**
	 * 
	 * @param engine
	 */
	public static void removeAllVertices(TinkerEngine engine) {
		GraphTraversal<Vertex, Long> iterate = engine.g.traversal().V().drop().iterate().count();
		while(iterate.hasNext()) {
			Long count = iterate.next();
			classLogger.info("Dropping " + count + " verticies from engine " + engine.getEngineName() + "__" + engine.getEngineId());
		}
	}
	
	/**
	 * Serialize the TinkerGraph in GraphML format
	 * @param dataframe
	 * @param directory
	 * @return
	 */
	public static String serializeGraph(TinkerFrame dataframe, String directory) {
		String absoluteFileName = null;
		final Graph graph = ((TinkerFrame) dataframe).g;
		absoluteFileName = "output" + java.lang.System.currentTimeMillis() + ".xml";
		String fileName = directory + "/" + absoluteFileName;
		OutputStream os = null;
		try {
			os = new FileOutputStream(fileName);
			graph.io(IoCore.graphml()).writer().normalize(true).create().writeGraph(os, graph);
		} catch (Exception ex) {
			classLogger.error(Constants.STACKTRACE, ex);
		} finally {
			try {
				if (os != null) {
					os.close();
				}
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		return absoluteFileName;
	}
}
