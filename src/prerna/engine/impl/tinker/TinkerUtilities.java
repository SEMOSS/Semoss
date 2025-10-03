package prerna.engine.impl.tinker;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.structure.Graph;
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
		engine.getGraph().traversal().V().drop().iterate();
	}
	
	/**
	 * Serialize the TinkerGraph in GraphML format
	 * @param dataframe
	 * @param directory
	 * @return
	 */
	public static String serializeGraph(TinkerFrame dataframe, String directory) {
		final Graph graph = ((TinkerFrame) dataframe).g;
		String fileName = "output" + java.lang.System.currentTimeMillis() + ".xml";
		String filePath = directory + "/" + fileName;
		OutputStream os = null;
		try {
			FileSystem fs = FileSystems.getDefault();
			Path p = fs.getPath(filePath);
			os = Files.newOutputStream(p);
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
		return filePath;
	}
}
