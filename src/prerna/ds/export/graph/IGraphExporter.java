package prerna.ds.export.graph;

import java.awt.Color;
import java.util.Map;

public interface IGraphExporter {

	/**
	 * Boolean if there are more edges to return
	 * @return
	 */
	boolean hasNextEdge();
	
	Map<String, Object> getNextEdge();
	
	/**
	 * Boolean if there are more vertices to return
	 * @return
	 */
	boolean hasNextVert();
	
	Map<String, Object> getNextVert();
	
	/**
	 * Get a string representation of the node color
	 * @param c
	 * @return
	 */
	static String getRgb(Color c) {
		return c.getRed() + "," + c.getGreen() + "," +c.getBlue();
	}
}
