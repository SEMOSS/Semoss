package prerna.ds.gexf;

public interface IGexfIterator {

	/**
	 * Get the start string for the gexf file
	 * @return
	 */
	String getStartString();
	
	/**
	 * Get the end string for the gexf file
	 * @return
	 */
	String getEndString();

	/**
	 * Get the start string for the node portion of the gexf file
	 * @return
	 */
	String getNodeStart();
	
	/**
	 * Get the end string for the node portion of the gexf file
	 * @return
	 */
	String getNodeEnd();
	
	/**
	 * Get the start string for the edge portion of the gexf file
	 * @return
	 */
	String getEdgeStart();
	
	/**
	 * Get the end string for the edge portion of the gexf file
	 * @return
	 */
	String getEdgeEnd();
	
	/**
	 * Boolean if there are more nodes to output
	 * @return
	 */
	boolean hasNextNode();
	
	/**
	 * String containing the next node with its properties
	 * @return
	 */
	String getNextNodeString();
	
	/**
	 * Boolean if there are more edges to output 
	 * @return
	 */
	boolean hasNextEdge();
	
	/**
	 * String containing the next edge with its properties
	 * @return
	 */
	String getNextEdgeString();
	
}
