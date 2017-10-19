package prerna.ds.export.graph;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public abstract class AbstractTableGraphExporter extends AbstractGraphExporter {

	/*
	 * This class is for abstracting out the methods required 
	 * in processing the edge hash (how vertices are related to each other)
	 * within a table to export as a graph
	 */
	
	// contains list of headers
	protected String curVertex;
	protected String aliasCurVertex;
	protected Set<String> vertices;
	protected Iterator<String> verticesIterator;

	// contains array of 2 headers designating a relationship
	// index 0 is source, index 1 is target
	protected String[] curRelationship;
	protected String[] aliasCurRelationship;
	protected Set<String[]> relationships;
	protected Iterator<String[]> relationshipIterator;
	
	
	/**
	 * Parse the edge hash to get lists of each individual
	 * node and relationship that we need to create an iterator for
	 * @param edgeHash
	 */
	protected void parseEdgeHash(Map<String, Set<String>> edgeHash) {
		this.vertices = new HashSet<String>();
		this.relationships = new HashSet<String[]>();

		for(String startNode : edgeHash.keySet()) {
			// add each start node to the vertex set
			this.vertices.add(startNode);

			// get the set of end nodes for this start node
			Set<String> endNodes = edgeHash.get(startNode);
			for(String endNode : endNodes) {
				// add each end node to the vertex set
				this.vertices.add(endNode);
				// and add each relationship to the relationship set
				this.relationships.add(new String[]{startNode, endNode});
			}
		}

		this.verticesIterator = this.vertices.iterator();
		this.relationshipIterator = this.relationships.iterator();
	}
}
