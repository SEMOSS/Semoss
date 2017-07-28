package prerna.ds.export.graph;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public abstract class AbstractTableGraphExporter extends AbstractGraphExporter {

	protected static final String ROW_ID = "DATA_ROW_ID";
	
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
	
	//TODO: i have it such that we will not send duplicate nodes to the FE
	//		however, still possible that i would send multiple edges
	protected Map<String, Set<String>> generateDupAliasMap(String[] columnHeaders, String[] columnAliasName) {
		// loop through and find duplicate headers
		Map<String, Set<String>> aliasMap = new Hashtable<String, Set<String>>();
		for(int i = 0; i < columnAliasName.length; i++) {
			String origHeader = columnHeaders[i];
			String alias = columnAliasName[i];
			// if they are the same, just put it in the map
			if(origHeader.equals(alias)) {
				Set<String> nameSet = null;
				if(aliasMap.containsKey(origHeader)) {
					nameSet = aliasMap.get(origHeader);
					nameSet.add(origHeader);
					aliasMap.put(origHeader, nameSet);
				} else {
					nameSet = new HashSet<String>();
					nameSet.add(origHeader);
					aliasMap.put(origHeader, nameSet);
				}
			} else {
				// we have a valid alias
				// add it to the map
				Set<String> nameSet = new HashSet<String>();
				if(aliasMap.containsKey(alias)) {
					nameSet = aliasMap.get(alias);
					nameSet.add(origHeader);
					aliasMap.put(alias, nameSet);
				} else {
					nameSet = new HashSet<String>();
					nameSet.add(origHeader);
					aliasMap.put(alias, nameSet);
				}
			}
		}
		
		return aliasMap;
	}
}
