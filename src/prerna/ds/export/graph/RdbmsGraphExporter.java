package prerna.ds.export.graph;

import java.awt.Color;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import prerna.ds.h2.H2Frame;
import prerna.ui.helpers.TypeColorShapeTable;
import prerna.util.Constants;

public class RdbmsGraphExporter extends AbstractGraphExporter {

	private H2Frame frame;

	// contains list of headers
	private String curVertex;
	private String aliasCurVertex;
	private Set<String> vertices;
	private Iterator<String> verticesIterator;

	// contains array of 2 headers designating a relationship
	// index 0 is source, index 1 is target
	private String[] curRelationship;
	private String[] aliasCurRelationship;
	private Set<String[]> relationships;
	private Iterator<String[]> relationshipIterator;

	// we need to keep track of which vertices
	// have an alias that ends up being the same
	// since this will result in 2 vertices being painted
	// on the FE
//	private Map<String, Set<String>> aliasMap;
	
	private ResultSet edgeRs;
	private ResultSet nodeRs;

	public RdbmsGraphExporter(H2Frame frame) {
		this.frame = frame;
		Map<String, Set<String>> edgeHash = frame.getEdgeHash();
		parseEdgeHash(edgeHash);
//		generateDupAliasMap(frame.getColumnHeaders(), frame.getColumnAliasName());
	}

//	private void generateDupAliasMap(String[] columnHeaders, String[] columnAliasName) {
//		// loop through and find duplicate headers
//		this.aliasMap = new Hashtable<String, Set<String>>();
//		for(int i = 0; i < columnAliasName.length; i++) {
//			String origHeader = columnHeaders[i];
//			String alias = columnAliasName[i];
//			for(int j = i+1; j < columnAliasName.length; j++) {
//				String otherOrigHeader = columnHeaders[j];
//				String otherAlias = columnAliasName[j];
//				
//				if(alias.equals(otherAlias)) {
//					// we have a match
//					Set<String> origHeadersSet = null;
//					if(aliasMap.containsKey(alias)) {
//						origHeadersSet = aliasMap.get(alias);
//					} else {
//						origHeadersSet = new HashSet<String>();
//						aliasMap.put(alias, origHeadersSet);
//					}
//					aliasMap.get(alias).add(origHeader);
//					aliasMap.get(alias).add(otherOrigHeader);
//				}
//			}
//		}
//	}

	/**
	 * Parse the edge hash to get lists of each individual
	 * node and relationship that we need to create an iterator for
	 * @param edgeHash
	 */
	private void parseEdgeHash(Map<String, Set<String>> edgeHash) {
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

	@Override
	public boolean hasNextEdge() {
		// first time, everything is null
		if(this.edgeRs == null && relationshipIterator.hasNext()) {
			this.curRelationship = relationshipIterator.next();
			this.aliasCurRelationship = getAliasRelationship();
			this.edgeRs = createEdgeRs(curRelationship);
			// so we made it, run this again to 
			// see if this relationship has values to return
			return hasNextEdge();
		} else {
			// next time, need to check if this iterator still
			// has things we need to output
			boolean hasNext = false;
			try {
				hasNext = this.edgeRs.next();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			if(hasNext) {
				// still have more
				return true;
			} else {
				try {
					this.edgeRs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				// okay, we are done with this one
				// got to see if there is another relationship to try
				if(this.relationshipIterator.hasNext()) {
					this.curRelationship = relationshipIterator.next();
					this.aliasCurRelationship = getAliasRelationship();
					this.edgeRs = createEdgeRs(curRelationship);
					// since we got to try and see if this has a next
					// do this by recursively calling this method
					return hasNextEdge();
				} else {
					// well, we got nothing
					return false;
				}
			}
		}
	}

	@Override
	public Map<String, Object> getNextEdge() {
		// TODO: Figure out how to do edge properties and stuff
		// till then, this is a really easy return
		Object sourceVal = null;
		Object targetVal = null;
		try {
			sourceVal = this.edgeRs.getObject(1);
			targetVal = this.edgeRs.getObject(2);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		String source = this.aliasCurRelationship[0] + "/" + sourceVal;
		String target = this.aliasCurRelationship[1] + "/" + targetVal;

		Map<String, Object> relationshipMap = new HashMap<String, Object>();
		relationshipMap.put("source", source);
		relationshipMap.put("target", target);
		relationshipMap.put("uri", source + ":" + target);

		// also push empty edge properties
		Map<String, Object> propMap = new HashMap<String, Object>();
		relationshipMap.put("propHash", propMap);

		return relationshipMap;
	}

	@Override
	public boolean hasNextVert() {
		// first time, everything is null
		if(this.nodeRs == null && verticesIterator.hasNext()) {
			this.curVertex = verticesIterator.next();
			this.aliasCurVertex = getAliasVertex();
			this.nodeRs = createNodeRs(curVertex);
			// so we made it, run this again to 
			// see if this vertex has values to return
			return hasNextVert();
		} else {
			// next time, need to check if this iterator still
			// has things we need to output
			boolean hasNext = false;
			try {
				hasNext = this.nodeRs.next();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			if(hasNext) {
				// still have more
				return true;
			} else {
				try {
					this.nodeRs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				// okay, we are done with this one
				// got to see if there is another vertex to try
				if(this.verticesIterator.hasNext()) {
					this.curVertex = verticesIterator.next();
					this.aliasCurVertex = getAliasVertex();
					this.nodeRs = createNodeRs(curVertex);
					// since we got to try and see if this has a next
					// do this by recursively calling this method
					return hasNextVert();
				} else {
					// well, we got nothing
					return false;
				}
			}
		}
	}

	@Override
	public Map<String, Object> getNextVert() {
		// TODO: Figure out how to do node properties and stuff
		// till then, this is a really easy return
		Object nodeVal = null;
		try {
			nodeVal = this.nodeRs.getObject(1);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		String node = this.aliasCurVertex + "/" + nodeVal;

		Map<String, Object> vertexMap = new HashMap<String, Object>();
		vertexMap.put("uri", node);
		vertexMap.put(Constants.VERTEX_NAME, nodeVal);
		vertexMap.put(Constants.VERTEX_TYPE, this.aliasCurVertex);

		// need to add in color
		Color color = TypeColorShapeTable.getInstance().getColor(this.aliasCurVertex, nodeVal.toString());
		vertexMap.put("VERTEX_COLOR_PROPERTY", IGraphExporter.getRgb(color));

		// also push empty vertex properties
		Map<String, Object> propMap = new HashMap<String, Object>();
		vertexMap.put("propHash", propMap);

		// add to the meta count
		addVertCount(this.aliasCurVertex);
		
		return vertexMap;
	}

	/**
	 * Return the relationship result set between 2 columns in the frame
	 * @param relationship
	 * @return
	 */
	private ResultSet createEdgeRs(String[] relationship) {
		String query = createQueryString(relationship);
		return this.frame.execQuery(query);
	}

	/**
	 * Create the query string to get a node and a list of properties
	 * @param nodeName
	 * @param props
	 * @return
	 */
	private String createQueryString(String[] selectors) {
		StringBuilder sql = new StringBuilder("SELECT DISTINCT ");
		sql.append(selectors[0]).append(" ");
		for(int i = 1; i < selectors.length; i++) {
			sql.append(", ").append(selectors[i]);
		}
		sql.append(" FROM ").append(frame.getTableName());
		String sqlFilter = frame.getSqlFilter();
		if(sqlFilter != null && !sqlFilter.isEmpty()) {
			sql.append(" WHERE ").append(frame.getSqlFilter());
		}
		return sql.toString();
	}

	/**
	 * Return the distinct values of a node
	 * @param nodeName
	 * @return
	 */
	private ResultSet createNodeRs(String nodeName) {
		String query = createQueryString(nodeName);
		return this.frame.execQuery(query);
	}

	/**
	 * Create the query string to get distinct values of a node
	 * @param selector
	 * @return
	 */
	private String createQueryString(String selector) {
		StringBuilder sql = new StringBuilder("SELECT DISTINCT ");
		sql.append(selector).append(" ");
		sql.append(" FROM ").append(frame.getTableName());
		String sqlFilter = frame.getSqlFilter();
		if(sqlFilter != null && !sqlFilter.isEmpty()) {
			sql.append(" WHERE ").append(frame.getSqlFilter());
		}
		return sql.toString();
	}
	
	private String[] getAliasRelationship() {
		String[] aliasCurRelationship = new String[2];
		aliasCurRelationship[0] = frame.getAliasForUniqueName(this.curRelationship[0]);
		aliasCurRelationship[1] = frame.getAliasForUniqueName(this.curRelationship[1]);
		return aliasCurRelationship;
	}

	private String getAliasVertex() {
		return frame.getAliasForUniqueName(this.curVertex);
	}
	
}
