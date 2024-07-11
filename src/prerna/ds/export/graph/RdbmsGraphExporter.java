//package prerna.ds.export.graph;
//
//import java.awt.Color;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Hashtable;
//import java.util.Map;
//import java.util.Set;
//
//import prerna.ds.h2.H2Frame;
//import prerna.ui.helpers.TypeColorShapeTable;
//import prerna.util.Constants;
//
//public class RdbmsGraphExporter extends AbstractTableGraphExporter {
//
//	private H2Frame frame;
//
//	// we need to keep track of which vertices
//	// have an alias that ends up being the same
//	// since this will result in 2 vertices being painted
//	// on the FE
//	private Map<String, Set<String>> aliasMap;
//	
//	private ResultSet edgeRs;
//	private ResultSet nodeRs;
//
//	public RdbmsGraphExporter(H2Frame frame) {
//		this.frame = frame;
//		// parent class handles the abstraction of the edge hash and determining
//		// which single vertex and relationship to push into the nodeRs and edgeRs
//		Map<String, Set<String>> edgeHash = frame.getEdgeHash();
//		parseEdgeHash(edgeHash);
//		generateDupAliasMap(frame.getColumnHeaders(), frame.getColumnAliasName());
//	}
//
//	//TODO: i have it such that we will not send duplicate nodes to the FE
//	//		however, still possible that i would send multiple edges
//	//		will address this in the future
//	private void generateDupAliasMap(String[] columnHeaders, String[] columnAliasName) {
//		// loop through and find duplicate headers
//		this.aliasMap = new Hashtable<String, Set<String>>();
//		for(int i = 0; i < columnAliasName.length; i++) {
//			String origHeader = columnHeaders[i];
//			String alias = columnAliasName[i];
//			// if they are the same, just put it in the map
//			if(origHeader.equals(alias)) {
//				Set<String> nameSet = null;
//				if(aliasMap.containsKey(origHeader)) {
//					nameSet = aliasMap.get(origHeader);
//					nameSet.add(origHeader);
//					aliasMap.put(origHeader, nameSet);
//				} else {
//					nameSet = new HashSet<String>();
//					nameSet.add(origHeader);
//					aliasMap.put(origHeader, nameSet);
//				}
//			} else {
//				// we have a valid alias
//				// add it to the map
//				Set<String> nameSet = new HashSet<String>();
//				if(aliasMap.containsKey(alias)) {
//					nameSet = aliasMap.get(alias);
//					nameSet.add(origHeader);
//					aliasMap.put(alias, nameSet);
//				} else {
//					nameSet = new HashSet<String>();
//					nameSet.add(origHeader);
//					aliasMap.put(alias, nameSet);
//				}
//			}
//		}
//		// need to override the vertices iterator to only use the alias
//		this.verticesIterator = aliasMap.keySet().iterator();
//	}
//
//	@Override
//	public boolean hasNextEdge() {
//		// first time, everything is null
//		if(this.edgeRs == null && relationshipIterator.hasNext()) {
//			this.curRelationship = relationshipIterator.next();
//			this.aliasCurRelationship = getAliasRelationship();
//			this.edgeRs = createEdgeRs(curRelationship);
//			// so we made it, run this again to 
//			// see if this relationship has values to return
//			return hasNextEdge();
//		} else {
//			// if we are here and the edge rs is still null
//			// that means there are no relationships
//			// ... seems like a dumb graph
//			if(this.edgeRs == null) {
//				return false;
//			}
//			// next time, need to check if this iterator still
//			// has things we need to output
//			boolean hasNext = false;
//			try {
//				hasNext = this.edgeRs.next();
//			} catch (SQLException e) {
//				classLogger.error(Constants.STACKTRACE, e);
//			}
//			if(hasNext) {
//				// still have more
//				return true;
//			} else {
//				try {
//					this.edgeRs.close();
//				} catch (SQLException e) {
//					classLogger.error(Constants.STACKTRACE, e);
//				}
//				// okay, we are done with this one
//				// got to see if there is another relationship to try
//				if(this.relationshipIterator.hasNext()) {
//					this.curRelationship = relationshipIterator.next();
//					this.aliasCurRelationship = getAliasRelationship();
//					this.edgeRs = createEdgeRs(curRelationship);
//					// since we got to try and see if this has a next
//					// do this by recursively calling this method
//					return hasNextEdge();
//				} else {
//					// well, we got nothing
//					return false;
//				}
//			}
//		}
//	}
//
//	@Override
//	public Map<String, Object> getNextEdge() {
//		// TODO: Figure out how to do edge properties and stuff
//		// till then, this is a really easy return
//		Object sourceVal = null;
//		Object targetVal = null;
//		try {
//			sourceVal = this.edgeRs.getObject(1);
//			targetVal = this.edgeRs.getObject(2);
//		} catch (SQLException e1) {
//			// TODO Auto-generated catch block
//			classLogger.error(Constants.STACKTRACE, e1);
//		}
//		// if we still have a null
//		if(sourceVal == null) {
//			sourceVal = "EMPTY";
//		}
//		if(targetVal == null) {
//			targetVal = "EMPTY";
//		}
//
//		String source = this.aliasCurRelationship[0] + "/" + sourceVal;
//		String target = this.aliasCurRelationship[1] + "/" + targetVal;
//
//		Map<String, Object> relationshipMap = new HashMap<String, Object>();
//		relationshipMap.put("source", source);
//		relationshipMap.put("target", target);
//		relationshipMap.put("uri", source + ":" + target);
//
//		// also push empty edge properties
//		Map<String, Object> propMap = new HashMap<String, Object>();
//		relationshipMap.put("propHash", propMap);
//
//		return relationshipMap;
//	}
//
//	@Override
//	public boolean hasNextVert() {
//		// first time, everything is null
//		if(this.nodeRs == null && verticesIterator.hasNext()) {
//			this.curVertex = verticesIterator.next();
//			this.nodeRs = createNodeRs(curVertex);
//			// so we made it, run this again to 
//			// see if this vertex has values to return
//			return hasNextVert();
//		} else {
//			// next time, need to check if this iterator still
//			// has things we need to output
//			boolean hasNext = false;
//			try {
//				hasNext = this.nodeRs.next();
//			} catch (SQLException e) {
//				classLogger.error(Constants.STACKTRACE, e);
//			}
//			if(hasNext) {
//				// still have more
//				return true;
//			} else {
//				try {
//					this.nodeRs.close();
//				} catch (SQLException e) {
//					classLogger.error(Constants.STACKTRACE, e);
//				}
//				// okay, we are done with this one
//				// got to see if there is another vertex to try
//				if(this.verticesIterator.hasNext()) {
//					this.curVertex = verticesIterator.next();
//					this.nodeRs = createNodeRs(curVertex);
//					// since we got to try and see if this has a next
//					// do this by recursively calling this method
//					return hasNextVert();
//				} else {
//					// well, we got nothing
//					return false;
//				}
//			}
//		}
//	}
//
//	@Override
//	public Map<String, Object> getNextVert() {
//		// TODO: Figure out how to do node properties and stuff
//		// till then, this is a really easy return
//		
//		/*
//		 * For the vertices iterator
//		 * in the method generateDupAliasMap
//		 * we already switch to using the alias
//		 * so we do not need to sue this.aliasCurVertex
//		 * and the method to get the unique set of vertices
//		 * takes this into consideration
//		 */
//		Object nodeVal = null;
//		try {
//			nodeVal = this.nodeRs.getObject(1);
//		} catch (SQLException e1) {
//			// TODO Auto-generated catch block
//			classLogger.error(Constants.STACKTRACE, e1);
//		}
//		
//		// if we still have a null
//		if(nodeVal == null) {
//			nodeVal = "EMPTY";
//		}
//
//		String node = this.curVertex + "/" + nodeVal;
//
//		Map<String, Object> vertexMap = new HashMap<String, Object>();
//		vertexMap.put("uri", node);
//		vertexMap.put(Constants.VERTEX_NAME, nodeVal);
//		vertexMap.put(Constants.VERTEX_TYPE, this.curVertex);
//
//		// need to add in color
//		Color color = TypeColorShapeTable.getInstance().getColor(this.curVertex, nodeVal.toString());
//		vertexMap.put("VERTEX_COLOR_PROPERTY", IGraphExporter.getRgb(color));
//
//		// also push empty vertex properties
//		Map<String, Object> propMap = new HashMap<String, Object>();
//		vertexMap.put("propHash", propMap);
//
//		// add to the meta count
//		addVertCount(this.curVertex);
//		
//		return vertexMap;
//	}
//
//	/**
//	 * Return the relationship result set between 2 columns in the frame
//	 * @param relationship
//	 * @return
//	 */
//	private ResultSet createEdgeRs(String[] relationship) {
//		String query = createQueryString(relationship);
//		return this.frame.execQuery(query);
//	}
//
//	/**
//	 * Create the query string to get a node and a list of properties
//	 * @param nodeName
//	 * @param props
//	 * @return
//	 */
//	private String createQueryString(String[] selectors) {
//		StringBuilder sql = new StringBuilder("SELECT DISTINCT ");
//		sql.append(selectors[0]).append(" ");
//		for(int i = 1; i < selectors.length; i++) {
//			sql.append(", ").append(selectors[i]);
//		}
//		sql.append(" FROM ").append(frame.getTableName());
//		String sqlFilter = frame.getSqlFilter();
//		if(sqlFilter != null && !sqlFilter.isEmpty()) {
//			sql.append(frame.getSqlFilter());
//		}
//		return sql.toString();
//	}
//
//	/**
//	 * Return the distinct values of a node
//	 * @param nodeName
//	 * @return
//	 */
//	private ResultSet createNodeRs(String nodeName) {
//		String query = createQueryString(nodeName);
//		return this.frame.execQuery(query);
//	}
//
//	/**
//	 * Create the query string to get distinct values of a node
//	 * @param selector
//	 * @return
//	 */
//	private String createQueryString(String selector) {
//		StringBuilder sql = new StringBuilder();
//		if(this.aliasMap.containsKey(selector)) {
//			sql.append("SELECT DISTINCT ");
//			Set<String> actualSelectors = aliasMap.get(selector);
//			boolean first = true;
//			for(String select : actualSelectors) {
//				if(first) {
//					sql.append(select).append(" AS ").append(selector)
//						.append(" FROM ").append(frame.getTableName());
//					String sqlFilter = frame.getSqlFilter();
//					if(sqlFilter != null && !sqlFilter.isEmpty()) {
//						sql.append(frame.getSqlFilter());
//					}
//					first = false;
//				} else {
//					sql.append(" UNION SELECT DISTINCT ").append(select).append(" AS ").append(selector)
//						.append(" FROM ").append(frame.getTableName());
//					String sqlFilter = frame.getSqlFilter();
//					if(sqlFilter != null && !sqlFilter.isEmpty()) {
//						sql.append(frame.getSqlFilter());
//					}
//				}
//			}
//		} else {
//			sql.append("SELECT DISTINCT ");
//			sql.append(selector).append(" ");
//			sql.append(" FROM ").append(frame.getTableName());
//			String sqlFilter = frame.getSqlFilter();
//			if(sqlFilter != null && !sqlFilter.isEmpty()) {
//				sql.append(frame.getSqlFilter());
//			}
//		}
//		return sql.toString();
//	}
//	
//	private String[] getAliasRelationship() {
//		String[] aliasCurRelationship = new String[2];
//		aliasCurRelationship[0] = frame.getAliasForUniqueName(this.curRelationship[0]);
//		aliasCurRelationship[1] = frame.getAliasForUniqueName(this.curRelationship[1]);
//		return aliasCurRelationship;
//	}
//
//	private String getAliasVertex() {
//		return frame.getAliasForUniqueName(this.curVertex);
//	}
//
//	@Override
//	public Object getData() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//	
//}
