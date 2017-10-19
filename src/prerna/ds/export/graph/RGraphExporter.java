//package prerna.ds.export.graph;
//
//import java.awt.Color;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//
//import prerna.ds.r.RDataTable;
//import prerna.ui.helpers.TypeColorShapeTable;
//import prerna.util.Constants;
//
//public class RGraphExporter extends AbstractTableGraphExporter {
//
//	private RDataTable frame;
//	
//	private Iterator<Object[]> edgeIterator;
//	private Iterator<Object[]> nodeIterator;
//	
//	public RGraphExporter(RDataTable frame) {
//		this.frame = frame;
//		// parent class handles the abstraction of the edge hash and determining
//		// which single vertex and relationship to push into the nodeRs and edgeRs
//		Map<String, Set<String>> edgeHash = frame.getEdgeHash();
//		parseEdgeHash(edgeHash);
//	}
//
//	@Override
//	public boolean hasNextEdge() {
//		// first time, everything is null
//		if(this.edgeIterator == null && relationshipIterator.hasNext()) {
//			this.curRelationship = relationshipIterator.next();
//			this.aliasCurRelationship = getAliasRelationship();
//			this.edgeIterator = createEdgeRs(curRelationship).iterator();
//			// so we made it, run this again to 
//			// see if this relationship has values to return
//			return hasNextEdge();
//		} else {
//			// if we are here and the edge rs is still null
//			// that means there are no relationships
//			// ... seems like a dumb graph
//			if(this.edgeIterator == null) {
//				return false;
//			}
//			// next time, need to check if this iterator still
//			// has things we need to output
//			boolean hasNext = this.edgeIterator.hasNext();
//			if(hasNext) {
//				// still have more
//				return true;
//			} else {
//				// okay, we are done with this one
//				// got to see if there is another relationship to try
//				if(this.relationshipIterator.hasNext()) {
//					this.curRelationship = relationshipIterator.next();
//					this.aliasCurRelationship = getAliasRelationship();
//					this.edgeIterator = createEdgeRs(curRelationship).iterator();
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
//		Object[] nextEdge = edgeIterator.next();
//		Object sourceVal = nextEdge[0];
//		Object targetVal = nextEdge[1];
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
//		if(this.nodeIterator == null && verticesIterator.hasNext()) {
//			this.curVertex = verticesIterator.next();
//			this.aliasCurVertex = getAliasVertex();
//			this.nodeIterator = createNodeRs(curVertex).iterator();
//			// so we made it, run this again to 
//			// see if this vertex has values to return
//			return hasNextVert();
//		} else {
//			// next time, need to check if this iterator still
//			// has things we need to output
//			boolean hasNext = this.nodeIterator.hasNext();
//			if(hasNext) {
//				// still have more
//				return true;
//			} else {
//				// okay, we are done with this one
//				// got to see if there is another vertex to try
//				if(this.verticesIterator.hasNext()) {
//					this.curVertex = verticesIterator.next();
//					this.aliasCurVertex = getAliasVertex();
//					this.nodeIterator = createNodeRs(curVertex).iterator();
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
//		Object nodeVal = this.nodeIterator.next()[0];
//
//		String node = this.aliasCurVertex + "/" + nodeVal;
//
//		Map<String, Object> vertexMap = new HashMap<String, Object>();
//		vertexMap.put("uri", node);
//		vertexMap.put(Constants.VERTEX_NAME, nodeVal);
//		vertexMap.put(Constants.VERTEX_TYPE, this.aliasCurVertex);
//
//		// need to add in color
//		Color color = TypeColorShapeTable.getInstance().getColor(this.aliasCurVertex, nodeVal.toString());
//		vertexMap.put("VERTEX_COLOR_PROPERTY", IGraphExporter.getRgb(color));
//
//		// also push empty vertex properties
//		Map<String, Object> propMap = new HashMap<String, Object>();
//		vertexMap.put("propHash", propMap);
//
//		// add to the meta count
//		addVertCount(this.aliasCurVertex);
//		
//		return vertexMap;
//	}
//
//	/**
//	 * Return the relationship result set between 2 columns in the frame
//	 * @param relationship
//	 * @return
//	 */
//	private List<Object[]> createEdgeRs(String[] relationship) {
//		String rScript = createQueryString(relationship);
//		return this.frame.getBulkDataRow(rScript, relationship);
//	}
//
//	/**
//	 * Create the query string to get a node and a list of properties
//	 * @param nodeName
//	 * @param props
//	 * @return
//	 */
//	private String createQueryString(String[] selectors) {
//		StringBuilder rScript = new StringBuilder();
//		rScript.append("unique(").append(this.frame.getTableName()).append("[")
//			.append(this.frame.getFilterString()).append(",{V0=").append(selectors[0])
//			.append("; V1=").append(selectors[1]).append("; list(")
//			.append(selectors[0]).append("=V0, ").append(selectors[1]).append("=V1)}])");
//		
//		return rScript.toString();
//	}
//
//	/**
//	 * Return the distinct values of a node
//	 * @param nodeName
//	 * @return
//	 */
//	private List<Object[]> createNodeRs(String nodeName) {
//		String rScript = createQueryString(nodeName);
//		return this.frame.getBulkDataRow(rScript, new String[]{nodeName});
//	}
//
//	/**
//	 * Create the query string to get distinct values of a node
//	 * @param selector
//	 * @return
//	 */
//	private String createQueryString(String selector) {
//		StringBuilder rScript = new StringBuilder();
//		rScript.append("unique(").append(this.frame.getTableName()).append("[")
//			.append(this.frame.getFilterString()).append(",{V0=").append(selector)
//			.append("; list(").append(selector).append("=V0)}])");
//		
//		return rScript.toString();
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
//}
