package prerna.ds.export.graph;

import java.awt.Color;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import prerna.ds.h2.H2Frame;
import prerna.ui.helpers.TypeColorShapeTable;
import prerna.util.Constants;

public class FlatRdbmsGraphExporter extends RdbmsGraphExporter {

	// we need to keep track of a full row of the grid
	// in order to return correctly the relationships to their row number
	private Object[] rowEdge;
	private int curRowEdgeIndex = 0;
	private String[] frameHeaders = null;
	private String[] frameHeaderAlias = null;
	
	public FlatRdbmsGraphExporter() {
		
	}
	
	public FlatRdbmsGraphExporter(H2Frame frame) {
		this.frame = frame;
		this.frameHeaders = this.frame.getColumnHeaders();
		this.frameHeaderAlias = this.frame.getColumnAliasName();
		// this will store in aliasMap an alias -> set of columns
		processDupHeaders(this.frameHeaders, this.frameHeaderAlias);
	}
	
	protected void processDupHeaders(String[] columnHeaders, String[] columnAliasName) {
		this.aliasMap = generateDupAliasMap(columnHeaders, columnAliasName);
		// account for the additional column we need to add for each row of data
		Set<String> aliasSet = new HashSet<String>();
		aliasSet.add(ROW_ID);
		this.aliasMap.put(ROW_ID, aliasSet);
		// need to override the vertices iterator to only use the alias
		this.verticesIterator = aliasMap.keySet().iterator();
	}
	
	@Override
	public boolean hasNextEdge() {
		// first time, everything is null
		if(this.edgeRs == null && this.frameHeaders.length > 1) {
			this.edgeRs = createEdgeRs(this.frameHeaders);
			// so we made it, run this again to 
			// see if this relationship has values to return
			return hasNextEdge();
		} else {
			// if we are here and the edge rs is still null
			// that means there are no relationships
			// ... seems like a dumb graph
			if(this.edgeRs == null) {
				return false;
			}
			if(this.rowEdge == null){
				try {
					// does the iterator have another row
					boolean hasNext = this.edgeRs.next();
					if(hasNext) {
						// yup, flush it out
						this.rowEdge = new Object[this.frameHeaders.length+1];
						for(int i = 0; i <= this.frameHeaders.length; i++) {
							this.rowEdge[i] = this.edgeRs.getObject(i+1);
						}
						// reset the index we are using for each get next edge
						this.curRowEdgeIndex = 0;
						return true;
					} else {
						// we dont have anything
						this.edgeRs.close();
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}

				// we only get here if in the above
				// we closed the rs
				return false;
			} else if(this.rowEdge.length == this.curRowEdgeIndex + 1) {
				// we went through the entire row
				// null out the rowEdge
				// and flush out the next edgeRs
				this.rowEdge = null;
				return hasNextEdge();
			} else {
				// we have more things to flush out from the current row edge
				// so return true
				return true;
			}
		}
	}

	@Override
	public Map<String, Object> getNextEdge() {
		// TODO: Figure out how to do edge properties and stuff
		// till then, this is a really easy return
		Object sourceVal = this.rowEdge[this.curRowEdgeIndex];
		Object targetVal = this.rowEdge[this.rowEdge.length-1];

		// if we still have a null
		if(sourceVal == null) {
			sourceVal = "EMPTY";
		}
		if(targetVal == null) {
			targetVal = "EMPTY";
		}

		String source = this.frameHeaderAlias[this.curRowEdgeIndex] + "/" + sourceVal;
		String target = ROW_ID + "/" + targetVal;

		Map<String, Object> relationshipMap = new HashMap<String, Object>();
		relationshipMap.put("source", source);
		relationshipMap.put("target", target);
		relationshipMap.put("uri", source + ":" + target);

		// also push empty edge properties
		Map<String, Object> propMap = new HashMap<String, Object>();
		relationshipMap.put("propHash", propMap);

		// update row index
		this.curRowEdgeIndex++;
		
		return relationshipMap;
	}

	@Override
	public boolean hasNextVert() {
		// first time, everything is null
		if(this.nodeRs == null && verticesIterator.hasNext()) {
			this.curVertex = verticesIterator.next();
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
		
		/*
		 * For the vertices iterator
		 * in the method generateDupAliasMap
		 * we already switch to using the alias
		 * so we do not need to sue this.aliasCurVertex
		 * and the method to get the unique set of vertices
		 * takes this into consideration
		 */
		Object nodeVal = null;
		try {
			nodeVal = this.nodeRs.getObject(1);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		// if we still have a null
		if(nodeVal == null) {
			nodeVal = "EMPTY";
		}

		String node = this.curVertex + "/" + nodeVal;

		Map<String, Object> vertexMap = new HashMap<String, Object>();
		vertexMap.put("uri", node);
		vertexMap.put(Constants.VERTEX_NAME, nodeVal);
		vertexMap.put(Constants.VERTEX_TYPE, this.curVertex);

		// need to add in color
		Color color = TypeColorShapeTable.getInstance().getColor(this.curVertex, nodeVal.toString());
		vertexMap.put("VERTEX_COLOR_PROPERTY", IGraphExporter.getRgb(color));

		// also push empty vertex properties
		Map<String, Object> propMap = new HashMap<String, Object>();
		vertexMap.put("propHash", propMap);

		// add to the meta count
		addVertCount(this.curVertex);
		
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
		// add the row num
		sql.append(", ROWNUM() ");
		sql.append(" FROM ").append(frame.getTableName());
		String sqlFilter = frame.getSqlFilter();
		if(sqlFilter != null && !sqlFilter.isEmpty()) {
			sql.append(frame.getSqlFilter());
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
		StringBuilder sql = new StringBuilder();
		// ACCOUNT FOR FLAT ID SINCE WE DO NOT HAVE A PROPER EDGE HASH
		if(selector.equals(ROW_ID)) {
			sql.append("SELECT DISTINCT ROWNUM() AS ").append(ROW_ID);
			sql.append(" FROM ").append(frame.getTableName());
			String sqlFilter = frame.getSqlFilter();
			if(sqlFilter != null && !sqlFilter.isEmpty()) {
				sql.append(frame.getSqlFilter());
			}
		} 
		// SAME AS NORMAL RDBMS GRAPH EXPORTER
		else if(this.aliasMap.containsKey(selector)) {
			sql.append("SELECT DISTINCT ");
			Set<String> actualSelectors = aliasMap.get(selector);
			boolean first = true;
			for(String select : actualSelectors) {
				if(first) {
					sql.append(select).append(" AS ").append(selector)
						.append(" FROM ").append(frame.getTableName());
					String sqlFilter = frame.getSqlFilter();
					if(sqlFilter != null && !sqlFilter.isEmpty()) {
						sql.append(frame.getSqlFilter());
					}
					first = false;
				} else {
					sql.append(" UNION SELECT DISTINCT ").append(select).append(" AS ").append(selector)
						.append(" FROM ").append(frame.getTableName());
					String sqlFilter = frame.getSqlFilter();
					if(sqlFilter != null && !sqlFilter.isEmpty()) {
						sql.append(frame.getSqlFilter());
					}
				}
			}
		} else {
			sql.append("SELECT DISTINCT ");
			sql.append(selector).append(" ");
			sql.append(" FROM ").append(frame.getTableName());
			String sqlFilter = frame.getSqlFilter();
			if(sqlFilter != null && !sqlFilter.isEmpty()) {
				sql.append(frame.getSqlFilter());
			}
		}
		return sql.toString();
	}
}
