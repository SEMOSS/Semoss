package prerna.ds.export.gexf;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import prerna.ds.h2.H2Frame;

public class RdbmsGexfIterator extends AbstractGexfIterator {

	private H2Frame dataframe;

	private ResultSet nodeRs;
	private String[] nodeRsColumns;

	private ResultSet edgeRs;
	private String[] edgeRsColumns;


	public RdbmsGexfIterator(H2Frame dataframe, String nodeMap, String edgeMap, Map<String, String> aliasMap) {
		super(nodeMap, edgeMap, aliasMap);
		this.dataframe = dataframe;
	}

	@Override
	public boolean hasNextNode() {
		try {
			if(this.nodeRs == null) {
				if(this.nodeMapSplit == null || this.nodeMapSplit.length == 0) {
					return false;
				}
				
				String nodeString = this.nodeMapSplit[this.nodeIndex];
				this.nodeRsColumns = nodeString.split(",");
				this.nodeRs = this.dataframe.execQuery( createQueryString(this.nodeRsColumns) );
				this.nodeIndex++;
				// use the method itself to execute the first hasNext
				return hasNextNode();
			} else {
				if(this.nodeRs.next()) {
					return true;
				} else {
					// we finished going through for all the nodes
					if(this.nodeIndex >= this.nodeMapSplit.length) {
						return false;
					} else {
						String nodeString = this.nodeMapSplit[this.nodeIndex];
						this.nodeRsColumns = nodeString.split(",");
						this.nodeRs = this.dataframe.execQuery( createQueryString(this.nodeRsColumns) );
						this.nodeIndex++;
						// use the method itself to execute the first hasNext
						return hasNextNode();
					}
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public String getNextNodeString() {
		StringBuilder node = new StringBuilder("<node ");
		try {
			// get the node name
			Object nodeName = this.nodeRs.getObject(1);
			// define the node id
			node.append("id=\"").append(nodeName).append("\" value=\"").append(nodeName).append("\">");
			// define the properties
			// default property on every node is the type
			node.append("<attvalues>");
			String typeName = this.nodeRsColumns[0];
			if(this.aliasMap.containsKey(typeName)) {
				typeName = this.aliasMap.get(typeName);
			}
			node.append("<attvalue for=\"type\" value=\"").append(typeName).append("\"/>");
			
			// add all the other properties
			for(int i = 1; i < this.nodeRsColumns.length; i++) {
				String propName = this.nodeRsColumns[i];
				if(this.aliasMap.containsKey(propName)) {
					propName = this.aliasMap.get(propName);
				}
				
				Object prop = this.nodeRs.getObject(i+1);
				if(prop != null) {
					if(prop instanceof Double || prop instanceof Integer) {
						node.append("<attvalue for=\"").append(propName).append("\" value=").append(prop).append("/>");
					} else {
						node.append("<attvalue for=\"").append(propName).append("\" value=\"").append(prop).append("\"/>");
					}
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		node.append("</attvalues></node>");
		return node.toString();
	}

	@Override
	public boolean hasNextEdge() {
		try {
			if(this.edgeRs == null) {
				if(this.edgeMapSplit == null || this.edgeMapSplit.length == 0) {
					return false;
				}
				
				String edgeString = this.edgeMapSplit[this.edgeIndex];
				this.edgeRsColumns = edgeString.split(",");
				this.edgeRs = this.dataframe.execQuery( createQueryString(this.edgeRsColumns) );
				this.edgeIndex++;
				// use the method itself to execute the first hasNext
				return hasNextEdge();
			} else {
				if(this.edgeRs.next()) {
					return true;
				} else {
					// we finished going through for all the nodes
					if(this.edgeIndex >= this.edgeMapSplit.length) {
						return false;
					} else {
						String edgeString = this.edgeMapSplit[this.edgeIndex];
						this.edgeRsColumns = edgeString.split(",");
						this.edgeRs = this.dataframe.execQuery( createQueryString(this.edgeRsColumns) );
						this.edgeIndex++;
						// use the method itself to execute the first hasNext
						return hasNextEdge();
					}
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public String getNextEdgeString() {
		StringBuilder edge = new StringBuilder("<edge ");
		try {
			// grab the source and target
			Object sourceName = this.edgeRs.getObject(1);
			Object targetName = this.edgeRs.getObject(2);

			// the id is the combination of the source and target
			edge.append("id=\"").append(sourceName).append("+++").append(targetName).append("\"");
			// define the source and target
			edge.append(" source=\"").append(sourceName).append("\" target=\"").append(targetName).append("\">");
			
			if(this.edgeRsColumns.length > 2) {
				edge.append("<attvalues>");
			}
			
			// add the edge properties
			for(int i = 2; i < this.edgeRsColumns.length; i++) {
				String propName = this.edgeRsColumns[i];
				if(this.aliasMap.containsKey(propName)) {
					propName = this.aliasMap.get(propName);
				}
				
				Object prop = this.edgeRs.getObject(i+1);
				if(prop != null) {
					if(prop instanceof Double || prop instanceof Integer) {
						edge.append("<attvalue for=\"").append(propName).append("\" value=").append(prop).append("/>");
					} else {
						edge.append("<attvalue for=\"").append(propName).append("\" value=\"").append(prop).append("\"/>");
					}
				}
			}
			
			if(this.edgeRsColumns.length > 2) {
				edge.append("</attvalues>");
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}

		edge.append("</edge>");
		return edge.toString();
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
		sql.append(" FROM ").append(dataframe.getName());
		return sql.toString();
	}
}
