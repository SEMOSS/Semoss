package prerna.reactor.export;

import java.awt.Color;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.lang.ArrayUtils;

import prerna.engine.api.IHeadersDataRow;
import prerna.ui.helpers.TypeColorShapeTable;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Constants;

public class GraphFormatter extends AbstractFormatter {

	// the nodes list to return
	private List<GraphFormatterMap> nodesMapList;
	// the edges list to return
	private List<GraphFormatterMap> edgesMapList;

	/*
	 * These are the options that the FE can define for graph output
	 * 1) connectionsMap
	 * 		Example : {upstreamNode1 -> [downstreamNode1 , downstreamnode2, .. etc]
	 * 		This defines the up node to its list of downnodes
	 * 		The same node can be an up node in some situations and down nodes in orders
	 * 
	 * 2) nodePropertiesMap
	 * 		Example : {node -> [prop1, prop2, ... propN]
	 * 		This defines the properties to add into the prop hash for a given node
	 * 
	 * 3) edgePropertiesMap
	 * 		Example: {upNode1.downNode1 -> [prop1, prop2, ... propN]
	 * 		This defines the properties to put inbetween a given relationship that 
	 * 		must already be defined in teh connectionsMap
	 * 
	 */
	private Map<String, List<String>> connectionsMap;
	private Map<String, List<String>> nodePropertiesMap;
	private Map<String, List<String>> edgePropertiesMap;
	private Map<String, Color> colorsMap;
	private List<String> nodeList;
	private Map<String, String> aliasMap;

	// this is used to make sure we do not add vertices twice
	protected Map<String, Set<String>> vertLabelUniqueValues;

	private List<Integer[]> indexConnections;

	// used for edge map
	public static final String EDGES = "edges";
	private static final String SOURCE = "source";
	private static final String TARGET = "target";

	// used for node map
	public static final String NODES = "nodes";
	private static final String VERTEX_TYPE_PROPERTY = "VERTEX_TYPE_PROPERTY";
	private static final String VERTEX_COLOR_PROPERTY = "VERTEX_COLOR_PROPERTY";
	private static final String VERTEX_LABEL_PROPERTY = "VERTEX_LABEL_PROPERTY";

	private static final String PROP_HASH = "propHash";
	public static final String GRAPH_META = "graphMeta";
	static final String URI = "uri";

	public GraphFormatter() {
		this.nodesMapList = new ArrayList<GraphFormatterMap>();
		this.edgesMapList = new ArrayList<GraphFormatterMap>();
		this.vertLabelUniqueValues = new HashMap<String, Set<String>>();
	}

	@Override
	public void addData(IHeadersDataRow nextData) {
		String[] headers = nextData.getHeaders();
		// also get the raw headers and see if you can try it
		String[] rawHeaders = nextData.getRawHeaders();
		Object[] values = nextData.getValues();
		if (this.indexConnections == null) {
			determineConnectionsIndex(headers, rawHeaders);
		}
		
		// process the nodes
		processNodes(headers, values);
		// process the relationships
		// if no connections, dont do any of this
		if(this.indexConnections != null) {
			processRelationships(headers, values);
		}
	}

	private void processNodes(String[] headers, Object[] values) {
		// add the node information
		
		// If node list is not provided, use all headers as nodes
		if(nodeList == null) {
			nodeList = Arrays.asList(headers);
		}
		for (int i = 0; i < nodeList.size(); i++) {
			String vertexType = nodeList.get(i);
			Object vertexLabel = values[ArrayUtils.indexOf(headers, vertexType)];
			if(vertexLabel == null) {
				continue;
			}
			vertexType = getVertexType(vertexType);
			String uri = vertexType + "/" + vertexLabel;

			// only process new nodes once
			if(alreadyProcessedId(this.nodesMapList, uri)) {
				continue;
			}
			
			// store the meta data around each node
			// and also ensure we do not add nodes twice unnecessarily
			if (this.vertLabelUniqueValues.containsKey(vertexType)) {
				Set<String> processedNodes = (Set<String>) this.vertLabelUniqueValues.get(vertexType);
				if (!processedNodes.contains(vertexLabel.toString())) {
					processedNodes.add(vertexLabel.toString());
				}
			} else {
				Set<String> processedNodes = new HashSet<String>();
				processedNodes.add(vertexLabel.toString());
				this.vertLabelUniqueValues.put(vertexType, processedNodes);
			}

			GraphFormatterMap nodeMap = new GraphFormatterMap();
			Color color = (this.colorsMap != null && this.colorsMap.get(vertexType) != null) 
					? this.colorsMap.get(vertexType)
					: TypeColorShapeTable.getInstance().getColor(vertexType, vertexLabel.toString());
			nodeMap.put(Constants.VERTEX_COLOR, getRgb(color));
			nodeMap.put(Constants.VERTEX_TYPE, vertexType);
			nodeMap.put(Constants.VERTEX_NAME, vertexLabel);
			nodeMap.put(URI, uri);

			Map<String, Object> propHash = new HashMap<String, Object>();
			if (this.nodePropertiesMap != null && !this.nodePropertiesMap.isEmpty()) {
				if (nodePropertiesMap.containsKey(vertexType)) {
					List<String> propertyTypes = nodePropertiesMap.get(vertexType);
					for (String property : propertyTypes) {
						int propertyIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(headers, property);
						if (propertyIndex < values.length) {
							propHash.put(property, values[propertyIndex]);
						}
					}

				}
			}
			nodeMap.put(PROP_HASH, propHash);
			this.nodesMapList.add(nodeMap);
		}
	}

	private void processRelationships(String[] headers, Object[] values) {
		// add the relationship information

		// we will use the index connections to determine the header locations
		// instead of calculating this every time
		if (this.indexConnections != null && !this.indexConnections.isEmpty()) {
			for (Integer[] index : indexConnections) {
				GraphFormatterMap edgeMap = new GraphFormatterMap();
				int upHeaderIndex = index[0];
				int downHeaderIndex = index[1];
				if (upHeaderIndex >= 0 && downHeaderIndex >= 0) {
					Object sValue = values[upHeaderIndex];
					if(sValue == null) {
						continue;
					}
					Object tValue = values[downHeaderIndex];
					if(tValue == null) {
						continue;
					}
					String source = getVertexType(headers[upHeaderIndex]) + "/" + sValue;
					String target = getVertexType(headers[downHeaderIndex]) + "/" + tValue;
					String uri = source + ":" + target;
					edgeMap.put(SOURCE, source);
					edgeMap.put(TARGET, target);
					edgeMap.put(URI, uri);
					
					// only process new edges
					if(alreadyProcessedId(this.edgesMapList, uri)) {
						continue;
					}

					// Add relationship properties col.col = ["col"]
					Map<String, Object> propHash = new HashMap<String, Object>();
					if (edgePropertiesMap != null && !edgePropertiesMap.isEmpty()) {
						for (String edgeLabel : edgePropertiesMap.keySet()) {
							// validate syntax col.col
							if (edgeLabel.contains(".")) {
								String[] split = edgeLabel.split("\\.");
								if (split.length > 0) {
									String startNode = split[0];
									String endNode = split[1];
									// check if edge exists in connections
									int startNodeIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(headers, startNode);
									int endNodeIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(headers, endNode);
									if (validEdgeLabel(startNodeIndex, endNodeIndex)) {
										List<String> properties = edgePropertiesMap.get(edgeLabel);
										for (String property : properties) {
											int propertyIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(headers, property);
											propHash.put(property, values[propertyIndex]);
										}
									}

								}
							}

						}
					}

					edgeMap.put(PROP_HASH, propHash);
					this.edgesMapList.add(edgeMap);
				}
			}
		}
	}

	private boolean validEdgeLabel(int startNodeIndex, int endNodeIndex) {
		if (startNodeIndex >= 0 && endNodeIndex >= 0) {
			for (Integer[] edge : indexConnections) {
				int edgeStartIndex = edge[0].intValue();
				int edgeEndIndex = edge[1].intValue();
				if (edgeStartIndex == startNodeIndex && edgeEndIndex == endNodeIndex) {
					return true;
				}
			}
		}
		return false;
	}
	
	private String getVertexType(String vertexType) {
		if(this.aliasMap != null && this.aliasMap.containsKey(vertexType)) {
			return this.aliasMap.get(vertexType);
		}
		// cant find it, return the original
		return vertexType;
	}

	/**
	 * Gets graph metadata
	 * "col" : unique instance count
	 */
	protected HashMap<String, Object> getGraphMeta() {
		HashMap<String, Object> meta = new HashMap<String, Object>();
		for(String vertexType : vertLabelUniqueValues.keySet()) {
			Set<String> values = vertLabelUniqueValues.get(vertexType);
			meta.put(vertexType, values.size());
		}
		return meta;
	}

	private void determineConnectionsIndex(String[] headers, String [] rawHeaders) {
		// loop through and find the indices to grab for each connection we want
		this.indexConnections = new ArrayList<Integer[]>();
		if (connectionsMap != null && !this.connectionsMap.isEmpty()) {
			for (String upstreamHeader : this.connectionsMap.keySet()) {
				// find the up header index
				int upHeaderIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(headers, upstreamHeader);
				if(upHeaderIndex < 0)
					upHeaderIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(rawHeaders, upstreamHeader);
				List<String> downstreamHeaderList = this.connectionsMap.get(upstreamHeader);
				for (String downstreamHeader : downstreamHeaderList) {
					// find the down header index
					int downHeaderIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(headers, downstreamHeader);
					// try it in raw headers as well
					if(downHeaderIndex < 0)
						downHeaderIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(rawHeaders, downstreamHeader);

					this.indexConnections.add(new Integer[] { upHeaderIndex, downHeaderIndex });
				}
			}
		}
	}

	@Override
	public Object getFormattedData() {
		Map<String, Object> formattedData = new HashMap<String, Object>();
		formattedData.put(NODES, nodesMapList);
		formattedData.put(EDGES, edgesMapList);
		formattedData.put(GRAPH_META, getGraphMeta());
		return formattedData;
	}

	@Override
	public void clear() {
		this.nodesMapList.clear();
		this.edgesMapList.clear();
	}

	@Override
	public String getFormatType() {
		return "GRAPH";
	}

	@SuppressWarnings("unchecked")
	@Override
	public void setOptionsMap(Map<String, Object> optionsMap) {
		super.setOptionsMap(optionsMap);
		String connections = (String) this.optionsMap.get("connections");
		if (connections != null && !connections.isEmpty()) {
			this.connectionsMap = generateEdgeHashFromStr(connections);
		}
		String nodeProperties = (String) this.optionsMap.get("nodeProperties");
		if (nodeProperties != null && !nodeProperties.isEmpty()) {
			this.nodePropertiesMap = generateEdgeHashFromStr(nodeProperties);
		}
		String edgeProperties = (String) this.optionsMap.get("edgeProperties");
		if (edgeProperties != null && !edgeProperties.isEmpty()) {
			this.edgePropertiesMap = generateEdgeHashFromStr(edgeProperties);
		}
		String alias = (String) this.optionsMap.get("alias");
		if(alias != null && !alias.isEmpty()) {
			this.aliasMap = generateAliasMapFromStr(alias);
		}
		String nodes = (String) this.optionsMap.get("nodes");
		if (nodes != null && !nodes.isEmpty()) {
			this.nodeList = generateNodeListFromStr(nodes);
		}
		Object colors = this.optionsMap.get("colors");
		if (colors != null) {
			this.colorsMap = (Map<String, Color>) colors;
		}
	}
	
	private List<String> generateNodeListFromStr(String nodes)
	{
		//Generates list of nodes from string option
		List<String> nList = new ArrayList<String>();
		nList = Arrays.asList(nodes.split(";"));
		return nList;
	}
	
	private Map<String, String> generateAliasMapFromStr(String aliasStr) {
		Map<String, String> aliasMap = new Hashtable<String, String>();
		// example string is UpSys.System;DownSys.System
		// we split on ";"
		// [UpSys.System   ,   DownSys.System]
		// then we split on "."
		// and we know the matching is
		// UpSys -> System
		// and
		// DownSys -> System
		String[] aliasArr = aliasStr.split(";");
		for(String aliasPair : aliasArr) {
			if(aliasPair.contains(".")) {
				String[] aliasPairArr = aliasPair.split("\\.");
				aliasMap.put(aliasPairArr[0], aliasPairArr[1]);
			}
		}
		return aliasMap;
	}

	public static Map<String, List<String>> generateEdgeHashFromStr(String edgeHashStr) {
		Map<String, List<String>> edgeHash = new Hashtable<String, List<String>>();
		// each path is separated by a semicolon
		String[] paths = edgeHashStr.split(";");
		for(String path : paths) {
			if(path.contains(".")) {
				String[] pathVertex = path.split("\\.");
				// we start at index 1 and take the index prior for ease of looping
				for(int i = 1; i < pathVertex.length; i++) {
					String startNode = pathVertex[i-1];
					if(startNode.contains("__")) {
						startNode = startNode.split("__")[1];
					}
					String endNode = pathVertex[i];
					if(endNode.contains("__")) {
						endNode = endNode.split("__")[1];
					}
					
					// update the edge hash correctly
					Set<String> downstreamNodes = null;
					Vector<String> list = new Vector<String>();

					if (edgeHash.containsKey(startNode)) {
						downstreamNodes = new HashSet<String>(edgeHash.get(startNode));
						downstreamNodes.add(endNode);
					} else {
						downstreamNodes = new HashSet<String>();
						downstreamNodes.add(endNode);
					}
					list.addAll(downstreamNodes);
					edgeHash.put(startNode, list);
				}
			} else {
				// ugh... when would this happen?
			}
		}
		return edgeHash;
	}
	
	private String getRgb(Color c) {
		return c.getRed() + "," + c.getGreen() + "," +c.getBlue();
	}
	
	private boolean alreadyProcessedId(List<GraphFormatterMap> list, String newId) {
		int size = list.size();
		for(int i = 0; i < size; i++) {
			if(list.get(i).equals(newId)) {
				return true;
			}
		}
		return false;
	}
}

// so we can compare the string uri to the map we are passing in
class GraphFormatterMap extends HashMap<String, Object> {
	
	@Override
	public boolean equals(Object o) {
		if(o instanceof String) {
			if(this.containsKey(GraphFormatter.URI)) {
				if(o.equals(this.get(GraphFormatter.URI))) {
					return true;
				} else {
					return false;
				}
			}
		}
		return super.equals(o);
	}
	
}
