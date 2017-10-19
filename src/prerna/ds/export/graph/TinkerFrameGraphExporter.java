package prerna.ds.export.graph;

import java.awt.Color;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.TinkerFrame;
import prerna.ui.helpers.TypeColorShapeTable;
import prerna.util.Constants;

public class TinkerFrameGraphExporter extends AbstractGraphExporter{

	// the tinker frame we are operating on
	private TinkerGraph g;
	private OwlTemporalEngineMeta meta;
	// the edge iterator
	private GraphTraversal<Edge, Edge> edgesIt;
	// the vert iterator
	private GraphTraversal<Vertex, Vertex> vertsIt;
	
	public TinkerFrameGraphExporter(TinkerFrame tf) {
		this.g = tf.g;
		this.meta = tf.getMetaData();
	}
	
	/**
	 * Boolean if there are more edges to return
	 * @return
	 */
	@Override
	public boolean hasNextEdge() {
		if(this.edgesIt == null) {
			createEdgesIt();
		}
		return this.edgesIt.hasNext();
	}
	
	@Override
	public Map<String, Object> getNextEdge() {
		if(this.edgesIt == null) {
			createEdgesIt();
		}
		
		// map representing the edge
		Map<String, Object> edgeMap = new HashMap<String, Object>();
		
		Edge e = this.edgesIt.next();
		// get the edge unique id
		edgeMap.put("uri", e.property(TinkerFrame.TINKER_ID).value().toString());
		// add the source and target
		edgeMap.put("source", getNodeAlias(e.outVertex().property(TinkerFrame.TINKER_TYPE).value() + "") + "/" + getNodeAlias(e.outVertex().property(TinkerFrame.TINKER_NAME).value() + ""));
		edgeMap.put("target", getNodeAlias(e.inVertex().property(TinkerFrame.TINKER_TYPE).value() + "") + "/" + getNodeAlias(e.inVertex().property(TinkerFrame.TINKER_NAME).value() + ""));

		// also push edge properties
		Map<String, Object> propMap = new HashMap<String, Object>();
		
		// need to add edge properties
		Iterator<Property<Object>> edgeProperties = e.properties();
		while(edgeProperties.hasNext()) {
			Property<Object> prop = edgeProperties.next();
			String propName = prop.key();
			if(!propName.equals(TinkerFrame.TINKER_ID) && !propName.equals(TinkerFrame.TINKER_NAME) && !propName.equals(TinkerFrame.TINKER_TYPE)) {
				propMap.put(propName, prop.value());
			}
		}
		edgeMap.put("propHash", propMap);

		// return the edge map
		return edgeMap;
	}
	
	
	/**
	 * Boolean if there are more vertices to return
	 * @return
	 */
	@Override
	public boolean hasNextVert() {
		if(this.vertsIt == null) {
			createVertsIt();
		}
		return this.vertsIt.hasNext();
	}
	
	@Override
	public Map<String, Object> getNextVert() {
		if(this.vertsIt == null) {
			createVertsIt();
		}
		
		// map representing the vertex
		Map<String, Object> vertexMap = new HashMap<String, Object>();
		
		Vertex v = this.vertsIt.next();
		
		// add the vertex unique id
		Object value = v.property(TinkerFrame.TINKER_NAME).value();
		String type = getNodeAlias(v.property(TinkerFrame.TINKER_TYPE).value() + "");
		
		vertexMap.put("uri", type + "/" + value);
		vertexMap.put(Constants.VERTEX_TYPE, type);
		vertexMap.put(Constants.VERTEX_NAME, value);
		
		// also push vertex properties
		Map<String, Object> propMap = new HashMap<String, Object>();
		
		Iterator<VertexProperty<Object>> vProperties = v.properties();
		while(vProperties.hasNext()) {
			VertexProperty<Object> prop = vProperties.next();
			String propName = prop.key();
			if(!propName.equals(TinkerFrame.TINKER_ID) && !propName.equals(TinkerFrame.TINKER_NAME) && !propName.equals(TinkerFrame.TINKER_TYPE)) {
				propMap.put(propName, prop.value());
			}
		}
		vertexMap.put("propHash", propMap);
		
		// need to add in color
		Color color = TypeColorShapeTable.getInstance().getColor(type, value.toString());
		vertexMap.put("VERTEX_COLOR_PROPERTY", IGraphExporter.getRgb(color));
		
		// add to the meta count
		addVertCount(type);
		
		return vertexMap;
	}
	
	/**
	 * Generate edges iterator
	 */
	private void createEdgesIt() {
		// get all edges that
		// 1) isn't the filtered edge
		// 2) neither vertex have an incoming filtered edge
		// 3) no vertex is a prim key
		this.edgesIt = this.g.traversal().E();
	}
	
	/**
	 * Generate vertices iterator
	 */
	private void createVertsIt() {
		// get all vertices that
		// 1) are not the filtered vertex
		// 2) do not have an in edge to the filtered vertex
		// 3) not prim key
		this.vertsIt = this.g.traversal().V();
	}
	
	/**
	 * For some of the nodes that have not been given an alias
	 * If there is an implicit alias on it (a physical name that matches an existing name)
	 * We will use that
	 * @param node
	 * @return
	 */
	private String getNodeAlias(String node) {
		if(meta == null) {
			return node;
		}
		return meta.getPhysicalName(node);
	}

	@Override
	public Object getData() {
		Map<String, Object> formattedData = new HashMap<String, Object>();
		List<Map<String, Object>> nodesMapList = new Vector<Map<String, Object>>();
		while(hasNextVert()) {
			nodesMapList.add(getNextVert());
		}
		List<Map<String, Object>> edgesMapList = new Vector<Map<String, Object>>();
		while(hasNextEdge()) {
			edgesMapList.add(getNextEdge());
		}
		formattedData.put("nodes", nodesMapList);
		formattedData.put("edges", edgesMapList);
		formattedData.put("graphMeta", getVertCounts());
		return formattedData;
	}
}
