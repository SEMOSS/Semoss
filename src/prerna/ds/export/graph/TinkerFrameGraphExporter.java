package prerna.ds.export.graph;

import java.awt.Color;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import prerna.ds.TinkerFrame;
import prerna.ui.helpers.TypeColorShapeTable;
import prerna.util.Constants;

public class TinkerFrameGraphExporter implements IGraphExporter{

	// the tinker frame we are operating on
	private TinkerFrame tf;
	private TinkerGraph g;
	// the edge iterator
	private GraphTraversal<Edge, Edge> edgesIt;
	// the vert iterator
	private GraphTraversal<Vertex, Vertex> vertsIt;
	
	public TinkerFrameGraphExporter(TinkerFrame tf) {
		this.tf = tf;
		this.g = tf.g;
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
		edgeMap.put("source", e.outVertex().property(TinkerFrame.TINKER_TYPE).value() + "/" + e.outVertex().property(TinkerFrame.TINKER_NAME).value().toString());
		edgeMap.put("target", e.inVertex().property(TinkerFrame.TINKER_TYPE).value() + "/" + e.inVertex().property(TinkerFrame.TINKER_NAME).value().toString());

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
		String type = v.property(TinkerFrame.TINKER_TYPE).value() + "";
		
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
		this.edgesIt = this.g.traversal().E()
				.not(__.or
						(__.has(TinkerFrame.TINKER_TYPE, TinkerFrame.TINKER_FILTER), 
						 __.bothV().in().has(TinkerFrame.TINKER_TYPE, TinkerFrame.TINKER_FILTER), 
						 __.V().has(TinkerFrame.PRIM_KEY, true)
						 )
					);
	}
	
	/**
	 * Generate vertices iterator
	 */
	private void createVertsIt() {
		// get all vertices that
		// 1) are not the filtered vertex
		// 2) do not have an in edge to the filtered vertex
		// 3) not prim key
		this.vertsIt = this.g.traversal().V()
				.not(__.or(
						__.has(TinkerFrame.TINKER_TYPE, TinkerFrame.TINKER_FILTER), 
						__.in().has(TinkerFrame.TINKER_TYPE, TinkerFrame.TINKER_FILTER), 
						__.has(TinkerFrame.PRIM_KEY, true)
						)
					);
	}
	
}
