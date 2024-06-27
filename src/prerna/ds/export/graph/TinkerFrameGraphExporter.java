package prerna.ds.export.graph;

import java.awt.Color;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.TinkerFrame;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.ui.helpers.TypeColorShapeTable;
import prerna.util.Constants;
import prerna.util.Constants;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class TinkerFrameGraphExporter extends AbstractGraphExporter {

	private static final Logger classLogger = LogManager.getLogger(TinkerFrameGraphExporter.class);

	// the tinker frame we are operating on
	private TinkerFrame tf;
	private TinkerGraph g;
	private OwlTemporalEngineMeta meta;
	private Map<String, Color> nodeColorMap;
	// the edge iterator
	private GraphTraversal<Edge, Edge> edgesIt;
	// the vert iterator
	private GraphTraversal<Vertex, Vertex> vertsIt;
	
	// to keep vertices unique
	private Set<String> vertSet;
	
	public TinkerFrameGraphExporter(TinkerFrame tf) {
		this.tf = tf;
		this.g = tf.g;
		this.meta = tf.getMetaData();

		// because i can't get the edge traversal properly :(
		this.vertSet = new HashSet<String>();
	}
	
	public TinkerFrameGraphExporter(TinkerFrame tf, Map<String, Color> colorMap) {
		this.tf = tf;
		this.g = tf.g;
		this.meta = tf.getMetaData();
		this.nodeColorMap = colorMap;
		// because i can't get the edge traversal properly :(
		this.vertSet = new HashSet<String>();
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
		if(this.vertsIt.hasNext()) {
			return true;
		} else {
			try {
				this.vertsIt.close();
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
			return false;
		}
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
		String type = getNodePhysicalType(v.property(TinkerFrame.TINKER_TYPE).value() + "");
		String uri =  type + "/" + value;
		vertexMap.put("uri", uri);
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
		Color color = (this.nodeColorMap != null && this.nodeColorMap.get(type) != null)
				? this.nodeColorMap.get(type)
				: TypeColorShapeTable.getInstance().getColor(type, value.toString());
		vertexMap.put(Constants.VERTEX_COLOR, IGraphExporter.getRgb(color));
		
		// add to the meta count
		addVertCount(type);
		
		// add to set for filtering out some annoying edges
		this.vertSet.add(uri);
		
		return vertexMap;
	}
	
	/**
	 * Generate vertices iterator
	 */
	private void createVertsIt() {
		this.vertsIt = this.g.traversal().V();
		
		boolean hasFilter = false;
		GenRowFilters filterGrs = tf.getFrameFilters();
		List<GraphTraversal> unionT = new Vector<GraphTraversal>();
		if(!filterGrs.isEmpty()) {
			hasFilter = true;
			// we have filters to consider
			List<String> vertexNames = this.meta.getOrderedAliasOrUniqueNames();
			// this is because we can have 2 meta that point
			// to the same physical
			Set<String> addedPhysical = new HashSet<String>();
			for(String v : vertexNames) {
				String physicalNodeType = getNodePhysicalType(v);
				if(addedPhysical.contains(physicalNodeType)) {
					continue;
				}
				addedPhysical.add(physicalNodeType);

				GraphTraversal traversal = __.has(TinkerFrame.TINKER_TYPE, physicalNodeType);
				List<SimpleQueryFilter> colFilters = filterGrs.getAllSimpleQueryFiltersContainingColumn(v);
				if(colFilters.isEmpty()) {
					unionT.add(traversal);
					continue;
				}
				for(SimpleQueryFilter filter : colFilters) {
					SimpleQueryFilter.FILTER_TYPE filterType = filter.getSimpleFilterType();
					NounMetadata lComp = filter.getLComparison();
					NounMetadata rComp = filter.getRComparison();
					String comp = filter.getComparator();

					if(filterType == SimpleQueryFilter.FILTER_TYPE.COL_TO_VALUES) {
						// here, lcomp is the column and rComp is a set of values
						processFilterColToValues(traversal, lComp, rComp, comp);
					} else if(filterType == SimpleQueryFilter.FILTER_TYPE.VALUES_TO_COL) {
						// here, lcomp is the values and rComp is a the column
						// so same as above, but switch the order
						processFilterColToValues(traversal, rComp, lComp, IQueryFilter.getReverseNumericalComparator(comp));
					}
				}
				unionT.add(traversal);
			}
		}

		if(hasFilter) {
			this.vertsIt.union(unionT.toArray(new GraphTraversal[unionT.size()]));
		}
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
		if(this.edgesIt.hasNext()) {
			return true;
		} else {
			try {
				this.edgesIt.close();
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
			return false;
		}
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
		String sUri = getNodePhysicalType(e.outVertex().property(TinkerFrame.TINKER_TYPE).value() + "") + "/" + e.outVertex().property(TinkerFrame.TINKER_NAME).value();
		String tUri = getNodePhysicalType(e.inVertex().property(TinkerFrame.TINKER_TYPE).value() + "") + "/" + e.inVertex().property(TinkerFrame.TINKER_NAME).value();
		
		if(!this.vertSet.contains(sUri) || !this.vertSet.contains(tUri)) {
			return new HashMap<String, Object>();
		}
		
		edgeMap.put("source", sUri);
		edgeMap.put("target", tUri);

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
	 * Generate edges iterator
	 */
	private void createEdgesIt() {
		// get all edges that
		// 1) isn't the filtered edge
		// 2) neither vertex have an incoming filtered edge
		// 3) no vertex is a prim key
		this.edgesIt = this.g.traversal().E();
		
		boolean hasFilter = false;
		GenRowFilters filterGrs = tf.getFrameFilters();
		List<GraphTraversal> unionT = new Vector<GraphTraversal>();
		if(!filterGrs.isEmpty()) {
			hasFilter = true;
			// we have filters to consider
			List<String[]> relationships = this.meta.getAllRelationships();
			for(String[] rel : relationships) {
				String start = rel[0];
				String end = rel[1];
				
				GraphTraversal traversal = __.V().has(TinkerFrame.TINKER_TYPE, getNodePhysicalType(start));
				
				// make sure both have filters
				List<SimpleQueryFilter> startFilters = filterGrs.getAllSimpleQueryFiltersContainingColumn(start);
				List<SimpleQueryFilter> endFilters = filterGrs.getAllSimpleQueryFiltersContainingColumn(end);

				// add filters to start
				for(SimpleQueryFilter filter : startFilters) {
					SimpleQueryFilter.FILTER_TYPE filterType = filter.getSimpleFilterType();
					NounMetadata lComp = filter.getLComparison();
					NounMetadata rComp = filter.getRComparison();
					String comp = filter.getComparator();

					if(filterType == SimpleQueryFilter.FILTER_TYPE.COL_TO_VALUES) {
						// here, lcomp is the column and rComp is a set of values
						processFilterColToValues(traversal, lComp, rComp, comp);
					} else if(filterType == SimpleQueryFilter.FILTER_TYPE.VALUES_TO_COL) {
						// here, lcomp is the values and rComp is a the column
						// so same as above, but switch the order
						processFilterColToValues(traversal, rComp, lComp, IQueryFilter.getReverseNumericalComparator(comp));
					}
				}
				
				// out edge
				traversal.as("startV").out(start + "+++" + end).has(TinkerFrame.TINKER_TYPE, getNodePhysicalType(end));
				// add filters to end
				for(SimpleQueryFilter filter : endFilters) {
					SimpleQueryFilter.FILTER_TYPE filterType = filter.getSimpleFilterType();
					NounMetadata lComp = filter.getLComparison();
					NounMetadata rComp = filter.getRComparison();
					String comp = filter.getComparator();

					if(filterType == SimpleQueryFilter.FILTER_TYPE.COL_TO_VALUES) {
						// here, lcomp is the column and rComp is a set of values
						processFilterColToValues(traversal, lComp, rComp, comp);
					} else if(filterType == SimpleQueryFilter.FILTER_TYPE.VALUES_TO_COL) {
						// here, lcomp is the values and rComp is a the column
						// so same as above, but switch the order
						processFilterColToValues(traversal, rComp, lComp, IQueryFilter.getReverseNumericalComparator(comp));
					}
				}
				
				// TODO: need to figure out how to make the inE be only for those V's
				// in the first set ... couldn't get it to work ... need to come back
				unionT.add(traversal.inE().dedup());
			}
		}

		if(hasFilter) {
			this.edgesIt.union(unionT.toArray(new GraphTraversal[unionT.size()]));
		}
	}
	
	/**
	 * Handle adding a column to set of values filter
	 * @param traversalSegment 
	 * @param colComp
	 * @param valuesComp
	 * @param comparison
	 */
	private void processFilterColToValues(GraphTraversal traversalSegment, NounMetadata colComp, NounMetadata valuesComp, String comparison) {
		Object filterObject = valuesComp.getValue();
		List<Object> filterValues = new Vector<Object>();
		// ughhh... this could be a list or an object
		// need to make this consistent!
		if(filterObject instanceof List) {
			filterValues.addAll(((List) filterObject));
		} else {
			filterValues.add(filterObject);
		}
		if (comparison.equals("==")) {
			traversalSegment = traversalSegment.has(TinkerFrame.TINKER_NAME, P.within(filterValues.toArray()));
		} else if (comparison.equals("<")) {
			traversalSegment = traversalSegment.has(TinkerFrame.TINKER_NAME, P.lt(filterValues.get(0)));
		} else if (comparison.equals(">")) {
			traversalSegment = traversalSegment.has(TinkerFrame.TINKER_NAME, P.gt(filterValues.get(0)));
		} else if (comparison.equals("<=")) {
			traversalSegment = traversalSegment.has(TinkerFrame.TINKER_NAME, P.lte(filterValues.get(0)));
		} else if (comparison.equals(">=")) {
			traversalSegment = traversalSegment.has(TinkerFrame.TINKER_NAME, P.gte(filterValues.get(0)));
		} else if (comparison.equals("!=")) {
			traversalSegment = traversalSegment.has(TinkerFrame.TINKER_NAME, P.without(filterValues.toArray()));
		}
	}
	
	/**
	 * For some of the nodes that have not been given an alias
	 * If there is an implicit alias on it (a physical name that matches an existing name)
	 * We will use that
	 * @param node
	 * @return
	 */
	private String getNodePhysicalType(String node) {
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
			Map<String, Object> edge = getNextEdge();
			if(!edge.isEmpty()) {
				edgesMapList.add(edge);
			}
		}
		formattedData.put("nodes", nodesMapList);
		formattedData.put("edges", edgesMapList);
		formattedData.put("graphMeta", getVertCounts());
		return formattedData;
	}
}
