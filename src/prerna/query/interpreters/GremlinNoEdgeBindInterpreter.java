package prerna.query.interpreters;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.TinkerFrame;
import prerna.engine.api.IDatabaseEngine;
import prerna.query.querystruct.filters.SimpleQueryFilter;

public class GremlinNoEdgeBindInterpreter extends GremlinInterpreter {

	/**	
	 * THIS CLASS IS EXTREMELY SIMILAR TO THE BASE GREMLIN INTERPRETER
	 * Only difference is we do not bind on the edge names
	 * We use this when connecting to external databases that do not following our 
	 * convention of what a node edge should be
	 * 
	 * TODO: capture this information and store it in the OWL as the relationship name
	 * Use that with the QS to perform the correct operation
	 */
	
	
	public GremlinNoEdgeBindInterpreter(GraphTraversalSource gt, Map<String, String> typeMap, Map<String, String> nameMap, IDatabaseEngine engine) {
		super(gt, typeMap, nameMap, engine);
	}
	
	public GremlinNoEdgeBindInterpreter(GraphTraversalSource gt, OwlTemporalEngineMeta meta) {
		super(gt, meta);
	}
	
	/**
	 * The main method to traversal the graph relationships
	 * @param startName
	 * @param edgeMap
	 * @param travelledEdges
	 * @param travelledNodeProps
	 * @param traversals
	 * @return
	 */
	protected List<GraphTraversal<Object, Object>> visitNode(
			String startName,
			Map<String, Set<String>> edgeMap,
			List<String> travelledEdges,
			List<String> travelledNodeProps, 
			List<GraphTraversal<Object, Object>> traversals) 
	{
		// TODO: should automatically add any properties that are required for the passed in node
		// instead of only doing it is the node has an upstream/downstream


		// first see if there are downstream nodes
		if (edgeMap.containsKey(startName)) {
			Iterator<String> downstreamIt = edgeMap.get(startName).iterator();
			while (downstreamIt.hasNext()) {
				// for each downstream node of this node
				String downstreamNodeType = downstreamIt.next();

				String edgeKey = startName + TinkerFrame.EDGE_LABEL_DELIMETER + downstreamNodeType;
				if (!travelledEdges.contains(edgeKey)) {
					if(logger.isDebugEnabled()) {
						logger.debug("travelling from node = '" + startName + "' to node = '" + downstreamNodeType + "'");
					}
					
					// get the traversal and store the necessary info
					GraphTraversal twoStepT = __.as(startName);

					// Get properties from startName node
					if (!travelledNodeProps.contains(startName)) {
						List<GraphTraversal<Object, Object>> propTraversals = getProperties(twoStepT, startName);

						if (propTraversals.size() > 0) {
							GraphTraversal[] propArray = new GraphTraversal[propTraversals.size()];
							twoStepT = twoStepT.match(propTraversals.toArray(propArray)).select(startName);
						}
					}

					twoStepT = twoStepT.out();
					twoStepT = queryNode(twoStepT, downstreamNodeType).as(downstreamNodeType);
					// add filters
					List<SimpleQueryFilter> nodeFilters = this.allFilters.getAllSimpleQueryFiltersContainingColumn(downstreamNodeType);
					addFiltersToPath(twoStepT, nodeFilters, getNodeName(downstreamNodeType));

					// add properties if present
					if (!travelledNodeProps.contains(downstreamNodeType)) {
						// get properties for the downstream node
						GraphTraversal downStepT = __.as(downstreamNodeType);

						// Get properties from downstream Node node
						List<GraphTraversal<Object, Object>> propTraversals = getProperties(downStepT, downstreamNodeType);

						if (propTraversals.size() > 0) {
							GraphTraversal[] propArray = new GraphTraversal[propTraversals.size()];
							twoStepT = twoStepT.match(propTraversals.toArray(propArray)).select(downstreamNodeType);
						}

						travelledNodeProps.add(downstreamNodeType);
					}

					traversals.add(twoStepT);
					travelledEdges.add(edgeKey);
					travelledNodeProps.add(startName);

					// recursively travel as far downstream as possible
					traversals = visitNode(downstreamNodeType, edgeMap, travelledEdges, travelledNodeProps, traversals);
				}
			}
		}

		// do the same thing for upstream
		// slightly more annoying to get upstream nodes...
		Set<String> upstreamNodes = getUpstreamNodes(startName, edgeMap);
		if (upstreamNodes != null && !upstreamNodes.isEmpty()) {
			Iterator<String> upstreamIt = upstreamNodes.iterator();
			while (upstreamIt.hasNext()) {
				String upstreamNodeType = upstreamIt.next();

				String edgeKey = upstreamNodeType + TinkerFrame.EDGE_LABEL_DELIMETER + startName;
				if (!travelledEdges.contains(edgeKey)) {
					if(logger.isDebugEnabled()) {
						logger.debug("travelling from node = '" + upstreamNodeType + "' to node = '" + startName + "'");
					}
					// get the traversal and store the necessary info
					GraphTraversal twoStepT = __.as(startName);

					// Get properties from startName node
					if (!travelledNodeProps.contains(startName)) {
						List<GraphTraversal<Object, Object>> propTraversals = getProperties(twoStepT, startName);

						if (propTraversals.size() > 0) {
							GraphTraversal[] propArray = new GraphTraversal[propTraversals.size()];
							twoStepT = twoStepT.match(propTraversals.toArray(propArray)).select(startName);
						}
					}
					twoStepT = twoStepT.in();
					twoStepT = queryNode(twoStepT, upstreamNodeType).as(upstreamNodeType);

					// add filtering
					List<SimpleQueryFilter> nodeFilters = this.allFilters.getAllSimpleQueryFiltersContainingColumn(upstreamNodeType);
					addFiltersToPath(twoStepT, nodeFilters, getNodeName(upstreamNodeType));

					// add properties if present
					if (!travelledNodeProps.contains(upstreamNodeType)) {
						// get properties for the upstream node
						GraphTraversal upStepT = __.as(upstreamNodeType);

						List<GraphTraversal<Object, Object>> propTraversals = getProperties(upStepT, upstreamNodeType);

						if (propTraversals.size() > 0) {
							GraphTraversal[] propArray = new GraphTraversal[propTraversals.size()];
							twoStepT = twoStepT.match(propTraversals.toArray(propArray)).select(upstreamNodeType);
						}

						travelledNodeProps.add(upstreamNodeType);
					}

					traversals.add(twoStepT);
					travelledEdges.add(edgeKey);
					travelledNodeProps.add(startName);

					// recursively travel as far upstream as possible
					traversals = visitNode(upstreamNodeType, edgeMap, travelledEdges, travelledNodeProps, traversals);
				}
			}
		}

		return traversals;
	}

}