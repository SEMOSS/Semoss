package prerna.ds;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import prerna.engine.impl.tinker.TinkerEngine;
import prerna.rdf.query.builder.IQueryInterpreter;

public class TinkerQueryEngineInterpreter extends AbstractTinkerInterpreter implements IQueryInterpreter {

	public TinkerQueryEngineInterpreter(TinkerEngine tinkerEngine) {
		this.g = tinkerEngine.getGraph();
		this.gt = g.traversal().V();
	}

	@Override
	public void setQueryStruct(QueryStruct qs) {
		this.qs = qs;
	}

	/**
	 * ugh... other interpreters return a string... but that is super
	 * inefficient on gremlin
	 */
	@Override
	public String composeQuery() {
		return null;
	}

	/**
	 * screw returning a string.. i'm going to go ahead and return an iterator..
	 * 
	 * @return
	 */
	public Iterator composeIterator() {
		addFilters();
		addJoins();
		addSelectors();

		return new TinkerIterator(g, gt, getSelector());
	}

	protected void addJoins() {
		// process the specific joins wanted in the traversal
		// this utilizes the previously definted filters

		// might want to consider doing some optimization in how i choose
		// the first node, similar to what is done in gremlin builder
		// but this will be a TODO
		if (edgeHash == null || edgeHash.isEmpty()) {
			edgeHash = generateEdgeMap();
		}
		addNodeEdge(edgeHash);

	}

	/**
	 * This is the bulk of the class Uses the edgeMap to figure out what things
	 * are connected
	 * 
	 * @param edgeMap
	 */
	public void addNodeEdge(Map<String, Set<String>> edgeMap) {
		if (edgeMap.isEmpty()) {
			return;
		}

		List<String> travelledEdges = new Vector<String>();
		List<GraphTraversal<Object, Vertex>> traversals = new Vector<GraphTraversal<Object, Vertex>>();

		String startUniqueName = edgeMap.keySet().iterator().next();
		System.out.println("START HERE" + startUniqueName);
		Hashtable <String, Vector<String>> sel = qs.selectors;
		// TODO remove if overwritten
		// this.edgeHash = this.qs.getReturnConnectionsHash();
		// Vertex startNode =
		// this.metaGraph.traversal().V().has(TinkerFrame.TINKER_NAME,
		// startUniqueName).next();
		GraphTraversal gt = g.traversal().V().has(TinkerFrame.TINKER_TYPE, startUniqueName);
//		for(String s: sel.keySet()) {
//			if(!s.contains("PRIM")) {
//				gt.has(s);
//			}
//		}
		Vertex startNode = (Vertex) gt.next();
//		Vertex startNode = g.traversal().V().has(TinkerFrame.TINKER_TYPE, startUniqueName).next();

		// TinkerFrame.TINKER_NAME changes while TinkerFrame.TINKER_VALUE stays
		// constant
		String nameType = startNode.property(TinkerFrame.TINKER_NAME).value() + "";
		// String valueType =
		// startNode.property(TinkerFrame.TINKER_VALUE).value()+"";

		// remove prim_key when making a heatMap
		// if(valueType.equals(TinkerFrame.PRIM_KEY)){
		// valueType = startNode.property(TinkerFrame.TINKER_NAME).value() + "";
		// }

		// gt = gt.has(TinkerFrame.TINKER_TYPE, valueType).as(nameType);
		// there is a boolean at the metamodel level if this type has any
		// filters
		if (startNode.keys().contains(TinkerFrame.TINKER_FILTER)) {
			Object filtered = startNode.value(TinkerFrame.TINKER_FILTER);
			if ((Boolean) filtered == true) {
				// filtered edges have a type of filter
				gt = gt.not(__.in(TinkerFrame.TINKER_FILTER + TinkerFrame.EDGE_LABEL_DELIMETER + nameType)
						.has(TinkerFrame.TINKER_TYPE, TinkerFrame.TINKER_FILTER));
			}
			if (this.filters.containsKey(nameType)) {
				addFilterInPath(gt, nameType, this.filters.get(nameType));
			}
		}

		// add the logic to traverse
		traversals = visitNode(startNode, travelledEdges, edgeMap, traversals);

		if (traversals.size() > 0) {
			GraphTraversal[] array = new GraphTraversal[traversals.size()];
			gt = gt.match(traversals.toArray(array));
		}

	}

	protected List<GraphTraversal<Object, Vertex>> visitNode(Vertex orig, List<String> travelledEdges,
			Map<String, Set<String>> edgeMap, List<GraphTraversal<Object, Vertex>> traversals) {
		// TinkerFrame.TINKER_NAME changes while TinkerFrame.TINKER_VALUE stays
		// constant
		String origName = orig.value(TinkerFrame.TINKER_TYPE);
		// String origValue = orig.value(TinkerFrame.TINKER_VALUE);

		// remove prim_key when making a heatMap
		// if(origValue.equals(TinkerFrame.PRIM_KEY)){
		// origValue = orig.property(TinkerFrame.TINKER_NAME).value() + "";
		// }

		Set<String> edgesToTraverse = edgeMap.get(origName);
		if (edgesToTraverse != null) {
		for (String s : edgesToTraverse) {
			String edgeKey = origName + TinkerFrame.EDGE_LABEL_DELIMETER + s;
			GraphTraversal<Vertex, Vertex> downstreamIt = g.traversal().V(orig);
			downstreamIt.out(edgeKey);

			// downstreamIt.outV();
			
				// for each downstream node of this meta node

				// TODO: this can be optimized, using the edgeMap do determine
				// the traversal instead of iterating and guessing
				// GraphTraversal<Vertex, Vertex> downstreamIt =
				// g.traversal().V().has(TinkerFrame.TINKER_TYPE).has(TinkerFrame.TINKER_ID,
				// orig.property(TinkerFrame.TINKER_ID).value()).out(TinkerFrame.TINKER_TYPE
				// + TinkerFrame.EDGE_LABEL_DELIMETER + edge);
				while (downstreamIt.hasNext()) {
					// for each downstream node of this meta node
					Vertex nodeV = downstreamIt.next();

					// TinkerFrame.TINKER_NAME changes while
					// TinkerFrame.TINKER_VALUE stays constant
					String nameNode = nodeV.property(TinkerFrame.TINKER_TYPE).value() + "";
					String valueNode = nodeV.property(TinkerFrame.TINKER_NAME).value() + "";

					// remove prim_key when making a heatMap
					if (valueNode.equals(TinkerFrame.PRIM_KEY)) {
						valueNode = nodeV.property(TinkerFrame.TINKER_NAME).value() + "";
					}

					// String edgeKey = origName +
					// TinkerFrame.EDGE_LABEL_DELIMETER + nameNode;

					if (!travelledEdges.contains(edgeKey) && edgesToTraverse.contains(nameNode)) {
						// LOGGER.info("travelling down to " + nameNode);

						GraphTraversal<Object, Vertex> twoStepT = __.as(origName).out(edgeKey);
						if (nodeV.keys().contains(TinkerFrame.TINKER_FILTER)) {
							Object filtered = nodeV.value(TinkerFrame.TINKER_FILTER);

							if ((Boolean) filtered == true) {
								twoStepT = twoStepT.not(
										__.in(TinkerFrame.TINKER_FILTER + TinkerFrame.EDGE_LABEL_DELIMETER + nameNode)
												.has(TinkerFrame.TINKER_TYPE, TinkerFrame.TINKER_FILTER));
							}
						}
						if (this.filters.containsKey(nameNode)) {
							addFilterInPath(twoStepT, nameNode, this.filters.get(nameNode));
						}

						twoStepT = twoStepT.as(nameNode);
						// LOGGER.info("twoStepT downstream : " + twoStepT);
						traversals.add(twoStepT);

						travelledEdges.add(edgeKey);
						// travel as far downstream as possible
						traversals = visitNode(nodeV, travelledEdges, edgeMap, traversals);
					}
				}
			}
		}
	

		// do the same thing for upstream
		// TODO: this can be optimized, using the edgeMap do determine the
		// traversal instead of iterating and guessing
		GraphTraversal<Vertex, Vertex> upstreamIt = this.g.traversal().V().has(TinkerFrame.TINKER_TYPE)
				.has(TinkerFrame.TINKER_ID, orig.property(TinkerFrame.TINKER_ID).value()).in(TinkerFrame.TINKER_TYPE);
		while (upstreamIt.hasNext()) {
			Vertex nodeV = upstreamIt.next();

			// TinkerFrame.TINKER_NAME changes while
			// TinkerFrame.TINKER_VALUE stays constant
			String nameNode = nodeV.property(TinkerFrame.TINKER_NAME).value() + "";
			String valueNode = nodeV.property(TinkerFrame.TINKER_VALUE).value() + "";

			// remove prim_key when making a heatMap
			if (valueNode.equals(TinkerFrame.PRIM_KEY)) {
				valueNode = nodeV.property(TinkerFrame.TINKER_NAME).value() + "";
			}

			 String edgeKey = nameNode + TinkerFrame.EDGE_LABEL_DELIMETER
			 + origName;
			if (!travelledEdges.contains(edgeKey) && edgesToTraverse.contains(nameNode)) {
				// LOGGER.info("travelling down to " + nameNode);

				GraphTraversal<Object, Vertex> twoStepT = __.as(origName).in(edgeKey).has(TinkerFrame.TINKER_TYPE,
						valueNode);

				Object filtered = nodeV.value(TinkerFrame.TINKER_FILTER);
				if ((Boolean) filtered == true) {
					twoStepT = twoStepT
							.not(__.in(TinkerFrame.TINKER_FILTER + TinkerFrame.EDGE_LABEL_DELIMETER + nameNode)
									.has(TinkerFrame.TINKER_TYPE, TinkerFrame.TINKER_FILTER));
				}
				if (this.filters.containsKey(nameNode)) {
					addFilterInPath(twoStepT, nameNode, this.filters.get(nameNode));
				}

				twoStepT = twoStepT.as(nameNode);
				// LOGGER.info("twoStepT upstream : " + twoStepT);
				traversals.add(twoStepT);

				travelledEdges.add(edgeKey);
				// travel as far upstream as possible
				traversals = visitNode(nodeV, travelledEdges, edgeMap, traversals);
			}
		}

		return traversals;
	}

	@Override
	public void setPerformCount(int performCount) {
		// TODO Auto-generated method stub

	}

	@Override
	public int isPerformCount() {
		return QueryStruct.NO_COUNT;
	}

	@Override
	public void clear() {

	}

}
