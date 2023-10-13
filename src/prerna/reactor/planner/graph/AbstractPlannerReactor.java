package prerna.reactor.planner.graph;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.reactor.AbstractReactor;
import prerna.reactor.PixelPlanner;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public abstract class AbstractPlannerReactor extends AbstractReactor {

	private static final Logger LOGGER = LogManager.getLogger(AbstractPlannerReactor.class.getName());

	/**
	 * Runs through and get all downstream vertices based on the execution order
	 * and will continue to traverse down and get downstream vertices that it
	 * can execute
	 * 
	 * @param vertsToRun
	 *            The starting set of vertices to execute
	 * @param downstreamVertIds
	 */
	protected void getAllDownstreamVertsBasedOnTraverseOrder(Set<Vertex> vertsToRun, List<String> downstreamVertIds) {
		// if there are no vertices to execute
		// just return
		if (vertsToRun.isEmpty()) {
			return;
		}

		// keep a set of the next vertices to try to execute
		Set<Vertex> nextVertsToRun = new LinkedHashSet<Vertex>();

		// we want to iterate through all the vertices we have defined
		for (Vertex nextVert : vertsToRun) {
			String vertId = nextVert.property(PixelPlanner.TINKER_ID).value().toString();
			// set the property PROCESSED so we can now properly traverse
			nextVert.property(PixelPlanner.PROCESSED, true);

			// add this operation to the pkslToRun
			downstreamVertIds.add(vertId);

			// for this root
			// find all the out nouns
			Iterator<Vertex> outNounsIt = nextVert.vertices(Direction.OUT);
			while (outNounsIt.hasNext()) {
				// grab the noun
				Vertex outNoun = outNounsIt.next();
				// get all ops that use this node
				Iterator<Vertex> outOpsIt = outNoun.vertices(Direction.OUT);
				while (outOpsIt.hasNext()) {
					Vertex outOp = outOpsIt.next();
					boolean processed = (boolean) outOp.value(PixelPlanner.PROCESSED);
					if (!processed) {
						// modify the PROCESSED key to be true so we dont get
						// this vert again
						outOp.property(PixelPlanner.PROCESSED, true);

						// add to next set to execute
						nextVertsToRun.add(outOp);
					}
				}
			}
		}

		// run through the method with the new set of vertices
		getAllDownstreamVertsBasedOnTraverseOrder(nextVertsToRun, downstreamVertIds);
	}

	/**
	 * Return an ordered list of the vertices based on the previous run plan
	 * 
	 * @param planner
	 * @param downstreamVertIds
	 * @return
	 */
	protected List<String> orderVertsAndGetPksls(PixelPlanner planner, List<String> downstreamVertIds) {
		List<String> returnPksls = new Vector<String>();

		GraphTraversal<Vertex, Vertex> newRootsTraversal = planner.g.traversal().V()
				.has(PixelPlanner.TINKER_ID, P.within(downstreamVertIds)).order().by(PixelPlanner.ORDER, Order.asc);
		while (newRootsTraversal.hasNext()) {
			Vertex vert = newRootsTraversal.next();
			returnPksls.add(getPksl(vert));
		}

		return returnPksls;
	}

	/**
	 * For errors
	 */
	protected void addOrderToNonExistentVerts(PixelPlanner planner) {
		GraphTraversal<Vertex, Vertex> newRootsTraversal = planner.g.traversal().V().hasNot(PixelPlanner.ORDER);
		while (newRootsTraversal.hasNext()) {
			Vertex vert = newRootsTraversal.next();
			vert.property(PixelPlanner.ORDER, 999999);
		}
	}

	protected void traverseDownstreamVertsProcessor(PixelPlanner planner, List<String> pkslsToRun) {
		int orderNum = 1;
		Set<Vertex> inputs = getZeroInDegreeVetices(planner);
//		addNounsToMainMap(planner, inputs);
		Queue<Vertex> processQueue = new LinkedList<Vertex>(inputs);
		while (!processQueue.isEmpty()) {
			// grab the root vertex to start to process
			// this root is a operation
			// poll... grab from the top
			int size = processQueue.size();
			for (int i = 0; i < size; i++) {
				Vertex root = processQueue.poll();
				String pkslOperation = getPksl(root);
				String type = root.value(PixelPlanner.TINKER_TYPE);

				// we get all the outs
				// these are the nouns that this operation creates
				Iterator<Vertex> nounIt = root.vertices(Direction.OUT);

				// iterate through them
				// note: all the downstream operations now have 1 less
				// dependency that has not
				// been executed because we can only get to the downstream op
				// by executing on its parents
				while (nounIt.hasNext()) {
					Vertex downOp = nounIt.next();
					if (!downOp.property("inVertexCount").isPresent()) {
						// we set the IN_DEGREE when we insert the node in the
						// planner
						downOp.property("inVertexCount", downOp.value(PixelPlanner.IN_DEGREE));
					}

					Integer currentIntCount = downOp.value("inVertexCount");
					downOp.property("inVertexCount", currentIntCount.intValue() - 1);
					if (currentIntCount.intValue() == 1) {
						processQueue.offer(downOp);
					}
				}

				// now, set the order within the vertex
				root.property(PixelPlanner.ORDER, orderNum);
				orderNum++;

				// and add it to the pksl list
				if (pkslOperation != null) {
					if (type.equals(PixelPlanner.OPERATION)) {
//						addOpToMainMap(planner, root);
						pkslsToRun.add(pkslOperation);
					}
				}
			}
		}

		LOGGER.info("DONE TRAVERSING THROUGH GRAPH!!!");
	}

	protected Set<Vertex> getZeroInDegreeVetices(PixelPlanner planner) {
		Set<Vertex> newRoots = new HashSet<Vertex>();
		GraphTraversal<Vertex, Vertex> vertexIt = planner.g.traversal().V();
		while (vertexIt.hasNext()) {
			Vertex vertex = vertexIt.next();
			if (vertex.property(PixelPlanner.IN_DEGREE).isPresent()) {
				if (vertex.value(PixelPlanner.IN_DEGREE).equals(0)) {
					newRoots.add(vertex);
				}
			} else {
				// set it to 0 so we dont need to check in future
				vertex.property(PixelPlanner.IN_DEGREE, 0);
				newRoots.add(vertex);
			}
		}
		return newRoots;
	}

	protected void addNounsToMainMap(PixelPlanner planner, Set<Vertex> nounVertexs) {
		Map<String, String> baseMap = null;
		HashMap<String, String> mainMap = null;
		if (!planner.hasProperty("MAIN_MAP", "MAIN_MAP")) {
			mainMap = new HashMap<String, String>();
			planner.addProperty("MAIN_MAP", "MAIN_MAP", mainMap);
		}
		mainMap = (HashMap<String, String>) planner.getProperty("MAIN_MAP", "MAIN_MAP");
		if (planner.hasProperty("BASE_MAP", "BASE_MAP")) {
			baseMap = (HashMap<String, String>) planner.getProperty("BASE_MAP", "BASE_MAP");
		}
		for (Vertex nounVertex : nounVertexs) {
			if (nounVertex.value(PixelPlanner.TINKER_TYPE).equals(PixelPlanner.NOUN)) {
				String nounName = (String) nounVertex.value(PixelPlanner.TINKER_NAME);
				if (baseMap == null || !baseMap.containsKey(nounName)) {
					NounMetadata noun = planner.getVariable(nounName);
					if(noun == null){
						System.out.println(nounName+" not found!");
						continue;
					}
					PixelDataType data = noun.getNounType();
					if (data == PixelDataType.CONST_DECIMAL) {
						mainMap.put(nounName, "double");
					} else if (data == PixelDataType.CONST_INT) {
						mainMap.put(nounName, "double");
					} else if (data == PixelDataType.COLUMN) {
						mainMap.put(nounName, data.toString());
					} else if (data == PixelDataType.BOOLEAN) {
						mainMap.put(nounName, "boolean");
					} else if (data == PixelDataType.CONST_STRING) {
						mainMap.put(nounName, "String");
					}
				}
			}
		}
	}

	protected void addOpToMainMap(PixelPlanner planner, Vertex opVertex) {
		HashMap<String, String> mainMap = (HashMap<String, String>) planner.getProperty("MAIN_MAP", "MAIN_MAP");
		Map<String, String> baseMap = null;
		if (planner.hasProperty("BASE_MAP", "BASE_MAP")) {
			baseMap = (HashMap<String, String>) planner.getProperty("BASE_MAP", "BASE_MAP");
		}
		String nounName = ((String) opVertex.value(PixelPlanner.TINKER_NAME)).split("=")[0].trim();
		if (baseMap == null || !baseMap.containsKey(nounName)) {
			
			mainMap.put(nounName, "double");
		}
	}

	/**
	 * Get the list of pksls to run
	 * 
	 * @return
	 */
	protected Set<Vertex> getRootPksls(PixelPlanner planner) {
		// vertsToRun will hold the roots
		// this set will be modified as we traverse
		// down the graph and get primary downstream nodes
		// of roots and so forth
		Set<Vertex> vertsToRun = new HashSet<Vertex>();

		GraphTraversal<Vertex, Long> getAllRootOpsCount = planner.g.traversal().V()
				.has(PixelPlanner.TINKER_TYPE, PixelPlanner.OPERATION).count();
		if (getAllRootOpsCount.hasNext()) {
			LOGGER.info("FOUND " + getAllRootOpsCount.next() + " OP VERTICES!");
		}

		// get all the operations
		// and need to figure out which ones are roots
		GraphTraversal<Vertex, Vertex> getAllRootOps = planner.g.traversal().V().has(PixelPlanner.TINKER_TYPE,
				PixelPlanner.OPERATION);

		// iterate through all the operations
		while (getAllRootOps.hasNext()) {
			Vertex nextOp = getAllRootOps.next();
			// the actual operation can be grab via substring of the
			// pkslOperation
			String pkslOperation = getPksl(nextOp);

			// grab all the in nouns
			// these nouns are what this operation depends on
			Iterator<Vertex> nounIterator = nextOp.vertices(Direction.IN);
			// we will determine that this operation is a root
			// if and only if all its in nouns have 0 inputs
			boolean isRoot = true;
			Vertex nextRootNoun = null;
			while (nounIterator.hasNext() && isRoot) {
				nextRootNoun = nounIterator.next();
				// grab all in vertices of the noun
				Iterator<Vertex> inOps = nextRootNoun.vertices(Direction.IN);
				// if it has in vertices, it is not a root
				if (inOps.hasNext()) {
					isRoot = false;
				}
			}

			// if we have a root
			if (isRoot) {
				// set a property "PROCESSED" to be false
				nextOp.property(PixelPlanner.PROCESSED, false);
				pkslOperation = getPksl(nextOp);// nextOp.property(PKSLPlanner.TINKER_ID).value().toString().substring(3)
												// + ";";

				// we ignore this stupid thing....
				if (!pkslOperation.isEmpty() && !pkslOperation.equals("FRAME") && !pkslOperation.equals("MAIN_MAP")) {
					vertsToRun.add(nextOp);
				}
			}
		}

		// return the verts to run
		return vertsToRun;
	}

	/**
	 * Given a set of roots Find the downstream nouns Within the planner, find
	 * the ops that have the downstream nouns as inputs Return those as the new
	 * roots
	 * 
	 * @param roots
	 * @param planner
	 * @return
	 */
	protected Set<Vertex> getDownstreamEffectsInPlanner(Set<Vertex> roots, PixelPlanner planner) {
		Set<Vertex> newRoots = new LinkedHashSet<Vertex>();
		for (Vertex v : roots) {
			// for this root
			// find all the out nouns
			Iterator<Vertex> outNounsIt = v.vertices(Direction.OUT);
			while (outNounsIt.hasNext()) {
				// grab the noun
				Vertex outNoun = outNounsIt.next();
				String outNounId = outNoun.value(PixelPlanner.TINKER_ID);
				// now, search in the planner and find if this outNoun is
				// present
				// and grab all the OPs that have this as an input
				// these will get added to the newRoots
				GraphTraversal<Vertex, Vertex> newRootsTraversal = planner.g.traversal().V()
						.has(PixelPlanner.TINKER_ID, outNounId).out().has(PixelPlanner.PROCESSED, false) // make
																										// sure
																										// it
																										// hasn't
																										// been
																										// already
																										// added
						.order().by(PixelPlanner.ORDER, Order.asc).dedup(); // return
																			// in
																			// the
																			// order
																			// of
																			// the
																			// original
																			// execution
																			// in
																			// run
																			// planner
				while (newRootsTraversal.hasNext()) {
					Vertex vert = newRootsTraversal.next();
					// modify the PROCESSED key to be true so we dont get this
					// vert again
					vert.property(PixelPlanner.PROCESSED, true);
					newRoots.add(vert);
				}
			}
		}
		return newRoots;
	}

	/**
	 * Reset the processed boolean to be false This is required to properly
	 * determine which nodes/how nodes need to be updated
	 * 
	 * @param planner
	 */
	protected void resetProcessedBoolean(PixelPlanner planner) {
		// grab all op nodes
		GraphTraversal<Vertex, Vertex> allOpsIt = planner.g.traversal().V().has(PixelPlanner.TINKER_TYPE,
				PixelPlanner.OPERATION);
		// iterate through and set the propertys
		while (allOpsIt.hasNext()) {
			Vertex opNode = allOpsIt.next();
			// set a property "PROCESSED" to be false
			opNode.property(PixelPlanner.PROCESSED, false);
		}
	}

	protected String getPksl(Vertex vert) {
		String pkslOperation = vert.property(PixelPlanner.TINKER_ID).value().toString().substring(3) + ";";
		return pkslOperation;
	}

	/**
	 * Determine if a pksl is just a reflection to itself again
	 * 
	 * @param pksl
	 * @return
	 */
	public static boolean isSimpleAssignment(String pksl) {
		String[] strs = pksl.split("=");
		if (strs[1].trim().matches("\\(\\s*" + strs[0].trim() + "\\s*\\);")) {
			return true;
		}
		return false;
	}
}
