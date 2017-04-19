package prerna.sablecc2.reactor.planner;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.ds.TinkerFrame;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.PKSLPlanner;

public abstract class AbstractPlannerReactor extends AbstractReactor {

	/**
	 * Runs through and get all downstream vertices based on the execution order
	 * and will continue to traverse down and get downstream vertices that it can execute
	 * @param vertsToRun			The starting set of vertices to execute
	 * @param pkslsToRun			The list of pksls that are being added in an order
	 * 								where dependents are executed first
	 */	
	protected void getAllDownstreamVertsBasedOnTraverseOrder(Set<Vertex> vertsToRun, List<String> downstreamVertIds) {
		// if there are no vertices to execute
		// just return
		if(vertsToRun.isEmpty()) {
			return;
		}
		
		// keep a set of the next vertices to try to execute
		Set<Vertex> nextVertsToRun = new LinkedHashSet<Vertex>();
		
		// we want to iterate through all the vertices we have defined
		for(Vertex nextVert : vertsToRun) {
			String vertId = nextVert.property(PKSLPlanner.TINKER_ID).value().toString();
			// set the property PROCESSED so we can now properly traverse
			nextVert.property(PKSLPlanner.PROCESSED, true);
			
			// add this operation to the pkslToRun
			downstreamVertIds.add(vertId);
			
			// store the list so we can order them together
			List<String> downstreamNodeIds = new Vector<String>();

			// for this root
			// find all the out nouns
			Iterator<Vertex> outNounsIt = nextVert.vertices(Direction.OUT);
			while(outNounsIt.hasNext()) {
				// grab the noun
				Vertex outNoun = outNounsIt.next();
				// get all ops that use this node
				Iterator<Vertex> outOpsIt = outNoun.vertices(Direction.OUT);
				while(outOpsIt.hasNext()) {
					Vertex outOp = outOpsIt.next();
					boolean processed = (boolean) outOp.value(PKSLPlanner.PROCESSED);
					if(!processed) {
						downstreamNodeIds.add(outOp.value(TinkerFrame.TINKER_ID).toString());
						// modify the PROCESSED key to be true so we dont get this vert again
						outOp.property(PKSLPlanner.PROCESSED, true);
						
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
	 * @param planner
	 * @param downstreamVertIds
	 * @return
	 */
	protected List<String> orderVertsAndGetPksls(PKSLPlanner planner, List<String> downstreamVertIds) {
		List<String> returnPksls = new Vector<String>();

		GraphTraversal<Vertex, Vertex> newRootsTraversal = planner.g.traversal().V().has(PKSLPlanner.TINKER_ID, P.within(downstreamVertIds)).order().by(PKSLPlanner.ORDER, Order.incr);
		while(newRootsTraversal.hasNext()) {
			Vertex vert = newRootsTraversal.next();
			returnPksls.add(vert.value(TinkerFrame.TINKER_ID).toString().substring(3) + ";");
		}
		
		return returnPksls;
	}

	/**
	 * Runs through and sees if it is possible to execute a set of vertices
	 * and will continue to traverse down and get downstream vertices that it can execute
	 * @param vertsToRun			The starting set of vertices to execute
	 * @param pkslsToRun			The list of pksls that are being added in an order
	 * 								where dependents are executed first
	 */	
	protected void traverseDownstreamVertsAndOrderProcessing(Set<Vertex> vertsToRun, List<String> pkslsToRun) {
		int startOrder = 0; // start at 1 and set the order as we itereate
		traverseDownstreamVertsProcessor(vertsToRun, pkslsToRun, true, startOrder);
	}
	
	/**
	 * Runs through and sees if it is possible to execute a set of vertices
	 * and will continue to traverse down and get downstream vertices that it can execute
	 * @param vertsToRun			The starting set of vertices to execute
	 * @param pkslsToRun			The list of pksls that are being added in an order
	 * 								where dependents are executed first
	 * @param orderNum				Integer to set the order of execution as a property on
	 * 								each operation vertex
	 */
	private void traverseDownstreamVertsProcessor(Set<Vertex> vertsToRun, List<String> pkslsToRun, boolean setOrder, int orderNum) {
		// if there are no vertices to execute
		// just return
		if(vertsToRun.isEmpty()) {
			return;
		}
		
		// keep a set of the next vertices to try to execute
		Set<Vertex> nextVertsToRun = new HashSet<>();
		
		// we want to iterate through all the vertices we have defined
		VERTEX_LOOP : for(Vertex nextVert : vertsToRun) {
			// get the pksl operation
			String pkslOperation = nextVert.property(PKSLPlanner.TINKER_ID).value().toString().substring(3) + ";";
			
			// grab all the in nouns for this operation
			Iterator<Vertex> inputNouns = nextVert.vertices(Direction.IN);
			while(inputNouns.hasNext()) {
				// get the input noun
				Vertex inputNoun = inputNouns.next();
				Iterator<Vertex> inputOps = inputNoun.vertices(Direction.IN);
				// get all the ops that have this noun as an output
				while(inputOps.hasNext()) {
					// if this op has been executed
					// we can add it
					// otherwise, we cannot
					Vertex inputOp = inputOps.next();
					if(inputOp.property(PKSLPlanner.PROCESSED).isPresent()) {
						Boolean isProcessed = inputOp.value(PKSLPlanner.PROCESSED);
						if(!isProcessed) {
							// well, we can't use you now
							// continue to next vertex
							continue VERTEX_LOOP;
						}
					} else {
						// well, we can't use you now
						// continue to next vertex
						continue VERTEX_LOOP;
					}
				}
			}
			// if you reached this point
			// the above loop was able to determine that every single
			// input has been executed
			// so we can now add this :)

			// set the property PROCESSED so we can now properly traverse
			nextVert.property(PKSLPlanner.PROCESSED, true);
			
			// if we are setting the order
			// update here
			if(setOrder) {
				nextVert.property(PKSLPlanner.ORDER, orderNum);
				orderNum++;
			}
			
			// add this operation to the pkslToRun
			pkslsToRun.add(pkslOperation);
			
			// add all the outOps to now go through and try adding
			Iterator<Vertex> outputNouns = nextVert.vertices(Direction.OUT);
			while(outputNouns.hasNext()) {
				Vertex outputNoun = outputNouns.next();
				Iterator<Vertex> outputOps = outputNoun.vertices(Direction.OUT);
				while(outputOps.hasNext()) {
					nextVertsToRun.add(outputOps.next());
				}
			}
		}
		
		// run through the method with the new set of vertices
		traverseDownstreamVertsProcessor(nextVertsToRun, pkslsToRun, setOrder, orderNum);
	}
	
	
	/**
	 * Get the list of pksls to run
	 * @return
	 */
	protected Set<Vertex> getRootPksls(PKSLPlanner planner) {
		// vertsToRun will hold the roots
		// this set will be modified as we traverse
		// down the graph and get primary downstream nodes
		// of roots and so forth
		Set<Vertex> vertsToRun = new HashSet<Vertex>();
		
		GraphTraversal<Vertex, Long> getAllRootOpsCount = planner.g.traversal().V().has(PKSLPlanner.TINKER_TYPE, PKSLPlanner.OPERATION).count(); 
		if(getAllRootOpsCount.hasNext()) {
			System.out.println("FOUND " + getAllRootOpsCount.next() + " OP VERTICES!");
		}
		
		// get all the operations
		// and need to figure out which ones are roots
		GraphTraversal<Vertex, Vertex> getAllRootOps = planner.g.traversal().V().has(PKSLPlanner.TINKER_TYPE, PKSLPlanner.OPERATION); 
		
		// iterate through all the operations
		while(getAllRootOps.hasNext()) {
			Vertex nextOp = getAllRootOps.next();
			// the actual operation can be grab via substring of the pkslOperation
			String pkslOperation = nextOp.property(PKSLPlanner.TINKER_ID).value().toString().substring(3) + ";";

			// grab all the in nouns
			// these nouns are what this operation depends on
			Iterator<Vertex> nounIterator = nextOp.vertices(Direction.IN);
			// we will determine that this operation is a root
			// if and only if all its in nouns have 0 inputs
			boolean isRoot = true;
			Vertex nextRootNoun = null;
			while(nounIterator.hasNext() && isRoot) {
				nextRootNoun = nounIterator.next();
				// grab all in vertices of the noun
				Iterator<Vertex> inOps = nextRootNoun.vertices(Direction.IN);
				// if it has in vertices, it is not a root
				if(inOps.hasNext()) {
					isRoot = false;
				}
			}
			
			// if we have a root
			if(isRoot) {
				// set a property "PROCESSED" to be false
				nextOp.property(PKSLPlanner.PROCESSED, false);
				pkslOperation = nextOp.property(PKSLPlanner.TINKER_ID).value().toString().substring(3) + ";";
				
				// we ignore this stupid thing....
				if(!pkslOperation.equals("FRAME")) {
					vertsToRun.add(nextOp);
				}
			}
		}
		
		// return the verts to run
		return vertsToRun;
	}
	
	/**
	 * Given a set of roots
	 * Find the downstream nouns
	 * Within the planner, find the ops that have the downstream nouns as inputs
	 * Return those as the new roots
	 * @param roots
	 * @param planner
	 * @return
	 */
	protected Set<Vertex> getDownstreamEffectsInPlanner(Set<Vertex> roots, PKSLPlanner planner) {
		Set<Vertex> newRoots = new LinkedHashSet<Vertex>();
		for(Vertex v : roots) {
			// for this root
			// find all the out nouns
			Iterator<Vertex> outNounsIt = v.vertices(Direction.OUT);
			while(outNounsIt.hasNext()) {
				// grab the noun
				Vertex outNoun = outNounsIt.next();
				String outNounId = outNoun.value(PKSLPlanner.TINKER_ID);
				// now, search in the planner and find if this outNoun is present
				// and grab all the OPs that have this as an input
				// these will get added to the newRoots
				GraphTraversal<Vertex, Vertex> newRootsTraversal = planner.g.traversal().V().has(PKSLPlanner.TINKER_ID, outNounId).out()
						.has(PKSLPlanner.PROCESSED, false) // make sure it hasn't been already added
						.order().by(PKSLPlanner.ORDER, Order.incr).dedup(); // return in the order of the original execution in run planner
				while(newRootsTraversal.hasNext()) {
					Vertex vert = newRootsTraversal.next();
					// modify the PROCESSED key to be true so we dont get this vert again
					vert.property(PKSLPlanner.PROCESSED, true);
					newRoots.add(vert);
				}
			}
		}
		return newRoots;
	}
	
	/**
	 * Reset the processed boolean to be false
	 * This is required to properly determine which nodes/how nodes need to be updated
	 * @param planner
	 */
	protected void resetProcessedBoolean(PKSLPlanner planner) {
		// grab all op nodes
		GraphTraversal<Vertex, Vertex> allOpsIt = planner.g.traversal().V().has(PKSLPlanner.TINKER_TYPE, PKSLPlanner.OPERATION);
		// iterate through and set the propertys
		while(allOpsIt.hasNext()) {
			Vertex opNode = allOpsIt.next();
			// set a property "PROCESSED" to be false
			opNode.property(PKSLPlanner.PROCESSED, false);
		}
	}
	
	
	/*
	 * For debugging purposes, will also create new pksls to define variables that should be defined
	 * This is only added right now since it is a pain to manually add these for tax use case
	 * Setting these values to 0
	 */
	protected List<String> getUndefinedVariablesPksls(PKSLPlanner planner) {
		// pksls to run will hold all the other operations to execute
		List<String> pkslsToRun = new Vector<String>();

		GraphTraversal<Vertex, Long> standAloneNounCount = planner.g.traversal().V().has(PKSLPlanner.TINKER_TYPE, PKSLPlanner.NOUN).count(); 
		if(standAloneNounCount.hasNext()) {
			System.out.println("FOUND " + standAloneNounCount.next() + " NOUNS VERTICES!");
		}

		GraphTraversal<Vertex, Vertex> standAloneNoun = planner.g.traversal().V().has(PKSLPlanner.TINKER_TYPE, PKSLPlanner.NOUN); 
		while(standAloneNoun.hasNext()) {
			Vertex vertNoun = standAloneNoun.next();
			Iterator<Vertex> inOpsIt = vertNoun.vertices(Direction.IN);
			if(inOpsIt.hasNext()) {
				// i dont care about you
			} else {
				// you are a noun which is a logical end point or a variable that wasn't defined
				// you should be a string or a number
				String w = vertNoun.property(PKSLPlanner.TINKER_ID).value().toString();
				if(w.startsWith("NOUN:A") && !w.contains(" ")) {
					String pksl = w.replaceFirst("NOUN:", "");
					pksl += " = 0;";
					pkslsToRun.add(pksl);
				}
			}
		}
		
		return pkslsToRun;
	}
	
}
