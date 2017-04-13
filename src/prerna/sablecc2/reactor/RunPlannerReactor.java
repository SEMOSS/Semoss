package prerna.sablecc2.reactor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.sablecc2.om.NounMetadata;

public class RunPlannerReactor extends AbstractReactor {

	@Override
	public void In() {
		curNoun("all");
	}

	@Override
	public Object Out() {
		return parentReactor;
	}
	
	@Override
	public NounMetadata execute()
	{
		List<String> pksls = getPksls();
		
		for(String pksl : pksls) 
		System.out.println(pksl);
		//what i want to do with the pksls is run them through the translation
		return null;
	}
	
	private List<String> getPksls() {
		
		//need to grab the pksls in the order we need to execute
		
		//so step 1.
			//grab all the pksl's 
		
		
//		String operation = newStart.property(PKSLPlanner.TINKER_ID).value().toString().substring(3);
//		GraphTraversal<Vertex, Vertex> getAlloperations = planner.g.traversal().V().has(PKSLPlanner.TINKER_TYPE, PKSLPlanner.OPERATION);
		
		//this grabs the end nouns
		//give me all the start nouns, i.e. no incoming dependencies
		GraphTraversal<Vertex, Vertex> getAllRootNouns = planner.g.traversal().V().has(PKSLPlanner.TINKER_TYPE, PKSLPlanner.NOUN).where(__.inE().count().is(0)); 
		
		
		Set<Vertex> vertsToRun = new HashSet<>();
		List<String> pkslsToRun = new ArrayList<>();
		
		
		//add all the leaf nouns, this is our starting point
		while(getAllRootNouns.hasNext()) {
			Vertex nextNoun = getAllRootNouns.next();
			Iterator<Vertex> opIterator = nextNoun.vertices(Direction.OUT);
			while(opIterator.hasNext()) {
				Vertex nextRoot = opIterator.next();
				nextRoot.property("PROCESSED", false);
				String pkslOperation = nextRoot.property(PKSLPlanner.TINKER_ID).value().toString().substring(3);
				System.out.println(pkslOperation);
				vertsToRun.add(nextRoot);
			}
		}
		
		runVerts(vertsToRun, pkslsToRun);
		
		return pkslsToRun;
	}
	
	private void runVerts(Set<Vertex> vertsToRun, List<String> pkslsToRun) {
		
		if(vertsToRun.isEmpty()) return;
		
		Set<Vertex> nextVertsToRun = new HashSet<>();
		for(Vertex nextVert : vertsToRun) {
		
			//if all the previous are flagged processed add to pkslsToRun
			boolean add = true;
			Iterator<Vertex> inputNouns = nextVert.vertices(Direction.IN);
			while(inputNouns.hasNext() && add) {
				Vertex inputNoun = inputNouns.next();
				Iterator<Vertex> inputOps = inputNoun.vertices(Direction.IN);
				while(inputOps.hasNext()) {
					Vertex inputOp = inputOps.next();
					try {
						Boolean isProcessed = inputOp.value("PROCESSED");
						if(!isProcessed) {
							add = false;
							break;
						}
					} catch(Exception e) {
						add = false;
						break;
					}
				}
			}
			
			if(add) {
				String pkslOperation = nextVert.property(PKSLPlanner.TINKER_ID).value().toString().substring(3);
				nextVert.property("PROCESSED", true);
				pkslsToRun.add(pkslOperation);
				
				Iterator<Vertex> outputNouns = nextVert.vertices(Direction.OUT);
				while(outputNouns.hasNext()) {
					Vertex outputNoun = outputNouns.next();
					Iterator<Vertex> outputOps = outputNoun.vertices(Direction.OUT);
					while(outputOps.hasNext()) {
						nextVertsToRun.add(outputOps.next());
					}
				}
			}
				//	add the pksl to pksls to Run, add nextVerts in vertsToRun
			//else don't
		}
		
		runVerts(nextVertsToRun, pkslsToRun);
	}
}
