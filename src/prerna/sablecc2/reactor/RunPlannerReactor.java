package prerna.sablecc2.reactor;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.sablecc2.Translation;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;
import prerna.sablecc2.reactor.storage.MapStore;

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
		long start = System.currentTimeMillis();
		List<String> pksls = getPksls();
		
		Translation translation = new Translation();
		
		String fileName = "C:\\Workspace\\Semoss_Dev\\failedpksls.txt";
		BufferedWriter bw = null;
		FileWriter fw = null;
		
		try {
			fw = new FileWriter(fileName);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		bw = new BufferedWriter(fw);
		int count = 0;
		int total = 0;
		for(String pkslString : pksls) {
			try {
				Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(pkslString.getBytes("UTF-8"))))));
				
				Start tree = p.parse();
				tree.apply(translation);
				bw.write("COMPLETE::: "+pkslString+"\n");
			} catch (ParserException | LexerException | IOException e) {
				count++;
				e.printStackTrace();
				System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
				System.out.println(pkslString);
				try {
					bw.write("PARSE ERROR::::   "+pkslString+"\n");
				} catch (IOException e1) {
					e1.printStackTrace();
				}
//				System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
			} catch(Exception e) {
				count++;
				e.printStackTrace();
				System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
				System.out.println(pkslString);
				try {
					bw.write("EVAL ERROR:::: " + e.getMessage()+"\n");
					bw.write("EVAL ERROR:::: "+pkslString+"\n");
					bw.write("\n");
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
			}
			total++;
		}
		
		try {
			bw.close();
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("****************    "+total+"      *************************");
		System.out.println("****************    "+count+"      *************************");
		
//		MapStore mapStore = getMapStore();
//		Set<String> variables = translation.planner.getVariables();
//		for(String variable : variables) {
//			System.out.println(variable +"::::::");
//			System.out.println(translation.planner.getVariable(variable) +"::::::");
//			mapStore.put(variable, translation.planner.getVariable(variable));
//		}
		
		long end = System.currentTimeMillis();
		System.out.println("****************    "+(end - start)+"      *************************");
		
		return null;
	}
	
	
	private MapStore getMapStore() {
		return new MapStore();
	}
	
	/**
	 * Get the list of pksls to run
	 * @return
	 */
	private List<String> getPksls() {
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
				// if you are a "column", you fucked up
				String w = vertNoun.property(PKSLPlanner.TINKER_ID).value().toString();
				if(w.startsWith("NOUN:a") && !w.contains(" ")) {
					System.out.println(w);
				}
			}
		}
		
		
		// vertsToRun will hold the roots
		// this set will be modified as we traverse
		// down the graph and get primary downstream nodes
		// of roots and so forth
		Set<Vertex> vertsToRun = new HashSet<>();
		// pksls to run will hold all the other operations to execute
		List<String> pkslsToRun = new ArrayList<>();
		
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
				System.out.println(pkslOperation);

				// set a property "PROCESSED" to be false
				nextOp.property("PROCESSED", false);
				pkslOperation = nextOp.property(PKSLPlanner.TINKER_ID).value().toString().substring(3) + ";";
				
//				if(pkslOperation.contains("aForm_3800_Forecasting__11__Net_income_tax")) {
//					System.out.println("this guy");
//				}
				
				// we ignore this stupid thing....
				if(!pkslOperation.equals("FRAME")) {
					vertsToRun.add(nextOp);
				}
			} else {
//				System.out.println("NOT A ROOT");
//				// grab all in vertices of the noun
//				Iterator<Vertex> inOps = nextRootNoun.vertices(Direction.IN);
//				// if it has in vertices, it is not a root
//				int counter = 1;
//				if(inOps.hasNext()) {
//					System.out.println("HAS INPUT #" + counter + " : " + inOps.next().value(PKSLPlanner.TINKER_ID));
//					counter++;
//				}
			}
		}
		
		// now that we have identified the roots
		// we iterate through
		runVerts(vertsToRun, pkslsToRun);
		
		// return the list of pksls to execute
		// in the order we have determined
		return pkslsToRun;
	}
	
	/**
	 * 
	 * @param vertsToRun
	 * @param pkslsToRun
	 */
	private void runVerts(Set<Vertex> vertsToRun, List<String> pkslsToRun) {
		// if there are no vertices to execute
		// just return
		if(vertsToRun.isEmpty()) {
			return;
		}
		
		// we want to iterate through all the vertices we have defined
		Set<Vertex> nextVertsToRun = new HashSet<>();
		VERTEX_LOOP : for(Vertex nextVert : vertsToRun) {
			// get the pksl operation
			String pkslOperation = nextVert.property(PKSLPlanner.TINKER_ID).value().toString().substring(3) + ";";
			
			if(pkslOperation.contains("aForm_3800_Forecasting__11__Net_income_tax")) {
				System.out.println("this guy");
			}
			
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
					if(inputOp.property("PROCESSED").isPresent()) {
						Boolean isProcessed = inputOp.value("PROCESSED");
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
			nextVert.property("PROCESSED", true);
			
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
		runVerts(nextVertsToRun, pkslsToRun);
	}
}
