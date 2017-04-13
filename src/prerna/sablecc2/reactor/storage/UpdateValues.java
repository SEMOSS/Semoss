package prerna.sablecc2.reactor.storage;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.io.StringBufferInputStream;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.sablecc2.Translation;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.PKSLPlanner;

public class UpdateValues extends AbstractReactor {

	public static final String PKSL_NOUN = "pksls";
	public static final String OUT_STORE_NOUN = "out_store";
	public static final String IN_STORE_NOUN = "in_store";
	
	@Override
	public void In() {
		curNoun("all");
	}

	@Override
	public Object Out() {
		return this.parentReactor;
	}
	
	@Override
	public NounMetadata execute()
	{
		GenRowStruct pksls = this.store.getNoun(PKSL_NOUN);
		
		// alright, we need to go through
		// and run all the new pksls
		// but we also want to carry out all the
		
		int numPksls = pksls.size();
		for(int i = 0; i < numPksls; i++) {
			
		}
		
		return null;
	}
	
	
	
	private void traverseDownNounNode(Translation t, PKSLPlanner planner, Vertex startNoun) {
		// get all the operations that use this start noun directly
		GraphTraversal<Vertex, Vertex> traversal = planner.g.traversal().V().has(PKSLPlanner.TINKER_ID, startNoun.property(PKSLPlanner.TINKER_ID).value()).out().has(PKSLPlanner.TINKER_TYPE, PKSLPlanner.OPERATION);
		while(traversal.hasNext()) {
			// keep the newstart node
			Vertex newStart = traversal.next();
			String operation = newStart.property(PKSLPlanner.TINKER_ID).value().toString().substring(3) + ";"; // have a substring for now to account for start of OP:
			System.out.println(">>>>>>>>>>>>>>>>>>>>> " + newStart + " ::: " + operation);
			// run the operation
			// then go down and execute for all the downstream
			try {
				Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new StringBufferInputStream(operation)))));
				Start tree = p.parse();
				tree.apply(t);
			} catch(Exception e) {
				e.printStackTrace();
			}
			
			// recursively go down the nodes
			traverDownOpNode(t, planner, newStart);
		}
	}
	
	private void traverDownOpNode(Translation t, PKSLPlanner planner, Vertex startOp) {
		GraphTraversal<Vertex, Vertex> traversal = planner.g.traversal().V().has(PKSLPlanner.TINKER_ID, startOp.property(PKSLPlanner.TINKER_ID).value()).out().out().has(PKSLPlanner.TINKER_TYPE, PKSLPlanner.OPERATION);
		while(traversal.hasNext()) {
			// keep the newstart node
			Vertex newStart = traversal.next();
			String operation = newStart.property(PKSLPlanner.TINKER_ID).value().toString().substring(3) + ";"; // have a substring for now to account for start of OP:
			System.out.println(">>>>>>>>>>>>>>>>>>>>> " + newStart + " ::: " + operation);
			// run the operation
			// then go down and execute for all the downstream
			try {
				Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(operation.getBytes("UTF-8"))))));
				Start tree = p.parse();
				tree.apply(t);
			} catch(Exception e) {
				e.printStackTrace();
			}
			
			// recursively go down the nodes
			traverDownOpNode(t, planner, newStart);
		}
	}

}
