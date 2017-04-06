package prerna.sablecc2.reactor;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.io.StringBufferInputStream;
import java.util.Vector;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.sablecc2.Translation;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;
import prerna.sablecc2.reactor.IReactor.TYPE;
import prerna.test.TestUtilityMethods;

public class TaxMain {

	public static void main(String[] args) throws ParserException, LexerException, IOException {
		TestUtilityMethods.loadDIHelper();
		
		// defining some variables to make it easier
		// will be overwriting some of the references
		Translation t = new Translation();
		PKSLPlanner planner = t.planner;
		Vector<String> inputs = new Vector<String>();
		Vector<String> outputs = new Vector<String>();
		TYPE opType = TYPE.MAP;
		
		// initially, we need something to set some variables
		// these will not be added to the planner
		// they are just definitions
		
		String pkslsToInitate = "test = MapStore();; "
				+ "StoreValue(store=[test], key=['thiskey1'], value=[100]); "
				+ "StoreValue(store=[test], key=['thiskey2'], value=[2]); "
				+ "StoreValue(store=[test], key=['thiskey3'], value=[3]); "
				+ "StoreValue(store=[test], key=['thiskey4'], value=[4]); "
				
				+ "if( ( RetrieveValue(store=['test'], key=['thiskey1']) > 10 ), " // condition
				+ "StoreValue(store=[test], key=['formula1Output'], value=[10]), " // true case
				+ "StoreValue(store=[test], key=['formula1Output'], value=[0])); " // false case
				
				+ "if( ( RetrieveValue(store=['test'], key=['thiskey2']) > 10 ), " // condition
				+ "StoreValue(store=[test], key=['formula2Output'], value=[50]), " // true case
				+ "StoreValue(store=[test], key=['formula2Output'], value=[0])); " // false case
				
				+ "x = RetrieveValue(store=['test'], key=['formula1Output']);; "
				+ "y = RetrieveValue(store=['test'], key=['formula2Output']);; "
				+ "finalResult = (x+y);";
		
		Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new StringBufferInputStream(pkslsToInitate)))));
		Start tree = p.parse();
		tree.apply(t);
		
//		System.out.println(">>>>>>> " + planner.getVariable("finalResult") );
//		
//		// now we will add some formulas that will use these inputs
//		
//		// start take in thiskey1 and output formula1Output
//		inputs = new Vector<String>();
//		inputs.add("test");
//		inputs.add("thiskey1");
//		planner.addInputs("if( ( RetrieveValue(store=['test'], key=['thiskey1']) > 10 ), " // condition
//				+ "StoreValue(store=[test], key=['formula1Output'], value=[10]), " // true case
//				+ "StoreValue(store=[test], key=['formula1Output'], value=[0]));" // false case
//				, inputs, opType);
//		
//		
//		outputs = new Vector<String>();
//		outputs.add("formula1Output");
//		planner.addOutputs("if( ( RetrieveValue(store=['test'], key=['thiskey1']) > 10 ), " // condition
//				+ "StoreValue(store=[test], key=['formula1Output'], value=[10]), " // true case
//				+ "StoreValue(store=[test], key=['formula1Output'], value=[0]));" // false case
//				, outputs, opType);
//		
//		// end take in thiskey1 and output formula1Output
//
//		// start take in thiskey2 and output formula2Output
//		inputs = new Vector<String>();
//		inputs.add("test");
//		inputs.add("thiskey2");
//		planner.addInputs("if( ( RetrieveValue(store=['test'], key=['thiskey2']) > 10 ), " // condition
//				+ "StoreValue(store=[test], key=['formula2Output'], value=[50]), " // true case
//				+ "StoreValue(store=[test], key=['formula2Output'], value=[0]));" // false case
//				, inputs, opType);
//		
//		outputs = new Vector<String>();
//		outputs.add("formula2Output");
//		planner.addOutputs("if( ( RetrieveValue(store=['test'], key=['thiskey2']) > 10 ), " // condition
//				+ "StoreValue(store=[test], key=['formula2Output'], value=[50]), " // true case
//				+ "StoreValue(store=[test], key=['formula2Output'], value=[0]));" // false case
//				, outputs, opType);
//		// end take in thiskey2 and output formula2Output
//
//		
//		// need to define variables to get around grammar
//		inputs = new Vector<String>();
//		inputs.add("test");
//		inputs.add("formula1Output");
//		planner.addInputs("x = RetrieveValue(store=['test'], key=['formula1Output']);;", inputs, opType);
//		outputs = new Vector<String>();
//		outputs.add("x");
//		planner.addOutputs("x = RetrieveValue(store=['test'], key=['formula1Output']);;", outputs, opType);
//
//		// need to define variables to get around grammar
//		inputs = new Vector<String>();
//		inputs.add("test");
//		inputs.add("formula2Output");
//		planner.addInputs("y = RetrieveValue(store=['test'], key=['formula2Output']);;", inputs, opType);
//		outputs = new Vector<String>();
//		outputs.add("y");
//		planner.addOutputs("y = RetrieveValue(store=['test'], key=['formula2Output']);;", outputs, opType);
//		
//		inputs = new Vector<String>();
//		inputs.add("x");
//		inputs.add("y");
//		planner.addInputs("finalResult = (x+y);", inputs, opType);
//		
//		// now i will execute a new query that is taking in thiskey1
//		String newPkqlToExecute = "StoreValue(store=[test], key=['thiskey1'], value=[0]);";
//		p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new StringBufferInputStream(newPkqlToExecute)))));
//		tree = p.parse();
//		tree.apply(t);
		
		// iterator should only return 1
		GraphTraversal<Vertex, Vertex> traversal = planner.g.traversal().V().has(PKSLPlanner.TINKER_TYPE, PKSLPlanner.NOUN).has(PKSLPlanner.TINKER_NAME, "thiskey1");
		if(traversal.hasNext()) {
			traverseDownNounNode(t, planner, traversal.next());
		}
		
//		System.out.println(">>>>>>> " + planner.getVariable("finalResult") );
	}
	
	
	public static void traverseDownNounNode(Translation t, PKSLPlanner planner, Vertex startNoun) {
		// get all the operations that use this start noun directly
		GraphTraversal<Vertex, Vertex> traversal = planner.g.traversal().V().has(PKSLPlanner.TINKER_ID, startNoun.property(PKSLPlanner.TINKER_ID).value()).out().has(PKSLPlanner.TINKER_TYPE, PKSLPlanner.OPERATION);
		while(traversal.hasNext()) {
			// keep the newstart node
			Vertex newStart = traversal.next();
			String operation = newStart.property(PKSLPlanner.TINKER_ID).value().toString().substring(3); // have a substring for now to account for start of OP:
			System.out.println(newStart + " ::: " + operation);
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
	
	public static void traverDownOpNode(Translation t, PKSLPlanner planner, Vertex startOp) {
		GraphTraversal<Vertex, Vertex> traversal = planner.g.traversal().V().has(PKSLPlanner.TINKER_ID, startOp.property(PKSLPlanner.TINKER_ID).value()).out().out().has(PKSLPlanner.TINKER_TYPE, PKSLPlanner.OPERATION);
		while(traversal.hasNext()) {
			// keep the newstart node
			Vertex newStart = traversal.next();
			String operation = newStart.property(PKSLPlanner.TINKER_ID).value().toString().substring(3); // have a substring for now to account for start of OP:
			System.out.println(newStart + " ::: " + operation);
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
	
}
