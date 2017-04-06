package prerna.sablecc2.reactor;

import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.io.StringBufferInputStream;
import java.util.Iterator;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.sablecc2.PlannerTranslation;
import prerna.sablecc2.Translation;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.parser.Parser;
import prerna.test.TestUtilityMethods;

public class TaxMain2 {

	public static void main(String[] args) throws Exception {
		
		TestUtilityMethods.loadDIHelper();
		
		// defining some variables to make it easier
		// will be overwriting some of the references
		PlannerTranslation t = new PlannerTranslation();
		PKSLPlanner planner = t.planner;
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
				+ "y = RetrieveValue(store=['test'], key=['formula2Output']);; ";
//				+ "finalResult = (x+y);";
		
		Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new StringBufferInputStream(pkslsToInitate)))));
		Start tree = p.parse();
		tree.apply(t);
		
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

		GraphTraversal<Vertex, Vertex> getAllV = planner.g.traversal().V().has(PKSLPlanner.TINKER_TYPE, PKSLPlanner.OPERATION);
		while(getAllV.hasNext()) {
			// get the vertex
			Vertex v = getAllV.next();
			System.out.println(">> " + v.property(PKSLPlanner.TINKER_ID).value().toString());
			
			// all of the vertex inputs
			Iterator<Edge> inputEdges = v.edges(Direction.IN);
			while(inputEdges.hasNext()) {
				Edge inputE = inputEdges.next();
				Vertex inputV = inputE.outVertex();
				System.out.println("\tinput >> " + inputV.property(PKSLPlanner.TINKER_ID).value().toString());
			}
			
			// all of the vertex inputs
			Iterator<Edge> outputEdges = v.edges(Direction.OUT);
			while(outputEdges.hasNext()) {
				Edge outputE = outputEdges.next();
				Vertex outputV = outputE.inVertex();
				System.out.println("\toutput >> " + outputV.property(PKSLPlanner.TINKER_ID).value().toString());
			}
		}
		
//		// iterator should only return 1
//		GraphTraversal<Vertex, Vertex> traversal = planner.g.traversal().V().has(PKSLPlanner.TINKER_TYPE, PKSLPlanner.NOUN).has(PKSLPlanner.TINKER_NAME, "thiskey1");
//		if(traversal.hasNext()) {
//			traverseDownNounNode(t, planner, traversal.next());
//		}
	}
	
	public static void traverseDownNounNode(Translation t, PKSLPlanner planner, Vertex startNoun) {
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
	
	public static void traverDownOpNode(Translation t, PKSLPlanner planner, Vertex startOp) {
		GraphTraversal<Vertex, Vertex> traversal = planner.g.traversal().V().has(PKSLPlanner.TINKER_ID, startOp.property(PKSLPlanner.TINKER_ID).value()).out().out().has(PKSLPlanner.TINKER_TYPE, PKSLPlanner.OPERATION);
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
	
}
