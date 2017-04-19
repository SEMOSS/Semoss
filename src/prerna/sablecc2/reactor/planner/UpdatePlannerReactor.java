package prerna.sablecc2.reactor.planner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.sablecc2.PlannerTranslation;
import prerna.sablecc2.Translation;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;
import prerna.sablecc2.reactor.PKSLPlanner;

public class UpdatePlannerReactor extends AbstractPlannerReactor {

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
		
		// grab all the pksls
		// store them in a list
		// and also keep a builder with all the executions
		List<String> pkslsToRun = new Vector<String>();
		StringBuilder builder = new StringBuilder();
		int numPksls = pksls.size();
		for(int i = 0; i < numPksls; i++) {
			builder.append(pksls.get(i));
			pkslsToRun.add(pksls.get(i).toString());
		}
		
		// know we execute these on a new planner
		// and then we will figure out the roots of these new values
		PlannerTranslation plannerT = new PlannerTranslation();
		try {
			Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(builder.toString().getBytes("UTF-8"))))));
			Start tree = p.parse();
			tree.apply(plannerT);
		} catch (ParserException | LexerException | IOException e) {
			e.printStackTrace();
		}
		
		// using this planner
		// get the roots
		Set<Vertex> roots = getRootPksls(plannerT.planner);
		// now we want to get all the output nouns of these roots
		// and go downstream to all the ops within the original planner 
		// that we are updating
		Set<Vertex> newRoots = getDownstreamEffectsInPlanner(roots, this.planner);
		
		// found the following roots
		for(Vertex vert : newRoots) {
			System.out.println("ROOT ::: " + vert.value(PKSLPlanner.TINKER_ID));
		}
		// traverse downstream and get all the other values we need to update
		traverseDownstreamVerts(newRoots, pkslsToRun);
		
		// now run through all the pksls and execute
		Translation translation = new Translation();
		translation.planner = this.planner;
		
		System.out.println(this.planner.getVariables());
		
		for(String pkslString : pkslsToRun) {
			try {
				Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(pkslString.getBytes("UTF-8"))))));
				Start tree = p.parse();
				tree.apply(translation);
			} catch (ParserException | LexerException | IOException e) {
				e.printStackTrace();
			}
		}
		
		Set<String> variables = translation.planner.getVariables();
		for(String variable : variables) {
			System.out.println("VARIABLE " + variable + " ::: " + translation.planner.getVariableValue(variable).getValue());
		}
		
		return null;
	}

}
