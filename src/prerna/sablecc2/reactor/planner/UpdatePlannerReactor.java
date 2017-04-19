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
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;
import prerna.sablecc2.reactor.PKSLPlanner;
import prerna.sablecc2.reactor.storage.InMemStore;
import prerna.sablecc2.reactor.storage.MapStore;

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
		PKSLPlanner myPlanner = getPlanner();
		// to properly update
		// we need to reset the "PROCESSED" property
		// that are currently set on the planner
		// when we first executed the plan
		resetProcessedBoolean(planner);
		
		// grab all the pksls
		GenRowStruct pksls = this.store.getNoun(PKSL_NOUN);
		
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
		Set<Vertex> newRoots = getDownstreamEffectsInPlanner(roots, myPlanner);
		
		// traverse downstream and get all the other values we need to update
		getAllDownstreamVertsBasedOnTraverseOrder(newRoots, pkslsToRun);
		
		// now run through all the pksls and execute
		executePKSLs(pkslsToRun, myPlanner);
		
		Set<String> variables = myPlanner.getVariables();
		InMemStore memStore = getInMemoryStore();
		for(String variable : variables) {
//			System.out.println("VARIABLE " + variable + " ::: " + this.planner.getVariableValue(variable).getValue());
			memStore.put(variable, myPlanner.getVariable(variable));
		}
		
		return new NounMetadata(memStore, PkslDataTypes.IN_MEM_STORE);
	}
	
	private InMemStore getMapStore() {
		InMemStore inMemStore = null;
		GenRowStruct grs = getNounStore().getNoun(IN_STORE_NOUN);
		if(grs != null) {
			inMemStore = (InMemStore) grs.get(0);
		} else {
			grs = getNounStore().getNoun(PkslDataTypes.IN_MEM_STORE.toString());
			if(grs != null) {
				inMemStore = (InMemStore) grs.get(0);
			}
		}
		
		return inMemStore;
	}
	
	/**
	 * Greedy execution of pksls
	 * @param pkslsToRun
	 */
	private void executePKSLs(List<String> pkslsToRun, PKSLPlanner planner) {
		// now run through all the pksls and execute
		Translation translation = new Translation();
		translation.planner = planner;
				
		for(String pkslString : pkslsToRun) {
			try {
				Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(pkslString.getBytes("UTF-8"))))));
				Start tree = p.parse();
				tree.apply(translation);
			} catch (ParserException | LexerException | IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private InMemStore getInMemoryStore() {
		InMemStore inMemStore = null;
//		GenRowStruct grs = getNounStore().getNoun(this.IN_MEM_STORE);
//		if(grs != null) {
//			inMemStore = (InMemStore) grs.get(0);
//		} else {
			GenRowStruct grs = getNounStore().getNoun(PkslDataTypes.IN_MEM_STORE.toString());
			if(grs != null) {
				inMemStore = (InMemStore) grs.get(0);
			} else {
				inMemStore = new MapStore();
			}
//		}
		
		return inMemStore;
	}
	
	private PKSLPlanner getPlanner() {
		GenRowStruct allNouns = getNounStore().getNoun(PkslDataTypes.PLANNER.toString());
		PKSLPlanner planner = null;
		if(allNouns != null) {
			planner = (PKSLPlanner) allNouns.get(0);
			return planner;
		} else {
			return this.planner;
		}
	}

}
