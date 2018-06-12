package prerna.sablecc2.translations;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import prerna.om.Insight;
import prerna.sablecc2.LazyTranslation;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AssignmentReactor;
import prerna.sablecc2.reactor.GenericReactor;
import prerna.sablecc2.reactor.IReactor;
import prerna.sablecc2.reactor.map.AbstractMapReactor;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.reactor.qs.WithReactor;
import prerna.sablecc2.reactor.qs.source.GoogleFileRetrieverReactor;

public class AbstractDatasourceModificationTranslation extends LazyTranslation {

	// create a variable to keep track of the current mapping of the original expression to the encoded expression
	public HashMap<String, String> encodedToOriginal = new HashMap<String, String>();
	
	protected static Set<String> importTypes = new HashSet<String>();

	static {
		importTypes.add("Database");
		importTypes.add("FileRead");
		importTypes.add("GoogleSheetSource");
	}
	
	public AbstractDatasourceModificationTranslation(Insight insight) {
		super(insight);
	}
	
	protected void deInitReactor()
	{
		Object parent = curReactor.Out();
		
		//set the curReactor
		prevReactor = curReactor;
    	if(parent != null && parent instanceof IReactor) {
    		curReactor = (IReactor) parent;
    	} else {
    		curReactor = null;
    	}
    	
    	boolean isQs = false;
    	boolean requireExec = false;
    	// need to merge generic reactors & maps for proper input setting
    	if(prevReactor !=null && (prevReactor instanceof GenericReactor || prevReactor instanceof AbstractMapReactor)) {
    		requireExec = true;
    	}
    	// need to merge qs
    	else if(prevReactor != null && prevReactor instanceof AbstractQueryStructReactor) {
    		if(prevReactor instanceof GoogleFileRetrieverReactor || prevReactor instanceof WithReactor) {
    			//TODO: need to figure out google sheets!!!!
    			//TODO: need to figure out google sheets!!!!
    			//TODO: need to figure out google sheets!!!!
    			//TODO: need to figure out google sheets!!!!
    			//TODO: need to figure out google sheets!!!!
    			isQs = false;
    		} else {
    			isQs = true;
    		}
		} 
    	
		// only want to execute for qs
		if(isQs || requireExec) {
			NounMetadata output = prevReactor.execute();
			
			// synchronize the result to the parent reactor
			if(output != null) {
	    		if(curReactor != null && !(curReactor instanceof AssignmentReactor)) {
	    			// add the value to the parent's curnoun
	    			curReactor.getCurRow().add(output);
		    	} else {
		    		//otherwise if we have an assignment reactor or no reactor then add the result to the planner
		    		this.planner.addVariable("$RESULT", output);
		    	}
	    	} else {
	    		this.planner.removeVariable("$RESULT");
	    	}
		}
	}

}
