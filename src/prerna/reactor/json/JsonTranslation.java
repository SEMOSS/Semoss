package prerna.reactor.json;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.om.Insight;
import prerna.reactor.AssignmentReactor;
import prerna.reactor.Assimilator;
import prerna.reactor.IReactor;
import prerna.reactor.IfReactor;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.node.AOperation;
import prerna.sablecc2.node.POtherOpInput;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class JsonTranslation extends LazyJsonTranslation {

	private static final Logger LOGGER = LogManager.getLogger(JsonTranslation.class.getName());

	protected PixelRunner runner;
	boolean validate = true;
	Object finalOutput = null;

	public JsonTranslation(PixelRunner runner, Insight insight) {
		super(insight);
		if(insight.getDataMaker() != null) {
			this.planner.addProperty("FRAME", "FRAME", insight.getDataMaker());
		}
		this.runner = runner;
	}

	public JsonTranslation(PixelRunner runner) {
		if(insight != null && insight.getDataMaker() != null) {
			this.planner.addProperty("FRAME", "FRAME", insight.getDataMaker());
		}
		this.runner = runner;
	}

	@Override
	public void caseAOperation(AOperation node) {
        inAOperation(node);
        if(node.getId() != null)
        {
            node.getId().apply(this);
        }
        if(node.getLPar() != null)
        {
        	node.getLPar().apply(this);
        }
        if(node.getOpInput() != null)
        {
        	node.getOpInput().apply(this);
        }
        {
        	if(curReactor instanceof IfReactor) {
        		boolean caseBool = ((IfReactor) curReactor).getBooleanEvaluation();
        		// case bool tells us if we should run the true case or the false case
        		// index 0 is the true case, index 1 is the false case
    			List<POtherOpInput> copy = new ArrayList<POtherOpInput>(node.getOtherOpInput());
        		if(!caseBool) {
    	            if(copy.size() >= 2) {
    	            	POtherOpInput e = copy.get(1);
    	            	e.apply(this);
    	            } else {
    	            	// wtf.. why would have you have more????
    	            	// syntax must always be if(boolean, true case, [optional] false case)
    	            }
        		} else {
        			POtherOpInput e = copy.get(0);
        			e.apply(this);
        		}
        	} else {
        		List<POtherOpInput> copy = new ArrayList<POtherOpInput>(node.getOtherOpInput());
        		for(POtherOpInput e : copy)
        		{
        			e.apply(this);
        		}
        	}
        }
        if(node.getRPar() != null)
        {
        	node.getRPar().apply(this);
        }
        if(node.getAsop() != null)
        {
        	node.getAsop().apply(this);
        }
        outAOperation(node);
	}

    protected void deInitReactor()
    {
    	// couple of things I need to do here
    	// a. see if this is a reduce operation if so.. execute the reactor at once
    	// b. see if it can be done all through the native query
    	// c. If it can't be done as native query <--- This will be the last piece I will come to
    	// get the operation signature to be executed along with the genRowStruct
    	// all of this logic is sitting in the out method
    	
    	// time for the last piece or may be ?
    	// this is an all out codagen for now - mostly
    	// Couple of things we need
    	// we need some kind of a java class where I can continue to plug what I need to plug
    	// - Need a query method which returns a row iterator
    	// - You can pass in a query at any time and it would return that row iterator
    	// - Each of the pieces are divided into blocks
    	// - There needs to be something that gets you the blocks every single time
    	// - The top of the block is always a query
    	// - and the end of the block is either a query or it is some kind of a reduce value.. we should possibly keep this as an object as well
    	// - When the expression is typically added, it should add the components i.e. the projectors to the block and add the other portion of the code to the block to indicate
    	// this is what will be run at every block
    	// Question - What decides the separation of blocks
    	// a. Reduce operation - This should include the update and insert into the frame if you would like the to think of it that way.. 
    	// b. FlatMap operation - This means something else being added
    	// c. It also needs to follow George's logic in terms of breaking it into components
    	
    	
    	
    	if(curReactor != null)
    	{
	    	Object parent = curReactor.Out();
	    	
	    	//TODO : i hate these special case checks....how do we make this more elegant
	    	if(parent instanceof Assimilator) {
	    		// if our parent is an assimilator
	    		// we want to not execute but put the curReactor as a lamda
	    		((Assimilator) parent).getCurRow().add(new NounMetadata(curReactor, PixelDataType.LAMBDA));
	    		// when the assimilator executes
	    		// it will call super.execute
	    		// which will evaluate this reactor and do a string replace
	    		// on the signature
	    		curReactor = (Assimilator)parent;
	    		return;
	    	}
	    	
	    	NounMetadata output = null;
	    	try {
	    		output = curReactor.execute();
	    	} catch(Exception e) {
	    		e.printStackTrace();
				throw new IllegalArgumentException(e.getMessage());
			}
	    	this.planner = curReactor.getPixelPlanner();
	    	
	    	//set the curReactor
	    	if(parent != null && parent instanceof IReactor) {
	    		curReactor = (IReactor)parent;
	    	} else {
	    		// print everything at this point
	    		//Hashtable thisHash = curReactor.getNounStore().getDataHash();
	    		boolean error = ((GreedyJsonReactor)curReactor).isError();
	    		
	    		finalOutput = ((GreedyJsonReactor)curReactor).getCleanOutput();
	    		//runner.addResult("$RESULT", result, error);
	    		// if you want to print the output see here
	    		System.out.println("Error " + ((GreedyJsonReactor)curReactor).getJsonOutput());
	    		
	    		// need a separate way to pump this into json
	    		// would be kind of like hasError
	    		toJson(finalOutput);
	    		
	    		curReactor = null;
	    		
	    	}
	    	
	    	// we will merge up to the parent if one is present
	    	// otherwise, we will store the result in the planner for future use
	    	// the beginning of the pksl command the beginning of each pipe is an independent routine and doesn't have a parent
	    	// these will push their output to the result in the pksl planner
	    	// if a routine does have children, we will push the to the result directly to the parent
	    	// so the out of the parent can utilize it for its execution
	    	// ( ex. Select(Studio, Sum(Movie_Budget) where the Sum is a child of the Select reactor )
	    	
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
	
	protected void postProcess(String pkslExpression) {
		// get the noun meta result
		// set that in the runner for later retrieval
		// if it is a frame
		// set it as the frame for the runner
		NounMetadata noun = planner.getVariableValue("$RESULT");
		if(noun != null) {
			this.runner.addResult(pkslExpression, noun, this.isMeta);
			// if there was a previous result
			// remove it
			this.planner.removeVariable("$RESULT");
		} 
		else {
			this.runner.addResult(pkslExpression, new NounMetadata("no output", PixelDataType.CONST_STRING), this.isMeta);
		}
		curReactor = null;
		prevReactor = null;
	}
}
