package prerna.sablecc2;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ds.h2.H2Frame;
import prerna.sablecc2.node.APlainRow;
import prerna.sablecc2.node.POthercol;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.AssignmentReactor;
import prerna.sablecc2.reactor.Assimilator;
import prerna.sablecc2.reactor.IReactor;
import prerna.sablecc2.reactor.IfReactor;
import prerna.sablecc2.reactor.PKSLPlanner;
import prerna.ui.components.playsheets.datamakers.IDataMaker;

public class GreedyTranslation extends LazyTranslation {

	private static final Logger LOGGER = LogManager.getLogger(GreedyTranslation.class.getName());

	protected PKSLRunner runner;

	public GreedyTranslation() {
		this.planner = new PKSLPlanner();
		this.planner.addProperty("FRAME", "FRAME", new H2Frame());
		this.runner = new PKSLRunner();
	}
	
	public GreedyTranslation(PKSLRunner runner) {
		this.planner = new PKSLPlanner();
		this.planner.addProperty("FRAME", "FRAME", new H2Frame());
		this.runner = runner;
	}
	
	public GreedyTranslation(PKSLRunner runner, VarStore varStore) {
		this.planner = new PKSLPlanner();
		this.planner.setVarStore(varStore);
		this.planner.addProperty("FRAME", "FRAME", new H2Frame());
		this.runner = runner;
	}
	
	public GreedyTranslation(PKSLRunner runner, IDataMaker dataMaker) {
		this.planner = new PKSLPlanner(dataMaker);
		this.planner.addProperty("FRAME", "FRAME", dataMaker);
		this.runner = runner;
	}
	
	public GreedyTranslation(PKSLRunner runner, VarStore varStore, IDataMaker dataMaker) {
		this.planner = new PKSLPlanner(dataMaker);
		this.planner.setVarStore(varStore);
		this.planner.addProperty("FRAME", "FRAME", dataMaker);
		this.runner = runner;
	}
	
    @Override
    public void caseAPlainRow(APlainRow node)
    {
        inAPlainRow(node);
        if(node.getLPar() != null)
        {
            node.getLPar().apply(this);
        }
        if(node.getColDef() != null)
        {
            node.getColDef().apply(this);
        }
        {
        	if(curReactor instanceof IfReactor) {
        		boolean caseBool = ((IfReactor) curReactor).getBooleanEvaluation();
        		// case bool tells us if we should run the true case or the false case
        		// index 0 is the true case, index 1 is the false case
    			List<POthercol> copy = new ArrayList<POthercol>(node.getOthercol());
        		if(!caseBool) {
    	            if(copy.size() >= 2) {
    	            	POthercol e = copy.get(1);
    	            	e.apply(this);
    	            } else {
    	            	// wtf.. why would have you have more????
    	            	// syntax must always be if(boolean, true case, [optional] false case)
    	            }
        		} else {
        			POthercol e = copy.get(0);
        			e.apply(this);
        		}
        	}
        	else {
	            List<POthercol> copy = new ArrayList<POthercol>(node.getOthercol());
	            for(POthercol e : copy)
	            {
	                e.apply(this);
	            }
        	}
        }
        if(node.getRPar() != null)
        {
            node.getRPar().apply(this);
        }
        outAPlainRow(node);
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
	    		((Assimilator) parent).getCurRow().add(new NounMetadata(curReactor, PkslDataTypes.LAMBDA));
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
	    		//should we make an error noun?
	    	}
	    	this.planner = ((AbstractReactor)curReactor).planner;
	    	
	    	//set the curReactor
	    	if(parent != null && parent instanceof IReactor) {
	    		curReactor = (IReactor)parent;
	    	} else {
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
	
	protected void postProcess() {
		// get the noun meta result
		// set that in the runner for later retrieval
		// if it is a frame
		// set it as the frame for the runner
		NounMetadata noun = planner.getVariableValue("$RESULT");
		if(noun != null) {
			this.runner.setResult(noun);
			Object frameNoun = noun.getValue();
			if(frameNoun instanceof IDataMaker){
				IDataMaker frame = (IDataMaker) frameNoun;
				this.runner.setDataFrame(frame);
			}
			// if there was a previous result
			// remove it
			this.planner.removeVariable("$RESULT");
		} else {
			this.runner.setResult(null);
		}
		curReactor = null;
		prevReactor = null;
		lastOperation = null;
	}
}
