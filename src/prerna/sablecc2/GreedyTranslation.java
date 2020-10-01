package prerna.sablecc2;

import java.util.ArrayList;
import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.om.Insight;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.node.AIdWordOrId;
import prerna.sablecc2.node.AOperation;
import prerna.sablecc2.node.POtherOpInput;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AssignmentReactor;
import prerna.sablecc2.reactor.Assimilator;
import prerna.sablecc2.reactor.IReactor;
import prerna.sablecc2.reactor.IfReactor;
import prerna.sablecc2.reactor.expression.IfError;
import prerna.sablecc2.reactor.imports.ImportSizeRetrictions;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.reactor.qs.filter.FilterReactor;
import prerna.sablecc2.reactor.qs.selectors.QuerySelectorExpressionAssimilator;
import prerna.sablecc2.reactor.qs.selectors.SelectReactor;
import prerna.sablecc2.reactor.qs.source.FrameReactor;
import prerna.sablecc2.reactor.utils.RemoveVariableReactor;

public class GreedyTranslation extends LazyTranslation {

	protected PixelRunner runner;

	public GreedyTranslation(PixelRunner runner, Insight insight) {
		super(insight);
		this.runner = runner;
	}
	
    /**
     * This is greedy because we get the variable value (which means if its an embedded reactor we call execute)
     * instead of just passing the noun which points to a lambda
     */
    @Override
    public void inAIdWordOrId(AIdWordOrId node)
    {
    	defaultIn(node);
    	String idInput = (node.getId()+"").trim();
    	
    	if(this.planner.hasVariable(idInput)) {
    		NounMetadata varValue = this.planner.getVariableValue(idInput);
    		PixelDataType varType = varValue.getNounType();
    		if(curReactor != null) {
    			if(curReactor instanceof RemoveVariableReactor) {
    				curReactor.getCurRow().addLiteral(idInput);
    			}
    			// we will do just a little bit of value validation
    			// so that we only push in basic data types
    			else if(curReactor instanceof SelectReactor 
    					|| curReactor instanceof QuerySelectorExpressionAssimilator 
    					|| curReactor instanceof FilterReactor) {
    				if(varType == PixelDataType.CONST_STRING || varType == PixelDataType.CONST_INT || varType == PixelDataType.CONST_DECIMAL
    						|| varType == PixelDataType.CONST_DATE || varType == PixelDataType.CONST_TIMESTAMP 
    						// also account for complex expressions!
    						|| varType == PixelDataType.TASK
    						|| varType == PixelDataType.FORMATTED_DATA_SET) {
    					// this is a basic type
    					// or a task which needs to be flushed out
    					// so i can probably add it to the 
    					// query to work properly
    					curReactor.getCurRow().add(varValue);
    				} else {
    					// if this is not a basic type
    					// will probably break a query
    					// so pass in the original stuff
    					QueryColumnSelector s = new QueryColumnSelector(idInput);
    					curReactor.getCurRow().addColumn(s);
    				}
    			} else if(curReactor instanceof FrameReactor && varType == PixelDataType.FRAME) {
    				// add the frame based on the variable
    				curReactor.getCurRow().add(varValue);
    			} else if(curReactor instanceof AbstractQueryStructReactor) {
    				// if it is a join or an as 
					curReactor.getCurRow().addColumn(idInput);
    			} else {
    				// let the assimilator handle the variables by defining it in the generated code
    				if(curReactor instanceof Assimilator) {
        				curReactor.getCurRow().addColumn(idInput);
    				} else {
        				curReactor.getCurRow().add(varValue);
    				}
    			}
    		} else {
    			if(varType == PixelDataType.FRAME) {
    				this.currentFrame = (ITableDataFrame) varValue.getValue();
    			}
    			this.planner.addVariable(this.resultKey, varValue);
    		}
    	} else {
    		if(curReactor != null) {
    			if(curReactor instanceof SelectReactor 
    					|| curReactor instanceof QuerySelectorExpressionAssimilator 
    					|| curReactor instanceof FilterReactor) {
    				// this is part of a query 
    				// add it as a proper query selector object
    				QueryColumnSelector s = new QueryColumnSelector(idInput);
					curReactor.getCurRow().addColumn(s);
    			} else {
    				curReactor.getCurRow().addColumn(idInput);
    				curReactor.setProp(node.toString().trim(), idInput);
    			}
    		} else {
    			// i guess we should return the actual column from the frame?
    			// TODO: build this out
    			// for now, will just return the column name again... 
    			NounMetadata noun = new NounMetadata(idInput, PixelDataType.CONST_STRING);
    			this.planner.addVariable(this.resultKey, noun);
    		}
    	}
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
        if(curReactor instanceof IfError) {
        	try {
	        	if(node.getOpInput() != null)
	            {
	            	node.getOpInput().apply(this);
	            }
        	} catch(Exception ex) {
        		// hey, we got an error
        		// so i will do the other stuff
        		// but i also need to update the reference
        		// since the curReactor has changed
        		IReactor parentR = this.curReactor.getParentReactor();
        		while(!(parentR instanceof IfError)) {
        			parentR = parentR.getParentReactor();
        		}
        		curReactor = parentR;
        		List<POtherOpInput> copy = new ArrayList<POtherOpInput>(node.getOtherOpInput());
        		for(POtherOpInput e : copy)
        		{
        			e.apply(this);
        		}
        	}
    	} 
        // need to account for if error
        else {
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
	    	
	    	NounMetadata output = curReactor.execute();
	    	this.planner = curReactor.getPixelPlanner();

	    	// this now becomes the prev reactor
    		this.prevReactor = this.curReactor;
	    	//set the curReactor
	    	if(parent != null && parent instanceof IReactor) {
	    		this.curReactor = (IReactor) parent;
	    	} else {
	    		this.curReactor = null;
	    	}
	    	
	    	// we will merge up to the parent if one is present
	    	// otherwise, we will store the result in the planner for future use
	    	// the beginning of the pixel command the beginning of each pipe is an independent routine and doesn't have a parent
	    	// these will push their output to the result in the pixel planner
	    	// if a routine does have children, we will push the to the result directly to the parent
	    	// so the out of the parent can utilize it for its execution
	    	// ( ex. Select(Studio, Sum(Movie_Budget) where the Sum is a child of the Select reactor )
	    	
	    	if(output != null) {
	    		// size check
	    		if(output.getNounType() == PixelDataType.FRAME) {
	    			ITableDataFrame frame = (ITableDataFrame) output.getValue();
	    			if(!ImportSizeRetrictions.frameWithinLimits(frame)) {
	    				SemossPixelException exception = new SemossPixelException(
		    					new NounMetadata("Frame size is too large, please limit the data size before proceeding", 
		    							PixelDataType.CONST_STRING, 
		    							PixelOperationType.FRAME_SIZE_LIMIT_EXCEEDED, PixelOperationType.ERROR));
		    				exception.setContinueThreadOfExecution(false);
		    				throw exception;
	    			}
	    		}
	    		
	    		if(curReactor != null && !(curReactor instanceof AssignmentReactor)) {
	    			// add the value to the parent's curnoun
	    			curReactor.getCurRow().add(output);
		    	} else {
		    		//otherwise if we have an assignment reactor or no reactor then add the result to the planner
		    		this.planner.addVariable(this.resultKey, output);
		    	}
	    	} else {
	    		this.planner.removeVariable(this.resultKey);
	    	}
    	}
    }
    
	protected void postProcess(String pixelExpression) {
		super.postProcess(pixelExpression);
		// get the noun meta result
		// set that in the runner for later retrieval
		// if it is a frame
		// set it as the frame for the runner
		NounMetadata noun = planner.getVariableValue(this.resultKey);
		if(noun != null) {
			this.runner.addResult(pixelExpression, noun, this.isMeta);
			// if there was a previous result
			// remove it
			this.planner.removeVariable(this.resultKey);
		} 
		else {
			this.runner.addResult(pixelExpression, new NounMetadata("no output", PixelDataType.CONST_STRING), this.isMeta);
		}
		this.curReactor = null;
		this.prevReactor = null;
		this.isMeta = false;
	}
	
	protected void postRuntimeErrorProcess(String pixelExpression, NounMetadata errorNoun, List<String> unexecutedPixels) {
		errorNoun.addAdditionalReturn(new NounMetadata(unexecutedPixels, PixelDataType.CONST_STRING, PixelOperationType.UNEXECUTED_PIXELS));
		this.runner.addResult(pixelExpression, errorNoun, this.isMeta);
		this.curReactor = null;
		this.prevReactor = null;
		this.isMeta = false;
	}
	
	protected void postRuntimeErrorProcess(String pixelExpression, NounMetadata errorNoun, boolean isMeta) {
		this.runner.addResult(pixelExpression, errorNoun, this.isMeta);
		this.curReactor = null;
		this.prevReactor = null;
		this.isMeta = false;
	}
}
