package prerna.sablecc2;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.Insight;
import prerna.om.Pixel;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.reactor.AssignmentReactor;
import prerna.reactor.Assimilator;
import prerna.reactor.EmbeddedRoutineReactor;
import prerna.reactor.EmbeddedScriptReactor;
import prerna.reactor.GenericReactor;
import prerna.reactor.IReactor;
import prerna.reactor.IfReactor;
import prerna.reactor.NegReactor;
import prerna.reactor.PixelPlanner;
import prerna.reactor.PowAssimilator;
import prerna.reactor.ReactorFactory;
import prerna.reactor.VectorReactor;
import prerna.reactor.expression.filter.OpAnd;
import prerna.reactor.expression.filter.OpFilter;
import prerna.reactor.expression.filter.OpOr;
import prerna.reactor.frame.filter.AbstractFilterReactor;
import prerna.reactor.map.MapListReactor;
import prerna.reactor.map.MapReactor;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.reactor.qs.GroupReactor;
import prerna.reactor.qs.filter.FilterReactor;
import prerna.reactor.qs.filter.IfQueryFilterComponentAnd;
import prerna.reactor.qs.filter.IfQueryFilterComponentOr;
import prerna.reactor.qs.filter.QueryFilterComponentAnd;
import prerna.reactor.qs.filter.QueryFilterComponentOr;
import prerna.reactor.qs.filter.QueryFilterComponentSimple;
import prerna.reactor.qs.selectors.QuerySelectorExpressionAssimilator;
import prerna.reactor.qs.selectors.SelectReactor;
import prerna.reactor.qs.source.FrameReactor;
import prerna.reactor.runtime.JavaReactor;
import prerna.sablecc2.analysis.DepthFirstAdapter;
import prerna.sablecc2.node.AAsop;
import prerna.sablecc2.node.AAssignRoutine;
import prerna.sablecc2.node.AAssignment;
import prerna.sablecc2.node.ABaseSimpleComparison;
import prerna.sablecc2.node.ABasicAndComparisonTerm;
import prerna.sablecc2.node.ABasicOrComparisonTerm;
import prerna.sablecc2.node.ABooleanScalar;
import prerna.sablecc2.node.ACommentExpr;
import prerna.sablecc2.node.AComplexAndComparisonExpr;
import prerna.sablecc2.node.AComplexOrComparisonExpr;
import prerna.sablecc2.node.ADivBaseExpr;
import prerna.sablecc2.node.ADotcol;
import prerna.sablecc2.node.AEmbeddedScriptchainExprComponent;
import prerna.sablecc2.node.AExplicitRel;
import prerna.sablecc2.node.AExplicitRelComparator;
import prerna.sablecc2.node.AFormula;
import prerna.sablecc2.node.AFractionDecimal;
import prerna.sablecc2.node.AHelpExpr;
import prerna.sablecc2.node.AIdWordOrId;
import prerna.sablecc2.node.AImplicitRel;
import prerna.sablecc2.node.AJavaOp;
import prerna.sablecc2.node.AListRegTerm;
import prerna.sablecc2.node.AMainCommentRoutine;
import prerna.sablecc2.node.AMap;
import prerna.sablecc2.node.AMapList;
import prerna.sablecc2.node.AMapNegNum;
import prerna.sablecc2.node.AMapVar;
import prerna.sablecc2.node.AMetaRoutine;
import prerna.sablecc2.node.AMinusBaseExpr;
import prerna.sablecc2.node.AModBaseExpr;
import prerna.sablecc2.node.AMultBaseExpr;
import prerna.sablecc2.node.ANegTerm;
import prerna.sablecc2.node.ANoun;
import prerna.sablecc2.node.ANullScalar;
import prerna.sablecc2.node.AOperation;
import prerna.sablecc2.node.AOutputRoutine;
import prerna.sablecc2.node.APlusBaseExpr;
import prerna.sablecc2.node.APower;
import prerna.sablecc2.node.AProp;
import prerna.sablecc2.node.ARcol;
import prerna.sablecc2.node.ARoutineConfiguration;
import prerna.sablecc2.node.ASubRoutine;
import prerna.sablecc2.node.AWholeDecimal;
import prerna.sablecc2.node.AWordMapKey;
import prerna.sablecc2.node.AWordWordOrId;
import prerna.sablecc2.node.Node;
import prerna.sablecc2.node.PRoutine;
import prerna.sablecc2.node.PSubRoutineOptions;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.ITask;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.util.Constants;
import prerna.util.insight.InsightUtility;
import prerna.util.usertracking.IUserTracker;
import prerna.util.usertracking.UserTrackerFactory;

public class LazyTranslation extends DepthFirstAdapter {

	private static final Logger logger = LogManager.getLogger(LazyTranslation.class);
	
	protected PixelPlanner planner;
	protected Insight insight;
	protected IReactor curReactor = null;
	protected IReactor prevReactor = null;

	protected String resultKey = null;
	
	// if we have META and a variable definition
	// we need to not record this variable
	// this includes embedded expressions
	protected List<String> metaVariables = new Vector<String>();
	protected Map<String, NounMetadata> prevVariables = new HashMap<String, NounMetadata>();
	
	public enum TypeOfOperation {PIPELINE, COMPOSITION};
	protected TypeOfOperation operationType = TypeOfOperation.COMPOSITION;
	
	protected ITableDataFrame currentFrame = null;
	protected Pixel pixelObj = null;
	
	public static String envClassPath = null;
	
	public LazyTranslation() {
		this.planner = new PixelPlanner();
	}
	
	public LazyTranslation(Insight insight) {
		this();
		this.insight = insight;
		if(this.insight != null) {
			this.planner.setVarStore(this.insight.getVarStore());
		}
	}
	
	/**
	 * Do nothing - just to override in other translations
	 * @param pixelExpression
	 */
	protected void postProcess(String pixelExpression) {
		// need to account for META variables
		// that were defined
//		if(this.isMeta) {
//			for(String var : this.metaVariables) {
//				this.planner.removeVariable(var);
//			}
//			
//			// now add back the other variables we had to keep
//			for(String var : this.prevVariables.keySet()) {
//				this.planner.addVariable(var, this.prevVariables.get(var));
//			}
//		}
	}
	
	protected void postRuntimeErrorProcess(String pixelExpression, NounMetadata errorNoun, List<String> unexecutedPixels) {
		// do nothing
		// only the lazy implements this
	}
	
	public PixelPlanner getPlanner() {
		return this.planner;
	}
	
	/*
	 * This is the main level method to execute any pixel operations
	 */
	
	@Override
	public void caseARoutineConfiguration(ARoutineConfiguration node) {
        List<PRoutine> copy = new ArrayList<PRoutine>(node.getRoutine());
        int size = copy.size();
        for(int pixelstep = 0; pixelstep < size; pixelstep++)
        {
        	PRoutine e = copy.get(pixelstep);
        	try {
        		// we will start to keep track of some metadata
        		// at the start of each pixel being processed
        		this.resultKey = "$RESULT_" + e.hashCode();
        		this.pixelObj = new Pixel("tempStorage", e.toString());
        		// if not META, store the starting frame headers
        		if(!(e instanceof AMetaRoutine)) {
            		this.pixelObj.setStartingFrameHeaders(InsightUtility.getAllFrameHeaders(this.planner.getVarStore()));
        		}
        		
        		// actually run the operation
        		e.apply(this);
        		
        		// reset the state of the frame
        		this.currentFrame = null;
        	} catch(SemossPixelException ex) {
        		trackError(e.toString(), this.pixelObj.isMeta(), ex);
        		logger.error(Constants.STACKTRACE, ex);
        		// if we want to continue the thread of execution
        		// nothing special
        		// just add the error to the return
        		if(ex.isContinueThreadOfExecution()) {
        			planner.addVariable(this.resultKey, ex.getNoun());
            		postProcess(e.toString().trim());
        		} else {
        			// if we do want to stop
        			// propagate the error up and the PixelRunner
        			// will handle grabbing the meta and returning it to the FE
        			postRuntimeErrorProcess(e.toString(), ex.getNoun(), 
        					copy.subList(pixelstep+1, size).stream().map(p -> p.toString()).collect(Collectors.toList()));
        			throw ex;
        		}
        	} catch(Exception ex) {
        		trackError(e.toString(), this.pixelObj.isMeta(), ex);
        		logger.error(Constants.STACKTRACE, ex);
        		planner.addVariable(this.resultKey, new NounMetadata(ex.getMessage(), PixelDataType.ERROR, PixelOperationType.ERROR));
        		postProcess(e.toString().trim());
        	}
        }
	}
	
	@Override
	public void caseASubRoutine(ASubRoutine node) {
		inASubRoutine(node);
		{
			List<PSubRoutineOptions> copy = new ArrayList<PSubRoutineOptions>(node.getSubRoutineOptions());
			int counter = 0;
			for(PSubRoutineOptions e : copy)
			{
				e.apply(this);
				// we will move the output from this one assignment from the curRow
				// and move it into the nounstore which will be returned
				// in the reactor execute
				NounMetadata thisNoun = this.curReactor.getCurRow().remove(0);
				this.curReactor.getNounStore().makeNoun(counter + "").add(thisNoun);
				counter++;
			}
		}
		outASubRoutine(node);
	}

	/**
	 * Track the error
	 * @param pixel
	 * @param ex
	 */
	protected void trackError(String pixel, boolean meta, Exception ex) {
		if(this.insight != null) {
			IUserTracker tracker = UserTrackerFactory.getInstance();
			if(tracker.isActive()) {
				String curReactorName = null;
				String parentReactorName = null;
				if(this.curReactor != null) {
					curReactorName = this.curReactor.getClass().getName();
					IReactor parentReactor = this.curReactor.getParentReactor();
					if(parentReactor != null) {
						parentReactorName = parentReactor.getClass().getName();
					}
				}
				tracker.trackError(this.insight, pixel, curReactorName, parentReactorName, meta, ex);
			}
		}
	}

	@Override
	public void inAHelpExpr(AHelpExpr node) {
		String reactorId = node.getId().getText();
		IReactor reactor = getReactor(reactorId, node.toString());
		NounMetadata noun = new NounMetadata(reactor.getHelp(), PixelDataType.CONST_STRING, PixelOperationType.HELP);
		if(curReactor == null) {
			this.planner.addVariable(this.resultKey, noun);
		} else {
			curReactor.getCurRow().add(noun);
		}
	}
	
	@Override
	public void inAOutputRoutine(AOutputRoutine node) {
		defaultIn(node);
	}
	
	@Override
	public void outAOutputRoutine(AOutputRoutine node) {
		defaultOut(node);
        // we do this in case someone is doing an embedded assignment 
        // instead of just doing a normal assignment...
        if(!(curReactor instanceof AssignmentReactor)) {
    		postProcess(node.toString().trim());
    	}
	}
	
	@Override
	public void inAMetaRoutine(AMetaRoutine node) {
		defaultIn(node);
		this.pixelObj.setMeta(true);
	}

    public void outAMetaRoutine(AMetaRoutine node) {
    	defaultOut(node);
        // we do this in case someone is doing an embedded assignment 
        // instead of just doing a normal assignment...
        if(!(curReactor instanceof AssignmentReactor)) {
    		postProcess(node.toString().trim());
    	}
    }
    
	@Override
	public void inAAssignRoutine(AAssignRoutine node) {
		defaultIn(node);
	}
	
	@Override
	public void outAAssignRoutine(AAssignRoutine node) {
		defaultOut(node);
        // we do this in case someone is dumb and is doing an embedded assignment 
        // instead of just doing a normal assignment...
    	postProcess(node.toString().trim());
	}
	
    @Override
    public void inAAssignment(AAssignment node) {
    	// if this is a META
    	// we need to store the original assignment if it exists
    	// and we need to make sure we clean this up at the end
    	defaultIn(node);
    	String var = node.getId().toString().trim();

//    	if(this.pixelObj.isMeta()) {
//    		if(this.planner.hasVariable(var)) {
//    			this.prevVariables.put(var, this.planner.getVariable(var));
//    		}
//    		// make sure we remove this at the end
//    		this.metaVariables.add(var);
//    	}
    	
    	AssignmentReactor assignmentReactor = new AssignmentReactor(this.resultKey);
        assignmentReactor.setPixel(var, node.toString().trim());
    	initReactor(assignmentReactor);
    	assignmentReactor.getCurRow().addLiteral(var);
    }

    @Override
    public void outAAssignment(AAssignment node) {
        defaultOut(node);
    	deInitReactor();
    }
	
	@Override
	public void inASubRoutine(ASubRoutine node) {
		defaultIn(node);
		EmbeddedRoutineReactor embeddedScriptReactor = new EmbeddedRoutineReactor();
		embeddedScriptReactor.setPixel(node.getSubRoutineOptions().toString().trim(), node.toString().trim());
    	initReactor(embeddedScriptReactor);
	}
	
	@Override
	public void outASubRoutine(ASubRoutine node) {
		defaultOut(node);
		deInitReactor();
	}
	
	@Override
	public void inAMainCommentRoutine(AMainCommentRoutine node) {
		String comment = node.getComment().getText();
		NounMetadata noun = new NounMetadata(comment, PixelDataType.CONST_STRING, PixelOperationType.RECIPE_COMMENT);
		this.planner.addVariable(this.resultKey, noun);
		postProcess(node.toString().trim());
	}
	
	@Override
	public void inACommentExpr(ACommentExpr node) {
		String comment = node.getComment().getText();
		NounMetadata noun = new NounMetadata(comment, PixelDataType.CONST_STRING, PixelOperationType.RECIPE_COMMENT);
		if(curReactor == null) {
			this.planner.addVariable(this.resultKey, noun);
		} else {
			curReactor.getCurRow().add(noun);
		}
	}
	
    @Override
    public void inAEmbeddedScriptchainExprComponent(AEmbeddedScriptchainExprComponent node) {
    	defaultIn(node);
		EmbeddedScriptReactor embeddedScriptReactor = new EmbeddedScriptReactor();
		embeddedScriptReactor.setPixel(node.getMandatoryScriptchain().toString().trim(), node.toString().trim());
    	initReactor(embeddedScriptReactor);
    }
    
    @Override
    public void outAEmbeddedScriptchainExprComponent(AEmbeddedScriptchainExprComponent node) {
    	defaultOut(node);
		deInitReactor();
    }
	
	//////////////////////////////////////////
	//////////////////////////////////////////
	// Below is consolidated into a single command type
	// called "Operation"
	
    // all the operation sits here - these are the script starting points
    // everything from here on is primarily an assimilation
    
    // this is the base frameop that is sitting
    // this is the highest level for all purposes
    // this in turn could have other frameops  
    
    // in the out functions of each of these, it needs to check if the gen row from the child i.e. could be another frame op or something
    // and evaluates 2 things - is this a reduce operation and is this fully sqlable
    // if either one of these, it needs to execute that before we can execute this one
    // not sure how to keep the output in some kind of no named variable
	

	@Override
	public void inAOperation(AOperation node) {
		defaultIn(node);
        logger.debug("Starting an operation " + node.toString());
        
        String reactorId = node.getId().toString().trim();
        IReactor newReactor = getReactor(reactorId, node.toString().trim());
        initReactor(newReactor);
        syncResult();
	}
	
	@Override
	public void outAOperation(AOperation node) {
		defaultOut(node);
		logger.debug("Ending operation " + node.toString());
    	// if we have an if reactor
    	// we will not deInit and just add this as a lambda within
    	// the if statement and only deinit when necessary
        if(curReactor.getParentReactor() instanceof IfReactor)
    	{
    		curReactor.getParentReactor().getCurRow().addLambda(curReactor);
    		curReactor = curReactor.getParentReactor();
    	}
    	// if this is not a child of an if
    	// just execute as normal
    	else
    	{
    		deInitReactor();
    	}
	}
	
    protected void syncResult() {
    	// after we init the new reactor
        // we will add the result of the last reactor 
        // into the noun store of this new reactor
    	NounMetadata prevResult = this.planner.getVariable(this.resultKey);
    	if(prevResult != null) {
    		PixelDataType nounType = prevResult.getNounType();
    		// grab the correct key for the noun when moving to the parent
    		GenRowStruct genRow = curReactor.getNounStore().makeNoun(nounType.getKey());
    		genRow.add(prevResult);
    		// then we will remove the result from the planner
        	this.planner.removeVariable(this.resultKey);
        	
        	// store the frame as an input
        	if(this.pixelObj != null && prevResult.getNounType() == PixelDataType.FRAME) {
        		try {
        			this.pixelObj.addFrameInput(((ITableDataFrame) prevResult.getValue()).getName());
        		} catch(Exception e) {
        			e.printStackTrace();
        		}
        	}
    	}
    }
	
    /************************************ SECOND is basically secondary operations ***********************/
	
	@Override
	public void inANegTerm(ANegTerm node) {
		if(curReactor instanceof Assimilator)
        {
        	return;
        }
		defaultIn(node);
    	IReactor negReactor = new NegReactor();
    	negReactor.setPixel(node.getTerm().toString().trim(), node.toString().trim());
    	initReactor(negReactor);
	}
	
	@Override
	public void outANegTerm(ANegTerm node) {
		if((node.toString()).trim().equalsIgnoreCase(curReactor.getOriginalSignature())) {
	    	deInitReactor();
		}
	}

    // setting the as value on a frame
    public void inAAsop(AAsop node)
    {
    	defaultIn(node);
    	IReactor opReactor;
    	if(this.curReactor != null && this.curReactor instanceof AbstractQueryStructReactor) {
    		opReactor = new prerna.reactor.qs.AsReactor();
    	} else {
    		opReactor = new prerna.reactor.AsReactor();
    	}
    	logger.debug("In the AS Component of frame op");
    	opReactor.setPixel("as", node.getAsOp() + "");
    	initReactor(opReactor);
    }

    public void outAAsop(AAsop node)
    {
    	defaultOut(node);
    	logger.debug("OUT the AS Component of frame op");
    	deInitReactor();
    }
    
    @Override
    public void inAJavaOp(AJavaOp node) {
    	defaultIn(node);
    	IReactor opReactor = new JavaReactor();
    	initReactor(opReactor);
    	String code = node.getJava().getText().trim();
    	// accounting for the start <j> and end </j>
    	code = code.substring(3, code.length()-4).trim();
    	opReactor.getCurRow().addLiteral(code);
    }
    
    @Override
    public void outAJavaOp(AJavaOp node) {
    	defaultOut(node);
    	deInitReactor();
    }

    @Override
    public void inANoun(ANoun node) {
    	defaultIn(node);
    	IReactor genReactor = new GenericReactor();
        genReactor.setPixel("PKSL", (node + "").trim());
        genReactor.getNounStore().makeNoun("KEY").addLiteral(node.getId().toString().trim());
        initReactor(genReactor);
    }

    @Override
    public void outANoun(ANoun node) {
    	defaultOut(node);
    	deInitReactor();
    }
    
    @Override
    public void inANullScalar(ANullScalar node) {
    	NounMetadata noun = new NounMetadata(null, PixelDataType.NULL_VALUE);
    	if(curReactor != null) {
    		curReactor.getCurRow().add(noun);
    	} else {
    		this.planner.addVariable(this.resultKey, noun);
    	}
    }
    
    @Override
    public void inAIdWordOrId(AIdWordOrId node) {
    	defaultIn(node);
    	String idInput = node.getId().getText().trim();
    	
    	if(this.planner.hasVariable(idInput)) {
    		// since this is lazy
    		// varValue might be pointing to a lambda so no real way to know what it is yet
    		NounMetadata varValue = this.planner.getVariable(idInput);
    		PixelDataType varType = varValue.getNounType();
    		if(curReactor != null) {
    			// we will do just a little bit of value validation
    			// so that we only push in basic data types
    			if(curReactor instanceof SelectReactor 
    					|| curReactor instanceof GroupReactor
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
    					|| curReactor instanceof GroupReactor  
    					|| curReactor instanceof QuerySelectorExpressionAssimilator 
    					|| curReactor instanceof FilterReactor) {
    				// this is part of a query 
    				// add it as a proper query selector object
    				QueryColumnSelector s = new QueryColumnSelector(idInput);
					curReactor.getCurRow().addColumn(s);
    			} else {
    				curReactor.getCurRow().addColumn(idInput);
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
    public void inAWordWordOrId(AWordWordOrId node) {
        defaultIn(node);
        String trimmedWord = node.getWord().toString().trim();
        String word = PixelUtility.removeSurroundingQuotes(trimmedWord);
        // also replace any escaped quotes
        word = word.replace("\\\"", "\"");
//        word = word.replace("\\\\", "\\");
       	// if its an assignment
    	// just put in results in case we are doing a |
    	if(curReactor != null && !(curReactor instanceof AssignmentReactor)) {
	        curReactor.getCurRow().addLiteral(word);
        } else {
        	NounMetadata noun = new NounMetadata(word, PixelDataType.CONST_STRING);
    		this.planner.addVariable(this.resultKey, noun);
        }
    }
    
    @Override
    public void inABooleanScalar(ABooleanScalar node) {
    	defaultIn(node);
    	String booleanStr = node.getBoolean().toString().trim();
    	Boolean bool = Boolean.parseBoolean(booleanStr);
       	// if its an assignment
    	// just put in results in case we are doing a |
    	if(curReactor != null && !(curReactor instanceof AssignmentReactor)) {
	        curReactor.getCurRow().addBoolean(bool);
        } else {
        	NounMetadata noun = new NounMetadata(bool, PixelDataType.BOOLEAN);
    		this.planner.addVariable(this.resultKey, noun);
        }
    }

    // atomic level stuff goes in here
    @Override
    public void inAWholeDecimal(AWholeDecimal node) {
    	defaultIn(node);
    	boolean isDouble = false;
    	String whole = "";
    	String fraction = "";
    	// get the whole portion
    	if(node.getWhole() != null) {
    		whole = node.getWhole().toString().trim();
    	}
    	// get the fraction portion
    	if(node.getFraction() != null) {
    		isDouble = true;
    		fraction = (node.getFraction()+"").trim();
    	} else {
    		fraction = "0";
    	}
    	BigDecimal retNum = new BigDecimal(whole + "." + fraction);
    	NounMetadata noun = null;
    	if(isDouble) {
    		noun = new NounMetadata(retNum.doubleValue(), PixelDataType.CONST_DECIMAL);
    	} else {
    		try {
    			noun = new NounMetadata(retNum.intValueExact(), PixelDataType.CONST_INT);
    		} catch(Exception e1) {
    			try {
    				noun = new NounMetadata(retNum.longValueExact(), PixelDataType.CONST_INT);
    			} catch(Exception e2) {
    	    		noun = new NounMetadata(retNum.doubleValue(), PixelDataType.CONST_DECIMAL);
    			}
    		}
    	}
    	// if its an assignment
    	// just put in results in case we are doing a |
    	if(curReactor != null && !(curReactor instanceof AssignmentReactor)) {
    		curReactor.getCurRow().add(noun);
	    	// modify the parent such that the signature has the correct
	    	// value of the numerical without any extra spaces
	    	// plain string will always modify to a integer even if we return double value
    		String replacementString = BigDecimal.valueOf(retNum.doubleValue()).toPlainString();
    		if(replacementString.endsWith(".0")) {
    			replacementString = replacementString.substring(0, replacementString.length()-2);
    		}
	    	curReactor.modifySignature(node.toString().trim(), replacementString);
    	} else {
    		// looks like you just have a number...
    		// i guess i will return this?
    		this.planner.addVariable(this.resultKey, noun);
    	}
    }

    @Override
    public void inAFractionDecimal(AFractionDecimal node) {
    	defaultIn(node);
    	String fraction = (node.getFraction()+"").trim();
		Number retNum = new BigDecimal("0." + fraction);
	   	// if its an assignment
    	// just put in results in case we are doing a |
    	if(curReactor != null && !(curReactor instanceof AssignmentReactor)) {
	    	// add the decimal to the cur row
	        curReactor.getCurRow().addDecimal(retNum.doubleValue());
	    	// modify the parent such that the signature has the correct
	    	// value of the numerical without any extra spaces
	    	// plain string will always modify to a integer even if we return double value
	    	curReactor.modifySignature(node.toString().trim(), BigDecimal.valueOf(retNum.doubleValue()).toPlainString());
    	} else {
    		NounMetadata noun = new NounMetadata(retNum.doubleValue(), PixelDataType.CONST_DECIMAL);
    		this.planner.addVariable(this.resultKey, noun);
    	}
    }
    
    @Override
    public void inADotcol(ADotcol node) {
    	defaultIn(node);
    	processAliasReference(node.getColumnName().toString().trim());
    }

    @Override
    public void inARcol(ARcol node) {
    	defaultIn(node);
    	processAliasReference(node.getColumnName().toString().trim());
    }

    private void processAliasReference(String colName) {
    	ITableDataFrame frame = (ITableDataFrame) this.insight.getDataMaker();
    	if(frame != null) {
			QueryColumnSelector colSelector = new QueryColumnSelector(colName);
    		if(curReactor != null) {
    			curReactor.getCurRow().addColumn(colSelector);
    		} else {
    			// well, this means the person just typed f$Title (for example)
    			// i guess we should just return the first 500 records of the column...
    			SelectQueryStruct qs = new SelectQueryStruct();
    			qs.addSelector(colSelector);
    			IRawSelectWrapper iterator = null;
				try {
					iterator = frame.query(qs);
				} catch (Exception e) {
	        		logger.error(Constants.STACKTRACE, e);
					throw new SemossPixelException(e.getMessage());
				}
    			ITask task = new BasicIteratorTask(qs, iterator);
    			this.insight.getTaskStore().addTask(task);
    			this.planner.addVariable(this.resultKey, new NounMetadata(task, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA));
    		}
    	}
    }
    
    @Override
    public void inAProp(AProp node) {
    	defaultIn(node);
    	IReactor genReactor = new GenericReactor();
        genReactor.setPixel("PKSL", (node + "").trim());
        genReactor.getNounStore().makeNoun("KEY").addLiteral(node.getId().toString().trim());
        initReactor(genReactor);
    }
    
    @Override
    public void outAProp(AProp node) {
    	defaultOut(node);
    	deInitReactor();
    }

    // you definitely need the other party to be in a relationship :)
    // this is of the form colDef followed by type of relation and another col def
    
    @Override
    public void inAImplicitRel(AImplicitRel node) {
    	defaultIn(node);    
    	// Need some way to track the state to say all the other things are interim
    	// so I can interpret the dot notation etc for frame columns
    }

    @Override
    public void outAImplicitRel(AImplicitRel node) {
        defaultOut(node);
    	String leftCol = node.getLcol().toString().trim();
    	String rightCol = node.getRcol().toString().trim();
    	String relType = node.getRelType().toString().trim();
    	curReactor.getCurRow().addRelation(leftCol, relType, rightCol);
    }
    
    @Override
    public void inAExplicitRelComparator(AExplicitRelComparator node) {
    	defaultIn(node);    
    	// Need some way to track the state to say all the other things are interim
    	// so I can interpret the dot notation etc for frame columns
    }
    
    @Override
    public void outAExplicitRelComparator(AExplicitRelComparator node) {
        defaultOut(node);
    	String leftCol = node.getLcol().toString().trim();
    	String rightCol = node.getRcol().toString().trim();
    	String relType = node.getRelType().toString().trim();
    	String comparator = node.getComparator().toString().trim();
    	curReactor.getCurRow().addRelation(leftCol, relType, rightCol, comparator, null);
    }
    
    @Override
    public void inAExplicitRel(AExplicitRel node) {
    	defaultIn(node);    
    	// Need some way to track the state to say all the other things are interim
    	// so I can interpret the dot notation etc for frame columns
    }

    @Override
    public void outAExplicitRel(AExplicitRel node) {
        defaultOut(node);
    	String leftCol = node.getLcol().toString().trim();
    	String rightCol = node.getRcol().toString().trim();
    	String relType = node.getRelType().toString().trim();
    	String relName = node.getRelationshipName().toString().trim();
    	curReactor.getCurRow().addRelation(leftCol, relType, rightCol, null, relName);
    }
    
    /////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////
    
    /*
     * Comparison logic
     */
    
    private IReactor getOrComparison() {
    	IReactor newReactor = null;
    	if(this.curReactor instanceof prerna.reactor.qs.selectors.IfReactor)
      	{
      		newReactor = new IfQueryFilterComponentOr(); // this is primarily a marker class
      	}
    	else if(this.curReactor != null && this.curReactor instanceof AbstractQueryStructReactor || this.curReactor instanceof AbstractFilterReactor) 
    	{
    		newReactor = new QueryFilterComponentOr();
    	}  
    	else 
    	{
    		newReactor = new OpOr();
    	}
    	
    	initReactor(newReactor);
    	syncResult();
    	return newReactor;
    }
    
    private IReactor getAndComparison() {
    	IReactor newReactor = null;
    	if(this.curReactor instanceof prerna.reactor.qs.selectors.IfReactor)
      	{
      		newReactor = new IfQueryFilterComponentAnd(); // this is primarily a marker class
      	}
    	else if(this.curReactor != null && this.curReactor instanceof AbstractQueryStructReactor || this.curReactor instanceof AbstractFilterReactor) 
    	{
    		newReactor = new QueryFilterComponentAnd();
    	}
    	else 
    	{
    		newReactor = new OpAnd();
    	}
    	
    	initReactor(newReactor);
    	syncResult();
    	return newReactor;
    }
    
    @Override
    public void inAComplexAndComparisonExpr(AComplexAndComparisonExpr node) {
    	defaultIn(node);
    	IReactor newReactor = getAndComparison();
    	newReactor.setPixel("AND_FILTER", node.toString().trim());
    }
    
    @Override
    public void outAComplexAndComparisonExpr(AComplexAndComparisonExpr node) {
    	defaultOut(node);
    	deInitReactor();
    }
    
    @Override
    public void inAComplexOrComparisonExpr(AComplexOrComparisonExpr node) {
    	defaultIn(node);
    	IReactor newReactor = getOrComparison();
    	newReactor.setPixel("OR_FILTER", node.toString().trim());
    }
    
    @Override
    public void outAComplexOrComparisonExpr(AComplexOrComparisonExpr node) {
    	defaultOut(node);
    	deInitReactor();
    }
    
    @Override
    public void inABasicAndComparisonTerm(ABasicAndComparisonTerm node) {
    	defaultIn(node);
    	IReactor newReactor = getAndComparison();
    	newReactor.setPixel("AND_FILTER", node.toString().trim());
    }
    
    @Override
    public void outABasicAndComparisonTerm(ABasicAndComparisonTerm node) {
    	defaultOut(node);
    	deInitReactor();
    }
    
    @Override
    public void inABasicOrComparisonTerm(ABasicOrComparisonTerm node) {
    	defaultIn(node);
    	IReactor newReactor = getOrComparison();
    	newReactor.setPixel("OR_FILTER", node.toString().trim());
    }
    
    @Override
    public void outABasicOrComparisonTerm(ABasicOrComparisonTerm node) {
    	defaultOut(node);
    	deInitReactor();
    }
    
    @Override
    public void inABaseSimpleComparison(ABaseSimpleComparison node) {
    	IReactor newReactor = null;
    	if(this.curReactor instanceof AbstractQueryStructReactor || this.curReactor instanceof AbstractFilterReactor) {
    		newReactor = new QueryFilterComponentSimple();
    	} else if(this.curReactor instanceof prerna.reactor.qs.selectors.IfReactor)
    	{
    		newReactor = new QueryFilterComponentSimple();
    	}
    	else
    	{
    		newReactor = new OpFilter();
    	}
    	
    	initReactor(newReactor);
    	newReactor.setPixel("BASIC_FILTER", node.toString());
    	syncResult();
    }

    @Override
    public void outABaseSimpleComparison(ABaseSimpleComparison node) {
    	defaultOut(node);
    	deInitReactor();
    }

    @Override
    public void caseABaseSimpleComparison(ABaseSimpleComparison node)
    {
        inABaseSimpleComparison(node);
        if(node.getLeft() != null)
        {
            node.getLeft().apply(this);
        }
        if(node.getComparator() != null)
        {
        	curReactor.getCurRow().addComparator(node.getComparator().toString().trim());
        }
        if(node.getRight() != null)
        {
            node.getRight().apply(this);
        }
        outABaseSimpleComparison(node);
    }
    
    @Override
    public void inAListRegTerm(AListRegTerm node) {
    	 defaultIn(node);
         IReactor opReactor = new VectorReactor();
         opReactor.setName("Vector");
         opReactor.setPixel("Vector", node.toString().trim());
         initReactor(opReactor);
    }
    
    @Override
    public void outAListRegTerm(AListRegTerm node) {
        defaultOut(node);
    	deInitReactor();
    }
    
    @Override
    public void inAFormula(AFormula node) {
        defaultIn(node);
        
        // need to account for formulas in assimilators
        if(curReactor instanceof Assimilator) {
        	((Assimilator) curReactor).addFormula(node.toString().trim());
        }
    }

    @Override
    public void outAFormula(AFormula node) {
        defaultOut(node);
    }
    
    /**************************************************
     *	Expression Methods
     **************************************************/

    @Override
    public void inAPlusBaseExpr(APlusBaseExpr node) {
    	String nodeExpr = node.toString().trim();
    	if(requireAssimilator(nodeExpr, "+", encapsulatedInFormula(node))) {
	    	defaultIn(node);
			
	    	Assimilator assm = new Assimilator();
	    	String leftKey = node.getLeft().toString().trim();
			String rightKey = node.getRight().toString().trim();
			initExpressionToReactor(assm, leftKey, rightKey, "+");
			assm.setPixel("EXPR", nodeExpr);
			initReactor(assm);
    	}
    }
    
    @Override
    public void outAPlusBaseExpr(APlusBaseExpr node)
    {
    	defaultOut(node);
    	// deinit this only if this is the same node
    	if((node.toString()).trim().equalsIgnoreCase(curReactor.getOriginalSignature())) {
  			deInitReactor();
    	}
    }
    
    @Override
    public void inAMinusBaseExpr(AMinusBaseExpr node) {
    	String nodeExpr = node.toString().trim();
    	if(requireAssimilator(nodeExpr, "-", encapsulatedInFormula(node))) {
	    	defaultIn(node);
			
	    	Assimilator assm = new Assimilator();
	    	String leftKey = null;
			if(node.getLeft() == null) {
				// if no left key is defined
				// then what we really have is a negative number
				// but the rightKey could be something a lot more complex
				// so for now, we will just pretend the leftKey is 0
				leftKey = "0.0";
			} else {
				leftKey = node.getLeft().toString().trim();
			}
			String rightKey = node.getRight().toString().trim();
			initExpressionToReactor(assm, leftKey, rightKey, "-");
			assm.setPixel("EXPR", nodeExpr);
			initReactor(assm);
    	}
    }
    
    @Override
    public void outAMinusBaseExpr(AMinusBaseExpr node)
    {
    	defaultOut(node);
    	if((node.toString()).trim().equalsIgnoreCase(curReactor.getOriginalSignature())) {
    		deInitReactor();
    	}
    }
    
    @Override
    public void inADivBaseExpr(ADivBaseExpr node) {
    	String nodeExpr = node.toString().trim();
    	if(requireAssimilator(nodeExpr, "/", encapsulatedInFormula(node))) {
	    	defaultIn(node);
			
	    	Assimilator assm = new Assimilator();
	    	String leftKey = node.getLeft().toString().trim();
			String rightKey = node.getRight().toString().trim();
			initExpressionToReactor(assm, leftKey, rightKey, "/");
			assm.setPixel("EXPR", nodeExpr);
			initReactor(assm);
    	}
    }
    
    @Override
    public void outADivBaseExpr(ADivBaseExpr node)
    {
    	defaultOut(node);
    	if((node.toString()).trim().equalsIgnoreCase(curReactor.getOriginalSignature())) {
    		deInitReactor();
    	}
    }
    
    @Override
    public void inAMultBaseExpr(AMultBaseExpr node) {
    	String nodeExpr = node.toString().trim();
    	if(requireAssimilator(nodeExpr, "*", encapsulatedInFormula(node))) {
	    	defaultIn(node);
			
	    	Assimilator assm = new Assimilator();
	    	String leftKey = node.getLeft().toString().trim();
			String rightKey = node.getRight().toString().trim();
			initExpressionToReactor(assm, leftKey, rightKey, "*");
			assm.setPixel("EXPR", nodeExpr);
			initReactor(assm);
    	}
    }
    
    @Override
    public void outAMultBaseExpr(AMultBaseExpr node)
    {
    	defaultOut(node);
    	if((node.toString()).trim().equalsIgnoreCase(curReactor.getOriginalSignature())) {
    		deInitReactor();
    	}
    }
    
    @Override
    public void inAModBaseExpr(AModBaseExpr node) {
    	String nodeExpr = node.toString().trim();
    	if(requireAssimilator(nodeExpr, "%", encapsulatedInFormula(node))) {
	    	defaultIn(node);
			
	    	Assimilator assm = new Assimilator();
	    	String leftKey = node.getLeft().toString().trim();
			String rightKey = node.getRight().toString().trim();
			initExpressionToReactor(assm, leftKey, rightKey, "%");
			assm.setPixel("EXPR", nodeExpr);
			initReactor(assm);
    	}
    }
    
    @Override
    public void outAModBaseExpr(AModBaseExpr node)
    {
    	defaultOut(node);
    	if((node.toString()).trim().equalsIgnoreCase(curReactor.getOriginalSignature())) {
    		deInitReactor();
    	}
    }
    
    /**
     * Use this to determine if we need to make an assimilator
     * Otherwise, figure out what reactor we need to generate
     * @param nodeExpr
     * @param math
     * @return
     */
	private boolean requireAssimilator(String nodeExpr, String math, boolean encapsulated) {
    	if(curReactor instanceof Assimilator) {
        	return false;
    	} else if(curReactor instanceof QuerySelectorExpressionAssimilator) {
    		QuerySelectorExpressionAssimilator qAssm = new QuerySelectorExpressionAssimilator();
        	qAssm.setMathExpr(math);
        	qAssm.setEncapsulated(encapsulated);
        	qAssm.setPixel("EXPR", nodeExpr);
    		initReactor(qAssm);	
        	return false;
    	} else if(curReactor instanceof SelectReactor) {  
        	QuerySelectorExpressionAssimilator qAssm = new QuerySelectorExpressionAssimilator();
        	qAssm.setMathExpr(math);
        	qAssm.setPixel("EXPR", nodeExpr);
    		initReactor(qAssm);	
        	return false;
        } 
    	// if the parent is a filter
    	// we need to aggregate this side of the column expression
    	else if(curReactor != null && curReactor.getParentReactor() instanceof FilterReactor) {
        	QuerySelectorExpressionAssimilator qAssm = new QuerySelectorExpressionAssimilator();
        	qAssm.setMathExpr(math);
        	qAssm.setPixel("EXPR", nodeExpr);
    		initReactor(qAssm);	
        	return false;
        }
      	else if(curReactor != null && ((curReactor instanceof prerna.reactor.qs.selectors.IfReactor) 
      			|| (curReactor.getParentReactor() instanceof prerna.reactor.qs.selectors.IfReactor))) 
      			{
        	QuerySelectorExpressionAssimilator qAssm = new QuerySelectorExpressionAssimilator();
        	qAssm.setMathExpr(math);
        	qAssm.setPixel("EXPR", nodeExpr);
    		initReactor(qAssm);	
        	return false;
        }
      	else if(curReactor != null && ((curReactor instanceof prerna.reactor.qs.filter.BetweenReactor) || (curReactor.getParentReactor() instanceof prerna.reactor.qs.filter.BetweenReactor))) 
		{
			QuerySelectorExpressionAssimilator qAssm = new QuerySelectorExpressionAssimilator();
			qAssm.setMathExpr(math);
			qAssm.setPixel("EXPR", nodeExpr);
			initReactor(qAssm);	
			return false;
		}
    	
    	return true;
    }
    
    private boolean encapsulatedInFormula(Node node) {
    	return node.parent().parent() instanceof AFormula;
    }
    
    @Override
    public void inAPower(APower node) {
    	// since this is defined via the signature
    	// which is
    	// expression ^ expression
    	defaultIn(node);
    	PowAssimilator powAssm = new PowAssimilator();
    	powAssm.setExpressions(node.getBase().toString().trim(), node.getExponent().toString().trim());
    	powAssm.setPixel("EXPR", node.toString().trim());
    	initReactor(powAssm);	
    }
    
    @Override
    public void outAPower(APower node) {
    	defaultOut(node);
    	// deinit this only if this is the same node
    	if((node.toString()).trim().equalsIgnoreCase(curReactor.getOriginalSignature())) {
    		deInitReactor();
    	}
    }

    private void initExpressionToReactor(IReactor reactor, String left, String right, String operation) {
    	GenRowStruct leftGenRow = reactor.getNounStore().makeNoun("LEFT");
		leftGenRow.add(left, PixelDataType.CONST_STRING);
    
    	GenRowStruct rightGenRow = reactor.getNounStore().makeNoun("RIGHT");
    	rightGenRow.add(right, PixelDataType.CONST_STRING);
    
    	GenRowStruct operator = reactor.getNounStore().makeNoun("OPERATOR");
    	operator.add(operation, PixelDataType.CONST_STRING);
    }
    
    /**************************************************
     *	End Expression Methods
     **************************************************/

    /*
     * Reactor creation, init, and deinit 
     */
    
    protected void initReactor(IReactor reactor)
    {
    	// make a check to see if the curReactor is not null
    	IReactor parentReactor = curReactor;
    	if(parentReactor != null) {
    		reactor.setParentReactor(parentReactor);
    		parentReactor.setChildReactor(reactor);
    	}
    	curReactor = reactor;
    	curReactor.setInsight(this.insight);
    	curReactor.setPixelPlanner(this.planner);
    	curReactor.In();

    	// need to account for embedded script reactors
    	// since i do not want it to the be the parent
    	if(parentReactor instanceof EmbeddedScriptReactor) {
    		// since the in/out will push things into the embedded script
    		// we need to now push the last value and sync it to the new reactor
    		int numInputs = parentReactor.getCurRow().size();
    		if(numInputs > 0) {
    			NounMetadata lastNoun = parentReactor.getCurRow().getNoun(numInputs-1);
        		PixelDataType lastNounType = lastNoun.getNounType();
        		GenRowStruct genRow = curReactor.getNounStore().makeNoun(lastNounType.getKey());
        		genRow.add(lastNoun);
    		}
    	} else if(parentReactor instanceof EmbeddedRoutineReactor) {
    		// need to push all the sub parts
    		int numInputs = parentReactor.getCurRow().size();
    		for(int i = 0; i < numInputs; i++) {
    			NounMetadata noun = parentReactor.getCurRow().remove(i);
        		PixelDataType nounType = noun.getNounType();
        		GenRowStruct genRow = curReactor.getNounStore().makeNoun(nounType.getKey());
        		genRow.add(noun);
    		}
    	}
    }

    protected void deInitReactor() {
    	if(curReactor != null) {
    		// merge up and update the plan
    		try {
    			curReactor.mergeUp();
    			curReactor.updatePlan();
    		} catch(Exception e) {
    			e.printStackTrace();
    			throw new IllegalArgumentException(e.getMessage());
    		}
    		// get the parent
    		Object parent = curReactor.Out();
    		// set the parent as the curReactor if it is present
    		prevReactor = curReactor;
    		if(parent != null && parent instanceof IReactor) {
    			curReactor = (IReactor) parent;
    		} else {
    			curReactor = null;
//    			// store the noun map in the pixel
//    			if(this.pixelObj != null) {
//    				this.pixelObj.addReactorInput(prevReactor.getStoreMap());
//    			}
    		}
    	}
    }

    /**
     * Return the reactor based on the reactorId
     * @param reactorId - reactor id or operation
     * @param nodeString - full operation
     * @return	IReactor		A new instance of the reactor
     */
    protected IReactor getReactor(String reactorId, String nodeString) {
    	
    	// try to see if there is java files to be compiled
    	//compileAllJava(insight.getInsightFolder());
    	
    	// oh wait why cant this be in reactor factory
    	// because it doesn't have control of the insight
    	// check if this is an insight specific DSL
    	IReactor insightReactor = insight.getReactor(reactorId);
    	if(insightReactor != null)
    	{
    		insightReactor.setPixel(reactorId, nodeString);
    		return insightReactor;
    	}
    	/*
    	if(insight != null && insight.getInsightFolder() != null) {
    		// TODO: make this consistent... why is this added w/o any consideration of existing flows
    		// just wasting peoples time having to debug
    		IReactor insightReactor = ReactorFactory.getIReactor(insight.getInsightFolder(), reactorId);
	    	if(insightReactor != null) {
	    		// setting this here and not in rector factory .. inconsistent...
	    		insightReactor.setPixel(reactorId, nodeString);
	    		return insightReactor;
	    	}
    	} */
    	
    	if(this.currentFrame != null) {
    		return ReactorFactory.getReactor(reactorId, nodeString, this.currentFrame, curReactor);
    	}
    	IDataMaker dataTable = null;
    	if(this.insight != null) {
    		dataTable = this.insight.getDataMaker();
    	}
    	if(dataTable != null) {
    		return ReactorFactory.getReactor(reactorId, nodeString, (ITableDataFrame) dataTable, curReactor);
    	} else {
    		return ReactorFactory.getReactor(reactorId, nodeString, null, curReactor);
    	}
    }
    
    //////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////
    
    /*
     * This section is for processing maps
     * These are all just things to store
     * No additional processing occurs within the map inputs
     */
    
    @Override
    public void inAMap(AMap node) {
    	defaultIn(node);
    	MapReactor mapR = new MapReactor();
    	initReactor(mapR);
    }
    
    @Override
    public void outAMap(AMap node) {
    	defaultOut(node);
    	deInitReactor();
    }
    
    @Override
    public void inAMapList(AMapList node) {
    	defaultIn(node);
    	MapListReactor mapListR = new MapListReactor();
    	initReactor(mapListR);
    }
    
    @Override
    public void outAMapList(AMapList node) {
    	defaultOut(node);
    	deInitReactor();
    }
    
    @Override
    public void inAWordMapKey(AWordMapKey node) {
    	String mapKey = PixelUtility.removeSurroundingQuotes(node.getWord().getText());
    	this.curReactor.getCurRow().addLiteral(mapKey);
    }
    
    @Override
    public void inAMapVar(AMapVar node) {
    	String variable = node.getVar().toString().trim();
    	NounMetadata value = null;
    	if(this.planner.hasVariable(variable)) {
    		value = this.planner.getVariableValue(variable);
    	} else {
    		value = new NounMetadata("{" + variable + "}", PixelDataType.CONST_STRING);
    	}
    	this.curReactor.getCurRow().add(value);
    }
    
    @Override
    public void outAMapNegNum(AMapNegNum node) {
    	/*
    	 * We are out a number
    	 * Which adds to the curRow of the map reactor
    	 * So take it and then multiple by -1 and add back
    	 */
    	GenRowStruct curRow = this.curReactor.getCurRow();
    	NounMetadata numNoun = curRow.remove(curRow.size()-1);
    	PixelDataType type = numNoun.getNounType();
    	
    	NounMetadata negNum = null;
    	if(type == PixelDataType.CONST_INT) {
    		negNum = new NounMetadata(-1 * ((Number) numNoun.getValue()).intValue(), PixelDataType.CONST_INT);
    	} else if(type == PixelDataType.CONST_DECIMAL){
    		negNum = new NounMetadata(-1.0 * ((Number) numNoun.getValue()).doubleValue(), PixelDataType.CONST_DECIMAL);
    	} else {
    		// ugh... how did you get here
    		// i'm just gonna add you back...
    		negNum = numNoun;
    	}
    	
		curRow.add(negNum);
    }
       
	//////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////
    
    public void defaultIn(Node node)
    {
        // Do nothing
//    	StackTraceElement[] ste = Thread.currentThread().getStackTrace();
//    	if(ste.length > 2) {
//    		LOGGER.info("CURRENT METHOD: "+ste[2].getMethodName());
//    		LOGGER.info("CURRENT NODE:" + node.toString());
// 
//    		
//    		if(curReactor != null && curReactor.getParentReactor() != null) {
//    			LOGGER.info("CURRENT REACTOR: "+curReactor.getClass().getSimpleName());
//    			LOGGER.info("PARENT REACTOR: "+curReactor.getParentReactor().getClass().getSimpleName() + "\n");
//    		} 
//    		
//    		else if (curReactor != null) {
//    			LOGGER.info("CURRENT REACTOR: "+curReactor.getClass().getSimpleName() + "\n");
//    		}
//    		
//    		else LOGGER.info("CURRENT REACTOR: null" + "\n");
//    		
//    	} else {
//    		LOGGER.info("THIS SHOULD NOT HAPPEN!!!!!!!!!!");
//    	}
    }

    public void defaultOut(Node node)
    {
        // Do nothing
//    	StackTraceElement[] ste = Thread.currentThread().getStackTrace();
//    	if(ste.length > 2) {
//    		LOGGER.info("CURRENT METHOD: "+ste[2].getMethodName());
//    		LOGGER.info("CURRENT NODE:" + node.toString());
// 
//    		
//    		if(curReactor != null && curReactor.getParentReactor() != null) {
//    			LOGGER.info("CURRENT REACTOR: "+curReactor.getClass().getSimpleName());
//    			LOGGER.info("PARENT REACTOR: "+curReactor.getParentReactor().getClass().getSimpleName() + "\n");
//    		} 
//    		
//    		else if (curReactor != null) {
//    			LOGGER.info("CURRENT REACTOR: "+curReactor.getClass().getSimpleName() + "\n");
//    		}
//    		
//    		else LOGGER.info("CURRENT REACTOR: null" + "\n");
//    		
//    	} else {
//    		LOGGER.info("THIS SHOULD NOT HAPPEN!!!!!!!!!!");
//    	}
    }
    

}