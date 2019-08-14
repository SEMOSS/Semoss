package prerna.sablecc2;

import java.io.File;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.stream.Collectors;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.NotFoundException;
import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.analysis.DepthFirstAdapter;
import prerna.sablecc2.node.AAsop;
import prerna.sablecc2.node.AAssignRoutine;
import prerna.sablecc2.node.AAssignment;
import prerna.sablecc2.node.ABaseSimpleComparison;
import prerna.sablecc2.node.ABasicAndComparisonTerm;
import prerna.sablecc2.node.ABasicOrComparisonTerm;
import prerna.sablecc2.node.ABooleanScalar;
import prerna.sablecc2.node.ACodeNoun;
import prerna.sablecc2.node.ACommentExpr;
import prerna.sablecc2.node.AComplexAndComparisonExpr;
import prerna.sablecc2.node.AComplexOrComparisonExpr;
import prerna.sablecc2.node.AConfiguration;
import prerna.sablecc2.node.ADivBaseExpr;
import prerna.sablecc2.node.ADotcol;
import prerna.sablecc2.node.AEmbeddedScriptchainExprComponent;
import prerna.sablecc2.node.AExplicitRel;
import prerna.sablecc2.node.AFormula;
import prerna.sablecc2.node.AFractionDecimal;
import prerna.sablecc2.node.AGeneric;
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
import prerna.sablecc2.node.ANullRegTerm;
import prerna.sablecc2.node.AOperation;
import prerna.sablecc2.node.AOutputRoutine;
import prerna.sablecc2.node.APlusBaseExpr;
import prerna.sablecc2.node.APower;
import prerna.sablecc2.node.AProp;
import prerna.sablecc2.node.ARcol;
import prerna.sablecc2.node.ASelectNoun;
import prerna.sablecc2.node.ASubRoutine;
import prerna.sablecc2.node.AWholeDecimal;
import prerna.sablecc2.node.AWordMapKey;
import prerna.sablecc2.node.AWordWordOrId;
import prerna.sablecc2.node.Node;
import prerna.sablecc2.node.PRoutine;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.reactor.AssignmentReactor;
import prerna.sablecc2.reactor.Assimilator;
import prerna.sablecc2.reactor.EmbeddedRoutineReactor;
import prerna.sablecc2.reactor.EmbeddedScriptReactor;
import prerna.sablecc2.reactor.GenericReactor;
import prerna.sablecc2.reactor.IReactor;
import prerna.sablecc2.reactor.IfReactor;
import prerna.sablecc2.reactor.NegReactor;
import prerna.sablecc2.reactor.PixelPlanner;
import prerna.sablecc2.reactor.PowAssimilator;
import prerna.sablecc2.reactor.ReactorFactory;
import prerna.sablecc2.reactor.VectorReactor;
import prerna.sablecc2.reactor.expression.filter.OpAnd;
import prerna.sablecc2.reactor.expression.filter.OpFilter;
import prerna.sablecc2.reactor.expression.filter.OpOr;
import prerna.sablecc2.reactor.frame.filter.AbstractFilterReactor;
import prerna.sablecc2.reactor.map.MapListReactor;
import prerna.sablecc2.reactor.map.MapReactor;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.reactor.qs.filter.FilterReactor;
import prerna.sablecc2.reactor.qs.filter.QueryFilterComponentAnd;
import prerna.sablecc2.reactor.qs.filter.QueryFilterComponentOr;
import prerna.sablecc2.reactor.qs.filter.QueryFilterComponentSimple;
import prerna.sablecc2.reactor.qs.selectors.QuerySelectorExpressionAssimilator;
import prerna.sablecc2.reactor.qs.selectors.SelectReactor;
import prerna.sablecc2.reactor.qs.source.FrameReactor;
import prerna.sablecc2.reactor.runtime.JavaReactor;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.util.git.GitAssetUtils;
import prerna.util.usertracking.IUserTracker;
import prerna.util.usertracking.UserTrackerFactory;

public class LazyTranslation extends DepthFirstAdapter {

	private static final Logger LOGGER = LogManager.getLogger(LazyTranslation.class.getName());
	
	protected PixelPlanner planner;
	protected Insight insight;
	protected IReactor curReactor = null;
	protected IReactor prevReactor = null;
	
	protected boolean isMeta = false;
	// if we have META and a variable definition
	// we need to not record this variable
	// this includes embedded expressions
	protected List<String> metaVariables = new Vector<String>();
	protected Map<String, NounMetadata> prevVariables = new HashMap<String, NounMetadata>();
	
	public enum TypeOfOperation {PIPELINE, COMPOSITION};
	protected TypeOfOperation operationType = TypeOfOperation.COMPOSITION;
	
	protected ITableDataFrame currentFrame = null;
	
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
	
	protected void postProcess(String pixelExpression) {
		// need to account for META variables
		// that were defined
		if(this.isMeta) {
			for(String var : this.metaVariables) {
				this.planner.removeVariable(var);
			}
			
			// now add back the other variables we had to keep
			for(String var : this.prevVariables.keySet()) {
				this.planner.addVariable(var, this.prevVariables.get(var));
			}
		}
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
	public void caseAConfiguration(AConfiguration node) {
        List<PRoutine> copy = new ArrayList<PRoutine>(node.getRoutine());
        int size = copy.size();
        for(int pixelstep = 0; pixelstep < size; pixelstep++)
        {
        	PRoutine e = copy.get(pixelstep);
        	try {
        		e.apply(this);
        		// reset the state of the frame
        		this.currentFrame = null;
        	} catch(SemossPixelException ex) {
        		trackError(e.toString(), this.isMeta, ex);
        		ex.printStackTrace();
        		// if we want to continue the thread of execution
        		// nothing special
        		// just add the error to the return
        		if(ex.isContinueThreadOfExecution()) {
        			planner.addVariable("$RESULT", ex.getNoun());
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
        		trackError(e.toString(), this.isMeta, ex);
        		ex.printStackTrace();
        		planner.addVariable("$RESULT", new NounMetadata(ex.getMessage(), PixelDataType.ERROR, PixelOperationType.ERROR));
        		postProcess(e.toString().trim());
        	}
        }
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
			this.planner.addVariable("$RESULT", noun);
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
		this.isMeta = true;
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

    	if(this.isMeta) {
    		if(this.planner.hasVariable(var)) {
    			this.prevVariables.put(var, this.planner.getVariable(var));
    		}
    		// make sure we remove this at the end
    		this.metaVariables.add(var);
    	}
    	
    	IReactor assignmentReactor = new AssignmentReactor();
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
		this.planner.addVariable("$RESULT", noun);
		postProcess(node.toString().trim());
	}
	
	@Override
	public void inACommentExpr(ACommentExpr node) {
		String comment = node.getComment().getText();
		NounMetadata noun = new NounMetadata(comment, PixelDataType.CONST_STRING, PixelOperationType.RECIPE_COMMENT);
		if(curReactor == null) {
			this.planner.addVariable("$RESULT", noun);
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
        LOGGER.debug("Starting an operation");
        
        String reactorId = node.getId().toString().trim();
        IReactor newReactor = getReactor(reactorId, node.toString().trim());
        initReactor(newReactor);
        syncResult();
	}
	
	@Override
	public void outAOperation(AOperation node) {
		defaultOut(node);
        LOGGER.debug("Ending operation");
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
    	NounMetadata prevResult = this.planner.getVariable("$RESULT");
    	if(prevResult != null) {
    		PixelDataType nounName = prevResult.getNounType();
    		GenRowStruct genRow = curReactor.getNounStore().makeNoun(nounName.toString());
    		genRow.add(prevResult);
    		// then we will remove the result from the planner
        	this.planner.removeVariable("$RESULT");
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
    		opReactor = new prerna.sablecc2.reactor.qs.AsReactor();
    	} else {
    		opReactor = new prerna.sablecc2.reactor.AsReactor();
    	}
    	LOGGER.debug("In the AS Component of frame op");
    	opReactor.setPixel("as", node.getAsOp() + "");
    	initReactor(opReactor);
    }

    public void outAAsop(AAsop node)
    {
    	defaultOut(node);
    	LOGGER.debug("OUT the AS Component of frame op");
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
    public void inAGeneric(AGeneric node) {
    	defaultIn(node);
    	IReactor genReactor = new GenericReactor();
        genReactor.setPixel("PKSL", (node + "").trim());
        genReactor.setProp("KEY", node.getId().toString().trim());
        initReactor(genReactor);
    }

    @Override
    public void outAGeneric(AGeneric node) {
    	defaultOut(node);
    	deInitReactor();
    }
    
    // accumulating secondary structures
    // the values of each of these are generic rows
    @Override
    public void inASelectNoun(ASelectNoun node)
    {
        defaultIn(node);
        // in on almost most stuff seems incredibly useless
        
        // get the previous reactor
        // Try to see if this is something that we can hold on to
        // if the overarching piece on the GenRow struct comes back to be
        // hey this is all sql
        // we can continue to build on it
        // at any point we see this is not the case, we should execute the sql and JAM the operation right after it
        
        // we really dont need the selectors.. the only thing we need are really the projectors
        
        // make a nounstore if there is not already one
        // make this as the current noun on the reactor
        String nounName = "s"; 
        curReactor.curNoun(nounName);
    }

    @Override
    public void outASelectNoun(ASelectNoun node)
    {
        defaultOut(node);
        // not sure if I need anything else right now
        curReactor.closeNoun("s");
    }
    
    public void inANullRegTerm(ANullRegTerm node) {
    	NounMetadata noun = new NounMetadata(null, PixelDataType.NULL_VALUE);
    	if(curReactor != null) {
			curReactor.getCurRow().add(noun);
		} else {
			this.planner.addVariable("$RESULT", noun);
		}
    }

    @Override
    public void inAIdWordOrId(AIdWordOrId node)
    {
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
    			this.planner.addVariable("$RESULT", varValue);
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
    			this.planner.addVariable("$RESULT", noun);
    		}
    	}
    }
    
    @Override
    public void inAWordWordOrId(AWordWordOrId node)
    {
        defaultIn(node);
        String trimmedWord = node.getWord().toString().trim();
        String word = PixelUtility.removeSurroundingQuotes(trimmedWord);
        // also replace any escaped quotes
        word = word.replace("\\\"", "\"");
       	// if its an assignment
    	// just put in results in case we are doing a |
    	if(curReactor != null && !(curReactor instanceof AssignmentReactor)) {
	        curReactor.getCurRow().addLiteral(word);
	        curReactor.setProp(node.toString().trim(), word);
        } else {
        	NounMetadata noun = new NounMetadata(word, PixelDataType.CONST_STRING);
    		this.planner.addVariable("$RESULT", noun);
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
	        curReactor.setProp(node.toString().trim(), booleanStr);
        } else {
        	NounMetadata noun = new NounMetadata(bool, PixelDataType.BOOLEAN);
    		this.planner.addVariable("$RESULT", noun);
        }
    }

    // atomic level stuff goes in here
    @Override
    public void inAWholeDecimal(AWholeDecimal node)
    {
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
		Number retNum = new BigDecimal(whole + "." + fraction);
    	NounMetadata noun = null;
    	if(isDouble) {
    		noun = new NounMetadata(retNum.doubleValue(), PixelDataType.CONST_DECIMAL);
    	} else {
    		noun = new NounMetadata(retNum.intValue(), PixelDataType.CONST_INT);
    	}
    	// if its an assignment
    	// just put in results in case we are doing a |
    	if(curReactor != null && !(curReactor instanceof AssignmentReactor)) {
    		curReactor.getCurRow().add(noun);
	    	// modify the parent such that the signature has the correct
	    	// value of the numerical without any extra spaces
	    	// plain string will always modify to a integer even if we return double value
	    	curReactor.modifySignature(node.toString().trim(), new BigDecimal(retNum.doubleValue()).toPlainString());
    	} else {
    		// looks like you just have a number...
    		// i guess i will return this?
    		this.planner.addVariable("$RESULT", noun);
    	}
    }

    @Override
    public void inAFractionDecimal(AFractionDecimal node)
    {
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
	    	curReactor.modifySignature(node.toString().trim(), new BigDecimal(retNum.doubleValue()).toPlainString());
    	} else {
    		NounMetadata noun = new NounMetadata(retNum.doubleValue(), PixelDataType.CONST_DECIMAL);
    		this.planner.addVariable("$RESULT", noun);
    	}
    }
    
    @Override
    public void inADotcol(ADotcol node)
    {
    	defaultIn(node);
    	processAliasReference(node.getColumnName().toString().trim());
    }

    @Override
    public void inARcol(ARcol node)
    {
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
    			IRawSelectWrapper iterator = frame.query(qs);
    			ITask task = new BasicIteratorTask(qs, iterator);
    			this.insight.getTaskStore().addTask(task);
    			this.planner.addVariable("$RESULT", new NounMetadata(task, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA));
    		}
    	}
    }
    
    @Override
    public void inAProp(AProp node)
    {
    	defaultIn(node);
    	String key = node.getId().toString().trim();
    	String propValue = node.getScalar().toString().trim();
        curReactor.setProp(key, propValue);
    }
    
    @Override
    public void inACodeNoun(ACodeNoun node)
    {
        defaultIn(node);
        String code = (node.getCodeAlpha() + "");
        code = code.replace("<c>",""); 
        code = code.replace("</c>","");
        code = code.trim();
        curReactor.setProp("CODE", code);
    }

    @Override
    public void outACodeNoun(ACodeNoun node)
    {
        defaultOut(node);
    }

    // you definitely need the other party to be in a relationship :)
    // this is of the form colDef followed by type of relation and another col def
    
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
    	curReactor.getCurRow().addRelation(leftCol, relType, rightCol, relName);
    }
    
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
    
    /////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////
    
    /*
     * Comparison logic
     */
    
    private void getOrComparison() {
    	IReactor newReactor = null;
    	if(this.curReactor != null && this.curReactor instanceof FilterReactor || this.curReactor instanceof AbstractFilterReactor) {
    		newReactor = new QueryFilterComponentOr();
    	} else {
    		newReactor = new OpOr();
    	}
    	
    	initReactor(newReactor);
    	syncResult();
    }
    
    private void getAndComparison() {
    	IReactor newReactor = null;
    	if(this.curReactor != null && this.curReactor instanceof FilterReactor || this.curReactor instanceof AbstractFilterReactor) {
    		newReactor = new QueryFilterComponentAnd();
    	} else {
    		newReactor = new OpAnd();
    	}
    	
    	initReactor(newReactor);
    	syncResult();
    }
    
    @Override
    public void inAComplexAndComparisonExpr(AComplexAndComparisonExpr node) {
    	defaultIn(node);
    	getAndComparison();
    }
    
    @Override
    public void outAComplexAndComparisonExpr(AComplexAndComparisonExpr node) {
    	defaultOut(node);
    	deInitReactor();
    }
    
    @Override
    public void inAComplexOrComparisonExpr(AComplexOrComparisonExpr node) {
    	defaultIn(node);
    	getOrComparison();
    }
    
    @Override
    public void outAComplexOrComparisonExpr(AComplexOrComparisonExpr node) {
    	defaultOut(node);
    	deInitReactor();
    }
    
    @Override
    public void inABasicAndComparisonTerm(ABasicAndComparisonTerm node) {
    	defaultIn(node);
    	getAndComparison();
    }
    
    @Override
    public void outABasicAndComparisonTerm(ABasicAndComparisonTerm node) {
    	defaultOut(node);
    	deInitReactor();
    }
    
    @Override
    public void inABasicOrComparisonTerm(ABasicOrComparisonTerm node) {
    	defaultIn(node);
    	getOrComparison();
    }
    
    @Override
    public void outABasicOrComparisonTerm(ABasicOrComparisonTerm node) {
    	defaultOut(node);
    	deInitReactor();
    }
    
    @Override
    public void inABaseSimpleComparison(ABaseSimpleComparison node) {
    	IReactor newReactor = null;
    	if(this.curReactor instanceof FilterReactor || this.curReactor instanceof AbstractFilterReactor) {
    		newReactor = new QueryFilterComponentSimple();
    	} else {
    		newReactor = new OpFilter();
    	}
    	
    	initReactor(newReactor);
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
        		PixelDataType lastNounName = lastNoun.getNounType();
        		GenRowStruct genRow = curReactor.getNounStore().makeNoun(lastNounName.toString());
        		genRow.add(lastNoun);
    		}
    	} else if(parentReactor instanceof EmbeddedRoutineReactor) {
    		// need to push all the sub parts
    		int numInputs = parentReactor.getCurRow().size();
    		for(int i = 0; i < numInputs; i++) {
    			NounMetadata noun = parentReactor.getCurRow().getNoun(i);
        		PixelDataType nounType = noun.getNounType();
        		GenRowStruct genRow = curReactor.getNounStore().makeNoun(nounType.toString());
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
    	// oh wait why cant this be in reactor factory
    	// because it doesn't have control of the insight
    	// check if this is an insight specific DSL
    	if(insight != null && insight.getInsightFolder() != null) {
    		// TODO: make this consistent... why is this added w/o any consideration of existing flows
    		// just wasting peoples time having to debug
    		IReactor insightReactor = ReactorFactory.getIReactor(insight.getInsightFolder(), reactorId);
	    	if(insightReactor != null) {
	    		// setting this here and not in rector factory .. inconsistent...
	    		insightReactor.setPixel(reactorId, nodeString);
	    		return insightReactor;
	    	}
    	}   	
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
    
	public void loadCG() throws MalformedURLException
	{
		String folder = "C:/Users/pkapaleeswaran/workspacej3/SemossDev/target/test-classes";
		folder = "C:/Users/pkapaleeswaran/workspacej3/SemossWeb/db/NewDB__db394ac3-f9ee-460b-949e-ea9e96ecf4a8/version/ca39473f-ca52-4afb-9b7a-565b34e3b6d1/classes";
		Map<String, List<String>> dirs = GitAssetUtils.browse(folder, folder);
		
		List<String> dirList = dirs.get("DIR_LIST");
		String [] packages = new String[dirList.size()];
		
		for(int dirIndex = 0;dirIndex < dirList.size();packages[dirIndex] = (String)dirList.get(dirIndex),dirIndex++);
		
		
		ScanResult sr = new ClassGraph()
				//.whitelistPackages("prerna")
				.overrideClasspath((new File(folder).toURI().toURL()))
				//.enableAllInfo()
				//.enableClassInfo()
				.whitelistPackages(packages)
				.scan();
		//ScanResult sr = new ClassGraph().whitelistPackages("prerna").scan();
		//ScanResult sr = new ClassGraph().enableClassInfo().whitelistPackages("prerna").whitelistPaths("C:/Users/pkapaleeswaran/workspacej3/MonolithDev3/target/classes").scan();

		//sr.getAllClasses(); //
		//ClassInfoList classes = sr.getAllClasses();//sr.getClassesImplementing("prerna.sablecc2.reactor.IReactor");
		//ClassInfoList classes = sr.getClassesImplementing("prerna.sablecc2.reactor.IReactor");
		ClassInfoList classes = sr.getSubclasses("prerna.sablecc2.reactor.AbstractReactor");
		
		
		Map <String, Class> reactors = new HashMap<String, Class>();
		

		ClassPool pool = ClassPool.getDefault();
		try {
			pool.insertClassPath(folder);
			//pool.appendPathList(folder);
		} catch (NotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		for(int classIndex = 0;classIndex < classes.size();classIndex++)
		{
			String name = classes.get(classIndex).getSimpleName();
			String packageName = classes.get(classIndex).getPackageName();
			Class actualClass = classes.get(classIndex).loadClass();
			
			try {
				// can I modify the class here
				CtClass clazz = pool.get(packageName + "." + name);

				String qClassName = "a.b." + packageName + "." + name;
				// change the name of the classes
				// ideally we would just have the pakcage name change to the insight
				// this is to namespace it appropriately to have no issues
				clazz.setName(qClassName);
				Class newClass = clazz.toClass();
				if(IReactor.class.isAssignableFrom(newClass))
					System.out.println("System.out.println>>> " +newClass.getName());
				
			} catch (NotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (CannotCompileException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}
}