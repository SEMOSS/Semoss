package prerna.sablecc2.reactor.json;

import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.algorithm.api.ITableDataFrame;
import prerna.om.Insight;
import prerna.sablecc2.LazyTranslation;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.analysis.DepthFirstAdapter;
import prerna.sablecc2.node.AAsop;
import prerna.sablecc2.node.AAssignRoutine;
import prerna.sablecc2.node.AAssignment;
import prerna.sablecc2.node.ABaseSimpleComparison;
import prerna.sablecc2.node.ABooleanScalar;
import prerna.sablecc2.node.ACodeNoun;
import prerna.sablecc2.node.ADivBaseExpr;
import prerna.sablecc2.node.AEmbeddedAssignmentExpr;
import prerna.sablecc2.node.AEmbeddedScriptchainExpr;
import prerna.sablecc2.node.AExplicitRel;
import prerna.sablecc2.node.AFormula;
import prerna.sablecc2.node.AFractionDecimal;
import prerna.sablecc2.node.AGeneric;
import prerna.sablecc2.node.AIdWordOrId;
import prerna.sablecc2.node.AImplicitRel;
import prerna.sablecc2.node.AJavaOp;
import prerna.sablecc2.node.AList;
import prerna.sablecc2.node.AListValues;
import prerna.sablecc2.node.AMap;
import prerna.sablecc2.node.AMapEntry;
import prerna.sablecc2.node.AMapList;
import prerna.sablecc2.node.AMapVar;
import prerna.sablecc2.node.AMetaRoutine;
import prerna.sablecc2.node.AMinusBaseExpr;
import prerna.sablecc2.node.AModBaseExpr;
import prerna.sablecc2.node.AMultBaseExpr;
import prerna.sablecc2.node.ANegTerm;
import prerna.sablecc2.node.ANestedMapMapExtendedInput;
import prerna.sablecc2.node.ANestedMapValues;
import prerna.sablecc2.node.ANormalScalarMapBaseInput;
import prerna.sablecc2.node.AOperation;
import prerna.sablecc2.node.AOutputRoutine;
import prerna.sablecc2.node.APlusBaseExpr;
import prerna.sablecc2.node.APower;
import prerna.sablecc2.node.AProp;
import prerna.sablecc2.node.ARcol;
import prerna.sablecc2.node.ASelectNoun;
import prerna.sablecc2.node.AWholeDecimal;
import prerna.sablecc2.node.AWordWordOrId;
import prerna.sablecc2.node.Node;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AssignmentReactor;
import prerna.sablecc2.reactor.Assimilator;
import prerna.sablecc2.reactor.EmbeddedScriptReactor;
import prerna.sablecc2.reactor.GenericReactor;
import prerna.sablecc2.reactor.IReactor;
import prerna.sablecc2.reactor.IfReactor;
import prerna.sablecc2.reactor.NegReactor;
import prerna.sablecc2.reactor.PixelPlanner;
import prerna.sablecc2.reactor.PowAssimilator;
import prerna.sablecc2.reactor.ReactorFactory;
import prerna.sablecc2.reactor.VectorReactor;
import prerna.sablecc2.reactor.expression.filter.OpFilter;
import prerna.sablecc2.reactor.json.validator.JsonValidatorReactorFactory;
import prerna.sablecc2.reactor.map.MapListReactor;
import prerna.sablecc2.reactor.map.MapMapReactor;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.reactor.qs.selectors.QuerySelectorExpressionAssimilator;
import prerna.sablecc2.reactor.runtime.JavaReactor;
import prerna.ui.components.playsheets.datamakers.IDataMaker;

public class LazyJsonTranslation extends DepthFirstAdapter {

	private static final Logger LOGGER = LogManager.getLogger(LazyTranslation.class.getName());
	
	public PixelPlanner planner;
	protected Insight insight;
	protected IReactor curReactor = null;
	protected IReactor prevReactor = null;
	protected boolean isMeta = false;

	public enum TypeOfOperation {PIPELINE, COMPOSITION};
	protected TypeOfOperation operationType = TypeOfOperation.COMPOSITION;

	// anthem specific stuff
	// the specific request type for the json operation
	// it is either CREATE, EDIT, or VIEW
	protected String requestType = "CREATE";
	protected String processNode = null;
	IReactor superParent = null;
	
	// we will set this to the specific factory
	// default is validation factory
	protected JsonValidatorReactorFactory factory = new JsonValidatorReactorFactory();
	// this is where the final output will be
	Object outputValue = null;
	// need some kind of tuple for exchanging result
	// that sits in the planner result
	// planner.getVariableValue($RESULT)
	
	public LazyJsonTranslation() {
		this.planner = new PixelPlanner();
	}
	
	public LazyJsonTranslation(Insight insight) {
		this.insight = insight;
		this.planner = new PixelPlanner();
		this.planner.setVarStore(this.insight.getVarStore());
	}
	
	protected void postProcess(String pkslExpression) {
		// do nothing
		// only the lazy implements this
	}
	
/********************** First is the main level operation, script chain or other script operations ******************/
	
	@Override
	public void inAOutputRoutine(AOutputRoutine node) {
		defaultIn(node);
		this.isMeta = false;
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
	public void inAEmbeddedScriptchainExpr(AEmbeddedScriptchainExpr node) {
		defaultIn(node);
		EmbeddedScriptReactor embeddedScriptReactor = new EmbeddedScriptReactor();
		embeddedScriptReactor.setPixel(node.getMandatoryScriptchain().toString().trim(), node.toString().trim());
    	initReactor(embeddedScriptReactor);
	}
	
	@Override
	public void outAEmbeddedScriptchainExpr(AEmbeddedScriptchainExpr node) {
		defaultOut(node);
		deInitReactor();
		// if no parent, that means this embedded script
		// is the only thing
		if(curReactor == null) {
			postProcess(node.toString().trim());
		}
	}
	
	@Override
	public void inAMetaRoutine(AMetaRoutine node) {
		this.isMeta = true;
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
        IReactor frameReactor = getReactor(reactorId, node.toString().trim());
        initReactor(frameReactor);
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
	
    private void syncResult() {
    	// after we init the new reactor
        // we will add the result of the last reactor 
        // into the noun store of this new reactor
    	NounMetadata prevResult = this.planner.getVariableValue("$RESULT");
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
    	// TODO: how do get this out of here...
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
    	opReactor.getCurRow().addLiteral(node.getJava().getText().trim());
    }
    
    @Override
    public void outAJavaOp(AJavaOp node) {
    	defaultOut(node);
    	deInitReactor();
    }

    @Override
    public void inAAssignment(AAssignment node)
    {
    	defaultIn(node);
    	IReactor assignmentReactor = new AssignmentReactor();
        assignmentReactor.setPixel(node.getId().toString().trim(), node.toString().trim());
    	initReactor(assignmentReactor);
    }

    @Override
    public void outAAssignment(AAssignment node)
    {
    	deInitReactor();
        defaultOut(node);
    }
    
    @Override
    public void outAEmbeddedAssignmentExpr(AEmbeddedAssignmentExpr node) {
    	deInitReactor();
        defaultOut(node);
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
    
    @Override
    public void outARcol(ARcol node)
    {
        defaultOut(node);
    }
    
    @Override
    public void inAIdWordOrId(AIdWordOrId node)
    {
    	defaultIn(node);
    	String column = (node.getId()+"").trim();
    	
    	//how do we determine the difference between a column and a variable?
    	//something could be a column but not loaded into a frame yet...i.e. select pksl in import
    	//something could be a variable but not be loaded as a variable yet...i.e. loadclient when loading pksls one by one into the graph in any order
    	if(curReactor != null) {
	    	curReactor.getCurRow().addColumn(column);
	    	curReactor.setProp(node.toString().trim(), column);
    	} else {
    		if(this.planner.hasVariable(column)) {
        		this.planner.addVariable("$RESULT", this.planner.getVariableValue(column));
    		} else {
    			// i guess we should return the actual column from the frame?
    			// TODO: build this out
    			// for now, will just return the column name again... 
    			NounMetadata noun = new NounMetadata(column, PixelDataType.CONST_STRING);
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
        // remove some of this so that the all doesn't come through
        if(curReactor != null) {
        	// making sure it doesnt add if it is a map reactor
        	//if(!(curReactor instanceof MapMapReactor))
        	{
		        curReactor.getCurRow().addLiteral(word);
		        curReactor.setProp(node.toString().trim(), word);
        	}
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
        if(curReactor != null) {
	        curReactor.getCurRow().addBoolean(bool);
	        curReactor.setProp(node.toString().trim(), booleanStr);
        } else {
        	NounMetadata noun = new NounMetadata(bool, PixelDataType.CONST_STRING);
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
    	// determine if it is a negative
    	// in which case, multiply by -1
//    	if(node.getPosOrNeg() != null) {
//    		String posOrNeg = node.getPosOrNeg().toString().trim();
//    		if(posOrNeg.equals("-")) {
//    			if(isDouble) {
//        			retNum = -1.0 * retNum.doubleValue();
//    			} else {
//        			retNum = -1 * retNum.intValue();
//    			}
//    		}
//    	}
    	NounMetadata noun = null;
    	if(isDouble) {
    		noun = new NounMetadata(retNum.doubleValue(), PixelDataType.CONST_DECIMAL);
    	} else {
    		noun = new NounMetadata(retNum.intValue(), PixelDataType.CONST_INT);
    	}
    	if(curReactor != null) {
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
    	// determine if it is a negative
    	// in which case, multiply by -1
//    	if(node.getPosOrNeg() != null) {
//    		String posOrNeg = node.getPosOrNeg().toString().trim();
//    		if(posOrNeg.equals("-")) {
//        		retNum = -1.0 * retNum.doubleValue();
//    		}
//    	}
    	if(curReactor != null) {
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
    
//    @Override
//    public void inAPercentageWholeDecimal(APercentageWholeDecimal node) {
//    	defaultIn(node);
//    	String whole = "";
//    	String fraction = "";
//    	// get the whole portion
//    	if(node.getWhole() != null) {
//    		whole = node.getWhole().toString().trim();
//    	}
//    	// get the fraction portion
//    	if(node.getFraction() != null) {
//    		fraction = (node.getFraction()+"").trim();
//    	} else {
//    		fraction = "0";
//    	}
//		Number retNum = new BigDecimal(whole + "." + fraction).divide(new BigDecimal(100));
//    	// determine if it is a negative
//    	// in which case, multiply by -1
////    	if(node.getPosOrNeg() != null) {
////    		String posOrNeg = node.getPosOrNeg().toString().trim();
////    		if(posOrNeg.equals("-")) {
////    			if(isDouble) {
////        			retNum = -1.0 * retNum.doubleValue();
////    			} else {
////        			retNum = -1 * retNum.intValue();
////    			}
////    		}
////    	}
//    	NounMetadata noun = new NounMetadata(retNum.doubleValue(), PkslDataTypes.CONST_DECIMAL);
//    	if(curReactor != null) {
//    		curReactor.getCurRow().add(noun);
//	    	// modify the parent such that the signature has the correct
//	    	// value of the numerical without any extra spaces
//	    	// plain string will always modify to a integer even if we return double value
//	    	curReactor.modifySignature(node.toString().trim(), new BigDecimal(retNum.doubleValue()).toPlainString());
//    	} else {
//    		// looks like you just have a number...
//    		// i guess i will return this?
//    		this.planner.addVariable("$RESULT", noun);
//    	}
//    }
//    
//    @Override
//    public void inAPercentageFractionDecimal(APercentageFractionDecimal node) {
//    	defaultIn(node);
//    	String fraction = (node.getFraction()+"").trim();
//		Number retNum = new BigDecimal("0.00" + fraction);
//    	// determine if it is a negative
//    	// in which case, multiply by -1
////    	if(node.getPosOrNeg() != null) {
////    		String posOrNeg = node.getPosOrNeg().toString().trim();
////    		if(posOrNeg.equals("-")) {
////        		retNum = -1.0 * retNum.doubleValue();
////    		}
////    	}
//    	if(curReactor != null) {
//	    	// add the decimal to the cur row
//	        curReactor.getCurRow().addDecimal(retNum.doubleValue());
//	    	// modify the parent such that the signature has the correct
//	    	// value of the numerical without any extra spaces
//	    	// plain string will always modify to a integer even if we return double value
//	    	curReactor.modifySignature(node.toString().trim(), new BigDecimal(retNum.doubleValue()).toPlainString());
//    	} else {
//    		NounMetadata noun = new NounMetadata(retNum.doubleValue(), PkslDataTypes.CONST_DECIMAL);
//    		this.planner.addVariable("$RESULT", noun);
//    	}
//    }

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
    
    @Override
    public void inABaseSimpleComparison(ABaseSimpleComparison node) {
    	defaultIn(node);
    	// I feel like mostly it would be this and not frame op
    	// I almost feel I should remove the frame op col def
    	IReactor opReactor = new OpFilter();
    	initReactor(opReactor);
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
    public void inAList(AList node)
    {
        defaultIn(node);
        IReactor opReactor = new VectorReactor();
        opReactor.setName("Vector");
        opReactor.setPixel("Vector", node.toString().trim());
        initReactor(opReactor);
        
    }

    @Override
    public void outAList(AList node)
    {
    	deInitReactor();
        defaultOut(node);
    }
    
    @Override
    public void inAFormula(AFormula node) {
        defaultIn(node);
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
        if(curReactor instanceof Assimilator) {
        	return;
        } else if(curReactor instanceof AbstractQueryStructReactor) {
        	QuerySelectorExpressionAssimilator qAssm = new QuerySelectorExpressionAssimilator();
        	qAssm.setMathExpr("+");
        	qAssm.setPixel("EXPR", node.toString().trim());
    		initReactor(qAssm);	
        	return;
        }
        
    	defaultIn(node);
		Assimilator assm = new Assimilator();
		
		String leftKey = node.getLeft().toString().trim();
		String rightKey = node.getRight().toString().trim();
		initExpressionToReactor(assm, leftKey, rightKey, "+");
		
		assm.setPixel("EXPR", node.toString().trim());
		initReactor(assm);	
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
    	if(curReactor instanceof Assimilator) {
        	return;
        } else if(curReactor instanceof AbstractQueryStructReactor) {
        	QuerySelectorExpressionAssimilator qAssm = new QuerySelectorExpressionAssimilator();
        	qAssm.setMathExpr("-");
        	qAssm.setPixel("EXPR", node.toString().trim());
    		initReactor(qAssm);	
        	return;
        }
    	
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
		
		assm.setPixel("EXPR", node.toString().trim());
		initReactor(assm);
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
    	if(curReactor instanceof Assimilator) {
        	return;
        } else if(curReactor instanceof AbstractQueryStructReactor) {
        	QuerySelectorExpressionAssimilator qAssm = new QuerySelectorExpressionAssimilator();
        	qAssm.setMathExpr("/");
        	qAssm.setPixel("EXPR", node.toString().trim());
    		initReactor(qAssm);	
        	return;
        }
    	
    	defaultIn(node);
		Assimilator assm = new Assimilator();
		
		String leftKey = node.getLeft().toString().trim();
		String rightKey = node.getRight().toString().trim();
		initExpressionToReactor(assm, leftKey, rightKey, "/");
		
		assm.setPixel("EXPR", node.toString().trim());
		initReactor(assm);
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
    	if(curReactor instanceof Assimilator) {
        	return;
        } else if(curReactor instanceof AbstractQueryStructReactor) {
        	QuerySelectorExpressionAssimilator qAssm = new QuerySelectorExpressionAssimilator();
        	qAssm.setMathExpr("*");
        	qAssm.setPixel("EXPR", node.toString().trim());
    		initReactor(qAssm);	
        	return;
        }
    	
    	defaultIn(node);
		Assimilator assm = new Assimilator();
		
		String leftKey = node.getLeft().toString().trim();
		String rightKey = node.getRight().toString().trim();
		initExpressionToReactor(assm, leftKey, rightKey, "*");
		
		assm.setPixel("EXPR", node.toString().trim());
		initReactor(assm);
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
    
    @Override
    public void inAModBaseExpr(AModBaseExpr node) {
    	if(curReactor instanceof Assimilator) {
        	return;
        } else if(curReactor instanceof AbstractQueryStructReactor) {
        	QuerySelectorExpressionAssimilator qAssm = new QuerySelectorExpressionAssimilator();
        	qAssm.setMathExpr("%");
        	qAssm.setPixel("EXPR", node.toString().trim());
    		initReactor(qAssm);	
        	return;
        }
    	
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
		
		assm.setPixel("EXPR", node.toString().trim());
		initReactor(assm);
    }
    
    @Override
    public void outAModBaseExpr(AModBaseExpr node)
    {
    	defaultOut(node);
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
//----------------------------- Private methods

    protected void initReactor(IReactor reactor)
    {
    	// make a check to see if the curReactor is not null
    	IReactor parentReactor = curReactor;
    	if(parentReactor != null) {
    		reactor.setParentReactor(parentReactor);
    		if(reactor instanceof GreedyJsonReactor)
    			((GreedyJsonReactor)reactor).superParentReactor(superParent);
    		parentReactor.setChildReactor(reactor);
    	}
    	else
    	{
    		superParent = reactor;
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
    	}
    }

	protected void deInitReactor()
	{
		if(curReactor != null)
		{
			// merge up and update the plan
			try {
				curReactor.mergeUp();
				curReactor.updatePlan();
			} catch(Exception e) {
				LOGGER.error(e.getMessage());
				throw new IllegalArgumentException(e.getMessage());
			}
			// get the parent
			Object parent = curReactor.Out();
						
			// set the parent as the curReactor if it is present
			if(parent != null && parent instanceof IReactor) {
				curReactor = (IReactor)parent;
			} else {
				curReactor = null;
			}
		}
	}
    
    public IDataMaker getDataMaker() {
    	try {
    		return (IDataMaker) planner.getProperty("FRAME", "FRAME");
    	} catch(Exception e) {
    		return null;
    	}
    }
    
    //////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////
    
    /*
     * This section is for processing maps
     * These are all just things to store
     * No additional processing occurs within the map inputs
     */
    
    public void inAMapEntry(AMapEntry node)
    {
    	// I need to make a check to see if the previous operation here
    	// is a map
    	// if so this could be a full array
    	
    	processNode = node.getKey().toString().trim();
    	processNode = PixelUtility.removeSurroundingQuotes(processNode);
    	
    	// the first JSON map
    	// will contain the request type
    	// which is either CREATE, EDIT, or VIEW
    	if(this.requestType == null) {
    		this.requestType = this.processNode;
    	}
    	
		// set the current noun so when the result comes it is meaningful
    	if(curReactor != null)
    		curReactor.curNoun(processNode);
    	// if the curReactor is null - make one here
    	else
    	{
    		IReactor newReactor = new LevelMaker();
    		GenRowStruct msg = new GenRowStruct();
    		msg.add(node.toString(), PixelDataType.CONST_STRING);
    		newReactor.getNounStore().addNoun(GreedyJsonReactor.FULL_MESSAGE, msg);
    		newReactor.setPixel(processNode, node.toString().trim());
			Vector <String>childVector = null;
			if(newReactor.hasProp("CHILDS"))
				childVector = (Vector<String>)newReactor.getProp("CHILDS");
			else 
				childVector = new Vector<String>();
			childVector.add(processNode);
			newReactor.setProp("CHILDS", childVector);
			initReactor(newReactor);
			newReactor.curNoun(processNode);
    	}
    	//System.out.println("Map Entry.. IN" + processNode);
        defaultIn(node);
    }

    public void outAMapEntry(AMapEntry node)
    {
    	processNode = node.getKey().toString().trim();
    	processNode = PixelUtility.removeSurroundingQuotes(processNode);
    	//System.out.println("Map Entry.. OUT" + processNode);
        defaultOut(node);
        if(curReactor.getParentReactor() == null)
        {
        	//System.out.println("This is where it should stop.. ");
        	deInitReactor();
        }
    }
    
    public void inANestedMapValues(ANestedMapValues node)
    {
    	//System.out.println("In nested.. " + node);

    	// couple of things to do here
    	// see if this is designated as a master type
    	// if this is a new super node
    	// if the previous one has not closed out
    	// then this is the child
    	// finish the validate
    	// get the hashtable
    	// create the new reactor and proceed
    	
    	// if this is a new super node
    	// it has already been closed out
    	// then this is a sibling
    	// set it as such as proceed    	
    	
    	// if not close out the previous one
    	// get the hash and set it here
    	IReactor thisReactor = factory.getReactor(processNode, this.requestType) ;

    	// need a way to get the hash so I can set it here
    	// i.e. hash from the sibling / from the parent
    	if(thisReactor != null)
    	{
    		thisReactor.setPixel(processNode, node.toString().trim());
    		// need to find a way to continue the nounstore
    		if(curReactor != null)
    		{
    	    	
    	    	// set the store and such
    	    	// the idea behind this is to take the store and add the child automatically and parent
    	    	// so that I can search
    	    	// this way, when the information comes back, I dont need to reset it as parent
    	    	// it is already there by the way of the child if I so chose to
    	    	// I am not sure this step is even needed now.. 
    	    	// I will come back to this part later
    	    	/*
	    		NounStore store = curReactor.getNounStore();
	    		NounStore newStore = thisReactor.getNounStore();
	    		
	    		GenRowStruct newStoreStruct = new GenRowStruct();
	    		newStoreStruct.add(newStore, PkslDataType.MAP);
	    		store.addNoun(IReactor.CHILD, newStoreStruct);
	    		
	    		GenRowStruct curStoreStruct = new GenRowStruct();
	    		curStoreStruct.add(store, PkslDataType.MAP);
	    		newStore.addNoun(IReactor.PARENT, newStoreStruct);
	    		*/
    			// set this as one of the child    			
    			// this is kind of a super index for everything else
    			Vector <String>childVector = null;
    			if(curReactor.hasProp("CHILDS"))
    				childVector = (Vector<String>)curReactor.getProp("CHILDS");
    			else 
    				childVector = new Vector<String>();
    			childVector.add(processNode);
    			curReactor.setProp("CHILDS", childVector);
    		}
			initReactor(thisReactor);
    	}
    	// need to record the last key so that the value can be set
    	if(thisReactor == null)
    	{
    		IReactor newReactor = new LevelMaker();
    		newReactor.setPixel(processNode, node.toString().trim());
			Vector <String>childVector = null;
			if(curReactor.hasProp("CHILDS"))
				childVector = (Vector<String>)curReactor.getProp("CHILDS");
			else 
				childVector = new Vector<String>();
			childVector.add(processNode);
			curReactor.setProp("CHILDS", childVector);
    		//curReactor.setProp(IReactor.LAST_KEY, node.getKey().getText());
    		// need a way to say this is another noun store
    		// right now it is getting flattened out
    		//curReactor.curNoun(processNode);
			initReactor(newReactor);
    	}
    }

    public void outANestedMapValues(ANestedMapValues node)
    {
    	//System.out.println("Out nested.. " + node);
    	//IReactor thisReactor =JsonReactorFactory.getReactor(node.getKey().getText()) ;
    	
    	//if(thisReactor != null)
    	{
    		//System.out.println("Processing >>  " + processNode);
    		//System.out.println(curReactor.getNounStore().getNounKeys());
    		deInitReactor();
    	}
    }

    // this is for arrays.. but I do think we already do this
    public void inAListValues(AListValues node)
    {
        defaultIn(node);
    }

    public void outAListValues(AListValues node)
    {
        defaultOut(node);
    }    
    // now I need to add the values as they come through
    // the value can be scalar 
    // this is the scalar
    // I actually dont need to worry about this
    // this automatically happens
    public void inANormalScalarMapBaseInput(ANormalScalarMapBaseInput node)
    {
        defaultIn(node);
    }

    public void outANormalScalarMapBaseInput(ANormalScalarMapBaseInput node)
    {
        defaultOut(node);
    }

    
    @Override
    public void inAMap(AMap node) {
    	defaultIn(node);
    	//IReactor thisReactor =JsonReactorFactory.getReactor(node.getMapEntry()) ;
    	// couple of things to do here
    	// see if this is designated as a master type
    	// if this is a new super node
    	// if the previous one has not closed out
    	// then this is the child
    	// finish the validate
    	// get the hashtable
    	// create the new reactor and proceed
    	
    	// if this is a new super node
    	// it has already been closed out
    	// then this is a sibling
    	// set it as such as proceed    	
    	
    	// if not close out the previous one
    	// get the hash and set it here
    	//initReactor(mapR);
    }
    
    @Override
    public void outAMap(AMap node) {
    	defaultOut(node);
    	// get the super parent of the currentt reactor and call it a day
    	//deInitReactor();
    }
    
    @Override
    public void inAMapList(AMapList node) {
    	// I need to try to instantiate the processor here again
    	// and obviously do the child manipulation again ?
    	defaultIn(node);
    	
    	IReactor thisReactor = factory.getReactor(processNode, this.requestType) ;
    	// need a way to get the hash so I can set it here
    	// i.e. hash from the sibling / from the parent
    	if(thisReactor != null)
    	{
    		thisReactor.setPixel(processNode, node.toString().trim());
    		// need to find a way to continue the nounstore
    		if(curReactor != null)
    		{
    	    	
    	    	// set the store and such
    	    	// the idea behind this is to take the store and add the child automatically and parent
    	    	// so that I can search
    	    	// this way, when the information comes back, I dont need to reset it as parent
    	    	// it is already there by the way of the child if I so chose to
    	    	// I am not sure this step is even needed now.. 
    	    	// I will come back to this part later
    	    	/*
	    		NounStore store = curReactor.getNounStore();
	    		NounStore newStore = thisReactor.getNounStore();
	    		
	    		GenRowStruct newStoreStruct = new GenRowStruct();
	    		newStoreStruct.add(newStore, PkslDataType.MAP);
	    		store.addNoun(IReactor.CHILD, newStoreStruct);
	    		
	    		GenRowStruct curStoreStruct = new GenRowStruct();
	    		curStoreStruct.add(store, PkslDataType.MAP);
	    		newStore.addNoun(IReactor.PARENT, newStoreStruct);
	    		*/
    			// set this as one of the child    			
    			// this is kind of a super index for everything else
    			Vector <String>childVector = null;
    			if(curReactor.hasProp("CHILDS"))
    				childVector = (Vector<String>)curReactor.getProp("CHILDS");
    			else 
    				childVector = new Vector<String>();
    			childVector.add(processNode);
    			curReactor.setProp("CHILDS", childVector);
    		}
			initReactor(thisReactor);
    	}
    	// need to record the last key so that the value can be set
    	if(thisReactor == null)
    	{
    		IReactor newReactor = new MapListReactor();
    		newReactor.setPixel(processNode, node.toString().trim());
			Vector <String>childVector = null;
			if(curReactor.hasProp("CHILDS"))
				childVector = (Vector<String>)curReactor.getProp("CHILDS");
			else 
				childVector = new Vector<String>();
			childVector.add(processNode);
			curReactor.setProp("CHILDS", childVector);
    		//curReactor.setProp(IReactor.LAST_KEY, node.getKey().getText());
    		// need a way to say this is another noun store
    		// right now it is getting flattened out
    		//curReactor.curNoun(processNode);
			initReactor(newReactor);
    	}

    	
    	//MapListReactor mapListR = new MapListReactor();
    	//initReactor(mapListR);
    }
    
    @Override
    public void outAMapList(AMapList node) {
    	defaultOut(node);
    	deInitReactor();
    }
  
    @Override
    public void inANestedMapMapExtendedInput(ANestedMapMapExtendedInput node)
    {
        defaultIn(node);
        // may be move the curReactor to the name of the processnode ?
        //curReactor.curNoun(processNode);
    	IReactor newReactor = new MapMapReactor();
    	initReactor(newReactor);
    	
    	//if(curReactor instanceof MapListReactor)
    	//	curReactor.setProp("Hashtable", true);
    }

    @Override
    public void outANestedMapMapExtendedInput(ANestedMapMapExtendedInput node)
    {
        defaultOut(node);
    	deInitReactor();
    	//if(curReactor instanceof MapListReactor)
    	//	curReactor.setProp("Hashtable", true);
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
    
    /////////////////////////////////////////////////////////////////
    ////////////////////// JSON Stuff///////////////////////////////
    ///////////////////////////////////////////////////////////////

	//////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////
    
    /**
     * 
     * @param reactorId - reactor id or operation
     * @param nodeString - full operation
     * @return
     * 
     * Return the reactor based on the reactorId
     * Sets the PKSL operations in the reactor
     */
    private IReactor getReactor(String reactorId, String nodeString) {
    	return ReactorFactory.getReactor(reactorId, nodeString, (ITableDataFrame) getDataMaker(), curReactor);
    }
    
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
    
    public void toJson(Object object)
    {
		
		StringBuffer retOutput = new StringBuffer();
		// right now I am keeping everything
		Gson gson = new GsonBuilder()
		.disableHtmlEscaping()
		.excludeFieldsWithModifiers(Modifier.STATIC, Modifier.TRANSIENT)
		//.registerTypeAdapter(Double.class, new NumberAdaptor())
		.create();
		String propString = gson.toJson(object);
		System.out.println("End JSON.. ");
		System.out.println(propString);
		//retOutput.append(propString);
    }
}