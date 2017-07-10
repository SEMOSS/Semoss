package prerna.sablecc2;

import java.math.BigDecimal;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc2.analysis.DepthFirstAdapter;
import prerna.sablecc2.node.AAsop;
import prerna.sablecc2.node.AAssignRoutine;
import prerna.sablecc2.node.AAssignment;
import prerna.sablecc2.node.ABooleanScalar;
import prerna.sablecc2.node.ACodeNoun;
import prerna.sablecc2.node.AComparisonExpr;
import prerna.sablecc2.node.ADivBaseExpr;
import prerna.sablecc2.node.ADotcol;
import prerna.sablecc2.node.AEmbeddedAssignment;
import prerna.sablecc2.node.AFormula;
import prerna.sablecc2.node.AFractionDecimal;
import prerna.sablecc2.node.AFrameop;
import prerna.sablecc2.node.AFrameopRegTerm;
import prerna.sablecc2.node.AGeneric;
import prerna.sablecc2.node.AIdWordOrId;
import prerna.sablecc2.node.AList;
import prerna.sablecc2.node.AMinusBaseExpr;
import prerna.sablecc2.node.AModBaseExpr;
import prerna.sablecc2.node.AMultBaseExpr;
import prerna.sablecc2.node.ANegTerm;
import prerna.sablecc2.node.AOperationFormula;
import prerna.sablecc2.node.AOpformulaRegTerm;
import prerna.sablecc2.node.AOutputRoutine;
import prerna.sablecc2.node.APlusBaseExpr;
import prerna.sablecc2.node.APower;
import prerna.sablecc2.node.AProp;
import prerna.sablecc2.node.ARcol;
import prerna.sablecc2.node.ARelationship;
import prerna.sablecc2.node.ASelectNoun;
import prerna.sablecc2.node.AWholeDecimal;
import prerna.sablecc2.node.AWordWordOrId;
import prerna.sablecc2.node.Node;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.reactor.AssignmentReactor;
import prerna.sablecc2.reactor.Assimilator;
import prerna.sablecc2.reactor.GenericReactor;
import prerna.sablecc2.reactor.IReactor;
import prerna.sablecc2.reactor.IfReactor;
import prerna.sablecc2.reactor.NegReactor;
import prerna.sablecc2.reactor.PKSLPlanner;
import prerna.sablecc2.reactor.PowAssimilator;
import prerna.sablecc2.reactor.ReactorFactory;
import prerna.sablecc2.reactor.VectorReactor;
import prerna.sablecc2.reactor.expression.OpFilter;
import prerna.sablecc2.reactor.qs.AsReactor;
import prerna.ui.components.playsheets.datamakers.IDataMaker;

public class LazyTranslation extends DepthFirstAdapter {

	private static final Logger LOGGER = LogManager.getLogger(LazyTranslation.class.getName());
	
	protected IReactor curReactor = null;
	protected IReactor prevReactor = null;
	public PKSLPlanner planner;

	protected TypeOfOperation operationType = TypeOfOperation.COMPOSITION;
	
	//TODO: should probably use this instead of just commenting out the code in default in/out
	protected boolean printTraceData = false;
	public enum TypeOfOperation {PIPELINE, COMPOSITION};
	
	// there really will be 2 versions of this
	// one for overall and one as the user does it
	// for now.. this is just how the user does it
	protected String lastOperation = null;
	
	public LazyTranslation() {
		this.planner = new PKSLPlanner();
	}
	
	public LazyTranslation(VarStore varStore) {
		this.planner = new PKSLPlanner();
		this.planner.setVarStore(varStore);
	}
	
	protected void postProcess() {
		// do nothing
	}
	
/********************** First is the main level operation, script chain or other script operations ******************/
	
	@Override
	public void inAOutputRoutine(AOutputRoutine node) {
		defaultIn(node);
	}
	
	@Override
	public void outAOutputRoutine(AOutputRoutine node) {
		defaultIn(node);
        // we do this in case someone is doing an embedded assignment 
        // instead of just doing a normal assignment...
        if(!(curReactor instanceof AssignmentReactor)) {
    		postProcess();
    	}
	}
	
	@Override
	public void inAAssignRoutine(AAssignRoutine node) {
		defaultIn(node);
	}
	
	@Override
	public void outAAssignRoutine(AAssignRoutine node) {
		defaultIn(node);
        // we do this in case someone is dumb and is doing an embedded assignment 
        // instead of just doing a normal assignment...
    	postProcess();
	}
	
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
	 public void inAFrameopRegTerm(AFrameopRegTerm node) {
		defaultIn(node);
    	LOGGER.debug("In a frameop term!");
        // once I am in here I am again in the realm of composition
        this.operationType = TypeOfOperation.COMPOSITION;
	}
	
	@Override
	public void outAFrameopRegTerm(AFrameopRegTerm node) {
		defaultOut(node);
    	LOGGER.debug("Out a frameop term!");
        this.operationType = TypeOfOperation.PIPELINE;
	}
	
	@Override
	public void inAOpformulaRegTerm(AOpformulaRegTerm node) {
		defaultIn(node);
    	LOGGER.debug("In a operational formula script");
        this.operationType = TypeOfOperation.COMPOSITION;
	}
	
	@Override
	public void outAOpformulaRegTerm(AOpformulaRegTerm node) {
		defaultOut(node);
        LOGGER.debug("Out a operational formula script");
        this.operationType = TypeOfOperation.PIPELINE;
	}

    /************************************ SECOND is basically secondary operations ***********************/
    /**** Part 1 - those things that can originate their own genrowstruct ***********/    
    // these things need to check to see if these can extend existing script or should it be its own thing
    
	@Override
	public void inANegTerm(ANegTerm node) {
		if(curReactor instanceof Assimilator)
        {
        	return;
        }
		defaultIn(node);
    	IReactor negReactor = new NegReactor();
    	negReactor.setPKSL(node.getTerm().toString().trim(), node.toString().trim());
    	initReactor(negReactor);
	}
	
	@Override
	public void outANegTerm(ANegTerm node) {
		if((node.toString()).trim().equalsIgnoreCase(curReactor.getOriginalSignature())) {
	    	deInitReactor();
		}
	}
	
    // first of which is a frame operation
    public void inAFrameop(AFrameop node)
    {
    	defaultIn(node);
        LOGGER.debug("Starting a frame operation");
        
        // create a sample reactor
//        IReactor frameReactor = new SampleReactor();
//        frameReactor.setPKSL(node.getId()+"", node+"");
        String reactorId = node.getId().toString().trim();
        IReactor frameReactor = getReactor(reactorId, node.toString().trim());
        initReactor(frameReactor);
        syncResult();
     }

    public void outAFrameop(AFrameop node)
    {
        // I realize I am not doing anything with the output.. this will come next
    	defaultOut(node);
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
        this.lastOperation = node + "";
    }
    // setting the as value on a frame
    public void inAAsop(AAsop node)
    {
    	defaultIn(node);
        IReactor opReactor = new AsReactor();
        LOGGER.debug("In the AS Component of frame op");
        opReactor.setPKSL("as", node.getAsOp() + "");
        initReactor(opReactor);
    }

    public void outAAsop(AAsop node)
    {
        defaultOut(node);
        LOGGER.debug("OUT the AS Component of frame op");
        deInitReactor();
    }

    public void inAAssignment(AAssignment node)
    {
    	defaultIn(node);
    	IReactor assignmentReactor = new AssignmentReactor();
        assignmentReactor.setPKSL(node.getWordOrId().toString().trim(), node.toString().trim());
    	initReactor(assignmentReactor);
    }

    public void outAAssignment(AAssignment node)
    {
    	deInitReactor();
        defaultOut(node);
    }
    
    public void inAEmbeddedAssignment(AEmbeddedAssignment node)
    {
    	defaultIn(node);
    	IReactor assignmentReactor = new AssignmentReactor();
        assignmentReactor.setPKSL(node.getId().toString().trim(), node.toString().trim());
    	initReactor(assignmentReactor);
    }

    public void outAEmbeddedAssignment(AEmbeddedAssignment node)
    {
    	deInitReactor();
        defaultOut(node);
    }
    
    public void inAGeneric(AGeneric node) {
    	defaultIn(node);
    	IReactor genReactor = new GenericReactor();
        genReactor.setPKSL("PKSL", (node + "").trim());
        genReactor.setProp("KEY", node.getId().toString().trim());
        initReactor(genReactor);
    }

    public void outAGeneric(AGeneric node) {
    	defaultOut(node);
    	deInitReactor();
    }

    // second is a simpler operation
    // something(a,b,c,d) <-- note the absence of square brackets, really not needed
    public void inAOperationFormula(AOperationFormula node)
    {
    	defaultIn(node);
        LOGGER.debug("Starting a formula operation " + node.getId());
        // I feel like mostly it would be this and not frame op
        // I almost feel I should remove the frame op col def
        IReactor opReactor = getReactor(node.getId().toString().trim(), node.toString().trim());
        initReactor(opReactor);
        syncResult();
    }

    private void syncResult() {
    	// after we init the new reactor
        // we will add the result of the last reactor 
        // into the noun store of this new reactor
    	NounMetadata prevResult = this.planner.getVariableValue("$RESULT");
    	if(prevResult != null) {
    		PkslDataTypes nounName = prevResult.getNounName();
    		GenRowStruct genRow = curReactor.getNounStore().makeNoun(nounName.toString());
    		genRow.add(prevResult, nounName);
    		// then we will remove the result from the planner
        	this.planner.removeVariable("$RESULT");
    	}
    }
    
    public void outAOperationFormula(AOperationFormula node)
    {
    	defaultOut(node);
    	String nodeKey = node.toString().trim();
    	if(node.getPlainRow() != null) {
	    	String childKey = node.getPlainRow().toString().trim();
	    	Object childOutput = curReactor.getProp(childKey);
	    	
	    	if(childOutput != null) {
	    		curReactor.setProp(nodeKey, childOutput);
	    	}
    	}
    	// if we have an if reactor
    	// we will not deInit and just add this as a lambda within
    	// the if statement and only deinit when necessary
    	if(curReactor.getParentReactor() instanceof IfReactor)
    	{
            curReactor.setPKSLPlanner(planner);
    		curReactor.getParentReactor().getCurRow().addLambda(curReactor);
    		curReactor = curReactor.getParentReactor();
    	}
    	// if this is not a child of an if
    	// just execute as normal
    	else
    	{
    		deInitReactor();
    	}
        this.lastOperation = node + "";
    }
    
//    public void inAROp(AROp node) {
//		RReactor reactor = new RReactor();
//		initReactor(reactor);
//		String nodeExpr = node.getR().toString().trim();
//		NounMetadata noun = new NounMetadata(nodeExpr, PkslDataTypes.RCODE);
//		curReactor.getNounStore().makeNoun("RCODE").add(noun, PkslDataTypes.RCODE);
//    }
//
//    public void outAROp(AROp node) {
//    	deInitReactor();
//    }
    
    
    // accumulating secondary structures
    // the values of each of these are generic rows
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

    public void outASelectNoun(ASelectNoun node)
    {
        defaultOut(node);
        // not sure if I need anything else right now
        curReactor.closeNoun("s");
    }

    
    // all the utility method, which merely accumulate the data in some fashion later to be used by other functions
    public void inARcol(ARcol node)
    {
        defaultIn(node);
        // I need to do the work in terms of finding what is the column name
        String column = node.getFrameprefix()+ "." + node.getNumber();
        curReactor.getCurRow().addColumn(column);
    }

    public void outARcol(ARcol node)
    {
        defaultOut(node);
    }
    
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
    			NounMetadata noun = new NounMetadata(column, PkslDataTypes.CONST_STRING);
        		this.planner.addVariable("$RESULT", noun);
    		}
    	}
    }
    
    public void inAWordWordOrId(AWordWordOrId node)
    {
        defaultIn(node);
        String trimmedWord = node.getWord().toString().trim();
        String word = PkslUtility.removeSurroundingQuotes(trimmedWord);
        if(curReactor != null) {
	        curReactor.getCurRow().addLiteral(word);
	        curReactor.setProp(node.toString().trim(), word);
        } else {
        	NounMetadata noun = new NounMetadata(word, PkslDataTypes.CONST_STRING);
    		this.planner.addVariable("$RESULT", noun);
        }
    }
    
    public void inABooleanScalar(ABooleanScalar node) {
    	defaultIn(node);
    	String booleanStr = node.getBoolean().toString().trim();
    	Boolean bool = Boolean.parseBoolean(booleanStr);
        if(curReactor != null) {
	        curReactor.getCurRow().addBoolean(bool);
	        curReactor.setProp(node.toString().trim(), booleanStr);
        } else {
        	NounMetadata noun = new NounMetadata(bool, PkslDataTypes.CONST_STRING);
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
    		noun = new NounMetadata(retNum.doubleValue(), PkslDataTypes.CONST_DECIMAL);
    	} else {
    		noun = new NounMetadata(retNum.intValue(), PkslDataTypes.CONST_INT);
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
    		NounMetadata noun = new NounMetadata(retNum.doubleValue(), PkslDataTypes.CONST_DECIMAL);
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
    
    
    public void inADotcol(ADotcol node)
    {
    	defaultIn(node);
        // I need to do the work in terms of finding what is the column name
        String column = (node.getFrameid()+ "." + node.getColumnName()).trim();
        curReactor.getCurRow().addColumn(column);
    }

    public void outADotcol(ADotcol node)
    {
        defaultOut(node);
    }
    
    public void inAProp(AProp node)
    {
    	defaultIn(node);
    	String key = node.getId().toString().trim();
    	String propValue = node.getScalar().toString().trim();
        curReactor.setProp(key, propValue);
    }
    
    public void inACodeNoun(ACodeNoun node)
    {
        defaultIn(node);
        String code = (node.getCodeAlpha() + "");
        code = code.replace("<c>",""); 
        code = code.replace("</c>","");
        code = code.trim();
        curReactor.setProp("CODE", code);
    }

    public void outACodeNoun(ACodeNoun node)
    {
        defaultOut(node);
    }

    // you definitely need the other party to be in a relationship :)
    // this is of the form colDef followed by type of relation and another col def
    
    public void inARelationship(ARelationship node)
    {
        defaultIn(node);    
        // Need some way to track the state to say all the other things are interim
        // so I can interpret the dot notation etc for frame columns
    }

    //why is relationship implemented differently than filter?
    //they do very similar things
    public void outARelationship(ARelationship node)
    {
    	String leftCol = node.getLcol().toString().trim();
    	String rightCol = node.getRcol().toString().trim();
    	String relType = node.getRelType().toString().trim();
    	curReactor.getCurRow().addRelation(leftCol, relType, rightCol);
        defaultOut(node);
    }

    @Override
    public void inAComparisonExpr(AComparisonExpr node) {
    	defaultIn(node);
    	// I feel like mostly it would be this and not frame op
    	// I almost feel I should remove the frame op col def
    	IReactor opReactor = new OpFilter();
    	initReactor(opReactor);
    	syncResult();

//    	defaultIn(node);
//        IReactor opReactor = new FilterReactor();
//        opReactor.setName("Filter");
//        opReactor.setPKSL("Filter", node.toString().trim());
//        initReactor(opReactor);
//        GenRowStruct genRow = curReactor.getNounStore().makeNoun("COMPARATOR");
//        genRow.add(node.getComparator().toString().trim(), PkslDataTypes.COMPARATOR);
        // Need some way to track the state to say all the other things are interim
        // so I can interpret the dot notation etc for frame columns
    }

    @Override
    public void outAComparisonExpr(AComparisonExpr node) {
    	defaultOut(node);
    	deInitReactor();
    }

    public void caseAComparisonExpr(AComparisonExpr node)
    {
        inAComparisonExpr(node);
        if(node.getLeft() != null)
        {
            node.getLeft().apply(this);
        }
        if(node.getComparator() != null)
        {
        	curReactor.getCurRow().addLiteral(node.getComparator().toString().trim());
        }
        if(node.getRight() != null)
        {
            node.getRight().apply(this);
        }
        outAComparisonExpr(node);
    }
    
//    @Override
//    public void caseAComparisonExpr(AComparisonExpr node)
//    {
//    	inAComparisonExpr(node);
////        if(node.getLPar() != null)
////        {
////            node.getLPar().apply(this);
////        }
//        if(node.getLeft() != null)
//        {
//        	// we will change the curNoun here
//        	// so when we hit a literal
//        	// it will be cast to the appropriate type
//        	curReactor.curNoun("LCOL");
//        	node.getLeft().apply(this);
//            // we need to account for various expressions being evaluated within a filter
//            // if there is a result, grab the result and set it as a lcol in the filter reactor
//            NounMetadata prevResult = this.planner.getVariableValue("$RESULT");
//        	if(prevResult != null) {
//        		GenRowStruct genRow = curReactor.getNounStore().makeNoun("LCOL");
//        		genRow.add(prevResult, prevResult.getNounName());
//        		this.planner.removeVariable("$RESULT");
//        	}
//        }
//        // we put the comparator into the noun store of the filter
//        // during the init of the filter reactor
////        if(node.getComparator() != null)
////        {
////            node.getComparator().apply(this);
////        }
//        if(node.getRight() != null)
//        {
//            // we will change the curNoun here
//        	// so when we hit a literal
//        	// it will be cast to the appropriate type
//        	curReactor.curNoun("RCOL");
//        	node.getRight().apply(this);
//            // we need to account for various expressions being evaluated within a filter
//            // if there is a result, grab the result and set it as a lcol in the filter reactor
//            NounMetadata prevResult = this.planner.getVariableValue("$RESULT");
//        	if(prevResult != null) {
//        		GenRowStruct genRow = curReactor.getNounStore().makeNoun("RCOL");
//        		genRow.add(prevResult, prevResult.getNounName());
//        		this.planner.removeVariable("$RESULT");
//        	}
//        }
////        if(node.getRPar() != null)
////        {
////            node.getRPar().apply(this);
////        }
//        outAComparisonExpr(node);
//    }
    
    /**************************************************
     *	Expression Methods
     **************************************************/

    public void inAList(AList node)
    {
        defaultIn(node);
        IReactor opReactor = new VectorReactor();
        opReactor.setName("Vector");
        opReactor.setPKSL("Vector", node.toString().trim());
        initReactor(opReactor);
        
    }

    public void outAList(AList node)
    {
    	deInitReactor();
        defaultOut(node);
    }
    
    public void inAFormula(AFormula node) {
        defaultIn(node);
    }

    public void outAFormula(AFormula node) {
        defaultOut(node);
    }
    
    @Override
    public void inAPlusBaseExpr(APlusBaseExpr node) {
        if(curReactor instanceof Assimilator)
        {
        	return;
        }
    	defaultIn(node);
		Assimilator assm = new Assimilator();
		
		String leftKey = node.getLeft().toString().trim();
		String rightKey = node.getRight().toString().trim();
		initExpressionToReactor(assm, leftKey, rightKey, "+");
		
		assm.setPKSL("EXPR", node.toString().trim());
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
        if(curReactor instanceof Assimilator)
        {
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
		
		assm.setPKSL("EXPR", node.toString().trim());
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
        if(curReactor instanceof Assimilator)
        {
        	return;
        }
    	defaultIn(node);
		Assimilator assm = new Assimilator();
		
		String leftKey = node.getLeft().toString().trim();
		String rightKey = node.getRight().toString().trim();
		initExpressionToReactor(assm, leftKey, rightKey, "/");
		
		assm.setPKSL("EXPR", node.toString().trim());
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
        if(curReactor instanceof Assimilator)
        {
        	return;
        }
    	defaultIn(node);
		Assimilator assm = new Assimilator();
		
		String leftKey = node.getLeft().toString().trim();
		String rightKey = node.getRight().toString().trim();
		initExpressionToReactor(assm, leftKey, rightKey, "*");
		
		assm.setPKSL("EXPR", node.toString().trim());
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
    	powAssm.setPKSL("EXPR", node.toString().trim());
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
        if(curReactor instanceof Assimilator)
        {
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
		
		assm.setPKSL("EXPR", node.toString().trim());
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
		leftGenRow.add(left, PkslDataTypes.CONST_STRING);
    
    	GenRowStruct rightGenRow = reactor.getNounStore().makeNoun("RIGHT");
    	rightGenRow.add(right, PkslDataTypes.CONST_STRING);
    
    	GenRowStruct operator = reactor.getNounStore().makeNoun("OPERATOR");
    	operator.add(operation, PkslDataTypes.CONST_STRING);
    }
    
    /**************************************************
     *	End Expression Methods
     **************************************************/
//----------------------------- Private methods

    protected void initReactor(IReactor reactor)
    {
        // make a check to see if the curReactor is not null
        if(curReactor != null)
        {
    		// yes I know they do the same thing.. 
        	// but I am atleast checking the operation type before doing it
        	if(operationType == TypeOfOperation.COMPOSITION) {
	        	reactor.setParentReactor(curReactor);
	        	curReactor.setChildReactor(reactor);
        	} else {
	        	reactor.setParentReactor(curReactor);
	        	curReactor.setChildReactor(reactor);
        	}
        }
        curReactor = reactor;
        curReactor.setPKSLPlanner(planner);
        curReactor.In();
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
    		return (IDataMaker)planner.getProperty("FRAME", "FRAME");
    	} catch(Exception e) {
    		return null;
    	}
    }
    
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
    	return ReactorFactory.getReactor(reactorId, nodeString, (ITableDataFrame)getDataMaker(), curReactor);
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

	public Object getResults() {
		return planner.getProperty("DATA", "DATA");
	}
}