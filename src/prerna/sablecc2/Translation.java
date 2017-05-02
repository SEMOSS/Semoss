package prerna.sablecc2;

import java.math.BigDecimal;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.h2.H2Frame;
import prerna.sablecc2.analysis.DepthFirstAdapter;
import prerna.sablecc2.node.AAsop;
import prerna.sablecc2.node.AAssignment;
import prerna.sablecc2.node.ACodeNoun;
import prerna.sablecc2.node.ADivExpr;
import prerna.sablecc2.node.ADotcol;
import prerna.sablecc2.node.ADotcolColDef;
import prerna.sablecc2.node.AEmbeddedAssignment;
import prerna.sablecc2.node.AFilter;
import prerna.sablecc2.node.AFormula;
import prerna.sablecc2.node.AFractionDecimal;
import prerna.sablecc2.node.AFrameop;
import prerna.sablecc2.node.AFrameopTerm;
import prerna.sablecc2.node.AGeneric;
import prerna.sablecc2.node.AIdWordOrId;
import prerna.sablecc2.node.AList;
import prerna.sablecc2.node.AMinusExpr;
import prerna.sablecc2.node.AMultExpr;
import prerna.sablecc2.node.AOperationFormula;
import prerna.sablecc2.node.AOpformulaTerm;
import prerna.sablecc2.node.AOutputRoutine;
import prerna.sablecc2.node.APlusExpr;
import prerna.sablecc2.node.APower;
import prerna.sablecc2.node.AProp;
import prerna.sablecc2.node.ARcol;
import prerna.sablecc2.node.ARelationship;
import prerna.sablecc2.node.ASelectNoun;
import prerna.sablecc2.node.ATermExpr;
import prerna.sablecc2.node.AWholeDecimal;
import prerna.sablecc2.node.AWordWordOrId;
import prerna.sablecc2.node.Node;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.AssignmentReactor;
import prerna.sablecc2.reactor.Assimilator;
import prerna.sablecc2.reactor.FilterReactor;
import prerna.sablecc2.reactor.GenericReactor;
import prerna.sablecc2.reactor.IReactor;
import prerna.sablecc2.reactor.IfReactor;
import prerna.sablecc2.reactor.PKSLPlanner;
import prerna.sablecc2.reactor.PowAssimilator;
import prerna.sablecc2.reactor.ReactorFactory;
import prerna.sablecc2.reactor.VectorReactor;
import prerna.sablecc2.reactor.qs.AsReactor;
import prerna.ui.components.playsheets.datamakers.IDataMaker;

public class Translation extends DepthFirstAdapter {

	private static final Logger LOGGER = LogManager.getLogger(Translation.class.getName());
	
	IReactor curReactor = null;
	IReactor prevReactor = null;
	
	TypeOfOperation operationType = TypeOfOperation.COMPOSITION;
	
	boolean printTraceData = false;
	String filepath = "";
	boolean logToFile = false;
	
	public enum TypeOfOperation {PIPELINE, COMPOSITION};
	
	// there really will be 2 versions of this
	// one for overall and one as the user does it
	// for now.. this is just how the user does it
	public PKSLPlanner planner;
	public String lastOperation = null;
	
	
	//The reactor factory associated with this translation
	//Need a reactor factory
	PKSLRunner runner; //need this to store return data to...maybe? or is that the planner
	
	String FILENAME = "C:\\Workspace\\Semoss_Dev\\log\\pksl_execution.log";
	/***************************
	 * CONSTRUCTORS
	 ***************************/
	public Translation() {
		this.planner = new PKSLPlanner();
		this.planner.addProperty("FRAME", "FRAME", new H2Frame());
		this.runner = new PKSLRunner();
	}
	
	public Translation(PKSLRunner runner) {
		this.planner = new PKSLPlanner();
		this.planner.addProperty("FRAME", "FRAME", new H2Frame());
		this.runner = runner;
	}
	
	public Translation(IDataMaker dataMaker, PKSLRunner runner) {
		this.planner = new PKSLPlanner(dataMaker);
		this.planner.addProperty("FRAME", "FRAME", dataMaker);
		this.runner = runner;
	}
	
	public void setFileName(String fileName) {
	}
 	
	/***************************
	 * END CONSTRUCTORS
	 ***************************/
	
	void postProcess() {
		//grab the frame here
		IDataMaker frame = null;
		try {
			//get the noun meta result
			//if it is a frame
			//then set the frame to the frame result
			NounMetadata noun = planner.getVariableValue("$RESULT");
			this.runner.setResult(noun);
			Object frameNoun = noun.getValue();
			if(frameNoun instanceof IDataMaker){
				frame = (IDataMaker) frameNoun;
			}
		} catch(Exception e) {
			
		}
		if(frame != null) {
			runner.setDataFrame(frame);
		}
		curReactor = null;
		prevReactor = null;
		lastOperation = null;
		this.planner.removeVariable("$RESULT");
	}
/********************** First is the main level operation, script chain or other script operations ******************/
	
	@Override
	public void inAOutputRoutine(AOutputRoutine node) {
		defaultIn(node);
	}
	
	public void outAOutputRoutine(AOutputRoutine node) {
		defaultIn(node);
        // we do this in case someone is dumb and is doing an embedded assignment 
        // instead of just doing a normal assignment...
        if(!(curReactor instanceof AssignmentReactor)) {
    		postProcess();
    	}
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
	public void inAFrameopTerm(AFrameopTerm node) {
		defaultIn(node);
    	LOGGER.debug("In a frameop term!");
        // once I am in here I am again in the realm of composition
        this.operationType = TypeOfOperation.COMPOSITION;
	}
	
	@Override
	public void outAFrameopTerm(AFrameopTerm node) {
		defaultOut(node);
    	LOGGER.debug("Out a frameop term!");
        this.operationType = TypeOfOperation.PIPELINE;
	}
	
	@Override
	public void inAOpformulaTerm(AOpformulaTerm node) {
		defaultIn(node);
    	LOGGER.debug("In a operational formula script");
        this.operationType = TypeOfOperation.COMPOSITION;
	}
	
	@Override
	public void outAOpformulaTerm(AOpformulaTerm node) {
		defaultOut(node);
        LOGGER.debug("Out a operational formula script");
        this.operationType = TypeOfOperation.PIPELINE;
	}

    /************************************ SECOND is basically secondary operations ***********************/
    /**** Part 1 - those things that can originate their own genrowstruct ***********/    
    // these things need to check to see if these can extend existing script or should it be its own thing
    
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
    	}
    	// then we will remove the result from the planner
    	this.planner.removeVariable("$RESULT");
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
    /*
    @Override
    
    public void caseAPlainRow(APlainRow node)
    {
        inAPlainRow(node);
        if(node.getColDef() != null)
        {
            node.getColDef().apply(this); //IF(comparison, truecomparison, falsecomparison)
        }
        
        {
        	//TODO : EXPLAIN THIS!!!
        	if(curReactor instanceof IfReactor) {
        		NounMetadata result = (NounMetadata)curReactor.getNounStore().getNoun("f").get(0);
        		
        		//determine the result of the comparison
        		boolean runFalseCase = result.getValue() == null || result.getValue().equals("FALSE") || result.getValue().equals(0);
        		
        		if(runFalseCase) {
        			List<POthercol> copy = new ArrayList<POthercol>(node.getOthercol());
    	            if(copy.size() >= 2) {
    	            	POthercol e = copy.get(1);
    	            	e.apply(this);
    	            }
        		} else {
        			List<POthercol> copy = new ArrayList<POthercol>(node.getOthercol());
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
        outAPlainRow(node);
        
    }
	*/
    
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
    	curReactor.getCurRow().addColumn(column);
    	curReactor.setProp(node.toString().trim(), column);
    }
    
    public void inAWordWordOrId(AWordWordOrId node)
    {
        defaultIn(node);
        String word = null;
        String trimedWord = node.getWord().toString().trim();
        if(trimedWord.length() == 2) {
        	// this is when you have an empty string
        	// so we will just add an empty string
        	word = "";
        } else {
        	// return values between the quotes
        	word = trimedWord.substring(1, trimedWord.length()-1).trim();
        }
        curReactor.getCurRow().addLiteral(word);
        curReactor.setProp(node.toString().trim(), word);
    }

    // this is a higher level method which is kind of useless
    public void inADotcolColDef(ADotcolColDef node)
    {
        defaultIn(node);
    }

    public void outADotcolColDef(ADotcolColDef node)
    {
        defaultOut(node);
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
    	if(node.getPosOrNeg() != null) {
    		String posOrNeg = node.getPosOrNeg().toString().trim();
    		if(posOrNeg.equals("-")) {
    			if(isDouble) {
        			retNum = -1.0 * retNum.doubleValue();
    			} else {
        			retNum = -1 * retNum.intValue();
    			}
    		}
    	}
    	// add the decimal to the cur row
    	if(isDouble) {
        	curReactor.getCurRow().addDecimal(retNum.doubleValue());
    	} else {
        	curReactor.getCurRow().addInteger(retNum.intValue());
    	}
    	// modify the parent such that the signature has the correct
    	// value of the numerical without any extra spaces
    	// plain string will always modify to a integer even if we return double value
    	curReactor.modifySignature(node.toString().trim(), new BigDecimal(retNum.doubleValue()).toPlainString());
    }

    public void outAWholeDecimal(AWholeDecimal node)
    {
    	defaultOut(node);
    }
    
    @Override
    public void inAFractionDecimal(AFractionDecimal node)
    {
    	defaultIn(node);
    	String fraction = (node.getFraction()+"").trim();
		Number retNum = new BigDecimal("0." + fraction);
    	// determine if it is a negative
    	// in which case, multiply by -1
    	if(node.getPosOrNeg() != null) {
    		String posOrNeg = node.getPosOrNeg().toString().trim();
    		if(posOrNeg.equals("-")) {
        		retNum = -1.0 * retNum.doubleValue();
    		}
    	}
    	// add the decimal to the cur row
        curReactor.getCurRow().addDecimal(retNum.doubleValue());
    	// modify the parent such that the signature has the correct
    	// value of the numerical without any extra spaces
    	// plain string will always modify to a integer even if we return double value
    	curReactor.modifySignature(node.toString().trim(), new BigDecimal(retNum.doubleValue()).toPlainString());
    }

    @Override
    public void outAFractionDecimal(AFractionDecimal node)
    {
    	defaultOut(node);
    }
    
    
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
    
    public void inATermExpr(ATermExpr node)
    {
        defaultIn(node);
        // seems like I need to see if this is part of a mult expression etc.
        // else start the assimilator. 
        // this is a special case where it is jsut coming through as a column || variable
        if(curReactor instanceof Assimilator)
        	LOGGER.debug("Ignore this");
//        else if(node.getTerm() instanceof AColTerm)
//        {
//        	// need to see if there are other things to work on
//    		Assimilator assm = new Assimilator();
//    		initExpressionToReactor(assm, node.getTerm()+"", 1+"", "*");
//    		assm.setPKSL("EXPR", node.toString().trim(), node.toString().trim());
//    		initReactor(assm);
//        	LOGGER.debug("Capture this");
//        }
    }

    public void outATermExpr(ATermExpr node)
    {
        defaultOut(node);
//        if(curReactor instanceof Assimilator && (node.toString()).trim().equalsIgnoreCase(curReactor.getSignature()))
//        {
//        	LOGGER.debug("Ignore this Term Expression OUT");
//    		deInitReactor();
//        }
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

    public void inAFilter(AFilter node)
    {
        defaultIn(node);
        IReactor opReactor = new FilterReactor();
        opReactor.setName("Filter");
        opReactor.setPKSL("Filter", node.toString().trim());
        initReactor(opReactor);
        GenRowStruct genRow = curReactor.getNounStore().makeNoun("COMPARATOR");
        genRow.add(node.getComparator().toString().trim(), PkslDataTypes.COMPARATOR);
        // Need some way to track the state to say all the other things are interim
        // so I can interpret the dot notation etc for frame columns
    }

    public void outAFilter(AFilter node) {
        defaultOut(node);
        deInitReactor();
    }
    
    @Override
    public void caseAFilter(AFilter node)
    {
        inAFilter(node);
//        if(node.getLPar() != null)
//        {
//            node.getLPar().apply(this);
//        }
        if(node.getLcol() != null)
        {
        	// we will change the curNoun here
        	// so when we hit a literal
        	// it will be cast to the appropriate type
        	curReactor.curNoun("LCOL");
        	node.getLcol().apply(this);
            // we need to account for various expressions being evaluated within a filter
            // if there is a result, grab the result and set it as a lcol in the filter reactor
            NounMetadata prevResult = this.planner.getVariableValue("$RESULT");
        	if(prevResult != null) {
        		GenRowStruct genRow = curReactor.getNounStore().makeNoun("LCOL");
        		genRow.add(prevResult, prevResult.getNounName());
        		this.planner.removeVariable("$RESULT");
        	}
        }
        // we put the comparator into the noun store of the filter
        // during the init of the filter reactor
//        if(node.getComparator() != null)
//        {
//            node.getComparator().apply(this);
//        }
        if(node.getRcol() != null)
        {
            // we will change the curNoun here
        	// so when we hit a literal
        	// it will be cast to the appropriate type
        	curReactor.curNoun("RCOL");
        	node.getRcol().apply(this);
            // we need to account for various expressions being evaluated within a filter
            // if there is a result, grab the result and set it as a lcol in the filter reactor
            NounMetadata prevResult = this.planner.getVariableValue("$RESULT");
        	if(prevResult != null) {
        		GenRowStruct genRow = curReactor.getNounStore().makeNoun("RCOL");
        		genRow.add(prevResult, prevResult.getNounName());
        		this.planner.removeVariable("$RESULT");
        	}
        }
//        if(node.getRPar() != null)
//        {
//            node.getRPar().apply(this);
//        }
        outAFilter(node);
    }
    
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
    public void inAPlusExpr(APlusExpr node) {
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
    public void outAPlusExpr(APlusExpr node)
    {
    	defaultOut(node);
    	// deinit this only if this is the same node
    	if((node.toString()).trim().equalsIgnoreCase(curReactor.getOriginalSignature()))
  			deInitReactor();
    }
    
    @Override
    public void inAMinusExpr(AMinusExpr node) {
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
    public void outAMinusExpr(AMinusExpr node)
    {
    	defaultOut(node);
    	if((node.toString()).trim().equalsIgnoreCase(curReactor.getOriginalSignature()))
    	deInitReactor();
    }
    
    @Override
    public void inADivExpr(ADivExpr node) {
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
    public void outADivExpr(ADivExpr node)
    {
    	defaultOut(node);
    	if((node.toString()).trim().equalsIgnoreCase(curReactor.getOriginalSignature()))
    	deInitReactor();
    }
    
    @Override
    public void inAMultExpr(AMultExpr node) {
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
    public void outAMultExpr(AMultExpr node)
    {
    	defaultOut(node);
    	if((node.toString()).trim().equalsIgnoreCase(curReactor.getOriginalSignature()))
    		deInitReactor();
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
    	if((node.toString()).trim().equalsIgnoreCase(curReactor.getOriginalSignature()))
    		deInitReactor();
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
	    	
	    	NounMetadata output = curReactor.execute();
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