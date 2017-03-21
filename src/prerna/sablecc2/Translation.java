package prerna.sablecc2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ds.h2.H2Frame;
import prerna.sablecc.expressions.sql.builder.ExpressionGenerator;
import prerna.sablecc.expressions.sql.builder.SqlExpressionBuilder;
import prerna.sablecc2.analysis.DepthFirstAdapter;
import prerna.sablecc2.node.AAsop;
import prerna.sablecc2.node.AAssignment;
import prerna.sablecc2.node.ACodeNoun;
import prerna.sablecc2.node.AColTerm;
import prerna.sablecc2.node.ADecimal;
import prerna.sablecc2.node.ADivExpr;
import prerna.sablecc2.node.ADotcol;
import prerna.sablecc2.node.ADotcolColDef;
import prerna.sablecc2.node.AEExprExpr;
import prerna.sablecc2.node.AExprColDef;
import prerna.sablecc2.node.AFilter;
import prerna.sablecc2.node.AFormula;
import prerna.sablecc2.node.AFrameop;
import prerna.sablecc2.node.AFrameopColDef;
import prerna.sablecc2.node.AFrameopScript;
import prerna.sablecc2.node.AGeneric;
import prerna.sablecc2.node.ALiteralColDef;
import prerna.sablecc2.node.AMinusExpr;
import prerna.sablecc2.node.AMultExpr;
import prerna.sablecc2.node.AOpScript;
import prerna.sablecc2.node.AOperationFormula;
import prerna.sablecc2.node.AOtherscript;
import prerna.sablecc2.node.APlainRow;
import prerna.sablecc2.node.APlusExpr;
import prerna.sablecc2.node.AProp;
import prerna.sablecc2.node.AROp;
import prerna.sablecc2.node.ARcol;
import prerna.sablecc2.node.ARelationship;
import prerna.sablecc2.node.AScriptchain;
import prerna.sablecc2.node.ASelectNoun;
import prerna.sablecc2.node.ATermExpr;
import prerna.sablecc2.node.Node;
import prerna.sablecc2.node.PColDef;
import prerna.sablecc2.node.POthercol;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.reactor.AsReactor;
import prerna.sablecc2.reactor.AssignmentReactor;
import prerna.sablecc2.reactor.ExprReactor;
import prerna.sablecc2.reactor.FilterReactor;
import prerna.sablecc2.reactor.GenericReactor;
import prerna.sablecc2.reactor.IReactor;
import prerna.sablecc2.reactor.IfReactor;
import prerna.sablecc2.reactor.PKSLPlanner;
import prerna.sablecc2.reactor.RReactor;
import prerna.sablecc2.reactor.ReactorFactory;
import prerna.sablecc2.reactor.SampleReactor;
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
	PKSLPlanner planner;
	String lastOperation = null;
	
	
	//The reactor factory associated with this translation
	//Need a reactor factory
	PKSLRunner runner; //need this to store return data to...maybe? or is that the planner
	
	String FILENAME = "C:\\Workspace\\Semoss_Dev\\log\\pksl_execution.log";
	/***************************
	 * CONSTRUCTORS
	 ***************************/
	public Translation() {
		planner = new PKSLPlanner();
	}
	
	public Translation(PKSLRunner runner) {
		this.runner = runner;
	}
	
	public Translation(IDataMaker dataMaker, PKSLRunner runner) {
		planner = new PKSLPlanner(dataMaker);
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
			frame = (IDataMaker)planner.getProperty("FRAME", "FRAME");
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
	
    public void inAScriptchain(AScriptchain node)
    {
    	defaultIn(node);
    	System.out.println(">>>>>>");
        System.out.println("Called script chain");
        System.out.println("First operation is " + node.getOtherscript());
        if(node.getOtherscript() != null && node.getOtherscript().size() > 0)
        	operationType = TypeOfOperation.PIPELINE;
        
    }
    
    public void outAScriptchain(AScriptchain node)
    {
    	defaultOut(node);
    	// this is really where the overall execution should happen
    	System.out.println("<<<<<<<" + node);
//    	planner.runMyPlan(lastOperation);
    	if(!(curReactor instanceof AssignmentReactor)) {
    		postProcess();
    	}
    }
    
    public void inAOtherscript(AOtherscript node)
    {
    	defaultIn(node);
        System.out.println("Other script.. " + node);
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
    public void inAFrameopScript(AFrameopScript node)
    {
    	defaultIn(node);
        System.out.println("In a script frameop");
        // once I am in here I am again in the realm of composition
        this.operationType = TypeOfOperation.COMPOSITION;
    }

    public void outAFrameopScript(AFrameopScript node)
    {
    	defaultOut(node);
        System.out.println("Out a script frameop");
        this.operationType = TypeOfOperation.PIPELINE;
    }

    public void inAOpScript(AOpScript node)
    {
    	defaultIn(node);
        System.out.println("In a operational formula script");
        this.operationType = TypeOfOperation.COMPOSITION;
    }

    public void outAOpScript(AOpScript node)
    {
    	defaultOut(node);
        System.out.println("Out a operational formula script");
        this.operationType = TypeOfOperation.PIPELINE;
    }

    // still need to accomodate for Java operation, assignment, r operation etc. 
    
    /************************************ SECOND is basically secondary operations ***********************/
    /**** Part 1 - those things that can originate their own genrowstruct ***********/    
    // these things need to check to see if these can extend existing script or should it be its own thing
    
    // first of which is a frame operation
    public void inAFrameop(AFrameop node)
    {
    	defaultIn(node);
        System.out.println("Starting a frame operation");
        
        // create a sample reactor
//        IReactor frameReactor = new SampleReactor();
//        frameReactor.setPKSL(node.getId()+"", node+"");
        String reactorId = node.getId().toString().trim();
        IReactor frameReactor = getReactor(reactorId, node.toString()); //get the reactor
        initReactor(frameReactor);
     }

    public void outAFrameop(AFrameop node)
    {
        // I realize I am not doing anything with the output.. this will come next
    	defaultOut(node);
    	deInitReactor();
    	/*
        curReactor.Out();
        if(curReactor.getParentReactor() != null)
        	curReactor = curReactor.getParentReactor();
        */
        this.lastOperation = node + "";
        System.out.println("OUT of Frame OP");
    }
    // setting the as value on a frame
    public void inAAsop(AAsop node)
    {
    	defaultIn(node);
        IReactor opReactor = new AsReactor();
        System.out.println("In the AS Component of frame op");
        opReactor.setPKSL("as", node.getAsOp() + "");
        initReactor(opReactor);
    }

    public void outAAsop(AAsop node)
    {
        defaultOut(node);
        System.out.println("OUT the AS Component of frame op");
        deInitReactor();
    }

    public void inAAssignment(AAssignment node)
    {
    	defaultIn(node);
//    	if(!this.planner.hasProperty("VARIABLE", "VARIABLE")) {
//    		this.planner.addProperty("VARIABLE", "VARIABLE", new HashMap<String, NounMetadata>());
//    	}
    	IReactor assignmentReactor = new AssignmentReactor();
        assignmentReactor.setPKSL(node.getId().toString().trim(), node.toString().trim());
    	initReactor(assignmentReactor);
    }

    public void outAAssignment(AAssignment node)
    {
    	deInitReactor();
        defaultOut(node);
    }
    
    public void inAGeneric(AGeneric node) {
    	defaultIn(node);
    	IReactor genReactor = new GenericReactor();
        genReactor.setPKSL("PKSL",(node + "").trim());
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
        System.out.println("Starting a formula operation " + node.getId());
        // I feel like mostly it would be this and not frame op
        // I almost feel I should remove the frame op col def
//        IReactor opReactor = getReactor(node.getId().toString().trim(), node.toString());
        IReactor opReactor = getReactor(node.getId().toString().trim(), node.toString());
        if(opReactor instanceof SampleReactor) {
	        opReactor = new ExprReactor();
	        Map<String, String> scriptReactors = new H2Frame().getScriptReactors();
	        String reactorName = scriptReactors.get(node.getId().toString().trim().toUpperCase());
	        
	        //if((node.getId() + "").trim().equalsIgnoreCase("as"))
	        //	opReactor = new AsReactor();
	        opReactor.setPKSL(node.getId().toString().trim(), node.toString().trim());
	        opReactor.setName("OPERATION_FORMULA");
	        opReactor.setProp("REACTOR_NAME", reactorName);
        }
        // since there are no square brackets.. there are no nouns
        //opReactor.curNoun("all");
        initReactor(opReactor);
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
    	
    	NounMetadata prevResult = this.planner.getVariable("$RESULT");
    	if(prevResult != null) {
    		String nounName = prevResult.getNounName();
    		GenRowStruct genRow = curReactor.getNounStore().makeNoun(nounName);
    		genRow.add(prevResult);
    	}
    	
        deInitReactor();
        System.out.println("Ending a formula operation");
        
//        if(curReactor.getParentReactor() == null)
//        {
//        	// this is the last stop
//        	
//        }
        this.lastOperation = node + "";
    }
    
//    @Override
//    public void caseAOperationFormula(AOperationFormula node)
//    {
//        inAOperationFormula(node);
//        if(node.getId() != null)
//        {
//            node.getId().apply(this);
//        }
//
//        if(node.getPlainRow() != null)
//        {
//        	//always run the first one
//        	//if first is if reactor
//        	//get the return
//        	APlainRow apr = (APlainRow)node.getPlainRow();
//        	PColDef x = apr.getColDef();
//        	//if x instance of if statement
//        	
//            node.getPlainRow().apply(this);
//        }
//
//        if(node.getAsop() != null)
//        {
//            node.getAsop().apply(this);
//        }
//        outAOperationFormula(node);
//    }
    
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

    public void inAROp(AROp node) {
//    	if (reactorNames.containsKey(PKQLEnum.JAVA_OP)) {
    		RReactor reactor = new RReactor();
			initReactor(reactor);
//			curReactor.put("PKQLRunner", runner);
			String nodeExpr = node.getR().toString().trim();
			NounMetadata noun = new NounMetadata(nodeExpr, "RCODE");
			curReactor.getNounStore().makeNoun("RCODE").add(noun);
//		}
    }

    public void outAROp(AROp node) {
    	deInitReactor();
    }
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

    public void inALiteralColDef(ALiteralColDef node)
    {
    	defaultIn(node);
    	String thisLiteral = node.getLiteral()+"";
    	thisLiteral = thisLiteral.replace("'","");
    	thisLiteral = thisLiteral.replace("\"","");
    	thisLiteral = thisLiteral.trim();
        curReactor.getCurRow().addLiteral(thisLiteral);
    }
    
    private Object getValueFromLiteral(String thisLiteral) {
    	if(thisLiteral.startsWith("'") || thisLiteral.startsWith("\"")) {
    		thisLiteral = thisLiteral.replace("'","");
        	thisLiteral = thisLiteral.replace("\"","");
        	thisLiteral = thisLiteral.trim();
        	return thisLiteral;
    	} else {
    		//it is either a column header or a variable
    		if(this.planner.hasVariable(thisLiteral)) {
    			return this.planner.getVariable(thisLiteral);
    		} else {
    			return thisLiteral; //its a column header in this case
    		}
    	}
    }

    public void outALiteralColDef(ALiteralColDef node)
    {
        defaultOut(node);
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

    // need to watch if at thsi point.. I need to start a new one or keep on with the previous one
    // still not clear to me if I do it here or in the frame op
    public void inAFrameopColDef(AFrameopColDef node)
    {
    	defaultIn(node);
        System.out.println("In a Frame op col DEF a.k.a already within a script" + node);
        // so this is where things REALLY get interesting
        // I have to work through here to figure out what to do with it
        // the only difference here is am I adding this to genrowstruct
        // or am I adding it as a full reactor
    }

    public void outAFrameopColDef(AFrameopColDef node)
    {
    	defaultOut(node);
        System.out.println("OUT a Frame op col DEF a.k.a already within a script" + node);
    }

    
    // I am still not sure if an expression qualifies but.. 
    // I also think at this point if we should just make it where the expressions are being printed out 
    // into the no named class for us to evaluate ?
	// things I should avoid.. if I am already in a expr and I keep coming into expression.. just ignore each one of those
	// the first one already has all that I need
    public void inAExprColDef(AExprColDef node)
    {
        defaultIn(node);
        System.out.println("In a Expr" + node);
        
        // this case is very similar to operational formula
        // I need to see if I am already in a parent which is a expr
        // if so.. I should just assimilate
        // instead of trying to start a new reactor here
        // this check should happen inside the reactor moving it to the reactor
        //if(curReactor != null && !curReactor.getName().equalsIgnoreCase("EXPR"))
//        {
//	        IReactor opReactor = new ExprReactor();
//	        opReactor.setName("EXPR");
//	        opReactor.setPKSL("EXPR",node.getExpr()+"");
//	        
////	        opReactor.setProp("REACTOR_NAME", reactorName);
//	        initReactor(opReactor);
//        }
        // else ignore.. this has been taken care of kind of
    }

    public void outAExprColDef(AExprColDef node)
    {
        defaultOut(node);
        // TODO need to do the operation of assimilating here
//        deInitReactor();
    }

    // code sits here
/*    public void inACodeColDef(ACodeColDef node)
    {
        IReactor codeReactor = new CodeReactor();
        String code = (node.getCode2()+"").trim();
        codeReactor.setName("Code");
        codeReactor.setPKSL("code", code);
        initReactor(codeReactor);
    }

    public void outACodeColDef(ACodeColDef node)
    {
        defaultOut(node);
        deInitReactor();
    }
*/
    
    // atomic level stuff goes in here
    
    public void inAEExprExpr(AEExprExpr node)
    {
    	defaultIn(node);
        System.out.println("IN Atomic.. " + "expr" + node);
    }

    public void outAEExprExpr(AEExprExpr node)
    {
    	defaultOut(node);
        System.out.println("OUT Atomic.. " + "expr" + node);
    }

    
    public void inADecimal(ADecimal node)
    {
    	defaultIn(node);
        System.out.println("Decimal is" + node.getWhole());
        String fraction = "0";
        if(node.getFraction() != null) fraction = (node.getFraction()+"").trim();
        String value = (node.getWhole()+"").trim() +"." + fraction;
        Double adouble = Double.parseDouble(value);
        curReactor.getCurRow().addDecimal(adouble);
        curReactor.setProp(node.toString().trim(), adouble);
        curReactor.setProp("LAST_VALUE", adouble);
    }

    public void outADecimal(ADecimal node)
    {
        defaultOut(node);
    }
    
    public void inAColTerm(AColTerm node)
    {
    	defaultIn(node);
        String column = (node.getCol()+"").trim();
        curReactor.getCurRow().addColumn(column);
        curReactor.setProp(node.toString().trim(), column);
    }

    public void outAColTerm(AColTerm node)
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
        System.out.println("Term.. " + node);
        defaultIn(node);
    }

    public void outATermExpr(ATermExpr node)
    {
        defaultOut(node);
    }
    
    public void inAProp(AProp node)
    {
    	defaultIn(node);
    	String key = node.getId().toString().trim();
    	String propValue = node.getNumberOrLiteral().toString().trim();
        curReactor.setProp(key, propValue);
    }
    
    public void inACodeNoun(ACodeNoun node)
    {
        defaultIn(node);
        String code = (node.getCodeAlpha() + "");
        code = code.replace("<c>","");
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
        opReactor.setPKSL("Filter",(node + "").trim());
        opReactor.setProp("LCOL", (node.getLcol()+"").trim());
        opReactor.setProp("COMPARATOR", (node.getComparator()+"").trim());
        initReactor(opReactor);
        // Need some way to track the state to say all the other things are interim
        // so I can interpret the dot notation etc for frame columns
    }

    public void outAFilter(AFilter node) {
        defaultOut(node);
        deInitReactor();
    }
    
    /**************************************************
     *	Expression Methods
     **************************************************/

    public void inAFormula(AFormula node) {
        defaultIn(node);
    }

    public void outAFormula(AFormula node) {
        defaultOut(node);
        String expr = node.getExpr().toString().trim();
        if(curReactor.hasProp(expr)) {
        	curReactor.setProp(node.toString().trim(), curReactor.getProp(expr));
        }
        System.out.println(curReactor.getProp("LAST_VALUE"));
    }
    
    public void outAPlusExpr(APlusExpr node)
    {
    	defaultOut(node);
    	
    	String leftKey = node.getLeft().toString().trim();
    	String rightKey = node.getRight().toString().trim();
    	
    	Object rightObj = curReactor.getProp(rightKey);
    	Object leftObj = curReactor.getProp(leftKey);
    	
    	Object result = null;
		if (rightObj instanceof Double && leftObj instanceof Double) {
			result = (Double) (leftObj) + (Double) (rightObj);
			curReactor.setProp(node.toString().trim(), result);
			curReactor.setProp("LAST_VALUE", result);
			System.out.println(result);
		}
		else  
		{
			//TODO: need to make this generic and get the correct type of expression it
			if(getDataMaker() instanceof H2Frame) {
				H2Frame frame = (H2Frame) getDataMaker();
				SqlExpressionBuilder builder = ExpressionGenerator.sqlGenerateSimpleMathExpressions(frame, leftObj, rightObj, node.getPlus().toString().trim());
				curReactor.setProp(node.toString().trim(), builder);
			}
    	}
    }
    
    public void outAMinusExpr(AMinusExpr node)
    {
    	String leftKey = node.getLeft().toString().trim();
    	String rightKey = node.getRight().toString().trim();
    	
    	Object rightObj = curReactor.getProp(rightKey);
    	Object leftObj = curReactor.getProp(leftKey);
    	
    	Object result = null;
		if (rightObj instanceof Double && leftObj instanceof Double) {
			result = (Double) (leftObj) + (Double) (rightObj);
			curReactor.setProp(node.toString().trim(), result);
			curReactor.setProp("LAST_VALUE", result);
			System.out.println(result);
		}
		else  
		{
			//TODO: need to make this generic and get the correct type of expression it
			if(getDataMaker() instanceof H2Frame) {
				H2Frame frame = (H2Frame) getDataMaker();
				SqlExpressionBuilder builder = ExpressionGenerator.sqlGenerateSimpleMathExpressions(frame, leftObj, rightObj, node.getMinus().toString().trim());
				curReactor.setProp(node.toString().trim(), builder);
			}
    	}
    }
    
    @Override
    public void outADivExpr(ADivExpr node)
    {
    	String leftKey = node.getLeft().toString().trim();
    	String rightKey = node.getRight().toString().trim();
    	
    	Object rightObj = curReactor.getProp(rightKey);
    	Object leftObj = curReactor.getProp(leftKey);
    	
    	Object result = null;
		if (rightObj instanceof Double && leftObj instanceof Double) {
			result = (Double) (leftObj) + (Double) (rightObj);
			curReactor.setProp(node.toString().trim(), result);
			curReactor.setProp("LAST_VALUE", result);
			System.out.println(result);
		}
		else  
		{
			//TODO: need to make this generic and get the correct type of expression it
			if(getDataMaker() instanceof H2Frame) {
				H2Frame frame = (H2Frame) getDataMaker();
				SqlExpressionBuilder builder = ExpressionGenerator.sqlGenerateSimpleMathExpressions(frame, leftObj, rightObj, node.getDiv().toString().trim());
				curReactor.setProp(node.toString().trim(), builder);
			}
    	}
    }
    
    @Override
    public void outAMultExpr(AMultExpr node)
    {
    	defaultOut(node);
    	String leftKey = node.getLeft().toString().trim();
    	String rightKey = node.getRight().toString().trim();
    	
    	Object rightObj = curReactor.getProp(rightKey);
    	Object leftObj = curReactor.getProp(leftKey);
    	
    	Object result = null;
		if (rightObj instanceof Double && leftObj instanceof Double) {
			result = (Double) (leftObj) + (Double) (rightObj);
			curReactor.setProp(node.toString().trim(), result);
			curReactor.setProp("LAST_VALUE", result);
			System.out.println(result);
		}
		else  
		{
			//TODO: need to make this generic and get the correct type of expression it
			if(getDataMaker() instanceof H2Frame) {
				H2Frame frame = (H2Frame) getDataMaker();
				SqlExpressionBuilder builder = ExpressionGenerator.sqlGenerateSimpleMathExpressions(frame, leftObj, rightObj, node.getMult().toString().trim());
				curReactor.setProp(node.toString().trim(), builder);
			}
    	}
    }
    
    
    
    /**************************************************
     *	End Expression Methods
     **************************************************/
//----------------------------- Private methods

    private void initReactor(IReactor reactor)
    {
        // make a check to see if the curReactor is not null
    	
    	// TODO : make decision on wether to execute the parent here or some place else
    	LOGGER.info("INIT REACTOR");
    	LOGGER.info("INCOMING REACTOR IS: "+reactor.getName());
    	
        if(curReactor != null)
        {
        	LOGGER.info("CURRENT REACTOR IS: "+curReactor.getName());
    		// yes I know they do the same thing.. 
        	// but I am atleast checking the operation type before doing it
        	if(operationType == TypeOfOperation.COMPOSITION)
        	{
	        	reactor.setParentReactor(curReactor);
	        	curReactor.setChildReactor(reactor);
	        	// in this case evaluation happens on out
        	}
        	else
        	{
	        	reactor.setParentReactor(curReactor);
	        	curReactor.setChildReactor(reactor);
	        	/*
	        	reactor.setChildReactor(curReactor);
	        	curReactor.setParentReactor(reactor);
	        	*/// in this case evaluation happens on in ?
        	}
        }
        curReactor = reactor;
        curReactor.setPKSLPlanner(planner);
        curReactor.In();
    }
    
    private void deInitReactor()
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
	    	Object output = curReactor.execute();
	    	
	    	//set the curReactor
	    	if(parent != null && parent instanceof IReactor) {
	    		curReactor = (IReactor)parent;
	    	} else {
	    		curReactor = null;
	    	}
	    	
	    	//Set the output
	    	if(output instanceof NounMetadata) {
	    		if(output instanceof NounMetadata) {
//	    			if(curReactor != null) {
//	    				String outputNounName = ((NounMetadata) output).getNounName();
//	    				GenRowStruct genRow = curReactor.getNounStore().getNoun(outputNounName);
//	    				if(genRow != null) {
//	    					genRow.add(output);
//	    				} else {
//	    					curReactor.getNounStore().makeNoun(outputNounName).add(output);
//	    				}
//	    			} else {
//	    				this.planner.addVariable("$RESULT", (NounMetadata)output);
//	    			}
	    			
	    			this.planner.addVariable("$RESULT", (NounMetadata)output);
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
    	return ReactorFactory.getReactor(reactorId, nodeString);
    }
    
    private void printData(Object node, String message) {
    	if(this.printTraceData) {
    		
    		if(this.logToFile) {
	    		try (BufferedWriter bw = new BufferedWriter(new FileWriter(FILENAME, true))) {
	
	
	    	    	bw.write("*********************************************");bw.write("\n");
	    	    	bw.write(message);bw.write("\n");
	    	    	StackTraceElement[] ste = Thread.currentThread().getStackTrace();
	    	    	if(ste.length > 2) {
	    	    		bw.write("CURRENT METHOD: "+ste[2].getMethodName());bw.write("\n");
	    	    	}
	    	    	else {
	    	    		bw.write("CURRENT METHOD: UNKNOWN");bw.write("\n");
	    	    	}
	    	    	bw.write("CURRENT NODE VALUE: "+node.toString());bw.write("\n");
	    	    	//print curReactor name
	    	    	if(curReactor != null) {
	    	    		bw.write("CURRENT REACTOR: "+this.curReactor.getName());bw.write("\n");
	    	    		//print current curRow of curReactor
	    	    		bw.write("CURRENT REACTOR'S GENROWSTRUCT: "+this.curReactor.getCurRow());bw.write("\n");
	    	    		bw.write("CURRENT REACTOR'S NOUNSTORE: \n"+curReactor.getNounStore().getDataString());bw.write("\n");
	    	    	} else {
	    	    		bw.write("CURRENT REACTOR: null");bw.write("\n");
	    	    	}
	    	    	bw.write("*********************************************\n");
	
	    			System.out.println("Done");
	
	    		} catch (IOException e) {
	    			e.printStackTrace();
	    		}
    		}
    		
    		else {

		    	LOGGER.info("*********************************************");
		    	LOGGER.info(message);
		    	StackTraceElement[] ste = Thread.currentThread().getStackTrace();
		    	if(ste.length > 2)
		    		LOGGER.info("CURRENT METHOD: "+ste[2].getMethodName());
		    	else
		    		LOGGER.info("CURRENT METHOD: UNKNOWN");
		    	LOGGER.info("CURRENT NODE VALUE: "+node.toString());
		    	//print curReactor name
		    	if(curReactor != null) {
		    		LOGGER.info("CURRENT REACTOR: "+this.curReactor.getName());
		    		//print current curRow of curReactor
		    		LOGGER.info("CURRENT REACTOR'S GENROWSTRUCT: "+this.curReactor.getCurRow());
		    		LOGGER.info("CURRENT REACTOR'S NOUNSTORE: \n"+curReactor.getNounStore().getDataString());
		    	} else {
		    		LOGGER.info("CURRENT REACTOR: null");
		    	}
		    	LOGGER.info("*********************************************\n");
    		}
    	}
    }
    
    public void defaultIn(@SuppressWarnings("unused") Node node)
    {
        // Do nothing
    	StackTraceElement[] ste = Thread.currentThread().getStackTrace();
    	if(ste.length > 2) {
    		LOGGER.info("CURRENT METHOD: "+ste[2].getMethodName());
    		LOGGER.info("CURRENT NODE:" + node.toString() );
    		
    		if(curReactor != null) {
    			LOGGER.info("CURRENT REACTOR: "+curReactor.getClass().getSimpleName());
    		}
    		else LOGGER.info("CURRENT REACTOR: null" + "\n");
    		
    		if(curReactor != null && curReactor.getParentReactor() != null) {
    			LOGGER.info("PARENT REACTOR: "+curReactor.getParentReactor().getClass().getSimpleName() + "\n");
    		}
    	} else {
    		LOGGER.info("THIS SHOULD NOT HAPPEN!!!!!!!!!!");
    	}
    }

    public void defaultOut(@SuppressWarnings("unused") Node node)
    {
        // Do nothing
    	StackTraceElement[] ste = Thread.currentThread().getStackTrace();
    	if(ste.length > 2) {
    		LOGGER.info("CURRENT METHOD: "+ste[2].getMethodName());
    		LOGGER.info("CURRENT NODE:" + node.toString());
    		
    		if(curReactor != null) {
    			LOGGER.info("CURRENT REACTOR: "+curReactor.getClass().getSimpleName());
    		}
    		else LOGGER.info("CURRENT REACTOR: null" + "\n");
    		
    		if(curReactor != null && curReactor.getParentReactor() != null) {
    			LOGGER.info("PARENT REACTOR: "+curReactor.getParentReactor().getClass().getSimpleName() + "\n");
    		}
    	} else {
    		LOGGER.info("THIS SHOULD NOT HAPPEN!!!!!!!!!!");
    	}
    }

	public Object getResults() {
		return planner.getProperty("DATA", "DATA");
	}
}