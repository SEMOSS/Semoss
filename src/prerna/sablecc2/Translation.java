package prerna.sablecc2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.sablecc2.analysis.DepthFirstAdapter;
import prerna.sablecc2.node.AAsop;
import prerna.sablecc2.node.ACodeNoun;
import prerna.sablecc2.node.AColTerm;
import prerna.sablecc2.node.ADecimal;
import prerna.sablecc2.node.ADotcol;
import prerna.sablecc2.node.ADotcolColDef;
import prerna.sablecc2.node.AEExprExpr;
import prerna.sablecc2.node.AExprColDef;
import prerna.sablecc2.node.AFilter;
import prerna.sablecc2.node.AFrameop;
import prerna.sablecc2.node.AFrameopColDef;
import prerna.sablecc2.node.AFrameopScript;
import prerna.sablecc2.node.ALiteralColDef;
import prerna.sablecc2.node.AOpScript;
import prerna.sablecc2.node.AOperationFormula;
import prerna.sablecc2.node.AOtherscript;
import prerna.sablecc2.node.AProp;
import prerna.sablecc2.node.ARcol;
import prerna.sablecc2.node.ARelationship;
import prerna.sablecc2.node.AScriptchain;
import prerna.sablecc2.node.ASelectNoun;
import prerna.sablecc2.node.ATermExpr;
import prerna.sablecc2.reactor.AsReactor;
import prerna.sablecc2.reactor.ExprReactor;
import prerna.sablecc2.reactor.FilterReactor;
import prerna.sablecc2.reactor.IReactor;
import prerna.sablecc2.reactor.ImportDataReactor;
import prerna.sablecc2.reactor.PKSLPlanner;
import prerna.sablecc2.reactor.SampleReactor;
import prerna.ui.components.playsheets.datamakers.IDataMaker;

public class Translation extends DepthFirstAdapter {

	private static final Logger LOGGER = LogManager.getLogger(Translation.class.getName());
	
	IReactor curReactor = null;
	IReactor prevReactor = null;
	
	private int spacecount = 0;
	
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
	
/********************** First is the main level operation, script chain or other script operations ******************/
	
    public void inAScriptchain(AScriptchain node)
    {
    	printData(node, "BEGIN");
    	spacecount++;
    	System.out.println(">>>>>>");
        System.out.println("Called script chain");
        System.out.println("First operation is " + node.getOtherscript());
        if(node.getOtherscript() != null && node.getOtherscript().size() > 0)
        	operationType = TypeOfOperation.PIPELINE;
        
        spacecount--;
        printData(node, "END");
    }
    
    public void outAScriptchain(AScriptchain node)
    {
    	printData(node, "BEGIN");
    	spacecount++;
    	// this is really where the overall execution should happen
    	System.out.println("<<<<<<<" + node);
    	planner.runMyPlan(lastOperation);
    	spacecount--;printData(node, "END");
    }
    
    public void inAOtherscript(AOtherscript node)
    {
    	printData(node, "BEGIN");
    	spacecount++;
    	spacecount--;printData(node, "END");
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
    	printData(node, "BEGIN"); spacecount++;
        System.out.println("In a script frameop");
        // once I am in here I am again in the realm of composition
        this.operationType = TypeOfOperation.COMPOSITION;
        spacecount--;printData(node, "END");
    }

    public void outAFrameopScript(AFrameopScript node)
    {
    	printData(node, "BEGIN"); spacecount++;
        System.out.println("Out a script frameop");
        this.operationType = TypeOfOperation.PIPELINE;
        spacecount--;printData(node, "END");
    }

    public void inAOpScript(AOpScript node)
    {
    	printData(node, "BEGIN"); spacecount++;
        System.out.println("In a operational formula script");
        this.operationType = TypeOfOperation.COMPOSITION;
        spacecount--;printData(node, "END");
    }

    public void outAOpScript(AOpScript node)
    {
    	printData(node, "BEGIN"); spacecount++;
        System.out.println("Out a operational formula script");
        this.operationType = TypeOfOperation.PIPELINE;
        spacecount--;printData(node, "END");
    }

    // still need to accomodate for Java operation, assignment, r operation etc. 
    
    /************************************ SECOND is basically secondary operations ***********************/
    /**** Part 1 - those things that can originate their own genrowstruct ***********/    
    // these things need to check to see if these can extend existing script or should it be its own thing
    
    // first of which is a frame operation
    public void inAFrameop(AFrameop node)
    {
    	printData(node, "BEGIN"); spacecount++;
        System.out.println("Starting a frame operation");
        
        // create a sample reactor
//        IReactor frameReactor = new SampleReactor();
//        frameReactor.setPKSL(node.getId()+"", node+"");
        String reactorId = node.getId().toString().trim();
        IReactor frameReactor = getReactor(reactorId, node.toString()); //get the reactor
        initReactor(frameReactor);
        spacecount--;printData(node, "END");
     }

    public void outAFrameop(AFrameop node)
    {
    	printData(node, "BEGIN"); spacecount++;
        // I realize I am not doing anything with the output.. this will come next
    	
        curReactor.Out();
        if(curReactor.getParentReactor() != null)
        	curReactor = curReactor.getParentReactor();
        
        this.lastOperation = node + "";
        spacecount--;printData(node, "END");
        System.out.println("OUT of Frame OP");
    }
    // setting the as value on a frame
    public void inAAsop(AAsop node)
    {
    	printData(node, "BEGIN"); spacecount++;
        IReactor opReactor = new AsReactor();
        System.out.println("In the AS Component of frame op");
        opReactor.setPKSL("as", node.getAsOp() + "");
        initReactor(opReactor);
        spacecount--;printData(node, "END");
    }

    public void outAAsop(AAsop node)
    {
    	printData(node, "BEGIN"); spacecount++;
        defaultOut(node);
        System.out.println("OUT the AS Component of frame op");
        deInitReactor();
        spacecount--;printData(node, "END");
    }


    // second is a simpler operation
    // something(a,b,c,d) <-- note the absence of square brackets, really not needed
    public void inAOperationFormula(AOperationFormula node)
    {
    	printData(node, "BEGIN"); spacecount++;
        System.out.println("Starting a formula operation " + node.getId());
        // I feel like mostly it would be this and not frame op
        // I almost feel I should remove the frame op col def
        IReactor opReactor = new SampleReactor();
        //if((node.getId() + "").trim().equalsIgnoreCase("as"))
        //	opReactor = new AsReactor();
        opReactor.setPKSL(node.getId()+"", node+"");
        opReactor.setName("OPERATION_FORMULA");
        // since there are no square brackets.. there are no nouns
        //opReactor.curNoun("all");
        initReactor(opReactor);
        spacecount--;printData(node, "END");
    }

    public void outAOperationFormula(AOperationFormula node)
    {
    	printData(node, "BEGIN"); spacecount++;
        deInitReactor();
        System.out.println("Ending a formula operation");
        
        if(curReactor.getParentReactor() == null)
        {
        	// this is the last stop
        	
        }
        this.lastOperation = node + "";
        spacecount--;printData(node, "END");
    }

    // accumulating secondary structures
    // the values of each of these are generic rows
    public void inASelectNoun(ASelectNoun node)
    {
    	printData(node, "BEGIN"); spacecount++;
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
        spacecount--;printData(node, "END");
    }

    public void outASelectNoun(ASelectNoun node)
    {
    	printData(node, "BEGIN"); spacecount++;
        defaultOut(node);
        // not sure if I need anything else right now
        curReactor.closeNoun("s");
        spacecount--;printData(node, "END");
    }

    
    // all the utility method, which merely accumulate the data in some fashion later to be used by other functions
    public void inARcol(ARcol node)
    {
    	printData(node, "BEGIN"); spacecount++;
        defaultIn(node);
        // I need to do the work in terms of finding what is the column name
        String column = node.getFrameprefix()+ "." + node.getNumber();
        curReactor.getCurRow().addColumn(column);
        spacecount--;printData(node, "END");
    }

    public void outARcol(ARcol node)
    {
    	printData(node, "BEGIN"); spacecount++;
        defaultIn(node);
        spacecount--;printData(node, "END");
    }

    public void inALiteralColDef(ALiteralColDef node)
    {
    	printData(node, "BEGIN"); spacecount++;
    	String thisLiteral = node.getLiteral()+"";
    	thisLiteral = thisLiteral.replace("'","");
    	thisLiteral = thisLiteral.replace("\"","");
    	thisLiteral = thisLiteral.trim();
        curReactor.getCurRow().addLiteral(thisLiteral);
        spacecount--;printData(node, "END");
    }

    public void outALiteralColDef(ALiteralColDef node)
    {
    	printData(node, "BEGIN"); spacecount++;
        defaultOut(node);
        spacecount--;printData(node, "END");
    }

    // this is a higher level method which is kind of useless
    public void inADotcolColDef(ADotcolColDef node)
    {
    	printData(node, "BEGIN"); spacecount++;
        defaultIn(node);
        spacecount--;printData(node, "END");
    }

    public void outADotcolColDef(ADotcolColDef node)
    {
    	printData(node, "BEGIN"); spacecount++;
        defaultOut(node);
        spacecount--;printData(node, "END");
    }

    // need to watch if at thsi point.. I need to start a new one or keep on with the previous one
    // still not clear to me if I do it here or in the frame op
    public void inAFrameopColDef(AFrameopColDef node)
    {
    	printData(node, "BEGIN"); spacecount++;
        System.out.println("In a Frame op col DEF a.k.a already within a script" + node);
        // so this is where things REALLY get interesting
        // I have to work through here to figure out what to do with it
        // the only difference here is am I adding this to genrowstruct
        // or am I adding it as a full reactor
    	spacecount--;printData(node, "END");
    }

    public void outAFrameopColDef(AFrameopColDef node)
    {
    	printData(node, "BEGIN"); spacecount++;
    	spacecount--;printData(node, "END");
        System.out.println("OUT a Frame op col DEF a.k.a already within a script" + node);
    }

    
    // I am still not sure if an expression qualifies but.. 
    // I also think at this point if we should just make it where the expressions are being printed out 
    // into the no named class for us to evaluate ?
	// things I should avoid.. if I am already in a expr and I keep coming into expression.. just ignore each one of those
	// the first one already has all that I need
    public void inAExprColDef(AExprColDef node)
    {
    	printData(node, "BEGIN"); spacecount++;
        defaultIn(node);
        System.out.println("In a Expr" + node);
        
        // this case is very similar to operational formula
        // I need to see if I am already in a parent which is a expr
        // if so.. I should just assimilate
        // instead of trying to start a new reactor here
        // this check should happen inside the reactor moving it to the reactor
        //if(curReactor != null && !curReactor.getName().equalsIgnoreCase("EXPR"))
        {
	        IReactor opReactor = new ExprReactor();
	        opReactor.setName("EXPR");
	        opReactor.setPKSL("EXPR",node.getExpr()+"");
	        initReactor(opReactor);
        }
        spacecount--;printData(node, "END");
        // else ignore.. this has been taken care of kind of
    }

    public void outAExprColDef(AExprColDef node)
    {
    	printData(node, "BEGIN"); spacecount++;
        defaultOut(node);
        // TODO need to do the operation of assimilating here
        deInitReactor();
        spacecount--;printData(node, "END");
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
    	printData(node, "BEGIN"); spacecount++;
    	spacecount--;printData(node, "END");
        System.out.println("IN Atomic.. " + "expr" + node);
    }

    public void outAEExprExpr(AEExprExpr node)
    {
    	printData(node, "BEGIN"); spacecount++;
    	spacecount--;printData(node, "END");
        System.out.println("OUT Atomic.. " + "expr" + node);
    }

    
    public void inADecimal(ADecimal node)
    {
    	printData(node, "BEGIN"); spacecount++;
        System.out.println("Decimal is" + node.getWhole());
        String fraction = "0";
        if(node.getFraction() != null) fraction = (node.getFraction()+"").trim();
        String value = (node.getWhole()+"").trim() +"." + fraction;
        Double adouble = Double.parseDouble(value);
        curReactor.getCurRow().addDecimal(adouble);
        spacecount--;printData(node, "END");
    }

    public void outADecimal(ADecimal node)
    {
    	printData(node, "BEGIN"); spacecount++;
        defaultOut(node);
        spacecount--;printData(node, "END");
    }
    
    public void inAColTerm(AColTerm node)
    {
    	printData(node, "BEGIN"); spacecount++;
        String column = (node.getCol()+"").trim();
        curReactor.getCurRow().addColumn(column);
        spacecount--;printData(node, "END");
    }

    public void outAColTerm(AColTerm node)
    {
    	printData(node, "BEGIN"); spacecount++;
    	spacecount--;printData(node, "END");
    }
    
    public void inADotcol(ADotcol node)
    {
    	printData(node, "BEGIN"); spacecount++;
        // I need to do the work in terms of finding what is the column name
        String column = (node.getFrameid()+ "." + node.getColumnName()).trim();
        curReactor.getCurRow().addColumn(column);
        spacecount--;printData(node, "END");
    }

    public void outADotcol(ADotcol node)
    {
    	printData(node, "BEGIN"); spacecount++;
        defaultOut(node);
        spacecount--;printData(node, "END");
    }
    
    public void inATermExpr(ATermExpr node)
    {
    	printData(node, "BEGIN"); spacecount++;
        System.out.println("Term.. " + node);
    	spacecount--;printData(node, "END");
    }

    public void outATermExpr(ATermExpr node)
    {
    	printData(node, "BEGIN"); spacecount++;
        defaultOut(node);
        spacecount--;printData(node, "END");
    }
    
    public void inAProp(AProp node)
    {
    	printData(node, "BEGIN"); spacecount++;
        curReactor.setProp((node.getId()+"").trim(), node.getNumberOrLiteral()+"");
        spacecount--;printData(node, "END");
    }
    
    public void inACodeNoun(ACodeNoun node)
    {
    	printData(node, "BEGIN"); spacecount++;
        defaultIn(node);
        String code = (node.getCodeAlpha() + "");
        code = code.replace("<c>","");
        code = code.trim();
        curReactor.setProp("CODE", code);
        spacecount--;printData(node, "END");
    }

    public void outACodeNoun(ACodeNoun node)
    {
    	printData(node, "BEGIN"); spacecount++;
        defaultOut(node);
        spacecount--;printData(node, "END");
    }

    // you definitely need the other party to be in a relationship :)
    // this is of the form colDef followed by type of relation and another col def
    
    public void inARelationship(ARelationship node)
    {
        defaultIn(node);    
        // Need some way to track the state to say all the other things are interim
        // so I can interpret the dot notation etc for frame columns
    }

    public void outARelationship(ARelationship node)
    {
    	curReactor.getCurRow().addRelation((node.getLcol() + "").trim(), (node.getRelType() + "").trim(), (node.getRcol() + "").trim());
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

    public void outAFilter(AFilter node)
    {
    	//curReactor.getCurRow().addFilter((node.getLcol() + "").trim(), (node.getComparator() + "").trim(), (node.getRcol() + "").trim());
        defaultOut(node);
        deInitReactor();
    }

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
	    	Object output = curReactor.Out();
	    	
	    	if(output instanceof IReactor)
	    		curReactor = (IReactor)output;
    	}
    	// else is the output
    	
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
    	IReactor reactor = new SampleReactor();
    	reactor.setPKSL(reactorId, nodeString);
    	return reactor;
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
	    		String space = "";
	    		for(int i = 0; i < spacecount; i++) {
	    			space += " ";
	    		}
		    	LOGGER.info(space+"*********************************************");
		    	LOGGER.info(space+message);
		    	StackTraceElement[] ste = Thread.currentThread().getStackTrace();
		    	if(ste.length > 2)
		    		LOGGER.info(space+"CURRENT METHOD: "+ste[2].getMethodName());
		    	else
		    		LOGGER.info(space+"CURRENT METHOD: UNKNOWN");
		    	LOGGER.info(space+"CURRENT NODE VALUE: "+node.toString());
		    	//print curReactor name
		    	if(curReactor != null) {
		    		LOGGER.info(space+"CURRENT REACTOR: "+this.curReactor.getName());
		    		//print current curRow of curReactor
		    		LOGGER.info(space+"CURRENT REACTOR'S GENROWSTRUCT: "+this.curReactor.getCurRow());
		    		LOGGER.info(space+"CURRENT REACTOR'S NOUNSTORE: \n"+curReactor.getNounStore().getDataString());
		    	} else {
		    		LOGGER.info(space+"CURRENT REACTOR: null");
		    	}
		    	LOGGER.info(space+"*********************************************\n");
    		}
    	}
    }
}