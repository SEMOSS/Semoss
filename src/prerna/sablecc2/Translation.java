package prerna.sablecc2;

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
import prerna.sablecc2.reactor.PKSLPlanner;
import prerna.sablecc2.reactor.SampleReactor;

public class Translation extends DepthFirstAdapter {

	IReactor curReactor = null;
	IReactor prevReactor = null;
	
	TypeOfOperation operationType = TypeOfOperation.COMPOSITION;
	
	
	public enum TypeOfOperation {PIPELINE, COMPOSITION};
	
	// there really will be 2 versions of this
	// one for overall and one as the user does it
	// for now.. this is just how the user does it
	PKSLPlanner planner = new PKSLPlanner();
	String lastOperation = null;
	
	
	
/********************** First is the main level operation, script chain or other script operations ******************/
	
    public void inAScriptchain(AScriptchain node)
    {
    	System.out.println(">>>>>>");
        System.out.println("Called script chain");
        System.out.println("First operation is " + node.getOtherscript());
        if(node.getOtherscript() != null && node.getOtherscript().size() > 0)
        	operationType = TypeOfOperation.PIPELINE; 
    }
    
    public void outAScriptchain(AScriptchain node)
    {
    	// this is really where the overall execution should happen
    	System.out.println("<<<<<<<" + node);
    	planner.runMyPlan(lastOperation);
    }
    
    public void inAOtherscript(AOtherscript node)
    {
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
        System.out.println("In a script frameop");
        // once I am in here I am again in the realm of composition
        this.operationType = TypeOfOperation.COMPOSITION;
    }

    public void outAFrameopScript(AFrameopScript node)
    {
        System.out.println("Out a script frameop");
        this.operationType = TypeOfOperation.PIPELINE;
    }

    public void inAOpScript(AOpScript node)
    {
        System.out.println("In a operational formula script");
        this.operationType = TypeOfOperation.COMPOSITION;
    }

    public void outAOpScript(AOpScript node)
    {
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
        System.out.println("Starting a frame operation");
        
        // create a sample reactor
        IReactor frameReactor = new SampleReactor();
        frameReactor.setPKSL(node.getId()+"", node+"");
        initReactor(frameReactor);
     }

    public void outAFrameop(AFrameop node)
    {
        
        // I realize I am not doing anything with the output.. this will come next
    	
        curReactor.Out();
        if(curReactor.getParentReactor() != null)
        	curReactor = curReactor.getParentReactor();
        
        this.lastOperation = node + "";
        System.out.println("OUT of Frame OP");
    }
    // setting the as value on a frame
    public void inAAsop(AAsop node)
    {
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


    
    // second is a simpler operation
    // something(a,b,c,d) <-- note the absence of square brackets, really not needed
    public void inAOperationFormula(AOperationFormula node)
    {
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
    }

    public void outAOperationFormula(AOperationFormula node)
    {
        deInitReactor();
        System.out.println("Ending a formula operation");
        
        if(curReactor.getParentReactor() == null)
        {
        	// this is the last stop
        	
        }
        this.lastOperation = node + "";
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
        defaultIn(node);
    }

    public void inALiteralColDef(ALiteralColDef node)
    {
    	String thisLiteral = node.getLiteral()+"";
    	thisLiteral = thisLiteral.replace("'","");
    	thisLiteral = thisLiteral.replace("\"","");
    	thisLiteral = thisLiteral.trim();
        curReactor.getCurRow().addLiteral(thisLiteral);
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
        System.out.println("In a Frame op col DEF a.k.a already within a script" + node);
        // so this is where things REALLY get interesting
        // I have to work through here to figure out what to do with it
        // the only difference here is am I adding this to genrowstruct
        // or am I adding it as a full reactor
    }

    public void outAFrameopColDef(AFrameopColDef node)
    {
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
        {
	        IReactor opReactor = new ExprReactor();
	        opReactor.setName("EXPR");
	        opReactor.setPKSL("EXPR",node.getExpr()+"");
	        initReactor(opReactor);
        }
        // else ignore.. this has been taken care of kind of
    }

    public void outAExprColDef(AExprColDef node)
    {
        defaultOut(node);
        // TODO need to do the operation of assimilating here
        deInitReactor();
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
        System.out.println("IN Atomic.. " + "expr" + node);
    }

    public void outAEExprExpr(AEExprExpr node)
    {
        System.out.println("OUT Atomic.. " + "expr" + node);
    }

    
    public void inADecimal(ADecimal node)
    {
        System.out.println("Decimal is" + node.getWhole());
        String fraction = "0";
        if(node.getFraction() != null) fraction = (node.getFraction()+"").trim();
        String value = (node.getWhole()+"").trim() +"." + fraction;
        Double adouble = Double.parseDouble(value);
        curReactor.getCurRow().addDecimal(adouble);
    }

    public void outADecimal(ADecimal node)
    {
        defaultOut(node);
    }
    
    public void inAColTerm(AColTerm node)
    {
        String column = (node.getCol()+"").trim();
        curReactor.getCurRow().addColumn(column);
    }

    public void outAColTerm(AColTerm node)
    {
    }
    
    public void inADotcol(ADotcol node)
    {
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
    }

    public void outATermExpr(ATermExpr node)
    {
        defaultOut(node);
    }
    
    public void inAProp(AProp node)
    {
        curReactor.setProp((node.getId()+"").trim(), node.getNumberOrLiteral()+"");
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
        if(curReactor != null)
        {
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
    
   
}