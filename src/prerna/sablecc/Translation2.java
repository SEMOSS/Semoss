package prerna.sablecc;

import java.util.Hashtable;
import java.util.Vector;

import prerna.algorithm.api.IAction;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.impl.ImportAction;
import prerna.ds.TinkerFrame;
import prerna.engine.api.IScriptReactor;
import prerna.sablecc.analysis.DepthFirstAdapter;
import prerna.sablecc.node.AAddColumn;
import prerna.sablecc.node.AApiBlock;
import prerna.sablecc.node.AColCsv;
import prerna.sablecc.node.AColDef;
import prerna.sablecc.node.AColWhere;
import prerna.sablecc.node.ACsvRow;
import prerna.sablecc.node.ADecimal;
import prerna.sablecc.node.ADivFactor;
import prerna.sablecc.node.AEExprExpr;
import prerna.sablecc.node.AExprGroup;
import prerna.sablecc.node.AExprRow;
import prerna.sablecc.node.AExprScript;
import prerna.sablecc.node.AExprTerm;
import prerna.sablecc.node.AImportColumn;
import prerna.sablecc.node.AImportData;
import prerna.sablecc.node.AMathFun;
import prerna.sablecc.node.AMathFunExpr;
import prerna.sablecc.node.AMinusExpr;
import prerna.sablecc.node.AModFactor;
import prerna.sablecc.node.AMultFactor;
import prerna.sablecc.node.ANumWordOrNum;
import prerna.sablecc.node.ANumberTerm;
import prerna.sablecc.node.APlusExpr;
import prerna.sablecc.node.ARelationDef;
import prerna.sablecc.node.ASetColumn;
import prerna.sablecc.node.ATermFactor;
import prerna.sablecc.node.AVarop;
import prerna.sablecc.node.AWhereClause;
import prerna.sablecc.node.AWord;
import prerna.sablecc.node.TNumber;

public class Translation2 extends DepthFirstAdapter {

	
	// this is the third version of this shit I am building
	// I need some way of having logical points for me to know when to start another reactor
	// for instance I could have an expr term within a math function which itself could be within another expr term
	// the question then is do I have 2 expr terms etc. 
	// so I start the first expr term
	// I start assimilating
	// get to a point where I start a new one
	// my vector tells me that
	// the init and deinit should take care of it I bet ?
	// how do I know when I am done ?
	// it has to be invoked at the last level
	Hashtable <String, Object> reactorHash = null;
	IScriptReactor curReactor = null;
	
	// reactor vector
	Vector <Hashtable<String, Object>> reactorHashHistory = new Vector <Hashtable<String, Object>>(); 
	
	Vector <Hashtable <String, Object>> reactorStack = new Vector<Hashtable <String, Object>>();
	
	// set of reactors
	// which serves 2 purposes 
	// a. Where to initiate
	// b. What is the name of the reactor
	
	Hashtable <String, String> reactorNames = new Hashtable<String, String>();
	
	ITableDataFrame frame = null;
	
	public Translation2()
	{
		// now get the data from tinker
		frame = new TinkerFrame();
		((TinkerFrame)frame).tryCustomGraph();
		fillReactors();
	}
	
	public Translation2(ITableDataFrame frame)
	{
		// now get the data from tinker
		this.frame = frame;
		fillReactors();

	}
	
	private void fillReactors()
	{
		reactorNames.put(TokenEnum.EXPR_TERM, "prerna.sablecc.ExprReactor");
		reactorNames.put(TokenEnum.EXPR_SCRIPT, "prerna.sablecc.ExprReactor");
		reactorNames.put(TokenEnum.MATH_FUN, "prerna.sablecc.MathReactor");
		reactorNames.put(TokenEnum.COL_CSV, "prerna.sablecc.ColCsvReactor"); // it almost feels like I need a way to tell when to do this and when not but let me see
		reactorNames.put(TokenEnum.ROW_CSV, "prerna.sablecc.RowCsvReactor");
		reactorNames.put(TokenEnum.API, "prerna.sablecc.ApiReactor");
		reactorNames.put(TokenEnum.WHERE, "prerna.sablecc.ColWhereReactor");
		reactorNames.put(TokenEnum.REL_DEF, "prerna.sablecc.RelReactor");
		reactorNames.put(TokenEnum.COL_ADD, "prerna.sablecc.ColAddReactor");
		reactorNames.put(TokenEnum.IMPORT_DATA, "prerna.sablecc.ImportDataReactor");
	}
	

	

	public void inAApiBlock(AApiBlock node) {

		if(reactorNames.containsKey(TokenEnum.API))
		{
			// simplify baby simplify baby simplify
			initReactor(TokenEnum.API);
			String nodeStr = node + "";
			curReactor.put(TokenEnum.API, nodeStr.trim());
			curReactor.put("ENGINE", node.getEngineName() + "");
			curReactor.put("INSIGHT", node.getInsight() + "");

		}		

	}
	
	public void outAApiBlock(AApiBlock node) 
	{
		String nodeStr = node + "";
		nodeStr = nodeStr.trim();
		
		IScriptReactor thisReactor = curReactor;
		
		Hashtable <String, Object> thisReactorHash = deinitReactor(TokenEnum.API, nodeStr, TokenEnum.API); // I need to make this into a string
		// I also need the various columns to join
		// infact I need a way to say what to pass
		// this should sit on the parent and not the child
		// the curReactor is the API
		if(curReactor != null)
		{
			String [] values2Sync = curReactor.getValues2Sync(TokenEnum.API);
			synchronizeValues(TokenEnum.API, values2Sync, thisReactor);
		}
	}

	private void synchronizeValues(String input, String[] values2Sync,
			IScriptReactor thisReactor) {
		// TODO Auto-generated method stub
		for(int valIndex = 0;valIndex < values2Sync.length;valIndex++)
		{
			Object value = thisReactor.getValue(values2Sync[valIndex]);
			if(value != null)
			{
				curReactor.put(input + "_" + values2Sync[valIndex], value);
			}
		}		
	}

	public void caseTNumber(TNumber node) {// When we see a number, we print it.
		//System.out.print(node);

	}

	public void inAExprScript(AExprScript node) {
		//System.out.println("In a script expr");
		// defaultIn(node);
		// everytime I need to open itW
		if(reactorNames.containsKey(TokenEnum.EXPR_SCRIPT))
		{
			// simplify baby simplify baby simplify
			initReactor(TokenEnum.EXPR_SCRIPT);
			String nodeStr = node.getExpr() + "";
			curReactor.put(TokenEnum.EXPR_TERM, nodeStr.trim());
		}		
	}

	public void outAExprScript(AExprScript node) {
		String nodeStr = node.getExpr() + "";
		nodeStr = nodeStr.trim();
		Hashtable <String, Object> thisReactorHash = deinitReactor(TokenEnum.EXPR_SCRIPT, nodeStr, (node + "").trim());
	}

	public void outAPlusExpr(APlusExpr node) {// out of alternative {plus} in
												// Expr, we print the plus.
		//System.out.print("Plus expression..  " + node.getPlus());
		String leftKeyName = node.getLeft() + "";
		String rightKeyName = node.getRight() + "";

		Object leftObj = curReactor.getValue(leftKeyName.trim());
		Object rightObj = curReactor.getValue(rightKeyName.trim());
		//System.out.println(node.getLeft() + " [][] " + node.getRight());
		Object result = null;
		if (rightObj instanceof Double && leftObj instanceof Double)
		{
			result = (Double)(leftObj)
					+ (Double)(rightObj);
			// remove the left and right key
			curReactor.addReplacer((node + "").trim(), result);
			curReactor.removeReplacer(leftKeyName.trim());
			curReactor.removeReplacer(rightKeyName.trim());
		}
	}
	


	public void inAMinusExpr(AMinusExpr node) {
		//System.out.println("MINUS... " + node);
	}

	public void outAMinusExpr(AMinusExpr node) {// out of alternative {plus} in
												// Expr, we print the plus.

		//System.out.println("MINUS again in out.. ");
		String leftKeyName = node.getLeft() + "";
		String rightKeyName = node.getRight() + "";

		Object leftObj = curReactor.getValue(leftKeyName.trim());
		Object rightObj = curReactor.getValue(rightKeyName.trim());
		//System.out.println(node.getLeft() + " [][] " + node.getRight());
		Object result = null;
		if (rightObj instanceof Double && leftObj instanceof Double)
		{
			result = (Double)(leftObj)
					- (Double)(rightObj);
			// remove the left and right key
			curReactor.addReplacer((node + "").trim(), result);
			curReactor.removeReplacer(leftKeyName);
			curReactor.removeReplacer(rightKeyName);
		}
	}

	public void inATermFactor(ATermFactor node) {
		// defaultIn(node);
		//System.out.println("In a term Factor");
		System.out.println(">>> " +node.getTerm());

	}

	public void inAExprTerm(AExprTerm node) {
		if(reactorNames.containsKey(TokenEnum.EXPR_TERM))
		{
				// get the appropriate reactor
				
				initReactor(TokenEnum.EXPR_TERM);
				// get the name of reactor
				String nodeStr = node.getExpr() + "";
				curReactor.put("G", frame);
				curReactor.put(TokenEnum.EXPR_TERM, nodeStr.trim());
		}	
		// I need to find if there is a parent to this
		// which is also an expr term
		// I need some way to see if the parent is the same
		// then I should just assimilate
		// instead of trying to redo it
		// I need some way to figure out
		// && whatICallThisInMyWorld.containsKey("PARENT") && whatICallThisInMyWorld.get("PARENT").equalsIgnoreCase("EXPR_TERM")					
		// this is the one that has paranthesis
	}

	public void outAExprTerm(AExprTerm node) {
		//System.out.println("Successful in retrieving the data for expr term ?+ node.getExpr() + 	+ dataKeeper.containsKey((node.getExpr() + "").trim()));
		// get the value of it
		// I am not goiong to do anything here
		System.out.println("Printing expression.. " + node.getExpr());
		Hashtable <String, Object> thisReactorHash = deinitReactor(TokenEnum.EXPR_TERM, node.getExpr().toString().trim(),  node.toString().trim());

		if (thisReactorHash.get(TokenEnum.EXPR_TERM) instanceof ExprReactor) {
			ExprReactor thisReactor = (ExprReactor)thisReactorHash.get(TokenEnum.EXPR_TERM);
			String expr = (String)thisReactor.getValue(TokenEnum.EXPR_TERM);
			curReactor.put("COL_DEF", thisReactor.getValue(TokenEnum.COL_DEF));
			curReactor.addReplacer(expr, thisReactor.getValue(expr));
			this.frame.setTempExpressionResult(thisReactor.getValue(expr));
		}
	}
	
    public void inAEExprExpr(AEExprExpr node)
    {
        System.out.println("In The EXPRESSION .. " + node);
    }

	
	private void remasterCol(String curNode, String parNode, Hashtable <String, Vector<String>> funCol)
	{
		if(funCol.containsKey(curNode))
		{
			Vector <String> curVector = funCol.get(curNode);
			if(funCol.containsKey(parNode))
				curVector.addAll(funCol.get(parNode));

			funCol.put(parNode, curVector);
			funCol.remove(curNode);
		}
	}

	public void outAMultFactor(AMultFactor node) {// out of alternative {mult}
													// in Factor, we print the
													// mult.
													// //System.out.print(node.getMult());
													// //System.out.print(node.getPlus());
		String leftKeyName = node.getLeft() + "";
		String rightKeyName = node.getRight() + "";

		Object leftObj = curReactor.getValue(leftKeyName.trim());
		Object rightObj = curReactor.getValue(rightKeyName.trim());
		//System.out.println(node.getLeft() + " [][] " + node.getRight());
		Object result = null;
		if (rightObj instanceof Double && leftObj instanceof Double)
		{
			result = (Double)(leftObj)
					* (Double)(rightObj);
			// remove the left and right key
			curReactor.addReplacer((node + "").trim(), result);
			curReactor.removeReplacer(leftKeyName);
			curReactor.removeReplacer(rightKeyName);
		}
	}

	public void outADivFactor(ADivFactor node) {// out of alternative {div} in
												// Factor, we print the div.
		String leftKeyName = node.getLeft() + "";
		String rightKeyName = node.getRight() + "";

		Object leftObj = curReactor.getValue(leftKeyName.trim());
		Object rightObj = curReactor.getValue(rightKeyName.trim());
		//System.out.println(node.getLeft() + " [][] " + node.getRight());
		Object result = null;
		if (rightObj instanceof Double && leftObj instanceof Double)
		{
			result = (Double)(leftObj)
					/ (Double)(rightObj);
			// remove the left and right key
			curReactor.addReplacer((node + "").trim(), result);
			curReactor.removeReplacer(leftKeyName);
			curReactor.removeReplacer(rightKeyName);
		}
	}

	public void inAColDef(AColDef node) {
		String colName = node.getColname().toString().trim();
		// adding to the reactor
		curReactor.set("COL_DEF", colName);
		curReactor.addReplacer((node + "").trim(), colName);
	}

	public void outAModFactor(AModFactor node) {// out of alternative {mod} in
												// Factor, we print the mod.
		//System.out.print(node.getMod());
	}

	public void outAAddColumn(AAddColumn node) {
		
		/*Vector <String> columns = (Vector <String>)myStore.get("COLUMNS_USED");
		String[] columnArray = convertVectorToArray(columns);
		String newCol = ((Vector)myStore.get("NEWCOL")).get(0).toString();
		
		Iterator it = (Iterator)((Vector)myStore.get("API")).get(0);

		AddColumnOperator addCol = new AddColumnOperator(frame, it, newCol, columnArray);
		addCol.apply();
		
		
		// if everything is right.. this would print me tinkerIterator
		System.out.println("Iterator to mess with " + myStore.get("API"));
		System.out.println("The new column name.. prints as a vector " + myStore.get("NEWCOL"));
		*/
		
		String nodeStr = node.getExpr() + "";
		nodeStr = nodeStr.trim();
		curReactor.put(TokenEnum.EXPR_TERM, nodeStr);
		Hashtable <String, Object> thisReactorHash = deinitReactor(TokenEnum.COL_ADD, nodeStr, (node + "").trim());

	}

	public void outAExprGroup(AExprGroup node) {
		//System.out.println("Node in expr group" + node);
		//System.out.println(dataKeeper);
		//System.out.println("Data keeper has expr.. [" + node.getExpr() + "]"+ dataKeeper.containsKey((node.getExpr() + "").trim()));
	}

	public void inAAddColumn(AAddColumn node) {
		
		if(reactorNames.containsKey(TokenEnum.COL_ADD))
		{
			// simplify baby simplify baby simplify
			initReactor(TokenEnum.COL_ADD);
			String nodeStr = node + "";
			curReactor.put(TokenEnum.COL_ADD, nodeStr.trim());
		}		
	}

    public void inAImportData(AImportData node){
		if(reactorNames.containsKey(TokenEnum.IMPORT_DATA))
		{
			// simplify baby simplify baby simplify
			initReactor(TokenEnum.IMPORT_DATA);
			String nodeStr = node + "";
			curReactor.put(TokenEnum.IMPORT_DATA, nodeStr.trim());
		}		
    }
    
    public void outAImportData(AImportData node){
		String nodeStr = node.getApi() + "";
		nodeStr = nodeStr.trim();
		curReactor.put(TokenEnum.EXPR_TERM, nodeStr);
		Hashtable <String, Object> thisReactorHash = deinitReactor(TokenEnum.IMPORT_DATA, nodeStr, (node + "").trim());
    }

	public void outASetColumn(ASetColumn node) {
		//System.out.println("Set.. [" + (node.getExpr() + "").trim() + "]");
	}

	public void outAVarop(AVarop node) {
		String varName = getCol(node.getName() + "");
		String expr = getCol(node.getExpr() + "");
		//System.out.println("Variable declaration " + varName + " =  " + expr);
		// defaultOut(node);
	}

	public void inANumberTerm(ANumberTerm node) {
		//System.out.println("Number term.. >>> " + node.getDecimal());
		String number = node.getDecimal() + "";
		//dataKeeper.put(number.trim(), Double.parseDouble(number));
	}
	
    public void inADecimal(ADecimal node)
    {
		//System.out.println("DECIMAL VALUE.. >>> " + node);
		String fraction = node.getFraction() +"";
		String number = (node.getWhole() + "").trim();
		if(node.getFraction() != null)
			number = number + "." + (node.getFraction() + "").trim();
		
    	// I also need to add this into mystore - need a cleaner way to do this
    	//curReactor.set( node.toString().trim(), Double.parseDouble(number));
    	curReactor.addReplacer(node.toString().trim(), Double.parseDouble(number));
	}
    
    public void inAWord(AWord node) {
    	
    }
    

    public void outAWord(AWord node)
    {
        //System.out.println("In a word.. " + node); // need to find a way to clean up information puts a space after the quote
        curReactor.set(TokenEnum.WORD_OR_NUM, (node + "").trim());
        //thisRow.addElement((node + "").trim());        
    }

    public void outANumWordOrNum(ANumWordOrNum node)
    {
    }

	private String getCol(String colName) {
		colName = colName.substring(colName.indexOf(":") + 1);
		colName = colName.trim();

		return colName;
	}
	
    public void inAExprRow(AExprRow node)
    {
    }

    public void outAExprRow(AExprRow node)
    {
    }


	public void inAMathFunExpr(AMathFunExpr node) {
		//System.out.println("Math Fun expression is ..  " + node);
		// currentMathFunction = node.getMathFun() + "";
	}

	public void inAMathFun(AMathFun node) {
		
		if(reactorNames.containsKey(TokenEnum.MATH_FUN))
		{
				// get the appropriate reactor
				String procedureName = (node.getId() + "").trim();
				String mathFunName = "MATH_FUN";
				
				initReactor(mathFunName);
				// get the name of reactor
				String nodeStr = node.getExpr() + "";
				curReactor.put("G", frame);
				curReactor.put(TokenEnum.MATH_FUN, nodeStr.trim());
				curReactor.put(TokenEnum.PROC_NAME, procedureName);
		}	
	}

	public void outAMathFun(AMathFun node) {
		// function would usually get
		/*
		 * a. Expression - what to compute b. List of columns to pull c.
		 * Iterator which has all of these different columns pulled d.
		 * getValue() method which will actually return an object e.
		 */
		// need to accomodate for the array that is there
		// I need to do something to find if I am at the right level
		// how no Shit works
		// I need to set some stuff
		// like the tinker frame etc.. 
		String procedureName = "MATH_FUN";
		String expr = node.getExpr().toString().trim();
		Hashtable <String, Object> thisReactorHash = deinitReactor(procedureName, expr,  (node + "").trim());
		if (thisReactorHash.get(procedureName) instanceof MathReactor) {
			MathReactor thisReactor = (MathReactor)thisReactorHash.get(procedureName);
			curReactor.put("COL_DEF", thisReactor.getValue(TokenEnum.COL_DEF));
			curReactor.addReplacer(expr, thisReactor.getValue(expr));
			this.frame.setTempExpressionResult(thisReactor.getValue(expr));
		}
	}

	public void outAMathFunExpr(AMathFunExpr node) {
		//System.out.println("OUT ... Math Fun expression is ..  ");
		// do some processing for sum here
//		System.out.println("Math fun..  " + node.getMathFun());

		// I need to see if I can replace anything here
		// I need to see if there are temp strings I can replace here

	}
	
    public void inAImportColumn(AImportColumn node)
    {
        //System.out.println("In the import col operation");
        //System.out.println("DATA ..... " + node.getData());
        //System.out.println("DATA ..... " + node.getCols());
        
        IAction thisAction = new ImportAction();
        thisAction.set("TF", frame); // will need to change to constants afterwards
        
        //currentListener.add(thisAction);
    }
    
    public void outAImportColumn(AImportColumn node)
    {
    	//System.out.println("Import Column Done.. ");
    }
        
    public void inAWhereClause(AWhereClause node)
    {
    	//currentMathFunction.addElement((node + "").trim());
    }

    public void outAWhereClause(AWhereClause node)
    {
    	//currentMathFunction.removeElement((node + "").trim());
    }
    
    public void inAColWhere(AColWhere node)
    {
		if(reactorNames.containsKey(TokenEnum.WHERE))
		{
			// simplify baby simplify baby simplify
			initReactor(TokenEnum.WHERE);
			String nodeStr = node + "";
			curReactor.put(TokenEnum.WHERE, nodeStr.trim());
			curReactor.put(TokenEnum.COMPARATOR, (node.getComparator()+"").trim());
		}		
    }

    public void outAColWhere(AColWhere node)
    {
        //System.out.println("COL WHERE DATAKEEPER " + myStore);
        // I need to do some kind of action and pop out the last one on everything
        // Action is here
		String nodeStr = node + "";
		nodeStr = nodeStr.trim();
		Hashtable <String, Object> thisReactorHash = deinitReactor(TokenEnum.WHERE, nodeStr, TokenEnum.FILTER, false);
    }


    public void inARelationDef(ARelationDef node)
    {
		if(reactorNames.containsKey(TokenEnum.REL_DEF))
		{
			// simplify baby simplify baby simplify
			initReactor(TokenEnum.REL_DEF);
			String nodeStr = node + "";
			curReactor.put(TokenEnum.REL_DEF, nodeStr.trim());
			curReactor.put(TokenEnum.REL_TYPE, (node.getRelType()+"").trim());
		}		

    }

    public void outARelationDef(ARelationDef node)
    {
		String nodeStr = node + "";
		nodeStr = nodeStr.trim();
		Hashtable <String, Object> thisReactorHash = deinitReactor(TokenEnum.REL_DEF, nodeStr, TokenEnum.JOINS, false);
    }
    
    public void inAColCsv(AColCsv node)
    {
    	System.out.println("Directly lands into col csv " + node);
    	//System.out.println("COL CSV is " + node);
    	//currentMathFunction.addElement((node + "").trim());
		if(reactorNames.containsKey(TokenEnum.COL_CSV))
		{
			// simplify baby simplify baby simplify
			initReactor(TokenEnum.COL_CSV);
			String nodeStr = node + "";
			curReactor.put(TokenEnum.COL_CSV, nodeStr.trim());
		}
    }


    public void outAColCsv(AColCsv node)
    {
    	String thisNode = (node + "").trim();
    	//System.out.println("COL CSV is " + node);
		Hashtable <String, Object> thisReactorHash = deinitReactor(TokenEnum.COL_CSV, thisNode, TokenEnum.COL_CSV);
		
    }

    public void inACsvRow(ACsvRow node)
    {
        //System.out.println("The Row is " + node);
        // need to tell it I am assimilating a vector here
       // thisRow = new Vector<Object>();
    	System.out.println("Directly lands into col csv " + node);
    	//System.out.println("COL CSV is " + node);
    	//currentMathFunction.addElement((node + "").trim());
		if(reactorNames.containsKey(TokenEnum.ROW_CSV))
		{
			// simplify baby simplify baby simplify
			initReactor(TokenEnum.ROW_CSV);
			String nodeStr = node + "";
			curReactor.put(TokenEnum.ROW_CSV, nodeStr.trim());
		}
    }
    
    
    public void outACsvRow(ACsvRow node)
    {
    	// I need to do an action here
    	// get the action
    	// call to say this has happened and then reset it to null;
    	//System.out.println("This row so far..  " + thisRow);
    	String thisNode = (node + "").trim();
    	//System.out.println("COL CSV is " + node);
		deinitReactor(TokenEnum.ROW_CSV, thisNode, TokenEnum.ROW_CSV);
    }
		
	
	public void initReactor(String myName)
	{
		String parentName = null;
		if(reactorHash != null)
			// I am not sure I need to add element here
			//reactorStack.addElement(reactorHash);
			// I need 2 things in here
			// I need the name of a parent i.e. what is my name and my parent name
			// actually I just need my name
			parentName = (String)reactorHash.get("SELF");
		reactorHash = new Hashtable<String, Object>();
		if(parentName != null)
			reactorHash.put("PARENT_NAME", parentName);
		reactorHash.put("SELF", myName);
		reactorStack.addElement(reactorHash);
		
		// I should alsoo possibly initialize the reactor here
		try {
			String reactorName = reactorNames.get(myName);
			curReactor = (IScriptReactor)Class.forName(reactorName).newInstance();
			curReactor.put(TokenEnum.G, frame);
			// this is how I can get access to the parent when that happens
			reactorHash.put(myName, curReactor);
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}			

	}
	
	public Hashtable <String, Object> deinitReactor(String myName, String input, String output, boolean put)
	{
		//reactorHashHistory.add(reactorHash);
		
		Hashtable <String, Object> thisReactorHash = reactorStack.lastElement();
		reactorStack.remove(thisReactorHash);
		IScriptReactor thisReactor = (IScriptReactor)thisReactorHash.get(myName);
		// this is still one level up
		thisReactor.process();
		Object value = 	thisReactor.getValue(input);		
		System.out.println("Value is .. " + value);		

		if(reactorStack.size() > 0)
		{
			reactorHash = reactorStack.lastElement();
			// also set the cur reactor
			String parent = (String)thisReactorHash.get("PARENT_NAME");
			
			// if the parent is not null
			if(parent != null && reactorHash.containsKey(parent))
			{
				// I need to make some decisions here
				// decisions decisions decisions
				curReactor = (IScriptReactor)reactorHash.get(parent);
				if(put)
					curReactor.put(output, value);
				else
					curReactor.set(output, value);
			}
		}
		return thisReactorHash;
	}		
	
	public Hashtable <String, Object> deinitReactor(String myName, String input, String output)
	{
		return deinitReactor(myName, input, output, true);

	}

}