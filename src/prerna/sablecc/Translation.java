package prerna.sablecc;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import prerna.algorithm.api.IAction;
import prerna.algorithm.impl.AddColumnOperator;
import prerna.algorithm.impl.ImportAction;
import prerna.ds.ExpressionIterator;
import prerna.ds.ExpressionReducer;
import prerna.ds.QueryStruct;
import prerna.ds.TinkerFrame;
import prerna.rdf.query.builder.SQLInterpreter;
import prerna.sablecc.analysis.DepthFirstAdapter;
import prerna.sablecc.node.AAddColumn;
import prerna.sablecc.node.AApiBlock;
import prerna.sablecc.node.AColCsv;
import prerna.sablecc.node.AColDef;
import prerna.sablecc.node.AColWhere;
import prerna.sablecc.node.ACsvRow;
import prerna.sablecc.node.ADecimal;
import prerna.sablecc.node.ADivFactor;
import prerna.sablecc.node.AExprGroup;
import prerna.sablecc.node.AExprRow;
import prerna.sablecc.node.AExprScript;
import prerna.sablecc.node.AExprTerm;
import prerna.sablecc.node.AGroupBy;
import prerna.sablecc.node.AImportColumn;
import prerna.sablecc.node.AMathFun;
import prerna.sablecc.node.AMathFunExpr;
import prerna.sablecc.node.AMinusExpr;
import prerna.sablecc.node.AModFactor;
import prerna.sablecc.node.AMultFactor;
import prerna.sablecc.node.ANumWordOrNum;
import prerna.sablecc.node.ANumberTerm;
import prerna.sablecc.node.APlusExpr;
import prerna.sablecc.node.ARelationClause;
import prerna.sablecc.node.ARelationDef;
import prerna.sablecc.node.ASelector;
import prerna.sablecc.node.ASetColumn;
import prerna.sablecc.node.ATermFactor;
import prerna.sablecc.node.AVarop;
import prerna.sablecc.node.AWhereClause;
import prerna.sablecc.node.AWord;
import prerna.sablecc.node.TNumber;

public class Translation extends DepthFirstAdapter {

	Hashtable<String, Object> dataKeeper = new Hashtable<String, Object>();
	Hashtable<String, Vector<String>> funCol = new Hashtable<String, Vector<String>>();// all
																						// the
																						// different
																						// columns
																						// needed
																						// by
																						// a
																						// given
																						// function

	Hashtable<String, Vector<String>> tempStrings = new Hashtable<String, Vector<String>>(); // the
																								// strings
																								// that
																								// will
																								// be
																								// utilzed
																								// to
																								// replace
																								// everything
	Hashtable <String, Vector<String>> arrayValues = new Hashtable<String, Vector<String>>(); /* All the different group by are kept */

	Vector<String> currentMathFunction = new Vector<String>();
	Vector <IAction> currentListener = new Vector<IAction>();
	
	Vector <Object> thisRow =null;
	Vector <String> colCSV = null;
	
	// so I need something that registers the events
	// COL_CSV_1 = Selector
	// something like that
	Hashtable <String, String> whatICallThisInMyWorld = new Hashtable <String, String>(); 
	
	Vector <Hashtable <String, String>> whatICallThisInMyWorldHistory = new Vector<Hashtable <String, String>>();
	
	// how many times I have come to this type of node
	// COL_CSV - number of times
	Hashtable <String, Integer> howManyTimesHaveIVisited = new Hashtable<String, Integer>(); 
	Vector <Hashtable <String, Integer>> howManyTimesHaveIVisitedHistory = new Vector<Hashtable <String, Integer>>();
	
	// where all values are kept
	Hashtable <String, Object> myStore = new Hashtable<String, Object>();
	Vector <Hashtable <String, Object>> myStoreHistory = new Vector<Hashtable <String, Object>>();
	
	TinkerFrame frame = null;
	
	public Translation()
	{
		// now get the data from tinker
		frame = new TinkerFrame();
		frame.tryCustomGraph();

	}
	
	public Translation(TinkerFrame frame)
	{
		// now get the data from tinker
		this.frame = frame;
	}
	

	public void inAApiBlock(AApiBlock node) {

		if(whatICallThisInMyWorld != null &&  whatICallThisInMyWorld.containsKey("API_BLOCK"))
		{
			//System.out.println("API...  " + node); // defaultIn(node);
			//System.out.println(node.getEngineName() + " <> " + node.getInsight());
	
			// I am tracking for this guy foor now
			//currentMathFunction.addElement((node + "").trim());
			
			whatICallThisInMyWorld = new Hashtable<String, String>();
			
			whatICallThisInMyWorld.put("COL_CSV", "SELECTOR");
			whatICallThisInMyWorld.put("COL_CSV_1", "SELECTOR");
			whatICallThisInMyWorld.put("COL_WHERE", "FILTERS");
			whatICallThisInMyWorld.put("RELATIONS", "JOINS");
			whatICallThisInMyWorld.put("RELATIONS_1", "JOINS");
			whatICallThisInMyWorld.put("API_BLOCK", "API_BLOCK");
	
			reinit();
		}
	}
	
	public void outAApiBlock(AApiBlock node) 
	{
		if(whatICallThisInMyWorld != null &&  whatICallThisInMyWorld.containsKey("API_BLOCK"))
		{
			//System.out.println("Will pull the data on this one and then make the calls to add to other things");
			myStore.put("ENGINE", node.getEngineName() + "");
			myStore.put("INSIGHT", node.getInsight() + "");
			
			Hashtable thisStore = myStore;
			deInit();
			
			saveData("API_BLOCK", thisStore);
			
			Vector <Hashtable> filtersToBeElaborated = new Vector<Hashtable>();
			Vector <String> tinkerSelectors = new Vector<String>();
			
			String engine = (String)thisStore.get("ENGINE");
			String insight = (String)thisStore.get("INSIGHT");
			
			QueryStruct qs = new QueryStruct();
			if(thisStore.containsKey("SELECTOR") && ((Vector)thisStore.get("SELECTOR")).size() > 0)
			{
				Vector <String> selectors = (Vector <String>)thisStore.get("SELECTOR");
				for(int selectIndex = 0;selectIndex < selectors.size();selectIndex++)
				{
					String thisSelector = selectors.get(selectIndex);
					String concept = thisSelector.substring(0, thisSelector.indexOf("__"));
					String property = thisSelector.substring(thisSelector.indexOf("__")+2);
					qs.addSelector(concept, property);
				}
			}
			if(thisStore.containsKey("FILTERS") && ((Vector)thisStore.get("FILTERS")).size() > 0)
			{
				Vector filters = (Vector)thisStore.get("FILTERS");
				for(int filterIndex = 0;filterIndex < filters.size();filterIndex++)
				{
					Hashtable thisFilter = (Hashtable)filters.get(filterIndex);
					String fromCol = (String)thisFilter.get("FROM_COL");
					String toCol = null;
					Vector filterData = new Vector();
					if(thisFilter.containsKey("TO_COL"))
					{
						toCol = (String)thisFilter.get("TO_COL");
						filtersToBeElaborated.add(thisFilter);
						tinkerSelectors.add(toCol);
						// need to pull this from tinker frame and do the due
						// interestingly this could be join
					}
					else
					{
						// this is a vector do some processing here					
						filterData = (Vector)thisFilter.get("TO_DATA");
						String comparator = (String)thisFilter.get("COMPARATOR");
						String concept = fromCol.substring(0, fromCol.indexOf("__"));
						String property = fromCol.substring(fromCol.indexOf("__")+2);
						qs.addFilter(fromCol, comparator, filterData);
					}
				}
			}
			if(thisStore.containsKey("JOINS") && ((Vector)thisStore.get("JOINS")).size() > 0)
			{
				Vector joinVector = (Vector)thisStore.get("JOINS");
				
				for(int joinIndex = 0;joinIndex < joinVector.size();joinIndex++)
				{
					Hashtable thisJoin = (Hashtable)joinVector.get(joinIndex);
					
					String fromCol = (String)thisJoin.get("FROM_COL");
					String toCol = (String)thisJoin.get("TO_COL");
					
					String relation = (String)thisJoin.get("REL_TYPE");	
					qs.addRelation(fromCol, toCol, relation);
				}
				
			}
			
			// I need to run through here tot find which ones are things to be elaborated
			// and then add it back to the query struct
			
			//System.out.println(">>>Set everything on query struct.. now I need to pull the API and run it<<<");
			///qs.print();
			
			SQLInterpreter in = new SQLInterpreter(qs);
			in.composeQuery();
			
		}
	}

	public void caseTNumber(TNumber node) {// When we see a number, we print it.
		//System.out.print(node);

	}

	public void inAExprScript(AExprScript node) {
		//System.out.println("In a script expr");
		defaultIn(node);
	}

	public void outAExprScript(AExprScript node) {
		String nodeStr = node.getExpr() + "";
		nodeStr = nodeStr.trim();

		// this is the last portion of everything

		//System.out.println("out of scroipt expr [" + node + "]");
		//System.out.println(" Found it in data keeper ?"+ dataKeeper.get(nodeStr));
		if (!dataKeeper.containsKey((node + "").trim())) {
			dataKeeper.put((node + "").trim(), dataKeeper.get(nodeStr));
		}

	}

	public void outAPlusExpr(APlusExpr node) {// out of alternative {plus} in
												// Expr, we print the plus.
		//System.out.print("Plus expression..  " + node.getPlus());
		String leftKeyName = node.getLeft() + "";
		String rightKeyName = node.getRight() + "";

		Object leftObj = dataKeeper.get(leftKeyName.trim());
		Object rightObj = dataKeeper.get(rightKeyName.trim());
		//System.out.println(node.getLeft() + " [][] " + node.getRight());
		Object result = null;
		if (rightObj instanceof Double && leftObj instanceof Double)
			result = (Double)(leftObj)
					+ (Double)(rightObj);

		else {
			// one of these is an iterator
			// pick it up and do the magic
			// stupidity for now
			result = leftObj + "" + rightObj;
		}
		//System.out.println("result is add " + result);

		//System.out.println("node itself looks like.. APlus [" + node + "]");
		dataKeeper.put((node + "").trim(), result);
	}
	


	public void inAMinusExpr(AMinusExpr node) {
		//System.out.println("MINUS... " + node);
	}

	public void outAMinusExpr(AMinusExpr node) {// out of alternative {plus} in
												// Expr, we print the plus.

		//System.out.println("MINUS again in out.. ");
		String leftKeyName = node.getLeft() + "";
		String rightKeyName = node.getRight() + "";

		Object leftObj = dataKeeper.get(leftKeyName.trim());
		Object rightObj = dataKeeper.get(rightKeyName.trim());
		Object result = null;
		if (rightObj instanceof Double && leftObj instanceof Double)
			result = (Double)(leftObj)
					- (Double)(rightObj);

		else {
			// one of these is an iterator
			// pick it up and do the magic
			// stupidity for now
			result = leftObj + "" + rightObj;
		}

		//System.out.println("result is" + result);

		//System.out.println(node.getLeft() + " [][] " + node.getRight());

		//System.out.println("node itself looks like.. minus[" + node + "]");
		dataKeeper.put((node + "").trim(), result);
	}

	public void inATermFactor(ATermFactor node) {
		// defaultIn(node);
		//System.out.println("In a term Factor");
		//System.out.println(">>> " +node.getTerm());

	}

	public void inAExprTerm(AExprTerm node) {
		if(whatICallThisInMyWorld.containsKey("EXPR_TERM"))
		{
			whatICallThisInMyWorld = new Hashtable<String, String>();
			whatICallThisInMyWorld.put("EXPR_TERM", "EXPR_TERM");
			whatICallThisInMyWorld.put("COL_DEF", "COL_DEF");		
			whatICallThisInMyWorld.put("REPLACE", "REPLACE");
			
			reinit();
		}
		//System.out.println("Printing expr term PAR " + node.getExpr());
		//currentMathFunction.add((node + "").trim());

		// this is the one that has paranthesis
	}

	public void outAExprTerm(AExprTerm node) {
		//System.out.println("Successful in retrieving the data for expr term ?+ node.getExpr() + 	+ dataKeeper.containsKey((node.getExpr() + "").trim()));
		// get the value of it
		Object value = dataKeeper.get((node.getExpr() + "").trim());
		
		// remove it from the function
		currentMathFunction.remove((node + "").trim());

		if(value instanceof Double)
		{
			dataKeeper.put((node + "").trim(), value);
		} else  if(whatICallThisInMyWorld.containsKey("EXPR_TERM")) {
			// 2 possibilities either it is a straight up so no col def
			if(myStore.containsKey("COL_DEF"))
			{
				Vector allCols = (Vector) myStore.get("COL_DEF");
				// get the iterator and then set it for API
				// I will assume I create the iterator through tinker here
				// I will call it a string for now
				Iterator iterator = frame.getIterator(allCols);
				String expression = node.getExpr().toString();
				expression = getModExpression(expression);
				ExpressionIterator it = new ExpressionIterator(iterator, convertVectorToArray(allCols), expression);
				System.out.println("The expression was  " + node);
				System.out.println("The columns were..  " + allCols);
				
				deInit();
				
				saveData("EXPR_TERM", it);
				myStore.put("COLUMNS_USED", allCols); // TODO: is there a better way to keep track of columns used in expression?
				
			}
			
		}
        
		/*
		else
		{
			// i need to add these columns to the parent ?
			// I need to only do this if the current Math function > 1
			if(true) //currentMathFunction.size() > 0)
			{
				//System.out.println(".. I need to do something here.. ");
				//System.out.println(funCol);
				//System.out.println(tempStrings);
				remasterCol((node + "").trim(), currentMathFunction.get(currentMathFunction.size() -1), funCol);
				remasterCol((node + "").trim(), currentMathFunction.get(currentMathFunction.size() -1), tempStrings);
				// nothing much else to do here
				//value = null;
			}
			else
			{
				// baby this is the end in itself
				// put another way. this is the generic mapper !!
				// ok.. I am not sure if it will EVER come here
				
				// same routine as outmath fun
				Vector <String> columns = null;
				Iterator data = null;
				String exprName = (node + "").trim();
				if(funCol.containsKey(exprName))
				{
					columns = funCol.get(exprName);
					data = getData(columns);	
					exprName = getModExpression(exprName);
				}
				
				ExpressionMapper mapper = new ExpressionIterator(data, convertVectorToArray(columns), exprName);
				Vector <Object> dataOutput = new Vector<Object> ();
				while(mapper.hasNext())
					dataOutput.addElement(mapper.next());
				//if(mapper.hasNext())
				value = dataOutput;
			}
		}
		//if(value != null)
			dataKeeper.put((node + "").trim(), value);
		//System.out.println("Value so far..  " + value);*/
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

		Object leftObj = dataKeeper.get(leftKeyName.trim());
		Object rightObj = dataKeeper.get(rightKeyName.trim());

		Object result = null;
		if (rightObj instanceof Double && leftObj instanceof Double)
			result = (Double)(leftObj)
					* (Double)(rightObj);

		else {
			// one of these is an iterator
			// pick it up and do the magic
			// stupidity for now
			result = leftObj + "" + rightObj;
		}

		//System.out.println("result is" + result);
		dataKeeper.put((node + "").trim(), result);
	}

	public void outADivFactor(ADivFactor node) {// out of alternative {div} in
												// Factor, we print the div.
		String leftKeyName = node.getLeft() + "";
		String rightKeyName = node.getRight() + "";

		Object leftObj = dataKeeper.get(leftKeyName.trim());
		Object rightObj = dataKeeper.get(rightKeyName.trim());

		Object result = null;
		if (rightObj instanceof Double && leftObj instanceof Double)
			result = (Double)(leftObj)
					/ (Double)(rightObj);

		else {
			// one of these is an iterator
			// pick it up and do the magic
			// stupidity for now
			result = leftObj + "" + rightObj;
		}

		//System.out.println("result is Div" + result);
		dataKeeper.put((node + "").trim(), result);
	}

	public void inAColDef(AColDef node) {
		String colName = node.getColname().toString().trim();

		if(whatICallThisInMyWorld.containsKey("REPLACE") && whatICallThisInMyWorld.get("REPLACE").equalsIgnoreCase("REPLACE")) {
    		Vector<String> replacements = new Vector<String>();
    		if(!myStore.containsKey("REPLACE")) {
    			myStore.put("REPLACE", replacements);
    		}
    		if (myStore.containsKey("REPLACE")) {
    			replacements = (Vector)myStore.get("REPLACE");
    			String uncleanName = "c: " + colName;
    			replacements.addElement(uncleanName);
    			myStore.put(uncleanName, colName);
    			myStore.put("REPLACE", replacements);
    		}
    	}
		//System.out.println("Inside col def.. ");

		// I will create the iterator here and put it but for now..
		// dataKeeper.put((node+"").trim(), dataKeeper.keys());
		//System.out.println(colName);
		//System.out.println("Full name is " + node);

		Vector<String> colVector = new Vector<String>();
		
		// add this to the existing columns this function is using
		if (currentMathFunction.size() > 0)
		{
			String thisFun = currentMathFunction.get(currentMathFunction.size() - 1);
			// need to keep this
			if(funCol.containsKey(thisFun))
				colVector = funCol.get(thisFun);
			// add this to call vector
			colVector.add((node.getColname() + "").trim());

			// add it back to the list of columns
			// just recording all the columns needed
			funCol.put(thisFun,	colVector);

			Vector <String> nextVector = new Vector<String>();
			// add this also to tempStrings because of the space issue
			if (tempStrings.containsKey(thisFun))
				// need to keep this
				nextVector = tempStrings.get(thisFun);
			// add this to call vector
			nextVector.add((node + "").trim());

			// add this to tempStrings
			tempStrings.put(
					currentMathFunction.get(currentMathFunction.size() - 1),
					nextVector);
		
			// put some dummy data into the data keeper for now
			dataKeeper.put((node + "").trim(), node.getColname());
		}
		else 
		{
			saveData("COL_DEF", colName);
			dataKeeper.put((node + "").trim(), dataKeeper.keys());			
		}
	}

	public void outAModFactor(AModFactor node) {// out of alternative {mod} in
												// Factor, we print the mod.
		//System.out.print(node.getMod());
	}

	public void outAAddColumn(AAddColumn node) {
		
		Vector <String> columns = (Vector <String>)myStore.get("COLUMNS_USED");
		String[] columnArray = convertVectorToArray(columns);
		String newCol = ((Vector)myStore.get("NEWCOL")).get(0).toString();
		
		Iterator it = (Iterator)((Vector)myStore.get("API")).get(0);

		AddColumnOperator addCol = new AddColumnOperator(frame, it, newCol, columnArray);
		addCol.apply();
		
	}

	public void outAExprGroup(AExprGroup node) {
		//System.out.println("Node in expr group" + node);
		//System.out.println(dataKeeper);
		//System.out.println("Data keeper has expr.. [" + node.getExpr() + "]"+ dataKeeper.containsKey((node.getExpr() + "").trim()));
		if( dataKeeper.containsKey((node.getExpr() + "").trim()))
			dataKeeper.put(node + "", dataKeeper.get((node.getExpr() + "").trim()));
	}

	public void inAAddColumn(AAddColumn node) {
		// need to do the same process here as the add
		//System.out.println("IN Adding the column.. " + node);
		String colName = getCol(node.getNewcol() + "");
		//System.out.println("New Column is.. [" + colName + "]");
		
		whatICallThisInMyWorld = new Hashtable<String, String>();
		whatICallThisInMyWorld.put("API_BLOCK", "API");
		whatICallThisInMyWorld.put("EXPR_TERM", "API");
        whatICallThisInMyWorld.put("COL_DEF", "NEWCOL");
		
		reinit();
		
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
    	if(whatICallThisInMyWorld.containsKey("REPLACE") && whatICallThisInMyWorld.get("REPLACE").equalsIgnoreCase("REPLACE")) {
    		Vector<String> replacements = new Vector<String>();
    		if(!myStore.containsKey("REPLACE")) {
    			myStore.put("REPLACE", replacements);
    		}
    		if (myStore.containsKey("REPLACE")) {
    			replacements = (Vector)myStore.get("REPLACE");
    			String valueString = node.toString().trim();
    			String fraction = node.getFraction() +"";
    			String number = (node.getWhole() + "").trim();
    			if(node.getFraction() != null)
    				number = number + "." + (node.getFraction() + "").trim();
    			replacements.addElement(valueString);
    			myStore.put(valueString, number);
    			myStore.put("REPLACE", replacements);
    		}
    	}
//		
//		dataKeeper.put((node + "").trim(), Double.parseDouble(number));		
	}
    

    public void outAWord(AWord node)
    {
        //System.out.println("In a word.. " + node); // need to find a way to clean up information puts a space after the quote
        saveData("WORD_OR_NUM", (node + "").trim());
        //thisRow.addElement((node + "").trim());        
    }

    public void outANumWordOrNum(ANumWordOrNum node)
    {
        //System.out.println("In a Num.. " + node);
        //System.out.println("Data Keeper is.. " + dataKeeper.get((node + "").trim()));
        //thisRow.addElement(dataKeeper.get((node + "").trim()));
        saveData("WORD_OR_NUM", (node + "").trim());
    }

	private String getCol(String colName) {
		colName = colName.substring(colName.indexOf(":") + 1);
		colName = colName.trim();

		return colName;
	}
	
    public void inAExprRow(AExprRow node)
    {
		whatICallThisInMyWorld = new Hashtable<String, String>();
		
		whatICallThisInMyWorld.put("COL_DEF", "SELECTOR");
		reinit();
    }

    public void outAExprRow(AExprRow node)
    {
    	Vector cols = (Vector)myStore.get("SELECTOR");
    	deInit();
    	myStore.put("SELECTORS", cols);
    }


	public void inAMathFunExpr(AMathFunExpr node) {
		//System.out.println("Math Fun expression is ..  " + node);
		// currentMathFunction = node.getMathFun() + "";
	}

	public void inAMathFun(AMathFun node) {
		//System.out.println("Math function is expr " + node.getExpr());
		//System.out.println("Math function is " + node.getId());

		// this will do some stuff and then set the value back
		//dataKeeper.put((node + "").trim(), "Final Value of this function");
		//String procedureName = (node.getId() + "").trim();

		// add this to the current math function
		//currentMathFunction.add(procedureName);
		whatICallThisInMyWorld = new Hashtable<String, String>();
		whatICallThisInMyWorld.put("COL_DEF", "SELECTORS");
		whatICallThisInMyWorld.put("COL_CSV", "GROUP");
		reinit();
	}

	public void outAMathFun(AMathFun node) {
		// function would usually get
		/*
		 * a. Expression - what to compute b. List of columns to pull c.
		 * Iterator which has all of these different columns pulled d.
		 * getValue() method which will actually return an object e.
		 */

		// need to accomodate for the array that is there

		try {
			
			String procedureName = (node.getId() + "").trim();

			// for now I am hard coding a few values for the purposes of testing
			String procedureAlgo = "prerna.algorithm.impl." + procedureName + "Algorithm";
			ExpressionReducer red = (ExpressionReducer) Class.forName(
					procedureAlgo).newInstance();
			
			// I need to find all the different columns for this function
			// this is sitting in the funCol
			
			//System.out.println("Math Fun is.. " + node);
			
			// I need to accomodate group by next
			//System.out.println("COL SCSV Group in the function is ..  " + arrayValues);
			
			//System.out.println("OUT MATH FUN...  " + myStore);
			
			Vector <String> columns = (Vector <String>)myStore.get("SELECTORS");

			Iterator iterator = getData(columns);
			
			//String expression = getModExpression((node.getExpr() + "").trim());
			
			String expression = (node.getExpr() + "").trim().replaceAll("c:", "");

			//System.out.println("Temp Strings.. " + tempStrings);
			//System.out.println("Columns... " + funCol);
			//System.out.println("New expression.. " + expression);
			
			
			// set it into the algorithm and then let it rip
			
			red.set(iterator, convertVectorToArray(columns), expression);
			double [] finalValue = (double [])red.reduce();
			//System.out.println("When this works.. the value is " + finalValue[0]);
			

			// now I need to set this value back
			// adding a dumy value for now say 999
			if(finalValue.length == 1)	
				dataKeeper.put((node + "").trim(), finalValue[0]);
			else
				dataKeeper.put((node + "").trim(), finalValue);
				
			
			// the job here is done.. now add it to tempStrings
			currentMathFunction.remove(procedureName);
			if (currentMathFunction.size() > 0) {
				String nextFunction = currentMathFunction
						.get(currentMathFunction.size() - 1);
				Vector<String> colVector = new Vector<String>();

				if (tempStrings.containsKey(nextFunction))
					colVector = tempStrings.get(nextFunction);

				colVector.add((node + "").trim());
				tempStrings.put(nextFunction, colVector);
			}
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

		// done with that logic now
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
        
        whatICallThisInMyWorld = new Hashtable<String, String>();
        whatICallThisInMyWorld.put("COL_CSV", "HEADERS");
        whatICallThisInMyWorld.put("CSV_ROW", "Data");
        whatICallThisInMyWorld.put("RELATIONS", "RELATIONS");

        reinit();
        IAction thisAction = new ImportAction();
        thisAction.set("TF", frame); // will need to change to constants afterwards
        
        //currentListener.add(thisAction);
    }
    
    public void outAImportColumn(AImportColumn node)
    {
    	//System.out.println("Import Column Done.. ");
    	deInit();
    }
    
    public void inASelector(ASelector node)
    {
    	//System.out.println("In a Selector Node");
    	//currentMathFunction.add((node + "").trim()); 
    }
    
    public void outASelector(ASelector node)
    {
        // this is happening in the api function
    	// I need to find the api action and set this value
    	String selector = (node + "").trim();
    	
    	// get the values from the funCol
    	Vector <String> selectors = funCol.get(selector);
    	funCol.remove(selector);
    	currentMathFunction.remove(selector);
    	
    	//System.out.println("$$ Got them selectors as " + selectors);
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
        //System.out.println("COL WHERE " + node);
        if(whatICallThisInMyWorld.containsKey("COL_WHERE"))
        {
			whatICallThisInMyWorld = new Hashtable<String, String>();
			whatICallThisInMyWorld.put("COL_DEF", "COL_DEF"); 
			whatICallThisInMyWorld.put("COL_DEF_1", "FROM_COL"); // at some point I need to turn this shit into constants
			whatICallThisInMyWorld.put("COL_DEF_2", "TO_COL"); // at some point I need to turn this shit into constants
			whatICallThisInMyWorld.put("CSV_ROW", "TO_DATA");
			whatICallThisInMyWorld.put("CSV_ROW_1", "TO_DATA");
			whatICallThisInMyWorld.put("COL_WHERE", "COL_WHERE");
			
			reinit();
			myStore.put("COMPARATOR", (node.getComparator()+"").trim());
        }
		// this is of the form
        // some column comparator some value
        //currentMathFunction.add((node+"").trim());	
    }

    public void outAColWhere(AColWhere node)
    {
        //System.out.println("COL WHERE DATAKEEPER " + myStore);
        // I need to do some kind of action and pop out the last one on everything
        // Action is here
        if(whatICallThisInMyWorld.containsKey("COL_WHERE"))
        {
        	Hashtable thisStore = myStore;
        	deInit();
        	saveData("COL_WHERE", thisStore);
        }
    }

    public void inARelationClause(ARelationClause node)
    {
    	if(whatICallThisInMyWorld.containsKey("RELATIONS"))
    	{
	    	whatICallThisInMyWorld = new Hashtable<String, String>();
	    	whatICallThisInMyWorld.put("REL_DEF", "JOINS");
	    	whatICallThisInMyWorld.put("RELATIONS", "RELATIONS");
	    	//whatICallThisInMyWorld.put("RELATIONS_1", "RELATIONS");
    	
	    	reinit();
    	}
    }
    

    public void outARelationClause(ARelationClause node)
    {
    	if(whatICallThisInMyWorld.containsKey("RELATIONS"))
    	{
    		//System.out.println("Got all the relationships.. " + myStore);
    		String joinName = whatICallThisInMyWorld.get("REL_DEF"); // what the join is called
    		
    		Hashtable thisStore = myStore;
    		
    		deInit();
	        //whatICallThisInMyWorldHistory.remove(whatICallThisInMyWorldHistory.lastElement());
	        //howManyTimesHaveIVisitedHistory.remove(howManyTimesHaveIVisitedHistory.lastElement());
    		// set this into the mystore now
    		saveData("RELATIONS", thisStore.get(joinName));
    	}
    }

    public void inARelationDef(ARelationDef node)
    {
    	if(whatICallThisInMyWorld.containsKey("REL_DEF"))
    	{
			whatICallThisInMyWorld = new Hashtable<String, String>();
			whatICallThisInMyWorld.put("COL_DEF", "COL_DEF"); 
			whatICallThisInMyWorld.put("COL_DEF_1", "FROM_COL"); // at some point I need to turn this shit into constants
			whatICallThisInMyWorld.put("COL_DEF_2", "TO_COL"); // at some point I need to turn this shit into constants
			whatICallThisInMyWorld.put("REL_DEF", "SELF");

			reinit();
		
			myStore.put("REL_TYPE", (node.getRelType()+"").trim());
    	}
    }

    public void outARelationDef(ARelationDef node)
    {
       // defaultOut(node);
    	//System.out.println("RELATION DEF OUT");
    	if(whatICallThisInMyWorld.containsKey("REL_DEF"))
    	{
        	Hashtable thisStore = myStore;
            deInit();
    		/// ok so it wanted it.. got it <-- Profound again baby
    		saveData("REL_DEF", thisStore);	
    	}
    }
    

    
    public void inAColCsv(AColCsv node)
    {
    	//System.out.println("COL CSV is " + node);
    	//currentMathFunction.addElement((node + "").trim());
    	if(whatICallThisInMyWorld.containsKey("COL_CSV")) // ok col csv is being tracked
    	{
    		whatICallThisInMyWorld = new Hashtable<String, String>();
    		whatICallThisInMyWorld.put("COL_DEF", "COL_DEF"); 
    		
    		reinit();
    	}
    }

    public void outAGroupBy(AGroupBy node)
    {
        defaultOut(node);
        // ok this is happening on math fun
    }

    public void outAColCsv(AColCsv node)
    {
    	String thisNode = (node + "").trim();
    	//System.out.println("COL CSV is " + node);
    	currentMathFunction.remove(thisNode);
    	// get it from the funcol and set it into the groupBy
    	/*if(funCol.containsKey(thisNode))
    	{
    		if(currentMathFunction.size() > 0)
    		{
	    		String latest = currentMathFunction.get(currentMathFunction.size() - 1);   
	    		// this is putting in the group by
	    		arrayValues.put(latest, funCol.get(thisNode));// it is set into the array Values
    		}
       		if(currentListener.size() > 0)
       		{
    			currentListener.get(currentListener.size() -1).set("COL_DEF", funCol.get(thisNode));
    			saveData("COL_CSV", funCol.get(thisNode));
       		}
    		funCol.remove(thisNode);
    	}
    	else*/
    	{
    		Object cols = myStore.get("COL_DEF");
    		deInit(); 
    		//myStore.remove("COL_DEF");
    		saveData("COL_CSV", cols);
    	}
    }

    public void inACsvRow(ACsvRow node)
    {
        //System.out.println("The Row is " + node);
        // need to tell it I am assimilating a vector here
       // thisRow = new Vector<Object>();
        if(whatICallThisInMyWorld.containsKey("CSV_ROW"))
        {    		
        	whatICallThisInMyWorld = new Hashtable<String, String>();
        	whatICallThisInMyWorld.put("WORD_OR_NUM", "WORD_OR_NUM");
        	reinit();

        }
    }
    
    
    public void outACsvRow(ACsvRow node)
    {
    	// I need to do an action here
    	// get the action
    	// call to say this has happened and then reset it to null;
    	//System.out.println("This row so far..  " + thisRow);
    	if(currentListener.size() > 0)
    	{
    		currentListener.get(currentListener.size() -1).processRow("CSV", myStore.get("WORD_OR_NUM"));
    	}
    	else if(whatICallThisInMyWorld.containsKey("WORD_OR_NUM"))
    	{
    		Object csvRow = myStore.get("WORD_OR_NUM");
    		deInit();
        	saveData("CSV_ROW", csvRow);
    		thisRow = null;
    	}
    }

	private Iterator getData(Vector <String> columns )
	{
		if(columns != null && columns.size() <= 1)
			columns.add(columns.get(0));
		// now I need to ask tinker to build me something for this
		Iterator iterator = frame.getIterator(columns);
		if(iterator.hasNext())
		{
			//System.out.println(iterator.next());
		}
		return iterator;
	}
	
	private String getModExpression(String inExpression)
	{
		if (myStore.get("REPLACE") != null) {
			for (String unmod : (Vector<String>)myStore.get("REPLACE")) {
				inExpression = inExpression.replaceAll(unmod, myStore.get(unmod).toString());
			}
		}
		return inExpression;
		// routine to replace it with value
//		String outExpression = inExpression;
//		Vector<String> ripStrings = tempStrings.get(currentMathFunction
//				.get(currentMathFunction.size() - 1));
//		for (int ripIndex = ripStrings.size() - 1; ripIndex >= 0; ripIndex--) {
//			String ripString = ripStrings.remove(ripIndex);
//			outExpression = outExpression.replace(ripString,
//					dataKeeper.get(ripString) + "");
//		}
//		return outExpression;
	}
	
	// move to utility
	private String[] convertVectorToArray(Vector <String> columns)
	{
		// convert this column array
		String [] colArr = new String[columns.size()];
		for(int colIndex = 0;colIndex < columns.size();colArr[colIndex] = columns.get(colIndex),colIndex++);
		return colArr;
	}
	
	private void saveData(String key, Object data)
	{
		if(whatICallThisInMyWorld.containsKey(key)) // ok he is tracking for it // this is where I am going to check all sorts of things... obviously I need to modify the math function later
		{
			// ok which means this is there
			int count = 1;
			if(howManyTimesHaveIVisited.containsKey(key)) // somebody should slap me for not using constants
				count = howManyTimesHaveIVisited.get(key) + 1;
			
			if(whatICallThisInMyWorld.containsKey(key + "_" + count))
				myStore.put(whatICallThisInMyWorld.get(key  + "_" + count), data);
			//else if(count != 1)
			//	dataKeeper.put("COL_DEF_" + count, node.getColname());
			else if(whatICallThisInMyWorld.containsKey(key))
			{
				Vector valVector = new Vector();
				if(myStore.containsKey(whatICallThisInMyWorld.get(key)))
					valVector = (Vector)myStore.get(whatICallThisInMyWorld.get(key));
				if(!valVector.contains(data))
					valVector.add(data);
				myStore.put(whatICallThisInMyWorld.get(key), valVector);
			}
			// random for now
			howManyTimesHaveIVisited.put(key,count);
			//dataKeeper.put((node + "").trim(), dataKeeper.keys());			
		}

	}
	
	/**
	 * Goes down a level (i.e. AddColumn to ExprTerm)
	 */
	private void reinit()
	{
		howManyTimesHaveIVisited = new Hashtable<String, Integer>();
		howManyTimesHaveIVisitedHistory.add(howManyTimesHaveIVisited);
		
		whatICallThisInMyWorldHistory.add(whatICallThisInMyWorld);
		
		myStore = new Hashtable<String, Object>();
		myStoreHistory.add(myStore);
	}
	
	/**
	 * Goes up a level
	 */
	private void deInit()
	{
		howManyTimesHaveIVisitedHistory.removeElement(howManyTimesHaveIVisitedHistory.lastElement());
		whatICallThisInMyWorldHistory.removeElement(whatICallThisInMyWorldHistory.lastElement());
		myStoreHistory.removeElement(myStoreHistory.lastElement());
		
		//System.out.println("My STORE.. " + myStore);
		
		if(myStoreHistory.size() > 0)
			myStore = myStoreHistory.lastElement();
		if(howManyTimesHaveIVisitedHistory.size() > 0)
			howManyTimesHaveIVisited = howManyTimesHaveIVisitedHistory.lastElement();
		if(whatICallThisInMyWorldHistory.size() > 0)
			whatICallThisInMyWorld = whatICallThisInMyWorldHistory.lastElement();
	}
		
}