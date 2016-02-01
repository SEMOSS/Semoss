package prerna.sablecc;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Vector;

import prerna.ds.ExpressionReducer;
import prerna.ds.TinkerFrame;
import prerna.sablecc.analysis.DepthFirstAdapter;
import prerna.sablecc.node.AAddColumn;
import prerna.sablecc.node.AApiBlock;
import prerna.sablecc.node.AColDef;
import prerna.sablecc.node.ADivFactor;
import prerna.sablecc.node.AExprGroup;
import prerna.sablecc.node.AExprScript;
import prerna.sablecc.node.AExprTerm;
import prerna.sablecc.node.AJoinColumn;
import prerna.sablecc.node.AMathFun;
import prerna.sablecc.node.AMathFunExpr;
import prerna.sablecc.node.AMinusExpr;
import prerna.sablecc.node.AModFactor;
import prerna.sablecc.node.AMultFactor;
import prerna.sablecc.node.ANumberTerm;
import prerna.sablecc.node.APlusExpr;
import prerna.sablecc.node.ASetColumn;
import prerna.sablecc.node.ATermFactor;
import prerna.sablecc.node.AVarop;
import prerna.sablecc.node.PColGroup;
import prerna.sablecc.node.TNumber;

class Translation extends DepthFirstAdapter {

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

	Vector<String> currentMathFunction = new Vector<String>();

	public void inAApiBlock(AApiBlock node) {
		System.out.println("API...  " + node); // defaultIn(node);
		System.out.println(node.getEngineName() + " <> " + node.getInsight());
	}

	public void caseTNumber(TNumber node) {// When we see a number, we print it.
		System.out.print(node);

	}

	public void inAExprScript(AExprScript node) {
		System.out.println("In a script expr");
		defaultIn(node);
	}

	public void outAExprScript(AExprScript node) {
		String nodeStr = node.getExpr() + "";
		nodeStr = nodeStr.trim();

		// this is the last portion of everything

		System.out.println("out of scroipt expr [" + node + "]");
		System.out.println(" Found it in data keeper ?"
				+ dataKeeper.get(nodeStr));
		if (!dataKeeper.containsKey((node + "").trim())) {
			dataKeeper.put((node + "").trim(), dataKeeper.get(nodeStr));
		}

	}

	public void outAPlusExpr(APlusExpr node) {// out of alternative {plus} in
												// Expr, we print the plus.
		System.out.print("Plus expression..  " + node.getPlus());
		String leftKeyName = node.getLeft() + "";
		String rightKeyName = node.getRight() + "";

		Object leftObj = dataKeeper.get(leftKeyName.trim());
		Object rightObj = dataKeeper.get(rightKeyName.trim());
		System.out.println(node.getLeft() + " [][] " + node.getRight());
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
		System.out.println("result is add " + result);

		System.out.println("node itself looks like.. APlus [" + node + "]");
		dataKeeper.put((node + "").trim(), result);
	}

	public void inAMinusExpr(AMinusExpr node) {
		System.out.println("MINUS... " + node);
	}

	public void outAMinusExpr(AMinusExpr node) {// out of alternative {plus} in
												// Expr, we print the plus.

		System.out.println("MINUS again in out.. ");
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

		System.out.println("result is" + result);

		System.out.println(node.getLeft() + " [][] " + node.getRight());

		System.out.println("node itself looks like.. minus[" + node + "]");
		dataKeeper.put((node + "").trim(), result);
	}

	public void inATermFactor(ATermFactor node) {
		// defaultIn(node);
		System.out.println("In a term Factor");
		String termKey = node.getTerm() + "";
		System.out.println(node.getTerm());

	}

	public void inAExprTerm(AExprTerm node) {
		System.out.println("Printing expr term PAR " + node.getExpr());
		currentMathFunction.add((node + "").trim());

		// this is the one that has paranthesis
	}

	public void outAExprTerm(AExprTerm node) {
		System.out.println("Successful in retrieving the data for expr term ? "
				+ node.getExpr() + " "
				+ dataKeeper.containsKey((node.getExpr() + "").trim()));
		// get the value of it
		Object value = dataKeeper.get((node.getExpr() + "").trim());
		// remove it from the function
		currentMathFunction.remove((node + "").trim());

		if(value instanceof Double)
		{
			Vector<String> colVector = new Vector<String>();
			if (currentMathFunction.size() > 0
					&& tempStrings.containsKey(currentMathFunction
							.get(currentMathFunction.size() - 1)))
				// need to keep this
				colVector = tempStrings.get(currentMathFunction
						.get(currentMathFunction.size() - 1));
			// add this to call vector
			colVector.add((node + "").trim());
	
			// I could come here after 
			// (2 + c:z)
			// or 2 + 3 - In the case of 2 + 3. It makes meaning to add to tempStrings
			// else hmhmmm.. 
			
			// add this to tempStrings
			if(currentMathFunction.size() > 0)
				tempStrings.put(
					currentMathFunction.get(currentMathFunction.size() - 1),
					colVector);
		}
		else
		{
			// i need to add these columns to the parent ?
			System.out.println(".. I need to do something here.. ");
			System.out.println(funCol);
			System.out.println(tempStrings);
			remasterCol((node + "").trim(), currentMathFunction.get(currentMathFunction.size() -1), funCol);
			remasterCol((node + "").trim(), currentMathFunction.get(currentMathFunction.size() -1), tempStrings);
		}
		dataKeeper.put((node + "").trim(), value);
		System.out.println("Value so far..  " + value);
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
													// System.out.print(node.getMult());
													// System.out.print(node.getPlus());
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

		System.out.println("result is" + result);
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

		System.out.println("result is Div" + result);
		dataKeeper.put((node + "").trim(), result);
	}

	public void inAColDef(AColDef node) {
		System.out.println("Inside col def.. ");
		String colName = node.getColname() + "";

		// I will create the iterator here and put it but for now..
		// dataKeeper.put((node+"").trim(), dataKeeper.keys());
		System.out.println(colName);
		System.out.println("Full name is " + node);

		Vector<String> colVector = new Vector<String>();

		String thisFun = currentMathFunction.get(currentMathFunction.size() - 1);
		
		// add this to the existing columns this function is using
		if (currentMathFunction.size() > 0)
		{
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
			// random for now
			dataKeeper.put((node + "").trim(), dataKeeper.keys());			
		}
	}

	public void outAModFactor(AModFactor node) {// out of alternative {mod} in
												// Factor, we print the mod.
		System.out.print(node.getMod());
	}

	public void outAAddColumn(AAddColumn node) {
		
		// this is where the majority of the work would be done
		
		System.out.println("Adding the column.. " + node);
		String colName = getCol(node.getNewcol() + "");
		System.out.println("New Column is.. [" + colName + "]");
		System.out.println("Column expression is [" + node.getExprGroup());
		System.out.println("And the value is present as.. "
				+ dataKeeper.get(node.getExprGroup()));
	}

	public void outAExprGroup(AExprGroup node) {
		System.out.println("Node in expr group" + node);
		System.out.println(dataKeeper);
		System.out.println("Data keeper has expr.. [" + node.getExpr() + "]"
				+ dataKeeper.containsKey((node.getExpr() + "").trim()));
		dataKeeper.put(node + "", dataKeeper.get(node.getExpr()));
	}

	public void inAAddColumn(AAddColumn node) {
		// need to do the same process here as the add
		System.out.println("IN Adding the column.. " + node);
		String colName = getCol(node.getNewcol() + "");
		System.out.println("New Column is.. [" + colName + "]");
		
		
		
	}

	public void outAJoinColumn(AJoinColumn node) {
		String col2Join = getCol(node.getNewcol() + "");
		LinkedList<PColGroup> otherColGroup = node.getJoincol();
		for (int otherIndex = 0; otherIndex < otherColGroup.size(); otherIndex++) {
			String otherCol = getCol(otherColGroup.get(otherIndex) + "");
			System.out.println(col2Join + "<<>>" + otherCol);

			// this is where we will add it into
		}
		defaultOut(node);
	}

	public void outASetColumn(ASetColumn node) {
		System.out.println("Set.. [" + (node.getExpr() + "").trim() + "]");
	}

	public void outAVarop(AVarop node) {
		String varName = getCol(node.getName() + "");
		String expr = getCol(node.getExpr() + "");
		System.out.println("Variable declaration " + varName + " =  " + expr);
		// defaultOut(node);
	}

	public void inANumberTerm(ANumberTerm node) {
		System.out.println("Number term.. ");
		String number = node.getNumber() + "";
		dataKeeper.put(number.trim(), Double.parseDouble(number));
	}

	private String getCol(String colName) {
		colName = colName.substring(colName.indexOf(":") + 1);
		colName = colName.trim();

		return colName;
	}

	public void inAMathFunExpr(AMathFunExpr node) {
		System.out.println("Math Fun expression is ..  " + node);
		// currentMathFunction = node.getMathFun() + "";
	}

	public void inAMathFun(AMathFun node) {
		System.out.println("Math function is expr " + node.getExpr());
		System.out.println("Math function is " + node.getId());

		// this will do some stuff and then set the value back
		dataKeeper.put((node + "").trim(), "Final Value of this function");
		String procedureName = (node.getId() + "").trim();

		// add this to the current math function
		currentMathFunction.add(procedureName);

	}

	public void outAMathFun(AMathFun node) {
		// function would usually get
		/*
		 * a. Expression - what to compute b. List of columns to pull c.
		 * Iterator which has all of these different columns pulled d.
		 * getValue() method which will actually return an object e.
		 */


		try {
			
			String procedureName = (node.getId() + "").trim();

			// for now I am hard coding a few values for the purposes of testing
			String procedureAlgo = "prerna.algorithm.impl." + procedureName + "Algorithm";
			ExpressionReducer red = (ExpressionReducer) Class.forName(
					procedureAlgo).newInstance();

			// now get the data from tinker
			TinkerFrame frame = new TinkerFrame();
			frame.tryCustomGraph();
			
			// I need to find all the different columns for this function
			// this is sitting in the funCol
			Vector <String> columns = funCol.get(procedureName);
			if(columns != null && columns.size() <= 1)
				columns.add(columns.get(0));
			// now I need to ask tinker to build me something for this
			Iterator iterator = frame.getIterator(columns);
			if(iterator.hasNext())
			{
				System.out.println(iterator.next());
			}
			
			System.out.println("Math Fun is.. " + node);

			// routine to replace it with value
			String expression = node.getExpr() + "";
			Vector<String> ripStrings = tempStrings.get(currentMathFunction
					.get(currentMathFunction.size() - 1));
			for (int ripIndex = ripStrings.size() - 1; ripIndex >= 0; ripIndex--) {
				String ripString = ripStrings.remove(ripIndex);
				expression = expression.replace(ripString,
						dataKeeper.get(ripString) + "");
			}

			System.out.println("Temp Strings.. " + tempStrings);
			System.out.println("Columns... " + funCol);
			System.out.println("New expression.. " + expression);
			
			// convert this column array
			String [] colArr = new String[columns.size()];
			for(int colIndex = 0;colIndex < columns.size();colArr[colIndex] = columns.get(colIndex),colIndex++);
			
			// set it into the algorithm and then let it rip
			red.set(iterator, colArr, expression);
			double finalValue = (Double)red.reduce();
			System.out.println("When this works.. the value is " + finalValue);
			

			// now I need to set this value back
			// adding a dumy value for now say 999

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
		System.out.println("OUT ... Math Fun expression is ..  ");
		// do some processing for sum here
		System.out.println("Math fun..  " + node.getMathFun());

		// I need to see if I can replace anything here
		// I need to see if there are temp strings I can replace here

	}
	
	
	
}