package prerna.sablecc;

import java.util.Iterator;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.AlgorithmStrategy;

public class MathReactor extends AbstractReactor {
	
	// every single thing I am listening to
	// I need
	//guns.. lot and lots of guns
	// COL_DEF
	// DECIMAL
	// NUMBER - listening just for the fun of it for now
	// It is almost like this will always react only to 
	// EXPR_TERM
	// oh wait.. array of expr_term
	// unless of course someone is trying to just sum the number
	
	Vector <String> replacers = new Vector<String>();
	AlgorithmStrategy strategy = null;
	
	
	public MathReactor() {
		String [] thisReacts = {TokenEnum.EXPR_TERM, TokenEnum.DECIMAL, TokenEnum.NUMBER, TokenEnum.GROUP_BY, TokenEnum.COL_DEF};
		super.whatIReactTo = thisReacts;
		super.whoAmI = TokenEnum.MATH_FUN;
	}
	
	public Vector<String> getColumns() {
		if (myStore.containsKey(TokenEnum.COL_DEF)) {
			return (Vector<String>)myStore.get(TokenEnum.COL_DEF);
		}
		else {
			return new Vector<String>();
		}
	}

	@Override
	public Iterator process() {
		try {
			modExpression();
			System.out.println("Printing the myStore..  " + myStore);

			String procedureName = (String)myStore.get(TokenEnum.PROC_NAME);
			String procedureAlgo = "prerna.algorithm.impl." + procedureName + "Algorithm";
			
			Object algorithm = Class.forName(procedureAlgo).newInstance();
			
			Vector <String> columns = (Vector <String>)myStore.get(TokenEnum.COL_DEF);
			String[] columnsArray = convertVectorToArray(columns);
			// this call needs to go to tinker
			// I need to find a way to do this - and I did
			Iterator iterator = getTinkerData(columns, (ITableDataFrame)myStore.get("G"));
			
			if(iterator.hasNext())
			{
				System.out.println("Ok.. I am getting SOMETHING..");
			}
			
			if (algorithm instanceof AlgorithmStrategy) {
				strategy = (AlgorithmStrategy) algorithm;
				strategy.setData(iterator, columnsArray, myStore.get("MOD_" + whoAmI).toString());
				Object finalValue = strategy.execute();
				String nodeStr = myStore.get(whoAmI).toString();
				myStore.put(nodeStr, finalValue);
			}
			
//			if(algorithm instanceof ExpressionReducer) {
//				red = (ExpressionReducer) algorithm;
//				// and action ?
//				red.set(iterator, columnsArray, myStore.get("MOD_" + whoAmI) + "");
//				double finalValue = (double)red.reduce();
//				
//				String nodeStr = (String)myStore.get(whoAmI);
//				
//				myStore.put(nodeStr, finalValue);
//				
//				System.out.println("Completed Funning Math.. ");
//				
//			} else if(algorithm instanceof ExpressionIterator) {
//				it = (ExpressionIterator) algorithm;
//				it.setData(iterator, columnsArray , myStore.get("MOD_" + whoAmI) + "");
//				String nodeStr = (String)myStore.get(whoAmI);
//				myStore.put(nodeStr, it);
//				System.out.println("Math Fun Iterator stored");
//			}
			
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


		// create the iterator and keep it moving
		// this is where I do the out math fun
		
		// this is where I would create the iterator to do various things
		return null;
	}
	
	
}
