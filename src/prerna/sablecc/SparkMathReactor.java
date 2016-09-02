package prerna.sablecc;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.AlgorithmStrategy;
import prerna.sablecc.PKQLRunner.STATUS;

public abstract class SparkMathReactor extends AbstractReactor {

	// every single thing I am listening to
	// I need
	// guns.. lot and lots of guns
	// COL_DEF
	// DECIMAL
	// NUMBER - listening just for the fun of it for now
	// It is almost like this will always react only to
	// EXPR_TERM
	// oh wait.. array of expr_term
	// unless of course someone is trying to just sum the number

	Vector<String> replacers = new Vector<String>();
	AlgorithmStrategy strategy = null;
	Hashtable<String, String[]> values2SyncHash = new Hashtable<String, String[]>();

	public SparkMathReactor() {
		String[] thisReacts = { PKQLEnum.EXPR_TERM, PKQLEnum.DECIMAL, PKQLEnum.NUMBER, PKQLEnum.GROUP_BY,
				PKQLEnum.COL_DEF, PKQLEnum.EXPLAIN };
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.MATH_FUN;

	}

	public Vector<String> getColumns() {
		if (myStore.containsKey(PKQLEnum.COL_DEF)) {
			return (Vector<String>) myStore.get(PKQLEnum.COL_DEF);
		} else {
			return new Vector<String>();
		}
	}

	@Override
	public Iterator process() {
		try {
			modExpression();
			System.out.println("Printing the myStore..  " + myStore);

			String procedureName = (String) myStore.get(PKQLEnum.PROC_NAME);
			String procedureAlgo = "prerna.algorithm.impl." + "Spark" + procedureName + "Algorithm";
			Object algorithm = Class.forName(procedureAlgo).newInstance();

			Vector<String> columns = (Vector<String>) myStore.get(PKQLEnum.COL_DEF);
			ITableDataFrame frame = (ITableDataFrame) myStore.get("G");
			Vector<String> groupBys = (Vector<String>) myStore.get(PKQLEnum.COL_CSV);

			Object finalValue = null;
			// if(frame instanceof SparkDataFrame) {
//				finalValue = ((SparkDataFrame)frame).mapReduce(columns, groupBys, procedureName);
			// }
			String nodeStr = myStore.get(whoAmI).toString();
			myStore.put(nodeStr, finalValue);
			myStore.put("STATUS", STATUS.SUCCESS);

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

		return null;
	}

	/*
	 * Explains the reactor using Mustache templating engine.
	 * 
	 * @return String explaining the reactor
	 */
	public String explain() {
		// values that are added to template engine
		HashMap<String, Object> values = new HashMap<String, Object>();
		values.put("operationName", myStore.get("PROC_NAME"));
		values.put("operation", myStore.get("MOD_MATH_FUN"));
		values.put("whoAmI", whoAmI);
		String template = "Performed {{operationName}} on {{operation}}";
		return generateExplain(template, values);
	}

}
