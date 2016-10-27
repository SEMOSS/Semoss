package prerna.sablecc;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.AlgorithmStrategy;
import prerna.ds.spark.SparkDataFrame;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.sablecc.meta.MathPkqlMetadata;

public abstract class MathReactor extends AbstractReactor {

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

	public MathReactor() {
		String[] thisReacts = { PKQLEnum.EXPR_TERM, "FORMULA", PKQLEnum.DECIMAL, PKQLEnum.NUMBER, PKQLEnum.GROUP_BY,
				PKQLEnum.COL_DEF, PKQLEnum.MATH_PARAM};
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
			String procedureAlgo = "prerna.algorithm.impl." + procedureName + "Algorithm";

			Object algorithm = Class.forName(procedureAlgo).newInstance();

			Vector<String> columns = (Vector<String>) myStore.get(PKQLEnum.COL_DEF);
			String[] columnsArray = convertVectorToArray(columns);
			// this call needs to go to tinker
			// I need to find a way to do this - and I did
			ITableDataFrame frame = (ITableDataFrame) myStore.get("G");

			Vector<String> groupBys = (Vector<String>) myStore.get(PKQLEnum.COL_CSV);
			if (groupBys != null && !groupBys.isEmpty()) {

				Map masterMap = new HashMap();
				if (frame instanceof SparkDataFrame) {
//					((SparkDataFrame)frame).mapReduce(columns, groupBys, procedureAlgo);
				} else {
					Iterator groupByIterator = getTinkerData(groupBys, frame, true);
					while (groupByIterator.hasNext()) {
						Object[] groupByVals = (Object[]) groupByIterator.next();
						Map<String, Object> valMap = new HashMap<String, Object>();
						for (String groupBy : groupBys) {
							valMap.put(groupBy, groupByVals[0]);
						}
						Iterator iterator = getTinkerData(columns, frame, valMap, false);
						Object finalValue = processThing(iterator, algorithm, columnsArray);

						masterMap.put(valMap, finalValue);
					}
				}
				String nodeStr = myStore.get(whoAmI).toString();
				myStore.put(nodeStr, masterMap);
				myStore.put("STATUS", STATUS.SUCCESS);
			} else {

				// bifurcation here
				Object finalValue = null;
				if (frame instanceof SparkDataFrame) {
//					((SparkDataFrame)frame).mapReduce(columns, null, procedureAlgo);
				} else {
					Iterator iterator = getTinkerData(columns, frame, false);
					finalValue = processThing(iterator, algorithm, columnsArray);
				}
				String nodeStr = myStore.get(whoAmI).toString();
				myStore.put(nodeStr, finalValue);
				myStore.put("STATUS", STATUS.SUCCESS);

			}

			// if(algorithm instanceof ExpressionReducer) {
			// red = (ExpressionReducer) algorithm;
			// // and action ?
//				red.set(iterator, columnsArray, myStore.get("MOD_" + whoAmI) + "");
			// double finalValue = (double)red.reduce();
			//
			// String nodeStr = (String)myStore.get(whoAmI);
			//
			// myStore.put(nodeStr, finalValue);
			//
			// System.out.println("Completed Funning Math.. ");
			//
			// } else if(algorithm instanceof ExpressionIterator) {
			// it = (ExpressionIterator) algorithm;
//				it.setData(iterator, columnsArray , myStore.get("MOD_" + whoAmI) + "");
			// String nodeStr = (String)myStore.get(whoAmI);
			// myStore.put(nodeStr, it);
			// System.out.println("Math Fun Iterator stored");
			// }

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

	private Object processThing(Iterator iterator, Object algorithm, String[] columnsArray) {
		if (iterator.hasNext()) {
			System.out.println("Ok.. I am getting SOMETHING..");
		}

		if (algorithm instanceof AlgorithmStrategy) {
			strategy = (AlgorithmStrategy) algorithm;
			strategy.setData(iterator, columnsArray, myStore.get("MOD_" + whoAmI).toString());
			return strategy.execute();
		}
		return null;
	}

	
	public IPkqlMetadata getPkqlMetadata() {
		MathPkqlMetadata metadata = new MathPkqlMetadata();
		metadata.setColumnsOperatedOn((Vector<String>) myStore.get(PKQLEnum.COL_DEF));
		metadata.setProcedureName((String) myStore.get("PROC_NAME"));
		metadata.setPkqlStr((String) myStore.get("MATH_EXPRESSION"));
		metadata.setGroupByColumns((List<String>) myStore.get(PKQLEnum.COL_CSV)); 
		metadata.setAdditionalInfo(myStore.get("ADDITIONAL_INFO"));
		return metadata;
	}
}
