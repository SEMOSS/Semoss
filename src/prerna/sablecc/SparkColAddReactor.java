package prerna.sablecc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.ExpressionIterator;
import prerna.ds.spark.SparkDataFrame;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.util.Utility;

public class SparkColAddReactor extends AbstractReactor {
	Hashtable<String, String[]> values2SyncHash = new Hashtable<String, String[]>();

	public SparkColAddReactor() {
		String [] thisReacts = {PKQLEnum.COL_DEF, PKQLEnum.COL_DEF + "_1", PKQLEnum.API}; // these are the input columns - there is also expr Term which I will come to shortly
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.COL_ADD;

		// this is the point where I specify what are the child values required
		// for various input
		String[] dataFromExpr = { PKQLEnum.COL_DEF };
		values2SyncHash.put(PKQLEnum.EXPR_TERM, dataFromExpr);

		String[] dataFromApi = { PKQLEnum.COL_CSV };
		values2SyncHash.put(PKQLEnum.API, dataFromApi);
	}

	@Override
	public Iterator process() {
		// TODO Auto-generated method stub
		// I need to take the col_def
		// and put it into who am I
		modExpression();
		String nodeStr = (String) myStore.get(whoAmI);
		System.out.println("My Store on COL CSV " + myStore);
		ITableDataFrame frame = (ITableDataFrame) myStore.get("G");

		String[] joinCols = null;
		Iterator it = null;
		String newCol = (String) myStore.get(PKQLEnum.COL_DEF + "_1");

		// ok this came in as an expr term
		// I need to do the iterator here
		System.err.println(myStore.get(PKQLEnum.EXPR_TERM));

		// ok.. so it would be definitely be cool to pass this to an expr script
		// right now and do the op
		// however I dont have this shit
		String expr = (String) myStore.get(PKQLEnum.EXPR_TERM);
		Vector<String> cols = (Vector<String>) myStore.get(PKQLEnum.COL_DEF);
		// col def of the parent will have all of the col defs of the children
		// need to remove the new column as it doesn't exist yet
		cols.remove(newCol);
		it = getTinkerData(cols, frame, false);
		joinCols = convertVectorToArray(cols);

		Object value = myStore.get(expr);
		if (value == null)
			value = myStore.get(PKQLEnum.API);

		if (value instanceof Iterator) {
			it = (ExpressionIterator) value;
			processIt(it, frame, joinCols, newCol);
		} else if (value instanceof Map) { // this will be the case when we are
											// adding group by data
			// this map is in the form { {groupedColName=groupedColValue} =
			// calculatedValue }
			Map vMap = (Map) value;
			boolean addMetaData = true;
			List<Object[]> rows = new ArrayList<>();
			for (Object mapKey : vMap.keySet()) {
				Vector<String> cols2 = new Vector<String>();
				for (Object key : ((Map) mapKey).keySet()) {
					cols2.add(key + "");
				}
				String[] joinColss = convertVectorToArray(cols2);

				Object newVal = vMap.get(mapKey);

				if (addMetaData) {
					Object[] newType = Utility.findTypes(newVal.toString());
					String type = "";
					type = newType[0].toString();
					Map<String, String> dataType = new HashMap<>(1);
					dataType.put(newCol, type);
					frame.connectTypes(joinColss, newCol, dataType);
					frame.setDerivedColumn(newCol, true);
					addMetaData = false;
					((SparkDataFrame) frame).mergeTable(expr, "inner join", newCol);
				}
			}
		} else {
			it = new ExpressionIterator(it, joinCols, value.toString());
			processIt(it, frame, joinCols, newCol);
		}
		myStore.put("RESPONSE", STATUS.SUCCESS.toString());
		myStore.put("STATUS", STATUS.SUCCESS);

		return null;
	}

	private void processIt(Iterator it, ITableDataFrame frame, String[] joinCols, String newCol) {
		if (it.hasNext()) {

			boolean addMetaData = true;

			while (it.hasNext()) {
				HashMap<String, Object> row = new HashMap<String, Object>();
				Object newVal = it.next();
				Object[] values = new Object[joinCols.length];
				if ((newVal instanceof List) && ((List) newVal).size() == 1)
					row.put(newCol, ((List) newVal).get(0));
				else {
					row.put(newCol, newVal);
				}
				for (int i = 0; i < joinCols.length; i++) {
					if (it instanceof ExpressionIterator) {
						Object rowVal = ((ExpressionIterator) it).getOtherBindings().get(joinCols[i]);
						row.put(joinCols[i], rowVal);
						values[i] = rowVal;
					}
				}

				if (addMetaData) {
					Object[] newType = Utility.findTypes(newVal.toString());
					String type = "";
					type = newType[0].toString();
					Map<String, String> dataType = new HashMap<>(1);
					dataType.put(newCol, type);
					frame.connectTypes(joinCols, newCol, dataType);
					frame.setDerivedColumn(newCol, true);
					addMetaData = false;
				}

				frame.addRelationship(row, row);
			}
			myStore.put("STATUS", STATUS.SUCCESS);
		} else {
			myStore.put("STATUS", STATUS.ERROR);
		}
	}

	// gets all the values to synchronize for this
	public String[] getValues2Sync(String input) {
		return values2SyncHash.get(input);
	}
}
