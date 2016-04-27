package prerna.sablecc;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.ExpressionIterator;
import prerna.ds.TinkerMetaHelper;
import prerna.engine.api.ISelectStatement;

public class ColAddReactor extends AbstractReactor {
	
	Hashtable <String, String[]> values2SyncHash = new Hashtable <String, String[]>();
	
	public ColAddReactor() {
		String [] thisReacts = {PKQLEnum.COL_DEF,PKQLEnum.COL_DEF + "_1", PKQLEnum.API}; // these are the input columns - there is also expr Term which I will come to shortly
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.COL_ADD;
		
		// this is the point where I specify what are the child values required for various input
		String [] dataFromExpr = {PKQLEnum.COL_DEF};
		values2SyncHash.put(PKQLEnum.EXPR_TERM, dataFromExpr);

		String [] dataFromApi = {PKQLEnum.COL_CSV};
		values2SyncHash.put(PKQLEnum.API, dataFromApi);
	}

	@Override
	public Iterator process() {
		// TODO Auto-generated method stub
		// I need to take the col_def
		// and put it into who am I
		modExpression();
		String nodeStr = (String)myStore.get(whoAmI);
		System.out.println("My Store on COL CSV " + myStore);
		
		ITableDataFrame frame = (ITableDataFrame) myStore.get("G");
		
		String [] joinCols = null;
		Iterator it = null;
		String newCol = (String)myStore.get(PKQLEnum.COL_DEF + "_1");
		
		// ok this came in as an expr term
		// I need to do the iterator here
		System.err.println(myStore.get(PKQLEnum.EXPR_TERM));
		
		// ok.. so it would be definitely be cool to pass this to an expr script right now and do the op
		// however I dont have this shit
		String expr = (String) myStore.get(PKQLEnum.EXPR_TERM);
		Vector <String> cols = (Vector <String>)myStore.get(PKQLEnum.COL_DEF);
		// col def of the parent will have all of the col defs of the children
		// need to remove the new column as it doesn't exist yet
		cols.remove(newCol);
		it = getTinkerData(cols, frame);
		joinCols = convertVectorToArray(cols);
		Object value = myStore.get(expr);
		if(value == null) value = myStore.get(PKQLEnum.API);
		
		if (value instanceof Iterator) {
			it = (ExpressionIterator)value;
			processIt(it, frame, joinCols, newCol);
		} else if (value instanceof Map){ // this will be the case when we are adding group by data
			// this map is in the form { {groupedColName=groupedColValue} = calculatedValue }
			Map vMap = (Map) value;
			for(Object mapKey : vMap.keySet())
			{
				it = new ExpressionIterator(it, joinCols, vMap.get(mapKey).toString());
				for(Object key : ((Map)mapKey).keySet()){
					((ExpressionIterator)it).setBinding(key+"", ((Map)mapKey).get(key));
					cols.add(key+"");
				}
				joinCols = convertVectorToArray(cols);
				processIt(it, frame, joinCols, newCol);
			}
		} else {
			it = new ExpressionIterator(it, joinCols, value.toString());
			processIt(it, frame, joinCols, newCol);
		}

		return null;
	}

	private void processIt(Iterator it, ITableDataFrame frame, String[] joinCols, String newCol) {
		if(it.hasNext()) {
			frame.connectTypes(joinCols, newCol);
		
			if (joinCols.length > 1) { // multicolumn join
				String primKeyName = TinkerMetaHelper.getPrimaryKey(joinCols);
				while(it.hasNext()) {
					HashMap<String, Object> row = new HashMap<String, Object>();
					Object newVal = it.next();
					Object[] values = new Object[joinCols.length];
					if ((newVal instanceof List) && ((List)newVal).size() == 1)
						row.put(newCol, ((List)newVal).get(0));
					else {
						row.put(newCol, newVal);
					}
					for(int i = 0; i < joinCols.length; i++) {
						if (it instanceof ExpressionIterator) {
							Object rowVal = ((ExpressionIterator)it).getOtherBindings().get(joinCols[i]);
							row.put(joinCols[i], rowVal);
							values[i] = rowVal;
						}
					}
					row.put(primKeyName, TinkerMetaHelper.getPrimaryKey(values));
					frame.addRelationship(row, row);
				}
				myStore.put("STATUS", "SUCCESS");			
			} else {
				while(it.hasNext()) {
					HashMap<String, Object> row = new HashMap<String, Object>();
					Object newVal = it.next();
					if ((newVal instanceof List) && ((List)newVal).size() == 1)
						row.put(newCol, ((List)newVal).get(0));
					else {
						row.put(newCol, newVal);
					}
					for(int i = 0; i < joinCols.length; i++) {
						if (it instanceof ExpressionIterator) {
							row.put(joinCols[i], ((ExpressionIterator)it).getOtherBindings().get(joinCols[i]));
						}
					}
				if (newVal instanceof ISelectStatement) {
					System.out.println(((ISelectStatement)newVal).getPropHash());
				}
					frame.addRelationship(row, row);
				}
				myStore.put("STATUS", "SUCCESS");
			}
		} else {
			myStore.put("STATUS", "FAIL");
		}
	}

	// gets all the values to synchronize for this 
	public String[] getValues2Sync(String input)
	{
		return values2SyncHash.get(input);
	}


}
