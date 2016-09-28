package prerna.sablecc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.ExpressionIterator;
import prerna.ds.TinkerFrame;
import prerna.ds.TinkerMetaHelper;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.util.Utility;

public class TinkerColAddReactor extends ColAddReactor {

	//TODO: the internal methods within this class need to be broken out within col add reactor
	// 		there is a lot of duplicated code, only reason we need to break out the logic is for
	//		cases when there is a multi group by (since tinker needs to create an intermediate node)
	//		but this currently copies the same logic for the case when it is not as well :(
	
	@Override
	public Iterator process() {
		// TODO Auto-generated method stub
		// I need to take the col_def
		// and put it into who am I
		modExpression();
		String nodeStr = (String)myStore.get(whoAmI);
		System.out.println("My Store on COL CSV " + myStore);
		TinkerFrame frame = (TinkerFrame) myStore.get("G");

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
		it = getTinkerData(cols, frame, false);
		joinCols = convertVectorToArray(cols);
		Object value = myStore.get(expr);
		if(value == null) value = myStore.get(PKQLEnum.API);

//		if(value instanceof ColAddIterator) {
//			((ColAddIterator)value).updateNewColName(newCol);
//			((ColAddIterator)value).processIterator(frame);
//		} else 
		if (value instanceof Iterator) {
			it = (ExpressionIterator)value;
			processIt(it, frame, joinCols, newCol);
		} else if (value instanceof Map){
			// this map is in the form { {groupedColName=groupedColValue} = calculatedValue }
			Map vMap = (Map) value;
			// first add the metadata and determine if it is a single column join vs. multi column join
			String[] joinColss = null;
			boolean singleCol = true;
			if(vMap.size() > 1) {
				Object fistMapKey = vMap.keySet().iterator().next();
				Vector<String> cols2 = new Vector<String>();
				for(Object key : ((Map)fistMapKey).keySet()){
					cols2.add(key+"");
				}
				joinColss = convertVectorToArray(cols2);
				singleCol = (joinColss.length == 1);

				Object firstNewVal = vMap.get(fistMapKey);
				Object[] newType = Utility.findTypes(firstNewVal.toString());
				String type = newType[0].toString();
				Map<String, String> dataType = new HashMap<String, String>(1);
				dataType.put(newCol, type);
				frame.connectTypes(joinColss, newCol, dataType);
				frame.setDerivedColumn(newCol, true);
			}

			// if single column, just go ahead and add the data
			if(singleCol) {
				for(Object mapKey : vMap.keySet())
				{
					Map mk = (Map)mapKey;
					Map<String, Object> row = new HashMap<String, Object>();
					for(Object key : mk.keySet()) {
						row.put(key+"", mk.get(key));
					}

					Object newVal = vMap.get(mapKey);
					row.put(newCol, newVal);
					frame.addRelationship(row);
				}
			} else {
				// this is a multi column join
				// need to create an intermediary node to keep table structure

				String primKeyConceptName = TinkerMetaHelper.getMetaPrimaryKeyName(joinColss);
				for(Object mapKey : vMap.keySet())
				{
					Map mk = (Map)mapKey;
					Map<String, Object> row = new HashMap<String, Object>();
					for(Object key : mk.keySet()) {
						row.put(key+"", mk.get(key));
					}
					Object[] values = new Object[joinColss.length];
					for(int i = 0; i < values.length; i++) {
						values[i] = mk.get(joinColss[i]);
					}
					
					row.put(newCol, vMap.get(mapKey));
					row.put(primKeyConceptName, TinkerMetaHelper.getPrimaryKey(values));

					frame.addRelationship(row);
				}
				
				// add appropriate blanks
				// this is so filtering doesn't get messed up
				// if you dont add blanks, you will not be able to unfilter properly
				List<String> addedCols = new ArrayList<String>();
				addedCols.add(primKeyConceptName);
				for(String column : joinColss) {
					frame.insertBlanks(column, addedCols);
				}
				addedCols.clear();
				addedCols.add(newCol);
				frame.insertBlanks(primKeyConceptName, addedCols);
			}
		} else {
			it = new ExpressionIterator(it, joinCols, value.toString());
			processIt(it, frame, joinCols, newCol);
		}
		myStore.put("RESPONSE", STATUS.SUCCESS.toString());
		myStore.put("STATUS", STATUS.SUCCESS);
		
		
		// update the data id so FE knows data has been changed
		frame.updateDataId();
		
		return null;
	}

	private void processIt(Iterator it, ITableDataFrame frame, String[] joinCols, String newCol) {
		if(it.hasNext()) {

			boolean addMetaData = true;

			if (joinCols.length > 1) { // multicolumn join
				String primKeyName = TinkerMetaHelper.getMetaPrimaryKeyName(joinCols);
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

					if(addMetaData) {
						Object[] newType = Utility.findTypes(newVal.toString());
						String type = "";
						type = newType[0].toString();
						Map<String, String> dataType = new HashMap<>(1);
						dataType.put(newCol, type);
						frame.connectTypes(joinCols, newCol, dataType);
						frame.setDerivedColumn(newCol, true);
						addMetaData = false;
					}

					frame.addRelationship(row);
				}
				myStore.put("STATUS", STATUS.SUCCESS);			
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

					if(addMetaData) {
						Object[] newType = Utility.findTypes(newVal.toString());
						String type = "";
						type = newType[0].toString();
						Map<String, String> dataType = new HashMap<>(1);
						dataType.put(newCol, type);
						frame.connectTypes(joinCols, newCol, dataType);
						frame.setDerivedColumn(newCol, true);
						addMetaData = false;
					}
					frame.addRelationship(row);
				}
				myStore.put("STATUS", STATUS.SUCCESS);
			}
		} else {
			myStore.put("STATUS", STATUS.ERROR);
		}
	}
}
