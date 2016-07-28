package prerna.sablecc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.util.ArrayUtilityMethods;

/**
 * 
 * Viz reactor is responsible for return data associated with a panel.viz pkql command
 *
 * The command will
 */
public class VizReactor extends AbstractReactor {


	Hashtable <String, String[]> values2SyncHash = new Hashtable <String, String[]>();
	
	public VizReactor()
	{
		//MATH_EXPRESSION = the formulas used 
		//PKQLEnum.PROC_NAME = type of math, i.e. Average, Sum
		//PKQLEnum.COL_DEF = columns to do math on
		//PKQLEnum.COL_CSV = group by columns
		String [] thisReacts = {PKQLEnum.WORD_OR_NUM, "TERM", "MATH_EXPRESSION", PKQLEnum.PROC_NAME+"2", PKQLEnum.COL_DEF+"2", PKQLEnum.COL_CSV+"2"};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.VIZ;
	}
	
	@Override
	public Iterator process() {
		Object termObject = myStore.get("VizTableData");
		ITableDataFrame frame = (ITableDataFrame)myStore.get("G");
		List<String> formulas = (List<String>)myStore.get("MATH_EXPRESSION");
		List<String> procedureTypes = (List<String>)myStore.get(PKQLEnum.PROC_NAME+"2");
		List<List<String>> groupBys = (List<List<String>>)myStore.get(PKQLEnum.COL_CSV+"2");
		List<List<String>> calculatedBy = (List<List<String>>)myStore.get(PKQLEnum.COL_DEF+"2");
		
		List<String> columns = new ArrayList<>();
		List<String> columnsToGrab  = new ArrayList<>();
		String[] keyColumns = new String[]{};
		Map<Map<String, Object>, Object> mainMap = new HashMap<>();
		List<Object[]> grid = new ArrayList<>(1);
		
		if(termObject instanceof List) {
			List<Object> listObject = (List<Object>)termObject;
			int counter = 0;
			for(Object nextObject : listObject) {
				if(nextObject instanceof Map) {
					String newColName = "newCol"+counter++;
					columns.add(newColName);
					
					if(keyColumns.length == 0) {
						keyColumns = ((Map<Map<String, Object>, Object>)nextObject).keySet().iterator().next().keySet().toArray(new String[]{});
					}
					
					mainMap = mergeMap(mainMap, (Map<Map<String, Object>, Object>)nextObject);
					
					
				} else if(nextObject instanceof String) {
					columnsToGrab.add(nextObject.toString());
				} else {
					//formulas which are not group bys and not columns should be taken care of here
				}
			}
		} else {
			//if this is the case what are we displaying?
		}
		
		//grab the iterator
		if(columnsToGrab.size() > 0 && keyColumns.length > 0) {
			Map<String, Object> options = new HashMap<>();
			options.put(TinkerFrame.SELECTORS, columnsToGrab);
			options.put(TinkerFrame.DE_DUP, true);
			Iterator<Object[]> iterator = frame.iterator(false, options);
			
			//convert to map and merge
			Map<Map<String, Object>, Object> newMap = convertIteratorDataToMap(iterator, columnsToGrab, keyColumns);
			mainMap = mergeMap(mainMap, newMap);
		} else if(columnsToGrab.size() > 0) {
			Map<String, Object> options = new HashMap<>();
			options.put(TinkerFrame.SELECTORS, columnsToGrab);
			options.put(TinkerFrame.DE_DUP, true);
			Iterator<Object[]> iterator = frame.iterator(false, options);
			
			grid = new ArrayList<>(100);
			while(iterator.hasNext()) {
				grid.add(iterator.next());
			}
		}
		
		
		for(String column : keyColumns) {
			columnsToGrab.remove(column);
		}
		columns.addAll(columnsToGrab);
		
		List<String> headerColumns = new ArrayList<>();
		for(String column : keyColumns) {
			headerColumns.add(column);
		}
		headerColumns.addAll(columns);
		
		//order of header columns will be : key columns (grouped columns) in stable order, then new function columns, then other columns
		
		if(keyColumns.length > 0) {
			grid = convertMapToGrid(mainMap, keyColumns);
		}
		
		//add in the grouped columns
		List columnList = new ArrayList<>(headerColumns.size());
		int i;
		for(i = 0; i < keyColumns.length; i++) {
			Map<String, Object> keyMap = new HashMap<>();
			keyMap.put("varKey", keyColumns[i]);
			keyMap.put("uri", keyColumns[i]);
			keyMap.put("type", frame.getDataType(keyColumns[i]).toString());
			keyMap.put("operation", new HashMap<>());
			columnList.add(keyMap);
		}
		
		//add in the function columns
		int formNum = 0;
		if(procedureTypes != null) {
			for(; i < procedureTypes.size()+keyColumns.length; i++) {
				Map<String, Object> keyMap = new HashMap<>();
				String columnName = headerColumns.get(i);
				if(keyColumns.length == 0) {
					keyMap.put("type", frame.getDataType(headerColumns.get(i)).toString());
					keyMap.put("operation", new HashMap<>());
				} else {
					keyMap.put("type", "NUMBER");
					Map<String, Object> operationMap = new HashMap<>();
					columnName = generateName(groupBys.get(formNum), calculatedBy.get(formNum), procedureTypes.get(formNum));
					operationMap.put("formula", formulas.get(formNum));
					operationMap.put("groupedBy", groupBys.get(formNum));
					operationMap.put("calculatedBy", calculatedBy.get(formNum));
					operationMap.put("math", procedureTypes.get(formNum));
					keyMap.put("operation", operationMap);
				}
				keyMap.put("varKey", columnName);
				keyMap.put("uri", columnName);
				columnList.add(keyMap);
				formNum++;
			}
		}
		
		//add in the rest of the columns, non group and non function
		for(; i < headerColumns.size(); i++) {
			Map<String, Object> keyMap = new HashMap<>();
			keyMap.put("varKey", headerColumns.get(i));
			keyMap.put("uri", headerColumns.get(i));
			keyMap.put("type", frame.getDataType(headerColumns.get(i)).toString());
			keyMap.put("operation", new HashMap<>());
			columnList.add(keyMap);
		}
		
		myStore.put("VizTableKeys", columnList);
		myStore.put("VizTableValues", grid);
		
		return null;
	}
	
	private String generateName(List<String> groupBys, List<String> calcBys, String math) {
		String generatedName = "";
		generatedName+= math;  //Average
		
		//AverageRevenue
		for(int i = 0; i < calcBys.size(); i++) {
			if(i > 0) {
				generatedName+="And"+calcBys.get(i);
			} else {
				generatedName+=calcBys.get(i);
			}
		}
		
		//AverageRevenueOn
		generatedName+="On";
		
		//AverageRevenueOnStudioAndBudget
		for(int i = 0; i < groupBys.size(); i++) {
			if(i > 0) {
				generatedName+= "And"+groupBys.get(i);
			} else {
				generatedName+= groupBys.get(i);
			}
		}
		
		return generatedName;
	}
	
	private Map<Map<String, Object>, Object> convertIteratorDataToMap(Iterator<Object[]> iterator, List<String> columnsToGrab, String[] keyColumns) {
		Map<Map<String, Object>, Object> retMap = new HashMap<>();
		while(iterator.hasNext()) {
			Map<String, Object> newKey = new HashMap<>();
			List<Object> newValue = new ArrayList<>();
			
			Object[] row = iterator.next();
			for(int i = 0; i < row.length; i++) {
				if(ArrayUtilityMethods.arrayContainsValueIgnoreCase(keyColumns, columnsToGrab.get(i))) {
					newKey.put(columnsToGrab.get(i), row[i]);
				} else {
					newValue.add(row[i]);
				}
			}
			retMap.put(newKey, newValue);
		}
		
		return retMap;
	}
	
	private Map<Map<String, Object>, Object> mergeMap(Map<Map<String, Object>, Object> firstMap, Map<Map<String, Object>, Object> secondMap) {
		
		if(firstMap == null || firstMap.isEmpty()) return secondMap;
		if(secondMap == null || secondMap.isEmpty()) return firstMap;
		
		Map<Map<String, Object>, Object> mergedMap = new HashMap<>();
		
		for(Map<String, Object> key : firstMap.keySet()) {
			mergedMap.put(key, firstMap.get(key));
		}
		
		for(Map<String, Object> key : secondMap.keySet()) {
			if(mergedMap.containsKey(key)) {
				Object obj = mergedMap.get(key);
				if(obj instanceof List) {
					((List)obj).add(secondMap.get(key));
				} else if(obj != null) {
					List<Object> newList = new ArrayList<>();
					newList.add(obj);
					Object secondObj = secondMap.get(key);
					if(secondObj instanceof List) {
						newList.addAll((List)secondObj);
					} else if(secondObj != null) {
						newList.add(secondMap.get(key));
					}
					mergedMap.put(key, newList);
				} else {
					List<Object> newList = new ArrayList<>();
					newList.add("");
					Object secondObj = secondMap.get(key);
					if(secondObj instanceof List) {
						newList.addAll((List)secondObj);
					} else if(secondObj != null) {
						newList.add(secondMap.get(key));
					} else {
						newList.add("");
					}
					mergedMap.put(key, newList);
				}
			} else {
				List<Object> valList = new ArrayList<Object>();
				valList.add("");
				valList.add(secondMap.get(key));
				mergedMap.put(key, valList);
			}
		}
		
		return mergedMap;
	}
	
	/**
	 * 
	 * @param mapData
	 * @param headers
	 * @return		  converts the return data from a group by math column into a grid format
	 * 
	 * mapData is of the form:
	 * {
	 * 		{} -> []
	 * }
	 */
    private List<Object[]> convertMapToGrid(Map<Map<String, Object>, Object> mapData, String[] headers) {
        List<Object[]> grid = new Vector<Object[]>();
        
        int numHeaders = headers.length;
        
        // iterate through each unique group
        Set<Map<String, Object>> unqiueGroupSet = mapData.keySet();
        for(Map<String, Object> group : unqiueGroupSet) {

        	List<Object> row = new ArrayList<>();
              // store each value of the group by
              for(int colIdx = 0; colIdx < numHeaders; colIdx++) {
                    row.add(group.get(headers[colIdx]));
              }
              // store the value for the group by result
              Object val = mapData.get(group);
              if(val instanceof List) {
            	  row.addAll((List)val);
              } else {
            	  row.add(val);
              }
              
              grid.add(row.toArray());
        }
        
        return grid;
    }
}
