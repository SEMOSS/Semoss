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
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.sablecc.meta.VizPkqlMetadata;
import prerna.util.ArrayUtilityMethods;

/**
 * 
 * Viz reactor is responsible for return data associated with a panel.viz pkql command
 *
 * The class 
 * 		1. will take column headers and/or group by data from the math reactors
 * 		2. retrieve data for column headers from the frame if they are not part of the group by
 * 		3. combine data coming from multiple math functions (ASSUMES THEY HAVE THE SAME GROUP BY***)
 * 		4. combine data coming from frames and data coming from reactors into a single grid
 * 		5. reorder the grid and header data to match the order it was passed in
 */
public class VizReactor extends AbstractReactor {

	Hashtable<String, String[]> values2SyncHash = new Hashtable<String, String[]>();

	public VizReactor() {
		// MATH_EXPRESSION = the formulas used
		// PKQLEnum.PROC_NAME = type of math, i.e. Average, Sum
		// PKQLEnum.COL_DEF = columns to do math on
		// PKQLEnum.COL_CSV = group by columns
		String[] thisReacts = { PKQLEnum.WORD_OR_NUM, "TERM", "MATH_EXPRESSION", PKQLEnum.PROC_NAME + "2",
				PKQLEnum.COL_DEF + "2", PKQLEnum.COL_CSV + "2"};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.VIZ;
	}

	@Override
	public Iterator process() {
		// grab the data i need
		Object termObject = myStore.get("VizTableData");
		ITableDataFrame frame = (ITableDataFrame) myStore.get("G");
		List<String> formulas = (List<String>) myStore.get("MATH_EXPRESSION");
		List<String> procedureTypes = (List<String>) myStore.get(PKQLEnum.PROC_NAME + "2");
		List<List<String>> groupBys = (List<List<String>>) myStore.get(PKQLEnum.COL_CSV + "2");
		List<List<String>> calculatedBy = (List<List<String>>) myStore.get(PKQLEnum.COL_DEF + "2");

		List<String> columns = new ArrayList<>();
		List<String> columnsToGrab = new ArrayList<>();
		String[] keyColumns = new String[]{}; //group by columns for the calculated columns, assumed to be the same for all calculated columns
		Map<Map<String, Object>, Object> mainMap = new HashMap<>();
		List<Object[]> grid = new ArrayList<>(1);

		Map<Integer, String> indexMap = new HashMap<>();

		int index = 0;
		if (termObject instanceof List) {
			List<Object> listObject = (List<Object>) termObject;
			int counter = 0;
			for (Object nextObject : listObject) {

				// if its a map we know it came from a math reactor
				if (nextObject instanceof Map) {
					String newColName = "newCol" + counter++;
					columns.add(newColName);
					// indexMap.put(newColName, index);
					indexMap.put(index, newColName);

					// we assume key columns are the same for all math reactors,
					// need to initialize
					if (keyColumns.length == 0) {
						keyColumns = ((Map<Map<String, Object>, Object>) nextObject).keySet().iterator().next().keySet()
								.toArray(new String[] {});
					}

					mainMap = mergeMap(mainMap, (Map<Map<String, Object>, Object>) nextObject);

				}

				// this is a column header
				else if (nextObject instanceof String) {
					columnsToGrab.add(nextObject.toString());
					// indexMap.put(nextObject.toString(), index);
					indexMap.put(index, nextObject.toString());
				}

				//this is an empty placeholder to maintain order, need to keep track
				else if (nextObject == null) {
					// indexMap.put("EMPTY"+index, index);
					indexMap.put(index, "EMPTY");
				}

				// this would be a formula that is not a group by, not
				// accounting for these yet
				// getting this would be huge for data visual, imagine scaling
				// axis based on data...i.e. logs
				else {
					// formulas which are not group bys and not columns should
					// be taken care of here
				}

				index++;
			}
		} else {
			// if this is the case what are we displaying?
		}

		boolean mathPerformed = keyColumns.length > 0;

		
//		//grab the iterator because we have columns that need data from the frame
		// if(columnsToGrab.size() > keyColumns.length) {
		//// if(columnsToGrab.size() > 0 && keyColumns.length > 0) {
		// Map<String, Object> options = new HashMap<>();
		// options.put(TinkerFrame.SELECTORS, columnsToGrab);
		// options.put(TinkerFrame.DE_DUP, true);
		// Iterator<Object[]> iterator = frame.iterator(false, options);
		//
		// //convert to map and merge
		// Map<Map<String, Object>, Object> newMap =
		// convertIteratorDataToMap(iterator, columnsToGrab, keyColumns);
		// mainMap = mergeMap(mainMap, newMap);
		// }

		//otherwise we only have column data to grab from the frame, no math was done
		if (columnsToGrab.size() > 0 && !mathPerformed) {
			Map<String, Object> options = new HashMap<>();
			options.put(TinkerFrame.SELECTORS, columnsToGrab);
			options.put(TinkerFrame.DE_DUP, true);
			Iterator<Object[]> iterator = frame.iterator(false, options);

			grid = new ArrayList<>(100);
			while (iterator.hasNext()) {
				grid.add(iterator.next());
			}
		}

		else if (columnsToGrab.size() > keyColumns.length) {
			// if(columnsToGrab.size() > 0 && keyColumns.length > 0) {
			Map<String, Object> options = new HashMap<>();
			options.put(TinkerFrame.SELECTORS, columnsToGrab);
			options.put(TinkerFrame.DE_DUP, true);
			Iterator<Object[]> iterator = frame.iterator(false, options);

			// convert to map and merge
			Map<Map<String, Object>, Object> newMap = convertIteratorDataToMap(iterator, columnsToGrab, keyColumns);
			mainMap = mergeMap(mainMap, newMap);
		}

		for (String column : keyColumns) {
			columnsToGrab.remove(column);
		}

		// columnsToGrab is now all the columns that are not group bys columns
		columns.addAll(columnsToGrab);

		List<String> headerColumns = new ArrayList<>();
		for (String column : keyColumns) {
			headerColumns.add(column);
		}
		headerColumns.addAll(columns);

		// order of header columns will be : key columns (grouped columns) in
		// stable order, then new function columns, then other columns

		if (mathPerformed) {
			grid = convertMapToGrid(mainMap, keyColumns);
		}

		// add in the grouped columns
		// List columnList = new ArrayList<>(headerColumns.size());
		Map<String, Object> columnMap = new HashMap<>();
		int i;
		for (i = 0; i < keyColumns.length; i++) {
			Map<String, Object> keyMap = new HashMap<>();
			keyMap.put("varKey", keyColumns[i]);
			keyMap.put("uri", keyColumns[i]);
			keyMap.put("type", frame.getDataType(keyColumns[i]).toString());
			keyMap.put("operation", new HashMap<>());
			// columnList.add(keyMap);
			columnMap.put(keyColumns[i], keyMap);
		}

		// add in the function columns
		int formNum = 0;
		if (procedureTypes != null) {
			for (; i < procedureTypes.size() + keyColumns.length; i++) {
				Map<String, Object> keyMap = new HashMap<>();
				String columnName = headerColumns.get(i);
				if (keyColumns.length == 0) {
					keyMap.put("type", frame.getDataType(headerColumns.get(i)).toString());
					keyMap.put("operation", new HashMap<>());
				} else {
					keyMap.put("type", "NUMBER");
					Map<String, Object> operationMap = new HashMap<>();
					String newColumnName = generateName(groupBys.get(formNum), calculatedBy.get(formNum),
							procedureTypes.get(formNum));

					// Integer val = indexMap.get(columnName);
					// indexMap.remove(columnName);
					// indexMap.put(newColumnName, val);

					for (Integer indexVal : indexMap.keySet()) {
						String column = indexMap.get(indexVal);
						if (column.equals(columnName)) {
							indexMap.put(indexVal, newColumnName);
						}
					}

					columnName = newColumnName;
					headerColumns.set(i, columnName);

					operationMap.put("formula", formulas.get(formNum));
					operationMap.put("groupedBy", groupBys.get(formNum));
					operationMap.put("calculatedBy", calculatedBy.get(formNum));
					operationMap.put("math", procedureTypes.get(formNum));
					keyMap.put("operation", operationMap);
				}
				keyMap.put("varKey", columnName);
				keyMap.put("uri", columnName);
				// columnList.add(keyMap);
				columnMap.put(columnName, keyMap);
				formNum++;
			}
		}

		// add in the rest of the columns, non group and non function
		for (; i < headerColumns.size(); i++) {
			Map<String, Object> keyMap = new HashMap<>();
			keyMap.put("varKey", headerColumns.get(i));
			keyMap.put("uri", headerColumns.get(i));
			keyMap.put("type", frame.getDataType(headerColumns.get(i)).toString());
			keyMap.put("operation", new HashMap<>());
			// columnList.add(keyMap);
			columnMap.put(headerColumns.get(i), keyMap);
		}

		grid = reorderGrid(grid, headerColumns, indexMap);
		Object[] finalHeaders = getColumnsInOrder(columnMap, indexMap);
		myStore.put("VizTableKeys", finalHeaders);
		myStore.put("VizTableValues", grid);

		return null;
	}

	private String generateName(List<String> groupBys, List<String> calcBys, String math) {
		String generatedName = "";
		generatedName += math; // Average

		// AverageRevenue
		for (int i = 0; i < calcBys.size(); i++) {
			if (i > 0) {
				generatedName += "And" + calcBys.get(i);
			} else {
				generatedName += calcBys.get(i);
			}
		}

		// AverageRevenueOn
		generatedName += "On";

		// AverageRevenueOnStudioAndBudget
		for (int i = 0; i < groupBys.size(); i++) {
			if (i > 0) {
				generatedName += "And" + groupBys.get(i);
			} else {
				generatedName += groupBys.get(i);
			}
		}

		return generatedName;
	}

	private Map<Map<String, Object>, Object> convertIteratorDataToMap(Iterator<Object[]> iterator,
			List<String> columnsToGrab, String[] keyColumns) {
		Map<Map<String, Object>, Object> retMap = new HashMap<>();
		while (iterator.hasNext()) {
			Map<String, Object> newKey = new HashMap<>();
			List<Object> newValue = new ArrayList<>();

			Object[] row = iterator.next();
			for (int i = 0; i < row.length; i++) {
				if (ArrayUtilityMethods.arrayContainsValueIgnoreCase(keyColumns, columnsToGrab.get(i))) {
					newKey.put(columnsToGrab.get(i), row[i]);
				} else {
					newValue.add(row[i]);
				}
			}
			retMap.put(newKey, newValue);
		}

		return retMap;
	}

	/**
	 * 
	 * @param firstMap
	 * @param secondMap
	 * @return
	 * 
	 * 		merges two maps
	 */
	private Map<Map<String, Object>, Object> mergeMap(Map<Map<String, Object>, Object> firstMap,
			Map<Map<String, Object>, Object> secondMap) {

		if (firstMap == null || firstMap.isEmpty())
			return secondMap;
		if (secondMap == null || secondMap.isEmpty())
			return firstMap;

		Map<Map<String, Object>, Object> mergedMap = new HashMap<>();

		for (Map<String, Object> key : firstMap.keySet()) {
			mergedMap.put(key, firstMap.get(key));
		}

		for (Map<String, Object> key : secondMap.keySet()) {
			if (mergedMap.containsKey(key)) {
				Object obj = mergedMap.get(key);
				if (obj instanceof List) {
					((List) obj).add(secondMap.get(key));
				} else if (obj != null) {
					List<Object> newList = new ArrayList<>();
					newList.add(obj);
					Object secondObj = secondMap.get(key);
					if (secondObj instanceof List) {
						newList.addAll((List) secondObj);
					} else if (secondObj != null) {
						newList.add(secondMap.get(key));
					}
					mergedMap.put(key, newList);
				} else {
					List<Object> newList = new ArrayList<>();
					newList.add("");
					Object secondObj = secondMap.get(key);
					if (secondObj instanceof List) {
						newList.addAll((List) secondObj);
					} else if (secondObj != null) {
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
	 * @return converts the return data from a group by math column into a grid
	 *         format
	 * 
	 *         mapData is of the form: { {Title = T1, Studio = S1} -> [0.8, 0.5]
	 *         }
	 */
	private List<Object[]> convertMapToGrid(Map<Map<String, Object>, Object> mapData, String[] headers) {
		List<Object[]> grid = new Vector<Object[]>();

		int numHeaders = headers.length;

		// iterate through each unique group
		Set<Map<String, Object>> unqiueGroupSet = mapData.keySet();
		for (Map<String, Object> group : unqiueGroupSet) {

			List<Object> row = new ArrayList<>();
			// store each value of the group by
			for (int colIdx = 0; colIdx < numHeaders; colIdx++) {
				row.add(group.get(headers[colIdx]));
			}
			// store the value for the group by result
			Object val = mapData.get(group);
			if (val instanceof List) {
				row.addAll((List) val);
			} else {
				row.add(val);
			}

			grid.add(row.toArray());
		}

		return grid;
	}

	// /**
	// *
	// * @param grid
	// * @param columnList
	// * @param indexMap
	// * @return need to reorder the grid :(
	// */
	// private List<Object[]> reorderGrid(List<Object[]> grid, List<String>
	// columnList, Map<String, Integer> indexMap) {
	// List<Object[]> returnGrid = new ArrayList<>();
	// int length = indexMap.keySet().size();
	//
	// String[] columns = new String[columnList.size()];
	// for(int i = 0; i < columns.length; i++) {
	// columns[i] = columnList.get(i);
	// }
	//
	// for(Object[] row : grid) {
	// Object[] newRow = new Object[length];
	// for(String key : indexMap.keySet()) {
	// int val = indexMap.get(key);
	// if(key.toUpperCase().startsWith("EMPTY")) {
	// newRow[val] = "";
	// } else {
	// newRow[val] = row[ArrayUtilityMethods.arrayContainsValueAtIndex(columns,
	// key)];
	// }
	// }
	// returnGrid.add(newRow);
	// }
	//
	// return returnGrid;
	// }
	//
	// private Object[] getColumnsInOrder(Map<String, Object> columnMap,
	// Map<String, Integer> indexMap) {
	// Object[] colArr = new Object[indexMap.keySet().size()];
	// for(String key : indexMap.keySet()) {
	// Integer val = indexMap.get(key);
	// if(key.toUpperCase().startsWith("EMPTY")) {
	// Map<String, Object> keyMap = new HashMap<>();
	// keyMap.put("varKey", "");
	// keyMap.put("uri", "");
	// keyMap.put("type", "");
	// keyMap.put("operation", new HashMap<>());
	// colArr[val] = keyMap;
	// } else {
	// colArr[val] = columnMap.get(key);
	// }
	// }
	// return colArr;
	// }

	/**
	 * 
	 * @param grid
	 * @param columnList
	 * @param indexMap
	 * @return need to reorder the grid :(
	 */
	private List<Object[]> reorderGrid(List<Object[]> grid, List<String> columnList, Map<Integer, String> indexMap) {
		List<Object[]> returnGrid = new ArrayList<>();
		int length = indexMap.keySet().size();

		String[] columns = new String[columnList.size()];
		for (int i = 0; i < columns.length; i++) {
			columns[i] = columnList.get(i);
		}

		for (Object[] row : grid) {
			Object[] newRow = new Object[length];
			for (Integer key : indexMap.keySet()) {
				String column = indexMap.get(key);
				if (column.toUpperCase().startsWith("EMPTY")) {
					newRow[key] = "";
				} else {
					newRow[key] = row[ArrayUtilityMethods.arrayContainsValueAtIndex(columns, column)];
				}
			}
			returnGrid.add(newRow);
		}

		return returnGrid;
	}

	private Object[] getColumnsInOrder(Map<String, Object> columnMap, Map<Integer, String> indexMap) {
		Object[] colArr = new Object[indexMap.keySet().size()];
		for (Integer key : indexMap.keySet()) {
			String column = indexMap.get(key);
			if (column.toUpperCase().startsWith("EMPTY")) {
				Map<String, Object> keyMap = new HashMap<>();
				keyMap.put("varKey", "");
				keyMap.put("uri", "");
				keyMap.put("type", "");
				keyMap.put("operation", new HashMap<>());
				colArr[key] = keyMap;
			} else {
				colArr[key] = columnMap.get(column);
			}
		}
		return colArr;
	}
	
	public IPkqlMetadata getPkqlMetadata() {
		VizPkqlMetadata metadata = new VizPkqlMetadata();
		metadata.setPkqlStr((String) myStore.get(PKQLEnum.VIZ));
		if (myStore.containsKey("configMap")) {
			configMap(metadata);
		}
		// if panel changes look and feel
		if (myStore.containsKey("lookAndFeel")) {
			lookAndFeel(metadata);
		}
		// if new visualization is created
		if (myStore.containsKey("layout")) {
			layout(metadata);
		}
		// if panel is cloned
		if (myStore.containsKey("clone")) {
			clonePanel(metadata);
		}
		// if panel is closed
		if (myStore.containsKey("closedPanel")) {
			closePanel(metadata);
		}
		// handle comments
		if (myStore.containsKey("commentAdded")) {
			commentAdded(metadata);
		}
		
		return metadata;
	}

	/**
	 * Add to metadta a comment add operation
	 * @param metadata
	 */
	private void commentAdded(VizPkqlMetadata metadata) {
		Map commentMap = (HashMap) myStore.get("commentAdded");
		String commentText = (String) commentMap.get("text");
		metadata.addVizComment(commentText);
	}

	/**
	 * Add to metadata a panel close operation
	 * @param metadata
	 * @return
	 */
	private void closePanel(VizPkqlMetadata metadata) {
		String closedPanel = (String) myStore.get("closedPanel");
		closedPanel = closedPanel.substring(0, closedPanel.indexOf('.'));
		metadata.addVizClose(closedPanel);
	}

	/**
	 * Add to metadata a close panel operation
	 * @param metadata
	 * @return
	 */
	private void clonePanel(VizPkqlMetadata metadata) {
		String oldPanel = (String) myStore.get("oldPanel");
		oldPanel = oldPanel.substring(0, oldPanel.indexOf('.'));
		String newPanel = (String) myStore.get("clone");
		metadata.addVizClone(oldPanel, newPanel);
	}

	/**
	 * Add to metadata a layout modification
	 * @param metadata
	 */
	private void layout(VizPkqlMetadata metadata) {
		String visualType = (String) myStore.get("layout");
		Vector<String> columns = (Vector<String>) myStore.get(PKQLEnum.TERM);
		if(columns == null || columns.isEmpty()) {
			columns = new Vector<String>();
			columns.add("USING ALL COLUMNS");
		}
		metadata.addVizLayout(visualType, columns);
		
//		HashMap<String, Object> values = new HashMap<String, Object>();
//		ArrayList<String> columns = new ArrayList<String>();
//		values.put("layout", );
//		values.put("whoAmI", whoAmI);
//		String template = "Created {{layout}}visualization.";
//		// null in case of grid or for all columns
//		
//		if (!term.get(0).equals("null")) {
//			Vector terms = (Vector) myStore.get(PKQLEnum.TERM);
//			Iterator it = terms.iterator();
//			while (it.hasNext()) {
//				String key = ((String) it.next()).trim();
//				System.out.println(key);
//				if (key.substring(0, 2).equals("m:")) { // TODO need to do this
//														// better once
//														// exprReplacer is
//														// implemented
//					columns.add(((String) myStore.get(PKQLEnum.EXPLAIN)).replace(".", ""));
//					myStore.put(PKQLEnum.EXPLAIN, "");
//				} else if (myStore.containsKey(key)) {
//					columns.add((String) myStore.get(key));
//				}
//			}
//			values.put("columns", columns);
//			template = template.substring(0, template.indexOf('.'));
//			template += " using {{columns}}.";
//		}
//		// if(myStore.containsKey("VizTableData")){
//		// values.put("columns", myStore.get("VizTableData"));
//		// s += "VIZREACTOR {{columns}}.";
//		// }
//		return generateExplain(template, values);
	}

	/**
	 * Add to the metadata a look-and-feel operation
	 * @param metadata
	 */
	private void lookAndFeel(VizPkqlMetadata metadata) {
		Map laf = (HashMap) myStore.get("lookAndFeel");
		metadata.AddVizLookAndFeel(laf);
	}

	/**
	 * Add to the metadata a config map operation
	 * @param metadata
	 */
	private void configMap(VizPkqlMetadata metadata) {
		Map config = (HashMap) myStore.get("configMap");
		Map size = (Map) config.get("size");
		Object position = config.get("position");
		
		String width = (String) size.get("width");
		String height = (String) size.get("height");
		String top = "";
		String left = "";
		if(position instanceof Map) {
			Map positionMap = (Map) position;
			top = (String) positionMap.get("top");
			left = (String) positionMap.get("left");
		} else {
			top = left = "auto";
		}
		// add to the metadata here
		metadata.addVizConfigMap(width, height, top, left);
	}
}
