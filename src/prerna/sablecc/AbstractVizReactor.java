package prerna.sablecc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.sablecc.meta.IPkqlMetadata;
import prerna.sablecc.meta.VizPkqlMetadata;

public abstract class AbstractVizReactor extends AbstractReactor {

	public AbstractVizReactor() {
		String[] thisReacts = {PKQLEnum.WORD_OR_NUM, "VIZ_SELECTOR", "VIZ_TYPE", "VIZ_FORMULA", "MERGE_HEADER_INFO", PKQLEnum.MAP_OBJ};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.VIZ;
	}
	
	
	protected void mergeIteratorWithMapData(List<Map> mergeMaps, List<String> mergeVizTypes, Iterator<Object[]> gridIterator, List<Map<String, Object>> tableKeys,
			List<String> queryHeaders, List<String> tableCols, Map<Object, Object> optionsMap) {
		
		// first, convert the grid into a map
		Map<Map<String, Object>, Object> map = convertIteratorDataToMap(gridIterator, queryHeaders, tableCols);
		
		// we need to merge some results here
		// consolidate all the terms into a single map
		for(int i = 0; i < mergeMaps.size(); i++) {
			map = mergeMap(map, mergeMaps.get(i));
		}
		
		List<Object[]> data = convertMapToGrid(map, tableCols.toArray(new String[]{}));
		
		Vector<Map<String, Object>> mergeTableKeys = (Vector<Map<String, Object>>) getValue("MERGE_HEADER_INFO");
		for(int i = 0; i < mergeMaps.size(); i++) {
			// map to store the info
			Map<String, Object> headMap = mergeTableKeys.get(i);
			headMap.put("vizType", mergeVizTypes.get(i).replace("=", ""));
			
			tableKeys.add(headMap);
		}
		
		myStore.put("VizTableKeys", tableKeys);
		
		// perform the limit/offset/sorting
		performLimitOffsetAndSorting(data, optionsMap, tableKeys);
	}
	
	protected void performLimitOffsetAndSorting(List<Object[]> data, Map<Object, Object> optionsMap, List<Map<String, Object>> tableKeys) {
		// we need to manually perform a limit/offset/sort
		int limit = 0;
		int offset = 0;
		int sortIndex = -1;
		String sortDataType = null;
		// we will have a default sort direction
		String sortDir = "DESC";
		if(optionsMap.containsKey("limit")) {
			limit = (int) optionsMap.get("limit");
		}
		if(optionsMap.containsKey("offset")) {
			offset = (int) optionsMap.get("offset");
		}
		
		if(optionsMap.containsKey("sortVar")) {
			String sortVar = optionsMap.get("sortVar").toString().trim();
			if(optionsMap.containsKey("sortDir")) {
				sortDir = optionsMap.get("sortDir") + "";
			}
			// this will match the formula used 
			// i.e. for a column, it is c: Studio
			// or it is m:Sum( blah blah )
			
			// first, see if it is a table column
			
			for(int i = 0; i < tableKeys.size(); i++) {
				Map<String, Object> tableKey = tableKeys.get(i);
				String uri = (String) tableKey.get("uri");
				if(sortVar.equalsIgnoreCase("c: " + uri)) {
					sortDataType = (String) tableKey.get("type");
					sortIndex = i;
					break;
				} else {
					Map<String, Object> operationMap = (Map<String, Object>) tableKey.get("operation");
					if(!operationMap.isEmpty()) {
						if(sortVar.equals(operationMap.get("formula"))) {
							sortDataType = (String) tableKey.get("type");
							sortIndex = i;
							break;
						}
					}
				}
			}
		}
		
		if(sortIndex > -1) {
			data = sortList(data, sortIndex, sortDir, sortDataType);
		}
		
		// need to do some validation here
		// if the FE has gotten to the point where they have
		// scrolled far enough
		// we should just return 0 data
		// that way they know they have gone to far
		int dataSize = data.size();
		if(limit > dataSize) {
			limit = dataSize;
		}
		if(offset > dataSize) {
			offset = dataSize;
		}
		if(limit > 0) {
			if(offset > 0) {
				data = data.subList(offset, offset+limit);
			} else {
				data = data.subList(0, limit);
			}
		} else if(offset > 0) {
			data = data.subList(offset, data.size());
		}
		
		myStore.put("VizTableValues", data);
	}

	protected List<Object[]> sortList(List<Object[]> data, int sortIndex, String sortDir, String dataType) {
		Collections.sort(data, new Comparator<Object[]>() {

			@Override
			public int compare(Object[] o1, Object[] o2) {
				if(sortDir.equalsIgnoreCase("ASC")) {
					if(dataType.equalsIgnoreCase("NUMBER")) {
						return Double.compare( ((Number) o1[sortIndex]).doubleValue(), ((Number) o2[sortIndex]).doubleValue());
					} else {
						return o1[sortIndex].toString().compareTo(o2[sortIndex].toString());
					}
				} else {
					if(dataType.equalsIgnoreCase("NUMBER")) {
						return -1 * Double.compare( ((Number) o1[sortIndex]).doubleValue(), ((Number) o2[sortIndex]).doubleValue());
					} else {
						return -1 * o1[sortIndex].toString().compareTo(o2[sortIndex].toString());
					}
				}
			}
		});
		
		return data;
	}
	
	protected Map<Map<String, Object>, Object> convertIteratorDataToMap(Iterator<Object[]> iterator, List<String> headers, List<String> groupCols) {
		Map<Map<String, Object>, Object> retMap = new HashMap<>();
		while (iterator.hasNext()) {
			
			Map<String, Object> newKey = new LinkedHashMap<>();
			List<Object> newValue = new ArrayList<>();

			Object[] row = iterator.next();
			for (int i = 0; i < row.length; i++) {
				if (groupCols.contains(headers.get(i))) {
					newKey.put(headers.get(i), row[i]);
				} else {
					newValue.add(row[i]);
				}
			}
			retMap.put(newKey, newValue);
		}

		return retMap;
	}
	
	protected List<Object[]> convertMapToGrid(Map<Map<String, Object>, Object> mapData, String[] headers) {
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
	
	protected Map<Map<String, Object>, Object> mergeMap(Map<Map<String, Object>, Object> firstMap, Map<Map<String, Object>, Object> secondMap) {

		if (firstMap == null || firstMap.isEmpty())
			return secondMap;
		if (secondMap == null || secondMap.isEmpty())
			return firstMap;

		Map<Map<String, Object>, Object> mergedMap = new LinkedHashMap<>();

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
		if(myStore.containsKey("commentEdited")) {
			commentEdited(metadata);
		}
		if(myStore.containsKey("commentRemoved")) {
			commentRemoved(metadata);
		}
		if(myStore.containsKey("tools")){
			tools(metadata);
		}
		
		return metadata;
	}
	
	/**
	 * Add to metadta a panel.tools operation
	 * @param metadata
	 */
	protected void tools(VizPkqlMetadata metadata) {
		metadata.addTools();			
		
	}

	/**
	 * Add to metadta a comment removed operation
	 * @param metadata
	 */
	protected void commentRemoved(VizPkqlMetadata metadata) {
		metadata.removeVizComment();			
	}

	/**
	 * Add to metadta a comment edit operation
	 * @param metadata
	 */
	protected void commentEdited(VizPkqlMetadata metadata) {
		Map commentMap = (HashMap) myStore.get("commentEdited");
		String commentText = (String) commentMap.get("text");
		metadata.editVizComment(commentText);		
	}

	/**
	 * Add to metadta a comment add operation
	 * @param metadata
	 */
	protected void commentAdded(VizPkqlMetadata metadata) {
		Map commentMap = (HashMap) myStore.get("commentAdded");
		String commentText = (String) commentMap.get("text");
		metadata.addVizComment(commentText);
	}

	/**
	 * Add to metadata a panel close operation
	 * @param metadata
	 * @return
	 */
	protected void closePanel(VizPkqlMetadata metadata) {
		String closedPanel = (String) myStore.get("closedPanel");
		closedPanel = closedPanel.substring(0, closedPanel.indexOf('.'));
		metadata.addVizClose(closedPanel);
	}

	/**
	 * Add to metadata a close panel operation
	 * @param metadata
	 * @return
	 */
	protected void clonePanel(VizPkqlMetadata metadata) {
		String oldPanel = (String) myStore.get("oldPanel");
		oldPanel = oldPanel.substring(0, oldPanel.indexOf('.'));
		String newPanel = (String) myStore.get("clone");
		metadata.addVizClone(oldPanel, newPanel);
	}

	/**
	 * Add to metadata a layout modification
	 * @param metadata
	 */
	protected void layout(VizPkqlMetadata metadata) {
		String visualType = (String) myStore.get("layout");
		Vector<String> columns = (Vector<String>) myStore.get(PKQLEnum.TERM);
		if(columns == null || columns.isEmpty()) {
			columns = new Vector<String>();
			columns.add("All Columns Selected");
		} 
		// TODO: really need to not do this...
		// super hack!
		else {
			for(int i = 0; i < columns.size(); i++) {
				String col = columns.get(i);
				if(col.startsWith("c:") && !col.contains("m:")) {
					col = col.replace("c:", "").trim();
				}
				columns.set(i, col);
			}
		}
		metadata.addVizLayout(visualType, columns);
	}

	/**
	 * Add to the metadata a look-and-feel operation
	 * @param metadata
	 */
	protected void lookAndFeel(VizPkqlMetadata metadata) {
		Map laf = (HashMap) myStore.get("lookAndFeel");
		metadata.AddVizLookAndFeel(laf);
	}

	/**
	 * Add to the metadata a config map operation
	 * @param metadata
	 */
	protected void configMap(VizPkqlMetadata metadata) {
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
