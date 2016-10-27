package prerna.sablecc;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;

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
public class VizReactor extends AbstractVizReactor {

	@Override
	public Iterator process() {
		ITableDataFrame frame = (ITableDataFrame) getValue("G");

		Vector<Object> selectors = (Vector<Object>) getValue("VIZ_SELECTOR");
		Vector<String> vizTypes = (Vector<String>) getValue("VIZ_TYPE");
		Vector<String> vizFormula = (Vector<String>) getValue("VIZ_FORMULA");

		if(selectors == null || selectors.size() == 0) {
			// this is the case when user wants a grid of everything
			// we do not send back any data through the pkql
			// they get it in the getNextTableData call
			return null;
		}

		Vector<String> columns = new Vector<String>();

		List<Map> mergeMaps = new Vector<Map>();
		List<String> mergeVizTypes = new Vector<String>();
		List<String> mergeVizFormula = new Vector<String>();

		List<String> colVizTypes = new Vector<String>();
		
		// we have at least one selector
		int size = selectors.size();
		for(int i = 0; i < size; i++) {
			Object term = selectors.get(i);
			if(term instanceof Map){
				// store this in a list, will use the old map merge logic on it after
				mergeMaps.add((Map) term);
				mergeVizTypes.add(vizTypes.get(i));
				mergeVizFormula.add(vizFormula.get(i));

			} else {
				String val = term.toString().trim();
				columns.add(val);
				colVizTypes.add(vizTypes.get(i));
			}
		}

		Iterator<Object[]> it = getTinkerData(columns, frame, true);
		
		List<Map<String, Object>> tableKeys = new Vector<Map<String, Object>>();
		// add in the rest of the columns, non group and non function
		for (int i = 0; i < columns.size(); i++) {
			String column = columns.get(i);
			Map<String, Object> keyMap = new HashMap<>();
			keyMap.put("varKey", column);
			keyMap.put("uri", column);
			keyMap.put("type", frame.getDataType(column).toString());
			keyMap.put("operation", new HashMap<>());
			keyMap.put("vizType", colVizTypes.get(i).replace("=", ""));

			tableKeys.add(keyMap);
		}
		
		if(mergeMaps.size() == 0) {
			
			List<Object[]> data = new Vector<Object[]>();
			while(it.hasNext()) {
				data.add(it.next());
			}
			
			myStore.put("VizTableValues", data);
			myStore.put("VizTableKeys", tableKeys);
		} else {
			
			Map<Map<String, Object>, Object> map = convertIteratorDataToMap(it, columns, columns);

			// we need to merge some results here
			// consolidate all the terms into a single map
			for(int i = 0; i < mergeMaps.size(); i++) {
				map = mergeMap(map, mergeMaps.get(i));
			}
			
			List<Object[]> data = convertMapToGrid(map, columns.toArray(new String[]{}));
			myStore.put("VizTableValues", data);
			
			Vector<Map<String, Object>> mergeTableKeys = (Vector<Map<String, Object>>) getValue("MERGE_HEADER_INFO");
			for(int i = 0; i < mergeMaps.size(); i++) {
				// map to store the info
				Map<String, Object> headMap = mergeTableKeys.get(i);
				headMap.put("vizType", mergeVizTypes.get(i).replace("=", ""));
				
				tableKeys.add(headMap);
			}
			
			myStore.put("VizTableKeys", tableKeys);
		}
		
		return null;
	}

}
