package prerna.sablecc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.AbstractTableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.util.Constants;
import prerna.util.Utility;

/*
 * This class handles a col.filterModel(c:col, 'filterWord'?, {"limit":"#", "offset":"#"}?)
 * filterWord and options map are optional.
 *
 */
public class TinkerColFilterModelReactor extends ColFilterModelReactor {

	@Override
	public Iterator process() {
		TinkerFrame table = (TinkerFrame) myStore.get("G");
		Vector<String> cols = (Vector) myStore.get(PKQLEnum.COL_DEF);
		String col = cols.get(0); // using only one column
		String filterWord = (String) myStore.get("filterWord");
		Integer limit = -1;
		Integer offset = -1;

		Map<String, ArrayList<String>> filteredValues = new HashMap<>();
		Map<String, ArrayList<Object>> unfilteredValues = new HashMap<>();
		Map<String, Map<String, Double>> minMaxValues = new HashMap<String, Map<String, Double>>();

		// options for unfilter iterator
		Map<String, Object> options = new HashMap<String, Object>();
		// retrieve limit && offset options
		if (myStore.containsKey(PKQLEnum.MATH_PARAM)) {
			Map<String, Object> filterOptions = (Map<String, Object>) myStore.get(PKQLEnum.MATH_PARAM);
			for (String key : filterOptions.keySet()) {
				if (key.toLowerCase().equals(AbstractTableDataFrame.LIMIT)) {
					limit = Integer.parseInt((String) filterOptions.get(key));
					options.put(AbstractTableDataFrame.LIMIT, limit);
				}
				if (key.toLowerCase().equals(AbstractTableDataFrame.OFFSET)) {
					offset = Integer.parseInt((String) filterOptions.get(key));
					options.put(AbstractTableDataFrame.OFFSET, offset);
				}
			}
		}
		// add selector and distinct options
		options.put(AbstractTableDataFrame.SELECTORS, cols);

		// clean filter word
		if (table != null && filterWord != null && table.getDataType(col).equals(IMetaData.DATA_TYPES.STRING)) {
			filterWord = Utility.cleanString(filterWord, true, true, false);
			Map<String, String> tempBindings = new HashMap();
			tempBindings.put(col, filterWord);
			options.put(AbstractTableDataFrame.TEMPORAL_BINDINGS, tempBindings);
		}

		options.put(AbstractTableDataFrame.DE_DUP, true);

		// get unfiltered values
		Iterator<Object[]> unFilterIt = table.iterator(options);

		// get unique values for unfiltered Set
		Set<String> unfilterSet = new HashSet<String>();
		while (unFilterIt.hasNext()) {
			Object[] row = unFilterIt.next();
			String rowValue = (String) row[0].toString();
			System.out.println(rowValue);
			unfilterSet.add(rowValue);

		}
		unfilteredValues.put(col, new ArrayList<Object>(unfilterSet));

		// get min and max values for numerical columns
		minMaxValues = table.getMinMaxValues(col, unfilterSet);

		// grab filtered values
		Map<String, Object[]> filteredCols = table.getFilterTransformationValues();
		HashSet<String> filteredSet = new HashSet<String>();

		// only get filtered values if the column was filtered
		if (filteredCols.containsKey(col)) {
			// only filter if word is not found in unfilterList
			if (filterWord != null && unfilterSet.isEmpty()) {
				filteredSet = table.filterModel(col, filterWord, limit, offset);
			}
			if (filterWord == null) {
				filteredSet = table.filterModel(col, filterWord, limit, offset);
			}
		}
		filteredValues.put(col, new ArrayList<String>(filteredSet));

		Map<String, Object> retMap = new HashMap<String, Object>();
		retMap.put("unfilteredValues", unfilteredValues);
		retMap.put("filteredValues", filteredValues);
		retMap.put("minMax", minMaxValues);
		myStore.put("filterRS", retMap);
		myStore.put("RESPONSE", STATUS.SUCCESS.toString());
		myStore.put("STATUS", STATUS.SUCCESS);

		// Store values in myStore
		return null;
	}
}
