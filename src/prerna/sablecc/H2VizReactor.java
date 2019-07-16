package prerna.sablecc;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.ds.rdbms.h2.H2Frame;
import prerna.sablecc.expressions.IExpressionSelector;
import prerna.sablecc.expressions.sql.H2SqlExpressionIterator;
import prerna.sablecc.expressions.sql.builder.SqlColumnSelector;
import prerna.sablecc.expressions.sql.builder.SqlConstantSelector;
import prerna.sablecc.expressions.sql.builder.SqlExpressionBuilder;
import prerna.sablecc.expressions.sql.builder.SqlSortSelector;
import prerna.util.ArrayUtilityMethods;

public class H2VizReactor extends AbstractVizReactor {

	@Override
	public Iterator process() {
		H2Frame frame = (H2Frame) getValue("G");
		String[] headers = frame.getColumnHeaders();
		
		List<Object> selectors = (List<Object>) getValue("VIZ_SELECTOR");
		List<String> vizTypes = (List<String>) getValue("VIZ_TYPE");
		List<String> vizFormula = (List<String>) getValue("VIZ_FORMULA");
		Map<Object, Object> optionsMap = (Map<Object, Object>) getValue(PKQLEnum.MAP_OBJ);
		String layout = (String) getValue("layout");
		
		if(selectors == null || selectors.size() == 0) {
			// this is the case when user wants a grid of everything
			// we do not send back any data through the pkql
			// they get it in the getNextTableData call
			return null;
		}
		
		// since the BE calculates the column headers for derived columns
		// determining what the sort column is going to be is annoying
		List<Map> mergeMaps = new Vector<Map>();
		List<String> mergeVizTypes = new Vector<String>();
		List<String> mergeVizFormula = new Vector<String>();

		SqlExpressionBuilder mainBuilder = new SqlExpressionBuilder(frame);
		// we have at least one selector
		int size = selectors.size();
		for(int i = 0; i < size; i++) {
			
			Object term = selectors.get(i);
			if(term instanceof SqlExpressionBuilder) {
				List<IExpressionSelector> mainGroups = mainBuilder.getGroupBySelectors();
				
				SqlExpressionBuilder builder = (SqlExpressionBuilder) term;
				List<IExpressionSelector> builderSelectors = builder.getSelectors();
				List<IExpressionSelector> builderGroups = builder.getGroupBySelectors();
				
				// add all the information together here
				if(mainGroups == null || mainGroups.size() == 0) {
					if(builderGroups != null && builderGroups.size()>0) {
						// we have no selectors to start with
						// just add everything
						for(IExpressionSelector group : builderGroups) {
							mainBuilder.addGroupBy(group);
						}
					}
				} else {
					// we need to compare the values here and make sure they are right
					
					Set<String> groups = new HashSet<String>();
					for(IExpressionSelector group : mainGroups) {
						groups.addAll(group.getTableColumns());
					}
					int startSize = groups.size();
					
					for(IExpressionSelector group : builderGroups) {
						groups.addAll(group.getTableColumns());
					}
					
					int endSize = groups.size();
					
					if(startSize != endSize) {
						throw new IllegalArgumentException("Expression contains group bys that are not the same.  Unable to process.");
					}
				}
				
				for(IExpressionSelector selector : builderSelectors) {
					mainBuilder.addSelector(selector);
					
				}
				
			} else if(term instanceof Map){
				// store this in a list, will use the old map merge logic on it after
				mergeMaps.add((Map) term);
				mergeVizTypes.add(vizTypes.get(i));
				mergeVizFormula.add(vizFormula.get(i));
				
			} else {
				IExpressionSelector selector = null;
				String val = term.toString().trim();
				if(ArrayUtilityMethods.arrayContainsValue(headers, val)) {
					// we have a header to return
					selector = new SqlColumnSelector(frame, val);
				} else {
					// we have a constant to return
					selector = new SqlConstantSelector(val);
				}
				mainBuilder.addSelector(selector);
			}
		}
		
		
		if(mergeMaps.size() == 0) {
			// yay, we can just run the query and return it
			if(optionsMap != null) {
				if(optionsMap.containsKey("limit")) {
					mainBuilder.addLimit((int) optionsMap.get("limit"));
				}
				if(optionsMap.containsKey("offset")) {
					mainBuilder.addOffset((int) optionsMap.get("offset"));
				}
				
				if(optionsMap.containsKey("sortVar")) {
					String sortVar = optionsMap.get("sortVar").toString().trim();
					String sortDir = "DESC";
					if(optionsMap.containsKey("sortDir")) {
						sortDir = optionsMap.get("sortDir") + "";
					}
					// this will match the formula used 
					// i.e. for a column, it is c: Studio
					// or it is m:Sum( blah blah )
					int valIndex = vizFormula.indexOf(sortVar);
					List<IExpressionSelector> builderSelectors = mainBuilder.getSelectors();
					SqlSortSelector sortSelector = new SqlSortSelector(frame, builderSelectors.get(valIndex).getName(), sortDir);
					mainBuilder.addSortSelector(sortSelector);
					
					// want to optimize by using indices
					// but we can only do this if we are sorting on a column that is not derived
					String sortName = sortSelector.getName();
					boolean notTransientHeader = ArrayUtilityMethods.arrayContainsValue(frame.getColumnHeaders(), sortName);
					if(notTransientHeader) {
						// does an index already exist for this column?
						if(!frame.getBuilder().columnIndexed(frame.getName(), sortName)) {
							frame.getBuilder().removeAllIndexes();
							frame.getBuilder().addColumnIndex(frame.getName(), sortName);
						}
					}
				}
			}

			H2SqlExpressionIterator it = new H2SqlExpressionIterator(mainBuilder);
			
			List<Object[]> data = new Vector<Object[]>();
			while(it.hasNext()) {
				data.add(it.next());
			}
			
			myStore.put("VizTableKeys", it.getHeaderInformation(vizTypes, vizFormula));
			myStore.put("VizTableValues", data);
		} else {
			// need to merge with existing iterator
			// need to flush iterator into a map :(
			// figure out the columns that were grabbed
			
			// this is all the selectors
			List<IExpressionSelector> querySelectors = mainBuilder.getSelectors();
			int selectorSize = querySelectors.size();
			// get all the headers
			List<String> queryHeaders = new Vector<String>();
			// we need all the selectors that are columns
			List<String> tableCols = new Vector<String>();
			for(int i = 0; i < selectorSize; i++) {
				if(querySelectors.get(i) instanceof SqlColumnSelector) {
					// while this is an add all
					// it will only return a single value
					tableCols.addAll(querySelectors.get(i).getTableColumns());
				}
				queryHeaders.add(querySelectors.get(i).toString());
			}
			
			H2SqlExpressionIterator it = new H2SqlExpressionIterator(mainBuilder);
			List<Map<String, Object>> tableKeys = it.getHeaderInformation(vizTypes, vizFormula);
			
			// this will store the table and headers in myStore
			// this will also add the limit/offset/sortby
			mergeIteratorWithMapData(mergeMaps, mergeVizTypes, it, tableKeys, queryHeaders, tableCols, optionsMap);
		}
		
		if(layout.equals("Clustergram")) {
			Object clusterData = returnClustergramData((List<Object[]>) myStore.get("VizTableValues"), (List<Map<String, Object>>) myStore.get("VizTableKeys"), vizTypes);
			myStore.put("VizTableValues", clusterData);
		}
		
		return null;
	}
}
