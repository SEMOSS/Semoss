package prerna.reactor.export;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.engine.api.IHeadersDataRow;
import prerna.util.ArrayUtilityMethods;

public class HierarchyFormatter extends AbstractFormatter {

	public static final String PATH_KEY = "path";

	private static final String CHILDREN = "children";
	private static final String PARENT = "parent";
	private static final String NAME = "name";
	private static final String NULL_PLACEHOLDER = "EMPTY_VALUE";
	
	private List<Object[]> data;
	private String[] headers;
	
	public HierarchyFormatter() {
		this.data = new ArrayList<Object[]>(100);
	}
	
	public HierarchyFormatter(List<Object[]> data, String[] headers) {
		this.data = data;
		this.headers = headers;
	}
	
	@Override
	public void addData(IHeadersDataRow nextData) {
		this.headers = nextData.getHeaders();
		this.data.add(nextData.getValues());
	}
	
	@Override
	public void clear() {
		this.data = new ArrayList<Object[]>(100);
		this.headers = null;
	}
	
	@Override
	public Object getFormattedData() {
		List<String> path = (List<String>) this.optionsMap.get(PATH_KEY);
		return getHierarchyData(this.data, this.headers, path);
	}
	
	private Map<Object, Object> getHierarchyData(List<Object[]> data, String[] headers, List<String> path) {
		// need to have 1 dendrogram map
		LinkedHashMap<Object, Object> hierarchyMap = new LinkedHashMap<Object, Object>();
		hierarchyMap.put(NAME, "root");
		hierarchyMap.put(PARENT, "null");
		hierarchyMap.put(CHILDREN, new LinkedHashSet<Map<Object, Object>>());
		
		// loop through to get the indices of the datarow we care about for the various components
		int num_components = path.size();
		List<Integer> indicies = new Vector<Integer>(num_components);
		for(int i = 0; i < num_components; i++) {
			indicies.add(ArrayUtilityMethods.arrayContainsValueAtIndex(headers, path.get(i)));
		}
		
		// now that we have the indices
		// loop through the data
		// and construct everything
		for(Object[] dataRow : data) {
			// generate the map for the x axis
			generateParentChildMap(hierarchyMap, dataRow, indicies, num_components);
		}
		
		return hierarchyMap;
	}

	/**
	 * This method is used to properly add values into the dendrograms that make up the 2 sides of the clustergram visualization
	 * @param staring_map
	 * @param dataRow
	 * @param indices
	 * @param num_components
	 * @param NAME
	 * @param PARENT
	 * @param CHILDREN
	 */
	private void generateParentChildMap(LinkedHashMap<Object, Object> staring_map, Object[] dataRow, List<Integer> indices, int num_components) {
		// we need to replace the reference with each iteration
		LinkedHashMap<Object, Object> subMap = staring_map;
		
		int counter = 0;
		while(counter < num_components) {
			LinkedHashSet childrenSet = (LinkedHashSet) subMap.get(CHILDREN);
			String parent = subMap.get(NAME).toString();

			int dataRowIndex = indices.get(counter);
			Object dataRowValue = dataRow[dataRowIndex];
			if(dataRowValue == null) {
				dataRowValue = NULL_PLACEHOLDER;
			}
			
			// see if child already exists
			LinkedHashMap childMap = getChildMap(dataRowValue, childrenSet);
			
			if(childMap == null) {
				// child doesn't exist
				// add him in
				childMap = new LinkedHashMap<Object, Object>();
				childMap.put(NAME, dataRowValue.toString());
				childMap.put(PARENT, parent);
				childMap.put(CHILDREN, new LinkedHashSet<Map<Object, Object>>());
				childrenSet.add(childMap);
			}
			// reset the x_sub map to point to this child
			subMap = childMap;

			// update the counter
			counter++;
		}
	}
	
	/**
	 * Iterate through to find a child node if it exists
	 * @param name
	 * @param childrenSet
	 * @param NAME
	 * @return
	 */
	private LinkedHashMap<Object, Object> getChildMap(Object name, LinkedHashSet<LinkedHashMap> childrenSet) {
		for(LinkedHashMap childMap : childrenSet) {
			if(childMap.get(NAME).toString().equals(name.toString())) {
				return childMap;
			}
		}
		
		return null;
	}
	
	@Override
	public String getFormatType() {
		return "HIERARCHY";
	}


}
