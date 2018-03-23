package prerna.sablecc2.reactor.export;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.engine.api.IHeadersDataRow;
import prerna.util.ArrayUtilityMethods;

public class HierarchyFormatter extends AbstractFormatter {

	public static final String X_CATEGORY_KEY = "x_category";

	private static final String CHILDREN = "children";
	private static final String PARENT = "parent";
	private static final String NAME = "name";
	private static final String INDEX = "index";
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
		List<String> xCategory = (List<String>) this.optionsMap.get(X_CATEGORY_KEY);
		return getHierarchyData(this.data, this.headers, xCategory);
	}
	
	private Map<String, Object> getHierarchyData(List<Object[]> data, String[] headers, List<String> xCategory) {
		// need to have 1 dendrogram map
		
		LinkedHashMap<Object, Object> x_map = new LinkedHashMap<Object, Object>();
		x_map.put(NAME, "root");
		x_map.put(PARENT, "null");
		x_map.put(CHILDREN, new LinkedHashSet<Map<Object, Object>>());
		
		// loop through to get the indices of the datarow we care about for the various components
		int num_x_components = xCategory.size();
		List<Integer> x_indices = new Vector<Integer>(num_x_components);
		for(int i = 0; i < num_x_components; i++) {
			x_indices.add(ArrayUtilityMethods.arrayContainsValueAtIndex(headers, xCategory.get(i)));
		}
		
		// now that we have the indices
		// loop through the data
		// and construct everything
		for(Object[] dataRow : data) {
			// generate the map for the x axis
			generateParentChildMap(x_map, dataRow, x_indices, num_x_components);
		}
		
		// now we need to loop through and calculate the indices for each each leaf
		addPositionsToMap(x_map);

		List<Map<Object, Object>> grid_data = new ArrayList<Map<Object, Object>>(data.size());
		LinkedHashSet<Map<Object, Object>> xChildrenSet = (LinkedHashSet<Map<Object, Object>>) x_map.get(CHILDREN);
		
		// return all maps
		Map<String, Object> retObj = new HashMap<String, Object>();
		retObj.put("topTree", x_map);
		return retObj;
	}

	/**
	 * Loop through and add the positions of the leaf nodes within the dendrogram
	 * @param main_map
	 * @param CHILDREN
	 * @param INDEX
	 */
	private void addPositionsToMap(LinkedHashMap<Object, Object> main_map) {
		int startindex = 0;
		// thankfully, we know that we always have a root node added at the beginning
		// so for each child, we just need to recursively go down to find the children set
		// and we start to add an index to those maps
		LinkedHashSet<Map<Object, Object>> childrenSet = (LinkedHashSet<Map<Object, Object>>) main_map.get(CHILDREN);
		collectChildren(childrenSet, startindex);
	}
	
	private int collectChildren(LinkedHashSet<Map<Object, Object>> childrenSet, int curChildIndex) {
		// in an ordered fashion, go through all the children
		for(Map<Object, Object> child : childrenSet) {
			LinkedHashSet<Map<Object, Object>> childChildrenSet = (LinkedHashSet<Map<Object, Object>>) child.get(CHILDREN);
			// if the child doesn't have any children
			// collect the child and add him to the set
			if(childChildrenSet.isEmpty()) {
				child.put(INDEX, curChildIndex);
				curChildIndex++;
			}
			// if not, we need to continue going down to the leaves
			else {
				// once we are done iterating all the way down
				// we need to reassign the value of curChildIndex since it is not updated via reference
				curChildIndex = collectChildren(childChildrenSet, curChildIndex);
			}
		}
		return curChildIndex;
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
		LinkedHashMap<Object, Object> x_subMap = staring_map;
		
		int counter = 0;
		while(counter < num_components) {
			LinkedHashSet childrenSet = (LinkedHashSet) x_subMap.get(CHILDREN);
			String parent = x_subMap.get(NAME).toString();

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
			x_subMap = childMap;

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
	
	private LinkedHashMap<Object, Object> getChildMap(Object[] dataRow, LinkedHashSet childrenSet, List<Integer> indices) {
		LinkedHashMap childMap = null;
		for(Integer index : indices) {
			Object name = dataRow[index];
			if(name == null) {
				name = NULL_PLACEHOLDER;
			}
			childMap = getChildMap(name, childrenSet);
			// update the children set
			// so we use this child's childrenset for the next value
			// so get the correct child map object
			childrenSet = (LinkedHashSet) childMap.get(CHILDREN);
		}
		return childMap;
	}
	
	private String getFullPath(Object[] dataRow, List<Integer> indicesToGet) {
		StringBuilder fullPath = new StringBuilder();
		// loop through the data row
		// and get the indices we want in the order we want them in
		// and append in between each index a "." 
		int numIndices = indicesToGet.size();
		for(int i = 0; i < numIndices; i++) {
			Object value = dataRow[indicesToGet.get(i)];
			if(value == null) {
				value = NULL_PLACEHOLDER;
			}
			fullPath.append(value);
			if((i+1) != numIndices) {
				fullPath.append(".");
			}
		}
		return fullPath.toString();
	}

	@Override
	public String getFormatType() {
		return "HIERARCHY";
	}


}
