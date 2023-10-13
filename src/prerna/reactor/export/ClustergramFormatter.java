package prerna.reactor.export;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.engine.api.IHeadersDataRow;
import prerna.util.ArrayUtilityMethods;

public class ClustergramFormatter extends AbstractFormatter {

	public static final String X_CATEGORY_KEY = "x_category";
	public static final String Y_CATEGORY_KEY = "y_category";
	public static final String HEAT_KEY = "heat";

	private static final String CHILDREN = "children";
	private static final String PARENT = "parent";
	private static final String NAME = "name";
	private static final String INDEX = "index";
	private static final String NULL_PLACEHOLDER = "EMPTY_VALUE";
	
	private List<Object[]> data;
	private String[] headers;
	
	public ClustergramFormatter() {
		this.data = new ArrayList<Object[]>(100);
	}
	
	public ClustergramFormatter(List<Object[]> data, String[] headers) {
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
		List<String> yCategory = (List<String>) this.optionsMap.get(Y_CATEGORY_KEY);
		String heat = (String) this.optionsMap.get(HEAT_KEY);
		return getClustergramData(this.data, this.headers, xCategory, yCategory, heat);
	}
	
	private Map<String, Object> getClustergramData(List<Object[]> data, String[] headers, List<String> xCategory, List<String> yCategory, String heat) {
		// need to have 3 maps
		// 1st map is for the x-axis dendrogram
		// 2nd map is for the y-axis dendrogram
		// 3rd map is for the grid
		
		LinkedHashMap<Object, Object> x_map = new LinkedHashMap<Object, Object>();
		x_map.put(NAME, "root");
		x_map.put(PARENT, "null");
		x_map.put(CHILDREN, new LinkedHashSet<Map<Object, Object>>());
		
		LinkedHashMap<Object, Object> y_map = new LinkedHashMap<Object, Object>();
		y_map.put(NAME, "root");
		y_map.put(PARENT, "null");
		y_map.put(CHILDREN, new LinkedHashSet<Map<Object, Object>>());
		
		// loop through to get the indices of the datarow we care about for the various components
		int num_x_components = xCategory.size();
		List<Integer> x_indices = new Vector<Integer>(num_x_components);
		for(int i = 0; i < num_x_components; i++) {
			x_indices.add(ArrayUtilityMethods.arrayContainsValueAtIndex(headers, xCategory.get(i)));
		}
		
		int num_y_components = yCategory.size();
		List<Integer> y_indices = new Vector<Integer>(num_y_components);
		for(int i = 0; i < num_y_components; i++) {
			y_indices.add(ArrayUtilityMethods.arrayContainsValueAtIndex(headers, yCategory.get(i)));
		}
		
		// now that we have the indices
		// loop through the data
		// and construct everything
		for(Object[] dataRow : data) {
			// generate the map for the x axis
			generateParentChildMap(x_map, dataRow, x_indices, num_x_components);
			
			// second, get the y_map
			generateParentChildMap(y_map, dataRow, y_indices, num_y_components);
		}
		
		// now we need to loop through and calculate the indices for each each leaf
		addPositionsToMap(x_map);
		addPositionsToMap(y_map);

		int heat_index = ArrayUtilityMethods.arrayContainsValueAtIndex(headers, heat);

		List<Map<Object, Object>> grid_data = new ArrayList<Map<Object, Object>>(data.size());
		LinkedHashSet<Map<Object, Object>> xChildrenSet = (LinkedHashSet<Map<Object, Object>>) x_map.get(CHILDREN);
		LinkedHashSet<Map<Object, Object>> yChildrenSet = (LinkedHashSet<Map<Object, Object>>) y_map.get(CHILDREN);
		// now loop through and make the grid map
		for(Object[] dataRow : data) {
			addRowToMainMap(grid_data, dataRow, x_indices, y_indices, xChildrenSet, yChildrenSet, heat_index);
		}
		
		// return all maps
		Map<String, Object> retObj = new HashMap<String, Object>();
		retObj.put("leftTree", y_map);
		retObj.put("topTree", x_map);
		retObj.put("gridData", grid_data);

		return retObj;
	}
	
	/**
	 * Add a row of data into the main grid
	 * This will use the indices added within each x & y map to get the positions
	 * @param grid_data
	 * @param dataRow
	 * @param x_map
	 * @param x_indices
	 * @param y_map
	 * @param y_indices
	 * @param heat_index 
	 */
	private void addRowToMainMap(List<Map<Object, Object>> grid_data, 
			Object[] dataRow,
			List<Integer> x_indices, 
			List<Integer> y_indices,
			LinkedHashSet<Map<Object, Object>> xChildrenSet, 
			LinkedHashSet<Map<Object, Object>> yChildrenSet, 
			int heat_index) 
	{
		Map<Object, Object> dataRowMap = new HashMap<Object, Object>();
		
		// find x child
		LinkedHashMap xChild = getChildMap(dataRow, xChildrenSet, x_indices);
		dataRowMap.put("x_index", xChild.get(INDEX));
		dataRowMap.put("x_path", getFullPath(dataRow, x_indices));

		LinkedHashMap yChild = getChildMap(dataRow, yChildrenSet, y_indices);
		dataRowMap.put("y_index", yChild.get(INDEX));
		dataRowMap.put("y_path", getFullPath(dataRow, y_indices));

		Object value = dataRow[heat_index];
		if(value == null) {
			value = NULL_PLACEHOLDER;
		}
		dataRowMap.put("value", value);
		
		grid_data.add(dataRowMap);
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
		// so for each child, we just need to recurisvely go down to find the children set
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
		return "CLUSTERGRAM";
	}


}
