package prerna.ui.components.playsheets.datamakers;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TableStatCounter;
import prerna.ds.MultiColumnTableStatCounter;
import prerna.util.ArrayUtilityMethods;

public class MathTransformation extends AbstractTransformation {

	private static final Logger LOGGER = LogManager.getLogger(MathTransformation.class.getName());
	public static final String METHOD_NAME = "math";
	public static final String UNDO_METHOD_NAME = "removeColumn";
	public static final String GROUPBY_COLUMNS = "groupByColumns";
	public static final String MATH_MAP = "mathMap";

	private ITableDataFrame dm;

	@Override
	public void setProperties(Map<String, Object> props) {
		this.props = props;
	}

	@Override
	public void setDataMakers(IDataMaker... dm){
		this.dm = (ITableDataFrame) dm[0];
	}
	
	@Override
	public void setDataMakerComponent(DataMakerComponent dmc){
		LOGGER.error("Math is indpendent of data maker components");
	}
	
	@Override
	public void setTransformationType(Boolean preTransformation){
		if(preTransformation){
			LOGGER.error("Cannot run math as pre-transformation");
		}
	}

	@Override
	public void runMethod() {
		
		// get the items from MathTransformation object
		Object list = this.props.get(GROUPBY_COLUMNS);

		String[] groupByCols = null;
		if(list instanceof List){
			groupByCols = ((List<String>)list).toArray(new String[((List<String>)list).size()]);
		}	
		else{
			groupByCols = (String[]) list;
		}
		Map<String, Object> functionMap =  (Map<String, Object>) this.props.get(MATH_MAP);
		
		// determines if its a singlecolumn, or if columns are the same
		//determine whether to do a single column group by or multi column group by
		//if groupByCols.length is 1 or length is 2 and those two columns are equal do single column
		boolean singleColumn = groupByCols.length == 1 || (groupByCols.length == 2 && groupByCols[0].equals(groupByCols[1]));

		//create the names for the new columns that will be added to the data maker
		if(singleColumn) {
			functionMap = createColumnNamesForColumnGrouping(groupByCols[0], functionMap);
		} else {
			functionMap = createColumnNamesForColumnGrouping(groupByCols, functionMap);
		}
		
		String[] columnHeaders = dm.getColumnHeaders();
		Set<String> columnSet = new HashSet<String>();
		// this makes sure the new column created doesn't already exist. it if does, remove it.
		for(String key : functionMap.keySet()) {
			Map<String, String> map = (Map)functionMap.get(key);
			String name = map.get("calcName");
			columnSet.add(name);
		}
		
		//delete any columns with the same name of the new columnheaders
		for(String name : columnSet) {
			if(ArrayUtilityMethods.arrayContainsValue(columnHeaders, name)) {
				dm.removeColumn(name);
			}
		}
		
		//only one group by or two of the same
		if(singleColumn) {
			TableStatCounter counter = new TableStatCounter();
			counter.addStatsToDataFrame(dm, groupByCols[0], functionMap);
		} else {
			MultiColumnTableStatCounter multiCounter = new MultiColumnTableStatCounter();
			multiCounter.addStatsToDataFrame(dm, groupByCols, functionMap);
		}
		return;
	}

	@Override
	public Map<String, Object> getProperties() {
		props.put(TYPE, METHOD_NAME);
		return this.props;
	}

	/**
	 * This method creates a new column name by combining the function name with the column header 
	 * its operated on.
	 * 
	 * TODO: add a new function that can take in a custom name for the new column
	 * 
	 * @param functionMap		a hashtable that describes what this function will do
	 * @param columnHeader		a string which describes the relevant column to look at 	
	 * @return					
	 */
	public static Map<String, Object> createColumnNamesForColumnGrouping(String columnHeader, Map<String, Object> functionMap) {
		
		for(String key : functionMap.keySet()) {
			
			Map<String, String> map = (Map<String, String>)functionMap.get(key);
			String name = map.get("name");
			String function = map.get("math");
			if(!name.equals(columnHeader)) {
				String newName = name+"_"+function+"_on_"+columnHeader;
				map.put("calcName", newName);
			}
		}
		
		return functionMap;
	}
	
	public static Map<String, Object>  createColumnNamesForColumnGrouping(String[] columnHeaders, Map<String, Object> functionMap) {
		String columnHeader = "";
		for(String c : columnHeaders) {
			columnHeader = columnHeader + c +"_and_";
		}
		
		columnHeader = columnHeader.substring(0, columnHeader.length() - 5);
		
		for(String key : functionMap.keySet()) {
			
			Map<String, String> map = (Map<String, String>)functionMap.get(key);
			String name = map.get("name");
			String function = map.get("math");
			
			String newName = name+"_"+function+"_on_"+columnHeader;
			map.put("calcName", newName);
		}
		
		return functionMap;
	}

	@Override
	public void undoTransformation() {
		List<String> addedCols = new ArrayList<String>();
		Map<String, Object> functionMap = ((Map<String, Object>) props.get(MATH_MAP));
		for(String key : functionMap.keySet()) {
			Map<String, String> map = (Map<String, String>)functionMap.get(key);
			addedCols.add(map.get("calcName"));
		}
		
		Method method = null;
		try {
			method = dm.getClass().getMethod(UNDO_METHOD_NAME, String.class);
			LOGGER.info("Successfully got method : " + UNDO_METHOD_NAME);
			
			// iterate from root to top for efficiency in removing connections
			for(int i = addedCols.size()-1; i >= 0; i--) {
				method.invoke(dm, addedCols.get(i));
				LOGGER.info("Successfully invoked method : " + UNDO_METHOD_NAME);
			}
		} catch (NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	/**
	 * copy method for saving tranformation
	 * 
	 * This transformation is required to be modified for after processing
	 */
	public MathTransformation copy() {
		return this;
//		
//		MathTransformation copy = new MathTransformation();
//		
//		copy.setDataMakers(this.dm);
//		copy.setId(this.id);
//
//		if(this.props != null) {
//			Gson gson = new GsonBuilder().disableHtmlEscaping().serializeSpecialFloatingPointValues().setPrettyPrinting().create();
//			String propCopy = gson.toJson(this.props);
//			Map<String, Object> newProps = gson.fromJson(propCopy, new TypeToken<Map<String, Object>>() {}.getType());
//			copy.setProperties(newProps);
//		}
//		
//		return copy;
}
	
}
