package prerna.ui.components.playsheets.datamakers;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Constants;

public class MathTransformation extends AbstractTransformation {

	private static final Logger LOGGER = LogManager.getLogger(MathTransformation.class.getName());
	public static final String METHOD_NAME = "math";
	public static final String UNDO_METHOD_NAME = "removeColumn";
	public static final String GROUPBY_COLUMNS = "groupByColumns"; //TODO : change this to be join columns
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
		LOGGER.info("Math is indpendent of data maker components");
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
		List<String> groupByCols = (List<String>)this.props.get(GROUPBY_COLUMNS);		
		
		//function map contains
		//	1.	function to perform (math, min, etc.) but should eventually contain the groovy script?
		//	2.  the columnHeader(s) to operate on
		//	3.  the name of the new column (after creating, should be sent from the front end)
		Map<String, Object> functionMap =  (Map<String, Object>) this.props.get(MATH_MAP);
		
		// determines if its a singlecolumn, or if columns are the same
		//determine whether to do a single column group by or multi column group by
		//if groupByCols.length is 1 or length is 2 and those two columns are equal do single column
//		boolean singleColumn = groupByCols.size() == 1 || (groupByCols.size() == 2 && groupByCols.get(0).equals(groupByCols.get(1)));

		//create the names for the new columns that will be added to the data maker
		functionMap = createColumnNamesForColumnGrouping(groupByCols, functionMap, dm.getColumnHeaders());
		
		
		//create a routine which will do the group by and add the values to the tinker graph
		Map<String, Object> map = null;
//		for(String key : functionMap.keySet()){
//			TinkerFrameStatRoutine routine = new TinkerFrameStatRoutine();
//			map = (Map<String, Object>)functionMap.get(key);
//			if(!map.containsKey("exists")){
//				map.put("GroupBy", groupByCols);
//				routine.setSelectedOptions(map);
//				
////				dm.performAnalyticTransformation(routine);
//			}
//		}
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
	 * 
	 * @param functionMap		a hashtable that describes what this function will do
	 * @param columnHeader		a string which describes the relevant column to look at 	
	 * @return					
	 */
	public static Map<String, Object>  createColumnNamesForColumnGrouping(List<String> columnHeaders, Map<String, Object> functionMap, String[] tableHeaders) {
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
			String origNewName = newName;
			
			//Check if name exists already within the table, if so append a counter for uniqueness
			boolean nameExists = true;
			int counter = 1;
//			while(nameExists) {
				if(ArrayUtilityMethods.arrayContainsValue(tableHeaders, newName)) {
//					newName = origNewName+counter;//
					map.put("exists", "exists");
				}
//				} else {
//					nameExists = false;
//				}
//			}
			
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
			if(!map.containsKey("exists")){
				addedCols.add(map.get("calcName"));
			}
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
			LOGGER.error(Constants.STACKTRACE, e);
		} catch (IllegalAccessException e) {
			LOGGER.error(Constants.STACKTRACE, e);
		} catch (IllegalArgumentException e) {
			LOGGER.error(Constants.STACKTRACE, e);
		} catch (InvocationTargetException e) {
			LOGGER.error(Constants.STACKTRACE, e);
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
