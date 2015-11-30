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

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.ITableStatCounter;
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
		Object list = this.props.get(GROUPBY_COLUMNS);

		String[] groupByCols = null;
		if(list instanceof List){
			groupByCols = ((List<String>)list).toArray(new String[((List<String>)list).size()]);
		}	
		else{
			groupByCols = (String[]) list;
		}
		Map<String, Object> functionMap =  (Map<String, Object>) this.props.get(MATH_MAP);
		

		boolean singleColumn = groupByCols.length == 1 || (groupByCols.length == 2 && groupByCols[0].equals(groupByCols[1]));

		if(singleColumn) {
			functionMap = createColumnNamesForColumnGrouping(groupByCols[0], functionMap);
		} else {
			functionMap = createColumnNamesForColumnGrouping(groupByCols, functionMap);
		}
		
		String[] columnHeaders = dm.getColumnHeaders();
		Set<String> columnSet = new HashSet<String>();
		for(String key : functionMap.keySet()) {
			Map<String, String> map = (Map)functionMap.get(key);
			String name = map.get("calcName");
			columnSet.add(name);
		}
		
		for(String name : columnSet) {
			if(ArrayUtilityMethods.arrayContainsValue(columnHeaders, name)) {
				dm.removeColumn(name);
			}
		}
		
		//only one group by or two of the same
		if(singleColumn) {
			ITableStatCounter counter = new ITableStatCounter();
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
	
}
