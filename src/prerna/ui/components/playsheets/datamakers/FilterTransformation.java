package prerna.ui.components.playsheets.datamakers;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.QueryStruct;
import prerna.ds.h2.H2Frame;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.util.Utility;

public class FilterTransformation extends AbstractTransformation {

	private static final Logger LOGGER = LogManager.getLogger(FilterTransformation.class.getName());
	public static final String METHOD_NAME = "filter";						// name of the method in all data makers to perform a filter
	public static final String UNDO_METHOD_NAME = "unfilter";				// name of the method in all data makers to perform undo filtering
	public static final String COLUMN_HEADER_KEY = "colHeader";				// key in properties map for the type to apply the filter for
	public static final String VALUES_KEY = "values";						// key in properties map for the list of values to filter
	public static final String VISIBLE_VALUES_KEY = "valueSet";				// key in properties map for the list of values to display in drop down

	private DataMakerComponent dmc;
	private Boolean preTrans;
	private IDataMaker dm;

	@Override
	public void setDataMakers(IDataMaker... dm){
		this.dm = dm[0];
	}
	
	@Override
	public void setDataMakerComponent(DataMakerComponent dmc){
		this.dmc = dmc;
	}
	
	@Override
	public void setTransformationType(Boolean preTransformation){
		this.preTrans = preTransformation;
	}

	@Override
	public void runMethod() {
		String colHeader = this.props.get(COLUMN_HEADER_KEY) + "";
		List<Object> values = ((List<Object>) this.props.get(VALUES_KEY));

		if (values == null) {
			LOGGER.info("VALUES FOR THIS FILTER HAS NOT BEEN SET.... THIS IS MOST LIKELY A FILTER PAIRED WITH A JOIN.... GRABBING VALUES FROM DATAMAKER");
			if (dm instanceof ITableDataFrame) {
				SelectQueryStruct qs2 = new SelectQueryStruct();
				Iterator<IHeadersDataRow> uniqIterator = null;
				if (dm instanceof H2Frame) {
					qs2.addSelector(((ITableDataFrame) dm).getName(), props.get(COLUMN_HEADER_KEY).toString());
					uniqIterator = ((ITableDataFrame) dm).query(qs2);
				} else {
					// tinker
					qs2.addSelector(props.get(COLUMN_HEADER_KEY).toString(), QueryStruct.PRIM_KEY_PLACEHOLDER);
					uniqIterator = ((ITableDataFrame) dm).query(qs2);
				}
				values = new Vector<Object>();
				while (uniqIterator.hasNext()) {
					values.add(uniqIterator.next());
				}
			}
		}
        
		// if this is a pre-transformation
		if (preTrans) {
			// if there is metamodel data, add this as a filter and let query builder do its thing
			QueryStruct builderData = this.dmc.getQueryStruct();
			if (builderData != null) {
				addFilterToComponentData(colHeader, new ArrayList<>(values), builderData);
			} else {
				// there is no metamodel data
				// need to fill the query with the selected value
				// this doesn't allow for multiselect from the UI
				String query = this.dmc.getQuery();
				query = Utility.normalizeParam(query);
				Map<String, List<Object>> paramHash = new Hashtable<String, List<Object>>();
				paramHash.put(colHeader, new ArrayList<>(values));
				query = Utility.fillParam(query, paramHash);
				this.dmc.setQuery(query);
			}
		}
		// if it is post trans
		// we need to call filter by reflection on the data maker
		else {
			runFilterMethod(colHeader, new ArrayList<>(values));
		}
	}


	
	/**
	 * Appends the filtering into the metamodel data within the component
	 * Used when the filtering transformation is a preTransformation
	 * @param colHeader						The name of type to filter
	 * @param values						The list of values for the filter
	 * @param metamodelData					The metamodel data
	 */
	private void addFilterToComponentData(String colHeader, List<Object> values, QueryStruct builderData){
//        Map<String, List<Object>> stringMap;
//        if(builderData.getFilterData() != null && !builderData.getFilterData().isEmpty()) {
//               stringMap = builderData.getFilterData();
//        } else {
//               stringMap = new HashMap<String, List<Object>>();
//        }
//        stringMap.put(colHeader, values);
//        builderData.setFilterData(stringMap);
		builderData.addFilter(colHeader, "=", values);
	}
		
	/**
	 * Runs the postTransformation filtering routine
	 * @param colHeader					The name of the type to filter
	 * @param values					The list of values to filter
	 */
	private void runFilterMethod(String colHeader, List<Object> values){
		try {
//			method = dm.getClass().getMethod(METHOD_NAME, String.class, List.class);
//			LOGGER.info("Successfully got method : " + METHOD_NAME);
//			
//			method.invoke(dm, colHeader, values);
//			LOGGER.info("Successfully invoked method : " + METHOD_NAME);
			filterColumn(dm, colHeader, values);
		} catch (NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		return;
	}
	
	public static void filterColumn(IDataMaker dm, String concept, List<Object> filterValuesArr) throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		
		List<Object> values = new ArrayList<Object>(filterValuesArr.size());
		//if column is numeric convert to double
		Method isNumericMethod = dm.getClass().getMethod("isNumeric", String.class);
		if((boolean) isNumericMethod.invoke(dm, concept)) {
			for(Object o: filterValuesArr) {
				try {
					values.add(Double.parseDouble(o.toString()));
				} catch(Exception e) {
					values.add(o);
				}
			}
		}
		else { 
			values.addAll(filterValuesArr);
//			for(Object o: filterValuesArr) {
//				values.add(Utility.getInstanceName(o+""));
//			}
		}
		
		//get filter and unfilter methods from data maker
		Method filterMethod = dm.getClass().getMethod(METHOD_NAME, String.class, List.class);
		filterMethod.invoke(dm, concept, values);

		LOGGER.info("Filtered column: "+concept);
	}

	@Override
	public Map<String, Object> getProperties() {
		props.put(TYPE, METHOD_NAME);
		return this.props;
	}

	@Override
	public void undoTransformation() {
		String colHeader = this.props.get(COLUMN_HEADER_KEY) +"";
//		Set<Object> values = ((Stack<Set<Object>>) this.props.get(VISIBLE_VALUES_KEY)).firstElement();
		
		Method method = null;
		try {
			method = dm.getClass().getMethod(UNDO_METHOD_NAME, String.class);
			LOGGER.info("Successfully got method : " + UNDO_METHOD_NAME);
			
			method.invoke(dm, colHeader);
			LOGGER.info("Successfully invoked method : " + UNDO_METHOD_NAME);

		} catch (NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		return;
	}

	@Override
	/**
	 * make a copy that can be saved in the insight
	 */
	public FilterTransformation copy() {
		
		FilterTransformation copy = new FilterTransformation();
		copy.setDataMakerComponent(dmc);
		copy.setDataMakers(dm);
		copy.setId(id);
		
		if(props != null) {
			Gson gson = new GsonBuilder().disableHtmlEscaping().serializeSpecialFloatingPointValues().setPrettyPrinting().create();
			String propCopy = gson.toJson(props);
			Map<String, Object> newProps = gson.fromJson(propCopy, new TypeToken<Map<String, Object>>() {}.getType());
			copy.setProperties(newProps);
		}
		
		copy.setTransformationType(preTrans);
		
		return copy;
	}	
}
