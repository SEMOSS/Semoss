package prerna.ds.H2;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.IMatcher;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.AbstractTableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.ds.TinkerMetaData2;
import prerna.ds.TinkerMetaHelper;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IScriptReactor;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.rdf.query.builder.IQueryInterpreter;
import prerna.rdf.query.builder.SQLInterpreter;
import prerna.sablecc.H2ImportDataReactor;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;
import prerna.ui.components.playsheets.datamakers.JoinTransformation;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Constants;
import prerna.util.Utility;

public class TinkerH2Frame extends AbstractTableDataFrame {

	private static final Logger LOGGER = LogManager.getLogger(TinkerH2Frame.class.getName());

	H2Builder builder;
	//TODO: need to keep its own once it is no longer extending from TinkerFrame!!!!!
//	IMetaData metaData;
	
	//maps column names to h2 column names
//	public Map<String, String> H2HeaderMap;
	
	//map excel sheets to h2 tables
//	Map<String, String> tableMap;
	
	//map excel sheets to columns in those sheets
//	Map<String, List<String>> columnMap;
	
	//defined relations in the excel
//	List<String> relations;
	
	//map excel sheets to types
//	Map<String, List<String>> typeMap;
	
//	IMetaData metaData;
//	String[] headerNames;
	
	IQueryInterpreter interp = new SQLInterpreter();
	
	public TinkerH2Frame(String[] headers) {
		this.headerNames = headers;
		builder = new H2Builder();
//		builder.create(headers);
		this.metaData = new TinkerMetaData2();
//		H2HeaderMap = new HashMap<String, String>();
//		updateHeaderMap();
	}
	
	public TinkerH2Frame() {
		builder = new H2Builder();
		this.metaData = new TinkerMetaData2();
//		H2HeaderMap = new HashMap<String, String>();
	}
	
	/*************************** AGGREGATION METHODS *************************/
	

    
	@Override
	public void addRow(Object[] rowCleanData, Object[] rowRawData) {
		addRow(rowCleanData, getColumnHeaders());
	}

    @Override
    public void addRow(Object[] cleanCells, Object[] rawCells, String[] headers) {
    	this.addRow(cleanCells, headers);
    }
	
    private void addRow(Object[] cells, String[] headers) {
        String tableName = getTableNameForUniqueColumn(headers[0]);
        // TODO: differences between the tinker meta and the flat meta stored in the data frame
        // TODO: results in us being unable to get the table name
        if(tableName == null) {
        	tableName = builder.tableName;
        }
        String[] types = new String[headers.length];
        for(int i = 0; i < types.length; i++) {
              types[i] = this.metaData.getDataType(headers[i]);
              // need to stringify everything
              cells[i] = cells[i] + "";
        }
        String[] stringArray = Arrays.copyOf(cells, cells.length, String[].class);
        		
        //get table for headers
        this.addRow2(tableName, stringArray, headers, types);
    }
    
    //need to make this private if we are going with single table h2
	public void addRow2(String tableName, String[] cells, String[] headers, String[] types) {
        String[] headerValues = new String[headers.length];
		for(int j = 0; j < headers.length; j++) {
			headerValues[j] = getValueForUniqueName(headers[j]);
		}
		
		this.builder.tableName = tableName;
		this.builder.addRow(tableName, cells, headerValues, types);
	}
	
	//TODO : this won't with main column table
	public String getTableNameForUniqueColumn(String uniqueName) {
		return this.metaData.getParentValueOfUniqueNode(uniqueName);
	}
	
	/************************** END AGGREGATION METHODS **********************/
	
	
	@Override
	public void processDataMakerComponent(DataMakerComponent component) {
		long startTime = System.currentTimeMillis();
        LOGGER.info("Processing Component..................................");
        
        
        List<ISEMOSSTransformation>  preTrans = component.getPreTrans();
        List<Map<String,String>> joinColList= new ArrayList<Map<String,String>> ();
        for(ISEMOSSTransformation transformation: preTrans){
     	   if(transformation instanceof JoinTransformation){
     		   Map<String, String> joinMap = new HashMap<String,String>();
     		  String joinCol1 = (String) ((JoinTransformation)transformation).getProperties().get(JoinTransformation.COLUMN_ONE_KEY);
     		  String joinCol2 = (String) ((JoinTransformation)transformation).getProperties().get(JoinTransformation.COLUMN_TWO_KEY);
     		  joinMap.put(joinCol2, joinCol1); // physical in query struct ----> logical in existing data maker
     		  joinColList.add(joinMap);
     	   }  
        }
        
        
        processPreTransformations(component, component.getPreTrans());
        long time1 = System.currentTimeMillis();
        LOGGER.info("	Processed Pretransformations: " +(time1 - startTime)+" ms");
        
        IEngine engine = component.getEngine();
        // automatically created the query if stored as metamodel
        // fills the query with selected params if required
        // params set in insightcreatrunner
        String query = component.fillQuery();
        
        String[] displayNames = null;
        if(query.trim().toUpperCase().startsWith("CONSTRUCT")){
//     	   TinkerGraphDataModel tgdm = new TinkerGraphDataModel();
//     	   tgdm.fillModel(query, engine, this);
        }
        
        else{
     	   ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
            //if component has data from which we can construct a meta model then construct it and merge it
            boolean hasMetaModel = component.getQueryStruct() != null;
            if(hasMetaModel) {
         	   String[] headers = getH2Headers();
         	   Map<String, Set<String>> edgeHash = component.getQueryStruct().getReturnConnectionsHash();
         	  TinkerMetaHelper.mergeQSEdgeHash(this.metaData, edgeHash, engine, joinColList);
         	   
         	   //send in new columns, not all
         	   builder.processWrapper(wrapper, headers);
         	   
            } 
            
            //else default to primary key tinker graph
            else {
                displayNames = wrapper.getDisplayVariables();
                Map<String, Set<String>> edgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(displayNames);
                TinkerMetaHelper.mergeEdgeHash(this.metaData, edgeHash, getNode2ValueHash(edgeHash));
         	   while(wrapper.hasNext()){
         		   this.addRow(wrapper.next());
         	   }
            }
        }
 	   List<String> fullNames = this.metaData.getColumnNames();
 	   this.headerNames = fullNames.toArray(new String[fullNames.size()]);

        long time2 = System.currentTimeMillis();
        LOGGER.info("	Processed Wrapper: " +(time2 - time1)+" ms");
        
        processPostTransformations(component, component.getPostTrans());
        processActions(component, component.getActions());

        long time4 = System.currentTimeMillis();
        LOGGER.info("Component Processed: " +(time4 - startTime)+" ms");
	}
	
	
	/****************************** FILTER METHODS **********************************************/
	
	/**
	 * String columnHeader - the column on which to filter on
	 * filterValues - the values that will remain in the 
	 */
	@Override
	public void filter(String columnHeader, List<Object> filterValues) {
		if(filterValues != null && filterValues.size() > 0) {
			this.metaData.setFiltered(columnHeader, true);
			columnHeader = this.metaData.getValueForUniqueName(columnHeader);
			builder.setFilters(columnHeader, filterValues, H2Builder.Comparator.EQUAL);
		}
	}

	@Override
	public void filter(String columnHeader, List<Object> filterValues, String comparator) {
		if(filterValues != null && filterValues.size() > 0) {
			
			if(comparator.equals("=")) {
				builder.addFilters(columnHeader, filterValues, H2Builder.Comparator.EQUAL);
			} else if(comparator.equals("!=")) { 
				builder.addFilters(columnHeader, filterValues, H2Builder.Comparator.NOT_EQUAL);
			} else if(comparator.equals("<")) {
				if(isNumeric(columnHeader)) {
					builder.addFilters(columnHeader, filterValues, H2Builder.Comparator.LESS_THAN);
				} else {
					throw new IllegalArgumentException(columnHeader + " is not a numeric column, cannot use operator " + comparator);
				}
			} else if(comparator.equals(">")) {
				if(isNumeric(columnHeader)) {
					builder.addFilters(columnHeader, filterValues, H2Builder.Comparator.GREATER_THAN);
				} else {
					throw new IllegalArgumentException(columnHeader + " is not a numeric column, cannot use operator " + comparator);
				}
			} else if(comparator.equals("<=")) {
				if(isNumeric(columnHeader)) {
					builder.addFilters(columnHeader, filterValues, H2Builder.Comparator.LESS_THAN_EQUAL);
				} else {
					throw new IllegalArgumentException(columnHeader + " is not a numeric column, cannot use operator " + comparator);
				}
			} else if(comparator.equals(">=")) {
				if(isNumeric(columnHeader)) {
					builder.addFilters(columnHeader, filterValues, H2Builder.Comparator.GREATER_THAN_EQUAL);
				} else {
					throw new IllegalArgumentException(columnHeader + " is not a numeric column, cannot use operator " + comparator);
				}
			} else {
				//comparator not recognized...do equal by default? or do nothing? or throw error?
			}
		}
	}
	
	@Override
	public void unfilter(String columnHeader) {
		this.metaData.setFiltered(columnHeader, false);
		columnHeader = this.metaData.getValueForUniqueName(columnHeader);
		builder.removeFilter(columnHeader);
	}

	@Override
	public void unfilter() {
		builder.clearFilters();
	}
	
	@Override
	/**
	 * This method returns the filter model for the graph in the form:
	 * 
	 * [
	 * 		{
	 * 			header_1 -> [UF_instance_01, UF_instance_02, ..., UF_instance_0N]
	 * 			header_2 -> [UF_instance_11, UF_instance_12, ..., UF_instance_1N]
	 * 			...
	 * 			header_M -> [UF_instance_M1, UF_instance_M2, ..., UF_instance_MN]
	 * 		}, 
	 * 
	 * 		{
	 * 			header_1 -> [F_instance_01, F_instance_02, ..., F_instance_0N]
	 * 			header_2 -> [F_instance_11, F_instance_12, ..., F_instance_1N]
	 * 			...
	 * 			header_M -> [F_instance_M1, F_instance_M2, ..., F_instance_MN]
	 * 		}	
	 * ]
	 * 
	 * the first object in the array is a Map<String, List<String>> where each header points to the list of UNFILTERED or VISIBLE values for that header
	 * the second object in the array is a Map<String, List<String>> where each header points to the list of FILTERED values for that header
	 */
	public Object[] getFilterModel() {

		
		Iterator<Object[]> iterator = this.iterator(true);
		
		List<String> selectors = this.getSelectors();
		int length = selectors.size();
		
		//initialize the objects
		Map<String, List<Object>> filteredValues = new HashMap<String, List<Object>>(length);
		Map<String, List<Object>> visibleValues = new HashMap<String, List<Object>>(length);
		
		//put instances into sets to remove duplicates
		Set<Object>[] columnSets = new HashSet[length];
		for(int i = 0; i < length; i++) {
			columnSets[i] = new HashSet<Object>(length);
		}
		
		while(iterator.hasNext()) {
			Object[] nextRow = iterator.next();
			for(int i = 0; i < length; i++) {
				columnSets[i].add(nextRow[i]);
			}
		}
		
		//put the visible collected values
		for(int i = 0; i < length; i++) {
			visibleValues.put(selectors.get(i), new ArrayList<Object>(columnSets[i]));
			filteredValues.put(selectors.get(i), new ArrayList<Object>());
		}
		
		Map<String, List<Object>> h2filteredValues = builder.getFilteredValues(getH2Selectors());
		for(String key : filteredValues.keySet()) {
			String h2key = H2Builder.cleanHeader(key);
			List<Object> values = h2filteredValues.get(h2key);
			if(values != null) {
				filteredValues.put(key, values);
			} else {
				filteredValues.put(key, new ArrayList<Object>());
			}
		}
		
		return new Object[]{visibleValues, filteredValues};
	}

	public Map<String, Object[]> getFilterTransformationValues() {
		Map<String, Object[]> retMap = new HashMap<String, Object[]>();
		// get meta nodes that are tagged as filtered
		Map<String, String> filters = this.metaData.getFilteredColumns();
		Map<String, List<Object>> filteredData = this.builder.filterHash;
		
		for(String name: filters.keySet()){
			
			//for each filtered column
			String h2Name = this.metaData.getValueForUniqueName(name);
			retMap.put(name, filteredData.get(h2Name).toArray());
		}
		
		
		return retMap;
	}
	
	/****************************** END FILTER METHODS ******************************************/
	
	
	@Override
	public Iterator<Object[]> iterator(boolean getRawData) {
//		List<String> h2selectors = new ArrayList<>();
//		for(String selector : getSelectors()) {
//			h2selectors.add(H2HeaderMap.get(selector));
//		}
//		QueryStruct struct = this.metaData.getQueryStruct(null);
//		for(String header : this.builder.filterHash.keySet()){
//			struct.addFilter(header, "=", this.builder.filterHash.get(header));
//		}
//		interp.setQueryStruct(struct);//
//		String query = interp.composeQuery();
//		return this.builder.buildIterator(query);
		return this.builder.buildIterator(getH2Selectors()); 
	}
	
	@Override
	public Iterator<Object[]> iterator(boolean getRawData, Map<String, Object> options) {
		// sort by
		String sortBy = (String)options.get(TinkerFrame.SORT_BY);
		String actualSortBy = null;
		
		List<String> selectors = (List<String>) options.get(TinkerFrame.SELECTORS);
		List<String> selectorValues = new Vector<String>();
		for(String name : selectors) {
			if(name.startsWith(TinkerFrame.PRIM_KEY)) {
				continue;
			} else {
				if(name.equals(sortBy)) {
					actualSortBy = this.metaData.getValueForUniqueName(name);
				}
				String uniqueName = this.metaData.getValueForUniqueName(name);
				if(uniqueName == null)
					uniqueName = name;
				selectorValues.add(uniqueName);
			}
		}
		options.put(TinkerFrame.SELECTORS, selectorValues);

		Map<Object, Object> temporalBindings = (Map<Object, Object>) options.get(TinkerFrame.TEMPORAL_BINDINGS); 
		// clean values always put into list so bifurcation in logic doesn't need to exist elsewhere
		Map<String, List<Object>> cleanTemporalBindings = new Hashtable<String, List<Object>>();
		if(temporalBindings != null) {
			for(Object key : temporalBindings.keySet()) {
				String cleanKey = this.metaData.getValueForUniqueName(key + "");
				
				Object val = temporalBindings.get(key);
				List<Object> cleanVal = new Vector<Object>();
				// if passed back a list
				if(val instanceof Collection) {
					Collection<? extends Object> collectionVal = (Collection<? extends Object>) val;
					for(Object valObj : collectionVal) {
						Object cleanObj = null;
						String strObj = valObj.toString().trim();
						String type = Utility.findTypes(strObj)[0] + "";
						if(type.equalsIgnoreCase("Date")) {
							cleanObj = Utility.getDate(strObj);
						} else if(type.equalsIgnoreCase("Double")) {
							cleanObj = Utility.getDouble(strObj);
						} else {
							cleanObj = Utility.cleanString(strObj, true, true, false);
						}
						((Vector) cleanVal).add(cleanObj);
					}
					cleanTemporalBindings.put(cleanKey, cleanVal);
				} else {
					// this means it is a single value
					Object cleanObj = null;
					String strObj = val.toString().trim();
					String type = Utility.findTypes(strObj)[0] + "";
					if(type.equalsIgnoreCase("Date")) {
						cleanObj = Utility.getDate(strObj);
					} else if(type.equalsIgnoreCase("Double")) {
						cleanObj = Utility.getDouble(strObj);
					} else {
						cleanObj = Utility.cleanString(strObj, true, true, false);
					}
					cleanVal.add(cleanObj);
					cleanTemporalBindings.put(cleanKey, cleanVal);
				}
			}
		}
		options.put(TinkerFrame.TEMPORAL_BINDINGS, cleanTemporalBindings);

//		if(selectors != null) {
//			List<String> h2selectors = new ArrayList<>();
//			for(String selector : selectors) {
//				h2selectors.add(H2HeaderMap.get(selector));
//			}
//			options.put(TinkerFrame.SELECTORS, h2selectors);
//		}
		
		if(actualSortBy != null) {
			options.put(TinkerFrame.SORT_BY, actualSortBy);
		}
		return builder.buildIterator(options);
	}
	
	public void applyGroupBy(String[] column, String newColumnName, String valueColumn, String mathType) {
//		column = H2HeaderMap.get(column);
//		valueColumn = H2HeaderMap.get(valueColumn);
//		newColumnName = H2HeaderMap.get(newColumnName);
		for(int i = 0; i < column.length; i++) {
			column[i] = this.metaData.getValueForUniqueName(column[i]);
		}
		valueColumn = this.metaData.getValueForUniqueName(valueColumn);
		newColumnName = this.metaData.getValueForUniqueName(newColumnName);
		builder.processGroupBy(column, newColumnName, valueColumn, mathType, getH2Headers());
	}
	
	@Override
	public int getNumRows() {
		Iterator<Object[]> iterator = this.iterator(false);
		int count = 0;
		while(iterator.hasNext()) {
			count++;
			iterator.next();
		}
		return count;
	}
	
	@Override
	public Object[] getColumn(String columnHeader) {
//		columnHeader = H2HeaderMap.get(columnHeader);
		columnHeader = this.metaData.getValueForUniqueName(columnHeader);
		Object[] array = builder.getColumn(columnHeader, false);
		return array;
	}
	
	@Override
	public Integer getUniqueInstanceCount(String columnHeader) {
//		columnHeader = H2HeaderMap.get(columnHeader);
		columnHeader = this.metaData.getValueForUniqueName(columnHeader);
		return builder.getColumn(columnHeader, true).length;
	}
	
	@Override
	public Double getMin(String columnHeader) {
//		columnHeader = H2HeaderMap.get(columnHeader);
		columnHeader = this.metaData.getValueForUniqueName(columnHeader);
		return builder.getStat(columnHeader, "MIN");
	}
	
	@Override
	public Double getMax(String columnHeader) {
//		columnHeader = H2HeaderMap.get(columnHeader);
		columnHeader = this.metaData.getValueForUniqueName(columnHeader);
		return builder.getStat(columnHeader, "MAX");
	}
	
	@Override 
	public boolean isNumeric(String columnHeader) {
		String dataType = this.metaData.getDataType(columnHeader);
		return dataType.equalsIgnoreCase("NUMBER");
	}
	
	@Override
	public Iterator<List<Object[]>> scaledUniqueIterator(String columnHeader, boolean getRawData) {
		columnHeader = this.metaData.getValueForUniqueName(columnHeader);
		Map<String, String> m = this.getH2HeadersAndTypes();
		String tableName = this.getTableNameForUniqueColumn(columnHeader);
		ScaledUniqueH2FrameIterator iterator = new ScaledUniqueH2FrameIterator(columnHeader, getRawData, tableName, builder, getMax(), getMin(), m, getH2Selectors());
		//set the types here
		return iterator;
	}
	
	@Override
	public Double[] getColumnAsNumeric(String columnHeader) {
		if(isNumeric(columnHeader)) {
			columnHeader = this.metaData.getValueForUniqueName(columnHeader);
			Object[] array = builder.getColumn(columnHeader, false);
			
			List<Double> numericCol = new ArrayList<Double>();
			Iterator<Object> it = Arrays.asList(array).iterator();
			while(it.hasNext()) {
				Object row = it.next();
				try {
					Double dval = ((Number) row).doubleValue();
					numericCol.add(dval);
				} catch (NumberFormatException e) {
					
				}
			}
			
			return numericCol.toArray(new Double[]{});
		}
		
		return null;
	}
	
	@Override
	public void addRelationship(Map<String, Object> cleanRow, Map<String, Object> rawRow) {
		
		//if the sets contain keys not in header names, remove them
		Set<String> keySet = cleanRow.keySet();
		Map<String, Object> adjustedCleanRow = new HashMap<String, Object>(keySet.size());
		for(String key : keySet) {
			if(ArrayUtilityMethods.arrayContainsValue(headerNames, key)) {
				adjustedCleanRow.put(key, cleanRow.get(key));
			}
		}
		cleanRow = adjustedCleanRow;
		
		int size = cleanRow.keySet().size();
		Object[] values = new Object[size];
		String[] columnHeaders = cleanRow.keySet().toArray(new String[]{});
		

		Arrays.sort(columnHeaders, new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				int firstIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(headerNames, o1);
				int secondIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(headerNames, o2);
				if(firstIndex < secondIndex) return -1;
				if(firstIndex == secondIndex) return 0;
				else return 1;
			}
			
		});
		
		for(int i = 0; i < columnHeaders.length; i++) {
			values[i] = cleanRow.get(columnHeaders[i]);
		}
		
		for(int i = 0; i < columnHeaders.length; i++) {
			columnHeaders[i] = H2Builder.cleanHeader(columnHeaders[i]);
		}
		builder.updateTable(getH2Headers(), values, columnHeaders);

	}
	
	@Override
	public void removeColumn(String columnHeader) {
		if(!ArrayUtilityMethods.arrayContainsValue(this.headerNames, columnHeader)) {
			return;
		}
		
//		builder.dropColumn(H2HeaderMap.get(columnHeader));
		builder.dropColumn(this.metaData.getValueForUniqueName(columnHeader));
		this.metaData.dropVertex(columnHeader);
		
		String[] newHeaders = new String[this.headerNames.length-1];
		int newHeaderIdx = 0;
		for(int i = 0; i < this.headerNames.length; i++){
			String name = this.headerNames[i];
			if(!name.equals(columnHeader)){
				newHeaders[newHeaderIdx] = name;
				newHeaderIdx ++;
			}
		}
		this.headerNames = newHeaders;
//		updateHeaderMap();
	}
	
	@Override
	public Iterator<Object> uniqueValueIterator(String columnHeader, boolean getRawData, boolean iterateAll) {

//		Map<String, Object> options = new HashMap<String, Object>();
//		options.put(DE_DUP, true);
//		
//		List<String> selectors = new ArrayList<String>();
//		selectors.add(columnHeader);
//		options.put(SELECTORS, selectors);
//		columnHeader = H2HeaderMap.get(columnHeader);
		columnHeader = this.metaData.getValueForUniqueName(columnHeader);
		return Arrays.asList(builder.getColumn(columnHeader, true)).iterator();
	}
	
	@Override
	public void save(String fileName) {
		this.metaData.save(fileName.substring(0, fileName.lastIndexOf(".")));
//		fileName = fileName.substring(0, fileName.length() - 3) + ".gz";
		builder.save(fileName, getH2Headers());
	}
	
	
	public TinkerH2Frame open(String fileName) {
		TinkerH2Frame tf = new TinkerH2Frame();
		tf.metaData.open(fileName.substring(0, fileName.lastIndexOf(".")));
		tf.builder = H2Builder.open(fileName);
		List<String> primKeys = tf.metaData.getPrimKeys();
		if(primKeys.size() == 1){
			tf.metaData.setVertexValue(primKeys.get(0), tf.builder.tableName);
		}

 	   List<String> fullNames = tf.metaData.getColumnNames();
	   tf.headerNames = fullNames.toArray(new String[fullNames.size()]);
	   
	   String[] types = new String[tf.headerNames.length];
	   for(int i = 0; i < tf.headerNames.length; i++) {
		   String type = tf.metaData.getDataType(tf.headerNames[i]);
		   if(type.equalsIgnoreCase("Number")) { 
			   types[i] = "Double";
		   } else if(type.equalsIgnoreCase("String")) {
			   types[i] = "Varchar(800)";
		   } else {
			   types[i] = "Date";
		   }
	   }
	   
//	   tf.builder.types = types;
		return tf;
	}
	
	protected void updateH2PhysicalNames() {
		
	}
	
	public List<String> getSelectors() {
		if(headerNames == null) return new ArrayList<String>();
		List<String> selectors = new ArrayList<String>();
		for(int i = 0; i < headerNames.length; i++) {
			if(!columnsToSkip.contains(headerNames[i])) {
				selectors.add(headerNames[i]);
			}
		}
		return selectors;
	}
	
	private List<String> getH2Selectors() {
		List<String> selectors = getSelectors();
		List<String> h2selectors = new ArrayList<>(selectors.size());
		for(int i = 0; i < selectors.size(); i++) {
			h2selectors.add(this.metaData.getValueForUniqueName(selectors.get(i)));
//			h2selectors.add(H2HeaderMap.get(selectors.get(i)));
		}
		return h2selectors;
	}
	
	private String[] getH2Headers() {
		if(headerNames == null) return null;
		String[] h2Headers = new String[headerNames.length];
		for(int i = 0; i < headerNames.length; i++) {
//			h2Headers[i] = H2HeaderMap.get(headerNames[i]);
			h2Headers[i] = this.metaData.getValueForUniqueName(headerNames[i]);
		}
		return h2Headers;
	}
	
//	private String[] getH2Types() {
//		if(headerNames == null) return null;
//		String[] h2Types = new String[headerNames.length];
//		for(int i = 0; i < headerNames.length; i++) {
//			h2Types[i] = this.metaData.getDBDataType(headerNames[i]);
//		}
//		return h2Types;
//	}
	
	private Map<String, String> getH2HeadersAndTypes() {
		
		if(headerNames == null) return null;
		Map<String, String> retMap = new HashMap<String, String>(headerNames.length);
		for(String header : headerNames) {
			String h2Header = this.metaData.getValueForUniqueName(header);
			String h2Type = this.metaData.getDataType(header);
			retMap.put(h2Header, h2Type);
		}
		return retMap;
	}
	
	//relationships in the form tableA.columnA.tableB.columnB
	public void setRelations(List<String> relations) {
		//use this to set the relationships gathered from the xl file helper
		Map<String, Set<String>> edgeHash = new HashMap<>();
		for(String relation : relations) {
			String[] relationComps = relation.split(".");
			String tableA = relationComps[0];
			String columnA = relationComps[1];
			String tableB = relationComps[2];
			String columnB = relationComps[3];
			
			String key = tableA+"__"+columnA;
			Set<String> valueSet = new HashSet<String>();
			String value = tableB+"__"+columnB;
			valueSet.add(value);
			
			if(edgeHash.containsKey(key) && edgeHash.get(key) != null) {
				edgeHash.get(key).addAll(valueSet);
			} else {
				edgeHash.put(key, valueSet);
			}
		}
		
		TinkerMetaHelper.mergeEdgeHash(this.metaData, edgeHash, getNode2ValueHash(edgeHash));
		
		
	}
	
	public void setMetaData(String tableName, String[] headers, String[] types) {
		
		if(headers.length != types.length) {
			throw new IllegalArgumentException("Number of headers and types not equal");
		}
			

		this.metaData.storeDataTypes(headers, types);
	}
	
	protected String getCleanHeader(String metaNodeName) {
		String metaNodeValue;
		if(metaNodeName.equals(TinkerFrame.PRIM_KEY)) {
			metaNodeValue = builder.getNewTableName();
		} else {
			metaNodeValue = H2Builder.cleanHeader(metaNodeName);
		}

		return metaNodeValue;
	}
	
	private Map<String, String> getNode2ValueHash(Map<String, Set<String>> edgeHash){
		Set<String> masterSet = new HashSet<String>();
		masterSet.addAll(edgeHash.keySet());
		Collection<Set<String>> valSet = edgeHash.values();
		for(Set<String> val : valSet){
			masterSet.addAll(val);
		}
		Map<String, String> trans = new HashMap<String,String>();
		for(String name : masterSet){
			if(name.startsWith(TinkerFrame.PRIM_KEY)) {
				trans.put(name, builder.getNewTableName());
			} else {
				trans.put(name, getCleanHeader(name));
			} 
		}
		return trans;
	}
	
	@Override
	public void connectTypes(String outType, String inType, Map<String, String> dataTypeMap) {

		Map<String, Set<String>> edgeHash = new HashMap<>();
		Set<String> set = new HashSet<>();
		set.add(inType);
		edgeHash.put(outType, set);
		mergeEdgeHash(edgeHash, dataTypeMap);

	}
	
	@Override
	public Map[] mergeQSEdgeHash(Map<String, Set<String>> edgeHash, IEngine engine, Vector<Map<String, String>> joinCols) {
		Map[] ret =  super.mergeQSEdgeHash(edgeHash, engine, joinCols);
		
		// alter table for new cols
		Set<String> headersSet = new LinkedHashSet<String>();
		for(String addHeader : edgeHash.keySet()) {
			if(addHeader.contains("__")) {
				headersSet.add(addHeader.split("__")[1]);
			} else {
				headersSet.add(addHeader);
			}
		}
		for(String header : edgeHash.keySet()) {
			Set<String> additionalHeaders = edgeHash.get(header);
			if(additionalHeaders != null) {
				for(String addHeader : additionalHeaders) {
					if(addHeader.contains("__")) {
						headersSet.add(addHeader.split("__")[1]);
					} else {
						headersSet.add(addHeader);
					}
				}
			}
		}
		String[] headers = headersSet.toArray(new String[]{});
		String[] types = new String[headers.length];
		for(int i = 0; i < types.length; i++) {
			types[i] = engine.getDataTypes(engine.getTransformedNodeName(Constants.DISPLAY_URI + headers[i], false));
			types[i] = Utility.getRawDataType(types[i].replace("TYPE:", ""));
		}
		builder.alterTableNewColumns(headers, types); 
		
		return ret;
	}
	
	@Override
	public void mergeEdgeHash(Map<String, Set<String>> primKeyEdgeHash, Map<String, String> dataTypeMap) {
		
		TinkerMetaHelper.mergeEdgeHash(this.metaData, primKeyEdgeHash, getNode2ValueHash(primKeyEdgeHash));
		
		if(dataTypeMap != null) {
			for(String key : dataTypeMap.keySet()) {
				String type = dataTypeMap.get(key);
				if(type == null) type = "STRING";
				this.metaData.storeDataType(key, type);
			}
		}
		
    	List<String> fullNames = this.metaData.getColumnNames();
    	this.headerNames = fullNames.toArray(new String[fullNames.size()]);
		String[] headers = this.headerNames;
		String[] cleanHeaders = new String[headers.length];
		String[] types = new String[headers.length];
		for(int i = 0; i < types.length; i++) {
			types[i] = Utility.getRawDataType(this.metaData.getDataType(headers[i]));
			cleanHeaders[i] = this.metaData.getValueForUniqueName(headers[i]);
		}
		builder.alterTableNewColumns(cleanHeaders, types);
		
	}

//	@Override
//	public void addMetaDataTypes(String[] headers, String[] types) {
//		this.metaData.storeDataTypes(headers, types);
//	}
	
	public static void main(String[] args) {
		
	}

	@Override
	public void join(ITableDataFrame table, String colNameInTable, String colNameInJoiningTable, double confidenceThreshold, IMatcher routine) {
		
	}

	public String getValueForUniqueName(String name) {
		return this.metaData.getValueForUniqueName(name);
	}
	
	@Override
	public void connectTypes(String[] joinCols, String newCol, Map<String, String> dataTypeMap) {
		connectTypes(joinCols[0], newCol, dataTypeMap); 
	}

	@Override
	public void addRelationship(Map<String, Object> rowCleanData, Map<String, Object> rowRawData, Map<String, Set<String>> edgeHash, Map<String, String> logicalToTypeMap) {
		addRelationship(rowCleanData, rowRawData);
	}
	
	@Override
	public void addRelationship(String[] headers, Object[] values, Object[] rawValues, Map<Integer, Set<Integer>> cardinality, Map<String, String> logicalToValMap) {
		for(int i = 0; i < headers.length; i++) {
			headers[i] = H2Builder.cleanHeader(headers[i]);
		}
		
		String[] currHeaders = getH2Headers();
		
		for(int i = 0; i < headers.length-1; i++) {
			String header1 = headers[i];
			Object value1 = values[i];
			for(int j = i+1; j < headers.length; j++) {
				String header2 = headers[j];
				Object value2 = values[j];

				int index1 = ArrayUtilityMethods.arrayContainsValueAtIndex(currHeaders, header1);
				int index2 = ArrayUtilityMethods.arrayContainsValueAtIndex(currHeaders, header2);

				if(index2 < index1) {
					headers[i] = header2;
					values[i] = value2;

					headers[j] = header1;
					values[j] = value1;
				}
			}
		}
		
		builder.updateTable(currHeaders, values, headers);
	}

	public String getJDBCURL() throws SQLException{
		String url = "";
		DatabaseMetaData md = builder.conn.getMetaData();
		url = md.getURL();
		return url;
	}
	
	public String getUserName() throws SQLException{
		String userName = "";
		DatabaseMetaData md = builder.conn.getMetaData();
		userName = md.getUserName();
		return userName;
	}

	@Override
	public void removeRelationship(Map<String, Object> cleanRow, Map<String, Object> rawRow) {
		
		Set<String> columnNames = cleanRow.keySet();
		String[] columns = new String[columnNames.size()];
		String[] values = new String[columnNames.size()];
		int i = 0;
		for(String column : cleanRow.keySet()) {
			String col = this.metaData.getValueForUniqueName(column);
			Object value = cleanRow.get(col);
			String val = Utility.cleanString(value.toString(), true, true, false);
			columns[i] = col;
			values[i] = val;
			i++;
		}
		builder.deleteRow(columns, values);
	}

	@Override
	public IScriptReactor getImportDataReactor() {
		return new H2ImportDataReactor(this);
	}
	
	public void processIterator(Iterator<IHeadersDataRow> iterator, String[] newHeaders, Map<String, String> logicalToValue) {
		
		String[] valueHeaders = new String[newHeaders.length];
		for(int i = 0; i < newHeaders.length; i++) {
			valueHeaders[i] = logicalToValue.get(newHeaders[i]);
		}
		String[] columnHeaders = getColumnHeaders();
		int length = columnHeaders.length;
		if(ArrayUtilityMethods.arrayContainsValueIgnoreCase(newHeaders, columnHeaders[length-1])) {
			length = length - 1;
		}
		
		String[] adjustedColHeaders = new String[length];
		for(int i = 0; i < length; i++) {
			adjustedColHeaders[i] = this.metaData.getValueForUniqueName(columnHeaders[i]);
		}
		this.builder.processIterator(iterator, adjustedColHeaders, valueHeaders);
	}
}
