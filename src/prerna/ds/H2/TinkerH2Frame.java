package prerna.ds.H2;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Io.Builder;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import com.google.gson.Gson;

import prerna.ds.QueryStruct;
import prerna.ds.TinkerFrame;
import prerna.ds.TinkerMetaData2;
import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.rdf.query.builder.IQueryInterpreter;
import prerna.rdf.query.builder.SQLInterpreter;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;
import prerna.ui.components.playsheets.datamakers.JoinTransformation;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Constants;
import prerna.util.MyGraphIoRegistry;

public class TinkerH2Frame extends TinkerFrame {

	
	private static final Logger LOGGER = LogManager.getLogger(TinkerH2Frame.class.getName());
	private static final String TYPES = "Table_Types";
	
	H2Builder builder;
	//TODO: need to keep its own once it is no longer extending from TinkerFrame!!!!!
//	IMetaData metaData;
	
	//maps column names to h2 column names
//	public Map<String, String> H2HeaderMap;
	
	//map excel sheets to h2 tables
	Map<String, String> tableMap;
	
	//map excel sheets to columns in those sheets
	Map<String, List<String>> columnMap;
	
	//defined relations in the excel
	List<String> relations;
	
	//map excel sheets to types
	Map<String, List<String>> typeMap;
	
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
	
//	public void addRow(Map<String, Object> row) {
//		String[] newRow = new String[row.keySet().size()];
//		
//		int i = 0;
//		for(String key : row.keySet()) {
//			newRow[i] = (String)row.get(key);
//			i++;
//		}
//		
////		addRow(newRow);
//	}
	
	public void addRow2(String tableName, String[] cells, String[] headers, String[] types) {
		this.builder.tableName = tableName;
		this.builder.types = types;
		this.builder.addRow(cells, headers);
	}
	
	public void addRow(String tableName, String[] row) {
		
//		this.metaData.getQueryStruct();
//		//get alias headers and types from the tableName (sheet name)
//		//get h2 table name
//		
//		//get headers associated with tableName
//		//get types for each header
//		
//		
//		this.metaData.ge
//		Map<String, String> typeMap = ((TinkerMetaData2) table.getMetaData()).getNodeTypesForUniqueAlias();
//		
//		List<String> headers = columnMap.get(tableName);
//		List<String> types = typeMap.get(tableName);
//		headers = getH2Headers(headers);
//		tableName = tableMap.get(tableName);
//		builder.addRow(row, headers.toArray(new String[]{}), types.toArray(new String[]{}), tableName);
	}
	
	public void setTypes(String tableName, String[] types) {
		
	}
	
//	public void addRelationship(Map<String, Object> row) {
//		//update the builder
//	}
	
	private QueryStruct getQueryStruct() {
		
		
		return null;
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
         	   this.mergeQSEdgeHash(edgeHash, engine, joinColList);
         	   
         	   //send in new columns, not all
         	   builder.processWrapper(wrapper, headers);
         	   
            } 
            
            //else default to primary key tinker graph
            else {
                displayNames = wrapper.getDisplayVariables();
         	   this.mergeEdgeHash(this.createPrimKeyEdgeHash(displayNames));
         	   while(wrapper.hasNext()){
         		   this.addRow(wrapper.next());
         	   }
            }
        }
        g.variables().set(Constants.HEADER_NAMES, this.headerNames); // I dont know if i even need this moving forward.. but for now I will assume it is
        redoLevels(this.headerNames);

        long time2 = System.currentTimeMillis();
        LOGGER.info("	Processed Wrapper: " +(time2 - time1)+" ms");
        
        processPostTransformations(component, component.getPostTrans());
        processActions(component, component.getActions());

        long time4 = System.currentTimeMillis();
        LOGGER.info("Component Processed: " +(time4 - startTime)+" ms");
	}
	
	@Override
	public List<Object[]> getData() {
		Vector<Object[]> retVector = new Vector<>();
		Iterator<Object[]> iterator = this.iterator(false);
		while(iterator.hasNext()) {
			retVector.add(iterator.next());
		}
		return retVector;
	}
	
	@Override
	public List<Object[]> getRawData() {
		Vector<Object[]> retVector = new Vector<>();
		Iterator<Object[]> iterator = this.iterator(true);
		while(iterator.hasNext()) {
			retVector.add(iterator.next());
		}
		return retVector;
	}
	
	
	/****************************** FILTER METHODS **********************************************/
	
	/**
	 * String columnHeader - the column on which to filter on
	 * filterValues - the values that will remain in the 
	 */
	@Override
	public void filter(String columnHeader, List<Object> filterValues) {
		//filterValues is what to keep
//		columnHeader = H2HeaderMap.get(columnHeader);
		columnHeader = this.metaData.getValueForUniqueName(columnHeader);
		builder.setFilters(columnHeader, filterValues);
	}

	@Override
	public void unfilter(String columnHeader) {
//		columnHeader = H2HeaderMap.get(columnHeader);
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
			String h2key = cleanHeader(key);
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
//		GraphTraversal<Vertex, Vertex> metaGt = g.traversal().V().has(Constants.TYPE, META).has(Constants.FILTER, true);
//		while(metaGt.hasNext()){
//			Vertex metaV = metaGt.next();
//			String vertType = metaV.value(Constants.NAME);
//			GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(Constants.TYPE, Constants.FILTER).out(Constants.FILTER+edgeLabelDelimeter+vertType).has(Constants.TYPE, vertType);
//			List<String> vertsList = new Vector<String>();
//			while(gt.hasNext()){
//				vertsList.add(gt.next().value(Constants.VALUE));
//			}
//			retMap.put(vertType, vertsList.toArray());
//		}
		
		return retMap;
	}
	
	/****************************** END FILTER METHODS ******************************************/
	
	
	@Override
	public Iterator<Object[]> iterator(boolean getRawData) {
//		List<String> h2selectors = new ArrayList<>();
//		for(String selector : getSelectors()) {
//			h2selectors.add(H2HeaderMap.get(selector));
//		}
		QueryStruct struct = this.metaData.getQueryStruct(null);
		for(String header : this.builder.filterHash.keySet()){
			struct.addFilter(header, "=", this.builder.filterHash.get(header));
		}
		interp.setQueryStruct(struct);
		String query = interp.composeQuery();
		return this.builder.buildIterator(query);
	}
	
	@Override
	public Iterator<Object[]> iterator(boolean getRawData, Map<String, Object> options) {
		// sort by
		String sortBy = (String)options.get(TinkerFrame.SORT_BY);
		String actualSortBy = null;
		
		List<String> selectors = (List<String>) options.get(TinkerFrame.SELECTORS);
		List<String> selectorValues = new Vector<String>();
		for(String name : selectors) {
			if(name.startsWith(PRIM_KEY)) {
				continue;
			} else {
				if(name.equals(sortBy)) {
					actualSortBy = this.metaData.getValueForUniqueName(name);
				}
				selectorValues.add(this.metaData.getValueForUniqueName(name));
			}
		}
		options.put(TinkerFrame.SELECTORS, selectorValues);
		
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
	
	public void applyGroupBy(String column, String newColumnName, String valueColumn, String mathType) {
//		column = H2HeaderMap.get(column);
//		valueColumn = H2HeaderMap.get(valueColumn);
//		newColumnName = H2HeaderMap.get(newColumnName);
		
		column = this.metaData.getValueForUniqueName(column);
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
		String[] types = builder.getTypes();
		int index = ArrayUtilityMethods.arrayContainsValueAtIndex(headerNames, columnHeader);
		boolean isNum = types[index].equalsIgnoreCase("int") || types[index].equalsIgnoreCase("double");
		return isNum;
	}
	
	@Override
	public Iterator<List<Object[]>> scaledUniqueIterator(String columnHeader, boolean getRawData) {
//		columnHeader = H2HeaderMap.get(columnHeader);
		columnHeader = this.metaData.getValueForUniqueName(columnHeader);
		return new ScaledUniqueH2FrameIterator(columnHeader, getRawData, getH2Selectors(), builder, getMax(), getMin());
	}
	
	@Override
	public Double[] getColumnAsNumeric(String columnHeader) {
		if(isNumeric(columnHeader)) {
//			columnHeader = H2HeaderMap.get(columnHeader);
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
			columnHeaders[i] = cleanHeader(columnHeaders[i]);
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
		try {
			long startTime = System.currentTimeMillis();
			g.variables().set(Constants.HEADER_NAMES, headerNames);
			g.variables().set(TYPES, builder.types);
			
			// create special vertex to save the order of the headers
			Vertex specialVert = this.upsertVertex(ENVIRONMENT_VERTEX_KEY, ENVIRONMENT_VERTEX_KEY, ENVIRONMENT_VERTEX_KEY);
			
			Gson gson = new Gson();
			Map<String, Object> varMap = g.variables().asMap();
			for(String key : varMap.keySet()) {
				specialVert.property(key, gson.toJson(varMap.get(key)));
			}
			Builder<GryoIo> builder = IoCore.gryo();
			builder.graph(g);
			IoRegistry kryo = new MyGraphIoRegistry();;
			builder.registry(kryo);
			GryoIo yes = builder.create();
			yes.writeGraph(fileName.substring(0, fileName.length() - 3) + "_META.tg");
			
			long endTime = System.currentTimeMillis();
			LOGGER.info("Successfully saved TinkerFrame to file: "+fileName+ "("+(endTime - startTime)+" ms)");
			
			// now we need to remvoe the special vert after it is saved since the user might extend the viz even further
			// we dont want it to continue to show up
			specialVert.remove();
		} catch (IOException e) {
			e.printStackTrace();
		}
//		fileName = fileName.substring(0, fileName.length() - 3) + ".gz";
		builder.save(fileName, getH2Headers());
	}
	
	
	synchronized public TinkerH2Frame open(String fileName) {
		TinkerGraph g = TinkerGraph.open();
		try {
			long startTime = System.currentTimeMillis();

			Builder<GryoIo> builder = IoCore.gryo();
			builder.graph(g);
			IoRegistry kryo = new MyGraphIoRegistry();
			builder.registry(kryo);
			GryoIo yes = builder.create();
			yes.readGraph(fileName.substring(0, fileName.length() - 3) + "_META.tg");
			
			long endTime = System.currentTimeMillis();
			LOGGER.info("Successfully loaded TinkerFrame from file: "+fileName+ "("+(endTime - startTime)+" ms)");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		//create new tinker frame and set its tinkergraph
		TinkerH2Frame tf = new TinkerH2Frame();
		g.createIndex(Constants.TYPE, Vertex.class);
		g.createIndex(Constants.ID, Vertex.class);
		g.createIndex(Constants.ID, Edge.class);
		tf.g = g;
		
		String[] headers = null;
		String[] types = null;
		GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(Constants.TYPE, ENVIRONMENT_VERTEX_KEY);
		while(gt.hasNext()) {
			Vertex specialVert = gt.next();
			
			// grab all environment properties from node
			Object headerProp = specialVert.property(Constants.HEADER_NAMES).value();
			Object typeProp = specialVert.property(TYPES).value();
			headers = new Gson().fromJson(headerProp + "", new String[]{}.getClass());
			types = new Gson().fromJson(typeProp + "", new String[]{}.getClass());
			// delete the vertex
			specialVert.remove();
		}
		
		if(headers == null) {
			LOGGER.info("Could not find the headers special vertex.  Will load headers from metadata with no guarantee of order.");
			List<String> headersList = new Vector<String>();
			GraphTraversal<Vertex, String> hTraversal = tf.g.traversal().V().has(Constants.TYPE, META).values(Constants.NAME);
			while(hTraversal.hasNext()) {
				headersList.add(hTraversal.next());
			}
			headers = headersList.toArray(new String[]{});
		}
		//gather header names
		tf.headerNames = headers;
		g.variables().set(Constants.HEADER_NAMES, headers);
//		fileName = fileName.substring(0, fileName.length() - 3) + ".gz";
		tf.builder = H2Builder.open(fileName);
//		tf.builder.setHeaders(headers);
		tf.builder.types = types;
//		tf.updateHeaderMap();
		
		return tf;
	}
	   
	protected void redoLevels(String [] newLevels)
	{
		if(this.headerNames == null){
			this.headerNames = newLevels;
//			updateHeaderMap();
			return;
		}
		
		// put it in a set to get unique values
		Set<String> myset = new LinkedHashSet<String>(Arrays.asList(headerNames));
		
		for(String newLevel : newLevels) {
			if(!newLevel.contains(primKeyDelimeter)) {
				myset.add(newLevel);
			}
		}
//		myset.remove(PRIM_KEY);
		
		String [] newLevelNames = myset.toArray(new String[myset.size()]);

		g.variables().set(Constants.HEADER_NAMES, newLevelNames); // I dont know if i even need this moving forward.. but for now I will assume it is	
		
		String[] testHeaders = (String[])(g.variables().get(Constants.HEADER_NAMES).get());
		System.out.println(Arrays.toString(testHeaders));
		
		headerNames = newLevelNames;
//		updateHeaderMap();
	}
	
//	protected void updateHeaderMap() {
////		this.metaData.
//		if(H2HeaderMap == null) {
//			H2HeaderMap = new HashMap<String, String>();
//		}
//		H2HeaderMap.clear();
//		for(String headerName : headerNames) {
//			H2HeaderMap.put(headerName, cleanHeader(headerName));
//		}
//	}
	
	protected void updateH2PhysicalNames() {
		
	}
	
	private String cleanHeader(String header) {
    	/*header = header.replaceAll(" ", "_");
    	header = header.replace("(", "_");
    	header = header.replace(")", "_");
    	header = header.replace("-", "_");
    	header = header.replace("'", "");*/
    	header = header.replaceAll("[#%!&()@#$'./-]*", ""); // replace all the useless shit in one go
    	header = header.replaceAll("\\s+","_");
    	header = header.replaceAll(",","_"); 
    	if(Character.isDigit(header.charAt(0)))
    		header = "c_" + header;
    	return header;
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
		
		this.mergeEdgeHash(edgeHash);
		
		
	}
	
	public void setMetaData(String tableName, String[] headers, String[] types) {
		
		if(headers.length != types.length) {
			throw new IllegalArgumentException("Number of headers and types not equal");
		}
			

		addMetaDataTypes(headers, types);
	}
	
	protected String getMetaNodeValue(String metaNodeName) {
		String metaNodeValue;
		if(metaNodeName.equals(PRIM_KEY)) {
			metaNodeValue = builder.getNewTableName();
		} else {
			metaNodeValue = cleanHeader(metaNodeName);
		}
		
//		// get metamodel info for metaModeName
//		GraphTraversal<Vertex, Vertex> metaT = g.traversal().V().has(Constants.TYPE, TinkerFrame.META).has(Constants.NAME, metaNodeName);
//		
//		// if metaT has metaNodeName then find the value else return metaNodeName
//		if (metaT.hasNext()) {
//			Vertex startNode = metaT.next();
//			metaNodeValue = startNode.property(Constants.VALUE).value() + "";
//		}

		return metaNodeValue;
	}
	
	@Override
	public void mergeEdgeHash(Map<String, Set<String>> newEdgeHash) {
		Set<String> newLevels = new LinkedHashSet<String>();
		for(String newNode : newEdgeHash.keySet()) {
			
			//grab the edges
			Set<String> edges = newEdgeHash.get(newNode);
			
			//grab/create the meta vertex associated with newNode
			this.metaData.storeVertex(newNode, getMetaNodeValue(newNode), null);
			if(newNode.startsWith(PRIM_KEY)) {
				this.metaData.setPrimKey(newNode, true);
			} else {
				//collect the column headers
				newLevels.add(newNode);
			}
			
			//for each edge in corresponding with newNode create the connection within the META graph
			for(String inVertString : edges){
				// now to insert the meta edge
				this.metaData.storeVertex(inVertString, getMetaNodeValue(inVertString), newNode);
				if(inVertString.startsWith(PRIM_KEY)) {
					this.metaData.setPrimKey(inVertString, true);
				} else {
					newLevels.add(inVertString);
				}
				this.metaData.storeRelation(newNode, inVertString);
			}
		}
		// need to make sure prim key is not added as header
//		newLevels.remove(PRIM_KEY);
		redoLevels(newLevels.toArray(new String[newLevels.size()]));
	}
	
	@Override
	public void connectTypes(String outType, String inType) {

		Set<String> newLevels = new LinkedHashSet<String>();
		this.metaData.storeVertex(outType, getMetaNodeValue(outType), null);
		newLevels.add(outType);
		
		if(inType!=null){
			this.metaData.storeVertex(inType, getMetaNodeValue(inType), null);
			this.metaData.storeRelation(outType, inType);
			newLevels.add(inType);
		}
		
		newLevels.remove(PRIM_KEY);
		redoLevels(newLevels.toArray(new String[newLevels.size()]));
	}
	
//	private List<String> getH2Headers(List<String> headers) {
//		List<String> retheaders = new ArrayList<String>(headers.size());
//		for(String header : headers) {
////			retheaders.add(H2HeaderMap.get(header));
//			retheaders.add(this.metaData.getValueForUniqueName(header));
//
//		}
//		return retheaders;
//	}
	
	public static void main(String[] args) {
		
	}
}
