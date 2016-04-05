package prerna.ds.H2;

import java.io.IOException;
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
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.Io.Builder;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import com.google.gson.Gson;

import prerna.ds.TinkerFrame;
import prerna.ds.TinkerFrameIterator;
import prerna.ds.TinkerMetaData;
import prerna.ds.UniqueScaledTinkerFrameIterator;
import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.om.TinkerGraphDataModel;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Constants;
import prerna.util.MyGraphIoRegistry;
import prerna.util.Utility;

public class TinkerH2Frame extends TinkerFrame {

	
	private static final Logger LOGGER = LogManager.getLogger(TinkerH2Frame.class.getName());
	private static final String TYPES = "Table_Types";
	
	H2Builder builder;
	TinkerMetaData metaData;
	Map<String, String> H2HeaderMap;
	
	public TinkerH2Frame(String[] headers) {
		this.headerNames = headers;
		builder = new H2Builder();
//		builder.create(headers);
		this.metaData = new TinkerMetaData();
		H2HeaderMap = new HashMap<String, String>();
		updateHeaderMap();
	}
	
	public TinkerH2Frame() {
		builder = new H2Builder();
		this.metaData = new TinkerMetaData();
		H2HeaderMap = new HashMap<String, String>();
	}
	
	/*************************** AGGREGATION METHODS *************************/
	
	public void addRow(Map<String, Object> row) {
		String[] newRow = new String[row.keySet().size()];
		
		int i = 0;
		for(String key : row.keySet()) {
			newRow[i] = (String)row.get(key);
			i++;
		}
		
		addRow(newRow);
	}
	
	public void addRow(String[] row) {
		builder.addRow(row, getH2Headers());
	}
	
//	public void addRelationship(Map<String, Object> row) {
//		//update the builder
//	}
	
	/************************** END AGGREGATION METHODS **********************/
	
	
	@Override
	public void processDataMakerComponent(DataMakerComponent component) {
		long startTime = System.currentTimeMillis();
        LOGGER.info("Processing Component..................................");
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
         	   this.mergeQSEdgeHash(edgeHash, engine);
         	   
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
		return builder.getData(getSelectors());
	}
	
	@Override
	public List<Object[]> getRawData() {
		return builder.getData(getSelectors());
	}
	
	
	/****************************** FILTER METHODS **********************************************/
	
	/**
	 * String columnHeader - the column on which to filter on
	 * filterValues - the values that will remain in the 
	 */
	@Override
	public void filter(String columnHeader, List<Object> filterValues) {
		//filterValues is what to keep
		columnHeader = H2HeaderMap.get(columnHeader);
		builder.setFilters(columnHeader, filterValues);
	}

	@Override
	public void unfilter(String columnHeader) {
		columnHeader = H2HeaderMap.get(columnHeader);
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
		
		int length = this.headerNames.length;
		
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
			visibleValues.put(headerNames[i], new ArrayList<Object>(columnSets[i]));
			filteredValues.put(headerNames[i], new ArrayList<Object>());
		}
		
		
		
		return new Object[]{visibleValues, builder.getFilteredValues(getSelectors())};
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
		List<String> h2selectors = new ArrayList<>();
		for(String selector : getSelectors()) {
			h2selectors.add(H2HeaderMap.get(selector));
		}
		return builder.buildIterator(h2selectors);
	}
	
	@Override
	public Iterator<Object[]> iterator(boolean getRawData, Map<String, Object> options) {
		List<String> selectors = (List<String>) options.get(TinkerFrame.SELECTORS);
		if(selectors != null) {
			List<String> h2selectors = new ArrayList<>();
			for(String selector : selectors) {
				h2selectors.add(H2HeaderMap.get(selector));
			}
			options.put(TinkerFrame.SELECTORS, h2selectors);
		}
		return builder.buildIterator(options);
	}
	
	public void applyGroupBy(String column, String newColumnName, String valueColumn, String mathType) {
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
		columnHeader = H2HeaderMap.get(columnHeader);
		Object[] array = builder.getColumn(columnHeader, false);
		return array;
	}
	
	@Override
	public Integer getUniqueInstanceCount(String columnHeader) {
		columnHeader = H2HeaderMap.get(columnHeader);
		return builder.getColumn(columnHeader, true).length;
	}
	
	@Override
	public Double getMin(String columnHeader) {
		columnHeader = H2HeaderMap.get(columnHeader);
		return builder.getStat(columnHeader, "MIN");
	}
	
	@Override
	public Double getMax(String columnHeader) {
		columnHeader = H2HeaderMap.get(columnHeader);
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
		columnHeader = H2HeaderMap.get(columnHeader);
		return new ScaledUniqueH2FrameIterator(columnHeader, getRawData, getH2Selectors(), builder, getMax(), getMin());
	}
	
	@Override
	public Double[] getColumnAsNumeric(String columnHeader) {
		if(isNumeric(columnHeader)) {
			columnHeader = H2HeaderMap.get(columnHeader);
			Object[] array = builder.getColumn(columnHeader, false);
			
			List<Double> numericCol = new ArrayList<Double>();
			Iterator<Object> it = Arrays.asList(array).iterator();
			while(it.hasNext()) {
				Object row = it.next();
				numericCol.add( ((Number) row).doubleValue() );
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
		builder.updateTable(getH2Headers(), values, columnHeaders);
		//find last column
		//update values for last column
	}
	
	@Override
	public void removeColumn(String columnHeader) {
		if(!ArrayUtilityMethods.arrayContainsValue(this.headerNames, columnHeader)) {
			return;
		}
		
		builder.dropColumn(H2HeaderMap.get(columnHeader));
		
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
	}
	
	@Override
	public Iterator<Object> uniqueValueIterator(String columnHeader, boolean getRawData, boolean iterateAll) {

//		Map<String, Object> options = new HashMap<String, Object>();
//		options.put(DE_DUP, true);
//		
//		List<String> selectors = new ArrayList<String>();
//		selectors.add(columnHeader);
//		options.put(SELECTORS, selectors);
		columnHeader = H2HeaderMap.get(columnHeader);
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
		updateHeaderMap();
		
		return tf;
	}
	
	@Override
	public Map<String, Set<String>> mergeQSEdgeHash(Map<String, Set<String>> newEdgeHash, IEngine engine) {
		Map<String, Set<String>> cleanedHash = new HashMap<String, Set<String>>();
		Set<String> newLevels = new LinkedHashSet<String>();
		for(String newNodeKey : newEdgeHash.keySet()) {
			
			//query struct only knows physical name but our tinker is built purely on logical names
			//need to translate to logical to get the types
			String outPhysicalUri = null;
			String physicalName = newNodeKey;
			String outConceptName = null;
			if(newNodeKey.contains("__")){
				outConceptName = newNodeKey.substring(0, newNodeKey.indexOf("__"));
				physicalName = newNodeKey.substring(newNodeKey.indexOf("__")+2);
				outPhysicalUri = Constants.PROPERTY_URI + physicalName;
			}
			else{
				outPhysicalUri = engine.getConceptUri4PhysicalName(physicalName);
			}
			String newNode = Utility.getInstanceName(engine.getTransformedNodeName(outPhysicalUri, true));
			Set<String> cleanSet = new HashSet<String>();
			cleanedHash.put(newNode, cleanSet);
			
			//grab the edges
			Set<String> edges = newEdgeHash.get(newNodeKey);
			
			//collect the column headers
			newLevels.add(newNode);
			
			//grab/create the meta vertex associated with newNode
			Vertex outVert = this.metaData.upsertVertex(META, newNode, cleanHeader(newNode), physicalName, outPhysicalUri, engine.getEngineName(), engine.getDataTypes(outPhysicalUri), outConceptName);
			//for each edge in corresponding with newNode create the connection within the META graph
			for(String inVertS : edges){
				//query struct doesn't know logical names at all but our tinker is built purely on logical
				//need to translate to logical to get the types
				String inPhysicalUri = null;
				String inPhysicalName = inVertS;
				String inConceptName = null;
				if(inVertS.contains("__")){
					inConceptName = inVertS.substring(0, inVertS.indexOf("__"));
					inPhysicalName = inVertS.substring(inVertS.indexOf("__")+2);
					inPhysicalUri = Constants.PROPERTY_URI + inPhysicalName;
				}
				else{
					inPhysicalUri = engine.getConceptUri4PhysicalName(inPhysicalName);
				}
				String inVertString = Utility.getInstanceName(engine.getTransformedNodeName(inPhysicalUri, true));
				
				newLevels.add(inVertString);
				cleanSet.add(inVertString);
				
				// now to insert the meta edge
				Vertex inVert = this.metaData.upsertVertex(META, inVertString, inVertString, inPhysicalName, inPhysicalUri, engine.getEngineName(), engine.getDataTypes(inPhysicalUri), inConceptName);
//				Vertex inVert = upsertVertex(META, inVertString, inVertString);
				
				upsertEdge(outVert, inVert);
			}
		}
		// need to make sure prim key is not added as header
		newLevels.remove(PRIM_KEY);
		redoLevels(newLevels.toArray(new String[newLevels.size()]));
		return cleanedHash;
	}
    
	protected void redoLevels(String [] newLevels)
	{
		if(this.headerNames == null){
			this.headerNames = newLevels;
			updateHeaderMap();
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
		updateHeaderMap();
	}
	
	protected void updateHeaderMap() {
		for(String headerName : headerNames) {
			H2HeaderMap.put(headerName, cleanHeader(headerName));
		}
	}
	
	private String cleanHeader(String header) {
    	/*header = header.replaceAll(" ", "_");
    	header = header.replace("(", "_");
    	header = header.replace(")", "_");
    	header = header.replace("-", "_");
    	header = header.replace("'", "");*/
    	header = header.replaceAll("[#%!&()@#$'./-]*", ""); // replace all the useless shit in one go
    	header = header.replaceAll("\\s+","_"); 
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
			h2selectors.add(H2HeaderMap.get(selectors.get(i)));
		}
		return h2selectors;
	}
	
	private String[] getH2Headers() {
		if(headerNames == null) return null;
		String[] h2Headers = new String[headerNames.length];
		for(int i = 0; i < headerNames.length; i++) {
			h2Headers[i] = H2HeaderMap.get(headerNames[i]);
		}
		return h2Headers;
	}
}
