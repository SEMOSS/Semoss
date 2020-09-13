package prerna.sablecc;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import cern.colt.Arrays;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.sablecc.meta.ImportDataMetadata;
import prerna.sablecc2.reactor.imports.IImporter;
import prerna.sablecc2.reactor.imports.ImportFactory;
import prerna.util.ArrayUtilityMethods;

public abstract class ImportDataReactor extends AbstractReactor {

	protected static final Logger LOGGER = LogManager.getLogger(ImportDataReactor.class.getName());
	
//	protected enum LOAD_TYPE {ENGINE_IMPORT, FILE_IMPORT}

	// this stores the specific values that need to be aggregated from the child reactors
	// based on the child, different information is needed in order to properly add the 
	// data into the frame
	protected Hashtable <String, String[]> values2SyncHash = new Hashtable <String, String[]>();

	
	///////////////////////////////// SHARED VARIABLES //////////////////////////////////////
	// useful variables needed in order to properly perform the importing of data into the frame
	
//	// keep track of the type of import being performed based on enum type
//	protected LOAD_TYPE loadType = null;
	// keep track of the starting headers in the frame
	protected String[] startingHeaders = null;
	/*
	 * keep track of the list of table joins used for this operation
	 * example format
	 * [
	 * 		// {NAME IN CURRENT TABLE -> NAME IN QUERY}
	 * 		{Title -> Movie},
	 * 		{Genre_Updated -> Genre}
	 * ]
	 */
	protected Vector<Map<String,String>> joinCols = null;
	/*
	 * keep track of the edge hash 
	 * this is only needed so we know how to insert data for graphical databases
	 * example format
	 * {
	 * 		Title -> [Genre, Studio],
	 * 		Actor -> [Title]
	 * }
	 */
	protected Map<String, Set<String>> edgeHash = null;
	/*
	 * keep track of modifications that need to be made to the headers
	 * this is important when you're starting frame is [Title, Studio, Genre]
	 * and you are connecting to [Movie, Movie_Budget] and you want the Title and Movie
	 * to be reconciled into the same column
	 * example format
	 * {
	 * 		Title -> Movie,
	 * 		Movie_Budget -> Movie_Budget
	 * }
	 */
	protected Map<String, String> modifyNamesMap = new HashMap<>();
	// keep track of the headers we are adding
	// this will be cleaned to take into consideration the join column modifications
	protected String[] newHeaders = null;
	// keep track of the table join type
	protected String joinType = null;
	// store the iterator being used
	protected Iterator<IHeadersDataRow> dataIterator = null;
	// store if the edge hash is a generated prim key edge hash
	protected boolean isPrimKey = false;
	// store if all new headers are accounted
	protected boolean allHeadersAccounted = false;
	// store if the current frame is empty
	protected boolean isFrameEmpty = false;
	
	// for loops
	// need a boolean to determine if we should make the other columns
	// that already exist unique
	// this will affect all other columns that are not the join column
	protected boolean enableLoops = false;
	
	/**
	 * Constructor for the abstract class
	 */
	public ImportDataReactor()
	{
		String [] thisReacts = {PKQLEnum.API, PKQLEnum.JOINS, PKQLEnum.PASTED_DATA, PKQLEnum.MAP_OBJ, PKQLEnum.QUERY_STRUCT};
		
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.IMPORT_DATA;

		// when the data is coming from an API (i.e. an engine or a file)
		String [] dataFromApi = {PKQLEnum.COL_CSV, "ENGINE", "EDGE_HASH", "QUERY_NUM_CELLS", PKQLEnum.QUERY_STRUCT};
		values2SyncHash.put(PKQLEnum.API, dataFromApi);

		// when the data is coming from user copy/paste data into the tool
		String [] dataFromPastedData = {"EDGE_HASH", PKQLEnum.COL_CSV, PKQLEnum.QUERY_STRUCT};
		values2SyncHash.put(PKQLEnum.PASTED_DATA, dataFromPastedData);

		String [] dataFromRawQuery = {"DATA_TYPE_MAP", "LOGICAL_TO_VALUE", PKQLEnum.QUERY_STRUCT};
		// same thing when hard coded query
		values2SyncHash.put(PKQLEnum.RAW_API, dataFromRawQuery);
	}

	@Override
	/**
	 * Processes through the information based on the values2SyncHash
	 * Based on the values2Sync, the required information will be extracted and inputted into the myStore
	 * for the frame specific data reactors to add the data
	 */
	public Iterator process() {
		/*
		 * Process for getting the base information from the child reactors to then be used by the
		 * frame specific import data reactors
		 * 
		 * We are going to go ahead and collect the meta data needed in order to determine the most
		 * optimal way of inserting data into the frame
		 * 
		 * 1) grab the frame
		 * 2) see if frame is empty
		 * 3) grab existing column headers in the frame
		 * 4) grab the table join column
		 * 5) based on the import type grab the appropriate iterator and pieces required
		 * *** look inside each import type to see steps used for processing
		 * 
		 * TODO:
		 * if no engine, it is a drag and drop file
		 * the user can still define an edge hash to load
		 * 
		 */

		LOGGER.info("STARTING TO PROCESS INFORMATION FOR DATA LOADING....");
		SelectQueryStruct qs = getQueryStruct();
		// 1) grab the frame
		ITableDataFrame frame = (ITableDataFrame) myStore.get("G");

		// 2) see if frame is empty
		// this will benefit us if we know we can simply just perform inserts
		// or more complex logic is needed
		// mainly used for H2Frame
		isFrameEmpty = frame.isEmpty();
		LOGGER.info(" >>> IS FRAME EMPTY? " + isFrameEmpty);

		// 3) grab existing column headers in the frame
		// technically only need this if the frame is not empty
		// this is also important to help if we need simply perform an insert
		// or update existing data
		startingHeaders = frame.getColumnHeaders();
		
		// 4) grab the table join column information if present
		joinCols = new Vector<Map<String,String>>();
		joinType = "";
		OwlTemporalEngineMeta meta = frame.getMetaData();
		Vector<Map<String, String>> joins = (Vector<Map<String, String>>) myStore.get(PKQLEnum.JOINS);
		if(joins!=null){
			for(Map<String,String> join : joins) {
				Map<String,String> joinMap = new HashMap<String,String>();
				joinMap.put(join.get(PKQLEnum.TO_COL), join.get(PKQLEnum.FROM_COL));
				joinCols.add(joinMap);
				joinType = join.get(PKQLEnum.REL_TYPE);
			}
			LOGGER.info(" >>> TABLE JOIN DEFINED AS " + joinCols);
		} else {
			LOGGER.info(" >>> NO TABLE JOIN DEFINED");
		}
		
		// this is greedy execution
		// will not return anything
		// but will update the frame in the pixel planner
		
		IImporter importer = ImportFactory.getImporter(frame, qs);
		if(joinCols.isEmpty()) {
			try {
				importer.insertData();
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
//			importer.mergeData(joins);
		}
		
//		// 5) grab the appropriate pieces based on the import type
//		if(myStore.containsKey(PKQLEnum.API)) {
//			// this is the case when we have an API import
//			// there are 2 possibilities in this situation
//			// 1 - this is a query via a QueryStruct on an engine
//			// 2 - this is an import via a file
//			
//			// try to grab the engine
//			// if it is not null, we have a query we are loading
//			// if it is null, we have a file
//			IEngine engine = (IEngine) Utility.getEngine((this.getValue(PKQLEnum.API + "_ENGINE")+"").trim());
//
//			// this is the first possibility when it is a query
//			if(engine != null) {
//				
//				// note, we do not update the edge hash
//				// if we modify the names for the join columns
//				// we will be screwed when we try to get metadata around that column name
//				// from the engine owl file ...
//				
//				// note, this edge hash will need to be cleansed based on the join information
//				// so this is just a local variable, not the class variable
//				Map<String, Set<String>> apiEdgeHash = (Map<String, Set<String>>) this.getValue(PKQLEnum.API + "_EDGE_HASH");
//				
//				// put the edge hash and the logicalToValue maps within the myStore
//				// will be used when the data is actually imported
//				if(frame instanceof NativeFrame) {
//					// note, this only updates the QS, doesn't need an iterator
//					if(((NativeFrame)frame).getEngineName().equals(engine.getEngineName())) {
//						// if the frame is a native frame and we want to append additional
//						// information, we need to combine the current qs with the new qs
//						// this method cleans the information based on the join and updates
//						// the frame metadata
//						System.out.println(meta.getFrameSelectors());
//						Map[] mergedMaps = OwlTemporalEngineMetaHelper.mergeQSEdgeHash(frame, meta, apiEdgeHash, engine, joinCols, null);
//						// set the class edge hash
//						this.edgeHash = mergedMaps[0];
//						// set the class modify names map
//						this.modifyNamesMap = mergedMaps[1];
//						// update the header names
////						this.newHeaders = updateNamesForJoins( ((IRawSelectWrapper) dataIterator).getDisplayVariables(), joinCols); 
//						
//						LOGGER.info(" >>> NATIVE FRAME IS MODIFYING EXISTING ENGINE QUERY STRUCT");
//					} else {
//						LOGGER.info(" >>> NATIVE FRAME IS SWITCHING INTO ANOTHER ENGINE!!!!");
//					}
//				} else {
//					// grab the iterator to use for importing
//					dataIterator  = (IRawSelectWrapper) myStore.get(PKQLEnum.API);
//			
//					// update the header names
//					String[] origNewHeaders = ((IRawSelectWrapper) dataIterator).getDisplayVariables();
//					this.newHeaders = updateNamesForJoins(origNewHeaders, joinCols); 
//					Map<String, Boolean> makeUniqueNameMap = getUniqueNameMap(frame, origNewHeaders, apiEdgeHash);
//					
//					LOGGER.info(" >>> FRAME IS LOADING DATA FROM AN ENGINE!!!!");
//					// update the metadata in the frame
//					Map[] mergedMaps = OwlTemporalEngineMetaHelper.mergeQSEdgeHash(frame, meta, apiEdgeHash, engine, joinCols, makeUniqueNameMap);
//					this.newHeaders = meta.getFrameSelectors().toArray(new String[meta.getFrameSelectors().size()]);
//					frame.syncHeaders();
//					this.edgeHash = mergedMaps[0];
//					// set the class modify names map
//					this.modifyNamesMap = mergedMaps[1];
//				}
//			}
//			// this is the second possibility when it is a file
//			else if(myStore.get(PKQLEnum.API) instanceof IFileIterator) {
//				// grab the iterator to use for importing
//				dataIterator  = (IFileIterator) myStore.get(PKQLEnum.API);
//				
//				LOGGER.info(" >>> FRAME IS LOADING FROM A FILE!!!!");
//				
//				String[] types = ((IFileIterator) dataIterator).getTypes();
//				String[] headers = ((IFileIterator) dataIterator).getHeaders();
//				Map<String, String> dataTypes = new HashMap<>();
//				for(int i = 0; i < types.length; i++) {
//					dataTypes.put(headers[i], types[i]);
//				}
//				
//				this.newHeaders = updateNamesForJoins(headers, joinCols);
//				dataTypes = updateDataTypesForJoins(dataTypes, joinCols);
//				this.edgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(this.newHeaders);
//				this.isPrimKey = true;
//				// update the metadata in the frame
//				OwlTemporalEngineMetaHelper.mergeEdgeHashDataframe(meta, edgeHash, dataTypes);
//				if (frame instanceof TinkerFrame) {
//					meta = OwlTemporalEngineMetaHelper.mergeDataTypeMapTinker(meta, dataTypes);
//
//				} else {
//					meta = OwlTemporalEngineMetaHelper.mergeDataTypeMapH2(meta, dataTypes);
//
//				}
//				frame.syncHeaders();
//			}  
////			else if(myStore.get(PKQLEnum.API) instanceof H2HeadersDataRowIterator) {
////				//we are merging data to this from from an iterator/frame
////				LOGGER.info(" >>> FRAME IS LOADING FROM Iterator<IHeadersDataRow>!!!!");
////
////				// grab the iterator to use for importing
////				dataIterator  = (H2HeadersDataRowIterator) myStore.get(PKQLEnum.API);
////				
////				if(dataIterator instanceof H2HeadersDataRowIterator) {
////					String[] types = ((H2HeadersDataRowIterator)dataIterator).getTypes();
////					String[] headers = ((H2HeadersDataRowIterator) dataIterator).getHeaders();
////					Map<String, String> dataTypes = new HashMap<>();
////					for(int i = 0; i < types.length; i++) {
////						dataTypes.put(headers[i], types[i]);
////					}
////					
////					this.newHeaders = updateNamesForJoins(headers, joinCols);
////					dataTypes = updateDataTypesForJoins(dataTypes, joinCols);
////					this.edgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(this.newHeaders);
////					this.isPrimKey = true;
////					// update the metadata in the frame
////					meta = OwlTemporalEngineMetaHelper.mergeEdgeHashDataframe(meta, edgeHash, dataTypes);
////				frame.syncHeaders();
////				}
////			}
//		} else if(myStore.containsKey(PKQLEnum.RAW_API)) {
//			LOGGER.info(" >>> FRAME IS LOADING DATA FROM AN INPUT QUERY!!!!");
//
//			dataIterator = (IRawSelectWrapper) myStore.get(PKQLEnum.RAW_API);
//
//			Map<String, String> dataTypes = (Map<String, String>) this.getValue(PKQLEnum.RAW_API + "_DATA_TYPE_MAP");
//			Map<String, String> logicalToValue = (Map<String, String>) this.getValue(PKQLEnum.RAW_API + "_LOGICAL_TO_VALUE");
//			
//			this.newHeaders = updateNamesForJoins( ((IRawSelectWrapper) dataIterator).getDisplayVariables(), joinCols); 
//			this.modifyNamesMap = updateModifyNamesForJoins(logicalToValue, joinCols);
//			dataTypes = updateDataTypesForJoins(dataTypes, joinCols);
//
//			this.edgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(newHeaders);
//			this.isPrimKey = true;
//			// update the metadata in the frame
//			OwlTemporalEngineMetaHelper.mergeEdgeHashDataframe(meta, edgeHash, dataTypes);
//			frame.syncHeaders();
//
//		} else if(myStore.containsKey(PKQLEnum.PASTED_DATA)) {
//			LOGGER.info(" >>> FRAME IS LOADING FROM PASTED TEXT!!!!");
//
//			// grab the iterator to use for importing
//			dataIterator  = (CsvFileIterator) myStore.get(PKQLEnum.PASTED_DATA);
//			
//			String[] types = ((CsvFileIterator) dataIterator).getTypes();
//			String[] headers = ((CsvFileIterator) dataIterator).getHeaders();
//			Map<String, String> dataTypes = new HashMap<>();
//			for(int i = 0; i < types.length; i++) {
//				dataTypes.put(headers[i], types[i]);
//			}
//			
//			this.newHeaders = updateNamesForJoins(headers, joinCols);
//			dataTypes = updateDataTypesForJoins(dataTypes, joinCols);
//			this.edgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(this.newHeaders);
//			this.isPrimKey = true;
//			// update the metadata in the frame
//			OwlTemporalEngineMetaHelper.mergeEdgeHashDataframe(meta, edgeHash, dataTypes);
//			frame.syncHeaders();
//		}
//		
//		
//		// update the data id on the frame
//		frame.updateDataId();

		return null;
	}

	private SelectQueryStruct getQueryStruct() {
		if(myStore.containsKey(PKQLEnum.API + "_" + PKQLEnum.QUERY_STRUCT)) {
			return (SelectQueryStruct) myStore.get(PKQLEnum.API + "_" + PKQLEnum.QUERY_STRUCT);
		}
		return null;
	}

	/**
	 * Update the names array based on the join columns
	 * Modifies 
	 * @param headers			The headers to be imported
	 * @param joinCols			The join columns
	 */
	protected String[] updateNamesForJoins(String[] headers, Vector<Map<String, String>> joinCols) {
		if(joinCols != null && !joinCols.isEmpty()){
			String[] newHeaders = new String[headers.length];
			for(int i = 0; i < newHeaders.length; i++) {
				newHeaders[i] = headers[i];
			}
			for(Map<String, String> join : joinCols) { // grab each join map, mapping is new name to existing name
				for(String otherName : join.keySet()) {
					String existingName = join.get(otherName);
					// if we find the new name inside the current header array
					// update it to be the old name which is currently inside the frame
					int index = ArrayUtilityMethods.arrayContainsValueAtIndex(headers, otherName);
					if(index > -1) {
						newHeaders[index] = existingName;
					}
				}
			}
			// override the existing reference
			headers = newHeaders;
		}
		return headers;
	}
	
	/**
	 * Update any keys for a map where they need to have modifications based on joins
	 * @param dataTypes
	 * @param joinCols
	 */
	protected Map<String, String> updateModifyNamesForJoins(Map<String, String> map, Vector<Map<String, String>> joinCols) {
		if(joinCols != null && !joinCols.isEmpty()){
			Map<String, String> editedMap = new Hashtable<String, String>();

			// loop through the data types and perform a replacement where necessary
			for(String colName : map.keySet()) {
				String value = map.get(colName);
				boolean foundMatchingName = false;

				JOIN_LOOP : for(Map<String, String> join : joinCols) { // grab each join map, mapping is new name to existing name
					for(String otherName : join.keySet()) {
						String existingName = join.get(otherName);
						
						if(colName.equals(otherName)) {
							editedMap.put(existingName, existingName);
							foundMatchingName = true;
							break JOIN_LOOP;
						}
					}
				}
				
				// if we didn't perform a swap after searching through all the join columns
				// add in the original value
				if(!foundMatchingName) {
					editedMap.put(colName, value);
				}
			}
			
			// override the method reference
			map = editedMap;
		}
		return map;
	}
	
	/**
	 * Update any keys for a map where they need to have modifications based on joins
	 * @param dataTypes
	 * @param joinCols
	 */
	protected Map<String, String> updateDataTypesForJoins(Map<String, String> map, Vector<Map<String, String>> joinCols) {
		if(joinCols != null && !joinCols.isEmpty()){
			Map<String, String> editedMap = new Hashtable<String, String>();

			// loop through the data types and perform a replacement where necessary
			for(String colName : map.keySet()) {
				String value = map.get(colName);
				boolean foundMatchingName = false;

				JOIN_LOOP : for(Map<String, String> join : joinCols) { // grab each join map, mapping is new name to existing name
					for(String otherName : join.keySet()) {
						String existingName = join.get(otherName);
						
						if(colName.equals(otherName)) {
							editedMap.put(existingName, value);
							foundMatchingName = true;
							break JOIN_LOOP;
						}
					}
				}
				
				// if we didn't perform a swap after searching through all the join columns
				// add in the original value
				if(!foundMatchingName) {
					editedMap.put(colName, value);
				}
			}
			
			// override the method reference
			map = editedMap;
		}
		return map;
	}
	
	protected Map<String, Set<String>> updateEdgeHashForJoin(Map<String, Set<String>> edgeHash, Vector<Map<String, String>> joinCols) {
		if(joinCols != null && !joinCols.isEmpty()){
			Map<String, Set<String>> cleanEdgeHash = new Hashtable<String, Set<String>>();

			// loop through the edge hash... first the parents and then the children
			for(String parentName : edgeHash.keySet()) {
				
				// see if we can clean the parent
				// start with assumption current one is good
				String effectiveParentName = parentName;
				
				JOIN_LOOP : for(Map<String, String> join : joinCols) { // grab each join map, mapping is new name to existing name
					for(String otherName : join.keySet()) {
						String existingName = join.get(otherName);
						
						if(parentName.equals(otherName)) { // grab each join map, mapping is new name to existing name
							// we found the name in the loop
							// override the effective parent
							effectiveParentName = existingName;
							break JOIN_LOOP;
						}
					}
				}
				
				// see if the children are clean
				Set<String> effectiveChildrenSet = new HashSet<String>();
				
				// loop through the current children in the edge hash
				Set<String> currentChildren = edgeHash.get(parentName);
				for(String child : currentChildren) {
					
					// see if we can clean the child
					// start with the assumption the current child is the good one
					String effectiveChild = child;
					
					JOIN_LOOP : for(Map<String, String> join : joinCols) { // grab each join map, mapping is new name to existing name
						for(String otherName : join.keySet()) {
							String existingName = join.get(otherName);
							
							if(child.equals(otherName)) { // grab each join map, mapping is new name to existing name
								// we found the name in the loop
								// override the effective parent
								effectiveChild = existingName;
								break JOIN_LOOP;
							}
						}
					}
					
					// add to the list
					effectiveChildrenSet.add(effectiveChild);
				}
				
				// now that we can cleaned the parent and its children
				// add to the edge hash
				cleanEdgeHash.put(effectiveParentName, effectiveChildrenSet);
			}
			
			// override the reference
			edgeHash = cleanEdgeHash;
		}
		return edgeHash;
	}
	
	private Map<String, Boolean> getUniqueNameMap(ITableDataFrame frame, String[] origNewHeaders, Map<String, Set<String>> apiEdgeHash) {
		// this is the logic we need to figure out how to deal with loops
		// logic is as follows
		// for each connection in the edge hash
		// if the start and end node both exist in the frame currently
		// and if a connection exists between them already
		// and the new connection imported is in a different order than the connection currently there
		// then we need to enforce uniqueness, otherwise, add as is
		
		// easier implementation
		// if there is a connection already between the nodes
		// set the map to make unique to false
		// otherwise, keep as true as it won't matter
		
		// another note
		// we know based on the join information which column will actually become unique
		// and which one will not and will use the existing name from the frame
		Boolean[] makeHeaderUniqueArr = new Boolean[this.newHeaders.length];
		Map<String, Boolean> makeUniqueNameMap = new Hashtable<String, Boolean>();
		OwlTemporalEngineMeta meta = frame.getMetaData();
		if(frame.isEmpty()) {
			// yeah, everything is new -> make unique
			for(String header : newHeaders) {
				makeUniqueNameMap.put(header, true);
			}
			return makeUniqueNameMap;
		}
		
		//need to determine whether to enable loops or not
		Map<Object, Object> map = (Map<Object, Object>) myStore.get(PKQLEnum.MAP_OBJ);
		if(map != null) {
			String enableLoopsVal = (String)map.get("enableLoops");
			this.enableLoops = "true".equalsIgnoreCase(enableLoopsVal);
			if(enableLoops) {
				for(Map<String, String> joinCol : joinCols) {
					for(String j : joinCol.keySet()) {
						makeUniqueNameMap.put(j, false);
					}
				}
				return makeUniqueNameMap;
			}
		}
		
//		// get the current edge hash
//		Map<String, Set<String>> currentEdgeHash = OwlTemporalEngineMetaHelper.convertRelationshipsToEdgeHash(meta);
//		
//		// sadly, will need to clean the edge hash here as well...
//		// this is a pain because i cannot pass it into the mergeQSEdgeHash
//		Map<String, Set<String>> cleanImportEdgeHash = updateEdgeHashForJoin(apiEdgeHash, joinCols);
//		
//		int numHeaders = this.newHeaders.length;
//		for(int index = 0; index < numHeaders; index++) {
//			String headerName = this.newHeaders[index];
//			
//			// see if it exists as a header in the clean import
//			if(cleanImportEdgeHash.containsKey(headerName)) {
//				// must also be a parent in the current edge hash to care
//				if(currentEdgeHash.containsKey(headerName)) {
//					// okay, both are there
//					// need to compare the children
//
//					// get the list of current children
//					Set<String> currentChildren = currentEdgeHash.get(headerName);
//
//					// get the list of children we are adding
//					Set<String> importingChildren = cleanImportEdgeHash.get(headerName);
//
//					// the overlapping children should not be unique
//					if(importingChildren.isEmpty()) {
//						// make sure to note override values set when we go through the
//						// children logic up above
//						if(makeHeaderUniqueArr[index] == null) {
//							// do not care if it is unique or not
//							// just keep as true
//							makeHeaderUniqueArr[index] = true;
//						}
//					} else {
//						for(String importingChild : importingChildren) {
//							if(currentChildren.contains(importingChild)) {
//								// the path in the import already exists
//								// do not make a unique header!!!
//								int importIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(this.newHeaders, importingChild);
//								makeHeaderUniqueArr[importIndex] = false;
//
//								importIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(this.newHeaders, headerName);
//								makeHeaderUniqueArr[importIndex] = false;
//							} else {
//								// make sure to note override values set when we go through the
//								// children logic up above
//								if(makeHeaderUniqueArr[index] == null) {
//									// do not care if it is unique or not
//									// just keep as true
//									makeHeaderUniqueArr[index] = true;
//								}
//							}
//						}
//					}
//				} else {
//					// make sure to note override values set when we go through the
//					// children logic up above
//					if(makeHeaderUniqueArr[index] == null) {
//						// do not care if it is unique or not
//						// just keep as true
//						makeHeaderUniqueArr[index] = true;
//					}
//				}
//				
//			} else {
//				// make sure to note override values set when we go through the
//				// children logic up above
//				if(makeHeaderUniqueArr[index] == null) {
//					// do not care if it is unique or not
//					// just keep as true
//					makeHeaderUniqueArr[index] = true;
//				}
//			}
//		}
//		
//		// now that i have determined which headers need the unique 
//		for(int index = 0; index < numHeaders; index++) {
//			makeUniqueNameMap.put(origNewHeaders[index], makeHeaderUniqueArr[index]);
//		}
//		
		return makeUniqueNameMap;
	}
//	/**
//	 * Update the edge hash based on the join information
//	 * @param egdeHash
//	 * @param joinCols
//	 * @return
//	 */
//	protected Map<String, Set<String>> updateEdgeHashForJoins(Map<String, Set<String>> egdeHash, Vector<Map<String, String>> joinCols) {
//		if(joinCols != null && !joinCols.isEmpty()){
//			Map<String, Set<String>> newEdgeHash = new Hashtable<String, Set<String>>(); // used to add new keys since cannot update existing map while looping
//			
//			for(String parentName : edgeHash.keySet()) {
//				Set<String> childrenName = edgeHash.get(parentName);
//				
//				// first edit the children
//				Set<String> newChildrenName = new HashSet<String>();
//				for(String child : childrenName) {
//					boolean foundChild = false;
//					JOIN_LOOP : for(Map<String, String> join : joinCols) {
//						for(String otherName : join.keySet()) {
//							String existingName = join.get(otherName);
//							
//							// swap out the name if it is there
//							if(childrenName.contains(otherName)) {
//								newChildrenName.add(existingName);
//								foundChild = true;
//								break JOIN_LOOP;
//							}
//						}
//					}
//					// if we didn't find a name change
//					// add the name as is
//					if(!foundChild) {
//						newChildrenName.add(child);
//					}
//				}
//				
//				// once we got the children
//				// do the parent
//				boolean foundParent = false;
//				JOIN_LOOP : for(Map<String, String> join : joinCols) {
//					for(String otherName : join.keySet()) {
//						String existingName = join.get(otherName);
//						
//						// swap out the name if it is there
//						if(parentName.equals(otherName)) {
//							newEdgeHash.put(existingName, newChildrenName);
//							foundParent = true;
//							break JOIN_LOOP;
//						}
//					}
//				}
//				// if we didn't find a name change for the parent
//				// add parent in with edited children set
//				if(!foundParent) {
//					newEdgeHash.put(parentName, newChildrenName);
//				}
//			}
//			// override the existing reference
//			edgeHash = newEdgeHash;
//		}
//		return edgeHash;
//	}
	
	/**
	 * Gets the values to load into the reactor
	 * This is used to synchronize between the various reactors that can feed into this reactor
	 * @param input			The type of child reactor
	 */
	public String[] getValues2Sync(String input)
	{
		return values2SyncHash.get(input);
	}

	/**
	 * Creates the return response string based on the iterator and the headers
	 * @param it					The iterator being used to insert data
	 * @param headers				String[] containing the headers that were added
	 */
	protected void inputResponseString(Iterator it, String[] headers) {
		// get rid of this bifurcation
		// push this into the iterators

		String nodeStr = (String)myStore.get(PKQLEnum.EXPR_TERM);
		// this is just from a file
		// create a boring response string
		myStore.put(nodeStr, createResponseString(headers));
	}

	/**
	 * Create the return response based on the headers
	 * @param headers			The headers of the data used to insert data
	 * @return					String returning the response
	 */
	protected String createResponseString(String[] headers){
		return "Successfully added data using:\n headers= " + Arrays.toString(headers);
	}

	// TODO as a result of this approach to get the message to send to the FE
	// no longer need the logic above for createResponseString()
	// need to push this information into the api reactors
	public IPkqlMetadata getPkqlMetadata() {
		ImportDataMetadata metadata = new ImportDataMetadata();
		metadata.setPkqlStr((String) myStore.get(PKQLEnum.IMPORT_DATA));
		if(myStore.containsKey("API_COL_CSV")) {
			metadata.setColumns((List<String>) myStore.get("API_COL_CSV"));
		} else if(myStore.containsKey("PASTED_DATA_COL_CSV")) {
			metadata.setColumns((List<String>) myStore.get("PASTED_DATA_COL_CSV"));
		} else if(myStore.containsKey("CSV_TABLE_COL_CSV")) {
			metadata.setColumns((List<String>) myStore.get("CSV_TABLE_COL_CSV"));
		} else if(myStore.containsKey("API_ENGINE")) {
			//create column info for api data
			String data = (String) myStore.get("MOD_IMPORT_DATA");
			if(data.contains("{")) {
				data = data.substring(data.indexOf("{") + 1, data.indexOf("}")).trim();
			} else {
				data = data.trim();
			}
			String column = myStore.get("API_ENGINE") + "-" + data;
			if(column.indexOf("\'url\'") > 0) {
				column = "Columns from url";
			}
			List<String> c = new Vector<String>();
			c.add(column);
			metadata.setColumns(c);
		}
		return metadata;
	}
}
