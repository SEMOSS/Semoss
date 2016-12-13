package prerna.sablecc;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import cern.colt.Arrays;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngineWrapper;
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.sablecc.meta.ImportDataMetadata;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;

/**
 * This is the base class used by the frame specific import data reactors.
 * This class doesn't perform any adding of data in the frames
 * It does however input specific pieces of information into the myStore map
 * such that the frame specific import data reactors can optimally 
 * add the data into the frame
 *
 */
public abstract class ImportDataReactor extends AbstractReactor {

	// this stores the specific values that need to be aggregated from the child reactors
	// based on the child, different information is needed in order to properly add the 
	// data into the frame
	Hashtable <String, String[]> values2SyncHash = new Hashtable <String, String[]>();

	/**
	 * Constructor for the abstract class
	 */
	public ImportDataReactor()
	{
		String [] thisReacts = {PKQLEnum.API, PKQLEnum.JOINS, PKQLEnum.CSV_TABLE, PKQLEnum.PASTED_DATA};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.IMPORT_DATA;

		// when the data is coming from an API (i.e. an engine or a file)
		String [] dataFromApi = {PKQLEnum.COL_CSV, "ENGINE", "EDGE_HASH", "QUERY_NUM_CELLS"};
		values2SyncHash.put(PKQLEnum.API, dataFromApi);

		// when the data is coming from user copy/paste data into the tool
		String [] dataFromPastedData = {"EDGE_HASH", PKQLEnum.COL_CSV};
		values2SyncHash.put(PKQLEnum.PASTED_DATA, dataFromPastedData);
		
		String [] dataFromRawQuery = {"EDGE_HASH", "DATA_TYPE_MAP", "LOGICAL_TO_VALUE"};
		// same thing when hard coded query
		values2SyncHash.put(PKQLEnum.RAW_API, dataFromRawQuery);
				
		// TODO: don't really need this, should probably remove it since the format is
		// not user friendly at all
		//
		// when the data is coming from a "csv table"
		String [] dataFromTable = {"EDGE_HASH"};
		values2SyncHash.put(PKQLEnum.CSV_TABLE, dataFromTable);
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
		 * 1) get the starting headers in the frame -> this is only used by H2ImportDataReactor and SparkImportDataReactor
		 * 2) get the join information
		 * 3) based on the type of child reactor, get the iterator
		 * only if the child reactor is an api reactor
		 * if there is an engine
		 * 4) perform the metadata update using the engine and edgeHash
		 * 5) store edgeHash and logicalToValueMap
		 * 6) update the data id on the frame so FE knows new data is being added
		 * 
		 * TODO:
		 * if no engine, it is a drag and drop file
		 * the user can still define an edge hash to load
		 * why are we just ignoring this :(
		 * 
		 * TODO:
		 * also super weird that the QSMergeEdgeHash occurs here (i.e. when its an engine api within the ImportDataReactor)
		 * but the other cases where a mergeEdgeHash method is called is done in the frame specific classes.... not consistent 
		 * for no reason
		 */
		
		modExpression();
		System.out.println("My Store on IMPORT DATA REACTOR: " + myStore);
		
		ITableDataFrame frame = (ITableDataFrame) myStore.get("G");
		
		// 1) get the starting headers
		// the starting headers is important to keep for the frame specific import data reactors 
		// They are responsible for knowing when to perform an addRow vs. an addRelationship 
		// (i.e. insert vs. update for H2ImportDataReactor)
		String[] startingHeaders = frame.getColumnHeaders();
		// store in mystore
		myStore.put("startingHeaders", startingHeaders);

		// 2) format and process the join information
		Vector<Map<String,String>> joinCols = new Vector<Map<String,String>>();
		Vector<Map<String, String>> joins = (Vector<Map<String, String>>) myStore.get(PKQLEnum.JOINS);
		String joinType = "";
		if(joins!=null){
			for(Map<String,String> join : joins){
				Map<String,String> joinMap = new HashMap<String,String>();
				joinMap.put(join.get(PKQLEnum.TO_COL), join.get(PKQLEnum.FROM_COL));
				joinCols.add(joinMap);
				joinType = join.get(PKQLEnum.REL_TYPE);
			}
		}
		// store in mystore
		myStore.put(PKQLEnum.JOINS, joinCols);
		myStore.put(PKQLEnum.REL_TYPE, joinType);
		
		
		// 3) grab the iterator
		// the iterator is stored based on the type of child reactor was part of the import data reactor
//		Iterator it = null;
		Object it = null;
		if(myStore.containsKey(PKQLEnum.API)) {			
			Map<String, Set<String>> edgeHash = (Map<String, Set<String>>) this.getValue(PKQLEnum.API + "_EDGE_HASH");
			IEngine engine = (IEngine) Utility.getEngine((this.getValue(PKQLEnum.API + "_ENGINE")+"").trim());
//			it  = (Iterator) myStore.get(PKQLEnum.API);
			it  = myStore.get(PKQLEnum.API);

			// 4 & 5) if engine is not null, merge the data into the frame
			// the engine is null when the api is actually a csv file
			if(engine != null) {
				// put the edge hash and the logicalToValue maps within the myStore
				// will be used when the data is actually imported
				if(frame instanceof NativeFrame) {
					if(((NativeFrame)frame).getEngineName().equals(engine.getEngineName())) {
						Map[] mergedMaps = frame.mergeQSEdgeHash(edgeHash, engine, joinCols);
						myStore.put("edgeHash", mergedMaps[0]);
						myStore.put("logicalToValue", mergedMaps[1]);
					} else {
						myStore.put(PKQLEnum.JOINS, joins);
					}
				} else {
					Map[] mergedMaps = frame.mergeQSEdgeHash(edgeHash, engine, joinCols);
					myStore.put("edgeHash", mergedMaps[0]);
					myStore.put("logicalToValue", mergedMaps[1]);
				}
			}
			// TODO: this is the logic we are ignoring
			// the edge hash may contain specific information the user wants to load
			// the logic needs to take into the fact that the default edge hash for when nothing 
			// is specified is each header pointing to an empty set
//			else {
//
//			}
		} else if(myStore.containsKey(PKQLEnum.RAW_API)) {
			Map<String, Set<String>> edgeHash = (Map<String, Set<String>>) this.getValue(PKQLEnum.RAW_API + "_EDGE_HASH");
			Map<String, String> dataTypeMap = (Map<String, String>) this.getValue(PKQLEnum.RAW_API + "_DATA_TYPE_MAP");
			Map<String, String> logicalToValue = (Map<String, String>) this.getValue(PKQLEnum.RAW_API + "_LOGICAL_TO_VALUE");
			
			frame.mergeEdgeHash(edgeHash, dataTypeMap);
			myStore.put("edgeHash", edgeHash);
			myStore.put("logicalToValue", logicalToValue);

			it = myStore.get(PKQLEnum.RAW_API);

		}else if(myStore.containsKey(PKQLEnum.PASTED_DATA)) {
			it = myStore.get(PKQLEnum.PASTED_DATA);
		} else if(myStore.containsKey(PKQLEnum.CSV_TABLE)) {
			it = myStore.get(PKQLEnum.CSV_TABLE);
		}
		
		
		// 6) update the data id on the frame
		frame.updateDataId();
		
		// store the iterator
		myStore.put("iterator", it);
		
		return null;
	}

	/**
	 * Method to update the values present in the edge hash, data type map, and headers based on the join cols
	 * @param primKeyEdgeHash					The edge hash to clean up based on the join columns
	 * @param dataType							The data type map to clean up based on the join columns
	 * @param headers							The header array to clean up based on the join columns
	 * @param joinCols							The set of join columns
	 */
	protected void updateDataForJoins(Map<String, Set<String>> primKeyEdgeHash, Map<String, String> dataType, String[] headers, Vector<Map<String, String>> joinCols) {
		if(joinCols != null && !joinCols.isEmpty()){
			Map<String, Set<String>> fixedEdgeMap = new HashMap<String, Set<String>>(); // used to add new keys since cannot update existing map while looping
			
			Set<String> processedJoins = new HashSet<String>(); // due to stupid doubling of joins that occurs need to keep track to not process twice
			for(Map<String, String> join : joinCols) { // grab each join map, mapping is new name to existing name
				Set<String> removeNames = new HashSet<String>(); // the new keys that were added since cannot update existing map while looping
				
				for(String otherName : join.keySet()) {
					String existingName = join.get(otherName);

					// to keep from doing stupid doubling
					processedJoins.add(existingName + otherName);
					
					if(!otherName.equals(existingName)) { // if names not the same, modify both prim key edge hash and data type map
						// loop through prim keys
						for(String key : primKeyEdgeHash.keySet()) {
							if(key.equals(otherName)) { // found a key, this requires us to add to new map to avoid concurrent modification
								fixedEdgeMap.put(existingName, primKeyEdgeHash.get(otherName)); // swap new name for old one
								removeNames.add(otherName); // add old key to remove
								
								// update data type map
								String dataValue = dataType.remove(otherName);
								if(dataValue != null) {
									dataType.put(existingName, dataValue);
								}
								// update the header
								int index = ArrayUtilityMethods.arrayContainsValueAtIndex(headers, otherName);
								if(index > -1) {
									headers[index] = existingName;
								}
							} else {
								// need to test the children
								Set<String> values = primKeyEdgeHash.get(key);
								if(values.contains(otherName)) {
									values.remove(otherName);
									values.add(existingName);
									
									// update data type map
									String dataValue = dataType.remove(otherName);
									if(dataValue != null) {
										dataType.put(existingName, dataValue);
									}
									// update the header
									int index = ArrayUtilityMethods.arrayContainsValueAtIndex(headers, otherName);
									if(index > -1) {
										headers[index] = existingName;
									}
								}
							}
						}
						
						// remove old keys, add new ones using existing values
						primKeyEdgeHash.keySet().removeAll(removeNames);
						primKeyEdgeHash.putAll(fixedEdgeMap);
					}
				}
			}
		}
	}
	
	
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
		
		String nodeStr = (String)myStore.get(whoAmI);
		// if the iterator is the return from an engine
		// get the query return response to send back
		if(it instanceof IEngineWrapper) {
			myStore.put(nodeStr, createResponseString((IEngineWrapper)it));
		} else {
			// this is just from a file
			// create a boring response string
			myStore.put(nodeStr, createResponseString(headers));
		}
	}
	
	/**
	 * Create the return response from a engine wrapper
	 * @param it				The iterator used to insert data
	 * @return					String returning the response
	 */
	protected String createResponseString(IEngineWrapper it){
		// get map containing the response metadata from the iterator
		Map<String, Object> map = it.getResponseMeta();
		// format fields from meta data map into a string
		String mssg = "";
		for(String key : map.keySet()){
			if(!mssg.isEmpty()){
				mssg = mssg + " \n";
			}
			mssg = mssg + key + ": " + map.get(key).toString();
		}
		String retStr = "Sucessfully added data using : \n" + mssg;
		return retStr;
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
