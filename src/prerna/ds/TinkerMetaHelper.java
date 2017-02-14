package prerna.ds;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.algorithm.api.IMetaData;
import prerna.engine.api.IEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class TinkerMetaHelper {

	/**
	 * Merges the engine specific edge hash into the existing metadata
	 * @param metaData						The existing metadata
	 * @param newEdgeHash					The edge hash to merge into the existing meta data. The edge hash contains the 
	 * 										query struct names for the query input
	 * 										Example edge hash is:
	 * 										{ Title -> [Title__Movie_Budget, Studio] } ; where Movie_Budget is a property on Title
	 * @param engine						The engine where all the columns in the edge hash come from
	 * @param makeUniqueNameMap 
	 * @param joinCols						The join columns for the merging
	 * 										This enables that we can declare columns to be equivalent between the existing frame
	 * 										and those we are going to add to the frame via the merge without them needing to be 
	 * 										exact matches
	 * @return								Return a map array containing the following
	 * 										index 0: this map contains a clean version of the edgeHash. the clean version is the 
	 * 											edge hash contains all the logical names (display names) as defined by the engine.
	 * 										index 1: this map contains the logical name (matching that in the clean edge hash at
	 * 											index 0 of the map array) pointing to the unique name of the column within the 
	 * 											metadata
	 */
	public static Map[] mergeQSEdgeHash(IMetaData metaData, Map<String, Set<String>> newEdgeHash, IEngine engine, List<Map<String,String>> joinColList, Map<String, Boolean> makeUniqueNameMap) {
		// if the engine is null, we will just grab the local master
		if(engine == null) {
			engine = (IEngine) DIHelper.getInstance().getLocalProp(Constants.LOCAL_MASTER_DB_NAME);
		}
		
		// the clean hash will contain the clean edge hash
		// this is index 0 of the returned map array
		Map<String, Set<String>> cleanedHash = new HashMap<String, Set<String>>();
		
		// this is the logical name to unique name map
		// this is index 1 of the returned map array
		Map<String, String> logicalToUniqueName = new HashMap<String, String>();
		
		// retrieve the unique name and unique parent name (if property) to be associated with each physical name in 
		// the query struct edge hash
		// using the example newEdgeHash above, the following would be in the physicalToLocal map
		// {
		//	Title -> [Title, null],
		//	Title__Movie_Budget => [Movie_Budget, Title],
		//	Studio -> [Studio, null]
		// }
		Map<String, String[]> physicalToLogical = metaData.getPhysical2LogicalTranslations(newEdgeHash, joinColList, makeUniqueNameMap);
		
		/*
		 * Logic for loop around edge hash:
		 * ----THIS PORTION OCCURS FOR EACH KEY IN THE EDGE HASH----
		 * 1. grab each key in the edge hash. each key corresponds to a node
		 * 2. get the physical name for the node corresponding to the key from the physicalToLogical map
		 * 3. store the node corresponding to the key in the tinker metadata
		 * 4. store the values necessary for the map[] return
		 * 
		 * ----THIS PORTION OCCURS FOR EACH ENTITY IN THE SET CORRESPONDING TO THE KEY IN THE EDGE HASH----
		 * 5. get the set of values for the inputed key. this corresponds to the downstream nodes to add in the tinker meta to the key
		 * 6. for each downstream node, get the physical name corresponding to the key from the physicalToLogical map
		 * 7. store each node in the tinker metadata
		 * 8. store the relationship from the key to each entity in the value set in the tinker meta
		 * 9. store the values necessary for the map[] return
		 */
		
		// 1) looping through to grab each key in the edge hash
		for(String newNodeKey : newEdgeHash.keySet()) {
			
			// 2) get the physical name and the physical parent name if property
			String outUniqueName = physicalToLogical.get(newNodeKey)[0];
			String outUniqueParentNameIfProperty = physicalToLogical.get(newNodeKey)[1];
			
			// 3) create the meta vertex associated with newNodeKey
			metaData.storeEngineDefinedVertex(outUniqueName, outUniqueParentNameIfProperty, engine.getEngineName(), newNodeKey);
			
			// 4) store the clean values of the edge hash and store the local to unique name
			Set<String> cleanSet = new HashSet<String>();
			String logicalOutName = metaData.getLogicalNameForUniqueName(outUniqueName, engine.getEngineName());
			logicalToUniqueName.put(logicalOutName, outUniqueName);
			cleanedHash.put(logicalOutName, cleanSet);

			// 5) grab the in downstream nodes
			Set<String> inNodesSet = newEdgeHash.get(newNodeKey);
			
			// need to iterate through all the in nodes
			if (inNodesSet != null && !inNodesSet.isEmpty()) {
				for (String inVertS : inNodesSet) {
					// 6) get the physical name and the physical parent name if property
					String inUniqueName = physicalToLogical.get(inVertS)[0];
					String inUniqueParentNameIfProperty = physicalToLogical.get(inVertS)[1];

					// 7) create the meta vertex associated with the inVertS
					metaData.storeEngineDefinedVertex(inUniqueName, inUniqueParentNameIfProperty, engine.getEngineName(), inVertS);
					// 8) create the relationship between the newNodeKey and the inVertS
					metaData.storeRelation(outUniqueName, inUniqueName);

					// 9) store the clean values of the edge hash and store the local to unique name
					String logicalInName = metaData.getLogicalNameForUniqueName(inUniqueName, engine.getEngineName());
					cleanSet.add(logicalInName);
					logicalToUniqueName.put(logicalInName, inUniqueName);
				}
			}
		}
		
		// create and then return the map array
		Map[] retMap = new Map[]{cleanedHash, logicalToUniqueName};
		return retMap;
	}
	
	/**
	 * 
	 * @param headers
	 * @return
	 * 
	 * Creates a default edge hash for graphs that have no metamodel, for example when creating a TinkerFrame from a csv
	 * 
	 * returns {PRIM_KEY -> col_1, col_2, ... col_N}
	 */
	public static Map<String, Set<String>> createPrimKeyEdgeHash(String[] headers) {
		Set<String> primKeyEdges = new LinkedHashSet<>();
		for(String header : headers) {
			primKeyEdges.add(header);
		}
		Map<String, Set<String>> edges = new LinkedHashMap<String, Set<String>>();
		edges.put(TinkerFrame.PRIM_KEY + "_" + Arrays.toString(headers).hashCode(), primKeyEdges);
		return edges;
	}

	/**
	 * 
	 * @param nodes
	 * @return
	 * 
	 * return the primary key needed for a primary key vertex downstream from all of nodes
	 */
	public static String getPrimaryKey(Object[] nodes) {
		String[] strings = new String[nodes.length];
		for(int i = 0; i < nodes.length; i++) {
			strings[i] = nodes[i].toString();
		}
		String primKeyString = "";
		Arrays.sort(strings);
		for(String s : strings) {
			primKeyString += s + TinkerFrame.PRIM_KEY_DELIMETER;
		}
		
		//hash the primKeyString
//		return primKeyString.hashCode()+"";
		return primKeyString;
	}
	
	/**
	 * 
	 * @param nodes
	 * @return
	 */
	public static String getMetaPrimaryKeyName(String... nodes) {
		// due to sorting, create a new list such that we don't mess up the ordering
		String[] newNodes = new String[nodes.length];
		for(int i = 0; i < nodes.length; i++) {
			newNodes[i] = nodes[i];
		}
		Arrays.sort(newNodes);
		String primKey = TinkerFrame.PRIM_KEY;
		for(String node : newNodes) {
			primKey += node + TinkerFrame.PRIM_KEY_DELIMETER;
		}
		return primKey;
	}
	
	/**
	 * 
	 * @param newEdgeHash
	 * 
	 * new EdgeHash in the form:
	 * 		{
	 * 			a : <b, c, d>,
	 * 			b : <x, y, z>
	 * 		}
	 * where key is column name and set is the columns key links to
	 * 
	 * This method takes the parameter edge hash information and incorporates it into the META graph incorporated within the graph
	 * 
	 * node2Value can be null or empty!!!! it is only if you want to set the value for your meta nodes
	 */
	public static void mergeEdgeHash(IMetaData metaData, Map<String, Set<String>> newEdgeHash, Map<String, String> node2Value) {
		for(String newNode : newEdgeHash.keySet()) {
			//grab the edges
			Set<String> edges = newEdgeHash.get(newNode);
			
			//grab/create the meta vertex associated with newNode
			String value = newNode;
			if(node2Value != null && node2Value.containsKey(newNode)){
				value = node2Value.get(newNode);
			}
			metaData.storeVertex(newNode, value, null);
			if(newNode.startsWith(TinkerFrame.PRIM_KEY)) {
				metaData.setPrimKey(newNode, true);
			}
			
			//for each edge in corresponding with newNode create the connection within the META graph
			for(String inVertString : edges){
				// now to insert the meta edge
				String value2 = inVertString;
				if(node2Value != null && node2Value.containsKey(inVertString)){
					value2 = node2Value.get(inVertString);
				}
				metaData.storeVertex(inVertString, value2, newNode);
				if(inVertString.startsWith(TinkerFrame.PRIM_KEY)) {
					metaData.setPrimKey(inVertString, true);
				}
				metaData.storeRelation(newNode, inVertString);
			}
		}
	}

	public static void mergeEdgeHash(IMetaData metaData, Map<String, Set<String>> newEdgeHash) {
		TinkerMetaHelper.mergeEdgeHash(metaData, newEdgeHash, null);
	}
}
