package prerna.ds;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.algorithm.api.IMetaData;
import prerna.engine.api.IEngine;
import prerna.sablecc.PKQLEnum;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class TinkerMetaHelper {


	public static Map[] mergeQSEdgeHash(IMetaData metaData, Map<String, Set<String>> newEdgeHash, IEngine engine, List<Map<String,String>> joinColList) {
		if(engine == null) {
			engine = (IEngine) DIHelper.getInstance().getLocalProp(Constants.LOCAL_MASTER_DB_NAME);
		}
		Map<String, Set<String>> cleanedHash = new HashMap<String, Set<String>>();
		Set<String> newLevels = new LinkedHashSet<String>();
		Map<String, String[]> physicalToLogical = metaData.getPhysical2LogicalTranslations(newEdgeHash, joinColList);
		
		Map<String, String> logicalToValue = new HashMap<String, String>();
		
		for(String newNodeKey : newEdgeHash.keySet()) {
			
			String outUniqueName = physicalToLogical.get(newNodeKey)[0];
			String outConceptUniqueName = physicalToLogical.get(newNodeKey)[1];
			//grab the edges
			Set<String> edges = newEdgeHash.get(newNodeKey);
			//collect the column headers
			newLevels.add(outUniqueName);
			
			//grab/create the meta vertex associated with newNode
			metaData.storeEngineDefinedVertex(outUniqueName, outConceptUniqueName, engine.getEngineName(), newNodeKey);
			
			Set<String> cleanSet = new HashSet<String>();
			String logicalOutName = metaData.getLogicalNameForUniqueName(outUniqueName, engine.getEngineName());
			logicalToValue.put(logicalOutName, metaData.getValueForUniqueName(outUniqueName));

			cleanedHash.put(logicalOutName, cleanSet);

			// for each edge in corresponding with newNode create the connection within the META graph
			if (edges != null && !edges.isEmpty()) {
				for (String inVertS : edges) {

					String inUniqueName = physicalToLogical.get(inVertS)[0];
					String inConceptUniqueName = physicalToLogical.get(inVertS)[1];

					newLevels.add(inUniqueName);

					// now to insert the meta edge Vertex inVert = this.metaData.upsertVertex(META,
					metaData.storeEngineDefinedVertex(inUniqueName, inConceptUniqueName, engine.getEngineName(), inVertS);
					metaData.storeRelation(outUniqueName, inUniqueName);

					String logicalInName = metaData.getLogicalNameForUniqueName(inUniqueName, engine.getEngineName());
					cleanSet.add(logicalInName);
					
					logicalToValue.put(logicalInName, metaData.getValueForUniqueName(inUniqueName));
				}
			}
		}
//		// need to make sure prim key is not added as header
//		Iterator<String> newLevelsIt = newLevels.iterator();
//		while(newLevelsIt.hasNext()) {
//			if(newLevelsIt.next().startsWith(PRIM_KEY)) {
//				newLevelsIt.remove();
//			}
//		}
//		redoLevels(newLevels.toArray(new String[newLevels.size()]));
		
		Map[] retMap = new Map[]{cleanedHash, logicalToValue};
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
		Map<String, Set<String>> edges = new HashMap<String, Set<String>>();
		edges.put(TinkerFrame.PRIM_KEY + "_" + headers.toString(), primKeyEdges);
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
			primKeyString += s + TinkerFrame.primKeyDelimeter;
		}
		
		//hash the primKeyString
//		return primKeyString.hashCode()+"";
		return primKeyString;
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
