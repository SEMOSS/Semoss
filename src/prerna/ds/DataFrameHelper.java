package prerna.ds;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.h2.H2Frame;
import prerna.engine.api.IHeadersDataRow;
import prerna.util.Utility;

public class DataFrameHelper {

	private static final Logger LOGGER = LogManager.getLogger(DataFrameHelper.class.getName());
	
	public static void removeData(ITableDataFrame frame, Iterator<IHeadersDataRow> it) {
		if(frame instanceof H2Frame) {
			while(it.hasNext()){
				IHeadersDataRow row = it.next();
				frame.removeRelationship(row.getHeaders(), row.getValues());
			}
		} else if(frame instanceof TinkerFrame) {
			IMetaData metaData = ((TinkerFrame)frame).metaData;
			String[] columnHeaders = frame.getColumnHeaders();
			H2Frame tempFrame = TableDataFrameFactory.convertToH2Frame(frame);
			while(it.hasNext()){
				IHeadersDataRow row = it.next();
				tempFrame.removeRelationship(row.getHeaders(), row.getValues());
			}
			
			TinkerFrame tframe = new TinkerFrame();
			tframe.metaData = metaData;
			Iterator<Object[]> iterator = tempFrame.iterator();
			while(iterator.hasNext()) {
				Map<String, Object> nextRow = new HashMap<String, Object>();
				Object[] row = iterator.next();
				for(int i = 0; i < row.length; i++) {
					nextRow.put(columnHeaders[i], row[i]);
				}
				tframe.addRelationship(nextRow);
			}
			((TinkerFrame) frame).g= tframe.g;
		}
	}
	
	
	public static TinkerFrame generateNewGraph(TinkerFrame tf, String relationshipStr, String traversalStr) {
		// get the new edge hash
		Map<String, Set<String>> edgeHash = generateEdgeHashFromStr(relationshipStr);
		List<Map<String, Set<String>>> traversalHash = generateListOfEdgeHashFromStr(traversalStr);
		return generateNewGraph(tf, edgeHash, traversalHash);
	}
	
	public static TinkerFrame generateNewGraph(TinkerFrame tf, Map<String, Set<String>> edgeHash, List<Map<String, Set<String>>> traversalHash) {
		// generate the new tinker frame and create its metadata
		TinkerFrame newTf = new TinkerFrame();

		// we need to get the unique names to the values
		Map<String, String> uniqueNameToValue = tf.metaData.getAllUniqueNamesToValues();
		
		// we need to get the data types
		// and need to get the relationships
		// we will also store a list of all the selector types for use when we need to insert any node 
		// that matches the type we want but is not connected to anything
		Set<String> allTypes = new HashSet<String>();
		for(String col : edgeHash.keySet()) {
			allTypes.add(uniqueNameToValue.get(col));
			newTf.metaData.storeVertex(col, uniqueNameToValue.get(col), tf.metaData.getParentValueOfUniqueNode(col));
			newTf.metaData.storeDataType(col, Utility.convertDataTypeToString( tf.metaData.getDataType(col)));
			
			Set<String> otherCols = edgeHash.get(col);
			for(String otherCol : otherCols) {
				allTypes.add(uniqueNameToValue.get(otherCol));
				newTf.metaData.storeVertex(otherCol, uniqueNameToValue.get(otherCol), tf.metaData.getParentValueOfUniqueNode(otherCol));
				newTf.metaData.storeDataType(otherCol, Utility.convertDataTypeToString( tf.metaData.getDataType(otherCol)));
				newTf.metaData.storeRelation(col, otherCol);
			}
		}
		
		// loop through the traversal hash to get the appropriate queries and insert those into the frame
		for(Map<String, Set<String>> traversal : traversalHash) {
			// go through the general stuff to create a graph traversal
			// nothing special, uses the match logic present throughout all the other gremlin iterators
			List<GraphTraversal<Object, Vertex>> gtTraversals = new Vector<GraphTraversal<Object, Vertex>>();
			List<String> travelledEdges = new Vector<String>();
			
			// select an arbitrary start type from the defined edge hash
			String startName = traversal.keySet().iterator().next();
			String startType = uniqueNameToValue.get(startName);
			GraphTraversal gt = tf.g.traversal().V().has(TinkerFrame.TINKER_TYPE, startType).as(startName);
			
			// add the logic to traverse the desired path
			gtTraversals = visitNode(gt, startName, uniqueNameToValue, traversal, travelledEdges, gtTraversals);
			if(gtTraversals.size()>0){
				GraphTraversal[] array = new GraphTraversal[gtTraversals.size()];
				gt = gt.match(gtTraversals.toArray(array));
			}
			
			// okay, now we need to figure out which selectors to return
			// this is basically going through all the nodes in the traversal map
			// and comparing it to all the values in the edge hash
			Set<String> allUsedNodesInTraversal = new HashSet<String>();
			for(String tNode : traversal.keySet()) {
				// add the from node
				allUsedNodesInTraversal.add(tNode);
				// add its set of to nodes and let the set remove duplicates
				allUsedNodesInTraversal.addAll(traversal.get(tNode));
			}
			
			// create the appropriate set of selectors
			// by comparing the traversal return and the edge hash
			List<String> gSelectors = new Vector<String>();
			for(String eNode : edgeHash.keySet()) {
				// if this node hasn't already been added
				if(!gSelectors.contains(eNode)) {
					// and this node is returned in this specific traversal
					if(allUsedNodesInTraversal.contains(eNode)) {
						// add the node
						gSelectors.add(eNode);
					}
				}
				
				Set<String> otherENodes = edgeHash.get(eNode);
				for(String otherE : otherENodes) {
					// if this node hasn't already been added
					if(!gSelectors.contains(otherE)) {
						// and this node is returned in this specific traversal
						if(allUsedNodesInTraversal.contains(otherE)) {
							// add the node
							gSelectors.add(otherE);
						}
					}
				}
			}
			
			int numGSelectors = gSelectors.size();
			// now we add the selectors
			if(numGSelectors == 1) {
				gt = gt.select(gSelectors.get(0));
			} else if(numGSelectors == 2) {
				gt = gt.select(gSelectors.get(0), gSelectors.get(1));
			} else if(numGSelectors > 2){
				String ret1 = gSelectors.remove(0);
				String ret2 = gSelectors.remove(1);
				gt = gt.select(ret1, ret2, gSelectors.toArray(new String[]{}));
				
				// add back the removed columns so we know the headers
				gSelectors.add(0, ret2);
				gSelectors.add(0, ret1);
			} else {
				LOGGER.info(">>>>>> FOUND 0 SELECTORS!!!");
			}
			
			// now that we have the selectors done
			// we need to figure out the connections
			// need to compare all the nodes in the selectors to each other and see if there is a
			// connection in the edge hash
			Map<Integer, Set<Integer>> cardinality = new Hashtable<Integer, Set<Integer>>();
			// loop through all the values in the edge hash
			for(String eNode : edgeHash.keySet()) {
				// we need a valid connection to insert into the cardinality
				if(gSelectors.contains(eNode)) {
					// we found the from node
					int toIndex = gSelectors.indexOf(eNode);
					// now we need to find a to node that is also returned
					Set<String> otherENodes = edgeHash.get(eNode);
					for(String otherE : otherENodes) {
						// now compare and make sure a downstream relationship is there to add
						if(gSelectors.contains(otherE)) {
							// we found the to node
							int fromIndex = gSelectors.indexOf(otherE);
							
							// now we add to cardinality
							if(cardinality.containsKey(toIndex)) {
								Set<Integer> downstreamSet = cardinality.get(toIndex);
								downstreamSet.add(fromIndex);
							} else {
								Set<Integer> downstreamSet = new HashSet<Integer>();
								downstreamSet.add(fromIndex);
								cardinality.put(toIndex, downstreamSet);
							}
							
						}
					}
				}
			}
			
			// YAYYY!!! After all of that matching and loop
			// we can finally insert
			
			// well, we want to pass in the headers as being the logical headers
			// which are those which are transformed
			// and the gSelectors are the unqiue names
			String[] logicalToUnqiueArr = gSelectors.toArray(new String[]{});
			String[] headers = new String[numGSelectors];
			for(int i = 0; i < numGSelectors; i++) {
				headers[i] = uniqueNameToValue.get(logicalToUnqiueArr[i]);
			}
			while(gt.hasNext()) {
				Map<String, Object> row = (Map<String, Object>) gt.next();
				Object[] values = new Object[gSelectors.size()];
				for(int i = 0; i < numGSelectors; i++) {
					values[i] = ((Vertex) row.get(gSelectors.get(i))).property(TinkerFrame.TINKER_NAME).value();
				}
				
				newTf.addRelationship(headers, values, cardinality, logicalToUnqiueArr);
			}
		}
		
		
		// need to also go through and add any "single" vertex of the types in the selectors
		// we add everything even if the vertex has no edge
				
		// cardinality is just an empty set
		Hashtable<Integer, Set<Integer>> cardinality = new Hashtable<Integer, Set<Integer>>();
		cardinality.put(0, new HashSet<Integer>());
		
		GraphTraversal<Vertex, Vertex> vertIt = tf.g.traversal().V().has(TinkerFrame.TINKER_TYPE, P.within(allTypes));
		while(vertIt.hasNext()) {
			Vertex vert = vertIt.next();
			String type = vert.value(TinkerFrame.TINKER_TYPE);
			Object value = vert.value(TinkerFrame.TINKER_NAME);
			
			String[] headers = {type};
			Object[] values = {value};
			Hashtable<String, String> logicalToTypeMap = new Hashtable<String, String>();
			logicalToTypeMap.put(type, type);
			
			newTf.addRelationship(headers, values, cardinality, logicalToTypeMap);
		}
		
		// update the data id
		int currId = tf.getDataId();
		for(int i = 0; i <= currId; i++) {
			newTf.updateDataId();
		}		
		
		return newTf;
	}

	/**
	 * Used to shift a given node to a property on another node
	 * @param tf
	 * @param conceptName
	 * @param propertyName
	 * @param edgeHash
	 */
	public static void shiftToNodeProperty(
			TinkerFrame tf, 
			String conceptName, 
			String propertyName,
			String edgeHashStr) 
	{
		Map<String, Set<String>> edgeHash = generateEdgeHashFromStr(edgeHashStr);
		shiftToNodeProperty(tf, conceptName, propertyName, edgeHash);
	}
	
	/**
	 * Used to shift a given node to a property on another node
	 * @param tf
	 * @param conceptName
	 * @param propertyName
	 * @param edgeHash
	 */
	public static void shiftToNodeProperty(
			TinkerFrame tf, 
			String conceptName, 
			String propertyName,
			Map<String, Set<String>> edgeHash) 
	{
		List<GraphTraversal<Object, Vertex>> traversals = new Vector<GraphTraversal<Object, Vertex>>();
		List<String> travelledEdges = new Vector<String>();

		// to allow for loops
		// we need the user to pass in the information using the unique names
		Map<String, String> uniqueNameToValue = tf.metaData.getAllUniqueNamesToValues();
		
		// select an arbitrary start type from the defined edge hash
		String startName = edgeHash.keySet().iterator().next();
		String startType = uniqueNameToValue.get(startName);
		GraphTraversal gt = tf.g.traversal().V().has(TinkerFrame.TINKER_TYPE, startType).as(uniqueNameToValue.get(startName));
		
		// add the logic to traverse the desired path
		traversals = visitNode(gt, startName, uniqueNameToValue, edgeHash, travelledEdges, traversals);
		if(traversals.size()>0){
			GraphTraversal[] array = new GraphTraversal[traversals.size()];
			gt = gt.match(traversals.toArray(array));
		}
		
		// only need to return the concept and the property
        gt = gt.select(conceptName, propertyName);
        
        while(gt.hasNext()) {
        	Map<String, Object> path = (Map<String, Object>) gt.next();
        	Vertex conceptVertex = (Vertex) path.get(conceptName);
        	Vertex propertyVertex = (Vertex) path.get(propertyName);
        	
        	// set the property name as a vertex on the concept
        	conceptVertex.property(propertyName, propertyVertex.value(TinkerFrame.TINKER_NAME));
        }
        
        // now remove the property as a node
        // we can only do this at the end because the same property vertex may be shared in the above traversal
        // so we can't remove until the end
        tf.removeColumn(propertyName);
	}
	
	public static void shiftToEdgeProperty(
			TinkerFrame tf, 
			String relationshipStr, 
			String propertyName,
			String edgeHashStr) 
	{
		String[] relationship = relationshipStr.split("\\.");
		Map<String, Set<String>> edgeHash = generateEdgeHashFromStr(edgeHashStr);
		shiftToEdgeProperty(tf, relationship, propertyName, edgeHash);
	}
	
	public static void shiftToEdgeProperty(
			TinkerFrame tf, 
			String[] relationship, 
			String propertyName,
			Map<String, Set<String>> edgeHash) 
	{
		List<GraphTraversal<Object, Vertex>> traversals = new Vector<GraphTraversal<Object, Vertex>>();
		List<String> travelledEdges = new Vector<String>();
		
		// to allow for loops
		// we need the user to pass in the information using the unique names
		Map<String, String> uniqueNameToValue = tf.metaData.getAllUniqueNamesToValues();
		
		// select an arbitrary start type from the defined edge hash
		String startName = edgeHash.keySet().iterator().next();
		String startType = uniqueNameToValue.get(startName);
		GraphTraversal gt = tf.g.traversal().V().has(TinkerFrame.TINKER_TYPE, startType).as(uniqueNameToValue.get(startName));
		
		// add the logic to traverse the desired path
		traversals = visitNode(gt, startName, uniqueNameToValue, edgeHash, travelledEdges, traversals);
		if(traversals.size()>0){
			GraphTraversal[] array = new GraphTraversal[traversals.size()];
			gt = gt.match(traversals.toArray(array));
		}
		
		String startSelector = relationship[0];
		String endSelector = relationship[1];
		// only need to return the concept and the property
        gt = gt.select(relationship[0], relationship[1], propertyName);
        
        while(gt.hasNext()) {
        	Map<String, Object> path = (Map<String, Object>) gt.next();
        	Vertex startVertex = (Vertex) path.get(startSelector);
        	Vertex endVertex = (Vertex) path.get(endSelector);
        	Vertex propertyVertex = (Vertex) path.get(propertyName);
        	
        	// remember, the start and end being passed are the unique names to allow for loops!
        	String edgeLabel = startSelector + TinkerFrame.EDGE_LABEL_DELIMETER + endSelector;

        	Iterator<Edge> edgeIt = startVertex.edges(Direction.OUT, edgeLabel);
        	boolean foundEdge = false;
        	while(edgeIt.hasNext()) {
        		Edge e = edgeIt.next();
        		if(e.inVertex().equals(endVertex)) {
        			foundEdge = true;
        			e.property(propertyName,  propertyVertex.value(TinkerFrame.TINKER_NAME));
        		}
        	}
        	
        	if(!foundEdge) {
        		// if not found, then we will just make a new edge and add the property onto it
        		String edgeId = edgeLabel + "/" + startVertex.value(TinkerFrame.TINKER_NAME) + ":" + endVertex.value(TinkerFrame.TINKER_NAME);
        		Edge e = startVertex.addEdge(edgeLabel, endVertex, TinkerFrame.TINKER_ID, edgeId, TinkerFrame.TINKER_COUNT, 1);
    			e.property(propertyName,  propertyVertex.value(TinkerFrame.TINKER_NAME));
        	}
        }
        
        // now remove the property as a node
        // we can only do this at the end because the same property vertex may be shared in the above traversal
        // so we can't remove until the end
        tf.removeColumn(propertyName);
	}
	
	private static List<GraphTraversal<Object, Vertex>> visitNode(GraphTraversal gt, String startName, Map<String, String> uniqueNameToValue, Map<String, Set<String>> edgeHash, List<String> travelledEdges, List<GraphTraversal<Object, Vertex>> traversals) {
		// first see if there are downstream nodes
		if(edgeHash.containsKey(startName)) {
			Iterator<String> downstreamIt = edgeHash.get(startName).iterator();
			while (downstreamIt.hasNext()) {
				// for each downstream node of this node
				String downstreamNodeName = downstreamIt.next();
				String downstreamNodeType = uniqueNameToValue.get(downstreamNodeName);

				String edgeKey = startName + TinkerFrame.EDGE_LABEL_DELIMETER + downstreamNodeName;
				if (!travelledEdges.contains(edgeKey)) {
					LOGGER.info("travelling from node = '" + startName + "' to node = '" + downstreamNodeName + "'");
					
					// get the traversal and store the necessary info
					GraphTraversal<Object, Vertex> twoStepT = __.as(startName).out(edgeKey).has(TinkerFrame.TINKER_TYPE, downstreamNodeType).as(downstreamNodeName);
					traversals.add(twoStepT);
					travelledEdges.add(edgeKey);
					
					// recursively travel as far downstream as possible
					traversals = visitNode(gt, downstreamNodeName, uniqueNameToValue, edgeHash, travelledEdges, traversals);
				}
			}
		}
		
		// do the same thing for upstream
		// slightly more annoying to get upstream nodes...
		Set<String> upstreamNodes = getUpstreamNodes(startName, edgeHash);
		if(upstreamNodes != null && !upstreamNodes.isEmpty()) {
			Iterator<String> upstreamIt = upstreamNodes.iterator();
			while(upstreamIt.hasNext()) {
				String upstreamNodeName = upstreamIt.next();
				String upstreamNodeType = uniqueNameToValue.get(upstreamNodeName);

				String edgeKey = upstreamNodeName + TinkerFrame.EDGE_LABEL_DELIMETER + startName;
				if (!travelledEdges.contains(edgeKey)) {
					LOGGER.info("travelling from node = '" + upstreamNodeName + "' to node = '" + startName + "'");

					// get the traversal and store the necessary info
					GraphTraversal<Object, Vertex> twoStepT = __.as(startName).in(edgeKey).has(TinkerFrame.TINKER_TYPE, upstreamNodeType).as(upstreamNodeName);
					traversals.add(twoStepT);
					travelledEdges.add(edgeKey);
					
					// recursively travel as far upstream as possible
					traversals = visitNode(gt, upstreamNodeName,uniqueNameToValue, edgeHash, travelledEdges, traversals);
				}
			}
		}
		
		return traversals;
	}

	/**
	 * Get the upstream nodes for a given downstream node
	 * @param downstreamNodeToFind
	 * @param edgeHash
	 * @return
	 */
	private static Set<String> getUpstreamNodes(String downstreamNodeToFind, Map<String, Set<String>> edgeHash) {
		Set<String> upstreamNodes = new HashSet<String>();
		for(String possibleUpstreamNode : edgeHash.keySet()) {
			Set<String> downstreamNodes = edgeHash.get(possibleUpstreamNode);
			if(downstreamNodes.contains(downstreamNodeToFind)) {
				// the node we want to find is listed as downstream
				upstreamNodes.add(possibleUpstreamNode);
			}
		}
		
		return upstreamNodes;
	}
	
	private static Map<String, Set<String>> generateEdgeHashFromStr(String edgeHashStr) {
		Map<String, Set<String>> edgeHash = new Hashtable<String, Set<String>>();
		// each path is separated by a semicolon
		String[] paths = edgeHashStr.split(";");
		for(String path : paths) {
			if(path.contains(".")) {
				String[] pathVertex = path.split("\\.");
				// we start at index 1 and take the index prior for ease of looping
				for(int i = 1; i < pathVertex.length; i++) {
					String startNode = pathVertex[i-1];
					String endNode = pathVertex[i];
					
					// update the edge hash correctly
					Set<String> downstreamNodes = null;
					if(edgeHash.containsKey(startNode)) {
						downstreamNodes = edgeHash.get(startNode);
						downstreamNodes.add(endNode);
					} else {
						downstreamNodes = new HashSet<String>();
						downstreamNodes.add(endNode);
						edgeHash.put(startNode, downstreamNodes);
					}
				}
			} else {
				// ugh... when would this happen?
			}
		}
		return edgeHash;
	}

	
	private static List<Map<String, Set<String>>> generateListOfEdgeHashFromStr(String listEdgeHashStr) {
		List<Map<String, Set<String>>> edgeHashList = new Vector<Map<String, Set<String>>>();
		// the list of edge hash's is delimited by +++
		String[] edgeHashArr = listEdgeHashStr.split("\\+\\+\\+");
		for(String edgeHashStr : edgeHashArr) {
			Map<String, Set<String>> edgeHash = generateEdgeHashFromStr(edgeHashStr);
			edgeHashList.add(edgeHash);
		}
		
		return edgeHashList;
	}

	public static TinkerFrame findSharedVertices(TinkerFrame tf, String type, String[] instances, int numTraversals) {
		// keep set of all vertices to keep
		Set<Vertex> instancesToKeep = new HashSet<Vertex>();
		
		int numInstances = instances.length;
		for(int index = 0; index < numInstances; index++) {
			String instance = instances[index];
			// find set of end positions
			String[] instancesToBind = new String[numInstances-1];
			int counter = 0;
			for(int i = 0; i < numInstances; i++) {
				if(index != i) {
					instancesToBind[counter] = instances[i];
					counter++;
				}
			}
			
			GraphTraversal t1 = tf.g.traversal().V().has(TinkerFrame.TINKER_TYPE, type).has(TinkerFrame.TINKER_NAME, instance).as("start");
			for(int i = 0; i < numTraversals; i++) {
				if(i == 0) {
					t1 = t1.both().as( (char) i + "");
				} else if(i == 1) {
					t1 = t1.both().as( (char) i + "").where( (char) i + "", P.neq("start"));
				} else {
					t1 = t1.both().as( (char) i + "").where( (char) i + "", P.neq((char) (i-2) + ""));
				}
			}
			t1 = t1.has(TinkerFrame.TINKER_NAME, P.within(instancesToBind));
			if(numTraversals == 1) {
				t1 = t1.select("start", (char) 0 + "");
			} else if(numTraversals >= 2) {
				String[] degreesToSelect = new String[numTraversals - 1];
				for(int degreeCount = 1; degreeCount < numTraversals; degreeCount++) {
					degreesToSelect[degreeCount-1] = (char) degreeCount + "";
				}
				t1 = t1.select("start", (char) 0 + "", degreesToSelect);
			}
			
			while(t1.hasNext()) {
				StringBuilder linkage = new StringBuilder();
				Object data = t1.next();
				if(data instanceof Map) {
					Vertex start = (Vertex) ((Map) data).get("start");
					instancesToKeep.add(start);
					linkage.append(start.value(TinkerFrame.TINKER_NAME) + "").append(" ->");
					for(int i = 0; i < numTraversals; i++) {
						Vertex v = (Vertex) ((Map) data).get( (char) i + "");
						instancesToKeep.add(v);
						linkage.append(v.value(TinkerFrame.TINKER_NAME) + "").append(" ->");
					}
				} else {
					System.err.println("Ughhh.... shouldn't get here");
				}
				
				System.out.println(linkage.toString());
			}
		}
		
		if(instancesToKeep.isEmpty()) {
			throw new IllegalStateException("Cannot create any path given the instances and the degrees of separation");
		}
		
		// store types filtered out
		Set<String> typesFiltered = new HashSet<String>();
		
		GraphTraversal<Vertex, Vertex> traversal = tf.g.traversal().V().not(__.has(TinkerFrame.TINKER_TYPE, TinkerFrame.TINKER_FILTER));
		Vertex filterNode = tf.upsertVertex(TinkerFrame.TINKER_FILTER, TinkerFrame.TINKER_FILTER);
		while(traversal.hasNext()) {
			Vertex vert = traversal.next();
			if(!instancesToKeep.contains(vert)) {
				// we want to filter this
				String vertType = vert.value(TinkerFrame.TINKER_TYPE);
				tf.upsertEdge(filterNode, TinkerFrame.TINKER_FILTER, vert, vertType);
				
				typesFiltered.add(vertType);
			}
		}
		
		// set them filtered at meta level
		Map<String, String> metaToType = tf.metaData.getAllUniqueNamesToValues();
		for(String uniqueName : metaToType.keySet()) {
			if(typesFiltered.contains( metaToType.get(uniqueName)) ) {
				tf.metaData.setFiltered(uniqueName, true);
			}
		}
		
		return tf;
	}
	
}
