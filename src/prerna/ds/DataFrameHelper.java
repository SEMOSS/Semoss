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
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;

public class DataFrameHelper {

	private static final Logger LOGGER = LogManager.getLogger(DataFrameHelper.class.getName());
	
	public static void removeData(ITableDataFrame frame, ISelectWrapper it) {
		if(frame instanceof H2Frame) {
			
			while(it.hasNext()){
				ISelectStatement ss = (ISelectStatement) it.next();
				System.out.println(((ISelectStatement)ss).getPropHash());
				frame.removeRelationship(ss.getPropHash());
			}
			
		} else if(frame instanceof TinkerFrame) {
			
			IMetaData metaData = ((TinkerFrame)frame).metaData;
			String[] columnHeaders = frame.getColumnHeaders();
			H2Frame tempFrame = TableDataFrameFactory.convertToH2Frame(frame);
			while(it.hasNext()){
				ISelectStatement ss = (ISelectStatement) it.next();
				System.out.println(((ISelectStatement)ss).getPropHash());
				tempFrame.removeRelationship(ss.getPropHash());
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
	
	
	public static TinkerFrame generateNewGraph(TinkerFrame tf, String[] selectors, Map<String, String> edgeTraversals) {
		// 1) get the paths
		List<String[]> paths = determineTraversals(selectors, edgeTraversals);
		
		// 2) create a new tinkerframe
		// define the new metadata based on the path end points
		TinkerFrame newTf = new TinkerFrame();
		// we need to create an edge hash to do this
		Map<String, Set<String>> newEdgeHash = new Hashtable<String, Set<String>>();
		Map<String, String> newDataTypes = new Hashtable<String, String>();
		
		for(String[] path : paths) {
			// the end points for each path because the valid connections for the new edge hash
			String start = path[0];
			String end = path[path.length-1];
			
			Set<String> endPoints = null;
			if(newEdgeHash.containsKey(start)) {
				endPoints = newEdgeHash.get(start);
			} else {
				endPoints = new HashSet<String>();
			}
			endPoints.add(end);
			newEdgeHash.put(start, endPoints);
			
			newDataTypes.put(start, Utility.convertDataTypeToString(tf.getDataType(start)) );
			newDataTypes.put(end, Utility.convertDataTypeToString(tf.getDataType(end)) );
		}
		
		newTf.mergeEdgeHash(newEdgeHash, newDataTypes);
		
		// 3) loop through all the paths to get the required vertices and their connections
		// will make new edges between the nodes if we are skipping intermediates
		
		// since we are only adding one path at a time
		// the cardinality will always be the first vertex to the second vertex
		Map<Integer, Set<Integer>> cardinality = new Hashtable<Integer, Set<Integer>>();
		Set<Integer> index = new HashSet<Integer>();
		index.add(1);
		cardinality.put(0, index);
		
		for(String[] path : paths) {
			String[] headers = {path[0], path[path.length-1]};

			// keep track to ensure names are unique in a given path
			Set<String> uniqueNames = new HashSet<String>();

			// create the traversal
			// will go through each vertex in the path for the traversal
			GraphTraversal gt = tf.g.traversal().V();
			String startType = path[0];
			gt = gt.has(TinkerFrame.TINKER_TYPE, startType).as(startType);
			uniqueNames.add(startType);
			String[] retVars = new String[2];
			retVars[0] = startType;
			
			int i = 1;
			int numTraverse = path.length;
			for(; i < numTraverse; i++) {
				String uniqueName = path[i];
				int counter = 1;
				// need to continuously update the last header
				// since we might have to modify it to be unique
				while(uniqueNames.contains(uniqueName)) {
					uniqueName = path[i] + "_" + counter;
					counter++;
				}
				retVars[1] = uniqueName;
				gt = gt.out().has(TinkerFrame.TINKER_TYPE, path[i]).as(uniqueName);
			}
			gt = gt.select(retVars[0], retVars[1]);
			System.out.println(gt);
			
			Map<String, String> logicalToTypeMap = new Hashtable<String, String>();
			logicalToTypeMap.put(headers[0], headers[0]);
			logicalToTypeMap.put(headers[1], headers[1]);
			
			// now we have the traversal, so loop through and add in the information
			while(gt.hasNext()) {
				Map<String, Object> row = (Map<String, Object>) gt.next();
				Object[] values = new Object[2];
				values[0] = ((Vertex) row.get(retVars[0])).property(TinkerFrame.TINKER_NAME).value();
				values[1] = ((Vertex) row.get(retVars[1])).property(TinkerFrame.TINKER_NAME).value();
				
				newTf.addRelationship(headers, values, cardinality, logicalToTypeMap);
			}
		}
		
		// 4) need to also go through and add any "single" vertex of the types in the selectors
		// we add everything even if the vertex has no edge
				
		// modify cardinality to be empty set
		cardinality = new Hashtable<Integer, Set<Integer>>();
		cardinality.put(0, new HashSet<Integer>());
		
		GraphTraversal<Vertex, Vertex> vertIt = tf.g.traversal().V().has(TinkerFrame.TINKER_TYPE, P.within(selectors));
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
		
		int currId = tf.getDataId();
		for(int i = 0; i <= currId; i++) {
			newTf.updateDataId(); // TODO: expose this elsewhere
		}
		
		return newTf;
	}
	
	private static List<String[]> determineTraversals(String[] selectors, Map<String, String> edgeTraversals) {
		List<String[]> traversals = new Vector<String[]>();
		
		for(String startNode : edgeTraversals.keySet()) {
			// we define the path starting at the current start node
			// if the start node is not in the selectors, we can skip
			// if it is, then we need to find the path starting at this
			// node, going through all intermediate nodes, till we get
			// to the next node that is returned
			
			if(!ArrayUtilityMethods.arrayContainsValue(selectors, startNode)) {
				continue;
			}
			
			List<String> path = new Vector<String>();
			recursivelyBuildList(startNode, selectors, edgeTraversals, path);
			traversals.add(path.toArray(new String[]{}));
		}
		
		for(String startNode : edgeTraversals.values()) {
			// we define the path starting at the current start node
			// if the start node is not in the selectors, we can skip
			// if it is, then we need to find the path starting at this
			// node, going through all intermediate nodes, till we get
			// to the next node that is returned
			
			if(!ArrayUtilityMethods.arrayContainsValue(selectors, startNode)) {
				continue;
			}
			
			List<String> path = new Vector<String>();
			recursivelyBuildList2(startNode, selectors, edgeTraversals, path);
			traversals.add(path.toArray(new String[]{}));
		}
		
		if(traversals.isEmpty()) {
			throw new IllegalArgumentException("Invalid edge traversals.  There is no valid start node in the path described.");
		}
		
		return traversals;
	}
	
	private static void recursivelyBuildList(String startNode, String[] selectors, Map<String, String> edgeTraversals, List<String> currentPath) {
		// the current point is required in the path
		// i.e. startNode is either the start of a path
		// or it is a intermediate node
		currentPath.add(startNode);
		// regardless if it is a start node or intermediate node
		// the edge traversal better define the next connection that 
		// is required
		String endNode = edgeTraversals.get(startNode);

		// if the edge traversals doesn't lead to a valid end point
		// then there is an error
		// this works because i require in main loop that my start is always a valid selector that needs
		// to be returned
		if(endNode == null) {
			throw new IllegalArgumentException("Invalid path. Path does not have a valid end point.");
		}
		
		if(ArrayUtilityMethods.arrayContainsValue(selectors, endNode)) {
			// we got the end node
			// we are done
			currentPath.add(endNode);
		} else {
			// we need to recursively loop for the next one
			recursivelyBuildList(endNode, selectors, edgeTraversals, currentPath);
		}
	}
	
	private static void recursivelyBuildList2(String startNode, String[] selectors, Map<String, String> edgeTraversals, List<String> currentPath) {
		// the current point is required in the path
		// i.e. startNode is either the start of a path
		// or it is a intermediate node
		currentPath.add(startNode);
		// regardless if it is a start node or intermediate node
		// the edge traversal better define the next connection that 
		// is required
		String endNode = null;
		for(String node : edgeTraversals.keySet()) {
			if(edgeTraversals.get(node).equals(startNode)) {
				endNode = node;
			}
		}

		// if the edge traversals doesn't lead to a valid end point
		// then there is an error
		// this works because i require in main loop that my start is always a valid selector that needs
		// to be returned
		if(endNode == null) {
			throw new IllegalArgumentException("Invalid path. Path does not have a valid end point.");
		}
		
		if(ArrayUtilityMethods.arrayContainsValue(selectors, endNode)) {
			// we got the end node
			// we are done
			currentPath.add(endNode);
		} else {
			// we need to recursively loop for the next one
			recursivelyBuildList2(endNode, selectors, edgeTraversals, currentPath);
		}
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


	public static TinkerFrame findSharedVertices(TinkerFrame tf, String type, String[] instances, int degree) {
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
			for(int i = 0; i < degree; i++) {
				t1 = t1.both().as( (char) i + "");
			}
			t1 = t1.has(TinkerFrame.TINKER_NAME, P.within(instancesToBind));
			if(degree == 1) {
				t1 = t1.select("start", (char) 0 + "");
			} else if(degree >= 2) {
				String[] degreesToSelect = new String[degree - 1];
				for(int degreeCount = 1; degreeCount < degree; degreeCount++) {
					degreesToSelect[degreeCount-1] = (char) degreeCount + "";
				}
				t1 = t1.select("start", (char) 0 + "", degreesToSelect);
			}
			
			while(t1.hasNext()) {
				Object data = t1.next();
				if(data instanceof Map) {
					Vertex start = (Vertex) ((Map) data).get("start");
					instancesToKeep.add(start);
					for(int i = 0; i < degree; i++) {
						Vertex v = (Vertex) ((Map) data).get( (char) i + "");
						instancesToKeep.add(v);
					}
				} else {
					System.out.println("shouldn't get here");
				}
			}
			
			System.out.println(instancesToKeep);
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
