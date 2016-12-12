package prerna.ds;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.h2.H2Frame;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Constants;
import prerna.util.Utility;

public class DataFrameHelper {

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
	
}
