package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

import org.openrdf.repository.RepositoryConnection;

import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.rdf.engine.impl.RDFFileSesameEngine;
import prerna.ui.components.ExecuteQueryProcessor;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.Constants;
import edu.uci.ics.jung.graph.DelegateForest;

public final class CentralityCalculator {
	
	private CentralityCalculator() {
		
	}
	
	/**
	 * Assumes that the relationships are added bidirectionally.
	 * @param edges
	 * @return
	 */
	public static Hashtable<String, Double> calculateCloseness(Hashtable<String,Set<String>> edges) {
		Hashtable<String, Double> nodeCloseness = new Hashtable<String, Double>();
		
		//for every node (go through edges)
		for(String node : edges.keySet()) {
			nodeCloseness.put(node,calculateCloseness(node,edges));
		}
		return nodeCloseness;
	}
	
	private static Double calculateCloseness(String node, Hashtable<String,Set<String>> edges) {
		Hashtable<String, Integer> distances = new Hashtable<String, Integer>();
		distances.put(node,0);
		
		int count = 1;
		Set<String> nodesToAdd = new HashSet<String>();
		nodesToAdd.addAll(edges.get(node));
		
		//if isolated, then centrality is 0
		if(nodesToAdd==null || nodesToAdd.size()==0)
			return 0.0;

		//if there is at least something connected, get distances to every other node in the database
		//stopping point when all nodes have been added to our distance hash
		//or when our count is the size of the database(will stop if there are islands)
		while(distances.size()<edges.size() && count <= edges.size()) {
			Set<String> nextNodesToAdd = new HashSet<String>();
			for(String nodeToAdd : nodesToAdd) {
				if(!distances.containsKey(nodeToAdd))
					distances.put(nodeToAdd, count);
				nextNodesToAdd.addAll(edges.get(nodeToAdd));
			}
			count++;
			nodesToAdd.addAll(nextNodesToAdd);
		}
		
		//sum up all distances
		double totalDistance = 0.0;
		for(String connectedNode : distances.keySet()) {
			totalDistance += distances.get(connectedNode);
		}
		
		return (distances.size()-1.0) / totalDistance;
			
	}
	
	public static Hashtable<String, Double> calculateBetweenness(Hashtable<String,Set<String>> edges) {
		Hashtable<String, Double> nodeBetweeness = new Hashtable<String, Double>();
		
		Hashtable<String,Integer> shortestPath = calculateShortestPaths(edges);
		
		for(String node : edges.keySet()) {
			nodeBetweeness.put(node,calculateBetweenness(node,edges,shortestPath));
		}
		
		return nodeBetweeness;
	}
	
	private static Double calculateBetweenness(String node, Hashtable<String,Set<String>> edges, Hashtable<String,Integer> shortestPath) {
		Double betweeness = 0.0;
		ArrayList<String> nodes = new ArrayList<String>(edges.keySet());
		for(int s=0;s<nodes.size();s++) {
			String nodeS = nodes.get(s);
			for(int t=s+1;t<nodes.size();t++) {
				String nodeT = nodes.get(t);
				if(isBetween(node,nodeS,nodeT,shortestPath))
					betweeness++;
			}
		}
		Double denominator = ((edges.size()-1.0)*(edges.size()-2.0)) / 2.0;
		return betweeness / denominator;
	}
	
	private static boolean isBetween(String center, String nodeS, String nodeT, Hashtable<String,Integer> shortestPath) {
		int shortestDistanceST = getShortestDistance(nodeS, nodeT, shortestPath);
		int shortestDistanceCenterS = getShortestDistance(center, nodeS, shortestPath);
		int shortestDistanceCenterT = getShortestDistance(center, nodeT, shortestPath);
		//if any of the nodes don't connect, return false, it is not on the shortest path
		if(shortestDistanceST == -1 || shortestDistanceCenterS == -1 || shortestDistanceCenterT == -1)
			return false;
		if(shortestDistanceST==shortestDistanceCenterS+shortestDistanceCenterT)
			return true;
		return false;
	}
	
	private static Integer getShortestDistance(String node1, String node2, Hashtable<String,Integer> shortestPath) {
		if(shortestPath.containsKey(node1 + "-" + node2))
			return shortestPath.get(node1 + "-" + node2);
		if(shortestPath.containsKey(node2 + "-" + node1))
			return shortestPath.get(node2 + "-" + node1);
		return -1;
	}
	
	/**
	 * Calculates the shortest path between all the nodes in the edge hash.
	 * Stores the shortest path for each set of nodes in a hashtable
	 * where the key is a concatenation of the nodes with a "-"
	 * the value is the shortest path between them and -1 if they do not connect
	 * @param edges Hashtable with keys representing nodes and values the nodes they are connected to independent of direction. ex: If nodes i and j are connected, then node i is a key with node j as a value in its Set and node j is a key with node i as a value in its Set.
	 * @return
	 */
	private static Hashtable<String,Integer> calculateShortestPaths(Hashtable<String,Set<String>> edges) {
		Hashtable<String,Integer> shortestPath = new Hashtable<String,Integer>();
		
		ArrayList<String> nodes = new ArrayList<String>(edges.keySet());
		for(int s=0;s<nodes.size();s++) {
			String nodeI = nodes.get(s);
			for(int t=s+1;t<nodes.size();t++) {
				String nodeJ = nodes.get(t);
				//find the shortest path between node i and node j
				int distance = 1;
				Set<String>  neighbors = new HashSet<String>();
				neighbors.addAll(edges.get(nodeI));
				while(!neighbors.contains(nodeJ)&&distance<=edges.size()) {
					Set<String>  nextNeighbors = new HashSet<String>();
					distance++;
					for(String neighbor : neighbors)
						nextNeighbors.addAll(edges.get(neighbor));
					neighbors = nextNeighbors;
				}
				if(!neighbors.contains(nodeJ))
					shortestPath.put(nodeI + "-" + nodeJ, -1);
				else
					shortestPath.put(nodeI + "-" + nodeJ, distance);
			}
		}
		return shortestPath;
	}
	
	public static Hashtable<SEMOSSVertex, Double> calculateEccentricity(Hashtable<String,SEMOSSVertex> vertStore, boolean directed) {
		Hashtable<SEMOSSVertex, Double> nodeEccentricity = new Hashtable<SEMOSSVertex, Double>();
		
		Hashtable<String,Set<String>> edges = processEdges(vertStore, directed);
		Hashtable<String,Integer> shortestPath = calculateShortestPaths(edges);
		
		for(String node : vertStore.keySet()) {
			SEMOSSVertex vert = vertStore.get(node);
			String type = (String) vert.propHash.get(Constants.VERTEX_NAME);
			nodeEccentricity.put(vert,calculateEccentricity(type,edges,shortestPath));
		}		
		return nodeEccentricity;
	}
	
	public static Hashtable<String, Double> calculateEccentricity(Hashtable<String,Set<String>> edges) {
		Hashtable<String, Double> nodeEccentricity = new Hashtable<String, Double>();
		
		Hashtable<String,Integer> shortestPath = calculateShortestPaths(edges);
		
		for(String node : edges.keySet()) {
			nodeEccentricity.put(node,calculateEccentricity(node,edges,shortestPath));
		}
		
		return nodeEccentricity;
	}
	
	private static Double calculateEccentricity(String node, Hashtable<String,Set<String>> edges, Hashtable<String,Integer> shortestPath) {
		//for every node (go through edges)
		double longestPath = 0.0;
		for(String otherNode : edges.keySet()) {
			double currPath = 0.0;
			if(shortestPath.containsKey(node+"-"+otherNode)) {
				currPath = shortestPath.get(node+"-"+otherNode);
			} else if(shortestPath.containsKey(otherNode+"-"+node)) {
				currPath = shortestPath.get(otherNode+"-"+node);
			}
			if(currPath>longestPath)
				longestPath=currPath;
		}
		if(longestPath==0.0)
			return 0.0;
		return 1.0 / longestPath;
	}
	
	public static Hashtable<String,Set<String>> processEdges(Hashtable<String, SEMOSSVertex> vertStore, boolean directed) {
		Hashtable<String,Set<String>> edges = new Hashtable<String,Set<String>>();
		for(String key : vertStore.keySet()) {
			SEMOSSVertex vertex= vertStore.get(key);
			String type = (String)vertex.propHash.get(Constants.VERTEX_NAME);
			
			Set<String> neighbors = new HashSet<String>();
			if(!directed) {
				Vector<SEMOSSEdge> inEdges = vertex.getInEdges();
				for(SEMOSSEdge edge : inEdges) {
					neighbors.add((String)(edge.outVertex.propHash.get(Constants.VERTEX_NAME)));
				}
			}
			Vector<SEMOSSEdge> outEdges = vertex.getOutEdges();
			for(SEMOSSEdge edge : outEdges) {
				neighbors.add((String)(edge.inVertex.propHash.get(Constants.VERTEX_NAME)));
			}
			edges.put(type, neighbors);
		}
		return edges;
	}
	
	/**
	 * Creates the GraphPlaySheet for a database that shows the metamodel.
	 * @param engine IEngine to create the metamodel from
	 * @return GraphPlaySheet that displays the metamodel
	 */
	public static GraphPlaySheet createMetamodel(RepositoryConnection rc, String query){
		ExecuteQueryProcessor exQueryProcessor = new ExecuteQueryProcessor();
		//hard code playsheet attributes since no insight exists for this
		//String sparql = "SELECT ?s ?p ?o WHERE {?s ?p ?o} LIMIT 1";
		String playSheetName = "prerna.ui.components.playsheets.GraphPlaySheet";
		RDFFileSesameEngine sesameEngine = new RDFFileSesameEngine();
		sesameEngine.setRC(rc);
		sesameEngine.setEngineName("Metamodel Engine");
		
		sesameEngine.setBaseData(sesameEngine);
		Hashtable<String, String> filterHash = new Hashtable<String, String>();
		filterHash.put("http://semoss.org/ontologies/Relation", "http://semoss.org/ontologies/Relation");
		sesameEngine.setBaseHash(filterHash);
		
		exQueryProcessor.prepareQueryOutputPlaySheet(sesameEngine, query, playSheetName, "", "");

		GraphPlaySheet playSheet= (GraphPlaySheet) exQueryProcessor.getPlaySheet();
		playSheet.getGraphData().setSubclassCreate(true);//this makes the base queries use subclass instead of type--necessary for the metamodel query
		playSheet.createData();
		playSheet.runAnalytics();
		playSheet.getForest();
		playSheet.createForest();
		return playSheet;
	}

	public static DelegateForest<SEMOSSVertex,SEMOSSEdge> makeForestUndirected(Hashtable<String, SEMOSSEdge> edgeStore, DelegateForest<SEMOSSVertex,SEMOSSEdge> forest) {
		for(String edgeKey : edgeStore.keySet()) {
			SEMOSSEdge oldEdge = edgeStore.get(edgeKey);
			SEMOSSEdge newEdge = new SEMOSSEdge(oldEdge.inVertex,oldEdge.outVertex,oldEdge.inVertex.getURI()+":"+oldEdge.outVertex.propHash.get(Constants.VERTEX_NAME));//pull from the old edge
			forest.addEdge(newEdge, oldEdge.inVertex,oldEdge.outVertex);			
		}
		return forest;
	}
}
