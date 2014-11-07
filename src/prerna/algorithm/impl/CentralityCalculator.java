package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

public class CentralityCalculator {
	/**
	 * Assumes that the relationships are added bidirectionally.
	 * @param edges
	 * @return
	 */
	public Hashtable<String, Double> calculateCloseness(Hashtable<String,Set<String>> edges) {
		Hashtable<String, Double> nodeCloseness = new Hashtable<String, Double>();
		
		//for every node (go through edges)
		for(String node : edges.keySet()) {
			nodeCloseness.put(node,calculateCloseness(node,edges));
		}
		return nodeCloseness;
	}
	
	private Double calculateCloseness(String node, Hashtable<String,Set<String>> edges) {
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
	
	public Hashtable<String, Double> calculateBetweenness(Hashtable<String,Set<String>> edges) {
		Hashtable<String, Double> nodeBetweeness = new Hashtable<String, Double>();
		
		Hashtable<String,Integer> shortestPath = calculateShortestPaths(edges);
		
		for(String node : edges.keySet()) {
			nodeBetweeness.put(node,calculateBetweenness(node,edges,shortestPath));
		}
		
		return nodeBetweeness;
	}
	
	private Double calculateBetweenness(String node, Hashtable<String,Set<String>> edges, Hashtable<String,Integer> shortestPath) {
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
	
	private boolean isBetween(String center, String nodeS, String nodeT, Hashtable<String,Integer> shortestPath) {
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
	
	private Integer getShortestDistance(String node1, String node2, Hashtable<String,Integer> shortestPath) {
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
	private Hashtable<String,Integer> calculateShortestPaths(Hashtable<String,Set<String>> edges) {
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
	
	//calculate shortest distance between all
	//hashtable with vertex-vertex pair as key.
	
}
