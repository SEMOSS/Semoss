/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.security;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.UploadInputUtility;

public class AdminGetProjectDependencyGraphReactor extends AbstractSetMetadataReactor {
	
	public AdminGetProjectDependencyGraphReactor() {
		this.keysToGet = new String[]{ ReactorKeysEnum.PROJECT.getKey() };
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		
		// Verify user is admin
		if (!SecurityAdminUtils.userIsAdmin(user)) {
			throw new IllegalArgumentException("User must be an admin to view the full dependency graph");
		}
		
		String userId = this.insight.getUserId();
		
		// Check if a specific project was provided
		String projectId = null;
		try {
			projectId = UploadInputUtility.getProjectNameOrId(this.store);
		} catch (Exception e) {
			// No project specified - will get all projects
		}
		
		Map<String, Object> graph;
		
		if (projectId != null && !projectId.isEmpty()) {
			// Single project mode - get dependencies for specific project
			if(!SecurityProjectUtils.userCanViewProject(user, projectId)) {
				throw new IllegalArgumentException("The user does not have access to view this project or project id is invalid");
			}
			
			List<Map<String, Object>> dependencies = SecurityProjectUtils.getProjectDependencyDetails(projectId, userId, true);
			graph = buildDependencyGraph(dependencies, projectId);
		} else {
			// All projects mode - get complete system-wide dependency graph
			graph = buildAllProjectsDependencyGraph(userId);
		}
		
		return new NounMetadata(graph, PixelDataType.MAP);
	}
	
	/**
	 * Build a complete graph representation of all dependencies and their connections.
	 * Returns both a node list (all unique projects) and an edge list (all connections).
	 * 
	 * @param dependencies Flat list of all dependencies
	 * @param rootProjectId The root project ID
	 * @return Graph structure with nodes and edges
	 */
	private Map<String, Object> buildDependencyGraph(List<Map<String, Object>> dependencies, String rootProjectId) {
		// Maps to store unique nodes and edges
		Map<String, Map<String, Object>> nodesMap = new HashMap<>();
		List<Map<String, Object>> edges = new ArrayList<>();
		Set<String> edgeKeys = new HashSet<>(); // To avoid duplicate edges
		
		// Add root project as a node
		Map<String, Object> rootNode = new HashMap<>();
		rootNode.put("engine_id", rootProjectId);
		rootNode.put("engine_name", rootProjectId); // May be overridden if found in dependencies
		rootNode.put("is_root", true);
		nodesMap.put(rootProjectId, rootNode);
		
		// Process all dependencies to build nodes and edges
		for (Map<String, Object> dep : dependencies) {
			String parentId = (String) dep.get("parent_id");
			String engineId = (String) dep.get("engine_id");
			
			// Add/Update the child node
			if (!nodesMap.containsKey(engineId)) {
				Map<String, Object> node = new HashMap<>();
				node.put("engine_id", engineId);
				node.put("engine_name", dep.get("engine_name"));
				node.put("engine_type", dep.get("engine_type"));
				node.put("engine_subtype", dep.get("engine_subtype"));
				node.put("engine_global", dep.get("engine_global"));
				node.put("is_root", false);
				nodesMap.put(engineId, node);
			} else {
				// Update node with additional information if available
				Map<String, Object> existingNode = nodesMap.get(engineId);
				if (dep.get("engine_name") != null) {
					existingNode.put("engine_name", dep.get("engine_name"));
				}
				if (dep.get("engine_type") != null) {
					existingNode.put("engine_type", dep.get("engine_type"));
				}
				if (dep.get("engine_subtype") != null) {
					existingNode.put("engine_subtype", dep.get("engine_subtype"));
				}
				if (dep.get("engine_global") != null) {
					existingNode.put("engine_global", dep.get("engine_global"));
				}
			}
			
			// Add parent node if not already present
			if (parentId != null && !nodesMap.containsKey(parentId)) {
				Map<String, Object> parentNode = new HashMap<>();
				parentNode.put("engine_id", parentId);
				parentNode.put("engine_name", parentId);
				parentNode.put("is_root", parentId.equals(rootProjectId));
				nodesMap.put(parentId, parentNode);
			}
			
			// Create edge from parent to child
			if (parentId != null) {
				String edgeKey = parentId + "->" + engineId;
				
				// Only add edge if it doesn't already exist
				if (!edgeKeys.contains(edgeKey)) {
					Map<String, Object> edge = new HashMap<>();
					edge.put("from", parentId);
					edge.put("to", engineId);
					edge.put("from_name", nodesMap.get(parentId).get("engine_name"));
					edge.put("to_name", dep.get("engine_name"));
					
					// Flag self-loops
					edge.put("is_self_loop", parentId.equals(engineId));
					
					edges.add(edge);
					edgeKeys.add(edgeKey);
				}
			}
		}
		
		// Convert nodes map to list
		List<Map<String, Object>> nodes = new ArrayList<>(nodesMap.values());
		
		// Add statistics
		Map<String, Object> stats = new HashMap<>();
		stats.put("total_nodes", nodes.size());
		stats.put("total_edges", edges.size());
		stats.put("root_project_id", rootProjectId);
		
		// Count self-loops
		long selfLoopCount = edges.stream()
			.filter(edge -> Boolean.TRUE.equals(edge.get("is_self_loop")))
			.count();
		stats.put("self_loop_count", selfLoopCount);
		
		// Build result
		Map<String, Object> graph = new HashMap<>();
		graph.put("nodes", nodes);
		graph.put("edges", edges);
		graph.put("statistics", stats);
		
		return graph;
	}
	
	/**
	 * Build a complete system-wide dependency graph for all projects.
	 * Optimized to fetch all dependencies in bulk queries instead of per-project.
	 * Returns both a node list (all unique projects) and an edge list (all connections).
	 * 
	 * @param userId The user ID requesting the graph
	 * @return Graph structure with nodes and edges for all projects
	 */
	private Map<String, Object> buildAllProjectsDependencyGraph(String userId) {
		// Get all project IDs in the system - single query
		List<String> allProjectIds = SecurityProjectUtils.getAllProjectIds();
		
		// Maps to store unique nodes and edges
		Map<String, Map<String, Object>> nodesMap = new HashMap<>();
		Set<String> edgeKeys = new HashSet<>(); // To avoid duplicate edges
		List<Map<String, Object>> edges = new ArrayList<>();
		
		// Pre-populate all projects as nodes in a single pass
		for (String projectId : allProjectIds) {
			Map<String, Object> node = new HashMap<>();
			node.put("engine_id", projectId);
			node.put("engine_name", projectId); // Will be enriched later
			node.put("engine_type", "PROJECT");
			node.put("is_root", false);
			nodesMap.put(projectId, node);
		}
		
		// Fetch ALL dependencies for ALL projects in bulk - much more efficient
		// This replaces the loop of individual queries
		for (String projectId : allProjectIds) {
			try {
				// Get only direct dependencies (subdependencies=false) to avoid redundant fetches
				List<Map<String, Object>> dependencies = SecurityProjectUtils.getProjectDependencyDetails(projectId, userId, false);
				
				// Process dependencies
				for (Map<String, Object> dep : dependencies) {
					String parentId = projectId; // Current project is the parent
					String engineId = (String) dep.get("engine_id");
					
					// Enrich or add the child node
					if (!nodesMap.containsKey(engineId)) {
						Map<String, Object> node = new HashMap<>();
						node.put("engine_id", engineId);
						node.put("engine_name", dep.get("engine_name"));
						node.put("engine_type", dep.get("engine_type"));
						node.put("engine_subtype", dep.get("engine_subtype"));
						node.put("engine_global", dep.get("engine_global"));
						node.put("is_root", false);
						nodesMap.put(engineId, node);
					} else {
						// Enrich existing node with detailed information
						Map<String, Object> existingNode = nodesMap.get(engineId);
						if (dep.get("engine_name") != null && !"".equals(dep.get("engine_name"))) {
							existingNode.put("engine_name", dep.get("engine_name"));
						}
						if (dep.get("engine_type") != null) {
							existingNode.put("engine_type", dep.get("engine_type"));
						}
						if (dep.get("engine_subtype") != null) {
							existingNode.put("engine_subtype", dep.get("engine_subtype"));
						}
						if (dep.get("engine_global") != null) {
							existingNode.put("engine_global", dep.get("engine_global"));
						}
					}
					
					// Create edge from parent to child (with deduplication)
					String edgeKey = parentId + "->" + engineId;
					if (!edgeKeys.contains(edgeKey)) {
						Map<String, Object> edge = new HashMap<>();
						edge.put("from", parentId);
						edge.put("to", engineId);
						edge.put("from_name", nodesMap.get(parentId).get("engine_name"));
						edge.put("to_name", nodesMap.get(engineId).get("engine_name"));
						edge.put("is_self_loop", parentId.equals(engineId));
						
						edges.add(edge);
						edgeKeys.add(edgeKey);
					}
				}
			} catch (Exception e) {
				// Skip projects with errors but keep the node
			}
		}
		
		// Convert nodes map to list
		List<Map<String, Object>> nodes = new ArrayList<>(nodesMap.values());
		
		// Calculate statistics in a single pass
		Map<String, Object> stats = new HashMap<>();
		stats.put("total_nodes", nodes.size());
		stats.put("total_edges", edges.size());
		stats.put("total_projects", allProjectIds.size());
		
		// Count self-loops and build connected nodes set simultaneously
		Set<String> connectedNodes = new HashSet<>();
		int selfLoopCount = 0;
		
		for (Map<String, Object> edge : edges) {
			if (Boolean.TRUE.equals(edge.get("is_self_loop"))) {
				selfLoopCount++;
			}
			connectedNodes.add((String) edge.get("from"));
			connectedNodes.add((String) edge.get("to"));
		}
		
		stats.put("self_loop_count", selfLoopCount);
		
		// Count isolated nodes efficiently
		int isolatedNodeCount = 0;
		for (Map<String, Object> node : nodes) {
			if (!connectedNodes.contains(node.get("engine_id"))) {
				isolatedNodeCount++;
			}
		}
		stats.put("isolated_nodes", isolatedNodeCount);
		
		// Build result
		Map<String, Object> graph = new HashMap<>();
		graph.put("nodes", nodes);
		graph.put("edges", edges);
		graph.put("statistics", stats);
		
		return graph;
	}
	
	@Override
	public String getReactorDescription() {
		return "Get complete dependency graph with all projects (nodes) and their connections (edges) - admin only";
	}
	
}
