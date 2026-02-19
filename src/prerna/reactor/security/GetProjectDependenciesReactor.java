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
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.UploadInputUtility;

public class GetProjectDependenciesReactor extends AbstractSetMetadataReactor {
	
	public GetProjectDependenciesReactor() {
		this.keysToGet = new String[]{ ReactorKeysEnum.PROJECT.getKey() };
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		String userId = this.insight.getUserId();
		String projectId = UploadInputUtility.getProjectNameOrId(this.store);
		if(!SecurityProjectUtils.userCanViewProject(user, projectId)) {
			throw new IllegalArgumentException("The user does not have access to view this project or project id is invalid");
		}
		
		// Get all dependencies with subdependencies
		List<Map<String, Object>> dependencies = SecurityProjectUtils.getProjectDependencyDetails(projectId, userId, true);
		
		// Build tree structure, excluding inaccessible nodes
		Map<String, Object> dependencyTree = buildDependencyTree(dependencies, projectId);
		
		return new NounMetadata(dependencyTree, PixelDataType.MAP);
	}
	
	/**
	 * Build a tree structure from flat list of dependencies, including only nodes
	 * the user has direct access to. Inaccessible nodes are completely excluded from the tree.
	 * 
	 * @param dependencies Flat list of all dependencies
	 * @param rootProjectId The root project ID
	 * @return Tree structure with nested children, excluding inaccessible nodes
	 */
	private Map<String, Object> buildDependencyTree(List<Map<String, Object>> dependencies, String rootProjectId) {
		// Create a map of engineId -> dependency for quick lookup
		// Use the original maps - we'll make copies only when adding to result
		Map<String, Map<String, Object>> dependencyMap = new HashMap<>();
		for (Map<String, Object> dep : dependencies) {
			String engineId = (String) dep.get("engine_id");
			dependencyMap.put(engineId, dep);
		}
		
		// Build parent-child relationships using engine IDs only
		Map<String, List<String>> childrenByParent = new HashMap<>();
		for (Map<String, Object> dep : dependencies) {
			String parentId = (String) dep.get("parent_id");
			String engineId = (String) dep.get("engine_id");
			if (parentId != null) {
				childrenByParent.computeIfAbsent(parentId, k -> new ArrayList<>())
					.add(engineId);
			}
		}
		
		// Build tree recursively with access control pruning
		Map<String, Object> root = new HashMap<>();
		root.put("project_id", rootProjectId);
		root.put("dependencies", buildChildrenTree(rootProjectId, dependencyMap, childrenByParent, new HashMap<>()));
		
		return root;
	}
	
	/**
	 * Recursively build children tree, only including nodes the user has direct access to.
	 * Continues recursion even when can_view_dependencies is false (which just means
	 * there are inaccessible nodes deeper in the tree).
	 * 
	 * @param parentId Parent engine ID
	 * @param dependencyMap Map of engine ID to dependency data
	 * @param childrenByParent Map of parent ID to list of child engine IDs
	 * @param visited Map tracking which nodes have been visited in the current path to detect cycles
	 * @return List of children with their nested dependencies (only accessible nodes)
	 */
	private List<Map<String, Object>> buildChildrenTree(String parentId,
	                                                     Map<String, Map<String, Object>> dependencyMap,
	                                                     Map<String, List<String>> childrenByParent,
	                                                     Map<String, Boolean> visited) {
		List<String> childrenIds = childrenByParent.get(parentId);
		if (childrenIds == null || childrenIds.isEmpty()) {
			return new ArrayList<>();
		}
		
		// Mark this node as being processed in the current path
		visited.put(parentId, true);
		
		List<Map<String, Object>> result = new ArrayList<>();
		for (String childEngineId : childrenIds) {
			
			// Check for circular dependency
			if (visited.containsKey(childEngineId) && visited.get(childEngineId)) {
				// Create a new map for the circular reference marker
				Map<String, Object> circularRef = new HashMap<>();
				Map<String, Object> originalChild = dependencyMap.get(childEngineId);
				if (originalChild != null) {
					// Copy basic info
					circularRef.put("engine_id", originalChild.get("engine_id"));
					circularRef.put("engine_name", originalChild.get("engine_name"));
					circularRef.put("engine_type", originalChild.get("engine_type"));
				}
				circularRef.put("circular_reference", true);
				circularRef.put("circular_reference_to", childEngineId);
				result.add(circularRef);
				continue;
			}
			
			Map<String, Object> originalChild = dependencyMap.get(childEngineId);
			if (originalChild == null) {
				continue;
			}
			
			// Check if user has direct view permission for this node
			Integer permission = (Integer) originalChild.get("permission");
			Boolean isGlobal = (Boolean) originalChild.get("engine_global");
			boolean hasDirectAccess = (permission != null) || (isGlobal != null && isGlobal);
			
			// Skip this node entirely if user doesn't have direct access
			if (!hasDirectAccess) {
				continue;
			}
			
			// Create a clean copy of the child node to avoid circular references in the map structure
			Map<String, Object> child = new HashMap<>();
			for (Map.Entry<String, Object> entry : originalChild.entrySet()) {
				// Copy all fields except 'dependencies' to avoid any existing circular references
				if (!"dependencies".equals(entry.getKey())) {
					child.put(entry.getKey(), entry.getValue());
				}
			}
			
			// Create a new visited map for this branch to allow the same node in different branches
			Map<String, Boolean> branchVisited = new HashMap<>(visited);
			
			// Add nested children recursively
			// Continue even if can_view_dependencies is false, as it just means some deeper node is inaccessible
			List<Map<String, Object>> subChildren = buildChildrenTree(childEngineId, dependencyMap, childrenByParent, branchVisited);
			if (!subChildren.isEmpty()) {
				child.put("dependencies", subChildren);
			}
			
			result.add(child);
		}
		
		// Unmark this node after processing (backtracking)
		visited.put(parentId, false);
		
		return result;
	}
	
	@Override
	public String getReactorDescription() {
		return "Get project dependencies in a tree structure, excluding inaccessible nodes";
	}
	
}
