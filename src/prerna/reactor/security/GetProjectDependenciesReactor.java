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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.UploadInputUtility;

public class GetProjectDependenciesReactor extends AbstractSetMetadataReactor {

	public GetProjectDependenciesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		String projectId = UploadInputUtility.getProjectNameOrId(this.store);
		if (!SecurityProjectUtils.userCanViewProject(user, projectId)) {
			throw new IllegalArgumentException(
					"The user does not have access to view this project or project id is invalid");
		}

		// Get all dependencies with subdependencies
		List<Map<String, Object>> dependencies = SecurityProjectUtils.getProjectDependencyDetails(projectId, user,
				true);

		// Build graph structure with unique nodes and their direct dependencies
		Map<String, Object> dependencyGraph = buildDependencyGraph(dependencies, projectId);

		return new NounMetadata(dependencyGraph, PixelDataType.MAP);
	}

	/**
	 * Build a graph structure from flat list of dependencies where each engine
	 * appears once. Only includes engines that are reachable following access
	 * rules.
	 * 
	 * @param dependencies  Flat list of all dependencies
	 * @param rootProjectId The root project ID
	 * @return Graph structure with unique engines and direct dependency lists
	 */
	private Map<String, Object> buildDependencyGraph(List<Map<String, Object>> dependencies, String rootProjectId) {
		// Build maps for quick lookup
		Map<String, Map<String, Object>> allEnginesMap = new HashMap<>();
		Map<String, Set<String>> childrenByParent = new HashMap<>();

		for (Map<String, Object> dep : dependencies) {
			String parentId = (String) dep.get("parent_id");
			String engineId = (String) dep.get("engine_id");

			// Store in engine map (avoid duplicates by using engineId as key)
			allEnginesMap.putIfAbsent(engineId, dep);

			// Build parent-child relationships (use Set to avoid duplicates)
			if (parentId != null) {
				childrenByParent.computeIfAbsent(parentId, k -> new LinkedHashSet<>()).add(engineId);
			}
		}

		// Traverse from root to find all reachable engines based on access rules
		Map<String, Map<String, Object>> reachableEngines = new HashMap<>();
		Set<String> rootDependencies = childrenByParent.get(rootProjectId);
		if (rootDependencies == null) {
			rootDependencies = new LinkedHashSet<>();
		}

		// Traverse and collect reachable engines
		traverseReachable(rootProjectId, allEnginesMap, childrenByParent, reachableEngines);

		// Build the final engines list with filtered dependencies
		List<Map<String, Object>> engines = new ArrayList<>();
		for (Map.Entry<String, Map<String, Object>> entry : reachableEngines.entrySet()) {
			String engineId = entry.getKey();
			Map<String, Object> originalEngine = entry.getValue();

			// Create engine object with all metadata except parent_id
			Map<String, Object> engine = new HashMap<>(originalEngine);
			engine.remove("parent_id");

			// Derive permission_name from the highest permission (lowest integer id) the
			// user holds, considering both direct and group permissions
			Integer directPerm = (Integer) engine.get("permission");
			Integer groupPerm = (Integer) engine.get("group_permission");
			Integer bestPermission = null;
			if (directPerm != null && groupPerm != null) {
				bestPermission = Math.min(directPerm, groupPerm);
			} else if (directPerm != null) {
				bestPermission = directPerm;
			} else if (groupPerm != null) {
				bestPermission = groupPerm;
			}
			if (bestPermission != null) {
				engine.put("permission_name", AccessPermissionEnum.getPermissionValueById(bestPermission));
			}

			// Check if user has access to this engine
			Integer permission = (Integer) originalEngine.get("permission");
			Integer groupPermission = (Integer) originalEngine.get("group_permission");
			Boolean isGlobal = (Boolean) originalEngine.get("engine_global");
			boolean hasAccess = (permission != null) || (groupPermission != null) || (isGlobal != null && isGlobal);

			// Only show dependencies if user has access to this engine
			if (hasAccess) {
				// Add dependencies - only include children that are reachable
				Set<String> allChildren = childrenByParent.get(engineId);
				List<String> reachableChildren = new ArrayList<>();
				if (allChildren != null) {
					for (String childId : allChildren) {
						if (reachableEngines.containsKey(childId)) {
							reachableChildren.add(childId);
						}
					}
				}
				engine.put("dependencies", reachableChildren);
			}

			engines.add(engine);
		}

		// Filter root dependencies to only include reachable ones
		List<String> filteredRootDeps = new ArrayList<>();
		for (String depId : rootDependencies) {
			if (reachableEngines.containsKey(depId)) {
				filteredRootDeps.add(depId);
			}
		}

		// Build result structure
		Map<String, Object> result = new HashMap<>();
		result.put("engines", engines);
		result.put("dependencies", filteredRootDeps);

		return result;
	}

	/**
	 * Recursively traverse and collect reachable engines based on access rules. -
	 * Direct children of accessible parents are always visible - Children's
	 * dependencies are only explored if user has access to the child
	 * 
	 * @param parentId         The current parent engine ID
	 * @param allEnginesMap    Map of all engine IDs to their full data
	 * @param childrenByParent Map of parent ID to set of child engine IDs
	 * @param reachableEngines Accumulator for reachable engines
	 */
	private void traverseReachable(String parentId, Map<String, Map<String, Object>> allEnginesMap,
			Map<String, Set<String>> childrenByParent, Map<String, Map<String, Object>> reachableEngines) {
		Set<String> children = childrenByParent.get(parentId);
		if (children == null || children.isEmpty()) {
			return;
		}

		for (String childId : children) {
			Map<String, Object> childEngine = allEnginesMap.get(childId);
			if (childEngine == null || reachableEngines.containsKey(childId)) {
				continue;
			}

			// Always add direct children (they're visible if parent is accessible)
			reachableEngines.put(childId, childEngine);

			// Only traverse into child's dependencies if user has access to the child
			Integer permission = (Integer) childEngine.get("permission");
			Integer groupPermission = (Integer) childEngine.get("group_permission");
			Boolean isGlobal = (Boolean) childEngine.get("engine_global");
			boolean hasAccess = (permission != null) || (groupPermission != null) || (isGlobal != null && isGlobal);

			if (hasAccess) {
				traverseReachable(childId, allEnginesMap, childrenByParent, reachableEngines);
			}
		}
	}

	@Override
	public String getReactorDescription() {
		return "Get project dependencies as a graph with unique engines and their direct dependencies";
	}

}