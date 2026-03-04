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
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		String userId = this.insight.getUserId();
		String projectId = UploadInputUtility.getProjectNameOrId(this.store);
		if (!SecurityProjectUtils.userCanViewProject(user, projectId)) {
			throw new IllegalArgumentException(
					"The user does not have access to view this project or project id is invalid");
		}

		// Get all dependencies with subdependencies
		List<Map<String, Object>> dependencies = SecurityProjectUtils.getProjectDependencyDetails(projectId, userId,
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
		Map<String, List<String>> childrenByParent = new HashMap<>();

		for (Map<String, Object> dep : dependencies) {
			String parentId = (String) dep.get("parent_id");
			String engineId = (String) dep.get("engine_id");

			// Store in engine map (avoid duplicates by using engineId as key)
			if (!allEnginesMap.containsKey(engineId)) {
				allEnginesMap.put(engineId, dep);
			}

			// Build parent-child relationships
			if (parentId != null) {
				childrenByParent.computeIfAbsent(parentId, k -> new ArrayList<>()).add(engineId);
			}
		}

		// Traverse from root to find all reachable engines based on access rules
		Map<String, Map<String, Object>> reachableEngines = new HashMap<>();
		List<String> rootDependencies = childrenByParent.get(rootProjectId);
		if (rootDependencies == null) {
			rootDependencies = new ArrayList<>();
		}

		// Traverse and collect reachable engines
		traverseReachable(rootProjectId, allEnginesMap, childrenByParent, reachableEngines);

		// Build the final engines list with filtered dependencies
		List<Map<String, Object>> engines = new ArrayList<>();
		for (Map.Entry<String, Map<String, Object>> entry : reachableEngines.entrySet()) {
			String engineId = entry.getKey();
			Map<String, Object> originalEngine = entry.getValue();

			// Create engine object with all metadata
			Map<String, Object> engine = new HashMap<>();

			// Copy all fields except parent_id
			for (Map.Entry<String, Object> field : originalEngine.entrySet()) {
				String key = field.getKey();
				if (!"parent_id".equals(key)) {
					engine.put(key, field.getValue());
				}
			}

			// Add dependencies - only include children that user has access to (reachable)
			List<String> allChildren = childrenByParent.get(engineId);
			List<String> reachableChildren = new ArrayList<>();
			if (allChildren != null) {
				for (String childId : allChildren) {
					if (reachableEngines.containsKey(childId)) {
						reachableChildren.add(childId);
					}
				}
			}
			engine.put("dependencies", reachableChildren);

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
	 * Recursively traverse and collect reachable engines based on access rules.
	 * - Engines are visible if user has permission, or if they're
	 * global
	 * - Children are only traversed if user has actual permission (not just
	 * discoverable)
	 * 
	 * @param parentId         The current parent engine ID
	 * @param allEnginesMap    Map of all engine IDs to their full data
	 * @param childrenByParent Map of parent ID to list of child engine IDs
	 * @param reachableEngines Accumulator for reachable engines
	 */
	private void traverseReachable(String parentId,
			Map<String, Map<String, Object>> allEnginesMap,
			Map<String, List<String>> childrenByParent,
			Map<String, Map<String, Object>> reachableEngines) {
		List<String> children = childrenByParent.get(parentId);
		if (children == null || children.isEmpty()) {
			return;
		}

		for (String childId : children) {
			Map<String, Object> childEngine = allEnginesMap.get(childId);
			if (childEngine == null || reachableEngines.containsKey(childId)) {
				continue;
			}

			// Check visibility: user can see engine if they have permission, or it's
			// global
			Integer permission = (Integer) childEngine.get("permission");
			Boolean isGlobal = (Boolean) childEngine.get("engine_global");

			boolean isVisible = (permission != null) || (isGlobal != null && isGlobal);

			// Check access: user can traverse into children only if they have permission or
			// it's global
			boolean hasAccess = (permission != null) || (isGlobal != null && isGlobal);

			if (isVisible) {
				// Add engine to reachable list
				reachableEngines.put(childId, childEngine);

				// Only traverse children if user has actual access (not just discoverable)
				if (hasAccess) {
					traverseReachable(childId, allEnginesMap, childrenByParent, reachableEngines);
				}
			}
		}
	}

	@Override
	public String getReactorDescription() {
		return "Get project dependencies as a graph with unique engines and their direct dependencies";
	}

}