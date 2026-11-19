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
import prerna.auth.utils.SecurityCatalogDependencyUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * The engines and projects that depend on an engine or a project: what deleting
 * or changing it would affect. Only the ones the user can view are listed; the
 * rest are counted.
 */
public class GetCatalogDependentsReactor extends AbstractReactor {

	private static final String RECURSIVE = "recursive";

	public GetCatalogDependentsReactor() {
		this.keysToGet = new String[] { CatalogDependencyKeys.CATALOG_ID, CatalogDependencyKeys.CATALOG_TYPE,
				RECURSIVE };
		this.keyRequired = new int[] { 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		String catalogId = this.keyValue.get(CatalogDependencyKeys.CATALOG_ID);
		String catalogType = SecurityCatalogDependencyUtils.resolveCatalogType(catalogId,
				this.keyValue.get(CatalogDependencyKeys.CATALOG_TYPE));
		if (!SecurityCatalogDependencyUtils.userCanView(user, catalogId, catalogType)) {
			throw new IllegalArgumentException("The user does not have access to view " + catalogId);
		}
		boolean recursive = Boolean.parseBoolean(this.keyValue.get(RECURSIVE));

		List<Map<String, Object>> visible = new ArrayList<>();
		Set<String> hidden = new HashSet<>();
		Map<String, Boolean> canView = new HashMap<>();
		for (Map<String, Object> dependent : SecurityCatalogDependencyUtils.getDependents(catalogId, recursive)) {
			String id = (String) dependent.get("catalog_id");
			String type = (String) dependent.get("catalog_type");
			boolean viewable = canView.computeIfAbsent(id, key -> {
				try {
					return SecurityCatalogDependencyUtils.userCanView(user, key, type);
				} catch (IllegalArgumentException e) {
					// a type no longer known
					return false;
				}
			});
			if (viewable) {
				visible.add(dependent);
			} else {
				hidden.add(id);
			}
		}

		Map<String, Object> result = new HashMap<>();
		result.put("dependents", visible);
		result.put("hiddenCount", hidden.size());
		return new NounMetadata(result, PixelDataType.MAP);
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition()
				.description("Get the engines and projects that depend on an engine or project, one row per link with "
						+ "is_automatic, role, dependency_name and dependency_description, and how many the user "
						+ "cannot view")
				.key(CatalogDependencyKeys.CATALOG_ID, "The engine id or project id")
				.key(CatalogDependencyKeys.CATALOG_TYPE, "Its engine type or project type; read when not given")
				.key(RECURSIVE, "Whether to add the items that depend on those, all the way up; false by default");
	}
}
