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
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.CatalogTypes;
import prerna.auth.utils.SecurityCatalogDependencyUtils;
import prerna.auth.utils.SecurityCatalogDependencyUtils.Dependency;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Replaces the dependencies a user picked for an engine or a project with the
 * engines and projects given, each with an optional name and description. The
 * automatic dependencies, which save reactors derive from the item's own
 * configuration, are kept. Each is checked against the security database alone,
 * so this works on any server of a cluster.
 */
public class SetCatalogDependenciesReactor extends AbstractReactor {

	public SetCatalogDependenciesReactor() {
		this.keysToGet = new String[] { CatalogDependencyKeys.CATALOG_ID, CatalogDependencyKeys.CATALOG_TYPE,
				CatalogDependencyKeys.DEPENDENCIES };
		this.keyRequired = new int[] { 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		String catalogId = this.keyValue.get(CatalogDependencyKeys.CATALOG_ID);
		String catalogType = SecurityCatalogDependencyUtils.resolveCatalogType(catalogId,
				this.keyValue.get(CatalogDependencyKeys.CATALOG_TYPE));
		if (!SecurityCatalogDependencyUtils.userCanEdit(user, catalogId, catalogType)) {
			throw new IllegalArgumentException("The user does not have access to edit " + catalogId);
		}

		List<Dependency> dependencies = new ArrayList<>();
		for (Map<String, Object> entry : getDependencyMaps()) {
			Object id = entry.get("id");
			Object type = entry.get("type");
			if (!(id instanceof String) || !(type instanceof String)) {
				throw new IllegalArgumentException("Each dependency needs an id and a type");
			}
			String dependencyId = (String) id;
			if (dependencyId.equals(catalogId)) {
				throw new IllegalArgumentException("An engine or project cannot depend on itself");
			}
			String dependencyType = CatalogTypes.normalize((String) type);
			String stored = CatalogTypes.resolve(dependencyId);
			if (stored == null) {
				throw new IllegalArgumentException("No engine or project has the id '" + dependencyId + "'");
			}
			if (!stored.equals(dependencyType)) {
				throw new IllegalArgumentException(
						"'" + dependencyId + "' is a " + stored + ", not a " + dependencyType);
			}
			Object name = entry.get("name");
			Object description = entry.get("description");
			if ((name != null && !(name instanceof String)) || (description != null && !(description instanceof String))) {
				throw new IllegalArgumentException("A dependency's name and description must be text");
			}
			dependencies.add(Dependency.manual(dependencyId, dependencyType, (String) name, (String) description));
		}

		SecurityCatalogDependencyUtils.setManualDependencies(user, catalogId, catalogType, dependencies);

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully set the new dependencies"));
		return noun;
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition()
				.description("Set the engines and projects a user picked for an engine or project to depend on; "
						+ "the automatic dependencies derived from its configuration are kept")
				.key(CatalogDependencyKeys.CATALOG_ID, "The engine id or project id")
				.key(CatalogDependencyKeys.CATALOG_TYPE,
						"Its engine type or project type, such as FUNCTION or SKILL; read when not given")
				.key(CatalogDependencyKeys.DEPENDENCIES,
						"The engines and projects it depends on, each as {id, type, name?, description?} with its "
								+ "own engine type or project type. A name is a key unique for the item. An empty list "
								+ "removes every picked dependency.");
	}

	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> getDependencyMaps() {
		List<Map<String, Object>> dependencyList = new ArrayList<>();
		GenRowStruct grs = this.store.getGenRowStruct(CatalogDependencyKeys.DEPENDENCIES);
		if (grs != null && !grs.isEmpty()) {
			for (int i = 0; i < grs.size(); i++) {
				Object value = grs.get(i);
				if (!(value instanceof Map)) {
					throw new IllegalArgumentException("Each dependency must be a map of id and type");
				}
				dependencyList.add((Map<String, Object>) value);
			}
		}
		return dependencyList;
	}

}
