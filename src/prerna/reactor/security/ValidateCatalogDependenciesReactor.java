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

import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityCatalogDependencyUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Whether the user can view each engine and project an engine or project
 * depends on, all the way down. Reads the security database alone, so it works
 * on any server of a cluster.
 */
public class ValidateCatalogDependenciesReactor extends AbstractReactor {

	public ValidateCatalogDependenciesReactor() {
		this.keysToGet = new String[] { CatalogDependencyKeys.CATALOG_ID, CatalogDependencyKeys.CATALOG_TYPE };
		this.keyRequired = new int[] { 1, 0 };
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
		Map<String, Object> canView = SecurityCatalogDependencyUtils.validateDependencies(user, catalogId);
		return new NounMetadata(canView, PixelDataType.MAP);
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition()
				.description("Whether the user can view each engine and project an engine or project depends on")
				.key(CatalogDependencyKeys.CATALOG_ID, "The engine id or project id")
				.key(CatalogDependencyKeys.CATALOG_TYPE,
						"Its engine type or project type, such as FUNCTION or SKILL; read when not given");
	}

}
