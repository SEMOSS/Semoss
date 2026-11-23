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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.CatalogDependencyResync;
import prerna.auth.utils.SecurityCatalogDependencyUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * Works out the automatic dependencies of one engine or project again from what
 * it has saved, and records them, as AdminBackfillCatalogDependencies does for
 * every item: an engine's smss references, guardrails and model router routes,
 * an agent's tools, knowledge, skills, model and subagents, an automation's
 * nodes, a blocks app's engines, and the ids in a code app's files. Picked
 * dependencies are kept. A project type with nothing to derive is left as it is.
 * Fails when the item cannot be read or its dependencies cannot be recorded.
 */
public class SyncCatalogDependenciesReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SyncCatalogDependenciesReactor.class);

	public SyncCatalogDependenciesReactor() {
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
		if (!SecurityCatalogDependencyUtils.userCanEdit(user, catalogId, catalogType)) {
			throw new IllegalArgumentException("The user does not have access to edit " + catalogId);
		}

		try {
			CatalogDependencyResync.resync(user, catalogId, catalogType);
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Could not sync the dependencies of {}", Utility.cleanLogString(catalogId), e);
			throw new IllegalStateException("Could not sync the dependencies of " + catalogId + ": " + e.getMessage(),
					e);
		}

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("catalogId", catalogId);
		result.put("catalogType", catalogType);
		result.put("automatic", SecurityCatalogDependencyUtils.countAutomaticDependencies(catalogId));
		NounMetadata noun = new NounMetadata(result, PixelDataType.MAP);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully synced the dependencies"));
		return noun;
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition()
				.description("Work out the automatic dependencies of an engine or project again from its saved "
						+ "configuration and record them; the dependencies a user picked are kept")
				.key(CatalogDependencyKeys.CATALOG_ID, "The engine id or project id")
				.key(CatalogDependencyKeys.CATALOG_TYPE,
						"Its engine type or project type, such as FUNCTION or SKILL; read when not given")
				.output(Map.class, "The catalogId, its catalogType, and how many automatic dependencies it has now");
	}
}
