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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.CatalogDependencyResync;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;

/**
 * Works out the automatic dependencies of every engine, agent, automation and app
 * from what each has saved, for items saved before dependencies were derived:
 * engines' guardrails and smss references, model routers' routes, agents' tools,
 * knowledge, skills, model and subagents, automations' nodes, blocks apps'
 * engines, and the ids in code apps' files. Picked
 * dependencies are kept; a row from before IS_AUTOMATIC that a derived one
 * stands for is replaced. Safe to run again. Each item is read by
 * {@link CatalogDependencyResync}, as SyncCatalogDependencies reads one.
 */
public class AdminBackfillCatalogDependenciesReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AdminBackfillCatalogDependenciesReactor.class);

	public AdminBackfillCatalogDependenciesReactor() {
		this.keysToGet = new String[0];
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		if (!SecurityAdminUtils.userIsAdmin(user)) {
			throw new IllegalArgumentException("User is not an admin and does not have access. Please login as an admin");
		}

		int engines = 0;
		List<String> failed = new ArrayList<>();
		for (String engineId : SecurityEngineUtils.getAllEngineIds()) {
			try {
				CatalogDependencyResync.resyncEngine(user, engineId);
				engines++;
			} catch (Exception e) {
				classLogger.warn("Could not backfill the dependencies of engine {}", engineId, e);
				failed.add(engineId);
			}
		}

		int workspaces = 0;
		for (String projectId : projectIdsOfType(IProject.PROJECT_TYPE.WORKSPACE.name())) {
			try {
				CatalogDependencyResync.resyncWorkspace(user, projectId);
				workspaces++;
			} catch (Exception e) {
				classLogger.warn("Could not backfill the dependencies of agent {}", projectId, e);
				failed.add(projectId);
			}
		}

		int automations = 0;
		for (String projectId : projectIdsOfType(IProject.PROJECT_TYPE.AUTOMATION.name())) {
			try {
				CatalogDependencyResync.resyncAutomation(user, projectId);
				automations++;
			} catch (Exception e) {
				classLogger.warn("Could not backfill the dependencies of automation {}", projectId, e);
				failed.add(projectId);
			}
		}

		int blocks = 0;
		for (String projectId : projectIdsOfType(IProject.PROJECT_TYPE.BLOCKS.name())) {
			try {
				CatalogDependencyResync.resyncBlocks(user, projectId);
				blocks++;
			} catch (Exception e) {
				classLogger.warn("Could not backfill the dependencies of blocks app {}", projectId, e);
				failed.add(projectId);
			}
		}

		int code = 0;
		for (String projectId : projectIdsOfType(IProject.PROJECT_TYPE.CODE.name())) {
			try {
				CatalogDependencyResync.resyncCode(user, projectId);
				code++;
			} catch (Exception e) {
				classLogger.warn("Could not backfill the dependencies of code app {}", projectId, e);
				failed.add(projectId);
			}
		}

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("engines", engines);
		result.put("agents", workspaces);
		result.put("automations", automations);
		result.put("blocks", blocks);
		result.put("code", code);
		result.put("failed", failed);
		return new NounMetadata(result, PixelDataType.MAP);
	}

	private static List<String> projectIdsOfType(String type) {
		try {
			return QueryExecutionUtility.queryList(SystemEngineRegistry.getSecurityDb(),
					"SELECT PROJECTID FROM PROJECT WHERE TYPE=?", ps -> ps.setString(1, type), rs -> rs.getString(1));
		} catch (Exception e) {
			classLogger.error("Could not list the {} projects", type, e);
			return List.of();
		}
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().noKeys()
				.description("Admin Only: Works out the automatic dependencies of every engine, agent, automation and app "
						+ "from its saved configuration: guardrails, judge models, vector databases, embedders, router "
						+ "models, agent tools, knowledge, skills, models and subagents, automation steps, blocks app engines "
						+ "and the ids in code apps' files. "
						+ "Picked dependencies are kept. Safe to run again.")
				.output(Map.class, "How many engines, agents, automations, blocks apps and code apps were read, and the ids that failed");
	}
}
