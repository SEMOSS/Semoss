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
package prerna.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class SystemDefaultEngines {

	private static final Set<String> SYSTEM_ENGINE_IDS = Set.of(Constants.SECURITY_DB, Constants.LOCAL_MASTER_DB,
			Constants.SCHEDULER_DB, Constants.THEMING_DB, Constants.USER_TRACKING_DB, Constants.PROMPT_DB,
			Constants.NOTIFICATION_DB, Constants.AUDIT_LOGS_DB, Constants.MODEL_INFERENCE_LOGS_DB);

	private static final List<String> IGNORE_DATABASE_OWL = Collections
			.unmodifiableList(new ArrayList<>(SYSTEM_ENGINE_IDS));

	private static final List<String> DATABASE_GENERATED_OWL = Collections
			.unmodifiableList(new ArrayList<>(SYSTEM_ENGINE_IDS));

	private static final List<String> DATABASE_IGNORE_LOCALMASTER = Collections
			.unmodifiableList(new ArrayList<>(SYSTEM_ENGINE_IDS));

	private static final List<String> DATABASE_IGNORE_SECURITY = Collections
			.unmodifiableList(new ArrayList<>(SYSTEM_ENGINE_IDS));

	private static final List<String> SYSTEM_APPS = List.of(Constants.APP_REACT);

	private static final List<String> SYSTEM_SKILLS = List.of(Constants.SKILL_AGENT_RUN, Constants.SKILL_APP_BOOTSTRAP,
			Constants.SKILL_APP_DATA, Constants.SKILL_BUILD_AND_PUBLISH, Constants.SKILL_DATABASE,
			Constants.SKILL_EXPORTS, Constants.SKILL_FILE_UPLOADS, Constants.SKILL_FUNCTIONS, Constants.SKILL_MODEL,
			Constants.SKILL_PAGINATION, Constants.SKILL_PERMISSIONS, Constants.SKILL_PYTHON, Constants.SKILL_ROOM,
			Constants.SKILL_STORAGE, Constants.SKILL_USER, Constants.SKILL_VECTOR);

	/**
	 * Platform MCPs cataloged at boot by {@code ProjectWatcher.init()}. Every entry
	 * must have a matching {@code project/platform__<id>} folder.
	 */
	private static final List<String> SYSTEM_MCPS = List.of(Constants.MCP_NODE_BUILDER, Constants.MCP_DATABASE_MAKER,
			Constants.MCP_REACTOR_HELP, Constants.MCP_BROWSER_AUTOMATION, Constants.MCP_APP_FILESYSTEM,
			Constants.MCP_ROOM_FILESYSTEM);

	/**
	 * Subset of {@link #SYSTEM_MCPS} seeded onto system agent workspaces. This is
	 * deliberately narrower than the cataloged list: UI-driven MCPs (whose tools
	 * open a sidebar app and wait on a user) are not useful to a headless agent, so
	 * they are cataloged and invokable without being attached to every system
	 * agent.
	 */
	private static final List<String> SYSTEM_AGENT_MCPS = List.of(Constants.MCP_NODE_BUILDER,
			Constants.MCP_DATABASE_MAKER, Constants.MCP_REACTOR_HELP);

	private static final List<String> SYSTEM_AGENTS = List.of(Constants.AGENT_APP_BUILDER);

	public static List<String> getIgnoreDatabaseOwlList() {
		return IGNORE_DATABASE_OWL;
	}

	public static List<String> getDatabasesWithGeneratedOwl() {
		return DATABASE_GENERATED_OWL;
	}

	public static List<String> getDatabaseIgnoreLocalMaster() {
		return DATABASE_IGNORE_LOCALMASTER;
	}

	public static List<String> getDatabaseIgnoreSecurity() {
		return DATABASE_IGNORE_SECURITY;
	}

	public static List<String> getSystemApps() {
		return SYSTEM_APPS;
	}

	public static List<String> getSystemSkills() {
		return SYSTEM_SKILLS;
	}

	public static List<String> getSystemMCPs() {
		return SYSTEM_MCPS;
	}

	public static List<String> getSystemAgentMCPs() {
		return SYSTEM_AGENT_MCPS;
	}

	public static List<String> getSystemAgents() {
		return SYSTEM_AGENTS;
	}

	/**
	 * Check if a string starts with any value within a collection
	 *
	 * @param strValue
	 * @param collection
	 * @return
	 */
	public static boolean valueStartsWith(String strValue, Collection<String> collection) {
		for (String c : collection) {
			if (strValue.startsWith(c)) {
				return true;
			}
		}
		return false;
	}
}
