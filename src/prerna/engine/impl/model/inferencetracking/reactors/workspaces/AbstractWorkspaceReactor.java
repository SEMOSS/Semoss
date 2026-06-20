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
package prerna.engine.impl.model.inferencetracking.reactors.workspaces;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.util.Utility;

/**
 * Base reactor for workspace operations that need to assemble workspace-resource mappings.
 * <p>
 * Provides common helper methods to construct normalized resource rows for engines, projects,
 * and prompts so concrete workspace reactors can reuse consistent payload structures.
 */
public abstract class AbstractWorkspaceReactor extends AbstractReactor {

	/** Resource type identifier used when a workspace resource points to a prompt. */
	public static final String PROMPT_RESOURCE_TYPE = "PROMPT";

	/** Resource type identifier used when a workspace resource points to a skill in the skill registry. */
	public static final String SKILL_RESOURCE_TYPE = "SKILL";

	/** Request key for workspace name. */
	static final String NAME = "name";
	/** Request key for workspace description. */
	static final String DESCRIPTION = "description";
	/** Request key for workspace-level system prompt. */
	static final String SYSTEM_PROMPT = "systemPrompt";
	/** Request key for prompt collection input. */
	static final String PROMPTS = "prompts";
	/** Request key for skill collection input. */
	static final String SKILLS = "skills";
	/** Request key for active/inactive workspace state. */
	static final String IS_ACTIVE = "isActive";

	/**
	 * Builds a workspace resource row for an engine, including engine type metadata.
	 *
	 * @param workspaceId workspace identifier that owns the resource
	 * @param engineId engine identifier being linked to the workspace
	 * @return map representing a row for workspace resource persistence
	 */
	Map<String, String> makeResourceEntryMap(String workspaceId, String engineId) {
		Map<String, String> resource = new HashMap<>();
		Object[] typeAndSubtype = SecurityEngineUtils.getEngineTypeAndSubtype(engineId);
		resource.put("workspace_resource_id", UUID.randomUUID().toString());
		resource.put("workspace_id", workspaceId);
		resource.put("resource_id", engineId);
		resource.put("resource_type", typeAndSubtype[0].toString());
		resource.put("resource_subtype", typeAndSubtype[1].toString());
		return resource;
	}

	/**
	 * Builds a workspace resource row for a project.
	 *
	 * @param workspaceId workspace identifier that owns the resource
	 * @param projectId project identifier being linked to the workspace
	 * @return map representing a row for workspace resource persistence
	 */
	Map<String, String> makeProjectResourceEntryMap(String workspaceId, String projectId) {
		Map<String, String> resource = new HashMap<>();
		IProject projectObj = Utility.getProject(projectId);
		resource.put("workspace_resource_id", UUID.randomUUID().toString());
		resource.put("workspace_id", workspaceId);
		resource.put("resource_id", projectId);
		resource.put("resource_type", CATALOG_TYPE.PROJECT.name());
		resource.put("resource_subtype", projectObj.getProjectType().name());
		return resource;
	}

	/**
	 * Builds a workspace resource row for a prompt.
	 *
	 * @param workspaceId workspace identifier that owns the resource
	 * @param promptId prompt identifier being linked to the workspace
	 * @return map representing a row for workspace resource persistence
	 */
	Map<String, String> makePromptResourceEntryMap(String workspaceId, String promptId) {
		Map<String, String> resource = new HashMap<>();
		resource.put("workspace_resource_id", UUID.randomUUID().toString());
		resource.put("workspace_id", workspaceId);
		resource.put("resource_id", promptId);
		resource.put("resource_type", PROMPT_RESOURCE_TYPE);
		resource.put("resource_subtype", null);
		return resource;
	}

	/**
	 * Builds a workspace resource row for a skill.
	 * <p>
	 * The {@code resource_subtype} carries the optional pinned skill version
	 * ({@code AgentConfigLoader.resolveSkills} reads it back as {@code pinned_version}).
	 * It is left {@code null} here because the workspace edit/add inputs only carry
	 * skill ids - matching {@link prerna.reactor.agent.skill.AttachSkillToWorkspaceReactor},
	 * which also attaches with no pinned version.
	 *
	 * @param workspaceId workspace identifier that owns the resource
	 * @param skillId skill identifier being linked to the workspace
	 * @return map representing a row for workspace resource persistence
	 */
	Map<String, String> makeSkillResourceEntryMap(String workspaceId, String skillId) {
		Map<String, String> resource = new HashMap<>();
		resource.put("workspace_resource_id", UUID.randomUUID().toString());
		resource.put("workspace_id", workspaceId);
		resource.put("resource_id", skillId);
		resource.put("resource_type", SKILL_RESOURCE_TYPE);
		resource.put("resource_subtype", null);
		return resource;
	}

	/**
	 * Returns MCP configuration entries from the incoming reactor request payload.
	 *
	 * @return list of MCP maps; each map represents one MCP configuration object
	 */
	List<Map<String, Object>> getMcpMapList() {
		return getList(ReactorKeysEnum.MCP.getKey(), List.of());
	}

}
