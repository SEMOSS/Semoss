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
package prerna.auth.access;

/**
 * What a permission batch changes: who can use a project, an engine or an
 * insight, or who is in or manages a custom group.
 */
public class PermissionTargetResource {

	/**
	 * The types of target resource: {@code TEAM} is a custom group's members and
	 * {@code TEAM_MANAGERS} its managers, each with the group's name as the id
	 */
	public enum TargetResourceType {
		PROJECT, ENGINE, INSIGHT, TEAM, TEAM_MANAGERS
	}

	/** What the target is */
	private TargetResourceType resourceType;
	private String id;
	/** For an insight, the project it belongs to */
	private String projectId;

	public PermissionTargetResource() {

	}

	/**
	 * Creates a target from its resource type, its id and, for an insight, its
	 * project.
	 *
	 * @param resourceType what the target is
	 * @param id           the project, engine, insight or group id
	 * @param projectId    for an insight, the project it belongs to, otherwise null
	 */
	public PermissionTargetResource(TargetResourceType resourceType, String id, String projectId) {
		this.resourceType = resourceType;
		this.id = id;
		this.projectId = projectId;
	}

	/**
	 * Builds the target for changing who can use a project.
	 *
	 * @return who can use the project
	 */
	public static PermissionTargetResource project(String projectId) {
		return new PermissionTargetResource(TargetResourceType.PROJECT, projectId, null);
	}

	/**
	 * Builds the target for changing who can use an engine.
	 *
	 * @return who can use the engine
	 */
	public static PermissionTargetResource engine(String engineId) {
		return new PermissionTargetResource(TargetResourceType.ENGINE, engineId, null);
	}

	/**
	 * Builds the target for changing who can use an insight in the given project.
	 *
	 * @return who can use the insight
	 */
	public static PermissionTargetResource insight(String projectId, String insightId) {
		return new PermissionTargetResource(TargetResourceType.INSIGHT, insightId, projectId);
	}

	/**
	 * Builds the target for changing who is a member of a custom group.
	 *
	 * @return who is in the custom group
	 */
	public static PermissionTargetResource team(String groupId) {
		return new PermissionTargetResource(TargetResourceType.TEAM, groupId, null);
	}

	/**
	 * Builds the target for changing who manages a custom group.
	 *
	 * @return who manages the custom group
	 */
	public static PermissionTargetResource teamManagers(String groupId) {
		return new PermissionTargetResource(TargetResourceType.TEAM_MANAGERS, groupId, null);
	}

	/**
	 * What the target is: a project, an engine, an insight, or a custom group's
	 * members or managers.
	 *
	 * @return the target's resource type
	 */
	public TargetResourceType getResourceType() {
		return resourceType;
	}

	public String getId() {
		return id;
	}

	public String getProjectId() {
		return projectId;
	}

	/**
	 * Describes the target as its type and id, or as the managers of a team for
	 * {@code TEAM_MANAGERS}.
	 *
	 * @return the target in words, for messages and logs, such as "project abc"
	 */
	public String describe() {
		if (resourceType == TargetResourceType.TEAM_MANAGERS) {
			return "the managers of team " + id;
		}
		String noun = resourceType == null ? "target"
				: resourceType == TargetResourceType.TEAM ? "team" : resourceType.name().toLowerCase();
		return noun + " " + id;
	}
}
