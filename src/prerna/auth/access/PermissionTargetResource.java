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
	 * The kinds of target: {@code TEAM} is a custom group's members and
	 * {@code TEAM_MANAGERS} its managers, each with the group's name as the id
	 */
	public enum Kind {
		PROJECT, ENGINE, INSIGHT, TEAM, TEAM_MANAGERS
	}

	private Kind kind;
	private String id;
	/** For an insight, the project it belongs to */
	private String projectId;

	public PermissionTargetResource() {

	}

	/**
	 * @param kind      the kind of target
	 * @param id        the project, engine, insight or group id
	 * @param projectId for an insight, the project it belongs to, otherwise null
	 */
	public PermissionTargetResource(Kind kind, String id, String projectId) {
		this.kind = kind;
		this.id = id;
		this.projectId = projectId;
	}

	/**
	 * @return who can use the project
	 */
	public static PermissionTargetResource project(String projectId) {
		return new PermissionTargetResource(Kind.PROJECT, projectId, null);
	}

	/**
	 * @return who can use the engine
	 */
	public static PermissionTargetResource engine(String engineId) {
		return new PermissionTargetResource(Kind.ENGINE, engineId, null);
	}

	/**
	 * @return who can use the insight
	 */
	public static PermissionTargetResource insight(String projectId, String insightId) {
		return new PermissionTargetResource(Kind.INSIGHT, insightId, projectId);
	}

	/**
	 * @return who is in the custom group
	 */
	public static PermissionTargetResource team(String groupId) {
		return new PermissionTargetResource(Kind.TEAM, groupId, null);
	}

	/**
	 * @return who manages the custom group
	 */
	public static PermissionTargetResource teamManagers(String groupId) {
		return new PermissionTargetResource(Kind.TEAM_MANAGERS, groupId, null);
	}

	public Kind getKind() {
		return kind;
	}

	public String getId() {
		return id;
	}

	public String getProjectId() {
		return projectId;
	}

	/**
	 * @return the target in words, for messages and logs, such as "project abc"
	 */
	public String describe() {
		if (kind == Kind.TEAM_MANAGERS) {
			return "the managers of team " + id;
		}
		String noun = kind == null ? "target" : kind == Kind.TEAM ? "team" : kind.name().toLowerCase();
		return noun + " " + id;
	}
}
