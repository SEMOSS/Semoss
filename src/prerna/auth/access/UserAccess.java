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

import prerna.auth.AccessPermissionEnum;

/**
 * The access a person holds on an engine or a project now, and where it comes
 * from: their own grant, a team's grant, or the engine or project being public.
 * When the person's own grant and a team's give the same level, the source is
 * their own.
 *
 * @param level    the highest level in effect
 * @param source   where that level comes from
 * @param teamId   the team whose grant gives the level, for
 *                 {@link Source#TEAM}; otherwise null
 * @param teamType that team's type, such as CUSTOM or a login provider's group
 *                 type; otherwise null
 */
public record UserAccess(AccessPermissionEnum level, Source source, String teamId, String teamType) {

	/** Where a person's access comes from */
	public enum Source {
		/** Their own grant */
		DIRECT,
		/** A team's grant: a custom team or a login provider's group they are in */
		TEAM,
		/** The engine or project is public, which gives everyone Read-Only */
		PUBLIC
	}

	/**
	 * Access from the person's own grant.
	 *
	 * @param level the level
	 * @return the access
	 */
	public static UserAccess direct(AccessPermissionEnum level) {
		return new UserAccess(level, Source.DIRECT, null, null);
	}

	/**
	 * Access from a team's grant.
	 *
	 * @param level    the level
	 * @param teamId   the team
	 * @param teamType its type
	 * @return the access
	 */
	public static UserAccess team(AccessPermissionEnum level, String teamId, String teamType) {
		return new UserAccess(level, Source.TEAM, teamId, teamType);
	}

	/**
	 * Read-Only access to a public engine or project.
	 *
	 * @return the access
	 */
	public static UserAccess publicRead() {
		return new UserAccess(AccessPermissionEnum.READ_ONLY, Source.PUBLIC, null, null);
	}

}
