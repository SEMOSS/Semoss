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
package prerna.auth.utils;

import prerna.auth.access.PermissionProblem;

/**
 * The permission changes a user may make on one target, worked out once for
 * each batch: what their own permissions allow, or everything in admin mode.
 * Outside admin mode, being an admin gives nothing extra.
 * {@code getPermissions} returns it as {@code allowedPermissionChanges}. Only
 * {@link PermissionBatchUtils} works it out, in
 * {@code readPermissionAllowedChanges}.
 */
final class PermissionAllowedChanges {

	/**
	 * Whether the user can change people's access, or a team's members or managers
	 */
	private boolean canChangePeople;
	/** Whether the user owns the target, so can give or change Author access */
	private boolean isOwner;
	/** Whether the user can change which teams have access */
	private boolean canChangeTeams;
	/** Whether only admins can give people access to the target */
	private boolean addsAreAdminOnly;
	/** Why the user cannot change people, for a change that needs it */
	private String peopleReason;
	/**
	 * The kind of problem when the user cannot change people: no permission, or a
	 * team that is not a custom team
	 */
	private PermissionProblem.Code peopleCode = PermissionProblem.Code.NO_PERMISSION;
	/** Why the user cannot change which teams have access */
	private String teamsReason;

	boolean canChangePeople() {
		return canChangePeople;
	}

	void setCanChangePeople(boolean canChangePeople) {
		this.canChangePeople = canChangePeople;
	}

	boolean isOwner() {
		return isOwner;
	}

	void setOwner(boolean isOwner) {
		this.isOwner = isOwner;
	}

	boolean canChangeTeams() {
		return canChangeTeams;
	}

	void setCanChangeTeams(boolean canChangeTeams) {
		this.canChangeTeams = canChangeTeams;
	}

	boolean addsAreAdminOnly() {
		return addsAreAdminOnly;
	}

	void setAddsAreAdminOnly(boolean addsAreAdminOnly) {
		this.addsAreAdminOnly = addsAreAdminOnly;
	}

	String getPeopleReason() {
		return peopleReason;
	}

	void setPeopleReason(String peopleReason) {
		this.peopleReason = peopleReason;
	}

	PermissionProblem.Code getPeopleCode() {
		return peopleCode;
	}

	void setPeopleCode(PermissionProblem.Code peopleCode) {
		this.peopleCode = peopleCode;
	}

	String getTeamsReason() {
		return teamsReason;
	}

	void setTeamsReason(String teamsReason) {
		this.teamsReason = teamsReason;
	}
}
