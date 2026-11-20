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

import java.util.Collection;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.access.PermissionTargetResource;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.util.SystemEngineRegistry;

/**
 * The one definition of a user's access level on a project, an engine or an
 * insight now: the highest level among the grants in effect, given to them
 * directly or to a team they are in. The permissions of a batch's user, and the
 * {@code userCanEdit*} and {@code userIsOwner} checks of
 * {@link SecurityProjectUtils}, {@link SecurityEngineUtils} and
 * {@link SecurityInsightUtils} all read it, so they always agree. Reading it
 * changes nothing: grants that have ended are left out, not removed.
 */
final class CurrentPermissionLevel {

	private static final Logger classLogger = LogManager.getLogger(CurrentPermissionLevel.class);

	private CurrentPermissionLevel() {

	}

	/**
	 * @return whether the user is an editor or an owner of the target now; false
	 *         when the rows cannot be read
	 */
	static boolean canEdit(User user, PermissionTargetResource target) {
		Integer level = ofOrNull(user, target);
		return level != null && AccessPermissionEnum.isEditor(level);
	}

	/**
	 * @return whether the user is an owner of the target now; false when the rows
	 *         cannot be read
	 */
	static boolean isOwner(User user, PermissionTargetResource target) {
		Integer level = ofOrNull(user, target);
		return level != null && AccessPermissionEnum.isOwner(level);
	}

	/**
	 * @return {@link #of}, or null when the rows cannot be read, which is logged
	 */
	private static Integer ofOrNull(User user, PermissionTargetResource target) {
		try {
			return of(user, target);
		} catch (IllegalArgumentException e) {
			return null;
		}
	}

	/**
	 * Reads the user's highest level on a project, an engine or an insight from the
	 * grants in effect now: their own, and those of the login provider groups and
	 * custom teams they are in. A grant, or a membership of a custom team, that has
	 * ended does not count, and neither does a row with no level. On an insight, an
	 * owner of its project is an owner of the insight.
	 *
	 * @return the level's id, or null when the user has no access now
	 * @throws IllegalArgumentException when the rows cannot be read
	 */
	static Integer of(User user, PermissionTargetResource target) {
		if (user == null) {
			return null;
		}
		String userTable;
		String groupTable;
		switch (target.getKind()) {
		case PROJECT:
			userTable = "PROJECTPERMISSION";
			groupTable = "GROUPPROJECTPERMISSION";
			break;
		case ENGINE:
			userTable = "ENGINEPERMISSION";
			groupTable = "GROUPENGINEPERMISSION";
			break;
		case INSIGHT:
			Integer projectLevel = of(user, PermissionTargetResource.project(target.getProjectId()));
			if (projectLevel != null && AccessPermissionEnum.isOwner(projectLevel)) {
				return projectLevel;
			}
			userTable = "USERINSIGHTPERMISSION";
			groupTable = "GROUPINSIGHTPERMISSION";
			break;
		default:
			return null;
		}

		SelectQueryStruct ownLevels = currentLevelsQuery(userTable, target);
		ownLevels.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(userTable + "__USERID", "==",
				AbstractSecurityUtils.getUserFiltersQs(user)));
		Integer best = highestLevel(ownLevels, target);

		IQueryFilter teams = currentTeamsFilter(user, groupTable);
		if (teams != null) {
			SelectQueryStruct teamLevels = currentLevelsQuery(groupTable, target);
			teamLevels.addExplicitFilter(teams);
			Integer teamLevel = highestLevel(teamLevels, target);
			if (teamLevel != null && (best == null || teamLevel < best)) {
				best = teamLevel;
			}
		}
		return best;
	}

	/**
	 * @return a query for the levels on the target in a permission table, leaving
	 *         out rows with no level and grants that have ended
	 */
	private static SelectQueryStruct currentLevelsQuery(String table, PermissionTargetResource target) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(table + "__PERMISSION"));
		filterToTarget(qs, table, target);
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter(table + "__PERMISSION", "!=", null, PixelDataType.NULL_VALUE));
		qs.addExplicitFilter(AbstractSecurityUtils.getUnexpiredFilter(table + "__ENDDATE"));
		return qs;
	}

	/**
	 * Keeps a query on a permission table to the target's rows: the project, the
	 * engine, or the insight in its project.
	 */
	static void filterToTarget(SelectQueryStruct qs, String table, PermissionTargetResource target) {
		if (target.getKind() == PermissionTargetResource.Kind.ENGINE) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(table + "__ENGINEID", "==", target.getId()));
		} else if (target.getKind() == PermissionTargetResource.Kind.INSIGHT) {
			qs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter(table + "__PROJECTID", "==", target.getProjectId()));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(table + "__INSIGHTID", "==", target.getId()));
		} else {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(table + "__PROJECTID", "==", target.getId()));
		}
	}

	/**
	 * @return a filter for the rows of the teams the user is in now: the groups
	 *         their logins carry, and the custom teams whose membership has not
	 *         ended; or null when they are in none
	 */
	private static IQueryFilter currentTeamsFilter(User user, String groupTable) {
		OrQueryFilter teams = new OrQueryFilter();
		boolean inAny = false;
		for (AuthProvider login : user.getLogins()) {
			AccessToken token = user.getAccessToken(login);
			Collection<String> loginGroups = token.getUserGroups();
			if (loginGroups != null && !loginGroups.isEmpty()) {
				AndQueryFilter loginGroup = new AndQueryFilter();
				loginGroup.addFilter(
						SimpleQueryFilter.makeColToValFilter(groupTable + "__TYPE", "==", token.getUserGroupType()));
				loginGroup.addFilter(SimpleQueryFilter.makeColToValFilter(groupTable + "__ID", "==", loginGroups));
				teams.addFilter(loginGroup);
				inAny = true;
			}
			List<String> customTeams = AdminSecurityGroupUtils.getUserCustomGroups(token);
			if (!customTeams.isEmpty()) {
				AndQueryFilter customTeam = new AndQueryFilter();
				customTeam.addFilter(SimpleQueryFilter.makeColToValFilter(groupTable + "__TYPE", "==",
						SecurityGroupManagerUtils.CUSTOM_GROUP_TYPE));
				customTeam.addFilter(SimpleQueryFilter.makeColToValFilter(groupTable + "__ID", "==", customTeams));
				teams.addFilter(customTeam);
				inAny = true;
			}
		}
		return inAny ? teams : null;
	}

	/**
	 * @return the highest level the query reads, where a lower id is a higher
	 *         level, or null when it reads none
	 * @throws IllegalArgumentException when the rows cannot be read
	 */
	private static Integer highestLevel(SelectQueryStruct qs, PermissionTargetResource target) {
		Integer best = null;
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance()
				.getRawWrapper(SystemEngineRegistry.getSecurityDb(), qs)) {
			while (wrapper.hasNext()) {
				Object value = wrapper.next().getValues()[0];
				if (value instanceof Number number && (best == null || number.intValue() < best)) {
					best = number.intValue();
				}
			}
		} catch (Exception e) {
			classLogger.error("Could not read the user's access to {}", target.describe(), e);
			throw new IllegalArgumentException("Could not read your access to " + target.describe());
		}
		return best;
	}
}
