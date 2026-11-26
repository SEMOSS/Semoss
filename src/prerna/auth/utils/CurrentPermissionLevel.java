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
	 * Whether the user's current level on the target, from {@link #of}, is at least
	 * editor, as the {@code userCanEdit*} checks read it.
	 *
	 * @return whether the user is an editor or an owner of the target now; false
	 *         when the rows cannot be read
	 */
	static boolean canEdit(User user, PermissionTargetResource target) {
		Integer level = ofOrNull(user, target);
		return level != null && AccessPermissionEnum.isEditor(level);
	}

	/**
	 * Whether the user's current level on the target, from {@link #of}, is owner,
	 * as the {@code userIsOwner} checks read it.
	 *
	 * @return whether the user is an owner of the target now; false when the rows
	 *         cannot be read
	 */
	static boolean isOwner(User user, PermissionTargetResource target) {
		Integer level = ofOrNull(user, target);
		return level != null && AccessPermissionEnum.isOwner(level);
	}

	/**
	 * Reads the user's current level on the target, treating a failed read as no
	 * access.
	 *
	 * @return {@link #of}, or null when the rows cannot be read, which is logged
	 */
	static Integer ofOrNull(User user, PermissionTargetResource target) {
		try {
			return of(user, target);
		} catch (IllegalArgumentException e) {
			return null;
		}
	}

	/**
	 * Reads the user's current level on the target and the team whose grant gives
	 * it, treating a failed read as no access.
	 *
	 * @return {@link #levelOf}, or null when the rows cannot be read, which is
	 *         logged
	 */
	static CurrentLevel levelOfOrNull(User user, PermissionTargetResource target) {
		try {
			return levelOf(user, target);
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
		CurrentLevel level = levelOf(user, target);
		return level == null ? null : level.levelId();
	}

	/**
	 * Reads the user's level on the target as {@link #of} does, and the team whose
	 * grant gives it when a team's grant is higher than the user's own. On an
	 * insight whose project the user owns, the level is the project's, from the
	 * user or the team that owns the project.
	 *
	 * @return the level and its team, or null when the user has no access now
	 * @throws IllegalArgumentException when the rows cannot be read
	 */
	static CurrentLevel levelOf(User user, PermissionTargetResource target) {
		if (user == null) {
			return null;
		}
		String userTable;
		String groupTable;
		switch (target.getResourceType()) {
		case PROJECT:
			userTable = "PROJECTPERMISSION";
			groupTable = "GROUPPROJECTPERMISSION";
			break;
		case ENGINE:
			userTable = "ENGINEPERMISSION";
			groupTable = "GROUPENGINEPERMISSION";
			break;
		case INSIGHT:
			CurrentLevel projectLevel = levelOf(user, PermissionTargetResource.project(target.getProjectId()));
			if (projectLevel != null && AccessPermissionEnum.isOwner(projectLevel.levelId())) {
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
		Integer own = highestLevel(ownLevels, target);

		IQueryFilter teams = currentTeamsFilter(user, groupTable);
		if (teams != null) {
			SelectQueryStruct teamLevels = currentLevelsQuery(groupTable, target);
			teamLevels.addSelector(new QueryColumnSelector(groupTable + "__ID"));
			teamLevels.addSelector(new QueryColumnSelector(groupTable + "__TYPE"));
			teamLevels.addExplicitFilter(teams);
			CurrentLevel team = highestTeamLevel(teamLevels, target);
			if (team != null && (own == null || team.levelId() < own)) {
				return team;
			}
		}
		return own == null ? null : new CurrentLevel(own, null, null);
	}

	/**
	 * A user's level on a project, an engine or an insight, and the team whose
	 * grant gives it.
	 *
	 * @param levelId  the level's id
	 * @param teamId   the team whose grant gives the level, or null when it is the
	 *                 user's own
	 * @param teamType that team's type, or null
	 */
	record CurrentLevel(int levelId, String teamId, String teamType) {
	}

	/**
	 * Builds a query for the {@code PERMISSION} column of the target's rows in a
	 * user or group permission table.
	 *
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
		if (target.getResourceType() == PermissionTargetResource.TargetResourceType.ENGINE) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(table + "__ENGINEID", "==", target.getId()));
		} else if (target.getResourceType() == PermissionTargetResource.TargetResourceType.INSIGHT) {
			qs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter(table + "__PROJECTID", "==", target.getProjectId()));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(table + "__INSIGHTID", "==", target.getId()));
		} else {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(table + "__PROJECTID", "==", target.getId()));
		}
	}

	/**
	 * Builds a filter on a group permission table for the user's teams, matching
	 * each login's groups by that login's group type, and the user's custom teams
	 * by the custom group type.
	 *
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
	 * Whether the user can open at least one insight of a project now: a public
	 * insight, or one given to them directly or to a login provider group or custom
	 * team they are in. A grant, or a membership of a custom team, that has ended
	 * does not count, and neither does a row with no level.
	 *
	 * @param user      the user
	 * @param projectId the project
	 * @return whether the project has an insight the user can open
	 * @throws IllegalArgumentException when the rows cannot be read
	 */
	static boolean canOpenAnInsightOf(User user, String projectId) {
		if (user == null) {
			return false;
		}
		PermissionTargetResource project = PermissionTargetResource.project(projectId);
		SelectQueryStruct publicInsights = new SelectQueryStruct();
		publicInsights.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTID"));
		publicInsights.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__PROJECTID", "==", projectId));
		publicInsights.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("INSIGHT__GLOBAL", "==", true, PixelDataType.BOOLEAN));
		if (hasRow(publicInsights, project)) {
			return true;
		}

		SelectQueryStruct ownInsights = insightGrantsQuery("USERINSIGHTPERMISSION", projectId);
		ownInsights.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==",
				AbstractSecurityUtils.getUserFiltersQs(user)));
		if (hasRow(ownInsights, project)) {
			return true;
		}

		IQueryFilter teams = currentTeamsFilter(user, "GROUPINSIGHTPERMISSION");
		if (teams == null) {
			return false;
		}
		SelectQueryStruct teamInsights = insightGrantsQuery("GROUPINSIGHTPERMISSION", projectId);
		teamInsights.addExplicitFilter(teams);
		return hasRow(teamInsights, project);
	}

	/**
	 * Builds a query for the insight grants in effect on any insight of a project,
	 * in a user or group insight permission table.
	 *
	 * @return a query for the grants, leaving out rows with no level and grants
	 *         that have ended
	 */
	private static SelectQueryStruct insightGrantsQuery(String table, String projectId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(table + "__INSIGHTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(table + "__PROJECTID", "==", projectId));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter(table + "__PERMISSION", "!=", null, PixelDataType.NULL_VALUE));
		qs.addExplicitFilter(AbstractSecurityUtils.getUnexpiredFilter(table + "__ENDDATE"));
		return qs;
	}

	/**
	 * Whether anyone has been given access to the target: a row of any level in its
	 * user or team permission table, ended or not. A project, an engine or an
	 * insight that was just created has none until its creator is made its owner.
	 *
	 * @param target the project, engine or insight
	 * @return whether the target has any permission row
	 * @throws IllegalArgumentException when the rows cannot be read
	 */
	static boolean hasAnyGrant(PermissionTargetResource target) {
		List<String> tables = switch (target.getResourceType()) {
		case PROJECT -> List.of("PROJECTPERMISSION", "GROUPPROJECTPERMISSION");
		case ENGINE -> List.of("ENGINEPERMISSION", "GROUPENGINEPERMISSION");
		case INSIGHT -> List.of("USERINSIGHTPERMISSION", "GROUPINSIGHTPERMISSION");
		default -> List.of();
		};
		for (String table : tables) {
			SelectQueryStruct qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector(table + "__PERMISSION"));
			filterToTarget(qs, table, target);
			if (hasRow(qs, target)) {
				return true;
			}
		}
		return false;
	}

	private static boolean hasRow(SelectQueryStruct qs, PermissionTargetResource target) {
		qs.setLimit(1);
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance()
				.getRawWrapper(SystemEngineRegistry.getSecurityDb(), qs)) {
			return wrapper.hasNext();
		} catch (Exception e) {
			classLogger.error("Could not read the user's access to {}", target.describe(), e);
			throw new IllegalArgumentException("Could not read your access to " + target.describe());
		}
	}

	/**
	 * Runs the query on the security database and keeps the highest level it reads.
	 *
	 * @return the highest level the query reads, where a lower id is a higher
	 *         level, or null when it reads none
	 * @throws IllegalArgumentException when the rows cannot be read
	 */
	/**
	 * Reads the highest level among a team permission query's rows, with the team
	 * that holds it: the first such team when several hold the same level.
	 *
	 * @param qs     a query selecting the level, the team's id and its type
	 * @param target the target, for the error
	 * @return the level and its team, or null when there are no rows
	 * @throws IllegalArgumentException when the rows cannot be read
	 */
	private static CurrentLevel highestTeamLevel(SelectQueryStruct qs, PermissionTargetResource target) {
		CurrentLevel best = null;
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance()
				.getRawWrapper(SystemEngineRegistry.getSecurityDb(), qs)) {
			while (wrapper.hasNext()) {
				Object[] values = wrapper.next().getValues();
				if (values[0] instanceof Number number && (best == null || number.intValue() < best.levelId())) {
					best = new CurrentLevel(number.intValue(), String.valueOf(values[1]), String.valueOf(values[2]));
				}
			}
		} catch (Exception e) {
			classLogger.error("Could not read the user's access to {}", target.describe(), e);
			throw new IllegalArgumentException("Could not read your access to " + target.describe());
		}
		return best;
	}

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
