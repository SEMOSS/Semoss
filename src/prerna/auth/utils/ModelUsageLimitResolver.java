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

import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.javatuples.Pair;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.access.ModelUsageLimit;
import prerna.auth.access.ModelUsageLimitKey;
import prerna.auth.access.ModelUsageLimitPath;
import prerna.auth.access.ModelUsageLimitPath.Layer;
import prerna.auth.access.ModelUsageLimitPath.Reason;
import prerna.auth.access.ModelUsageLimitPath.State;
import prerna.auth.access.ModelUsageLimitPath.Step;
import prerna.auth.access.ModelUsageLimitPath.StepLimitType;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.ValueUtils;

/**
 * Works out how model usage limits apply to one person on one model, without
 * looking at what is used: which ceilings there are, which sources have a limit
 * defined, and which source a call is billed to first.
 *
 * <p>
 * A person is looked up by their primary login, and their other logins' grants
 * and teams count as theirs too, the primary login's first, because access is
 * checked across all of them. The order of the sources is: the person's own
 * grant, their teams' grants (Unlimited teams first, then by when the person
 * got access through each team), the model's public use when the model is
 * public, the person's all-models default and the platform default. The first
 * one with a limit defined is billed. A grant with no limit defined passes the
 * decision on; Unlimited ends it.
 * </p>
 *
 * <ul>
 * <li><b>Ceilings.</b> The model's total is the path's ceiling, and each team
 * step carries the team's total; both are checked on every call, whatever is
 * billed, Unlimited included.</li>
 * <li><b>Teams.</b> The resolver only puts the teams with a limit defined in
 * billing order. At call time the guard bills the first one with usage left and
 * moves on to the next when one has none; only when none has usage left is the
 * call refused.</li>
 * <li><b>Who.</b> {@link #personOf} is the signed in user, with every login's
 * custom teams and the login provider teams from their sign-in.
 * {@link #storedPerson} is someone looked up by id and type, with custom teams
 * only, since provider teams are known only from the person's own sign-in; a
 * path read for someone else can miss a provider team's limit.</li>
 * <li><b>Once per model.</b> {@link #resolveStored} reads the model's grants
 * and limits once for many people, as Effective Permissions needs.</li>
 * </ul>
 */
public final class ModelUsageLimitResolver {

	private static final String CUSTOM = SecurityGroupManagerUtils.CUSTOM_GROUP_TYPE;

	private ModelUsageLimitResolver() {

	}

	/**
	 * A team the person is in.
	 *
	 * @param teamId   the team id
	 * @param teamType the team's type, {@code CUSTOM} or the login provider's group
	 *                 type
	 * @param joined   when the person joined it, or null for a team from a login
	 *                 provider, whose members are not stored
	 */
	public record TeamMembership(String teamId, String teamType, Timestamp joined) {
	}

	/**
	 * Someone a limit can apply to.
	 *
	 * @param userId the primary login's id
	 * @param type   the primary login's type
	 * @param logins every login, as id and type, the primary login first
	 * @param teams  the teams of every login
	 */
	public record Person(String userId, String type, List<Pair<String, String>> logins, List<TeamMembership> teams) {

		/**
		 * Checks that the person has a primary login, listed first in their logins, so
		 * every step of their path has a login to read its limit for.
		 *
		 * @throws IllegalArgumentException when the user id is blank, or the logins do
		 *                                  not start with the primary login
		 */
		public Person {
			ValueUtils.requireNonBlank(userId, "Must define the user");
			if (logins == null || logins.isEmpty() || !Pair.with(userId, type).equals(logins.get(0))) {
				throw new IllegalArgumentException("A person's logins must start with their primary login");
			}
			logins = List.copyOf(logins);
			teams = teams == null ? List.of() : List.copyOf(teams);
		}
	}

	/**
	 * Builds the person limits apply to from the signed in user: every login, the
	 * primary login first, with each login's custom teams and the login provider
	 * teams from its sign-in.
	 *
	 * @param user the signed in user
	 * @return the user as a person limits apply to, with every login's teams
	 * @throws IllegalArgumentException for an anonymous user, who cannot use models
	 */
	public static Person personOf(User user) {
		if (user == null || user.isAnonymous()) {
			throw new IllegalArgumentException("Sign in to use models");
		}
		Pair<String, String> primary = User.getPrimaryUserIdAndTypePair(user);
		List<Pair<String, String>> logins = new ArrayList<>();
		logins.add(primary);
		for (Pair<String, String> login : User.getUserIdAndType(user)) {
			if (!logins.contains(login)) {
				logins.add(login);
			}
		}
		List<TeamMembership> teams = new ArrayList<>();
		Set<String> seen = new HashSet<>();
		AuthProvider primaryLogin = user.getPrimaryLogin();
		List<AuthProvider> ordered = new ArrayList<>();
		ordered.add(primaryLogin);
		for (AuthProvider login : user.getLogins()) {
			if (login != primaryLogin) {
				ordered.add(login);
			}
		}
		for (AuthProvider login : ordered) {
			AccessToken token = user.getAccessToken(login);
			if (token == null) {
				continue;
			}
			for (TeamMembership membership : readCustomMemberships(token.getId(), login.getLabel())) {
				if (seen.add(membership.teamType() + ":" + membership.teamId())) {
					teams.add(membership);
				}
			}
			Collection<String> providerGroups = token.getUserGroups();
			String providerGroupType = token.getUserGroupType();
			if (providerGroups != null && providerGroupType != null) {
				for (String groupId : providerGroups) {
					if (seen.add(providerGroupType + ":" + groupId)) {
						teams.add(new TeamMembership(groupId, providerGroupType, null));
					}
				}
			}
		}
		return new Person(primary.getValue0(), primary.getValue1(), logins, teams);
	}

	/**
	 * Builds the person limits apply to from a stored user id and login type, for
	 * reading the path of someone other than the signed in user.
	 *
	 * @param userId the person's user id
	 * @param type   the person's login type
	 * @return the person as stored, with their custom teams only, since the teams
	 *         from a login provider are known only from the person's own sign-in
	 */
	public static Person storedPerson(String userId, String type) {
		ValueUtils.requireNonBlank(userId, "Must define the user");
		return new Person(userId, type, List.of(Pair.with(userId, type)), readCustomMemberships(userId, type));
	}

	/**
	 * Resolves how limits apply to a person on a model: the ceilings, which sources
	 * have a limit defined, and the source billed.
	 *
	 * @param person   the person
	 * @param engineId the model
	 * @return how limits apply to the person on the model
	 */
	public static ModelUsageLimitPath resolve(Person person, String engineId) {
		return resolve(person, EngineContext.load(engineId));
	}

	/**
	 * Resolves many stored people on one model, reading the model's grants and
	 * limits once.
	 *
	 * @param engineId the model
	 * @param people   the people, as user id and login type
	 * @return each person's path, by user id and login type
	 */
	public static Map<Pair<String, String>, ModelUsageLimitPath> resolveStored(String engineId,
			Collection<Pair<String, String>> people) {
		EngineContext context = EngineContext.load(engineId);
		Map<Pair<String, String>, ModelUsageLimitPath> paths = new LinkedHashMap<>();
		for (Pair<String, String> person : people) {
			paths.put(person, resolve(storedPerson(person.getValue0(), person.getValue1()), context));
		}
		return paths;
	}

	private static ModelUsageLimitPath resolve(Person person, EngineContext context) {
		String engineId = context.engineId;
		ModelUsageLimitPath path = new ModelUsageLimitPath(engineId, person.userId(), person.type());

		// the ceilings: the model's total
		ModelUsageLimitKey modelTotal = ModelUsageLimitKey.modelTotal(engineId);
		Step modelTotalStep = new Step(Layer.CEILING, StepLimitType.MODEL_TOTAL, modelTotal,
				context.engineLimits.get(modelTotal));
		if (modelTotalStep.isDefined()) {
			modelTotalStep.setState(State.CHECKED);
		}
		path.addCeiling(modelTotalStep);

		// the person's teams with a grant on the model, and the people's limits that
		// are not on the model: all-models defaults and team totals
		List<TeamMembership> teamsWithGrant = new ArrayList<>();
		for (TeamMembership membership : person.teams()) {
			if (context.teamGrants.containsKey(teamKey(membership.teamId(), membership.teamType()))) {
				teamsWithGrant.add(membership);
			}
		}
		List<ModelUsageLimitKey> personal = new ArrayList<>();
		for (Pair<String, String> login : person.logins()) {
			personal.add(ModelUsageLimitKey.allModels(login.getValue0(), login.getValue1()));
		}
		for (TeamMembership membership : teamsWithGrant) {
			personal.add(ModelUsageLimitKey.teamTotal(membership.teamId(), membership.teamType()));
		}
		personal.add(ModelUsageLimitKey.platform());
		Map<ModelUsageLimitKey, ModelUsageLimit> personalLimits = SecurityModelUsageLimitUtils.getLimits(personal);

		// 1. direct: the first login, primary first, whose grant has a limit defined,
		// or else the first login with a grant
		Step direct = null;
		for (Pair<String, String> login : person.logins()) {
			if (!context.directGrantUserIds.contains(login.getValue0())) {
				continue;
			}
			ModelUsageLimitKey key = ModelUsageLimitKey.direct(engineId, login.getValue0(), login.getValue1());
			Step step = new Step(Layer.BILLED, StepLimitType.DIRECT, key, SecurityModelUsageLimitUtils
					.findDirectLimit(context.engineLimits, engineId, login.getValue0(), login.getValue1()));
			if (direct == null || (!direct.isDefined() && step.isDefined())) {
				direct = step;
			}
		}
		if (direct == null) {
			direct = new Step(Layer.BILLED, StepLimitType.DIRECT,
					ModelUsageLimitKey.direct(engineId, person.userId(), person.type()), null).skipped(Reason.NO_GRANT);
		}
		path.addStep(direct);

		// 2. teams: the ones with a limit defined in billing order, then the rest
		List<Step> definedTeams = new ArrayList<>();
		List<Step> otherTeams = new ArrayList<>();
		Map<Step, Timestamp> accessSince = new HashMap<>();
		for (TeamMembership membership : teamsWithGrant) {
			ModelUsageLimitKey key = ModelUsageLimitKey.team(engineId, membership.teamId(), membership.teamType());
			ModelUsageLimitKey totalKey = ModelUsageLimitKey.teamTotal(membership.teamId(), membership.teamType());
			Timestamp granted = context.teamGrants.get(teamKey(membership.teamId(), membership.teamType()));
			Step step = new Step(Layer.BILLED, StepLimitType.TEAM, key, context.engineLimits.get(key))
					.withTeamTotal(totalKey, personalLimits.get(totalKey))
					// the person could use the model through the team only once both had
					// happened: they joined the team and the team got its grant
					.withAccessSince(isoUtc(later(membership.joined(), granted)));
			accessSince.put(step, later(membership.joined(), granted));
			(step.isDefined() ? definedTeams : otherTeams).add(step);
		}
		// Unlimited teams first, then by when the person got access through each
		// team, then by team id
		definedTeams.sort(Comparator.comparing((Step step) -> step.isUnlimited() ? 0 : 1)
				.thenComparing(accessSince::get, Comparator.nullsLast(Comparator.<Timestamp>naturalOrder()))
				.thenComparing(step -> step.getKey().subjectId()));
		definedTeams.forEach(path::addStep);
		otherTeams.forEach(path::addStep);

		// 3. public use, only on a public model
		ModelUsageLimitKey publicUseKey = ModelUsageLimitKey.publicUse(engineId);
		Step publicUse = new Step(Layer.BILLED, StepLimitType.PUBLIC, publicUseKey,
				context.engineLimits.get(publicUseKey));
		if (!context.isPublic) {
			publicUse.skipped(Reason.NOT_PUBLIC);
		}
		path.addStep(publicUse);

		// 4. the all-models default: the first login with one defined, primary first,
		// or else the primary login's
		ModelUsageLimitKey primaryAllModels = ModelUsageLimitKey.allModels(person.userId(), person.type());
		Step allModels = new Step(Layer.BILLED, StepLimitType.ALL_MODELS, primaryAllModels,
				personalLimits.get(primaryAllModels));
		for (Pair<String, String> login : person.logins()) {
			if (allModels.isDefined()) {
				break;
			}
			ModelUsageLimitKey key = ModelUsageLimitKey.allModels(login.getValue0(), login.getValue1());
			ModelUsageLimit limit = personalLimits.get(key);
			if (limit != null) {
				allModels = new Step(Layer.BILLED, StepLimitType.ALL_MODELS, key, limit);
			}
		}
		path.addStep(allModels);

		// 5. the platform default
		Step platform = new Step(Layer.BILLED, StepLimitType.PLATFORM, ModelUsageLimitKey.platform(),
				personalLimits.get(ModelUsageLimitKey.platform()));
		path.addStep(platform);

		// the first source with a limit defined is billed; the ones after it are not
		// reached
		StepLimitType billed = null;
		List<Step> order = new ArrayList<>();
		if (direct.isDefined()) {
			billed = StepLimitType.DIRECT;
			order.add(direct);
		} else if (!definedTeams.isEmpty()) {
			billed = StepLimitType.TEAM;
			order.addAll(definedTeams);
		} else if (context.isPublic && publicUse.isDefined()) {
			billed = StepLimitType.PUBLIC;
			order.add(publicUse);
		} else if (allModels.isDefined()) {
			billed = StepLimitType.ALL_MODELS;
			order.add(allModels);
		} else if (platform.isDefined()) {
			billed = StepLimitType.PLATFORM;
			order.add(platform);
		}
		path.setBilled(billed, order);
		boolean passed = false;
		for (Step step : path.getSteps()) {
			if (order.contains(step)) {
				passed = true;
			} else if (passed && step.isDefined() && step.getState() == State.DEFINED) {
				step.setState(State.NOT_REACHED);
			}
		}
		return path;
	}

	private static Timestamp later(Timestamp a, Timestamp b) {
		if (a == null) {
			return b;
		}
		if (b == null) {
			return a;
		}
		return a.after(b) ? a : b;
	}

	private static String isoUtc(Timestamp timestamp) {
		return timestamp == null ? null : timestamp.toLocalDateTime().toInstant(ZoneOffset.UTC).toString();
	}

	private static String teamKey(String teamId, String teamType) {
		return teamType + ":" + teamId;
	}

	private static boolean hasEnded(Timestamp endDate, Timestamp now) {
		return endDate != null && endDate.before(now);
	}

	private static List<TeamMembership> readCustomMemberships(String userId, String type) {
		if (userId == null || type == null) {
			return Collections.emptyList();
		}
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Timestamp now = Utility.getCurrentSqlTimestampUTC();
		try {
			List<TeamMembership> memberships = QueryExecutionUtility.queryList(securityDb,
					"SELECT GROUPID, DATEADDED, ENDDATE FROM CUSTOMGROUPASSIGNMENT WHERE USERID = ? AND TYPE = ?",
					ps -> {
						ps.setString(1, userId);
						ps.setString(2, type);
					}, rs -> hasEnded(rs.getTimestamp("ENDDATE"), now) ? null
							: new TeamMembership(rs.getString("GROUPID"), CUSTOM, rs.getTimestamp("DATEADDED")));
			memberships.removeIf(Objects::isNull);
			return memberships;
		} catch (Exception e) {
			throw new IllegalStateException("Unable to read the person's teams", e);
		}
	}

	/**
	 * What is read once for a model: who has a grant on it, whether it is public,
	 * and the limits defined on it.
	 */
	private static final class EngineContext {
		private final String engineId;
		private boolean isPublic;
		private final Set<String> directGrantUserIds = new HashSet<>();
		private final Map<String, Timestamp> teamGrants = new HashMap<>();
		private Map<ModelUsageLimitKey, ModelUsageLimit> engineLimits;

		private EngineContext(String engineId) {
			this.engineId = engineId;
		}

		private static EngineContext load(String engineId) {
			ValueUtils.requireNonBlank(engineId, "Must define the model");
			IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
			EngineContext context = new EngineContext(engineId);
			Timestamp now = Utility.getCurrentSqlTimestampUTC();
			try {
				Boolean global = QueryExecutionUtility.queryOne(securityDb,
						"SELECT GLOBAL FROM ENGINE WHERE ENGINEID = ?", ps -> ps.setString(1, engineId),
						rs -> rs.getBoolean(1));
				context.isPublic = Boolean.TRUE.equals(global);
				QueryExecutionUtility.queryList(securityDb,
						"SELECT USERID, ENDDATE FROM ENGINEPERMISSION WHERE ENGINEID = ? AND PERMISSION IS NOT NULL",
						ps -> ps.setString(1, engineId), rs -> {
							if (!hasEnded(rs.getTimestamp("ENDDATE"), now)) {
								context.directGrantUserIds.add(rs.getString("USERID"));
							}
							return null;
						});
				QueryExecutionUtility.queryList(securityDb,
						"SELECT ID, TYPE, DATEADDED, ENDDATE FROM GROUPENGINEPERMISSION WHERE ENGINEID = ? "
								+ "AND PERMISSION IS NOT NULL",
						ps -> ps.setString(1, engineId), rs -> {
							if (!hasEnded(rs.getTimestamp("ENDDATE"), now)) {
								context.teamGrants.put(teamKey(rs.getString("ID"), rs.getString("TYPE")),
										rs.getTimestamp("DATEADDED"));
							}
							return null;
						});
			} catch (Exception e) {
				throw new IllegalStateException("Unable to read the model's grants", e);
			}
			context.engineLimits = SecurityModelUsageLimitUtils.getEngineLimits(engineId);
			return context;
		}
	}
}
