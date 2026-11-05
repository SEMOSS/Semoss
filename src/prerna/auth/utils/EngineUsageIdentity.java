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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.javatuples.Pair;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.ValueUtils;

/** Shared primary-login and team identity for engine usage adapters. */
public final class EngineUsageIdentity {
	private static final String CUSTOM = SecurityGroupManagerUtils.CUSTOM_GROUP_TYPE;

	private EngineUsageIdentity() {
	}

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
	 * @throws IllegalArgumentException for an anonymous user, who cannot use
	 *                                  engines
	 */
	public static Person personOf(User user) {
		if (user == null || user.isAnonymous()) {
			throw new IllegalArgumentException("Sign in to use engines");
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

}
