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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;

import prerna.auth.User;
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.engine.impl.usage.UsageLimit;
import prerna.engine.impl.usage.UsageLimitKey;
import prerna.engine.impl.usage.UsageLimitKey.Source;
import prerna.engine.impl.usage.UsagePlan;
import prerna.engine.impl.usage.UsagePlan.Candidate;
import prerna.engine.impl.usage.UsagePlan.Target;
import prerna.engine.impl.usage.UsageRule.Counts;
import prerna.engine.impl.usage.UsageStoreException;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

/**
 * Shared policy hierarchy for new catalog adapters; policies do not grant
 * access.
 */
public final class EngineUsagePolicyResolver {
	private final CATALOG_TYPE catalog;

	private EngineUsagePolicyResolver(CATALOG_TYPE catalog) {
		this.catalog = catalog;
	}

	public static UsagePlan resolve(User user, CATALOG_TYPE catalog, String engineId) {
		SecurityEngineUsageLimitUtils.validate(new UsageLimitKey(catalog, Source.PLATFORM, null, null, null), null);
		return new EngineUsagePolicyResolver(catalog).resolveUser(user, engineId);
	}

	private UsagePlan resolveUser(User user, String engineId) {
		Boolean isPublic = registeredPublic(engineId);
		if (isPublic == null)
			return new UsagePlan(List.of(), List.of(), List.of());
		if (user != null && user.isAnonymous()
				&& SecurityEngineUsageLimitUtils.readForEngine(catalog, engineId).isEmpty())
			return new UsagePlan(List.of(), List.of(), List.of());
		var person = EngineUsageIdentity.personOf(user);
		List<SecurityEngineUsageLimitUtils.Entry> entries = SecurityEngineUsageLimitUtils.readForPerson(catalog,
				engineId, person);
		if (entries.isEmpty())
			return new UsagePlan(List.of(), List.of(), List.of());
		Map<UsageLimitKey, UsageLimit> limits = new HashMap<>();
		for (var entry : entries) {
			SecurityEngineUsageLimitUtils.validate(entry.key(), entry.usageLimit());
			limits.put(entry.key(), entry.usageLimit());
		}
		List<Target> ceilings = targets(person, key(Source.ENGINE_TOTAL, engineId, null, null), limits, false);
		List<Target> tracking = new ArrayList<>();
		List<Candidate> candidates = new ArrayList<>();
		// A direct policy stays attached to the identity even if access is removed
		// and re-added, or the engine becomes public. It does not grant access.
		UsageLimitKey direct = null;
		UsageLimitKey total = null;
		Set<String> granted=directGrants(engineId);
		for (var login : person.logins()) {
			var d = key(Source.DIRECT, engineId, login.getValue0(), login.getValue1());
			var t = key(Source.USER_TOTAL, null, login.getValue0(), login.getValue1());
			if (direct == null && granted.contains(login.getValue0()) && limits.containsKey(d))
				direct = d;
			if (total == null && limits.containsKey(t))
				total = t;
		}
		if (direct != null) {
			tracking.addAll(targets(person, direct, limits, false));
			candidates.add(candidate(person, direct, limits));
		}
		List<Team> teams = teams(person, engineId, limits);
		if (candidates.isEmpty())
			for (Team team : teams) {
				var targets = new ArrayList<>(targets(person, team.key, limits, false));
				targets.addAll(targets(person,
						key(Source.TEAM_TOTAL, null, team.key.subjectId(), team.key.subjectType()), limits, false));
				candidates.add(new Candidate(team.key, targets));
			}
		var publicKey = key(Source.PUBLIC, engineId, null, null);
		tracking.addAll(targets(person, publicKey, limits, true));
		if (candidates.isEmpty() && isPublic && limits.containsKey(publicKey))
			candidates.add(candidate(person, publicKey, limits));
		if (total != null) {
			tracking.addAll(targets(person, total, limits, false));
			if (candidates.isEmpty())
				candidates.add(candidate(person, total, limits));
		}
		var platform = key(Source.PLATFORM, null, null, null);
		tracking.addAll(targets(person, platform, limits, false));
		if (candidates.isEmpty() && limits.containsKey(platform))
			candidates.add(candidate(person, platform, limits));
		return new UsagePlan(ceilings, candidates, tracking);
	}

	private Set<String> directGrants(String engine) {
		try {
			var now=Utility.getCurrentSqlTimestampUTC();
			Set<String> ids=new HashSet<>();
			QueryExecutionUtility.queryList(SystemEngineRegistry.getSecurityDb(),"SELECT USERID,ENDDATE FROM ENGINEPERMISSION WHERE ENGINEID = ? AND PERMISSION IS NOT NULL",ps -> ps.setString(1,engine),rs -> {
				var end=rs.getTimestamp(2);if(end==null || !end.before(now))ids.add(rs.getString(1));return null;
			});return ids;
		} catch(Exception failure) {throw new UsageStoreException("Unable to resolve engine grants",failure);}
	}

	private UsageLimitKey key(Source source, String engine, String id, String type) {
		return new UsageLimitKey(catalog, source, engine, id, type);
	}

	private Candidate candidate(EngineUsageIdentity.Person person, UsageLimitKey key,
			Map<UsageLimitKey, UsageLimit> limits) {
		return new Candidate(key, targets(person, key, limits, false));
	}

	private List<Target> targets(EngineUsageIdentity.Person person, UsageLimitKey key,
			Map<UsageLimitKey, UsageLimit> limits, boolean eachOnly) {
		UsageLimit limit = limits.get(key);
		if (limit == null || limit.unlimited())
			return List.of();
		List<Target> targets = new ArrayList<>();
		for (var rule : limit.rules()) {
			if (eachOnly && rule.counts() != Counts.EACH_PERSON)
				continue;
			String scope;
			switch (key.source()) {
			case DIRECT -> scope = scope("PERSON_ENGINE", key.engineId(), person.userId(), person.type());
			case PUBLIC -> scope = rule.counts() == Counts.EACH_PERSON
					? scope("PERSON_ENGINE", key.engineId(), person.userId(), person.type())
					: scope("PUBLIC_ENGINE", key.engineId());
			case USER_TOTAL, PLATFORM -> scope = scope("PERSON_CATALOG", person.userId(), person.type());
			case ENGINE_TOTAL -> scope = scope("ENGINE_TOTAL", key.engineId());
			case TEAM_TOTAL -> scope = scope("TEAM_TOTAL", key.subjectId(), key.subjectType());
			case TEAM -> scope = rule.counts() == Counts.SHARED
					? scope("TEAM_ENGINE", key.engineId(), key.subjectId(), key.subjectType())
					: scope("TEAM_MEMBER", key.engineId(), key.subjectId(), key.subjectType(), person.userId(),
							person.type());
			default -> throw new IllegalStateException("Unknown usage source");
			}
			targets.add(new Target(scope, rule));
		}
		return targets;
	}

	private String scope(String kind, String... ids) {
		return catalog.name() + "|" + UsageLimitKey.digest(List.of(kind, List.of(ids)));
	}

	private record Team(UsageLimitKey key, boolean unlimited, Timestamp accessSince) {
	}

	private List<Team> teams(EngineUsageIdentity.Person person, String engine, Map<UsageLimitKey, UsageLimit> limits) {
		Map<String, Timestamp> grants = new HashMap<>();
		try {
			var now = Utility.getCurrentSqlTimestampUTC();
			QueryExecutionUtility.queryList(SystemEngineRegistry.getSecurityDb(),
					"SELECT ID,TYPE,DATEADDED,ENDDATE FROM GROUPENGINEPERMISSION WHERE ENGINEID = ? AND PERMISSION IS NOT NULL",
					ps -> ps.setString(1, engine), rs -> {
						var end = rs.getTimestamp(4);
						if (end == null || !end.before(now))
							grants.put(UsageLimitKey.digest(List.of(rs.getString(1), rs.getString(2))),
									rs.getTimestamp(3));
						return null;
					});
		} catch (Exception e) {
			throw new UsageStoreException("Unable to resolve engine team policies", e);
		}
		List<Team> teams = new ArrayList<>();
		for (var membership : person.teams()) {
			var key = key(Source.TEAM, engine, membership.teamId(), membership.teamType());
			String id = UsageLimitKey.digest(List.of(membership.teamId(), membership.teamType()));
			if (!limits.containsKey(key) || !grants.containsKey(id))
				continue;
			Timestamp since = grants.get(id);
			if (since == null || (membership.joined() != null && membership.joined().after(since)))
				since = membership.joined();
			teams.add(new Team(key, limits.get(key).unlimited(), since));
		}
		teams.sort(Comparator.comparing((Team t) -> !t.unlimited)
				.thenComparing(Team::accessSince, Comparator.nullsLast(Comparator.naturalOrder()))
				.thenComparing(t -> t.key.subjectId()).thenComparing(t -> t.key.subjectType()));
		return teams;
	}

	/** Null means an internal/unregistered resource or a different catalog. */
	private Boolean registeredPublic(String engine) {
		try {
			return QueryExecutionUtility.queryOne(SystemEngineRegistry.getSecurityDb(),
					"SELECT GLOBAL FROM ENGINE WHERE ENGINEID = ? AND ENGINETYPE = ?", ps -> {
						ps.setString(1, engine);
						ps.setString(2, catalog.name());
					}, rs -> rs.getBoolean(1));
		} catch (Exception e) {
			throw new UsageStoreException("Unable to resolve engine public policy", e);
		}
	}
}
