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

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.javatuples.Pair;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.auth.access.ModelUsageLimit;
import prerna.auth.access.ModelUsageLimitKey;
import prerna.auth.access.ModelUsageLimitPath;
import prerna.auth.access.ModelUsageLimitPath.State;
import prerna.auth.access.ModelUsageLimitPath.Step;
import prerna.auth.access.ModelUsageLimitPath.StepLimitType;
import prerna.auth.access.ModelUsageLimitRule;
import prerna.auth.access.PermissionSubject;
import prerna.auth.access.PermissionTargetResource;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.util.ValueUtils;

/**
 * Who can use a project or an engine in the end, one row per person: the most
 * access they get from being given access directly and from every custom group
 * they are in, with each of those sources. Access and memberships whose end
 * date has passed are left out. Members of groups that mirror a login
 * provider's groups are not kept in the security database, so those groups are
 * listed apart.
 *
 * <p>
 * On a model, each person's row also says which usage limit their calls are
 * billed to, worked out by {@link ModelUsageLimitResolver#resolveStored} for
 * everyone at once, and the response adds the model's public use limit and its
 * total. How much of a limit is used is not read here.
 * </p>
 */
public class EffectivePermissionUtils extends AbstractSecurityUtils {

	private static final String CUSTOM = SecurityGroupManagerUtils.CUSTOM_GROUP_TYPE;

	/** Where a person's access comes from */
	public enum AccessSource {
		DIRECT, TEAM
	}

	/** Why a limit that is defined does not apply to a person */
	public enum NotAppliedReason {
		/** The person's own grant has a limit, which comes first */
		DIRECT_WINS,
		/** A team's limit comes first */
		TEAM_WINS,
		/** An Unlimited team comes first */
		UNLIMITED_TEAM_WINS,
		/** The public use limit comes first */
		PUBLIC_WINS,
		/** The person's all-models default comes first */
		ALL_MODELS_WINS,
		/** The public use limit applies only while the model is public */
		NOT_PUBLIC
	}

	private EffectivePermissionUtils() {

	}

	/**
	 * Everyone who can use a project or an engine, with the access they end up with
	 * and where it comes from.
	 *
	 * @param user   an editor or owner of the target, since the rows show who is in
	 *               its groups; being an admin counts for nothing outside admin
	 *               mode
	 * @param target the project or engine
	 * @return {@code people}: one row per person with {@code subject} (category,
	 *         id, type, name, email), {@code level} (the most access any source
	 *         gives) and {@code sources}, each with {@code sourceType} (DIRECT or
	 *         TEAM), the group's {@code id} and {@code type}, {@code level} and
	 *         {@code endDate}, the most access first; {@code unlistedTeams}: the
	 *         groups whose members come from a login provider, with {@code id},
	 *         {@code type} and {@code level}. On a model, each person's row adds
	 *         {@code usage}: {@code billedTo} (the source their calls are billed
	 *         to, with {@code limitType}, {@code id} and {@code type}, or null for
	 *         no limit), {@code unlimited} and {@code rules} (its limit),
	 *         {@code nextTeams} (for a team, the teams calls move on to, in billing
	 *         order, each with {@code id}, {@code type}, {@code unlimited} and
	 *         {@code rules}) and {@code notApplied} (the limits defined that do not
	 *         apply, each with {@code limitType}, {@code id}, {@code type} and a
	 *         {@link NotAppliedReason}); each {@code unlistedTeams} row adds its
	 *         {@code usageLimit}; and the response adds {@code publicUse}
	 *         ({@code isPublic} and {@code usageLimit}) and {@code ceilings} (the
	 *         model's total: {@code limitType} {@code MODEL_TOTAL} and
	 *         {@code usageLimit})
	 * @throws IllegalAccessException when the user is not an editor or owner of the
	 *                                target
	 */
	public static Map<String, Object> getEffectivePermissions(User user, PermissionTargetResource target)
			throws IllegalAccessException {
		return readEffectiveAccess(user, target, false);
	}

	/**
	 * {@link #getEffectivePermissions(User, PermissionTargetResource)} in admin
	 * mode, for any project or engine
	 *
	 * @param adminUtils the admin utilities, which prove the user is an admin
	 */
	public static Map<String, Object> getEffectivePermissions(SecurityAdminUtils adminUtils, User user,
			PermissionTargetResource target) throws IllegalAccessException {
		if (adminUtils == null) {
			throw new IllegalArgumentException("Admin mode needs the admin utilities");
		}
		return readEffectiveAccess(user, target, true);
	}

	private static Map<String, Object> readEffectiveAccess(User user, PermissionTargetResource target, boolean asAdmin)
			throws IllegalAccessException {
		if (target == null || target.getResourceType() == null || ValueUtils.isBlank(target.getId())) {
			throw new IllegalArgumentException("Must define the project or engine");
		}
		boolean isProject = target.getResourceType() == PermissionTargetResource.TargetResourceType.PROJECT;
		if (!isProject && target.getResourceType() != PermissionTargetResource.TargetResourceType.ENGINE) {
			throw new IllegalArgumentException("Only a project's or an engine's effective access can be read");
		}
		String id = target.getId().trim();
		String noun = isProject ? "project" : "engine";
		boolean canSee = asAdmin || (isProject ? SecurityProjectUtils.userCanEditProject(user, id)
				: SecurityEngineUtils.userCanEditEngine(user, id));
		if (!canSee) {
			throw new IllegalAccessException(
					"Only this " + noun + "'s editors and owners can see who can use it in the end");
		}

		Map<String, Person> people = new LinkedHashMap<>();
		PermissionTargetResource resource = new PermissionTargetResource(target.getResourceType(), id, null);
		List<Map<String, Object>> direct = PermissionBatchUtils.readPeople(user, resource, null, asAdmin);
		for (Map<String, Object> row : direct) {
			if (hasEnded(row.get("end_date"))) {
				continue;
			}
			person(people, row.get("id"), row).sources.add(source(AccessSource.DIRECT, null, null,
					String.valueOf(row.get("permission")), row.get("end_date")));
		}

		List<Map<String, Object>> groups = PermissionBatchUtils.readGroups(user, resource, asAdmin);
		List<Map<String, Object>> unlistedTeams = new ArrayList<>();
		AdminSecurityGroupUtils groupUtils = AdminSecurityGroupUtils.getUncheckedInstance();
		for (Map<String, Object> group : groups) {
			String groupId = String.valueOf(group.get("ID"));
			String groupType = String.valueOf(group.get("TYPE"));
			String level = AccessPermissionEnum.getPermissionValueById(String.valueOf(group.get("PERMISSION")));
			if (hasEnded(group.get("ENDDATE"))) {
				continue;
			}
			if (!CUSTOM.equals(groupType) || !groupUtils.isCustomGroup(groupId)) {
				Map<String, Object> team = new LinkedHashMap<>();
				team.put("id", groupId);
				team.put("type", groupType);
				team.put("level", level);
				unlistedTeams.add(team);
				continue;
			}
			for (Map<String, Object> member : readCurrentMembers(groupId)) {
				person(people, member.get("userid"), member).sources
						.add(source(AccessSource.TEAM, groupId, groupType, level, group.get("ENDDATE")));
			}
		}

		Map<String, Object> modelLimits = new LinkedHashMap<>();
		if (!isProject && PermissionBatchUtils.isModelEngine(id)) {
			addUsageLimits(id, people, unlistedTeams, modelLimits);
		}

		List<Map<String, Object>> rows = new ArrayList<>();
		people.values().stream()
				.sorted(Comparator.comparing((Person person) -> person.sortName()).thenComparing(Person::key))
				.forEach(person -> rows.add(person.toRow()));
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("people", rows);
		result.put("unlistedTeams", unlistedTeams);
		result.putAll(modelLimits);
		return result;
	}

	/**
	 * Adds a model's usage limits to its effective access: each person's
	 * {@code usage}, from one {@link ModelUsageLimitResolver#resolveStored} for
	 * everyone; each login provider team's {@code usageLimit}; the public use
	 * limit; and the model's total as its ceiling.
	 *
	 * @param engineId      the model
	 * @param people        everyone listed, by id and login type
	 * @param unlistedTeams the login provider teams with access
	 * @param modelLimits   where {@code publicUse} and {@code ceilings} go
	 */
	private static void addUsageLimits(String engineId, Map<String, Person> people,
			List<Map<String, Object>> unlistedTeams, Map<String, Object> modelLimits) {
		List<Pair<String, String>> logins = new ArrayList<>();
		for (Person person : people.values()) {
			logins.add(person.login());
		}
		Map<Pair<String, String>, ModelUsageLimitPath> paths = ModelUsageLimitResolver.resolveStored(engineId, logins);
		for (Person person : people.values()) {
			ModelUsageLimitPath path = paths.get(person.login());
			if (path != null) {
				person.usage = usageOf(path);
			}
		}

		Map<ModelUsageLimitKey, ModelUsageLimit> limits = SecurityModelUsageLimitUtils.getEngineLimits(engineId);
		for (Map<String, Object> team : unlistedTeams) {
			team.put("usageLimit", limits.get(ModelUsageLimitKey.team(engineId, String.valueOf(team.get("id")),
					String.valueOf(team.get("type")))));
		}
		Map<String, Object> publicUse = new LinkedHashMap<>();
		publicUse.put("isPublic", SecurityEngineUtils.engineIsGlobal(engineId));
		publicUse.put("usageLimit", limits.get(ModelUsageLimitKey.publicUse(engineId)));
		modelLimits.put("publicUse", publicUse);
		Map<String, Object> modelTotal = new LinkedHashMap<>();
		modelTotal.put("limitType", StepLimitType.MODEL_TOTAL.name());
		modelTotal.put("usageLimit", limits.get(ModelUsageLimitKey.modelTotal(engineId)));
		modelLimits.put("ceilings", List.of(modelTotal));
	}

	/**
	 * Builds a person's usage on a model from their limit path: the first source in
	 * the billing order is the one billed, the rest are the teams calls move on to,
	 * and each other limit defined is listed as not applied when the billed source
	 * comes first or the model is not public.
	 *
	 * @param path how limits apply to one person on the model
	 * @return the person's {@code usage}: {@code billedTo}, {@code unlimited},
	 *         {@code rules}, {@code nextTeams} and {@code notApplied}
	 */
	private static Map<String, Object> usageOf(ModelUsageLimitPath path) {
		List<Step> order = path.getBillingOrder();
		Step billed = order.isEmpty() ? null : order.get(0);
		Map<String, Object> usage = new LinkedHashMap<>();
		usage.put("billedTo", billed == null ? null : sourceOf(billed));
		usage.put("unlimited", billed != null && billed.isUnlimited());
		usage.put("rules", billed == null ? List.of() : ruleMaps(billed.getLimit()));

		List<Map<String, Object>> nextTeams = new ArrayList<>();
		for (Step team : order.subList(Math.min(1, order.size()), order.size())) {
			Map<String, Object> next = new LinkedHashMap<>();
			next.put("id", team.getKey().subjectId());
			next.put("type", team.getKey().subjectType());
			next.put("unlimited", team.isUnlimited());
			next.put("rules", ruleMaps(team.getLimit()));
			nextTeams.add(next);
		}
		usage.put("nextTeams", nextTeams);

		List<Map<String, Object>> notApplied = new ArrayList<>();
		for (Step step : path.getSteps()) {
			if (!step.isDefined() || order.contains(step)) {
				continue;
			}
			NotAppliedReason reason = step.getState() == State.SKIPPED ? NotAppliedReason.NOT_PUBLIC : winsOver(billed);
			if (reason == null) {
				continue;
			}
			Map<String, Object> source = sourceOf(step);
			source.put("reason", reason.name());
			notApplied.add(source);
		}
		usage.put("notApplied", notApplied);
		return usage;
	}

	/**
	 * Picks the reason, from the type of limit billed, that a limit defined outside
	 * the billing order does not apply.
	 *
	 * @param billed the source billed, or null
	 * @return why the sources after it do not apply, or null when nothing is billed
	 */
	private static NotAppliedReason winsOver(Step billed) {
		if (billed == null) {
			return null;
		}
		switch (billed.getLimitType()) {
		case DIRECT:
			return NotAppliedReason.DIRECT_WINS;
		case TEAM:
			return billed.isUnlimited() ? NotAppliedReason.UNLIMITED_TEAM_WINS : NotAppliedReason.TEAM_WINS;
		case PUBLIC:
			return NotAppliedReason.PUBLIC_WINS;
		case ALL_MODELS:
			return NotAppliedReason.ALL_MODELS_WINS;
		default:
			return null;
		}
	}

	/**
	 * Names the source a step of a limit path reads its limit from, for the
	 * response.
	 *
	 * @return the source of a step: {@code limitType}, {@code id} and {@code type}
	 */
	private static Map<String, Object> sourceOf(Step step) {
		Map<String, Object> source = new LinkedHashMap<>();
		source.put("limitType", step.getLimitType().name());
		source.put("id", step.getKey().subjectId());
		source.put("type", step.getKey().subjectType());
		return source;
	}

	/**
	 * Turns a usage limit's rules into maps for the response, with an empty list
	 * for no limit.
	 *
	 * @param limit a limit, or null
	 * @return its rules, each with {@code metric}, {@code amount}, {@code window},
	 *         {@code windowLength} (left out for 1) and {@code counts}
	 */
	private static List<Map<String, Object>> ruleMaps(ModelUsageLimit limit) {
		List<Map<String, Object>> rules = new ArrayList<>();
		if (limit == null) {
			return rules;
		}
		for (ModelUsageLimitRule rule : limit.getRules()) {
			Map<String, Object> map = new LinkedHashMap<>();
			map.put("metric", rule.getMetric() == null ? null : rule.getMetric().name());
			map.put("amount", rule.getAmount());
			map.put("window", rule.getWindow() == null ? null : rule.getWindow().name());
			map.put("windowLength", rule.getWindowLength() > 1 ? rule.getWindowLength() : null);
			map.put("counts", rule.getCounts() == null ? null : rule.getCounts().name());
			rules.add(map);
		}
		return rules;
	}

	/**
	 * Reads a custom group's members from {@code CUSTOMGROUPASSIGNMENT}, joined to
	 * {@code SMSS_USER} for their names and emails, leaving out memberships whose
	 * end date has passed.
	 *
	 * @return the custom group's members whose membership has not ended, with the
	 *         keys {@code userid}, {@code type}, {@code name} and {@code email}
	 */
	private static List<Map<String, Object>> readCurrentMembers(String groupId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("CUSTOMGROUPASSIGNMENT__USERID"));
		qs.addSelector(new QueryColumnSelector("CUSTOMGROUPASSIGNMENT__TYPE"));
		qs.addSelector(new QueryColumnSelector("CUSTOMGROUPASSIGNMENT__ENDDATE"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("CUSTOMGROUPASSIGNMENT__GROUPID", "==", groupId));
		qs.addRelation("CUSTOMGROUPASSIGNMENT__USERID", "SMSS_USER__ID", "inner.join");
		qs.addRelation("CUSTOMGROUPASSIGNMENT__TYPE", "SMSS_USER__TYPE", "inner.join");
		List<Map<String, Object>> members = new ArrayList<>();
		for (Map<String, Object> member : getSimpleQuery(qs)) {
			if (!hasEnded(member.get("enddate"))) {
				members.add(member);
			}
		}
		return members;
	}

	/**
	 * Whether an end date as stored is before the current time in UTC; a null end
	 * date never passes.
	 *
	 * @param endDate an end date as stored, in UTC, or null
	 * @return whether it has passed
	 */
	private static boolean hasEnded(Object endDate) {
		String iso = PermissionBatchUtils.toIsoUtc(endDate);
		return iso != null && OffsetDateTime.parse(iso).isBefore(OffsetDateTime.now(ZoneOffset.UTC));
	}

	/** A person and the sources of their access */
	private static final class Person {
		final Map<String, Object> subject = new LinkedHashMap<>();
		final List<Map<String, Object>> sources = new ArrayList<>();
		/** On a model, which usage limit the person's calls are billed to */
		Map<String, Object> usage;

		String key() {
			return subject.get("id") + ":" + subject.get("type");
		}

		/**
		 * The key the usage limit resolver reads and returns the person's limit path
		 * by.
		 *
		 * @return the person's user id and login type
		 */
		Pair<String, String> login() {
			Object type = subject.get("type");
			return Pair.with(String.valueOf(subject.get("id")), ValueUtils.toStringOrNull(type));
		}

		String sortName() {
			Object name = subject.get("name");
			Object label = ValueUtils.toNonBlankString(name) == null ? subject.get("id") : name;
			return String.valueOf(label).toLowerCase(Locale.ROOT);
		}

		Map<String, Object> toRow() {
			sources.sort(Comparator.comparingInt(
					source -> AccessPermissionEnum.getIdByPermission(String.valueOf(source.get("level")))));
			Map<String, Object> row = new LinkedHashMap<>();
			row.put("subject", subject);
			row.put("level", sources.get(0).get("level"));
			row.put("sources", sources);
			if (usage != null) {
				row.put("usage", usage);
			}
			return row;
		}
	}

	/**
	 * Finds the person in the map by id and login type, adding them with the row's
	 * name and email the first time.
	 *
	 * @return the person for an id and login type, added the first time either is
	 *         seen
	 */
	private static Person person(Map<String, Person> people, Object id, Map<String, Object> row) {
		String key = id + ":" + row.get("type");
		return people.computeIfAbsent(key, k -> {
			Person person = new Person();
			person.subject.put("category", PermissionSubject.SubjectCategory.USER.name());
			person.subject.put("id", id);
			person.subject.put("type", row.get("type"));
			person.subject.put("name", row.get("name"));
			person.subject.put("email", row.get("email"));
			return person;
		});
	}

	private static Map<String, Object> source(AccessSource accessSource, String groupId, String groupType, String level,
			Object endDate) {
		Map<String, Object> source = new LinkedHashMap<>();
		source.put("sourceType", accessSource.name());
		if (groupId != null) {
			source.put("id", groupId);
			source.put("type", groupType);
		}
		source.put("level", level);
		source.put("endDate", PermissionBatchUtils.toIsoUtc(endDate));
		return source;
	}
}
