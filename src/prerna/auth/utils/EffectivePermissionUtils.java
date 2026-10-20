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

import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
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
 */
public class EffectivePermissionUtils extends AbstractSecurityUtils {

	private static final String CUSTOM = SecurityGroupManagerUtils.CUSTOM_GROUP_TYPE;

	/** Where a person's access comes from */
	public enum SourceKind {
		DIRECT, TEAM
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
	 * @return {@code people}: one row per person with {@code subject} (kind, id,
	 *         type, name, email), {@code level} (the most access any source gives)
	 *         and {@code sources}, each with {@code kind} (DIRECT or TEAM), the
	 *         group's {@code id} and {@code type}, {@code level} and
	 *         {@code endDate}, the most access first; {@code unlistedTeams}: the
	 *         groups whose members come from a login provider, with {@code id},
	 *         {@code type} and {@code level}
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
		if (target == null || target.getKind() == null || ValueUtils.isBlank(target.getId())) {
			throw new IllegalArgumentException("Must define the project or engine");
		}
		boolean isProject = target.getKind() == PermissionTargetResource.Kind.PROJECT;
		if (!isProject && target.getKind() != PermissionTargetResource.Kind.ENGINE) {
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
		PermissionTargetResource resource = new PermissionTargetResource(target.getKind(), id, null);
		List<Map<String, Object>> direct = PermissionBatchUtils.readPeople(user, resource, null, asAdmin);
		for (Map<String, Object> row : direct) {
			if (hasEnded(row.get("end_date"))) {
				continue;
			}
			person(people, row.get("id"), row).sources.add(
					source(SourceKind.DIRECT, null, null, String.valueOf(row.get("permission")), row.get("end_date")));
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
						.add(source(SourceKind.TEAM, groupId, groupType, level, group.get("ENDDATE")));
			}
		}

		List<Map<String, Object>> rows = new ArrayList<>();
		people.values().stream()
				.sorted(Comparator.comparing((Person person) -> person.sortName()).thenComparing(Person::key))
				.forEach(person -> rows.add(person.toRow()));
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("people", rows);
		result.put("unlistedTeams", unlistedTeams);
		return result;
	}

	/**
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

		String key() {
			return subject.get("id") + ":" + subject.get("type");
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
			return row;
		}
	}

	/**
	 * @return the person for an id and login type, added the first time either is
	 *         seen
	 */
	private static Person person(Map<String, Person> people, Object id, Map<String, Object> row) {
		String key = id + ":" + row.get("type");
		return people.computeIfAbsent(key, k -> {
			Person person = new Person();
			person.subject.put("kind", PermissionSubject.Kind.USER.name());
			person.subject.put("id", id);
			person.subject.put("type", row.get("type"));
			person.subject.put("name", row.get("name"));
			person.subject.put("email", row.get("email"));
			return person;
		});
	}

	private static Map<String, Object> source(SourceKind kind, String groupId, String groupType, String level,
			Object endDate) {
		Map<String, Object> source = new LinkedHashMap<>();
		source.put("kind", kind.name());
		if (groupId != null) {
			source.put("id", groupId);
			source.put("type", groupType);
		}
		source.put("level", level);
		source.put("endDate", PermissionBatchUtils.toIsoUtc(endDate));
		return source;
	}
}
