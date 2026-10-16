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
package prerna.usertracking;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IRDBMSEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;

/**
 * What the catalog holds now for the engines and apps audit events were on: an
 * engine's or app's current name, and an engine's kind and provider, which pick
 * its logo. The readers of the audit trail add these to each event they return,
 * so an event shows its target as it is now, even one recorded before the
 * target was renamed or with only its id. A target the catalog no longer has,
 * such as a deleted engine, gets none, and a lookup that fails leaves the
 * events as they were recorded.
 */
public final class AuditTargetCatalog {

	private static final Logger classLogger = LogManager.getLogger(AuditTargetCatalog.class);

	/** The fields a read adds to each event, after the audit trail's own */
	public static final List<String> COLUMNS = List.of("TARGET_DISPLAY_NAME", "TARGET_ENGINE_TYPE",
			"TARGET_ENGINE_SUBTYPE");

	/**
	 * A target as the catalog has it now.
	 *
	 * @param displayName   its display name, else its name
	 * @param engineType    an engine's catalog type, such as MODEL; null for an app
	 * @param engineSubtype an engine's provider, such as OPEN_AI, or null
	 */
	public record Entry(String displayName, String engineType, String engineSubtype) {

		/**
		 * The entry's values, for an event.
		 *
		 * @return the display name, engine type and engine subtype, in the order of
		 *         {@link AuditTargetCatalog#COLUMNS}
		 */
		public List<Object> values() {
			return Arrays.asList(displayName, engineType, engineSubtype);
		}
	}

	/** The entry of a target the catalog does not have */
	private static final Entry NONE = new Entry(null, null, null);

	private final Map<String, Entry> engines;
	private final Map<String, Entry> projects;

	private AuditTargetCatalog(Map<String, Entry> engines, Map<String, Entry> projects) {
		this.engines = engines;
		this.projects = projects;
	}

	/**
	 * Looks up the targets of a page of events, with one read of the engines and
	 * one of the apps.
	 *
	 * @param targets each event's stored target type and target id
	 * @return the catalog's entries for them
	 */
	public static AuditTargetCatalog lookup(Collection<String[]> targets) {
		Set<String> engineIds = new LinkedHashSet<>();
		Set<String> projectIds = new LinkedHashSet<>();
		for (String[] target : targets) {
			AuditTargetType type = AuditTargetType.find(target[0]);
			if (type == null || target[1] == null || target[1].isBlank()) {
				continue;
			}
			if (type.isEngine()) {
				engineIds.add(target[1]);
			} else if (type == AuditTargetType.PROJECT) {
				projectIds.add(target[1]);
			}
		}
		IRDBMSEngine securityDb = engineIds.isEmpty() && projectIds.isEmpty() ? null
				: SystemEngineRegistry.getSecurityDb();
		if (securityDb == null) {
			return new AuditTargetCatalog(Map.of(), Map.of());
		}
		return new AuditTargetCatalog(readEngines(securityDb, engineIds), readProjects(securityDb, projectIds));
	}

	/**
	 * Adds the catalog's entry for its target to each event of a page.
	 *
	 * @param events the events, each with its TARGET_TYPE and TARGET_ID
	 */
	public static void addTo(List<Map<String, Object>> events) {
		AuditTargetCatalog catalog = lookup(events.stream()
				.map(event -> new String[] { text(event.get("TARGET_TYPE")), text(event.get("TARGET_ID")) }).toList());
		for (Map<String, Object> event : events) {
			List<Object> values = catalog.get(text(event.get("TARGET_TYPE")), text(event.get("TARGET_ID"))).values();
			for (int i = 0; i < COLUMNS.size(); i++) {
				event.put(COLUMNS.get(i), values.get(i));
			}
		}
	}

	/**
	 * The catalog's entry for an event's target.
	 *
	 * @param targetType the stored target type
	 * @param targetId   the stored target id
	 * @return the target as the catalog has it now, or an entry with no values
	 */
	public Entry get(String targetType, String targetId) {
		AuditTargetType type = AuditTargetType.find(targetType);
		if (type == null || targetId == null) {
			return NONE;
		}
		Map<String, Entry> entries = type.isEngine() ? engines : type == AuditTargetType.PROJECT ? projects : Map.of();
		return entries.getOrDefault(targetId, NONE);
	}

	private static Map<String, Entry> readEngines(IRDBMSEngine securityDb, Set<String> ids) {
		if (ids.isEmpty()) {
			return Map.of();
		}
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEDISPLAYNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINETYPE"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINESUBTYPE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", List.copyOf(ids)));
		return read(securityDb, qs, "engines",
				rs -> Map.entry(rs.getString("ENGINEID"),
						new Entry(displayName(rs.getString("ENGINEDISPLAYNAME"), rs.getString("ENGINENAME")),
								rs.getString("ENGINETYPE"), rs.getString("ENGINESUBTYPE"))));
	}

	private static Map<String, Entry> readProjects(IRDBMSEngine securityDb, Set<String> ids) {
		if (ids.isEmpty()) {
			return Map.of();
		}
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTDISPLAYNAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", List.copyOf(ids)));
		return read(securityDb, qs, "apps", rs -> Map.entry(rs.getString("PROJECTID"),
				new Entry(displayName(rs.getString("PROJECTDISPLAYNAME"), rs.getString("PROJECTNAME")), null, null)));
	}

	/** Reads entries by id, leaving them out when the catalog cannot be read */
	private static Map<String, Entry> read(IRDBMSEngine securityDb, SelectQueryStruct qs, String what,
			QueryExecutionUtility.RowMapper<Map.Entry<String, Entry>> mapper) {
		Map<String, Entry> entries = new HashMap<>();
		try {
			for (Map.Entry<String, Entry> entry : QueryExecutionUtility.queryList(securityDb, qs, mapper,
					AuditReadQuerySupport.QUERY_TIMEOUT_SECONDS)) {
				entries.put(entry.getKey(), entry.getValue());
			}
		} catch (Exception e) {
			classLogger.warn("Unable to read the catalog's {} for the audit trail; events show their stored targets",
					what, e);
		}
		return entries;
	}

	private static String displayName(String displayName, String name) {
		return displayName != null && !displayName.isBlank() ? displayName : name;
	}

	private static String text(Object value) {
		return value instanceof String text ? text : null;
	}
}
