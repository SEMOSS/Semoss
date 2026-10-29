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
package prerna.collaboration;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.reactor.automation.definition.AutomationDefinitionService;

/** The opted-in daily inbox brief: reply drafts, today's meetings, and open action items, run by {@link WorkRoutine}. */
public final class WorkInboxRoutine {
	public static final String ID = "inbox-daily-brief";
	/** AUTOMATION_ID of the brief's runs in the scheduler database. */
	public static final String AUTOMATION_ID = "collaboration:" + ID;
	/** The brief's sections in writing order; the owner may turn any of them off, but not all. */
	static final List<String> SECTIONS = List.of("priorities", "replies", "meetings", "actions");
	static final WorkRoutine.Kind KIND = new WorkRoutine.Kind(ID, "WORK_INBOX_ROUTINE", "WorkRunInboxRoutine();",
			"Daily inbox brief", "inbox-daily-brief.automation.json", "brief", "inbox brief", "brief",
			Map.of("time", "08:00", "days", WorkRoutine.WEEKDAYS.subList(0, 5), "timezone", "UTC", "lookbackHours", 24,
					"preferences", "", "sections", SECTIONS, "priorityCount", 5, "writeDrafts", true),
			WorkInboxRoutine::settings, WorkInboxRoutine::inputs, null);

	private WorkInboxRoutine() { }

	public static Map<String, Object> get(Insight insight) {
		return WorkRoutine.get(KIND, insight);
	}

	public static Map<String, Object> get(User user) {
		return WorkRoutine.get(KIND, user);
	}

	public static Map<String, Object> save(User user, Map<String, Object> changes) {
		return WorkRoutine.save(KIND, user, changes);
	}

	public static Map<String, Object> run(Insight insight) {
		return WorkRoutine.run(KIND, insight);
	}

	static Map<String, Object> defaults() {
		return new LinkedHashMap<>(KIND.defaults());
	}

	static Map<String, Object> validate(Map<String, Object> input) {
		return WorkRoutine.validate(KIND, input);
	}

	static String cron(Map<String, Object> config) {
		return WorkRoutine.cron(config);
	}

	/** The server-held brief automation, with the owner's model filled in on its agent steps. */
	static AutomationDefinitionService.DefinitionFiles briefDefinition(String engineId) {
		return WorkRoutine.definition(KIND, engineId);
	}

	private static void settings(Map<String, Object> input, Map<String, Object> config) {
		Object hours = input.get("lookbackHours");
		if (!(hours instanceof Number number) || number.doubleValue() != number.intValue() || number.intValue() < 1 || number.intValue() > 720) {
			throw new IllegalArgumentException("lookbackHours must be an integer from 1 to 720");
		}
		config.put("lookbackHours", number.intValue());
		// Settings saved before the brief had knobs have none: the whole brief, five priorities, drafts on.
		Object sections = input.get("sections") == null ? SECTIONS : input.get("sections");
		if (!(sections instanceof List<?> chosen) || chosen.isEmpty() || !SECTIONS.containsAll(chosen)
				|| chosen.stream().distinct().count() != chosen.size()) {
			throw new IllegalArgumentException("sections must be one or more of " + String.join(", ", SECTIONS));
		}
		Object count = input.get("priorityCount") == null ? 5 : input.get("priorityCount");
		if (!(count instanceof Number priorities) || priorities.doubleValue() != priorities.intValue()
				|| priorities.intValue() < 1 || priorities.intValue() > 10) {
			throw new IllegalArgumentException("priorityCount must be an integer from 1 to 10");
		}
		Object drafts = input.get("writeDrafts") == null ? Boolean.TRUE : input.get("writeDrafts");
		if (!(drafts instanceof Boolean writeDrafts)) {
			throw new IllegalArgumentException("writeDrafts must be true or false");
		}
		config.put("sections", SECTIONS.stream().filter(chosen::contains).toList());
		config.put("priorityCount", priorities.intValue());
		config.put("writeDrafts", writeDrafts);
	}

	private static Map<String, Object> inputs(Map<String, Object> row, Map<String, Object> config) {
		return Map.of("lookback_hours", ((Number) config.get("lookbackHours")).intValue(), "sections", config.get("sections"),
				"priority_count", ((Number) config.get("priorityCount")).intValue(), "write_drafts", config.get("writeDrafts"),
				// approval requests get a review for the owner, never a drafted answer to the requester
				"skip_reply_subjects", WorkExpenseRoutine.SUBJECTS);
	}
}
