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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.reactor.automation.definition.AutomationDefinitionService;

/**
 * The opted-in weekly meeting agendas: proposed topics, timing, and leads for the upcoming sittings of the meetings
 * the owner picked, run by {@link WorkRoutine}.
 */
public final class WorkAgendaRoutine {
	public static final String ID = "weekly-meeting-agendas";
	/** AUTOMATION_ID of the agendas' runs in the scheduler database. */
	public static final String AUTOMATION_ID = "collaboration:" + ID;
	/** How far ahead each run looks for the chosen meetings. */
	static final int DAYS_AHEAD = 21;
	private static final int MAX_MEETINGS = 20;
	private static final int MAX_IGNORED = 200;
	static final WorkRoutine.Kind KIND = new WorkRoutine.Kind(ID, "WORK_AGENDA_ROUTINE", "WorkRunAgendaRoutine();",
			"Weekly meeting agendas", "weekly-meeting-agendas.automation.json", "agendas", "weekly agenda", "agenda",
			Map.of("time", "08:00", "days", List.of("MON"), "timezone", "UTC", "meetings", List.of(), "ignored", List.of()),
			WorkAgendaRoutine::meetings, WorkAgendaRoutine::inputs);

	private WorkAgendaRoutine() { }

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

	static Map<String, Object> validate(Map<String, Object> input) {
		return WorkRoutine.validate(KIND, input);
	}

	/** The server-held agenda automation, with the owner's model filled in on its agent step. */
	static AutomationDefinitionService.DefinitionFiles definition(String engineId) {
		return WorkRoutine.definition(KIND, engineId);
	}

	// Meetings are kept by title: a series keeps its title from week to week, while event ids change per sitting.
	// ignored only hides titles from the owner's picker; runs read meetings alone.
	private static void meetings(Map<String, Object> input, Map<String, Object> config) {
		if (((List<?>) config.get("days")).size() != 1) {
			throw new IllegalArgumentException("Choose one day of the week for the agendas");
		}
		List<String> meetings = titles(input.get("meetings"), MAX_MEETINGS, "meetings");
		if (Boolean.TRUE.equals(config.get("enabled")) && meetings.isEmpty()) {
			throw new IllegalArgumentException("Choose at least one meeting to prepare agendas for");
		}
		// Settings saved before the ignore list existed have none.
		List<String> ignored = titles(input.get("ignored") == null ? List.of() : input.get("ignored"), MAX_IGNORED, "ignored");
		if (ignored.stream().anyMatch(title -> meetings.stream().anyMatch(title::equalsIgnoreCase))) {
			throw new IllegalArgumentException("A meeting cannot be both chosen and ignored");
		}
		config.put("meetings", meetings);
		config.put("ignored", ignored);
	}

	private static List<String> titles(Object value, int max, String name) {
		if (!(value instanceof List<?> list) || list.size() > max) {
			throw new IllegalArgumentException(name + " must be a list of up to " + max + " meeting titles");
		}
		List<String> titles = new ArrayList<>();
		for (Object meeting : list) {
			String title = meeting instanceof String text ? text.strip() : "";
			if (title.isEmpty() || title.length() > 255 || titles.stream().anyMatch(title::equalsIgnoreCase)) {
				throw new IllegalArgumentException("Each meeting must be a distinct title of 1 to 255 characters");
			}
			titles.add(title);
		}
		return titles;
	}

	private static Map<String, Object> inputs(Map<String, Object> row, Map<String, Object> config) {
		Map<String, Object> inputs = new LinkedHashMap<>();
		inputs.put("meetings", config.get("meetings"));
		inputs.put("days_ahead", DAYS_AHEAD);
		return inputs;
	}
}
