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
import java.util.Set;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * The opted-in event planning: for the meetings the owner pinned, the lineup of each coming sitting (topics,
 * presenters, minutes, confirmation, materials) kept in one spreadsheet per meeting in their OneDrive, with the asks
 * to presenters saved as drafts. Run by {@link WorkRoutine}; one structured model call plans, and the automation
 * reads and writes the sheets and saves the drafts.
 */
public final class WorkEventPlanningRoutine {
	public static final String ID = "event-planning";
	/** AUTOMATION_ID of the runs in the scheduler database. */
	public static final String AUTOMATION_ID = "collaboration:" + ID;
	/** Where the sheets live in the owner's OneDrive. */
	static final String FOLDER = "Collaboration/Event Planning";
	static final int DAYS_AHEAD = 21;
	private static final int MAX_MEETINGS = 10;
	private static final int MAX_IGNORED = 200;
	private static final int MAX_TITLE = 120;
	private static final int MAX_ID = 1024;
	private static final Set<String> PIN_KINDS = Set.of("series", "event");
	static final WorkRoutine.Kind KIND = new WorkRoutine.Kind(ID, "WORK_EVENT_PLANNING_ROUTINE",
			"WorkRunEventPlanningRoutine();", "Event planning", "event-planning.automation.json", "deliver",
			"event planning", "plan",
			Map.of("time", "08:00", "days", List.of("MON"), "timezone", "UTC", "meetings", List.of(), "ignored",
					List.of(), "preferences", ""),
			WorkEventPlanningRoutine::settings, WorkEventPlanningRoutine::inputs, null);

	private WorkEventPlanningRoutine() { }

	public static Map<String, Object> get(Insight insight) {
		Map<String, Object> result = new LinkedHashMap<>(WorkRoutine.get(KIND, insight));
		result.put("folder", FOLDER);
		return result;
	}

	public static Map<String, Object> save(User user, Map<String, Object> changes) {
		return WorkRoutine.save(KIND, user, changes);
	}

	// the cron only fires on the chosen day, so the day is not checked again, which lets Run now use the same job
	public static Map<String, Object> run(Insight insight) {
		return WorkRoutine.run(KIND, insight, null, true);
	}

	public static void runNow(User user) {
		WorkRoutine.runNow(KIND, user, null);
	}

	private static void settings(Map<String, Object> input, Map<String, Object> config) {
		if (((List<?>) config.get("days")).size() != 1) {
			throw new IllegalArgumentException("Choose one day of the week for event planning");
		}
		List<Map<String, Object>> meetings = pins(input.get("meetings"), MAX_MEETINGS, "meetings");
		if (Boolean.TRUE.equals(config.get("enabled")) && meetings.isEmpty()) {
			throw new IllegalArgumentException("Pin at least one meeting to plan");
		}
		// each pinned meeting's title names its sheet, so two cannot share one
		List<String> titles = new ArrayList<>();
		for (Map<String, Object> pin : meetings) {
			String title = ((String) pin.get("title")).toLowerCase();
			if (titles.contains(title)) {
				throw new IllegalArgumentException("Two pinned meetings cannot have the same title");
			}
			titles.add(title);
		}
		List<Map<String, Object>> ignored = pins(input.get("ignored") == null ? List.of() : input.get("ignored"),
				MAX_IGNORED, "ignored");
		if (ignored.stream().anyMatch(pin -> meetings.stream().anyMatch(meeting -> sameMeeting(pin, meeting)))) {
			throw new IllegalArgumentException("A meeting cannot be both pinned and ignored");
		}
		config.put("meetings", meetings);
		config.put("ignored", ignored);
	}

	// A pin names one meeting: a series by its series id, a one-off by its event id; ids survive a rename.
	// The title is how the owner saw it when pinning, kept for display and as the sheet's name.
	private static List<Map<String, Object>> pins(Object value, int max, String name) {
		if (!(value instanceof List<?> list) || list.size() > max) {
			throw new IllegalArgumentException(name + " must be a list of up to " + max + " meetings");
		}
		List<Map<String, Object>> pins = new ArrayList<>();
		for (Object item : list) {
			if (!(item instanceof Map<?, ?> map)) {
				throw new IllegalArgumentException("Each meeting must have a kind, an id and a title");
			}
			String kind = text(map.get("kind"));
			String id = text(map.get("id"));
			String title = text(map.get("title")).replaceAll("\\s+", " ");
			if (!PIN_KINDS.contains(kind) || id.isEmpty() || id.length() > MAX_ID || title.isEmpty()
					|| title.length() > MAX_TITLE) {
				throw new IllegalArgumentException("Each meeting must be a series or event with an id and a title of 1 to "
						+ MAX_TITLE + " characters");
			}
			Map<String, Object> pin = new LinkedHashMap<>();
			pin.put("kind", kind);
			pin.put("id", id);
			pin.put("title", title);
			if (pins.stream().anyMatch(other -> sameMeeting(other, pin))) {
				throw new IllegalArgumentException("A meeting is listed twice");
			}
			pins.add(pin);
		}
		return pins;
	}

	private static boolean sameMeeting(Map<String, Object> a, Map<String, Object> b) {
		return a.get("kind").equals(b.get("kind")) && a.get("id").equals(b.get("id"));
	}

	private static Map<String, Object> inputs(Map<String, Object> row, Map<String, Object> config) {
		Map<String, Object> inputs = new LinkedHashMap<>();
		inputs.put("meetings", config.get("meetings"));
		inputs.put("days_ahead", DAYS_AHEAD);
		inputs.put("folder", FOLDER);
		// the same model the run already checked the owner can use before starting
		inputs.put("plan_model", text(Utility.getDIHelperProperty(Constants.COLLAB_LLM_ENGINE_ID)));
		return inputs;
	}

	private static String text(Object value) {
		return value == null ? "" : String.valueOf(value).trim();
	}
}
