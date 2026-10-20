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
import java.util.Map;
import java.util.Objects;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.reactor.automation.definition.AutomationDefinitionService;

/** The opted-in daily inbox brief: reply drafts, today's meetings, and open action items, run by {@link WorkRoutine}. */
public final class WorkInboxRoutine {
	public static final String ID = "inbox-daily-brief";
	/** AUTOMATION_ID of the brief's runs in the scheduler database. */
	public static final String AUTOMATION_ID = "collaboration:" + ID;
	static final WorkRoutine.Kind KIND = new WorkRoutine.Kind(ID, "WORK_INBOX_ROUTINE", "WorkRunInboxRoutine();",
			"Daily inbox brief", "inbox-daily-brief.automation.json", "brief", "inbox brief", "brief",
			Map.of("time", "08:00", "days", WorkRoutine.WEEKDAYS.subList(0, 5), "timezone", "UTC", "emailCount", 25),
			WorkInboxRoutine::emailCount, WorkInboxRoutine::inputs, null);

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

	private static void emailCount(Map<String, Object> input, Map<String, Object> config) {
		Object count = input.get("emailCount");
		if (!(count instanceof Number number) || number.doubleValue() != number.intValue() || number.intValue() < 1 || number.intValue() > 100) {
			throw new IllegalArgumentException("emailCount must be an integer from 1 to 100");
		}
		config.put("emailCount", number.intValue());
	}

	private static Map<String, Object> inputs(Map<String, Object> row, Map<String, Object> config) {
		Map<String, Object> inputs = new LinkedHashMap<>();
		// Replies are drafted only for mail that came after the last brief, so a day's drafts are not made twice.
		inputs.put("since", Objects.toString(row.get("lastSuccessAt"), ""));
		inputs.put("email_count", ((Number) config.get("emailCount")).intValue());
		return inputs;
	}
}
