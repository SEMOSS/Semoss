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
package prerna.reactor.collaboration;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.collaboration.WorkCustomRoutine;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkSaveCustomRoutine(name=["Client asks"], prompt=["..."], time=["08:00"], days=["MON","FRI"], timezone=["America/New_York"], delivery=["email"]);
public class WorkSaveCustomRoutineReactor extends AbstractCollaborationReactor {

	private static final String ROUTINE_ID = "routineId";
	private static final String NAME = "name";
	private static final String PROMPT = "prompt";
	private static final String TIME = "time";
	private static final String DAYS = "days";
	private static final String TIMEZONE = "timezone";
	private static final String DELIVERY = "delivery";
	private static final String SKILLS = "skills";
	private static final String AGENT_ID = "agentId";
	private static final String ENABLED = "enabled";

	public WorkSaveCustomRoutineReactor() {
		this.keysToGet = new String[] { ROUTINE_ID, NAME, PROMPT, TIME, DAYS, TIMEZONE, DELIVERY, SKILLS, AGENT_ID,
				ENABLED };
		this.keyRequired = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		Map<String, Object> changes = new LinkedHashMap<>();
		for (String key : List.of(NAME, PROMPT, TIME, TIMEZONE, DELIVERY, AGENT_ID)) {
			String value = getString(key);
			if (value != null) {
				changes.put(key, value);
			}
		}
		for (String key : List.of(DAYS, SKILLS)) {
			List<String> values = list(key);
			if (values != null) {
				changes.put(key, DAYS.equals(key) ? values.stream().map(String::toUpperCase).toList() : values);
			}
		}
		if (getBoolean(ENABLED) != null) {
			changes.put(ENABLED, getBoolean(ENABLED));
		}
		return mapResult(WorkCustomRoutine.save(getUser(), getString(ROUTINE_ID), changes));
	}

	// null when the key is absent; one comma separated value reads as several
	private List<String> list(String key) {
		GenRowStruct grs = this.store.getNoun(key);
		if (grs == null) {
			return null;
		}
		List<String> values = new ArrayList<>();
		for (int i = 0; i < grs.size(); i++) {
			for (String part : String.valueOf(grs.get(i)).split(",")) {
				if (!part.isBlank()) {
					values.add(part.trim());
				}
			}
		}
		return values;
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		// a schedule that keeps reading the owner's data, so it waits for their approval
		Map<String, String> meta = super.getMcpToolMetadata();
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.ASK.getValue());
		return meta;
	}

	@Override
	protected MCP_KEY_TYPE getKeyTypeForMCP(String key) {
		if (DAYS.equals(key) || SKILLS.equals(key)) {
			return MCP_KEY_TYPE.ARRAY;
		}
		if (ENABLED.equals(key)) {
			return MCP_KEY_TYPE.BOOLEAN;
		}
		return super.getKeyTypeForMCP(key);
	}

	@Override
	public String getReactorDescription() {
		return "Creates or changes one of the owner's routines: an instruction their agent does on a schedule, with "
				+ "read-only tools, and delivers only to the owner. Use it when the owner asks for something to happen "
				+ "regularly (\"every weekday at 8, summarize what my clients asked for and email it to me\"). Leave "
				+ "out routineId to create one; pass it, from ListRoutines, to change one, sending only what changes. "
				+ "The owner approves the exact routine on a card before it is saved. A routine can never send to "
				+ "anyone else. To try a saved routine now, use ManageRoutine with action run. Load "
				+ "collaboration/references/routines/schedule-a-routine.md before creating one.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		switch (key) {
		case ROUTINE_ID:
			return "Id of the routine to change, from ListRoutines. Leave out to create a new routine.";
		case NAME:
			return "Short name the owner will recognize, at most 80 characters, such as \"Client asks digest\".";
		case PROMPT:
			return "The owner's request in their own words, a short paragraph: what to look at and what the result "
					+ "should contain. Do not write steps, tool names, tool options, the time zone, a recipient, or "
					+ "any instruction to send; delivery is its own argument and happens automatically. Put a lasting "
					+ "layout in a skill instead. At most 4000 characters.";
		case TIME:
			return "Time of day to run, HH:mm in 24-hour format, in the routine's time zone. Default 08:00.";
		case DAYS:
			return "Days to run: MON, TUE, WED, THU, FRI, SAT, SUN. Default weekdays.";
		case TIMEZONE:
			return "IANA time zone of the time, such as America/New_York. Use the owner's own time zone.";
		case DELIVERY:
			return "Where the answer goes, always only to the owner: email (their own inbox), teams (a Teams message "
					+ "to themselves) or none (kept in Routines). Default email.";
		case SKILLS:
			return "Ids of the owner's own skills the agent loads, from ListMySkills. Optional.";
		case AGENT_ID:
			return "Agent to run it, when the owner names one; blank uses their assistant's agent.";
		case ENABLED:
			return "false saves the routine paused. New routines are on.";
		default:
			return super.getDescriptionForKey(key);
		}
	}
}
