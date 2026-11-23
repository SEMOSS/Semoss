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

import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import prerna.collaboration.WorkRoutinePreferences;
import prerna.reactor.ReactorDefinition;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkSaveRoutinePreferences(routine=["inbox-daily-brief"], preferences=["Keep drafts to three sentences."]);
// The assistant's SetRoutinePreferences tool: the text the Preferences box in Routines edits
public class WorkSaveRoutinePreferencesReactor extends AbstractCollaborationReactor {

	private static final String ROUTINE = "routine";
	private static final String PREFERENCES = "preferences";

	public WorkSaveRoutinePreferencesReactor() {
		this.keysToGet = new String[] { ROUTINE, PREFERENCES };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		return mapResult(WorkRoutinePreferences.save(getUser(), getString(ROUTINE), getString(PREFERENCES)));
	}

	@Override
	public JSONObject getMcpProperties() {
		JSONObject properties = super.getMcpProperties();
		properties.getJSONObject(ROUTINE).put("enum", new JSONArray(WorkRoutinePreferences.ROUTINE_IDS));
		return properties;
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		// changes what a scheduled routine is told on every run, so it waits for the owner's approval
		Map<String, String> meta = super.getMcpToolMetadata();
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.ASK.getValue());
		return meta;
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description(
				"Changes the preferences of one of the owner's routines: the daily inbox brief, the weekly meeting "
						+ "agendas, or event planning. This is the free text of the Preferences box in Routines. The "
						+ "routine reads it on every run and never reads memories, so use this, not a memory, when the "
						+ "owner says how that routine's results should be from now on. The Routine preferences section "
						+ "of your instructions gives each routine's current text and what it can shape. The owner "
						+ "approves the exact text on a card before it is saved; it applies from the routine's next run.")
				.key(ROUTINE, "The routine to change: " + String.join(", ", WorkRoutinePreferences.ROUTINE_IDS) + ".")
				.key(PREFERENCES,
						"The routine's complete new preferences, replacing its current text: start from the current "
								+ "text, keep what still applies, and change only what the owner asked for. Short, plain "
								+ "instructions, one per line.");
	}
}
