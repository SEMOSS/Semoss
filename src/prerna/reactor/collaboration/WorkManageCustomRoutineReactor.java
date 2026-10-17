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

import prerna.auth.User;
import prerna.collaboration.WorkCustomRoutine;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkManageCustomRoutine(routineId=["..."], action=["pause"]);
public class WorkManageCustomRoutineReactor extends AbstractCollaborationReactor {

	private static final String ROUTINE_ID = "routineId";
	private static final String ACTION = "action";

	public WorkManageCustomRoutineReactor() {
		this.keysToGet = new String[] { ROUTINE_ID, ACTION };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		String routineId = getString(ROUTINE_ID);
		String action = String.valueOf(getString(ACTION)).trim().toLowerCase();
		switch (action) {
		case "pause":
			return mapResult(WorkCustomRoutine.setEnabled(user, routineId, false));
		case "resume":
			return mapResult(WorkCustomRoutine.setEnabled(user, routineId, true));
		case "run":
			WorkCustomRoutine.runNow(user, routineId);
			// a routine is not a sub-agent of this chat; say so, or the model waits on it with sub-agent tools
			return mapResult(Map.of("routineId", routineId, "status", "started", "note",
					"The routine now runs on its own, apart from this chat, and usually takes a few minutes. It is "
							+ "not a sub-agent: do not wait for it or check it with sub-agent tools. Tell the owner "
							+ "it started and where the result will go. To see whether it finished, use ListRoutines "
							+ "later, only if the owner asks."));
		case "delete":
			WorkCustomRoutine.delete(user, routineId);
			return mapResult(Map.of("routineId", routineId, "deleted", true));
		default:
			throw new IllegalArgumentException("action must be pause, resume, run or delete");
		}
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		// changes or runs the owner's routine, so it waits for their approval
		Map<String, String> meta = super.getMcpToolMetadata();
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.ASK.getValue());
		return meta;
	}

	@Override
	public String getReactorDescription() {
		return "Pauses, resumes, runs now, or deletes one of the owner's routines, by its id from ListRoutines. When "
				+ "the owner asks to run, test, try or preview a routine, call this with action run right away: the "
				+ "owner approves it on a card, so never tell them to do it themselves. A run uses today's data and "
				+ "runs on its own, apart from this chat, for a few minutes; it is not a sub-agent, so do not wait "
				+ "for it. Its result shows in Routines and goes where the routine delivers; ListRoutines shows "
				+ "whether it is still running.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ROUTINE_ID.equals(key)) {
			return "Id of the routine, from ListRoutines.";
		}
		if (ACTION.equals(key)) {
			return "pause, resume, run or delete.";
		}
		return super.getDescriptionForKey(key);
	}
}
