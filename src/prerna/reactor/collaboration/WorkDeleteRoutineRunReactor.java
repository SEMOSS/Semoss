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

import java.util.List;
import java.util.Map;

import prerna.collaboration.WorkRoutineRoom;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class WorkDeleteRoutineRunReactor extends AbstractCollaborationReactor {
	private static final String ROUTINE_ID = "routineId";
	private static final String RUN_ID = "runId";

	public WorkDeleteRoutineRunReactor() {
		this.keysToGet = new String[] { ROUTINE_ID, RUN_ID };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		getUser();
		List<String> roomIds = WorkRoutineRoom.deleteRun(this.insight, getString(ROUTINE_ID), getString(RUN_ID));
		return mapResult(Map.of("deleted", true, "roomIds", roomIds));
	}

	@Override
	public String getReactorDescription() {
		return "Deletes one finished run of one of the signed-in user's routines from its run history, with the chats "
				+ "opened from it and the rooms its agent worked in, and returns those rooms' ids; when it was the "
				+ "routine's latest run, Routines stops showing its result";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ROUTINE_ID.equals(key)) {
			return "Routine id: inbox-daily-brief, weekly-meeting-agendas, event-planning, expense-review, or the id of "
					+ "one of the owner's own routines";
		}
		if (RUN_ID.equals(key)) {
			return "The run to delete, from the routine's run history";
		}
		return super.getDescriptionForKey(key);
	}
}
