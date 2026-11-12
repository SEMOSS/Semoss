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

import prerna.collaboration.WorkTaskUtils;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkGetWork(taskId=["..."]);
public class WorkGetWorkReactor extends AbstractWorkTaskReactor {
	private static final String TASK_ID = "taskId";
	private static final String ROOM_ID = "roomId";

	public WorkGetWorkReactor() {
		this.keysToGet = new String[] { TASK_ID, ROOM_ID };
		this.keyRequired = new int[] { 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		String taskId = getString(TASK_ID);
		String roomId = getString(ROOM_ID);
		if ((taskId == null || taskId.isBlank()) && (roomId == null || roomId.isBlank())) {
			throw new IllegalArgumentException("Pass taskId or roomId");
		}
		return mapResult(taskId != null && !taskId.isBlank() ? WorkTaskUtils.getWork(getUser(), taskId)
				: WorkTaskUtils.getWorkForRoom(getUser(), roomId));
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition()
				.description(
						"One of the owner's work items in full: every step and its status, the timeline of updates and "
								+ "decisions, the threads that moved it, and its topics")
				.key(TASK_ID, "The work item's id, from ListMyWork")
				.key(ROOM_ID, "An owned assistant room; resolves its linked work item, or an empty map");
	}
}
