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

import java.util.LinkedHashMap;
import java.util.Map;

import prerna.collaboration.WorkTaskUtils;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkAddStep(text=["..."], taskId=["..."], topicId=["..."], threadId=["..."], due=["2026-11-07"], assigneeId=["..."]);
public class WorkAddStepReactor extends AbstractWorkTaskReactor {
	private static final String TEXT = "text";
	private static final String TASK_ID = "taskId";
	private static final String TOPIC_ID = "topicId";
	private static final String THREAD_ID = "threadId";
	private static final String DUE = "due";
	private static final String ASSIGNEE_ID = "assigneeId";

	public WorkAddStepReactor() {
		this.keysToGet = new String[] { TEXT, TASK_ID, TOPIC_ID, THREAD_ID, DUE, ASSIGNEE_ID };
		this.keyRequired = new int[] { 1, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		Map<String, Object> args = new LinkedHashMap<>();
		args.put("text", getString(TEXT));
		args.put("taskId", getString(TASK_ID));
		args.put("topicId", getString(TOPIC_ID));
		String thread = getString(THREAD_ID);
		args.put("threadId", thread != null ? thread : chatThreadId());
		args.put("due", getString(DUE));
		args.put("assigneeId", getString(ASSIGNEE_ID));
		return mapResult(WorkTaskUtils.addStep(getUser(), args, origin()));
	}

	@Override
	public String getReactorDescription() {
		return "Adds a to-do to the owner's list when they ask (\"add X to my list\", \"remind me to Y by "
				+ "Friday\"): under a work item when it serves one, otherwise loose under a topic. A step for "
				+ "someone else (assigneeId) is something the owner is waiting on. In a thread's chat it is linked "
				+ "to that thread";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		switch (key) {
		case TEXT: return "One short imperative line, e.g. Send Kira the revised budget";
		case TASK_ID: return "The work item it serves, from ListMyWork";
		case TOPIC_ID: return "The topic it belongs to when it has no work item";
		case THREAD_ID: return "The thread it came from; defaults to this chat's thread";
		case DUE: return "YYYY-MM-DD";
		case ASSIGNEE_ID: return "Person id when someone else has to do it; leave out for the owner";
		default: return super.getDescriptionForKey(key);
		}
	}
}
