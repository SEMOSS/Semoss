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

// WorkSaveWork(taskId=["..."], title=["..."], status=["done"], nextStep=["..."], topicIds=["..."], note=["..."]);
public class WorkSaveWorkReactor extends AbstractWorkTaskReactor {
	private static final String TASK_ID = "taskId";
	private static final String TITLE = "title";
	private static final String DESCRIPTION = "description";
	private static final String STATUS = "status";
	private static final String NEXT_STEP = "nextStep";
	private static final String WAITING_ON = "waitingOnPersonId";
	private static final String DUE = "due";
	private static final String PRIORITY = "priority";
	private static final String ONGOING = "ongoing";
	private static final String TOPIC_IDS = "topicIds";
	private static final String THREAD_IDS = "threadIds";
	private static final String NOTE = "note";
	private static final String ACCEPT = "accept";

	public WorkSaveWorkReactor() {
		this.keysToGet = new String[] { TASK_ID, TITLE, DESCRIPTION, STATUS, NEXT_STEP, WAITING_ON, DUE, PRIORITY,
				ONGOING, TOPIC_IDS, THREAD_IDS, NOTE, ACCEPT };
		this.keyRequired = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		Map<String, Object> args = new LinkedHashMap<>();
		args.put("id", getString(TASK_ID));
		for (String key : new String[] { TITLE, DESCRIPTION, STATUS, NEXT_STEP, WAITING_ON, DUE, PRIORITY, NOTE }) {
			if (this.store.getNoun(key) != null) {
				args.put(key, getString(key));
			}
		}
		if (getBoolean(ONGOING) != null) {
			args.put(ONGOING, getBoolean(ONGOING));
		}
		if (Boolean.TRUE.equals(getBoolean(ACCEPT))) {
			args.put(ACCEPT, true);
		}
		args.put(TOPIC_IDS, strings(TOPIC_IDS));
		java.util.List<String> threads = strings(THREAD_IDS);
		// a new work item made in a thread's chat is about that thread
		String chatThread = chatThreadId();
		if (getString(TASK_ID) == null && threads.isEmpty() && chatThread != null) {
			threads.add(chatThread);
		}
		args.put(THREAD_IDS, threads);
		return mapResult(WorkTaskUtils.saveWork(getUser(), args, origin()));
	}

	@Override
	protected MCP_KEY_TYPE getKeyTypeForMCP(String key) {
		if (TOPIC_IDS.equals(key) || THREAD_IDS.equals(key)) {
			return MCP_KEY_TYPE.ARRAY;
		}
		if (ONGOING.equals(key) || ACCEPT.equals(key)) {
			return MCP_KEY_TYPE.BOOLEAN;
		}
		return super.getKeyTypeForMCP(key);
	}

	@Override
	public String getReactorDescription() {
		return "Creates or changes one of the owner's work items: an outcome they are driving, such as \"Ship the "
				+ "6.0.0 release\", not a single ask (add those with AddStep). Leave out taskId to create one with a "
				+ "title. Pass taskId to rename it, set its status (active, waiting, blocked, done, dropped), next "
				+ "step, who it waits on, due date, or topics, or add a note to its timeline. Use it when the owner "
				+ "asks; a work item can sit under several topics";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		switch (key) {
		case TASK_ID: return "The work item to change; leave out to create one";
		case TITLE: return "Short outcome, e.g. Remediate the container scan findings";
		case DESCRIPTION: return "What done looks like";
		case STATUS: return "active, waiting, blocked, done, or dropped";
		case NEXT_STEP: return "The one next thing to do";
		case WAITING_ON: return "Person id it is waiting on";
		case DUE: return "YYYY-MM-DD";
		case PRIORITY: return "high, medium, or low";
		case ONGOING: return "True for a long-running item with no end date";
		case TOPIC_IDS: return "Topic ids it belongs to (products and clients it touches)";
		case THREAD_IDS: return "Thread ids that move it";
		case NOTE: return "A line for its timeline, e.g. a decision or an update";
		case ACCEPT: return "True to accept one Brain proposed";
		default: return super.getDescriptionForKey(key);
		}
	}
}
