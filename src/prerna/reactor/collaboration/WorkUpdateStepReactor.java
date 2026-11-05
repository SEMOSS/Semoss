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

// WorkUpdateStep(stepId=["..."], status=["done"], note=["..."]);
public class WorkUpdateStepReactor extends AbstractWorkTaskReactor {
	private static final String STEP_ID = "stepId";
	private static final String STATUS = "status";
	private static final String TEXT = "text";
	private static final String DUE = "due";
	private static final String NOTE = "note";
	private static final String TASK_ID = "taskId";
	private static final String ASSIGNEE_ID = "assigneeId";

	public WorkUpdateStepReactor() {
		this.keysToGet = new String[] { STEP_ID, STATUS, TEXT, DUE, NOTE, TASK_ID, ASSIGNEE_ID };
		this.keyRequired = new int[] { 1, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		Map<String, Object> args = new LinkedHashMap<>();
		for (String key : new String[] { STATUS, TEXT, DUE, NOTE, TASK_ID, ASSIGNEE_ID }) {
			if (this.store.getNoun(key) != null) {
				args.put(key, getString(key));
			}
		}
		return mapResult(WorkTaskUtils.updateStep(getUser(), getString(STEP_ID), args, origin()));
	}

	@Override
	public String getReactorDescription() {
		return "Changes a step on the owner's list when they say what happened: mark it done, dropped, or "
				+ "waiting, with a note on the outcome; change its text or due date; or move it under a work item";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		switch (key) {
		case STEP_ID: return "The step's id, from ListMyWork or GetWork";
		case STATUS: return "open, waiting, done, or dropped";
		case TEXT: return "New text";
		case DUE: return "YYYY-MM-DD";
		case NOTE: return "Notes or the outcome, e.g. what was decided";
		case TASK_ID: return "Move it under this work item";
		case ASSIGNEE_ID: return "Person id when someone else now has to do it";
		default: return super.getDescriptionForKey(key);
		}
	}
}
