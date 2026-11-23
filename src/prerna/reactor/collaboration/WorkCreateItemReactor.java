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

import prerna.auth.User;
import prerna.collaboration.WorkItemUtils;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkCreateItem(threadId=["..."], title=["..."], askType=["errand"], assignee=["..."], dueAt=["..."]);
public class WorkCreateItemReactor extends AbstractCollaborationReactor {

	private static final String THREAD_ID = "threadId";
	private static final String TITLE = "title";
	private static final String ASK_TYPE = "askType";
	private static final String ASSIGNEE = "assignee";
	private static final String DUE_AT = "dueAt";

	public WorkCreateItemReactor() {
		this.keysToGet = new String[] { THREAD_ID, TITLE, ASK_TYPE, ASSIGNEE, DUE_AT };
		this.keyRequired = new int[] { 1, 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		return mapResult(WorkItemUtils.createItem(user, getString(THREAD_ID), getString(TITLE), getString(ASK_TYPE),
				getString(ASSIGNEE), getString(DUE_AT), false));
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description("Creates a Work item of your own on a thread")
				.key(THREAD_ID, "Thread the item belongs to").key(TITLE, "Short action phrase")
				.key(ASK_TYPE, "reply, approve, attend, review, waiting_on, errand, or fyi")
				.key(ASSIGNEE, "Person id, default you").key(DUE_AT, "ISO-8601 due time");
	}
}
