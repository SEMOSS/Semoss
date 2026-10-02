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
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkListItems(view=["needs_me"], topicId=["..."], channel=["email"], sort=["priority"], limit=[30], offset=[0]);
public class WorkListItemsReactor extends AbstractCollaborationReactor {

	private static final String VIEW = "view";
	private static final String TOPIC_ID = "topicId";
	private static final String CHANNEL = "channel";
	private static final String SORT = "sort";
	private static final String LIMIT = "limit";
	private static final String OFFSET = "offset";

	public WorkListItemsReactor() {
		this.keysToGet = new String[] { VIEW, TOPIC_ID, CHANNEL, SORT, LIMIT, OFFSET };
		this.keyRequired = new int[] { 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		Integer limit = getIntFromKeyOrCurRow(LIMIT);
		Integer offset = getIntFromKeyOrCurRow(OFFSET);
		return mapResult(WorkItemUtils.listView(user, getString(VIEW), getString(TOPIC_ID), getString(CHANNEL),
				getString(SORT), limit == null ? 30 : limit, offset == null ? 0 : offset));
	}

	@Override
	public String getReactorDescription() {
		return "Lists the signed-in user's Work items for one queue view as { items, total, fyiCount, automatedSkippedCount }";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (VIEW.equals(key)) {
			return "needs_me (default), waiting, suggested, done_today, or all";
		} else if (TOPIC_ID.equals(key)) {
			return "Only items on this topic";
		} else if (CHANNEL.equals(key)) {
			return "email, teams, calendar, room, or task";
		} else if (SORT.equals(key)) {
			return "priority (default), received, or closed";
		} else if (LIMIT.equals(key)) {
			return "Page size, default 30";
		} else if (OFFSET.equals(key)) {
			return "Rows to skip, default 0";
		}
		return super.getDescriptionForKey(key);
	}
}
