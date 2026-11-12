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
import prerna.collaboration.BrainThreadUtils;
import prerna.collaboration.BrainTopicUtils;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainListThreads(filter=["muted"], topicId=["..."], channel=["email"], limit=[30], offset=[0], detail=[true]);
public class BrainListThreadsReactor extends AbstractCollaborationReactor {

	private static final String FILTER = "filter";
	private static final String TOPIC_ID = "topicId";
	private static final String CHANNEL = "channel";
	private static final String LIMIT = "limit";
	private static final String OFFSET = "offset";
	private static final String DETAIL = "detail";

	public BrainListThreadsReactor() {
		this.keysToGet = new String[] { FILTER, TOPIC_ID, CHANNEL, LIMIT, OFFSET, DETAIL };
		this.keyRequired = new int[] { 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		Integer limit = getIntFromKeyOrCurRow(LIMIT);
		Integer offset = getIntFromKeyOrCurRow(OFFSET);
		return mapResult(BrainThreadUtils.listThreads(user, getString(FILTER), getString(TOPIC_ID), getString(CHANNEL),
				limit == null ? BrainTopicUtils.DEFAULT_LIMIT : limit, offset == null ? 0 : offset,
				Boolean.TRUE.equals(getBoolean(DETAIL))));
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition()
				.description("Lists the signed-in user's Brain threads as { items, total }, newest first")
				.key(FILTER, "muted or needs_topic_choice").key(TOPIC_ID, "Only threads linked to this topic")
				.key(CHANNEL, "email, teams, or calendar").key(LIMIT, "Page size, default 30")
				.key(OFFSET, "Rows to skip, default 0")
				.key(DETAIL, "true adds participants and summary to each thread");
	}
}
