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
import prerna.collaboration.BrainPeopleUtils;
import prerna.collaboration.BrainTopicUtils;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainListPeople(query=["priya"], accountId=["..."], topicId=["..."], limit=[30], offset=[0]);
public class BrainListPeopleReactor extends AbstractCollaborationReactor {

	private static final String QUERY = "query";
	private static final String ACCOUNT_ID = "accountId";
	private static final String TOPIC_ID = "topicId";
	private static final String LIMIT = "limit";
	private static final String OFFSET = "offset";

	public BrainListPeopleReactor() {
		this.keysToGet = new String[] { QUERY, ACCOUNT_ID, TOPIC_ID, LIMIT, OFFSET };
		this.keyRequired = new int[] { 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		Integer limit = getIntFromKeyOrCurRow(LIMIT);
		Integer offset = getIntFromKeyOrCurRow(OFFSET);
		return mapResult(BrainPeopleUtils.listPeople(user, getString(QUERY), getString(ACCOUNT_ID),
				getString(TOPIC_ID), limit == null ? BrainTopicUtils.DEFAULT_LIMIT : limit, offset == null ? 0 : offset));
	}

	@Override
	public String getReactorDescription() {
		return "Lists the signed-in user's Brain people as { items, total }, strongest first";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (QUERY.equals(key)) {
			return "Name or email substring";
		} else if (ACCOUNT_ID.equals(key)) {
			return "Only people in this account";
		} else if (TOPIC_ID.equals(key)) {
			return "Only members of this topic";
		} else if (LIMIT.equals(key)) {
			return "Page size, default 30";
		} else if (OFFSET.equals(key)) {
			return "Rows to skip, default 0";
		}
		return super.getDescriptionForKey(key);
	}
}
