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

import prerna.auth.User;
import prerna.collaboration.BrainTopicUtils;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainListTopics(status=["active"], accountId=["google"], limit=[30], offset=[0]);
public class BrainListTopicsReactor extends AbstractCollaborationReactor {

	private static final String STATUS = "status";
	private static final String ACCOUNT_ID = "accountId";
	private static final String LIMIT = "limit";
	private static final String OFFSET = "offset";

	public BrainListTopicsReactor() {
		this.keysToGet = new String[] { STATUS, ACCOUNT_ID, LIMIT, OFFSET };
		this.keyRequired = new int[] { 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		List<String> statuses = getNounAsStringList(STATUS);
		Integer limit = getIntFromKeyOrCurRow(LIMIT);
		Integer offset = getIntFromKeyOrCurRow(OFFSET);
		return mapResult(BrainTopicUtils.listTopics(user, statuses, getString(ACCOUNT_ID),
				limit == null ? BrainTopicUtils.DEFAULT_LIMIT : limit, offset == null ? 0 : offset));
	}

	@Override
	public String getReactorDescription() {
		return "Lists the signed-in user's Brain topics as { items, total }";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (STATUS.equals(key)) {
			return "Topic statuses to include: suggested, active, dormant, archived; all when omitted";
		} else if (ACCOUNT_ID.equals(key)) {
			return "Only topics in this account";
		} else if (LIMIT.equals(key)) {
			return "Page size, default 30";
		} else if (OFFSET.equals(key)) {
			return "Rows to skip, default 0";
		}
		return super.getDescriptionForKey(key);
	}
}
