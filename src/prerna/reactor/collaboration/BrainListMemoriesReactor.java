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
import prerna.collaboration.BrainMemoryUtils;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainListMemories(state=["active", "suggested"], refType=["person"], refId=["..."], query=["..."], limit=[50],
// offset=[0]);
public class BrainListMemoriesReactor extends AbstractCollaborationReactor {

	private static final String STATE = "state";
	private static final String REF_TYPE = "refType";
	private static final String REF_ID = "refId";
	private static final String QUERY = "query";
	private static final String LIMIT = "limit";
	private static final String OFFSET = "offset";

	public BrainListMemoriesReactor() {
		this.keysToGet = new String[] { STATE, REF_TYPE, REF_ID, QUERY, LIMIT, OFFSET };
		this.keyRequired = new int[] { 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		List<String> states = getNounAsStringList(STATE);
		Integer limit = getIntFromKeyOrCurRow(LIMIT);
		Integer offset = getIntFromKeyOrCurRow(OFFSET);
		return mapResult(BrainMemoryUtils.listMemories(user, states, getString(REF_TYPE), getString(REF_ID),
				getString(QUERY), limit == null ? BrainMemoryUtils.DEFAULT_LIMIT : limit, offset == null ? 0 : offset));
	}

	@Override
	public String getReactorDescription() {
		return "Lists the signed-in user's Brain memories as { items, total }";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (STATE.equals(key)) {
			return "Memory states to include: active, suggested, superseded, dismissed; active and suggested when omitted";
		} else if (REF_TYPE.equals(key)) {
			return "With refId, only memories about this: person, topic, account, or thread";
		} else if (REF_ID.equals(key)) {
			return "Id of the person, topic, account, or thread";
		} else if (QUERY.equals(key)) {
			return "Words to match; best matches first";
		} else if (LIMIT.equals(key)) {
			return "Page size, default 50";
		} else if (OFFSET.equals(key)) {
			return "Rows to skip, default 0";
		}
		return super.getDescriptionForKey(key);
	}
}
