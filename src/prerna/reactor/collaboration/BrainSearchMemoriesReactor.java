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

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

import prerna.auth.User;
import prerna.collaboration.BrainMemoryUtils;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainSearchMemories(query=["Priya budget"], about=[{"type": "person", "id": "..."}], limit=[10]);
// The thread assistant's SearchMemories tool: the owner's active memories, best match first
public class BrainSearchMemoriesReactor extends AbstractCollaborationReactor {

	private static final String QUERY = "query";
	private static final String ABOUT = "about";
	private static final String LIMIT = "limit";

	public BrainSearchMemoriesReactor() {
		this.keysToGet = new String[] { QUERY, ABOUT, LIMIT };
		this.keyRequired = new int[] { 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		BrainMemoryUtils.requireAssistantMemory(user);
		List<BrainMemoryUtils.Ref> about = new ArrayList<>();
		GenRowStruct grs = this.store.getNoun(ABOUT);
		if (grs != null) {
			for (int i = 0; i < grs.size(); i++) {
				BrainMemoryUtils.Ref ref = BrainMemoryUtils.parseRef(grs.getNoun(i).getValue());
				if (ref != null) {
					about.add(ref);
				}
			}
		}
		return mapResult(BrainMemoryUtils.search(user, getString(QUERY), about, getIntFromKeyOrCurRow(LIMIT)));
	}

	@Override
	public JSONObject getMcpProperties() {
		JSONObject properties = super.getMcpProperties();
		JSONObject ref = new JSONObject().put("type", "object")
				.put("properties", new JSONObject()
						.put("type", new JSONObject().put("type", "string").put("enum",
								new JSONArray(List.of(BrainMemoryUtils.PERSON, BrainMemoryUtils.TOPIC,
										BrainMemoryUtils.ACCOUNT, BrainMemoryUtils.THREAD))))
						.put("id", new JSONObject().put("type", "string")))
				.put("required", new JSONArray(List.of("type", "id")));
		properties.getJSONObject(ABOUT).put("type", "array").put("items", ref);
		properties.getJSONObject(LIMIT).put("type", "integer");
		return properties;
	}

	@Override
	public String getReactorDescription() {
		return "Search the owner's memories for something not under What you remember, for example what they told you "
				+ "about a person in another thread. Returns the best matches with their ids.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (QUERY.equals(key)) {
			return "Words to look for, such as a name or a subject; omit to list the newest";
		} else if (ABOUT.equals(key)) {
			return "Only memories about one of these, as {type, id}";
		} else if (LIMIT.equals(key)) {
			return "Most results to return, default 10";
		}
		return super.getDescriptionForKey(key);
	}
}
