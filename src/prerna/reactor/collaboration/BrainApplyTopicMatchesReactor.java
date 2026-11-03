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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.collaboration.BrainTopicMatches;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainApplyTopicMatches(topicId=["..."], changes=[{"threadId":"...","op":"move"}, {"threadId":"...","op":"reject"}]);
// op is move, also_link or reject
public class BrainApplyTopicMatchesReactor extends AbstractCollaborationReactor {

	private static final String TOPIC_ID = "topicId";
	private static final String CHANGES = "changes";

	public BrainApplyTopicMatchesReactor() {
		this.keysToGet = new String[] { TOPIC_ID, CHANGES };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		List<Map<String, Object>> changes = new ArrayList<>();
		GenRowStruct grs = this.store.getGenRowStruct(CHANGES);
		for (int i = 0; grs != null && i < grs.size(); i++) {
			if (!(grs.get(i) instanceof Map<?, ?> change)) {
				throw new IllegalArgumentException("Each change is a map {threadId, op}");
			}
			Map<String, Object> copy = new LinkedHashMap<>();
			change.forEach((k, v) -> copy.put(String.valueOf(k), v));
			changes.add(copy);
		}
		return mapResult(BrainTopicMatches.apply(getUser(), getString(TOPIC_ID), changes));
	}

	@Override
	public String getReactorDescription() {
		return "Applies the owner's choices from BrainPreviewTopicMatches, each checked against the conversation as it "
				+ "is now: move replaces the topics Brain guessed (never the owner's own links), also_link adds the "
				+ "topic, reject remembers the conversation is not about it; returns each change as applied or skipped";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (TOPIC_ID.equals(key)) {
			return "The topic the preview was for";
		} else if (CHANGES.equals(key)) {
			return "Maps of threadId and op (move, also_link or reject); conversations left out are not changed";
		}
		return super.getDescriptionForKey(key);
	}
}
