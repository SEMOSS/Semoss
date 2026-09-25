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
import prerna.collaboration.BrainTopicUtils;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainMergeTopics(sourceTopicId=["..."], targetTopicId=["..."]);
public class BrainMergeTopicsReactor extends AbstractCollaborationReactor {

	private static final String SOURCE_TOPIC_ID = "sourceTopicId";
	private static final String TARGET_TOPIC_ID = "targetTopicId";

	public BrainMergeTopicsReactor() {
		this.keysToGet = new String[] { SOURCE_TOPIC_ID, TARGET_TOPIC_ID };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		String sourceTopicId = getString(SOURCE_TOPIC_ID);
		String targetTopicId = getString(TARGET_TOPIC_ID);
		if (sourceTopicId == null || targetTopicId == null) {
			throw new IllegalArgumentException("Must pass a sourceTopicId and targetTopicId");
		}
		return mapResult(BrainTopicUtils.mergeTopics(user, sourceTopicId, targetTopicId));
	}

	@Override
	public String getReactorDescription() {
		return "Merges one Brain topic into another; threads, people, goals, notes, and keywords move to the target";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (SOURCE_TOPIC_ID.equals(key)) {
			return "Topic to merge away; it is removed after the merge";
		} else if (TARGET_TOPIC_ID.equals(key)) {
			return "Topic that receives everything";
		}
		return super.getDescriptionForKey(key);
	}
}
